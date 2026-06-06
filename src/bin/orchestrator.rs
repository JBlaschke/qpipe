// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Multi-frame routing design:
//   The single SharedQueue is replaced by a Router holding, under ONE mutex:
//     - `shared`:   the FIFO of unassigned frames (singles + unclaimed chunks)
//     - `directed`: one small VecDeque per connected consumer, for chunks
//                   that belong to a message that consumer has claimed
//     - `assign`:   msg_id -> (owner, delivered, count, last_seen) metadata
//     - `tomb`:     msg_ids whose messages can no longer complete; straggler
//                   frames of these are dropped at push or pop
//   Claiming happens at POP time: the first consumer handler to pop a chunk
//   of an unclaimed message becomes its owner. If a handler pops a chunk
//   owned by someone else, it moves the frame to the owner's directed queue
//   (total in-flight count unchanged, so redirects never block) and keeps
//   popping. Handlers always serve their directed queue before the shared
//   one, so claimed messages drain with priority and every redirected frame
//   is handled exactly once — no livelock, no busy-wait, no per-message
//   queues or threads.
//   Assignments are deleted the moment delivered == count. Consumer death
//   tombstones its in-flight messages; a periodic sweep expires idle
//   assignments into tombstones (QPIPE_ASSIGN_TTL_SECS, default 600) and
//   ages tombstones out (QPIPE_TOMBSTONE_TTL_SECS, default 600).
//   NOTE: stats counters count FRAMES, not messages, since chunks flow
//   through the queue individually.

use std::collections::{HashMap, VecDeque};
use std::env;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use rand::{rngs::SysRng, TryRng};

use log::{debug, info, warn, error};

use qpipe::{
    read_frame_ext, request_drain, request_shutdown,
    write_chunk_frame, write_frame, Frame,
    ACK_DRAIN, ACK_HEALTH, ACK_SHUTDOWN,
    ROLE_CONSUMER, ROLE_DRAIN, ROLE_HEALTHCHECK, ROLE_PRODUCER, ROLE_SHUTDOWN,
    TOKEN_LEN,
};

// Orchestrator lifecycle state. New producer/consumer sessions are only
// admitted while RUNNING; any other state stops admitting them but keeps
// the control port open so admin commands (health/drain/shutdown) still
// reach the orchestrator.
const STATE_RUNNING:       u8 = 0;
const STATE_DRAINING:      u8 = 1; // wait forever for queue + producers
const STATE_SHUTTING_DOWN: u8 = 2; // wait with timeout, then exit anyway

// How long to wait for queue drain after a shutdown is requested before
// giving up and exiting anyway. Tunable via QPIPE_DRAIN_TIMEOUT_SECS.
const DEFAULT_DRAIN_TIMEOUT_SECS: u64 = 30;

// Multi-frame housekeeping. An assignment idle longer than the assign TTL
// is presumed orphaned (producer died mid-message, or its consumer is gone)
// and becomes a tombstone; a tombstone idle longer than the tombstone TTL
// is forgotten. Tunable via QPIPE_ASSIGN_TTL_SECS / QPIPE_TOMBSTONE_TTL_SECS.
const DEFAULT_ASSIGN_TTL_SECS:    u64 = 600;
const DEFAULT_TOMBSTONE_TTL_SECS: u64 = 600;
const SWEEP_EVERY: Duration = Duration::from_secs(5);

type MsgId      = u128;
type ConsumerId = u64;

#[derive(Default)]
struct Stats {
    // Frames accepted from producers and enqueued
    posted_msgs:      AtomicU64,
    posted_bytes:     AtomicU64,
    // Frames successfully written to consumer sockets
    collected_msgs:   AtomicU64,
    collected_bytes:  AtomicU64,
    // Frames popped/held but NOT delivered (write failed, dead message, ...)
    dropped_msgs:     AtomicU64,
    dropped_bytes:    AtomicU64,
    // Connection counts
    active_producers: AtomicUsize,
    active_consumers: AtomicUsize,
}

enum ConnKind { Producer, Consumer }

struct ConnGuard {
    kind:  ConnKind,
    stats: Arc<Stats>,
}

impl ConnGuard {
    fn new(kind: ConnKind, stats: Arc<Stats>) -> Self {
        match kind {
            ConnKind::Producer => {
                stats.active_producers.fetch_add(1, Ordering::Relaxed);
            }
            ConnKind::Consumer => {
                stats.active_consumers.fetch_add(1, Ordering::Relaxed);
            }
        }
        Self { kind, stats }
    }
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        match self.kind {
            ConnKind::Producer => {
                self.stats.active_producers.fetch_sub(1, Ordering::Relaxed);
            }
            ConnKind::Consumer => {
                self.stats.active_consumers.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}

/// Per in-flight multi-frame message: who owns it and how far along it is.
struct Assign {
    owner:     ConsumerId,
    delivered: u32,
    count:     u32,
    last_seen: Instant,
}

#[derive(Default)]
struct RouterInner {
    shared:   VecDeque<Frame>,
    directed: HashMap<ConsumerId, VecDeque<Frame>>,
    assign:   HashMap<MsgId, Assign>,
    tomb:     HashMap<MsgId, Instant>,
    /// Frames in `shared` plus all `directed` queues; capacity applies here.
    total:    usize,
}

enum Disposition {
    Deliver,
    DropTombstoned,
    Redirect(ConsumerId),
}

struct Router {
    inner:         Mutex<RouterInner>,
    not_empty:     Condvar,
    not_full:      Condvar,
    capacity:      usize,
    next_consumer: AtomicU64,
    stats:         Arc<Stats>,
}

impl Router {
    fn new(capacity: usize, stats: Arc<Stats>) -> Self {
        Self {
            inner: Mutex::new(RouterInner::default()),
            not_empty: Condvar::new(),
            not_full:  Condvar::new(),
            capacity,
            next_consumer: AtomicU64::new(1),
            stats,
        }
    }

    fn depth(&self) -> usize {
        self.inner.lock().unwrap().total
    }

    /// (frames in flight, live multi-frame assignments, tombstones)
    fn gauges(&self) -> (usize, usize, usize) {
        let g = self.inner.lock().unwrap();
        (g.total, g.assign.len(), g.tomb.len())
    }

    fn register_consumer(&self) -> ConsumerId {
        let id = self.next_consumer.fetch_add(1, Ordering::Relaxed);
        self.inner.lock().unwrap().directed.insert(id, VecDeque::new());
        id
    }

    fn unregister_consumer(&self, id: ConsumerId) {
        let mut g = self.inner.lock().unwrap();

        // Doom every in-flight message this consumer still owns. A claim
        // only survives to this point if at least one of its chunks was
        // ACKed by the consumer (fail_delivery rescinds never-ACKed claims
        // before the handler exits), and those chunks die with it.
        let now = Instant::now();
        let owned: Vec<MsgId> = g.assign.iter()
            .filter(|(_, a)| a.owner == id)
            .map(|(m, _)| *m)
            .collect();
        for m in owned {
            g.assign.remove(&m);
            g.tomb.insert(m, now);
        }

        // Frames parked in the directed queue: drop the doomed ones, requeue
        // the rest — e.g. a chunk redirected here while the claim was being
        // rescinded still belongs to a message another consumer can take
        // whole, and dropping it would strand that message forever.
        let leftovers = g.directed.remove(&id).unwrap_or_default();
        let mut requeued = false;
        for f in leftovers {
            let doomed = matches!(
                &f, Frame::Chunk { id, .. } if g.tomb.contains_key(id)
            );
            if doomed {
                g.total -= 1;
                self.stats.dropped_msgs.fetch_add(1, Ordering::Relaxed);
                self.stats.dropped_bytes
                    .fetch_add(f.payload_len() as u64, Ordering::Relaxed);
            } else {
                g.shared.push_back(f); // already counted in `total`
                requeued = true;
            }
        }
        if requeued {
            self.not_empty.notify_all();
        }
        self.not_full.notify_all();
    }

    /// Enqueue one frame from a producer. Blocks while the system is at
    /// capacity (shared + directed combined). Returns false if the frame
    /// belongs to a tombstoned message and was dropped instead (accounted
    /// in the dropped counters).
    fn push(&self, frame: Frame) -> bool {
        let mut g = self.inner.lock().unwrap();
        loop {
            if let Frame::Chunk { id, .. } = &frame {
                let now = Instant::now();
                if let Some(ts) = g.tomb.get_mut(id) {
                    *ts = now; // keep tomb alive while stragglers trickle in
                    self.stats.dropped_msgs.fetch_add(1, Ordering::Relaxed);
                    self.stats.dropped_bytes
                        .fetch_add(frame.payload_len() as u64, Ordering::Relaxed);
                    return false;
                }
                // Refresh active assignments even when delivery is backed up,
                // so a jammed queue doesn't expire an in-flight message.
                if let Some(a) = g.assign.get_mut(id) {
                    a.last_seen = now;
                }
            }
            if g.total < self.capacity {
                break;
            }
            g = self.not_full.wait(g).unwrap();
        }
        g.shared.push_back(frame);
        g.total += 1;
        // notify_all, not notify_one: a chunk of a claimed message can only
        // be delivered by its owner, but any consumer might be the one that
        // wakes first and redirects it there.
        self.not_empty.notify_all();
        true
    }

    /// Blocks until a frame deliverable by consumer `me` is available.
    /// Directed frames (chunks of messages `me` owns) take priority; shared
    /// frames are claimed, redirected, or dropped per the assignment map.
    fn pop_for(&self, me: ConsumerId) -> Frame {
        let mut g = self.inner.lock().unwrap();
        loop {
            let frame = match g.directed.get_mut(&me).and_then(|q| q.pop_front())
            {
                Some(f) => f,
                None => match g.shared.pop_front() {
                    Some(f) => f,
                    None => {
                        g = self.not_empty.wait(g).unwrap();
                        continue;
                    }
                },
            };

            match Self::classify(&mut g, me, &frame) {
                Disposition::Deliver => {
                    g.total -= 1;
                    self.not_full.notify_one();
                    return frame;
                }
                Disposition::DropTombstoned => {
                    g.total -= 1;
                    self.not_full.notify_one();
                    self.stats.dropped_msgs.fetch_add(1, Ordering::Relaxed);
                    self.stats.dropped_bytes
                        .fetch_add(frame.payload_len() as u64, Ordering::Relaxed);
                }
                Disposition::Redirect(owner) => {
                    match g.directed.get_mut(&owner) {
                        Some(q) => {
                            // The frame stays in the system: `total` is
                            // unchanged, so a redirect never blocks on
                            // capacity (no deadlock path), and each frame
                            // redirects at most once (shared -> directed).
                            q.push_back(frame);
                            self.not_empty.notify_all();
                        }
                        None => {
                            // Owner vanished without tombstoning. Unreachable
                            // (unregister tombstones under this same lock),
                            // but drop defensively rather than leak.
                            if let Frame::Chunk { id, .. } = &frame {
                                let id = *id;
                                g.assign.remove(&id);
                                g.tomb.insert(id, Instant::now());
                            }
                            g.total -= 1;
                            self.not_full.notify_one();
                            self.stats.dropped_msgs
                                .fetch_add(1, Ordering::Relaxed);
                            self.stats.dropped_bytes.fetch_add(
                                frame.payload_len() as u64, Ordering::Relaxed
                            );
                        }
                    }
                }
            }
        }
    }

    /// Decide what to do with a popped frame, updating assignment
    /// bookkeeping. Singles always deliver. Chunks deliver if unclaimed
    /// (claiming them for `me`) or owned by `me`; redirect if owned by
    /// another consumer; drop if their message is tombstoned.
    fn classify(g: &mut RouterInner, me: ConsumerId, f: &Frame) -> Disposition {
        let (id, count) = match f {
            Frame::Msg(_) => return Disposition::Deliver,
            Frame::Chunk { id, count, .. } => (*id, *count),
        };
        let now = Instant::now();

        if let Some(ts) = g.tomb.get_mut(&id) {
            *ts = now;
            return Disposition::DropTombstoned;
        }

        enum Hit { Unclaimed, Mine(bool /* complete */), Other(ConsumerId) }
        let hit = match g.assign.get_mut(&id) {
            None => Hit::Unclaimed,
            Some(a) if a.owner == me => {
                a.delivered += 1;
                a.last_seen = now;
                Hit::Mine(a.delivered >= a.count)
            }
            Some(a) => Hit::Other(a.owner),
        };

        match hit {
            Hit::Unclaimed => {
                // First chunk of this message to be popped: claim it.
                if count > 1 {
                    g.assign.insert(id, Assign {
                        owner: me, delivered: 1, count, last_seen: now,
                    });
                } // degenerate 1-chunk message completes on delivery; no entry
                Disposition::Deliver
            }
            Hit::Mine(complete) => {
                if complete {
                    g.assign.remove(&id); // happy-path cleanup
                }
                Disposition::Deliver
            }
            Hit::Other(owner) => Disposition::Redirect(owner),
        }
    }

    /// Handle a failed write of `frame` to consumer `me`, losing as little
    /// as possible. write_frame/write_chunk_frame return Ok only after the
    /// consumer's ACK, so on failure THIS frame never arrived — and if it
    /// was also the only frame of its message ever handed to `me`
    /// (delivered == 1), nothing of the message reached the dead consumer:
    /// rescind the claim and requeue the frame so another consumer can take
    /// the whole message. Singles are always requeued (original behavior).
    /// Only when earlier chunks WERE ACKed — they died inside the dead
    /// consumer — is the message doomed: tombstone it, drop the frame.
    /// Returns true if the frame was requeued. (Requeueing can block while
    /// the queue is at capacity, like push — the same exposure the original
    /// single-frame requeue had.)
    fn fail_delivery(&self, me: ConsumerId, frame: Frame) -> bool {
        enum Verdict { Requeue, UnclaimAndRequeue, Doom }

        let mut g = self.inner.lock().unwrap();
        let verdict = match &frame {
            Frame::Msg(_) => Verdict::Requeue,
            Frame::Chunk { id, count, .. } => match g.assign.get(id) {
                // Never-ACKed first chunk: fully salvageable.
                Some(a) if a.owner == me && a.delivered == 1 => {
                    Verdict::UnclaimAndRequeue
                }
                // Someone else's claim (unreachable in practice): leave it
                // alone, put the frame back; pop will redirect it.
                Some(a) if a.owner != me => Verdict::Requeue,
                // delivered > 1: earlier chunks were ACKed by the dead
                // consumer and died with it.
                Some(_) => Verdict::Doom,
                // Entry already removed => the failed frame was the LAST
                // chunk of a completed claim — earlier chunks were ACKed —
                // unless it's a degenerate 1-chunk message, which is
                // self-contained and salvageable.
                None if *count <= 1 => Verdict::Requeue,
                None => Verdict::Doom,
            },
        };

        let (requeue, unclaim) = match verdict {
            Verdict::Doom => (false, false),
            Verdict::Requeue => (true, false),
            Verdict::UnclaimAndRequeue => (true, true),
        };

        if !requeue {
            if let Frame::Chunk { id, .. } = &frame {
                let id = *id;
                g.assign.remove(&id);
                g.tomb.insert(id, Instant::now());
            }
            return false;
        }
        if unclaim {
            if let Frame::Chunk { id, .. } = &frame {
                g.assign.remove(id);
            }
        }
        while g.total >= self.capacity {
            g = self.not_full.wait(g).unwrap();
        }
        g.shared.push_back(frame);
        g.total += 1;
        self.not_empty.notify_all();
        true
    }

    /// Expire stale state. Returns (assignments expired, tombstones purged).
    fn sweep(&self, assign_ttl: Duration, tomb_ttl: Duration) -> (usize, usize) {
        let mut g = self.inner.lock().unwrap();
        let now = Instant::now();

        // An expired assignment becomes a tombstone: its already-delivered
        // chunks are stuck at the old owner, so remaining frames can never
        // form a complete message for anyone.
        let expired: Vec<MsgId> = g.assign.iter()
            .filter(|(_, a)| now.duration_since(a.last_seen) >= assign_ttl)
            .map(|(m, _)| *m)
            .collect();
        for m in &expired {
            g.assign.remove(m);
            g.tomb.insert(*m, now);
        }

        let before = g.tomb.len();
        g.tomb.retain(|_, t| now.duration_since(*t) < tomb_ttl);
        (expired.len(), before - g.tomb.len())
    }
}

fn env_duration_secs(name: &str, default_secs: u64) -> Duration {
    env::var(name)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(default_secs))
}

fn main() -> ExitCode {
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("warn")
    ).init();

    let args: Vec<String> = env::args().skip(1).collect();

    // ── Client modes ────────────────────────────────────────────────────────
    match args.first().map(String::as_str) {
        Some("--shutdown") => {
            let addr = args.get(1).cloned()
                .unwrap_or_else(|| "127.0.0.1:7000".to_string());
            return match request_shutdown(&addr) {
                Ok(()) => {
                    info!("shutdown acknowledged by {}", addr);
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("shutdown request to {} failed: {}", addr, e);
                    ExitCode::FAILURE
                }
            };
        }
        Some("--drain") => {
            let addr = args.get(1).cloned()
                .unwrap_or_else(|| "127.0.0.1:7000".to_string());
            return match request_drain(&addr) {
                Ok(()) => {
                    info!("drain acknowledged by {}", addr);
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("drain request to {} failed: {}", addr, e);
                    ExitCode::FAILURE
                }
            };
        }
        _ => {}
    }

    // ── Server mode ─────────────────────────────────────────────────────────
    match run_server(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!("orchestrator failed: {}", e);
            ExitCode::FAILURE
        }
    }
}

fn run_server(args: &[String]) -> io::Result<()> {
    let listen_addr = args.first().cloned()
        .unwrap_or_else(|| "0.0.0.0:7000".to_string());
    let capacity: usize = args.get(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
    let sfreq: u64 = args.get(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let stats  = Arc::new(Stats::default());
    let router = Arc::new(Router::new(capacity, stats.clone()));
    let state  = Arc::new(AtomicU8::new(STATE_RUNNING));
    // Signals the accept loop to stop. Set after drain completes so any
    // late admin commands are still served until the very last moment.
    let exit   = Arc::new(AtomicBool::new(false));

    let assign_ttl = env_duration_secs(
        "QPIPE_ASSIGN_TTL_SECS", DEFAULT_ASSIGN_TTL_SECS
    );
    let tomb_ttl = env_duration_secs(
        "QPIPE_TOMBSTONE_TTL_SECS", DEFAULT_TOMBSTONE_TTL_SECS
    );

    // Reporter thread.
    {
        let stats  = stats.clone();
        let router = router.clone();
        let state  = state.clone();
        thread::spawn(
            move || stats_reporter(stats, router, state, Duration::from_secs(sfreq))
        );
    }

    let listener = TcpListener::bind(&listen_addr)?;
    listener.set_nonblocking(true)?;

    info!(
        "Orchestrator control listening on {} (queue capacity {})",
        listener.local_addr()?, capacity
    );

    // Accept loop runs in its own thread for the entire lifetime of the
    // orchestrator. While the orchestrator is draining or shutting down it
    // still admits admin requests (health/drain/shutdown) but rejects new
    // producers and consumers — see handle_control.
    let accept_handle = {
        let router = router.clone();
        let stats  = stats.clone();
        let state  = state.clone();
        let exit   = exit.clone();
        thread::spawn(move || accept_loop(listener, router, stats, state, exit))
    };

    // Block until something flips the state out of RUNNING, expiring stale
    // multi-frame bookkeeping every few seconds along the way.
    let mut last_sweep = Instant::now();
    while state.load(Ordering::SeqCst) == STATE_RUNNING {
        thread::sleep(Duration::from_millis(100));
        if last_sweep.elapsed() >= SWEEP_EVERY {
            let (expired, _purged) = router.sweep(assign_ttl, tomb_ttl);
            if expired > 0 {
                warn!(
                    "expired {} stale multi-frame assignment(s); \
                     straggler frames will be dropped",
                    expired
                );
            }
            last_sweep = Instant::now();
        }
    }

    // ── Drain phase ─────────────────────────────────────────────────────────
    // While we're in here the accept loop is still running, so a follow-up
    // `--shutdown` can land and upgrade STATE_DRAINING → STATE_SHUTTING_DOWN.
    let drain_result = drain(router, stats, state, assign_ttl, tomb_ttl);

    // Stop accepting and join.
    exit.store(true, Ordering::SeqCst);
    let _ = accept_handle.join();
    drain_result
}

fn accept_loop(
            listener: TcpListener,
            router:   Arc<Router>,
            stats:    Arc<Stats>,
            state:    Arc<AtomicU8>,
            exit:     Arc<AtomicBool>,
        ) {
    while !exit.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, _peer)) => {
                debug!("Spawning handler thread");
                let router = router.clone();
                let stats  = stats.clone();
                let state  = state.clone();
                thread::spawn(move || {
                    if let Err(e) = handle_control(stream, router, stats, state) {
                        warn!("Session error: '{}'", e);
                    }
                    debug!("Handler thread done");
                });
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(100));
            }
            Err(e) => warn!("Control accept error: '{}'", e),
        }
    }
}

fn drain(
            router: Arc<Router>,
            stats:  Arc<Stats>,
            state:  Arc<AtomicU8>,
            assign_ttl: Duration,
            tomb_ttl:   Duration,
        ) -> io::Result<()> {
    let initial_state = state.load(Ordering::SeqCst);
    let drain_timeout = env_duration_secs(
        "QPIPE_DRAIN_TIMEOUT_SECS", DEFAULT_DRAIN_TIMEOUT_SECS
    );

    match initial_state {
        STATE_DRAINING => {
            info!("entering DRAIN state — no timeout, waiting for queue to empty");
        }
        STATE_SHUTTING_DOWN => {
            info!(
                "entering SHUTDOWN state — drain timeout {:?}",
                drain_timeout
            );
        }
        _ => {} // unreachable but harmless
    }

    let drain_start = Instant::now();
    // Set the first time we observe STATE_SHUTTING_DOWN; the timeout is
    // measured from that point. For a pure shutdown this is the very first
    // iteration; for drain-then-shutdown it's whenever the upgrade happens.
    let mut shutdown_at: Option<Instant> = None;
    let mut last_progress_log = Instant::now();
    let mut last_sweep = Instant::now();

    loop {
        let st        = state.load(Ordering::SeqCst);
        let depth     = router.depth();
        let producers = stats.active_producers.load(Ordering::Relaxed);
        let consumers = stats.active_consumers.load(Ordering::Relaxed);

        // Detect drain → shutdown transition; start the timeout clock.
        if st == STATE_SHUTTING_DOWN && shutdown_at.is_none() {
            shutdown_at = Some(Instant::now());
            if initial_state == STATE_DRAINING {
                info!(
                    "drain upgraded to shutdown; timeout {:?} from now",
                    drain_timeout
                );
            }
        }

        // Clean exit: nothing buffered, no producers still feeding.
        if depth == 0 && producers == 0 {
            info!(
                "drained cleanly in {:?} (consumers still attached: {})",
                drain_start.elapsed(), consumers
            );
            return Ok(());
        }

        // Timeout only applies when shutdown was requested (now or earlier).
        if let Some(t0) = shutdown_at {
            if t0.elapsed() >= drain_timeout {
                warn!(
                    "drain timeout after {:?}: depth={}, active_producers={}, active_consumers={}; exiting",
                    drain_timeout, depth, producers, consumers
                );
                return Ok(());
            }
        }

        // Keep multi-frame bookkeeping tidy while draining, too.
        if last_sweep.elapsed() >= SWEEP_EVERY {
            let _ = router.sweep(assign_ttl, tomb_ttl);
            last_sweep = Instant::now();
        }

        // Periodic progress line so the log reflects ongoing drain state.
        if last_progress_log.elapsed() >= Duration::from_secs(5) {
            let label = if st == STATE_DRAINING { "draining" } else { "shutting down" };
            info!(
                "{}: in_queue={}, active_producers={}, active_consumers={}, elapsed={:?}",
                label, depth, producers, consumers, drain_start.elapsed()
            );
            last_progress_log = Instant::now();
        }

        thread::sleep(Duration::from_millis(200));
    }
}

fn stats_reporter(
            stats:  Arc<Stats>,
            router: Arc<Router>,
            state:  Arc<AtomicU8>,
            every:  Duration,
        ) {
    let mut last_posted_msgs     = 0u64;
    let mut last_posted_bytes    = 0u64;
    let mut last_collected_msgs  = 0u64;
    let mut last_collected_bytes = 0u64;
    let mut last_dropped_msgs    = 0u64;
    let mut last_dropped_bytes   = 0u64;

    // Run only while accepting traffic; stop once the orchestrator is
    // draining or shutting down so the drain-phase log lines aren't
    // interleaved with throughput noise.
    while state.load(Ordering::Relaxed) == STATE_RUNNING {
        thread::sleep(every);

        let posted_msgs     = stats.posted_msgs.load(Ordering::Relaxed);
        let posted_bytes    = stats.posted_bytes.load(Ordering::Relaxed);
        let collected_msgs  = stats.collected_msgs.load(Ordering::Relaxed);
        let collected_bytes = stats.collected_bytes.load(Ordering::Relaxed);
        let dropped_msgs    = stats.dropped_msgs.load(Ordering::Relaxed);
        let dropped_bytes   = stats.dropped_bytes.load(Ordering::Relaxed);

        let dm_posted    = posted_msgs - last_posted_msgs;
        let db_posted    = posted_bytes - last_posted_bytes;
        let dm_collected = collected_msgs - last_collected_msgs;
        let db_collected = collected_bytes - last_collected_bytes;
        let dm_dropped   = dropped_msgs - last_dropped_msgs;
        let db_dropped   = dropped_bytes - last_dropped_bytes;

        last_posted_msgs     = posted_msgs;
        last_posted_bytes    = posted_bytes;
        last_collected_msgs  = collected_msgs;
        last_collected_bytes = collected_bytes;
        last_dropped_msgs    = dropped_msgs;
        last_dropped_bytes   = dropped_bytes;

        let (qd, assigns, tombs) = router.gauges();
        let prod = stats.active_producers.load(Ordering::Relaxed);
        let cons = stats.active_consumers.load(Ordering::Relaxed);

        info!(
            "[stats] +{dm_posted} frames ({db_posted} B) posted | \
             +{dm_collected} frames ({db_collected} B) collected | \
             +{dm_dropped} frames ({db_dropped} B) dropped | \
             in_queue={qd} multiframe_assignments={assigns} tombstones={tombs} | \
             producers={prod} consumers={cons} | totals: posted={posted_msgs} collected={collected_msgs} dropped={dropped_msgs}"
        );
    }
}

fn handle_control(
            mut ctrl: TcpStream,
            router:   Arc<Router>,
            stats:    Arc<Stats>,
            state:    Arc<AtomicU8>,
        ) -> io::Result<()> {
    ctrl.set_nodelay(true).ok();

    let mut role = [0u8; 1];
    ctrl.read_exact(&mut role)?;
    let role = role[0];

    // ── Admin roles ─────────────────────────────────────────────────────────
    // Always honored, regardless of lifecycle state. In particular, SHUTDOWN
    // is honored while DRAINING — that's the whole point of being able to
    // get impatient.

    if role == ROLE_HEALTHCHECK {
        ctrl.write_all(&[ACK_HEALTH])?;
        ctrl.flush()?;
        return Ok(());
    }

    if role == ROLE_DRAIN {
        let peer = ctrl.peer_addr().ok().map(|a| a.to_string())
            .unwrap_or_else(|| "<unknown>".into());
        info!("drain requested by {}", peer);
        ctrl.write_all(&[ACK_DRAIN])?;
        ctrl.flush()?;
        // Enter drain only if currently running. Idempotent if already
        // draining; doesn't downgrade from shutting-down.
        let _ = state.compare_exchange(
            STATE_RUNNING, STATE_DRAINING,
            Ordering::SeqCst, Ordering::SeqCst
        );
        return Ok(());
    }

    if role == ROLE_SHUTDOWN {
        let peer = ctrl.peer_addr().ok().map(|a| a.to_string())
            .unwrap_or_else(|| "<unknown>".into());
        info!("shutdown requested by {}", peer);
        ctrl.write_all(&[ACK_SHUTDOWN])?;
        ctrl.flush()?;
        // Shutdown overrides any prior state, including drain.
        state.store(STATE_SHUTTING_DOWN, Ordering::SeqCst);
        return Ok(());
    }

    if role != ROLE_PRODUCER && role != ROLE_CONSUMER {
        return Err(
            io::Error::new(io::ErrorKind::InvalidData, "unknown role byte")
        );
    }

    // ── Producer / consumer ────────────────────────────────────────────────
    // Only admitted while RUNNING. During drain/shutdown the orchestrator is
    // trying to wind down; admitting a fresh producer would extend the drain
    // indefinitely, and a fresh consumer has nothing useful to do (and the
    // process is about to exit anyway).
    if state.load(Ordering::SeqCst) != STATE_RUNNING {
        debug!(
            "rejecting role 0x{:02x} session: orchestrator is not running",
            role
        );
        return Ok(());
    }

    let bind_ip = ctrl.local_addr()?.ip();
    let data_listener = TcpListener::bind(SocketAddr::new(bind_ip, 0))?;
    let port = data_listener.local_addr()?.port();

    let mut token = [0u8; TOKEN_LEN];
    SysRng.try_fill_bytes(&mut token).map_err(
        |e| io::Error::new(io::ErrorKind::Other, e)
    )?;

    ctrl.write_all(&port.to_be_bytes())?;
    ctrl.write_all(&token)?;
    ctrl.flush()?;
    drop(ctrl);

    let mut data = loop {
        let (mut s, peer) = data_listener.accept()?;
        s.set_nodelay(true).ok();
        s.set_read_timeout(Some(Duration::from_secs(5))).ok();

        let mut got = [0u8; TOKEN_LEN];
        match s.read_exact(&mut got) {
            Ok(()) if got == token => {
                s.set_read_timeout(None).ok();
                debug!("client {} authenticated on ephemeral port {}", peer, port);
                break s;
            }
            _ => continue,
        }
    };

    if role == ROLE_PRODUCER {
        debug!("Starting producer");
        let x = run_producer(&mut data, router, stats);
        debug!("Stopping producer");
        x
    } else {
        debug!("Starting consumer");
        let x = run_consumer(&mut data, router, stats);
        debug!("Stopping consumer");
        x
    }
}

fn run_producer(
            stream: &mut TcpStream,
            router: Arc<Router>,
            stats:  Arc<Stats>,
        ) -> io::Result<()> {
    let _guard = ConnGuard::new(ConnKind::Producer, stats.clone());

    loop {
        match read_frame_ext(stream)? {
            Some(frame) => {
                let len = frame.payload_len() as u64;
                stats.posted_msgs.fetch_add(1, Ordering::Relaxed);
                stats.posted_bytes.fetch_add(len, Ordering::Relaxed);
                if !router.push(frame) {
                    // Straggler of a tombstoned message; push already
                    // accounted for it in the dropped counters.
                    debug!("dropped straggler frame of a dead message");
                }
            }
            None => return Ok(()),
        }
    }
}

fn run_consumer(
            stream: &mut TcpStream,
            router: Arc<Router>,
            stats:  Arc<Stats>,
        ) -> io::Result<()> {
    let _guard = ConnGuard::new(ConnKind::Consumer, stats.clone());

    // RAII registration: the directed queue and any owned assignments must
    // be cleaned up on EVERY exit path, or frames leak and drain never ends.
    struct Registration<'a> {
        router: &'a Router,
        id:     ConsumerId,
    }
    impl Drop for Registration<'_> {
        fn drop(&mut self) {
            self.router.unregister_consumer(self.id);
        }
    }
    let cid = router.register_consumer();
    let _reg = Registration { router: router.as_ref(), id: cid };

    loop {
        let frame = router.pop_for(cid);
        let len = frame.payload_len() as u64;

        let res = match &frame {
            Frame::Msg(p) => write_frame(stream, p),
            Frame::Chunk { id, idx, count, payload } => {
                write_chunk_frame(stream, *id, *idx, *count, payload)
            }
        };

        match res {
            Ok(()) => {
                stats.collected_msgs.fetch_add(1, Ordering::Relaxed);
                stats.collected_bytes.fetch_add(len, Ordering::Relaxed);
                stream.flush().ok();
            }
            Err(e) => {
                stats.dropped_msgs.fetch_add(1, Ordering::Relaxed);
                stats.dropped_bytes.fetch_add(len, Ordering::Relaxed);

                // write_frame only returns Ok after the consumer's ACK, so
                // this frame never arrived. Let the router salvage what it
                // can: singles and never-ACKed first chunks are requeued for
                // another consumer; messages that already had chunks ACKed
                // by this (now dead) consumer are doomed and tombstoned.
                if router.fail_delivery(cid, frame) {
                    debug!("requeued undelivered frame for another consumer");
                }

                if matches!(
                    e.kind(),
                    io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::UnexpectedEof
                ) {
                    warn!("Write failed with: '{}'. Dropping client.", e);
                    return Ok(());
                }
                error!("Write failed with: '{}'. Dropping client.", e);
                return Err(e);
            }
        }
    }
}

// Deterministic, single-threaded tests for the Router's claim / redirect /
// tombstone logic. Each sequence is arranged so pop_for never blocks.
#[cfg(test)]
mod router_tests {
    use super::*;
    use qpipe::Frame;

    fn mk(capacity: usize) -> Router {
        Router::new(capacity, Arc::new(Stats::default()))
    }

    fn ch(id: u128, idx: u32, count: u32) -> Frame {
        Frame::Chunk { id, idx, count, payload: vec![idx as u8] }
    }

    #[test]
    fn singles_flow_through() {
        let r = mk(8);
        let a = r.register_consumer();
        assert!(r.push(Frame::Msg(b"x".to_vec())));
        assert_eq!(r.pop_for(a), Frame::Msg(b"x".to_vec()));
        assert_eq!(r.gauges(), (0, 0, 0));
    }

    #[test]
    fn chunks_follow_the_claiming_consumer() {
        let r = mk(8);
        let a = r.register_consumer();
        let b = r.register_consumer();
        for f in [
            ch(1, 0, 3), ch(1, 1, 3), ch(1, 2, 3),
            Frame::Msg(b"solo".to_vec()),
        ] {
            assert!(r.push(f));
        }

        // A pops first and thereby claims message 1.
        assert_eq!(r.pop_for(a), ch(1, 0, 3));
        // B pops chunks 1 and 2, redirects both into A's directed queue, and
        // ends up delivering the unrelated single-frame message.
        assert_eq!(r.pop_for(b), Frame::Msg(b"solo".to_vec()));
        // A drains its directed queue.
        assert_eq!(r.pop_for(a), ch(1, 1, 3));
        assert_eq!(r.pop_for(a), ch(1, 2, 3));
        // Completion removed the assignment; nothing left anywhere.
        assert_eq!(r.gauges(), (0, 0, 0));
    }

    #[test]
    fn consumer_death_tombstones_its_messages() {
        let r = mk(8);
        let a = r.register_consumer();
        assert!(r.push(ch(9, 0, 3)));
        assert_eq!(r.pop_for(a), ch(9, 0, 3)); // claim
        r.unregister_consumer(a);

        // Stragglers of the dead message are refused at the door...
        assert!(!r.push(ch(9, 1, 3)));
        assert_eq!(r.gauges(), (0, 0, 1));

        // ...and the tombstone itself eventually ages out.
        let (expired, purged) =
            r.sweep(Duration::from_secs(600), Duration::ZERO);
        assert_eq!((expired, purged), (0, 1));
        assert_eq!(r.gauges(), (0, 0, 0));
    }

    #[test]
    fn unregister_drops_directed_frames() {
        let r = mk(8);
        let a = r.register_consumer();
        let b = r.register_consumer();
        for f in [ch(5, 0, 2), ch(5, 1, 2), Frame::Msg(b"x".to_vec())] {
            assert!(r.push(f));
        }
        assert_eq!(r.pop_for(a), ch(5, 0, 2));               // A claims msg 5
        assert_eq!(r.pop_for(b), Frame::Msg(b"x".to_vec())); // 5/1 → A's queue
        assert_eq!(r.depth(), 1);
        r.unregister_consumer(a); // drops the parked frame, tombstones msg 5
        assert_eq!(r.gauges(), (0, 0, 1));
    }

    #[test]
    fn stale_assignments_expire_into_tombstones() {
        let r = mk(8);
        let a = r.register_consumer();
        assert!(r.push(ch(7, 0, 2)));
        assert_eq!(r.pop_for(a), ch(7, 0, 2));
        assert_eq!(r.gauges().1, 1);

        // assign_ttl = 0 expires immediately; the entry becomes a tombstone.
        let (expired, _) = r.sweep(Duration::ZERO, Duration::from_secs(600));
        assert_eq!(expired, 1);
        let (_, assigns, tombs) = r.gauges();
        assert_eq!((assigns, tombs), (0, 1));
        assert!(!r.push(ch(7, 1, 2)), "late chunk is dropped, not re-claimed");
    }

    #[test]
    fn first_chunk_write_failure_is_salvaged() {
        let r = mk(8);
        let a = r.register_consumer();
        let b = r.register_consumer();
        assert!(r.push(ch(3, 0, 2)));
        let f = r.pop_for(a); // A claims msg 3 — but the write will fail
        assert_eq!(f, ch(3, 0, 2));

        assert!(r.fail_delivery(a, f), "never-ACKed first chunk is requeued");
        assert_eq!(r.gauges(), (1, 0, 0)); // back in queue; claim rescinded

        assert_eq!(r.pop_for(b), ch(3, 0, 2)); // B re-claims from scratch
        assert!(r.push(ch(3, 1, 2)), "message is alive, not tombstoned");
    }

    #[test]
    fn later_chunk_write_failure_dooms_message() {
        let r = mk(8);
        let a = r.register_consumer();
        for f in [ch(4, 0, 3), ch(4, 1, 3)] {
            assert!(r.push(f));
        }
        assert_eq!(r.pop_for(a), ch(4, 0, 3)); // delivered + ACKed (simulated)
        let f = r.pop_for(a);                  // this one's write fails
        assert_eq!(f, ch(4, 1, 3));

        assert!(!r.fail_delivery(a, f), "chunk 0 died with the consumer");
        assert_eq!(r.gauges(), (0, 0, 1));
        assert!(!r.push(ch(4, 2, 3)), "stragglers are refused");
    }

    #[test]
    fn salvage_then_unregister_loses_nothing() {
        // The "zombie handler" scenario: a disconnected consumer's handler
        // (parked in pop_for; disconnects are detected lazily) claims a
        // fresh message's first chunk, the write fails, and a chunk that
        // was redirected to it meanwhile must survive its unregistration.
        let r = mk(8);
        let a = r.register_consumer();
        let b = r.register_consumer();
        for f in [ch(6, 0, 2), ch(6, 1, 2), Frame::Msg(b"x".to_vec())] {
            assert!(r.push(f));
        }

        let first = r.pop_for(a);                            // A claims msg 6
        assert_eq!(first, ch(6, 0, 2));
        assert_eq!(r.pop_for(b), Frame::Msg(b"x".to_vec())); // B parks 6/1 at A

        assert!(r.fail_delivery(a, first)); // claim rescinded, chunk 0 requeued
        r.unregister_consumer(a);           // 6/1 must be requeued, not dropped

        assert_eq!(r.pop_for(b), ch(6, 0, 2)); // B takes the whole message
        assert_eq!(r.pop_for(b), ch(6, 1, 2));
        assert_eq!(r.gauges(), (0, 0, 0));     // completed; no tombstones
    }
}
