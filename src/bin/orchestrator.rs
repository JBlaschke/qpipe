// SPDX-License-Identifier: AGPL-3.0-or-later
use std::collections::VecDeque;
use std::env;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::ExitCode;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use rand::{rngs::SysRng, TryRng};

use log::{debug, info, warn, error};

use qpipe::{
    read_frame, request_drain, request_shutdown, write_frame,
    ACK_DRAIN, ACK_HEALTH, ACK_SHUTDOWN,
    ROLE_CONSUMER, ROLE_DRAIN, ROLE_HEALTHCHECK, ROLE_PRODUCER, ROLE_SHUTDOWN,
    TOKEN_LEN,
};

// Orchestrator lifecycle state. The accept loop runs only while RUNNING;
// any other state stops new sessions and enters the drain phase.
const STATE_RUNNING:       u8 = 0;
const STATE_DRAINING:      u8 = 1; // wait forever for queue + producers
const STATE_SHUTTING_DOWN: u8 = 2; // wait with timeout, then exit anyway

// How long to wait for queue drain after a shutdown is requested before
// giving up and exiting anyway. Tunable via QPIPE_DRAIN_TIMEOUT_SECS.
const DEFAULT_DRAIN_TIMEOUT_SECS: u64 = 30;

#[derive(Default)]
struct Stats {
    // Messages accepted from producers and enqueued
    posted_msgs:      AtomicU64,
    posted_bytes:     AtomicU64,
    // Messages successfully written to consumer sockets
    collected_msgs:   AtomicU64,
    collected_bytes:  AtomicU64,
    // Messages popped but NOT delivered because consumer write failed
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

struct SharedQueue {
    inner:     Mutex<VecDeque<Vec<u8>>>,
    not_empty: Condvar,
    not_full:  Condvar,
    capacity:  usize,
    depth:     AtomicUsize,
}

impl SharedQueue {
    fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::new()),
            not_empty: Condvar::new(),
            not_full:  Condvar::new(),
            capacity,
            depth: AtomicUsize::new(0),
        }
    }

    fn push(&self, msg: Vec<u8>) {
        let mut q = self.inner.lock().unwrap();
        while q.len() >= self.capacity {
            q = self.not_full.wait(q).unwrap();
        }
        q.push_back(msg);
        self.depth.fetch_add(1, Ordering::Relaxed);
        self.not_empty.notify_one();
    }

    fn pop(&self) -> Vec<u8> {
        let mut q = self.inner.lock().unwrap();
        while q.is_empty() {
            q = self.not_empty.wait(q).unwrap();
        }
        let msg = q.pop_front().unwrap();
        self.depth.fetch_sub(1, Ordering::Relaxed);
        self.not_full.notify_one();
        msg
    }

    fn depth(&self) -> usize {
        self.depth.load(Ordering::Relaxed)
    }
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

    let queue = Arc::new(SharedQueue::new(capacity));
    let stats = Arc::new(Stats::default());
    let state = Arc::new(AtomicU8::new(STATE_RUNNING));

    // Reporter thread.
    {
        let stats = stats.clone();
        let queue = queue.clone();
        let state = state.clone();
        thread::spawn(
            move || stats_reporter(stats, queue, state, Duration::from_secs(sfreq))
        );
    }

    let listener = TcpListener::bind(&listen_addr)?;
    listener.set_nonblocking(true)?;

    info!(
        "Orchestrator control listening on {} (queue capacity {})",
        listener.local_addr()?, capacity
    );

    // ── Accept loop ─────────────────────────────────────────────────────────
    while state.load(Ordering::SeqCst) == STATE_RUNNING {
        match listener.accept() {
            Ok((stream, _peer)) => {
                debug!("Spawning handler thread");
                let queue = queue.clone();
                let stats = stats.clone();
                let state = state.clone();
                thread::spawn(move || {
                    if let Err(e) = handle_control(stream, queue, stats, state) {
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

    // ── Drain phase ─────────────────────────────────────────────────────────
    drain(queue, stats, state)
}

fn drain(
            queue: Arc<SharedQueue>,
            stats: Arc<Stats>,
            state: Arc<AtomicU8>,
        ) -> io::Result<()> {
    let initial_state = state.load(Ordering::SeqCst);
    let drain_timeout = env::var("QPIPE_DRAIN_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(DEFAULT_DRAIN_TIMEOUT_SECS));

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

    loop {
        let st        = state.load(Ordering::SeqCst);
        let depth     = queue.depth();
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
            stats: Arc<Stats>,
            queue: Arc<SharedQueue>,
            state: Arc<AtomicU8>,
            every: Duration,
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

        let qd   = queue.depth();
        let prod = stats.active_producers.load(Ordering::Relaxed);
        let cons = stats.active_consumers.load(Ordering::Relaxed);

        info!(
            "[stats] +{dm_posted} msgs ({db_posted} B) posted | \
             +{dm_collected} msgs ({db_collected} B) collected | \
             +{dm_dropped} msgs ({db_dropped} B) dropped | \
             in_queue={qd} | producers={prod} consumers={cons} | totals: posted={posted_msgs} collected={collected_msgs} dropped={dropped_msgs}"
        );
    }
}

fn handle_control(
            mut ctrl: TcpStream,
            queue:    Arc<SharedQueue>,
            stats:    Arc<Stats>,
            state:    Arc<AtomicU8>,
        ) -> io::Result<()> {
    ctrl.set_nodelay(true).ok();

    let mut role = [0u8; 1];
    ctrl.read_exact(&mut role)?;
    let role = role[0];

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
        let x = run_producer(&mut data, queue, stats);
        debug!("Stopping producer");
        x
    } else {
        debug!("Starting consumer");
        let x = run_consumer(&mut data, queue, stats);
        debug!("Stopping consumer");
        x
    }
}

fn run_producer(
            stream: &mut TcpStream,
            queue:  Arc<SharedQueue>,
            stats:  Arc<Stats>,
        ) -> io::Result<()> {
    let _guard = ConnGuard::new(ConnKind::Producer, stats.clone());

    loop {
        match read_frame(stream)? {
            Some(msg) => {
                let len = msg.len() as u64;
                queue.push(msg);
                stats.posted_msgs.fetch_add(1, Ordering::Relaxed);
                stats.posted_bytes.fetch_add(len, Ordering::Relaxed);
            }
            None => return Ok(()),
        }
    }
}

fn run_consumer(
            stream: &mut TcpStream,
            queue:  Arc<SharedQueue>,
            stats:  Arc<Stats>,
        ) -> io::Result<()> {
    let _guard = ConnGuard::new(ConnKind::Consumer, stats.clone());

    loop {
        let msg = queue.pop();
        let len = msg.len() as u64;

        match write_frame(stream, &msg) {
            Ok(()) => {
                stats.collected_msgs.fetch_add(1, Ordering::Relaxed);
                stats.collected_bytes.fetch_add(len, Ordering::Relaxed);
                stream.flush().ok();
            }
            Err(e) => {
                stats.dropped_msgs.fetch_add(1, Ordering::Relaxed);
                stats.dropped_bytes.fetch_add(len, Ordering::Relaxed);
                queue.push(msg);

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
