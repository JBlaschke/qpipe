// SPDX-License-Identifier: AGPL-3.0-or-later
use std::collections::VecDeque;
use std::env;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use rand::{rngs::SysRng, TryRng};

use log::{info, warn};

use qpipe::{read_frame, write_frame, ROLE_CONSUMER, ROLE_PRODUCER, TOKEN_LEN};

#[derive(Default)]
struct Stats {
    // Messages accepted from producers and enqueued
    posted_msgs: AtomicU64,
    posted_bytes: AtomicU64,

    // Messages successfully written to consumer sockets
    collected_msgs: AtomicU64,
    collected_bytes: AtomicU64,

    // Messages popped but NOT delivered because consumer write failed
    dropped_msgs: AtomicU64,
    dropped_bytes: AtomicU64,

    // Connection counts
    active_producers: AtomicUsize,
    active_consumers: AtomicUsize,
}

enum ConnKind {
    Producer,
    Consumer,
}

struct ConnGuard {
    kind: ConnKind,
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
    inner: Mutex<VecDeque<Vec<u8>>>,
    not_empty: Condvar,
    not_full: Condvar,
    capacity: usize,

    // Exact queue depth maintained under the same lock used by push/pop.
    depth: AtomicUsize,
}

impl SharedQueue {
    fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::new()),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
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

fn main() -> io::Result<()> {
    // By default emit warnings
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("warn")
    ).init();

    let listen_addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "0.0.0.0:7000".to_string());
    let capacity: usize = env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);

    let queue = Arc::new(SharedQueue::new(capacity));
    let stats = Arc::new(Stats::default());

    // Reporter thread: emits a one-line summary every second.
    {
        let stats = stats.clone();
        let queue = queue.clone();
        thread::spawn(
            move || stats_reporter(stats, queue, Duration::from_secs(1))
        );
    }

    let listener = TcpListener::bind(&listen_addr)?;
    info!(
        "orchestrator control listening on {} (queue capacity {})",
        listener.local_addr()?,
        capacity
    );

    // One accept loop; one thread per client session.
    for conn in listener.incoming() {
        match conn {
            Ok(stream) => {
                let queue = queue.clone();
                let stats = stats.clone();
                thread::spawn(move || {
                    if let Err(e) = handle_control(stream, queue, stats) {
                        warn!("session error: {}", e);
                    }
                });
            }
            Err(e) => warn!("control accept error: {}", e),
        }
    }

    Ok(())
}

fn stats_reporter(
            stats: Arc<Stats>,
            queue: Arc<SharedQueue>,
            every: Duration
        ) {
    let mut last_posted_msgs = 0u64;
    let mut last_posted_bytes = 0u64;
    let mut last_collected_msgs = 0u64;
    let mut last_collected_bytes = 0u64;
    let mut last_dropped_msgs = 0u64;
    let mut last_dropped_bytes = 0u64;

    loop {
        thread::sleep(every);

        let posted_msgs = stats.posted_msgs.load(Ordering::Relaxed);
        let posted_bytes = stats.posted_bytes.load(Ordering::Relaxed);
        let collected_msgs = stats.collected_msgs.load(Ordering::Relaxed);
        let collected_bytes = stats.collected_bytes.load(Ordering::Relaxed);
        let dropped_msgs = stats.dropped_msgs.load(Ordering::Relaxed);
        let dropped_bytes = stats.dropped_bytes.load(Ordering::Relaxed);

        let dm_posted = posted_msgs - last_posted_msgs;
        let db_posted = posted_bytes - last_posted_bytes;
        let dm_collected = collected_msgs - last_collected_msgs;
        let db_collected = collected_bytes - last_collected_bytes;
        let dm_dropped = dropped_msgs - last_dropped_msgs;
        let db_dropped = dropped_bytes - last_dropped_bytes;

        last_posted_msgs = posted_msgs;
        last_posted_bytes = posted_bytes;
        last_collected_msgs = collected_msgs;
        last_collected_bytes = collected_bytes;
        last_dropped_msgs = dropped_msgs;
        last_dropped_bytes = dropped_bytes;

        let qd = queue.depth();
        let prod = stats.active_producers.load(Ordering::Relaxed);
        let cons = stats.active_consumers.load(Ordering::Relaxed);

        // One compact line per interval; goes to stderr.
        info!(
            "[stats] +{dm_posted} msg/s ({db_posted} B/s) posted | \
             +{dm_collected} msg/s ({db_collected} B/s) collected | \
             +{dm_dropped} msg/s ({db_dropped} B/s) dropped | \
             in_queue={qd} | producers={prod} consumers={cons} | totals: posted={posted_msgs} collected={collected_msgs} dropped={dropped_msgs}"
        );
    }
}

fn handle_control(
            mut ctrl: TcpStream,
            queue: Arc<SharedQueue>,
            stats: Arc<Stats>
        ) -> io::Result<()> {
    ctrl.set_nodelay(true).ok();

    // Read role byte.
    let mut role = [0u8; 1];
    ctrl.read_exact(&mut role)?;
    let role = role[0];

    if role != ROLE_PRODUCER && role != ROLE_CONSUMER {
        return Err(
            io::Error::new(io::ErrorKind::InvalidData, "unknown role byte")
        );
    }

    // Bind ephemeral port on same IP family as the control socket.
    let bind_ip = ctrl.local_addr()?.ip();
    let data_listener = TcpListener::bind(SocketAddr::new(bind_ip, 0))?;
    let port = data_listener.local_addr()?.port();

    // Session token to prevent accidental/hijacked connects to the ephemeral
    // port.
    let mut token = [0u8; TOKEN_LEN];
    //OsRng.fill_bytes(&mut token);
    SysRng.try_fill_bytes(&mut token).map_err(
        |e| io::Error::new(io::ErrorKind::Other, e)
    )?;

    // Reply to control session: [u16 port][TOKEN_LEN token]
    ctrl.write_all(&port.to_be_bytes())?;
    ctrl.write_all(&token)?;
    ctrl.flush()?;
    drop(ctrl);

    // Accept until a client presents the correct token.
    let mut data = loop {
        let (mut s, peer) = data_listener.accept()?;
        s.set_nodelay(true).ok();
        s.set_read_timeout(Some(Duration::from_secs(5))).ok();

        let mut got = [0u8; TOKEN_LEN];
        match s.read_exact(&mut got) {
            Ok(()) if got == token => {
                s.set_read_timeout(None).ok();
                info!(
                    "client {} authenticated on ephemeral port {}", peer, port
                );
                break s;
            }
            _ => continue,
        }
    };

    // One thread per client worker.
    if role == ROLE_PRODUCER {
        run_producer(&mut data, queue, stats)
    } else {
        run_consumer(&mut data, queue, stats)
    }
}

fn run_producer(
            stream: &mut TcpStream,
            queue: Arc<SharedQueue>,
            stats: Arc<Stats>
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
            None => return Ok(()), // producer disconnected
        }
    }
}

fn run_consumer(
            stream: &mut TcpStream,
            queue: Arc<SharedQueue>,
            stats: Arc<Stats>
        ) -> io::Result<()> {
    let _guard = ConnGuard::new(ConnKind::Consumer, stats.clone());

    loop {
        let msg = queue.pop();
        let len = msg.len() as u64;

        match write_frame(stream, &msg) {
            Ok(()) => {
                // “Collected” here means “successfully written to the consumer
                // socket.”
                stats.collected_msgs.fetch_add(1, Ordering::Relaxed);
                stats.collected_bytes.fetch_add(len, Ordering::Relaxed);
                stream.flush().ok();
            }
            Err(e) => {
                // At-most-once semantics: popped message is dropped if consumer
                // dies mid-send.
                stats.dropped_msgs.fetch_add(1, Ordering::Relaxed);
                stats.dropped_bytes.fetch_add(len, Ordering::Relaxed);

                if matches!(
                    e.kind(),
                    io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::UnexpectedEof
                ) {
                    return Ok(());
                }
                return Err(e);
            }
        }
    }
}
