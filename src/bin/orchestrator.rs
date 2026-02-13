use std::collections::VecDeque;
use std::env;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use rand::{rngs::SysRng, TryRng};

use qpipe::{read_frame, write_frame, ROLE_CONSUMER, ROLE_PRODUCER, TOKEN_LEN};

struct SharedQueue {
    inner: Mutex<VecDeque<Vec<u8>>>,
    not_empty: Condvar,
    not_full: Condvar,
    capacity: usize,
}

impl SharedQueue {
    fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::new()),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
            capacity,
        }
    }

    fn push(&self, msg: Vec<u8>) {
        let mut q = self.inner.lock().unwrap();
        while q.len() >= self.capacity {
            q = self.not_full.wait(q).unwrap();
        }
        q.push_back(msg);
        self.not_empty.notify_one();
    }

    fn pop(&self) -> Vec<u8> {
        let mut q = self.inner.lock().unwrap();
        while q.is_empty() {
            q = self.not_empty.wait(q).unwrap();
        }
        let msg = q.pop_front().unwrap();
        self.not_full.notify_one();
        msg
    }
}

fn main() -> io::Result<()> {
    let listen_addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "0.0.0.0:7000".to_string());
    let capacity: usize = env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);

    let queue = Arc::new(SharedQueue::new(capacity));
    let listener = TcpListener::bind(&listen_addr)?;
    println!(
        "orchestrator control listening on {} (queue capacity {})",
        listener.local_addr()?,
        capacity
    );

    // One accept loop; one thread per client session.
    for conn in listener.incoming() {
        match conn {
            Ok(stream) => {
                let queue = queue.clone();
                thread::spawn(move || {
                    if let Err(e) = handle_control(stream, queue) {
                        eprintln!("session error: {e}");
                    }
                });
            }
            Err(e) => eprintln!("control accept error: {e}"),
        }
    }

    Ok(())
}

fn handle_control(mut ctrl: TcpStream, queue: Arc<SharedQueue>) -> io::Result<()> {
    ctrl.set_nodelay(true).ok();

    // Read role byte.
    let mut role = [0u8; 1];
    ctrl.read_exact(&mut role)?;
    let role = role[0];

    if role != ROLE_PRODUCER && role != ROLE_CONSUMER {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "unknown role byte"));
    }

    // Bind ephemeral port on same IP family as the control socket.
    let bind_ip = ctrl.local_addr()?.ip();
    let data_listener = TcpListener::bind(SocketAddr::new(bind_ip, 0))?;
    let port = data_listener.local_addr()?.port();

    // Session token to prevent accidental/hijacked connects to the ephemeral port.
    let mut token = [0u8; TOKEN_LEN];
    //OsRng.fill_bytes(&mut token);
    SysRng.try_fill_bytes(&mut token)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

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
                eprintln!("client {peer} authenticated on ephemeral port {port}");
                break s;
            }
            _ => {
                // Wrong token or timeout — close and keep waiting.
                continue;
            }
        }
    };

    // Now this thread is the "one thread per client" worker.
    if role == ROLE_PRODUCER {
        run_producer(&mut data, queue)
    } else {
        run_consumer(&mut data, queue)
    }
}

fn run_producer(stream: &mut TcpStream, queue: Arc<SharedQueue>) -> io::Result<()> {
    loop {
        match read_frame(stream)? {
            Some(msg) => queue.push(msg),
            None => return Ok(()), // producer disconnected
        }
    }
}

fn run_consumer(stream: &mut TcpStream, queue: Arc<SharedQueue>) -> io::Result<()> {
    loop {
        let msg = queue.pop();
        if let Err(e) = write_frame(stream, &msg) {
            // At-most-once semantics: if consumer dies mid-send, message is dropped.
            if matches!(
                e.kind(),
                io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset | io::ErrorKind::UnexpectedEof
            ) {
                return Ok(());
            }
            return Err(e);
        }
        stream.flush().ok();
    }
}
