// SPDX-License-Identifier: AGPL-3.0-or-later
//! Minimal networked MPMC "work queue" channel:
//! - Many producers send binary frames to an orchestrator.
//! - Many consumers receive frames; each frame is delivered to exactly one consumer.
//!
//! Transport: TCP.
//! Framing: big-endian u32 length prefix + raw bytes.
//! Session: connect to control port, send role byte, receive (ephemeral_port, token),
//!         then connect to ephemeral_port and send token.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::thread;
use std::time::{Duration, Instant};

pub const ROLE_PRODUCER: u8    = b'P';
pub const ROLE_CONSUMER: u8    = b'C';
pub const ROLE_HEALTHCHECK: u8 = b'H';
pub const ROLE_DRAIN: u8       = b'D';
pub const ROLE_SHUTDOWN: u8    = b'S';
pub const ACK_PAYLOAD: u8      = b'A';
pub const ACK_HEALTH: u8       = b'H';
pub const ACK_SHUTDOWN: u8     = b'S';
pub const ACK_DRAIN: u8        = b'D';

pub const TOKEN_LEN: usize = 16;
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024; // 16 MiB

/// Single-shot health probe. Opens a control connection, sends the
/// healthcheck role byte, and waits for the orchestrator's ack. Succeeds
/// only when the orchestrator is alive and processing role bytes — not
/// just that the TCP listener accepted the socket.
pub fn healthcheck(orchestrator: &str) -> io::Result<()> {
    let addr = resolve_first(orchestrator)?;
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(5))?;
    s.set_nodelay(true).ok();
    s.set_read_timeout(Some(Duration::from_secs(5))).ok();
    s.set_write_timeout(Some(Duration::from_secs(5))).ok();

    s.write_all(&[ROLE_HEALTHCHECK])?;
    s.flush()?;

    let mut ack = [0u8; 1];
    s.read_exact(&mut ack)?;
    if ack[0] != ACK_HEALTH {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected healthcheck ack: 0x{:02x}", ack[0]),
        ));
    }
    Ok(())
}

/// Blocks until the orchestrator passes a healthcheck, or the optional
/// timeout elapses. Retries with capped exponential backoff (100ms → 2s).
/// Pass `None` to wait forever.
pub fn wait_until_healthy(
            orchestrator: &str,
            timeout: Option<Duration>,
        ) -> io::Result<()> {
    let start = Instant::now();
    let mut delay = Duration::from_millis(100);
    let max_delay = Duration::from_secs(2);

    loop {
        match healthcheck(orchestrator) {
            Ok(()) => return Ok(()),
            Err(e) => {
                if let Some(t) = timeout {
                    if start.elapsed() >= t {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            format!(
                                "orchestrator at {} not healthy within {:?}: {}",
                                orchestrator, t, e
                            ),
                        ));
                    }
                }
                thread::sleep(delay);
                delay = (delay * 2).min(max_delay);
            }
        }
    }
}

/// Request that an orchestrator enter the drain state. Returns `Ok(())`
/// when the orchestrator acknowledges. Unlike `request_shutdown`, drain
/// has no timeout on the server side — the orchestrator will wait for the
/// queue to empty and producers to detach no matter how long it takes.
/// A subsequent `request_shutdown` will override the drain and impose the
/// usual timeout.
pub fn request_drain(orchestrator: &str) -> io::Result<()> {
    let addr = resolve_first(orchestrator)?;
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(5))?;
    s.set_nodelay(true).ok();
    s.set_read_timeout(Some(Duration::from_secs(10))).ok();
    s.set_write_timeout(Some(Duration::from_secs(5))).ok();

    s.write_all(&[ROLE_DRAIN])?;
    s.flush()?;

    let mut ack = [0u8; 1];
    s.read_exact(&mut ack)?;
    if ack[0] != ACK_DRAIN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected drain ack: 0x{:02x}", ack[0]),
        ));
    }
    Ok(())
}

/// Request that an orchestrator shut down gracefully. Returns `Ok(())` when
/// the orchestrator acknowledged the request — *not* when shutdown is
/// complete. The orchestrator will drain its queue and exit on its own
/// schedule after acking.
///
/// Failure modes (any return `Err`):
///   - connect refused / timed out
///   - orchestrator closed the socket without acking
///   - ack byte was something other than `ACK_SHUTDOWN`
pub fn request_shutdown(orchestrator: &str) -> io::Result<()> {
    let addr = resolve_first(orchestrator)?;
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(5))?;
    s.set_nodelay(true).ok();
    s.set_read_timeout(Some(Duration::from_secs(10))).ok();
    s.set_write_timeout(Some(Duration::from_secs(5))).ok();

    s.write_all(&[ROLE_SHUTDOWN])?;
    s.flush()?;

    let mut ack = [0u8; 1];
    s.read_exact(&mut ack)?;
    if ack[0] != ACK_SHUTDOWN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected shutdown ack: 0x{:02x}", ack[0]),
        ));
    }
    Ok(())
}

fn resolve_first(addr: &str) -> io::Result<SocketAddr> {
    addr.to_socket_addrs()?
        .next()
        .ok_or_else(
            || io::Error::new(
                io::ErrorKind::InvalidInput, "could not resolve address"
            )
        )
}

fn read_port_token<R: Read>(r: &mut R) -> io::Result<(u16, [u8; TOKEN_LEN])> {
    let mut port_buf = [0u8; 2];
    r.read_exact(&mut port_buf)?;
    let port = u16::from_be_bytes(port_buf);

    let mut token = [0u8; TOKEN_LEN];
    r.read_exact(&mut token)?;
    Ok((port, token))
}

fn connect_data(
            orchestrator_ctrl: SocketAddr,
            port: u16,
            token: [u8; TOKEN_LEN]
        ) -> io::Result<TcpStream> {
    let data_addr = SocketAddr::new(orchestrator_ctrl.ip(), port);
    let mut s = TcpStream::connect(data_addr)?;
    s.set_nodelay(true).ok();

    // Authenticate immediately on the ephemeral port.
    s.write_all(&token)?;
    s.flush()?;
    Ok(s)
}

pub fn write_frame<S: Write + Read>(
            s: &mut S,
            payload: &[u8]
        ) -> io::Result<()> {
    if payload.len() > MAX_FRAME_SIZE {
        return Err(
            io::Error::new(io::ErrorKind::InvalidInput, "Frame too large")
        );
    }

    // Send frame data
    let len = payload.len() as u32;
    s.write_all(&len.to_be_bytes())?;
    s.write_all(payload)?;
    // Read acknowledgement
    let mut ack_byte = [0u8; 1];
    s.read_exact(&mut ack_byte)?; // Reads exactly 1 byte (should be b'A')
    if ack_byte[0] != ACK_PAYLOAD {
        return Err(
            io::Error::new(io::ErrorKind::InvalidData, "Invalid ACK bit")
        );
    }

    Ok(())
}

/// Returns `Ok(None)` on clean EOF (peer closed).
pub fn read_frame<S: Read + Write>(s: &mut S) -> io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match s.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }

    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_SIZE {
        return Err(
            io::Error::new(
                io::ErrorKind::InvalidData, "incoming frame too large"
            )
        );
    }

    // Read data
    let mut payload = vec![0u8; len];
    s.read_exact(&mut payload)?;
    // Send acknowledgement
    s.write_all(&[ACK_PAYLOAD])?;

    Ok(Some(payload))
}

pub struct Producer {
    stream: TcpStream,
}

impl Producer {
    pub fn connect(orchestrator: &str) -> io::Result<Self> {
        let orchestrator_ctrl = resolve_first(orchestrator)?;
        let mut ctrl = TcpStream::connect(orchestrator_ctrl)?;
        ctrl.set_nodelay(true).ok();

        ctrl.write_all(&[ROLE_PRODUCER])?;
        ctrl.flush()?;

        let (port, token) = read_port_token(&mut ctrl)?;
        drop(ctrl);

        let stream = connect_data(orchestrator_ctrl, port, token)?;
        Ok(Self { stream })
    }

    pub fn send(&mut self, payload: &[u8]) -> io::Result<()> {
        write_frame(&mut self.stream, payload)?;
        self.stream.flush()?; // optional for TCP, but helps interactive demos
        Ok(())
    }
}

pub struct Consumer {
    stream: TcpStream,
}

impl Consumer {
    pub fn connect(orchestrator: &str) -> io::Result<Self> {
        let orchestrator_ctrl = resolve_first(orchestrator)?;
        let mut ctrl = TcpStream::connect(orchestrator_ctrl)?;
        ctrl.set_nodelay(true).ok();

        ctrl.write_all(&[ROLE_CONSUMER])?;
        ctrl.flush()?;

        let (port, token) = read_port_token(&mut ctrl)?;
        drop(ctrl);

        let stream = connect_data(orchestrator_ctrl, port, token)?;
        Ok(Self { stream })
    }

    /// Blocks until the next message arrives (or the orchestrator closes).
    pub fn recv(&mut self) -> io::Result<Vec<u8>> {
        match read_frame(&mut self.stream)? {
            Some(msg) => Ok(msg),
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "orchestrator closed consumer connection",
            )),
        }
    }
}
