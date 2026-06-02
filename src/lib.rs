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

/// Fill `buf` completely from `s`, distinguishing a clean stream end from a
/// truncated one — which `Read::read_exact` cannot do, because it reports both
/// "EOF with 0 bytes read" and "EOF after a partial read" as the same
/// `UnexpectedEof`, and leaves the buffer contents unspecified.
///
/// Returns:
///   `Ok(true)`           — `buf` was filled completely.
///   `Ok(false)`          — clean EOF: the stream ended with ZERO bytes read
///                          into `buf` (i.e. exactly at a frame boundary).
///   `Err(UnexpectedEof)` — the stream ended PARTWAY through `buf` (truncation).
///   `Err(_)`             — any other I/O error (incl. read timeouts).
///
/// `Interrupted` is retried, matching `read_exact`'s own behavior; `WouldBlock`
/// / `TimedOut` are propagated (a stalled peer mid-prefix is an error, not a
/// reason to spin).
fn read_exact_or_eof<S: Read>(s: &mut S, buf: &mut [u8]) -> io::Result<bool> {
    let mut filled = 0;
    while filled < buf.len() {
        match s.read(&mut buf[filled..]) {
            Ok(0) => {
                return if filled == 0 {
                    Ok(false) // clean EOF at a boundary: no more frames
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "connection closed mid-frame (truncated length prefix)",
                    ))
                };
            }
            Ok(n) => filled += n,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {} // retry
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}

/// Returns `Ok(None)` only on a *clean* EOF at a frame boundary. A truncated
/// length prefix or a short payload read is now a hard error, not a silent None.
pub fn read_frame<S: Read + Write>(s: &mut S) -> io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    // Clean EOF before any prefix byte => no more frames. A *partial* prefix is
    // truncation, surfaced as Err by the helper (and propagated by `?`).
    if !read_exact_or_eof(s, &mut len_buf)? {
        return Ok(None);
    }

    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "incoming frame too large",
        ));
    }

    // Payload truncation is ALWAYS an error: once we've read a valid length we're
    // committed to a frame, so a short read means the stream broke. read_exact's
    // UnexpectedEof is exactly the right signal here — let it propagate via `?`.
    let mut payload = vec![0u8; len];
    s.read_exact(&mut payload)?;

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

// Unit tests for the framing layer. Because write_frame/read_frame are both
// bounded `Read + Write` (the ACK round-trip is part of the framing layer),
// we can't drive them with a plain Cursor — we need one object that is BOTH
// readable and writable. `DuplexMock` is that: an in-memory full-duplex pipe
// with two independent byte buffers.
//
// NOTE: adjust the ACK constant if the crate names it differently. The README
// documents ACK = 'A' (0x41); if lib.rs exposes a const (e.g. `ACK_BYTE`),
// prefer referencing that over the literal here.

#[cfg(test)]
mod frame_tests {
    use super::*;
    use std::io::{self, Read, Write};

    const ACK: u8 = ACK_PAYLOAD; // reference the crate's constant, not a literal

    /// In-memory full-duplex stream.
    /// - `read_buf`  : bytes the "peer" has sent to us, that our code will read.
    /// - `write_buf` : bytes our code writes out, that we can inspect afterward.
    struct DuplexMock {
        read_buf: io::Cursor<Vec<u8>>,
        write_buf: Vec<u8>,
    }

    impl DuplexMock {
        /// Start with `incoming` queued on the read side.
        fn with_incoming(incoming: Vec<u8>) -> Self {
            Self { read_buf: io::Cursor::new(incoming), write_buf: Vec::new() }
        }

        /// Queue exactly one ACK byte for write_frame's round-trip to consume.
        fn ready_for_one_ack() -> Self {
            Self::with_incoming(vec![ACK])
        }

        /// What our code wrote out.
        fn written(&self) -> &[u8] {
            &self.write_buf
        }
    }

    impl Read for DuplexMock {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.read_buf.read(buf)
        }
    }

    impl Write for DuplexMock {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.write_buf.write(buf)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    // ---- write_frame ----

    #[test]
    fn write_frame_emits_length_prefix_and_payload() {
        let mut io = DuplexMock::ready_for_one_ack();
        write_frame(&mut io, b"hello").unwrap();

        let out = io.written();
        // [u32 BE length][payload]; ACK flows the other way (into read_buf).
        assert_eq!(&out[0..4], &5u32.to_be_bytes());
        assert_eq!(&out[4..9], b"hello");
    }

    #[test]
    fn write_frame_empty_payload() {
        let mut io = DuplexMock::ready_for_one_ack();
        write_frame(&mut io, b"").unwrap();
        assert_eq!(io.written(), &0u32.to_be_bytes());
    }

    #[test]
    fn write_frame_rejects_oversize() {
        // One byte over MAX_FRAME_SIZE must be refused on the send path.
        // If MAX_FRAME_SIZE isn't pub, hardcode 16 * 1024 * 1024.
        let too_big = vec![0u8; MAX_FRAME_SIZE + 1];
        let mut io = DuplexMock::ready_for_one_ack();
        let res = write_frame(&mut io, &too_big);
        assert!(res.is_err(), "oversize frame should be rejected");
    }

    // ---- read_frame ----

    /// Build a valid on-the-wire frame body (length prefix + payload), i.e.
    /// what a reader would find waiting on its read side.
    fn framed(payload: &[u8]) -> Vec<u8> {
        let mut v = Vec::with_capacity(4 + payload.len());
        v.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        v.extend_from_slice(payload);
        v
    }

    #[test]
    fn read_frame_reads_one_payload() {
        let mut io = DuplexMock::with_incoming(framed(b"world"));
        let got = read_frame(&mut io).unwrap();
        assert_eq!(got, Some(b"world".to_vec()));
        // read_frame should have sent an ACK back out.
        assert_eq!(io.written(), &[ACK]);
    }

    #[test]
    fn read_frame_empty_payload() {
        let mut io = DuplexMock::with_incoming(framed(b""));
        assert_eq!(read_frame(&mut io).unwrap(), Some(Vec::new()));
    }

    #[test]
    fn read_frame_clean_eof_is_none() {
        // Nothing queued: a clean EOF before any length prefix => Ok(None),
        // NOT an error. This is the signal the data loop uses to stop.
        let mut io = DuplexMock::with_incoming(Vec::new());
        assert_eq!(read_frame(&mut io).unwrap(), None);
    }

    #[test]
    fn read_frame_partial_length_prefix_errors() {
        // With read_exact_or_eof, a partial prefix (EOF after 1–3 of 4 bytes) is a
        // truncation error — distinct from a clean boundary EOF (0 bytes -> Ok(None)).
        let mut io = DuplexMock::with_incoming(vec![0x00]);
        assert!(read_frame(&mut io).is_err());
    }

    #[test]
    fn read_frame_truncated_payload_errors() {
        // Claims 10 bytes, supplies 3.
        let mut wire = 10u32.to_be_bytes().to_vec();
        wire.extend_from_slice(b"abc");
        let mut io = DuplexMock::with_incoming(wire);
        assert!(read_frame(&mut io).is_err());
    }

    #[test]
    fn read_frame_rejects_oversize_length() {
        // A length prefix claiming > MAX_FRAME_SIZE must be rejected before
        // any attempt to allocate/read that many bytes.
        let oversize_len = (MAX_FRAME_SIZE as u32) + 1;
        let mut io = DuplexMock::with_incoming(oversize_len.to_be_bytes().to_vec());
        assert!(read_frame(&mut io).is_err());
    }

    // ---- round-trip ----

    #[test]
    fn write_then_read_roundtrip() {
        // Write a frame, capture the bytes, feed them into a fresh reader.
        let payload = b"round-trip payload";
        let mut writer = DuplexMock::ready_for_one_ack();
        write_frame(&mut writer, payload).unwrap();

        // The reader sees the length-prefixed body (strip nothing; the ACK was
        // on the writer's *read* side, not in what it wrote out).
        let mut reader = DuplexMock::with_incoming(writer.written().to_vec());
        assert_eq!(read_frame(&mut reader).unwrap(), Some(payload.to_vec()));
    }
}

// ---------------------------------------------------------------------------
// Property-based tests. Requires `proptest` as a dev-dependency:
//   [dev-dependencies]
//   proptest = "1"
// (Remember: add the dep, then `cargo vendor vendor` and commit, or it won't
//  resolve under the vendored source.)
#[cfg(test)]
mod frame_proptests {
    use super::*;
    use proptest::prelude::*;
    use std::io::{self, Read, Write};

    const ACK: u8 = ACK_PAYLOAD;

    struct DuplexMock {
        read_buf: io::Cursor<Vec<u8>>,
        write_buf: Vec<u8>,
    }
    impl Read for DuplexMock {
        fn read(&mut self, b: &mut [u8]) -> io::Result<usize> { self.read_buf.read(b) }
    }
    impl Write for DuplexMock {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> { self.write_buf.write(b) }
        fn flush(&mut self) -> io::Result<()> { Ok(()) }
    }

    proptest! {
        // Any payload within the size cap must survive write -> read unchanged.
        #[test]
        fn arbitrary_payload_roundtrips(payload in proptest::collection::vec(any::<u8>(), 0..4096)) {
            let mut writer = DuplexMock { read_buf: io::Cursor::new(vec![ACK]), write_buf: Vec::new() };
            write_frame(&mut writer, &payload).unwrap();

            let mut reader = DuplexMock { read_buf: io::Cursor::new(writer.write_buf.clone()), write_buf: Vec::new() };
            let got = read_frame(&mut reader).unwrap();
            prop_assert_eq!(got, Some(payload));
        }
    }
}
