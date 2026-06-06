// SPDX-License-Identifier: AGPL-3.0-or-later
//! Minimal networked MPMC "work queue" channel:
//! - Many producers send binary messages to an orchestrator.
//! - Many consumers receive messages; each message is delivered to exactly
//!   one consumer.
//!
//! Transport: TCP.
//! Framing: big-endian u32 length prefix + raw bytes. Bit 31 of the prefix
//! is the CHUNK flag:
//!   - flag clear: the frame body is one complete message — wire-identical
//!     to the original single-frame protocol.
//!   - flag set:   the frame body is one chunk of a multi-frame message:
//!       [u128 msg_id BE][u32 chunk idx BE][u32 chunk count BE][chunk bytes]
//! Every frame (single or chunk) is acknowledged with one ACK_PAYLOAD byte.
//! Session: connect to control port, send role byte, receive
//! (ephemeral_port, token), then connect to ephemeral_port and send token.
//!
//! Multi-frame messages: `Producer::send` transparently chunks payloads
//! larger than MAX_FRAME_SIZE; smaller payloads use the original
//! single-frame path unchanged. The orchestrator routes every chunk of a
//! message to whichever consumer claimed its first chunk; chunks may reach
//! that consumer out of order. `Consumer::recv` reassembles internally and
//! returns complete messages in COMPLETION order. Partials can be orphaned
//! if a producer dies mid-message — call `Consumer::gc_partials`
//! periodically to discard them.

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::thread;
use std::time::{Duration, Instant};

use rand::{rngs::SysRng, TryRng};

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

/// Bit 31 of the length prefix. MAX_FRAME_SIZE needs only 25 bits, so the
/// flag can never collide with a valid single-frame length. An old reader
/// that receives a chunk frame sees an absurd length and fails loudly
/// ("incoming frame too large") rather than silently mis-framing.
pub const FRAME_FLAG_CHUNK: u32 = 0x8000_0000;

/// Chunk extension header: u128 message id + u32 idx + u32 count.
pub const CHUNK_HEADER_LEN: usize = 16 + 4 + 4;

/// Per-chunk payload capacity (the ext header counts against the frame cap).
pub const MAX_CHUNK_PAYLOAD: usize = MAX_FRAME_SIZE - CHUNK_HEADER_LEN;

/// Upper bound on chunks per message; with MAX_CHUNK_PAYLOAD this caps a
/// reassembled message at ~64 GiB. Tune both consts to taste. The
/// reassembler never pre-allocates from the declared count, so a hostile
/// header cannot force a large allocation — memory grows only with bytes
/// actually received.
pub const MAX_CHUNKS: u32 = 4096;
pub const MAX_MESSAGE_SIZE: usize = MAX_CHUNK_PAYLOAD * (MAX_CHUNKS as usize);

/// One frame on the wire, post-parse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// A complete single-frame message (the original protocol).
    Msg(Vec<u8>),
    /// One chunk of a multi-frame message.
    Chunk { id: u128, idx: u32, count: u32, payload: Vec<u8> },
}

impl Frame {
    pub fn payload_len(&self) -> usize {
        match self {
            Frame::Msg(p) => p.len(),
            Frame::Chunk { payload, .. } => payload.len(),
        }
    }
}

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

/// 16 random bytes — globally unique without coordination between producers
/// (collision odds are birthday-bound at ~2^64 messages). Same RNG pattern
/// the orchestrator already uses for session tokens.
fn new_msg_id() -> io::Result<u128> {
    let mut b = [0u8; 16];
    SysRng
        .try_fill_bytes(&mut b)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    Ok(u128::from_be_bytes(b))
}

/// Write one complete single-frame message. Wire format and behavior are
/// unchanged from the original protocol: `[u32 BE len][payload]`, then wait
/// for one ACK byte from the peer.
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

/// Write one chunk of a multi-frame message:
/// `[u32 BE FLAG|body_len][u128 id][u32 idx][u32 count][payload]`, then wait
/// for one ACK byte — the same per-frame flow control as single frames.
pub fn write_chunk_frame<S: Write + Read>(
            s: &mut S,
            id: u128,
            idx: u32,
            count: u32,
            payload: &[u8],
        ) -> io::Result<()> {
    if payload.len() > MAX_CHUNK_PAYLOAD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput, "chunk payload too large",
        ));
    }
    if count == 0 || idx >= count {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput, "invalid chunk header (idx/count)",
        ));
    }
    if count > MAX_CHUNKS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput, "chunk count exceeds MAX_CHUNKS",
        ));
    }

    let body_len = (CHUNK_HEADER_LEN + payload.len()) as u32;
    s.write_all(&(body_len | FRAME_FLAG_CHUNK).to_be_bytes())?;
    s.write_all(&id.to_be_bytes())?;
    s.write_all(&idx.to_be_bytes())?;
    s.write_all(&count.to_be_bytes())?;
    s.write_all(payload)?;

    let mut ack_byte = [0u8; 1];
    s.read_exact(&mut ack_byte)?;
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

/// Read one frame — single OR chunk — acknowledging it with one ACK byte.
/// Returns `Ok(None)` only on a *clean* EOF at a frame boundary; truncation
/// anywhere (prefix, header, or payload) is a hard error.
pub fn read_frame_ext<S: Read + Write>(s: &mut S) -> io::Result<Option<Frame>> {
    let mut len_buf = [0u8; 4];
    // Clean EOF before any prefix byte => no more frames. A *partial* prefix
    // is truncation, surfaced as Err by the helper (and propagated by `?`).
    if !read_exact_or_eof(s, &mut len_buf)? {
        return Ok(None);
    }

    let raw = u32::from_be_bytes(len_buf);
    let is_chunk = raw & FRAME_FLAG_CHUNK != 0;
    let body_len = (raw & !FRAME_FLAG_CHUNK) as usize;
    if body_len > MAX_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "incoming frame too large",
        ));
    }

    if !is_chunk {
        // Original single-frame path, unchanged. Payload truncation is ALWAYS
        // an error: once we've read a valid length we're committed to a frame.
        let mut payload = vec![0u8; body_len];
        s.read_exact(&mut payload)?;
        s.write_all(&[ACK_PAYLOAD])?;
        return Ok(Some(Frame::Msg(payload)));
    }

    if body_len < CHUNK_HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk frame too short for extension header",
        ));
    }
    let mut hdr = [0u8; CHUNK_HEADER_LEN];
    s.read_exact(&mut hdr)?;
    let id    = u128::from_be_bytes(hdr[0..16].try_into().unwrap());
    let idx   = u32::from_be_bytes(hdr[16..20].try_into().unwrap());
    let count = u32::from_be_bytes(hdr[20..24].try_into().unwrap());

    // Validate before allocating the payload. A malformed header is a fatal
    // protocol error; the session is doomed, so stream desync is moot.
    if count == 0 || idx >= count {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData, "invalid chunk header (idx/count)",
        ));
    }
    if count > MAX_CHUNKS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData, "chunk count exceeds MAX_CHUNKS",
        ));
    }

    let mut payload = vec![0u8; body_len - CHUNK_HEADER_LEN];
    s.read_exact(&mut payload)?;
    s.write_all(&[ACK_PAYLOAD])?;
    Ok(Some(Frame::Chunk { id, idx, count, payload }))
}

/// Legacy single-frame reader, kept for compatibility. Identical behavior to
/// before for single frames (`Ok(None)` only on clean boundary EOF). If a
/// chunk frame arrives, that's an error — transports that may carry
/// multi-frame traffic should use `read_frame_ext`.
pub fn read_frame<S: Read + Write>(s: &mut S) -> io::Result<Option<Vec<u8>>> {
    match read_frame_ext(s)? {
        None => Ok(None),
        Some(Frame::Msg(p)) => Ok(Some(p)),
        Some(Frame::Chunk { .. }) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "received multi-frame chunk; use read_frame_ext",
        )),
    }
}

/// Reorders and reassembles chunks into complete messages. Used internally
/// by `Consumer`; public so custom consumers built on `read_frame_ext` can
/// reuse it.
#[derive(Debug, Default)]
pub struct Reassembler {
    partials: HashMap<u128, Partial>,
}

#[derive(Debug)]
struct Partial {
    count: u32,
    chunks: HashMap<u32, Vec<u8>>, // idx -> bytes; sparse, order-agnostic
    bytes: usize,
    last_update: Instant,
}

impl Reassembler {
    pub fn new() -> Self {
        Self { partials: HashMap::new() }
    }

    /// Absorb one chunk. Returns `Ok(Some(msg))` when this chunk completes a
    /// message, `Ok(None)` if the message is still partial. Protocol
    /// violations (bad idx/count, duplicate chunk, count mismatch within a
    /// message, oversize message) are `Err`.
    pub fn absorb(
                &mut self,
                id: u128,
                idx: u32,
                count: u32,
                payload: Vec<u8>,
            ) -> io::Result<Option<Vec<u8>>> {
        if count == 0 || idx >= count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData, "invalid chunk header (idx/count)",
            ));
        }
        if count > MAX_CHUNKS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData, "chunk count exceeds MAX_CHUNKS",
            ));
        }

        let mismatch = self.partials.get(&id).map_or(false, |p| p.count != count);
        if mismatch {
            self.partials.remove(&id);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chunk count mismatch within a message",
            ));
        }
        let dup = self.partials.get(&id)
            .map_or(false, |p| p.chunks.contains_key(&idx));
        if dup {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData, "duplicate chunk",
            ));
        }

        let now = Instant::now();
        let p = self.partials.entry(id).or_insert_with(|| Partial {
            count,
            chunks: HashMap::new(),
            bytes: 0,
            last_update: now,
        });
        p.bytes += payload.len();
        p.chunks.insert(idx, payload);
        p.last_update = now;
        let complete = p.chunks.len() as u32 == p.count;
        let oversize = p.bytes > MAX_MESSAGE_SIZE; // unreachable via valid
                                                   // frames; defense in depth

        if oversize {
            self.partials.remove(&id);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "reassembled message exceeds MAX_MESSAGE_SIZE",
            ));
        }
        if !complete {
            return Ok(None);
        }

        let p = self.partials.remove(&id).expect("entry exists");
        let mut out = Vec::with_capacity(p.bytes);
        for i in 0..p.count {
            out.extend_from_slice(p.chunks.get(&i).expect("all chunks present"));
        }
        Ok(Some(out))
    }

    /// Drop partial messages that haven't received a chunk for at least
    /// `idle_for`; returns how many were discarded. Orphans accumulate when
    /// a producer dies mid-message or the orchestrator expires a stale
    /// assignment, so call this on whatever cadence suits your latency
    /// expectations (e.g. every few hundred recvs, or when the app idles).
    pub fn gc(&mut self, idle_for: Duration) -> usize {
        let now = Instant::now();
        let before = self.partials.len();
        self.partials
            .retain(|_, p| now.duration_since(p.last_update) < idle_for);
        before - self.partials.len()
    }

    /// (number of partial messages, total bytes buffered across them).
    pub fn pending(&self) -> (usize, usize) {
        let bytes = self.partials.values().map(|p| p.bytes).sum();
        (self.partials.len(), bytes)
    }
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

    /// Send one message. Payloads up to MAX_FRAME_SIZE take the original
    /// single-frame path, unchanged. Larger payloads are transparently split
    /// into chunk frames under a fresh random message id; each chunk is
    /// individually ACKed, so backpressure behaves exactly like a stream of
    /// single frames.
    pub fn send(&mut self, payload: &[u8]) -> io::Result<()> {
        if payload.len() <= MAX_FRAME_SIZE {
            write_frame(&mut self.stream, payload)?;
            self.stream.flush()?; // optional for TCP, but helps interactive demos
            return Ok(());
        }

        if payload.len() > MAX_MESSAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput, "message exceeds MAX_MESSAGE_SIZE",
            ));
        }
        let count = payload.len().div_ceil(MAX_CHUNK_PAYLOAD); // >= 2 here,
                                                               // <= MAX_CHUNKS
        let id = new_msg_id()?;
        for (idx, chunk) in payload.chunks(MAX_CHUNK_PAYLOAD).enumerate() {
            write_chunk_frame(
                &mut self.stream, id, idx as u32, count as u32, chunk
            )?;
        }
        self.stream.flush()?;
        Ok(())
    }
}

pub struct Consumer {
    stream: TcpStream,
    asm: Reassembler,
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
        Ok(Self { stream, asm: Reassembler::new() })
    }

    /// Blocks until the next complete *message* arrives (or the orchestrator
    /// closes). Single-frame messages return as soon as their frame is read.
    /// Chunks are buffered internally and the call keeps reading frames —
    /// completing other messages first if they finish earlier — until some
    /// message completes. Messages are therefore returned in COMPLETION
    /// order, and a stalled multi-frame message never blocks other traffic.
    /// Every frame is ACKed as it is read, so orchestrator-side flow control
    /// is unaffected by reassembly.
    pub fn recv(&mut self) -> io::Result<Vec<u8>> {
        loop {
            let frame = read_frame_ext(&mut self.stream)?.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "orchestrator closed consumer connection",
                )
            })?;
            match frame {
                Frame::Msg(p) => return Ok(p),
                Frame::Chunk { id, idx, count, payload } => {
                    if let Some(msg) = self.asm.absorb(id, idx, count, payload)? {
                        return Ok(msg);
                    }
                }
            }
        }
    }

    /// Discard partial multi-frame messages that haven't received a chunk
    /// for at least `idle_for`. Returns how many messages were dropped.
    pub fn gc_partials(&mut self, idle_for: Duration) -> usize {
        self.asm.gc(idle_for)
    }

    /// (partial message count, bytes buffered) currently held for reassembly.
    pub fn pending_partials(&self) -> (usize, usize) {
        self.asm.pending()
    }
}

// Unit tests for the framing layer. Because the frame functions are bounded
// `Read + Write` (the ACK round-trip is part of the framing layer), we drive
// them with `DuplexMock`: an in-memory full-duplex pipe with two independent
// byte buffers.

#[cfg(test)]
mod frame_tests {
    use super::*;
    use std::io::{self, Read, Write};

    const ACK: u8 = ACK_PAYLOAD;

    /// In-memory full-duplex stream.
    /// - `read_buf`  : bytes the "peer" has sent to us, that our code will read.
    /// - `write_buf` : bytes our code writes out, that we can inspect afterward.
    struct DuplexMock {
        read_buf: io::Cursor<Vec<u8>>,
        write_buf: Vec<u8>,
    }

    impl DuplexMock {
        fn with_incoming(incoming: Vec<u8>) -> Self {
            Self { read_buf: io::Cursor::new(incoming), write_buf: Vec::new() }
        }

        /// Queue exactly `n` ACK bytes for write-side round-trips to consume.
        fn ready_for_acks(n: usize) -> Self {
            Self::with_incoming(vec![ACK; n])
        }

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

    // ---- write_frame (single) ----

    #[test]
    fn write_frame_emits_length_prefix_and_payload() {
        let mut io = DuplexMock::ready_for_acks(1);
        write_frame(&mut io, b"hello").unwrap();

        let out = io.written();
        assert_eq!(&out[0..4], &5u32.to_be_bytes());
        assert_eq!(&out[4..9], b"hello");
        assert_eq!(out[0] & 0x80, 0, "single frames must not set the chunk flag");
    }

    #[test]
    fn write_frame_empty_payload() {
        let mut io = DuplexMock::ready_for_acks(1);
        write_frame(&mut io, b"").unwrap();
        assert_eq!(io.written(), &0u32.to_be_bytes());
    }

    #[test]
    fn write_frame_rejects_oversize() {
        let too_big = vec![0u8; MAX_FRAME_SIZE + 1];
        let mut io = DuplexMock::ready_for_acks(1);
        assert!(write_frame(&mut io, &too_big).is_err());
    }

    // ---- read_frame (single) ----

    fn framed(payload: &[u8]) -> Vec<u8> {
        let mut v = Vec::with_capacity(4 + payload.len());
        v.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        v.extend_from_slice(payload);
        v
    }

    #[test]
    fn read_frame_reads_one_payload() {
        let mut io = DuplexMock::with_incoming(framed(b"world"));
        assert_eq!(read_frame(&mut io).unwrap(), Some(b"world".to_vec()));
        assert_eq!(io.written(), &[ACK]);
    }

    #[test]
    fn read_frame_empty_payload() {
        let mut io = DuplexMock::with_incoming(framed(b""));
        assert_eq!(read_frame(&mut io).unwrap(), Some(Vec::new()));
    }

    #[test]
    fn read_frame_clean_eof_is_none() {
        let mut io = DuplexMock::with_incoming(Vec::new());
        assert_eq!(read_frame(&mut io).unwrap(), None);
    }

    #[test]
    fn read_frame_partial_length_prefix_errors() {
        let mut io = DuplexMock::with_incoming(vec![0x00]);
        assert!(read_frame(&mut io).is_err());
    }

    #[test]
    fn read_frame_truncated_payload_errors() {
        let mut wire = 10u32.to_be_bytes().to_vec();
        wire.extend_from_slice(b"abc");
        let mut io = DuplexMock::with_incoming(wire);
        assert!(read_frame(&mut io).is_err());
    }

    #[test]
    fn read_frame_rejects_oversize_length() {
        let oversize_len = (MAX_FRAME_SIZE as u32) + 1;
        let mut io = DuplexMock::with_incoming(oversize_len.to_be_bytes().to_vec());
        assert!(read_frame(&mut io).is_err());
    }

    // ---- chunk frames ----

    const ID: u128 = 0x0011_2233_4455_6677_8899_AABB_CCDD_EEFF;

    #[test]
    fn chunk_frame_roundtrip_and_flag_bit() {
        let mut w = DuplexMock::ready_for_acks(1);
        write_chunk_frame(&mut w, ID, 1, 3, b"abc").unwrap();

        let wire = w.written();
        assert_ne!(wire[0] & 0x80, 0, "chunk flag must be set");
        let body_len =
            u32::from_be_bytes(wire[0..4].try_into().unwrap()) & !FRAME_FLAG_CHUNK;
        assert_eq!(body_len as usize, CHUNK_HEADER_LEN + 3);

        let mut r = DuplexMock::with_incoming(wire.to_vec());
        let f = read_frame_ext(&mut r).unwrap().unwrap();
        assert_eq!(
            f,
            Frame::Chunk { id: ID, idx: 1, count: 3, payload: b"abc".to_vec() }
        );
        assert_eq!(r.written(), &[ACK]);
    }

    #[test]
    fn read_frame_ext_reads_singles_too() {
        let mut io = DuplexMock::with_incoming(framed(b"plain"));
        assert_eq!(
            read_frame_ext(&mut io).unwrap(),
            Some(Frame::Msg(b"plain".to_vec()))
        );
    }

    #[test]
    fn legacy_read_frame_rejects_chunks() {
        let mut w = DuplexMock::ready_for_acks(1);
        write_chunk_frame(&mut w, ID, 0, 2, b"x").unwrap();
        let mut r = DuplexMock::with_incoming(w.written().to_vec());
        assert!(read_frame(&mut r).is_err());
    }

    #[test]
    fn write_chunk_frame_rejects_bad_headers() {
        let mut io = DuplexMock::ready_for_acks(1);
        assert!(write_chunk_frame(&mut io, ID, 2, 2, b"x").is_err()); // idx >= count
        assert!(write_chunk_frame(&mut io, ID, 0, 0, b"x").is_err()); // count == 0
        assert!(
            write_chunk_frame(&mut io, ID, 0, MAX_CHUNKS + 1, b"x").is_err()
        );
    }

    #[test]
    fn read_frame_ext_rejects_bad_chunk_header() {
        // Hand-craft a chunk frame with idx == count (invalid).
        let mut wire =
            ((CHUNK_HEADER_LEN as u32) | FRAME_FLAG_CHUNK).to_be_bytes().to_vec();
        wire.extend_from_slice(&ID.to_be_bytes());
        wire.extend_from_slice(&2u32.to_be_bytes()); // idx
        wire.extend_from_slice(&2u32.to_be_bytes()); // count
        let mut io = DuplexMock::with_incoming(wire);
        assert!(read_frame_ext(&mut io).is_err());
    }

    // ---- round-trip ----

    #[test]
    fn write_then_read_roundtrip() {
        let payload = b"round-trip payload";
        let mut writer = DuplexMock::ready_for_acks(1);
        write_frame(&mut writer, payload).unwrap();

        let mut reader = DuplexMock::with_incoming(writer.written().to_vec());
        assert_eq!(read_frame(&mut reader).unwrap(), Some(payload.to_vec()));
    }
}

#[cfg(test)]
mod reassembly_tests {
    use super::*;

    #[test]
    fn out_of_order_chunks_complete() {
        let mut asm = Reassembler::new();
        assert_eq!(asm.absorb(7, 2, 3, b"cc".to_vec()).unwrap(), None);
        assert_eq!(asm.absorb(7, 0, 3, b"aa".to_vec()).unwrap(), None);
        assert_eq!(
            asm.absorb(7, 1, 3, b"bb".to_vec()).unwrap(),
            Some(b"aabbcc".to_vec())
        );
        assert_eq!(asm.pending(), (0, 0));
    }

    #[test]
    fn interleaved_messages_complete_independently() {
        let mut asm = Reassembler::new();
        assert_eq!(asm.absorb(1, 0, 2, b"A0".to_vec()).unwrap(), None);
        assert_eq!(asm.absorb(2, 1, 2, b"B1".to_vec()).unwrap(), None);
        assert_eq!(
            asm.absorb(2, 0, 2, b"B0".to_vec()).unwrap(),
            Some(b"B0B1".to_vec())
        );
        assert_eq!(
            asm.absorb(1, 1, 2, b"A1".to_vec()).unwrap(),
            Some(b"A0A1".to_vec())
        );
    }

    #[test]
    fn single_chunk_message_completes_immediately() {
        // Lenient read side: our producer never emits count == 1, but it's
        // well-defined if some other implementation does.
        let mut asm = Reassembler::new();
        assert_eq!(
            asm.absorb(3, 0, 1, b"solo".to_vec()).unwrap(),
            Some(b"solo".to_vec())
        );
    }

    #[test]
    fn duplicate_chunk_is_error() {
        let mut asm = Reassembler::new();
        asm.absorb(4, 0, 3, b"x".to_vec()).unwrap();
        assert!(asm.absorb(4, 0, 3, b"x".to_vec()).is_err());
    }

    #[test]
    fn count_mismatch_is_error() {
        let mut asm = Reassembler::new();
        asm.absorb(5, 0, 3, b"x".to_vec()).unwrap();
        assert!(asm.absorb(5, 1, 4, b"y".to_vec()).is_err());
        assert_eq!(asm.pending(), (0, 0), "corrupt partial is discarded");
    }

    #[test]
    fn gc_drops_stale_partials_and_keeps_fresh_ones() {
        let mut asm = Reassembler::new();
        asm.absorb(6, 0, 2, b"half".to_vec()).unwrap();
        assert_eq!(asm.pending(), (1, 4));
        assert_eq!(asm.gc(Duration::from_secs(3600)), 0); // fresh: kept
        assert_eq!(asm.gc(Duration::ZERO), 1);            // any age: dropped
        assert_eq!(asm.pending(), (0, 0));
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

        // Any payload split into arbitrary-sized chunks, framed, read back,
        // and absorbed in REVERSE arrival order must reassemble exactly.
        #[test]
        fn chunked_roundtrip_reassembles(
            payload in proptest::collection::vec(any::<u8>(), 1..4096),
            chunk_size in 1usize..128,
        ) {
            let chunks: Vec<&[u8]> = payload.chunks(chunk_size).collect();
            let count = chunks.len() as u32;
            let id = 0xDEAD_BEEFu128;

            let mut writer = DuplexMock {
                read_buf: io::Cursor::new(vec![ACK; chunks.len()]),
                write_buf: Vec::new(),
            };
            for (i, c) in chunks.iter().enumerate() {
                write_chunk_frame(&mut writer, id, i as u32, count, c).unwrap();
            }

            let mut reader = DuplexMock {
                read_buf: io::Cursor::new(writer.write_buf.clone()),
                write_buf: Vec::new(),
            };
            let mut frames = Vec::new();
            while let Some(f) = read_frame_ext(&mut reader).unwrap() {
                frames.push(f);
            }
            frames.reverse(); // simulate out-of-order arrival

            let mut asm = Reassembler::new();
            let mut done = None;
            for f in frames {
                match f {
                    Frame::Chunk { id, idx, count, payload } => {
                        if let Some(msg) = asm.absorb(id, idx, count, payload).unwrap() {
                            prop_assert!(done.is_none());
                            done = Some(msg);
                        }
                    }
                    Frame::Msg(_) => prop_assert!(false, "expected chunk frame"),
                }
            }
            prop_assert_eq!(done, Some(payload));
        }
    }
}
