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

pub const ROLE_PRODUCER: u8 = b'P';
pub const ROLE_CONSUMER: u8 = b'C';

pub const TOKEN_LEN: usize = 16;
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024; // 16 MiB

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

pub fn write_frame<W: Write>(w: &mut W, payload: &[u8]) -> io::Result<()> {
    if payload.len() > MAX_FRAME_SIZE {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "frame too large"));
    }
    let len = payload.len() as u32;
    w.write_all(&len.to_be_bytes())?;
    w.write_all(payload)?;
    Ok(())
}

/// Returns `Ok(None)` on clean EOF (peer closed).
pub fn read_frame<R: Read>(r: &mut R) -> io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf) {
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

    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload)?;
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
