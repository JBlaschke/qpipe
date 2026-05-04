// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Modes (mirror the consumer's framing options):
//   --lines    : one stdin line = one frame, raw UTF-8 bytes (default; the
//                original `producer` behavior; works for NDJSON input).
//   --base64   : one base64-encoded stdin line = one binary frame.
//   --msgpack  : read concatenated MessagePack values from stdin, send each
//                as one frame. Pairs with the consumer's
//                  consumer ADDR --raw | from msgpack --objects
//                for typed end-to-end Nushell pipelines.

use std::env;
use std::io::{self, BufRead};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use qpipe::Producer;
use rmpv::decode::{read_value, Error as DecodeError};
use rmpv::encode::write_value;

use log::info;

#[derive(Copy, Clone)]
enum Mode {
    Lines,
    Base64,
    Msgpack,
}

impl Mode {
    fn parse(s: &str) -> io::Result<Self> {
        match s {
            "--lines"   => Ok(Mode::Lines),
            "--base64"  => Ok(Mode::Base64),
            "--msgpack" => Ok(Mode::Msgpack),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "mode must be --lines, --base64, or --msgpack (got {s:?})"
                ),
            )),
        }
    }
}

fn main() -> io::Result<()> {
    // By default emit warnings
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("warn")
    ).init();

    let mut args = env::args().skip(1);
    let orchestrator = args
        .next()
        .unwrap_or_else(|| "127.0.0.1:7000".to_string());
    let mode = match args.next() {
        Some(s) => Mode::parse(&s)?,
        None    => Mode::Lines,
    };

    let mut p = Producer::connect(&orchestrator)?;
    info!("producer connected via {}", orchestrator);

    let stdin = io::stdin();
    let mut stdin = stdin.lock();

    match mode {
        Mode::Lines => {
            info!("reading lines from stdin; each line becomes one frame");
            let mut line = String::new();
            loop {
                line.clear();
                let n = stdin.read_line(&mut line)?;
                if n == 0 { break; }
                let payload = line.trim_end_matches(&['\n', '\r'][..]);
                p.send(payload.as_bytes())?;
            }
        }
        Mode::Base64 => {
            info!("reading base64 lines from stdin; each decodes to one frame");
            let mut line = String::new();
            loop {
                line.clear();
                let n = stdin.read_line(&mut line)?;
                if n == 0 { break; }
                let trimmed = line.trim();
                if trimmed.is_empty() { continue; }
                let bytes = STANDARD.decode(trimmed).map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
                })?;
                p.send(&bytes)?;
            }
        }
        Mode::Msgpack => {
            info!("reading msgpack values from stdin; each becomes one frame");
            let mut buf = Vec::with_capacity(4096);
            loop {
                match read_value(&mut stdin) {
                    Ok(val) => {
                        buf.clear();
                        write_value(&mut buf, &val).map_err(|e| {
                            io::Error::new(io::ErrorKind::InvalidData, e.to_string())
                        })?;
                        p.send(&buf)?;
                    }
                    // Clean EOF between values — nothing more to read.
                    Err(DecodeError::InvalidMarkerRead(e))
                        if e.kind() == io::ErrorKind::UnexpectedEof =>
                    {
                        break;
                    }
                    Err(e) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("msgpack decode error: {e}"),
                        ));
                    }
                }
            }
        }
    }

    Ok(())
}
