// SPDX-License-Identifier: AGPL-3.0-or-later
use std::env;
use std::io::{self, Write};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use qpipe::Consumer;

use log::info;

#[derive(Copy, Clone)]
enum Mode {
    /// Human-readable: log each message (utf-8 if valid, else hex preview)
    /// to the env_logger sink. This is the original `consumer` behavior.
    Log,
    /// One UTF-8 line per message on stdout (NDJSON-friendly).
    Jsonl,
    /// One base64-encoded line per message on stdout.
    Base64,
}

impl Mode {
    fn parse(s: &str) -> io::Result<Self> {
        match s {
            "--log"    => Ok(Mode::Log),
            "--jsonl"  => Ok(Mode::Jsonl),
            "--base64" => Ok(Mode::Base64),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("mode must be --log, --jsonl, or --base64 (got {s:?})"),
            )),
        }
    }
}

fn hex_preview(bytes: &[u8], max: usize) -> String {
    let mut out = String::new();
    for (i, b) in bytes.iter().take(max).enumerate() {
        if i > 0 {
            out.push(' ');
        }
        out.push_str(&format!("{:02x}", b));
    }
    if bytes.len() > max {
        out.push_str(" …");
    }
    out
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
        None    => Mode::Log,
    };

    let mut c = Consumer::connect(&orchestrator)?;
    info!("consumer connected via {}", orchestrator);

    let mut out = io::stdout().lock();

    loop {
        let msg = c.recv()?;

        match mode {
            Mode::Log => {
                if let Ok(s) = std::str::from_utf8(&msg) {
                    info!("msg ({} bytes) utf8: {}", msg.len(), s);
                } else {
                    info!(
                        "msg ({} bytes) hex: {}",
                        msg.len(), hex_preview(&msg, 32)
                    );
                }
            }
            Mode::Jsonl => {
                // Validate UTF-8 so Nu isn't fed broken text.
                let s = std::str::from_utf8(&msg).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "payload not valid UTF-8",
                    )
                })?;

                // Ensure exactly one line per message (NDJSON style). If your
                // producers might send pretty-printed JSON with newlines,
                // either compact it before sending, or switch to --base64.
                if s.contains('\n') {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "payload contains newline; not valid for --jsonl \
                         (use compact JSON or --base64)",
                    ));
                }

                out.write_all(s.as_bytes())?;
                out.write_all(b"\n")?;
                out.flush()?;
            }
            Mode::Base64 => {
                let line = STANDARD.encode(&msg);
                out.write_all(line.as_bytes())?;
                out.write_all(b"\n")?;
                out.flush()?;
            }
        }
    }
}
