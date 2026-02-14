// SPDX-License-Identifier: AGPL-3.0-or-later
use std::env;
use std::io::{self, Write};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use qpipe::Consumer;

fn main() -> io::Result<()> {
    let mut args = env::args().skip(1);
    let orchestrator = args
        .next()
        .unwrap_or_else(|| "127.0.0.1:7000".to_string());

    // Modes:
    //   --jsonl  : treat payload as UTF-8 JSON and print as a single line
    //   --base64 : print base64(payload) as a single line
    let mode = args.next().unwrap_or_else(|| "--base64".to_string());

    let mut c = Consumer::connect(&orchestrator)?;
    let mut out = io::stdout().lock();

    loop {
        let msg = c.recv()?;

        match mode.as_str() {
            "--jsonl" => {
                // Validate UTF-8 so Nu isn't fed broken text.
                let s = std::str::from_utf8(&msg)
                    .map_err(
                        |_| io::Error::new(
                            io::ErrorKind::InvalidData,
                            "payload not valid UTF-8"
                        )
                    )?;

                // Ensure exactly one line per message (NDJSON style). If your
                // producers might send pretty-printed JSON with newlines,
                // either compact it before sending, or switch to --base64.
                if s.contains('\n') {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "payload contains newline; not valid for --jsonl (use compact JSON or --base64)",
                    ));
                }

                out.write_all(s.as_bytes())?;
                out.write_all(b"\n")?;
            }
            "--base64" => {
                let line = STANDARD.encode(&msg);
                out.write_all(line.as_bytes())?;
                out.write_all(b"\n")?;
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "mode must be --jsonl or --base64",
                ));
            }
        }

        out.flush()?;
    }
}
