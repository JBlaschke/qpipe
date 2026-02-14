// SPDX-License-Identifier: AGPL-3.0-or-later
use std::env;
use std::io;

use qpipe::Consumer;

use log::info;

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

    let orchestrator = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:7000".to_string());

    let mut c = Consumer::connect(&orchestrator)?;
    info!("consumer connected via {}", orchestrator);

    loop {
        let msg = c.recv()?;
        if let Ok(s) = std::str::from_utf8(&msg) {
            info!(
                "msg ({} bytes) utf8: {}",
                msg.len(), s
            );
        } else {
            info!(
                "msg ({} bytes) hex: {}",
                msg.len(), hex_preview(&msg, 32)
            );
        }
    }
}
