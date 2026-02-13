// SPDX-License-Identifier: AGPL-3.0-or-later
use std::env;
use std::io::{self, BufRead};

use qpipe::Producer;

fn main() -> io::Result<()> {
    let orchestrator = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:7000".to_string());

    let mut p = Producer::connect(&orchestrator)?;
    eprintln!("producer connected via {orchestrator}");
    eprintln!("type lines; each line becomes one binary frame");

    for line in io::stdin().lock().lines() {
        let line = line?;
        p.send(line.as_bytes())?;
    }

    Ok(())
}
