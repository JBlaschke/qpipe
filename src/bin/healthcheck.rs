// SPDX-License-Identifier: AGPL-3.0-or-later
use std::env;
use std::process::ExitCode;

use qpipe::wait_until_healthy;

fn main() -> ExitCode {
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("warn")
    ).init();

    let orchestrator = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:7000".to_string());

    match wait_until_healthy(&orchestrator, None) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("healthcheck failed: {}", e);
            ExitCode::FAILURE
        }
    }
}
