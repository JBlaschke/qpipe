// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Black-box integration tests for the orchestrator / producer / consumer
// binaries. Requires dev-dependencies:
//   [dev-dependencies]
//   assert_cmd = "2"
//   predicates = "3"
// (add them, then re-vendor: `cargo vendor vendor` and commit.)
//
// The orchestrator loop lives in its binary (not the library), so we spawn it
// as a child process. Readiness + teardown use the crate's own public helpers
// (qpipe::wait_until_healthy / qpipe::request_shutdown) rather than hand-rolled
// polling or SIGKILL.

use assert_cmd::cargo::cargo_bin;
use assert_cmd::Command;
use predicates::prelude::*;
use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::process::{Child, Command as StdCommand};
use std::sync::mpsc;
use std::time::Duration;

/// Grab a free port by binding :0, then immediately releasing it. Small
/// TOCTOU window before the orchestrator rebinds, but reliable on loopback.
/// (No --port 0 / port-announce mechanism exists in the binary, so we must
/// pick the port ourselves and pass it as LISTEN_ADDR.)
fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// A spawned orchestrator that shuts down gracefully on Drop.
struct Orchestrator {
    addr: String,
    child: Option<Child>,
}

impl Orchestrator {
    fn start() -> Self {
        let addr = format!("127.0.0.1:{}", free_port());

        // First positional arg is LISTEN_ADDR (README: orchestrator [LISTEN_ADDR] [CAPACITY] [STATS]).
        let child = StdCommand::new(cargo_bin("orchestrator"))
            .arg(&addr)
            .env("RUST_LOG", "warn")
            .spawn()
            .expect("failed to spawn orchestrator binary");

        // Readiness via the crate's own healthcheck. wait_until_healthy has
        // its own internal backoff (100ms -> 2s); we just give it a total budget.
        qpipe::wait_until_healthy(&addr, Some(Duration::from_secs(5)))
            .expect("orchestrator never became healthy");

        Self { addr, child: Some(child) }
    }
}

impl Drop for Orchestrator {
    fn drop(&mut self) {
        // Graceful shutdown through the control protocol; fall back to kill.
        // NOTE: request_shutdown is ack-on-receipt, not ack-on-completion —
        // the orchestrator exits on its own schedule afterward. That's fine for
        // teardown (we don't depend on it for correctness), but it's why tests
        // must NOT use shutdown/drain as a delivery barrier. See read_n_lines.
        let _ = qpipe::request_shutdown(&self.addr);
        if let Some(mut child) = self.child.take() {
            // Give it a moment to exit cleanly, then force.
            std::thread::sleep(Duration::from_millis(100));
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Read exactly `n` lines from a child's piped stdout, failing (rather than
/// hanging) if they don't arrive within `timeout`. A reader thread pushes lines
/// through a channel; the main thread enforces the deadline.
///
/// This is the actual delivery barrier for the round-trip tests: the frames
/// themselves are the success condition, so we wait on *them*, not on any
/// control-protocol signal (drain/shutdown both ack on receipt, not on
/// completion, so neither guarantees the consumer has pulled everything).
fn read_n_lines(child: &mut Child, n: usize, timeout: Duration) -> Vec<String> {
    let stdout = child.stdout.take().expect("consumer stdout not piped");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            match line {
                Ok(l) => {
                    if tx.send(l).is_err() {
                        break; // receiver gone; stop reading
                    }
                }
                Err(_) => break, // pipe closed / EOF
            }
        }
    });

    let mut lines = Vec::with_capacity(n);
    let deadline = std::time::Instant::now() + timeout;
    while lines.len() < n {
        let remaining = deadline
            .checked_duration_since(std::time::Instant::now())
            .unwrap_or(Duration::ZERO);
        match rx.recv_timeout(remaining) {
            Ok(line) => lines.push(line),
            Err(_) => panic!(
                "timed out after {timeout:?} waiting for {n} lines; got {}: {lines:?}",
                lines.len()
            ),
        }
    }
    lines
}

// ---------------------------------------------------------------------------
// Argument / mode validation — no orchestrator needed. Cheapest tier.
// NOTE: these assert that *invalid* invocations fail. If qpipe uses a hand-
// rolled arg parser (the positional style suggests it might, not clap), the
// exact stderr text and exit codes may differ — adjust the predicates to match
// actual output. Run each once and read the real message.

#[test]
fn producer_rejects_unknown_mode() {
    // The binary reports an invalid mode via an InvalidInput io::Error whose
    // message contains "mode must be ...". (Confirm the producer validates the
    // mode BEFORE attempting to connect — if it connects first, this needs a
    // live orchestrator to reach the validation. See the privileged-port check.)
    Command::new(cargo_bin("producer"))
        .args(["127.0.0.1:1", "--bogus-mode"])
        .timeout(Duration::from_secs(5))
        .assert()
        .failure()
        .stderr(predicate::str::contains("mode must be"));
}

#[test]
fn consumer_rejects_unknown_mode() {
    // Consumer modes are --log/--jsonl/--base64/--raw; expect a similar message.
    // ADJUST the substring to the consumer's actual wording (run it once:
    // `cargo run --bin consumer -- 127.0.0.1:1 --bogus-mode`).
    Command::new(cargo_bin("consumer"))
        .args(["127.0.0.1:1", "--bogus-mode"])
        .timeout(Duration::from_secs(5))
        .assert()
        .failure()
        .stderr(predicate::str::contains("mode must be"));
}

#[test]
fn producer_with_no_orchestrator_fails_to_connect() {
    // Valid mode, dead address: should fail at connect. Asserts failure; the
    // exact connect-error wording is OS-dependent, so we don't pin the message.
    let dead = format!("127.0.0.1:{}", free_port());
    Command::new(cargo_bin("producer"))
        .args([dead.as_str(), "--lines"])
        .write_stdin("hello\n")
        .timeout(Duration::from_secs(5))
        .assert()
        .failure();
}

// ---------------------------------------------------------------------------
// End-to-end round-trips through a live orchestrator. Thinner tier — these
// exercise the framing contracts that actually cross the process boundary.

#[test]
fn lines_mode_roundtrip_via_jsonl_consumer() {
    let orch = Orchestrator::start();

    // Consumer in --jsonl: each frame becomes one stdout line.
    // Spawn it first so it's waiting in the queue when the producer sends.
    let mut consumer = StdCommand::new(cargo_bin("consumer"))
        .args([orch.addr.as_str(), "--jsonl"])
        .stdout(std::process::Stdio::piped())
        .env("RUST_LOG", "warn")
        .spawn()
        .expect("spawn consumer");

    // Producer in --lines: each stdin line -> one frame. Exits when stdin ends.
    Command::new(cargo_bin("producer"))
        .args([orch.addr.as_str(), "--lines"])
        .write_stdin("alpha\nbravo\ncharlie\n")
        .timeout(Duration::from_secs(10))
        .assert()
        .success();

    // Wait on the frames themselves: read exactly 3 lines or fail. No sleep,
    // no reliance on drain/shutdown timing — the lines arriving IS the barrier.
    let lines = read_n_lines(&mut consumer, 3, Duration::from_secs(10));
    assert!(lines.iter().any(|l| l.contains("alpha")), "missing alpha in {lines:?}");
    assert!(lines.iter().any(|l| l.contains("bravo")), "missing bravo in {lines:?}");
    assert!(lines.iter().any(|l| l.contains("charlie")), "missing charlie in {lines:?}");

    // Frames confirmed delivered; orch's Drop tears down the rest.
}

#[test]
fn base64_mode_roundtrips_binary() {
    use base64::{engine::general_purpose::STANDARD, Engine};

    let orch = Orchestrator::start();

    let mut consumer = StdCommand::new(cargo_bin("consumer"))
        .args([orch.addr.as_str(), "--base64"])
        .stdout(std::process::Stdio::piped())
        .env("RUST_LOG", "warn")
        .spawn()
        .expect("spawn consumer");

    // Binary payload with embedded NUL and high bytes — would corrupt under
    // a line-oriented mode, must survive base64.
    let raw = [0x00u8, 0xff, 0x10, b'\n', 0x42];
    let line = format!("{}\n", STANDARD.encode(raw));

    Command::new(cargo_bin("producer"))
        .args([orch.addr.as_str(), "--base64"])
        .write_stdin(line)
        .timeout(Duration::from_secs(10))
        .assert()
        .success();

    // Wait for exactly one line (the single frame's base64), then decode it.
    let lines = read_n_lines(&mut consumer, 1, Duration::from_secs(10));
    let decoded = STANDARD
        .decode(lines[0].trim().as_bytes())
        .expect("consumer emitted valid base64");
    assert_eq!(decoded, raw);
}
