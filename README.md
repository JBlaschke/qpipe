# qpipe
Work Queue + MPMC Pipe -- even accross networks (for the brave!). Minimal
networked MPMC work queue over TCP. Producers push binary frames to a central
orchestrator; consumers pull frames out. Each frame is delivered to exactly one
consumer — work-queue semantics, not pub/sub.

```
┌──────────┐               ┌──────────────┐               ┌──────────┐
│ producer │ ──► frames ──►│              │──► frames ──► │ consumer │
└──────────┘               │ orchestrator │               └──────────┘
┌──────────┐               │  (bounded    │               ┌──────────┐
│ producer │ ──► frames ──►│   queue)     │──► frames ──► │ consumer │
└──────────┘               └──────────────┘               └──────────┘
```

## Quick start

```bash
cargo build --release
```

Run the three roles in three terminals:

```bash
# orchestrator
RUST_LOG=info cargo run --release --bin orchestrator

# consumer
cargo run --release --bin consumer 127.0.0.1:7000 --jsonl

# producer
echo '{"hello":"world"}' | cargo run --release --bin producer 127.0.0.1:7000
```

The consumer prints the JSON line on stdout. Drop `--jsonl` (or use
`RUST_LOG=info` with no mode) to get the human-readable log format instead.

## Binaries

### `orchestrator`

```
orchestrator [LISTEN_ADDR] [CAPACITY] [STATS_INTERVAL_SECS]
```

| Arg | Default | Description |
|---|---|---|
| `LISTEN_ADDR` | `0.0.0.0:7000` | Address for the control port |
| `CAPACITY` | `10000` | Max frames buffered in the queue (producers block when full) |
| `STATS_INTERVAL_SECS` | `1` | How often the stats line is emitted to stderr |

Set `RUST_LOG=info` to see the startup banner and the periodic stats line;
`RUST_LOG=debug` for per-connection trace.

### `producer`

```
producer [ORCHESTRATOR_ADDR] [MODE]
```

| Mode | Description |
|---|---|
| `--lines` *(default)* | One stdin line = one frame (raw UTF-8 bytes, trailing newline stripped). Works for plain text and NDJSON. |
| `--base64` | One base64-encoded stdin line decodes to one binary frame. |
| `--msgpack` | Reads a stream of concatenated MessagePack values from stdin; each value becomes one frame. Pairs with the consumer's `--raw` mode for typed end-to-end pipelines. |

### `consumer`

```
consumer [ORCHESTRATOR_ADDR] [MODE]
```

| Mode | Description |
|---|---|
| `--log` *(default)* | Logs each frame to stderr via `env_logger` (UTF-8 if valid, else hex preview). Requires `RUST_LOG=info` to actually emit anything. For interactive debugging. |
| `--jsonl` | Writes each frame as one line on stdout. Validates UTF-8 and rejects payloads containing a newline. |
| `--base64` | Writes each frame as a base64-encoded line on stdout. Binary-safe over text. |
| `--raw` | Writes each frame's bytes verbatim to stdout — no encoding, no framing. Pairs with self-delimiting binary formats like MessagePack. |

## Nushell integration

Nushell has built-in MessagePack support, so qpipe pairs naturally with it for
typed structured-data pipelines.

```nu
# consumer side
consumer 127.0.0.1:7000 --raw | from msgpack --objects

# producer side — single record
{event: "login", user: "alice", at: (date now)} | to msgpack
    | producer 127.0.0.1:7000 --msgpack

# producer side — many records
ls | each { to msgpack } | bytes collect | producer 127.0.0.1:7000 --msgpack
```

The `--raw` consumer mode is a passthrough because MessagePack values are
self-delimiting — `from msgpack --objects` reads them one at a time without
needing newlines or length prefixes layered on top. Types survive the round
trip: `datetime`, `filesize`, `duration`, binary, and nested records all come
out the other end as the same Nushell types.

For text-only pipelines:

```nu
# NDJSON in, NDJSON out
consumer 127.0.0.1:7000 --jsonl | from json --objects

# arbitrary bytes via base64
consumer 127.0.0.1:7000 --base64 | lines | each { decode base64 }
```

## Wire protocol

Two phases per session.

**Session setup** (over the control port):

1. Client connects and sends one role byte: `P` (`0x50`, producer) or `C`
   (`0x43`, consumer).
2. Orchestrator binds an ephemeral port on the same IP family as the control
   socket and generates a 16-byte random token.
3. Orchestrator replies on the control connection with `[u16 BE port][16-byte
   token]`, then closes the control connection.
4. Client connects to the ephemeral port and sends the 16-byte token. Wrong
   tokens are dropped silently and the orchestrator keeps accepting on that
   ephemeral port until the right one shows up (or the listener is dropped).

**Data phase** (over the ephemeral port):

Every frame is:

```
[u32 BE length][...payload...][1 byte ACK = 'A' (0x41)]
```

The ACK flows from receiver back to sender (consumer→orchestrator and
orchestrator→producer), so every send blocks until the next hop has the bytes.
This gives natural backpressure; the trade-off is one round-trip per frame, so
throughput is latency-bound.

Frame size limit: **16 MiB** (`MAX_FRAME_SIZE` in `src/lib.rs`). Larger frames
are rejected on both send and receive paths.

## Delivery semantics

- **MPMC** — many producers, many consumers, one orchestrator.
- **Work-queue, not pub/sub** — each frame is delivered to exactly one
  consumer.
- **FIFO** within the central queue. Across multiple consumers, distribution
  depends on which consumer is currently waiting in `pop()` — effectively a
  load-balanced fan-out.
- **Bounded queue** — when full, producers block on `Producer::send` until a
  consumer drains a slot. The default capacity is 10,000 frames.
- **Re-queue on consumer write failure** — if the orchestrator's write to a
  consumer fails, the in-flight message is pushed to the back of the queue and
  the consumer connection is dropped. The message will be delivered to the next
  available consumer, behind whatever is already queued.
- **At-most-once at the application level** — consumers ACK frames
  automatically at the framing layer on receipt, before application code sees
  them. A consumer that crashes between receiving and processing a frame loses
  it. Build idempotency or use external persistence if you need stronger
  guarantees.
- **No persistence** — the queue lives in orchestrator memory. Restarting the
  orchestrator drops everything in flight.

## Library use

`qpipe` is also a library. The shared module exposes `Producer`, `Consumer`,
and the lower-level `read_frame` / `write_frame` helpers if you want to build
your own client:

```rust
use qpipe::Producer;

let mut p = Producer::connect("127.0.0.1:7000")?;
p.send(b"hello")?;
```

```rust
use qpipe::Consumer;

let mut c = Consumer::connect("127.0.0.1:7000")?;
let msg: Vec<u8> = c.recv()?;
```

For typed payloads, pair with `rmp-serde` on both ends:

```rust
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct Event { id: u64, name: String }

// producer
let bytes = rmp_serde::to_vec(&Event { id: 1, name: "alice".into() })?;
producer.send(&bytes)?;

// consumer
let event: Event = rmp_serde::from_slice(&consumer.recv()?)?;
```

## Operational notes

- **Logging** — all binaries use `env_logger` and default to `warn`. Use
  `RUST_LOG=info` for orchestrator stats and consumer `--log` mode output;
  `RUST_LOG=debug` for per-session trace.
- **Throughput vs. latency** — the per-frame ACK means each `send` is one
  network round-trip. For high-throughput workloads, batch records into a
  single frame (e.g. a msgpack array) rather than sending one frame per record.
- **Ephemeral port range** — the orchestrator uses the OS-assigned ephemeral
  range. If you run behind a firewall, the data port is not predictable; either
  run all three roles on the same host, or open the full ephemeral range
  between them.

## Development: building the Python bindings and running the tests

A reference for building the `qpipe-py` bindings and running the full test
suite — Rust (core + CLI) and Python (codec + end-to-end).

### Prerequisites

- A **Rust** toolchain (`cargo`).
- **[uv](https://docs.astral.sh/uv/)** for Python environments. No Conda is
  used or required.
- **Python 3.9+** (3.14 works). The bindings use **PyO3 0.28+**, which is the
  first release with Python 3.14 support — older PyO3 will not build against
  3.14.
- Optionally **[cargo-nextest](https://nexte.st/)** (`cargo install
  cargo-nextest`) for cleaner output and per-test timeouts on the networked
  tests. Plain `cargo test` works too.

### ⚠️ Vendored dependencies — read this first

This repository vendors its dependencies (`[source]` replacement into
`vendor/`), so Cargo will **not** fetch from crates.io. Every change to
`[dependencies]`, `[dev-dependencies]`, or `[build-dependencies]` — in the root
crate **or** in `bindings/python` — must be followed by a re-vendor:

```sh
cargo vendor vendor                 # re-resolve the whole workspace into vendor/
git add vendor && git commit -m "vendor: add <crate>"
```

If a build fails with `error: no matching package found … location searched:
…/vendor`, that is a missing vendor entry, not a broken build — re-vendor and
commit. Do **not** use `cargo add` (it contacts crates.io and will stall);
hand-edit `Cargo.toml`, then `cargo vendor vendor`.

### Building the Python bindings

The bindings live in `bindings/python/` (Cargo crate `qpipe-py`, producing the
native module `qpipe._qpipe`, wrapped by the pure-Python package in
`python/qpipe/`). They are a workspace member, and a bare `cargo build` at the
repo root deliberately does **not** build them (`default-members = ["."]`), so
use maturin:

```sh
cd bindings/python
uv venv                             # create .venv (uv auto-detects it afterward; no activation needed)
uv pip install maturin              # build backend
uv run maturin develop --uv         # compile the Rust and install `qpipe` into .venv
```

Verify the extension imports:

```sh
uv run python -c "import qpipe; print(qpipe.__version__)"
```

Re-run `uv run maturin develop --uv` after editing the Rust
(`bindings/python/src/lib.rs`). Edits to the pure-Python layer
(`python/qpipe/__init__.py`) are picked up without a rebuild.

> Release wheels are `abi3` (one wheel per platform, covering CPython ≥ 3.9)
> and are built in CI; see `.github/workflows/`.

### Running the Rust tests

The Rust tests need three dev-dependencies in the **root** `Cargo.toml`
(`base64` is already a normal dependency and is reused by the tests):

```toml
[dev-dependencies]
assert_cmd = "2"
predicates = "3"
proptest   = "1"
```

Add them, **re-vendor** (see above), then run:

```sh
cargo nextest run                   # or: cargo test
```

This covers two tiers. The **core/framing** tier is the inline `#[cfg(test)]`
modules in `src/lib.rs` (`frame_tests`, `frame_proptests`) — frame round-trips,
empty and oversize payloads, clean-EOF vs. truncation behavior, and a
`proptest` over arbitrary payloads, all in-memory with no network. The **CLI
integration** tier is `tests/cli.rs`, which spawns the
`orchestrator`/`producer`/`consumer` binaries and exercises the mode flags
end-to-end over loopback.

Optionally, add per-test timeouts in `.config/nextest.toml` (helpful because
the networked tests block on sockets, so a deadlock fails fast instead of
hanging the run):

```toml
[profile.default]
slow-timeout = { period = "10s", terminate-after = 3 }
```

### Running the Python tests

Put the Python dev dependencies (`pytest`, `pytest-timeout`, `msgpack`) in a
dev group so `uv` installs them automatically. The suite is split into two
tiers by the `e2e` marker.

The **fast tier** is the codec/unit tests — they mock the native module, need
no orchestrator, and run in milliseconds:

```sh
cd bindings/python
uv run pytest -m "not e2e"
```

The **end-to-end tier** spins up the real `orchestrator` binary, so build it
first (a normal `cargo build`, separate from the extension build) and point the
fixtures at it:

```sh
# from the repo root:
cargo build                         # produces target/debug/{orchestrator,producer,consumer}

cd bindings/python
export QPIPE_ORCHESTRATOR_BIN=../../target/debug/orchestrator
uv run pytest -m e2e
```

> **fish:** use `set -x QPIPE_ORCHESTRATOR_BIN ../../target/debug/orchestrator`
> instead of `export`. Or run `cargo install --path . --debug` to put the
> binaries on your `PATH`; the fixtures then find `orchestrator` by name and no
> env var is needed.

The e2e tier includes `test_mpmc_no_loss_no_duplication`, the
multi-producer/multi-consumer concurrency test.

To run **both tiers** at once:

```sh
cd bindings/python
QPIPE_ORCHESTRATOR_BIN=../../target/debug/orchestrator uv run pytest
```

### Running the whole suite, in order

The tiers have a build-before-test dependency, so from a clean checkout the
order is:

```sh
# 1. Rust: core + CLI tests
cargo nextest run

# 2. build the binaries the Python e2e tests shell out to
cargo build

# 3. build the extension into the venv
#    (first time only: `cd bindings/python && uv venv && uv pip install maturin`)
cd bindings/python && uv run maturin develop --uv

# 4. Python: both tiers
QPIPE_ORCHESTRATOR_BIN=../../target/debug/orchestrator uv run pytest
```

If you keep a top-level `justfile`:

```sh
just test-all                       # all of the above, in order
just test-quick                     # Rust + Python codec tier only (skips process-spawning e2e)
```

### Common pitfalls

| Symptom | Cause | Fix |
| --- | --- | --- |
| `error: no matching package found … vendor` | a dependency isn't vendored | `cargo vendor vendor`, then commit `vendor/` |
| `unresolved import` for a crate used only in `tests/` | dev-dependency missing from `Cargo.toml` | add to `[dev-dependencies]`, then re-vendor |
| pytest collects `0 items` / a test file is silently ignored | filename doesn't match `test_*.py` | rename (e.g. `codec_tests.py` → `test_codecs.py`) |
| pytest: `fixture 'orchestrator' not found` | the fixtures file is misnamed or missing | it must be exactly `tests/conftest.py` |
| e2e tests can't find the orchestrator | binary not built / not on `PATH` | `cargo build`; set `QPIPE_ORCHESTRATOR_BIN` |
| `import qpipe` fails, or is stale after a Rust edit | extension not (re)built | `uv run maturin develop --uv` |
| `maturin develop` complains there's no virtualenv | no `.venv` detected | run `uv venv` in `bindings/python` first |
| PyO3 build errors against Python 3.14 | PyO3 older than 0.28 | the pin is `0.28`; re-vendor if you changed it |
| occasional `LEAK` on a run that was cancelled by a failing test | a spawned process outlived the cancelled test | benign; disappears once the run is green |

## License

`qpipe` is offered under a **dual-licensing** model. You may choose **one** of
the following licenses:

1. **Open Source License**: GNU Affero General Public License, version 3 or
   later  SPDX: `AGPL-3.0-or-later`  See: `LICENSE-AGPL` (and/or `LICENSE`)

2. **Commercial License**: A separate commercial license is available from
   Johannes Blaschke without the conditions of the GNU Affero GPL. See:
   `COMMERCIAL.md`

If you do not have a commercial license agreement with Johannes Blaschke, your
use of this project is governed by the **AGPL-3.0-or-later**.

### What this means (high level)

- The AGPL is an OSI-approved open-source license. You may use `qpipe`
  commercially under the AGPL if you comply with its terms.
- If you modify `qpipe` and run it to provide network access to users (e.g., as
  a service), the AGPL includes obligations related to offering the
  corresponding source code of the version you run.
- If your organization cannot or does not want to comply with the AGPL’s
  requirements, you can obtain a commercial license.

For commercial licensing inquiries: **Johannes Blaschke, johannes@blaschke.science**

## Contributing

We welcome contributions!

To preserve the ability to offer `qpipe` under both open-source and commercial
licenses, all contributions must be made under the Contributor License
Agreement:

- See: `CLA.md`

By submitting a pull request (or otherwise contributing code), you agree that
your contribution is made under the terms of the CLA.

---

Copyright (c) 2026 Johannes Blaschke
