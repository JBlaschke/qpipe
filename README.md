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
