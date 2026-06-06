# SPDX-License-Identifier: AGPL-3.0-or-later
"""Python bindings for qpipe — MPMC work queue over TCP.

The native `_qpipe` extension moves raw frames (bytes in, bytes out). This
module wraps it with an optional `codec` that serializes Python objects into
frame payloads and back, mirroring the parts of the CLI's mode flags that are
actually about payload encoding.

    raw      bytes <-> bytes (default; same as the native API)
    json     compact, newline-free JSON  (wire-compatible with consumer --jsonl)
    msgpack  one MessagePack value per frame (wire-compatible with --msgpack)

Pass a name, or any object with .encode(obj)->bytes and .decode(bytes)->obj
for a custom codec.

Messages of any size: payloads larger than MAX_FRAME_SIZE are transparently
chunked by send() and reassembled by recv(), which returns complete messages
in completion order. Partial messages orphaned by producer failures can be
inspected with Consumer.pending_partials() and discarded with
Consumer.gc_partials().
"""
from __future__ import annotations

import json as _json
from typing import Any, Self

from ._qpipe import (
    Producer as _Producer, Consumer as _Consumer, QpipeError,
    healthcheck, wait_until_healthy, request_drain, request_shutdown,
    MAX_FRAME_SIZE, CHUNK_HEADER_LEN, MAX_CHUNK_PAYLOAD,
    MAX_CHUNKS, MAX_MESSAGE_SIZE, FRAME_FLAG_CHUNK,
)

__all__ = [
    "Producer", "Consumer", "QpipeError", "healthcheck", "wait_until_healthy",
    "request_drain", "request_shutdown",
    "MAX_FRAME_SIZE", "CHUNK_HEADER_LEN", "MAX_CHUNK_PAYLOAD",
    "MAX_CHUNKS", "MAX_MESSAGE_SIZE", "FRAME_FLAG_CHUNK",
    "__version__",
]
__version__ = "1.5.0"


# ----- codecs -----

class _RawCodec:
    def encode(self, obj: Any) -> bytes:
        if not isinstance(obj, (bytes, bytearray, memoryview)):
            raise TypeError(
                f"raw codec needs bytes-like, got {type(obj).__name__}; "
                "use codec='json'/'msgpack' to send objects"
            )
        return bytes(obj)

    def decode(self, data: bytes) -> bytes:
        return data


class _JsonCodec:
    # compact separators => no embedded newlines => safe for a --jsonl consumer
    def encode(self, obj: Any) -> bytes:
        return _json.dumps(obj, separators=(",", ":")).encode("utf-8")

    def decode(self, data: bytes) -> Any:
        return _json.loads(data)


class _MsgpackCodec:
    def __init__(self) -> None:
        try:
            import msgpack  # optional dependency
        except ImportError as e:  # pragma: no cover
            raise ImportError(
                "codec='msgpack' requires the 'msgpack' package "
                "(pip install 'qpipe[msgpack]')"
            ) from e
        self._msgpack = msgpack

    def encode(self, obj: Any) -> bytes:
        return self._msgpack.packb(obj, use_bin_type=True)

    def decode(self, data: bytes) -> Any:
        return self._msgpack.unpackb(data, raw=False)


_CODECS = {"raw": _RawCodec, "json": _JsonCodec, "msgpack": _MsgpackCodec}


def _resolve_codec(codec: Any) -> Any:
    if codec is None or isinstance(codec, str):
        try:
            return _CODECS[codec or "raw"]()
        except KeyError:
            raise ValueError(
                f"unknown codec {codec!r}; choose from {sorted(_CODECS)} "
                "or pass an object with encode()/decode()"
            ) from None
    if hasattr(codec, "encode") and hasattr(codec, "decode"):
        return codec
    raise TypeError("codec must be a name or an object with encode()/decode()")


# ----- wrappers -----

class Producer:
    """Push messages to an orchestrator. `with` closes on exit."""

    __slots__ = ("_inner", "_codec")

    def __init__(self, inner: _Producer, codec: Any) -> None:
        self._inner = inner
        self._codec = codec

    @classmethod
    def connect(cls, addr: str, codec: Any = "raw") -> Self:
        return cls(_Producer.connect(addr), _resolve_codec(codec))

    def send(self, obj: Any) -> None:
        """Encode `obj` with the codec and send it as one message."""
        self._inner.send(self._codec.encode(obj))

    def send_bytes(self, data: bytes) -> None:
        """Bypass the codec; send raw message bytes regardless of codec."""
        self._inner.send(data)

    def close(self) -> None:
        self._inner.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> bool:
        self.close()
        return False


class Consumer:
    """
    Pull messages from an orchestrator. Iterable; stops when the connection
    drops.
    """

    __slots__ = ("_inner", "_codec")

    def __init__(self, inner: _Consumer, codec: Any) -> None:
        self._inner = inner
        self._codec = codec

    @classmethod
    def connect(cls, addr: str, codec: Any = "raw") -> Self:
        return cls(_Consumer.connect(addr), _resolve_codec(codec))

    def recv(self) -> Any:
        """Receive one complete message and decode it with the codec."""
        return self._codec.decode(self._inner.recv())

    def recv_bytes(self) -> bytes:
        """Bypass the codec; return the raw message bytes."""
        return self._inner.recv()

    def gc_partials(self, idle_secs: float) -> int:
        """Discard partial multi-frame messages that haven't received a chunk
        for at least `idle_secs` seconds. Returns how many were discarded.

        Partials are orphaned when a producer dies mid-message (or the
        orchestrator expires a stale message); they cost memory until
        dropped. Call this on whatever cadence suits your latency
        expectations, e.g. every few hundred recvs or when the app idles.
        """
        return self._inner.gc_partials(idle_secs)

    def pending_partials(self) -> tuple[int, int]:
        """(message count, buffered bytes) of incomplete multi-frame
        messages currently held for reassembly."""
        return self._inner.pending_partials()

    def close(self) -> None:
        self._inner.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> bool:
        self.close()
        return False

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> Any:
        try:
            return self._codec.decode(self._inner.recv())
        except QpipeError:
            raise StopIteration from None
