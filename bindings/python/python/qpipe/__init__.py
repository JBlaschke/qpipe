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
"""
from __future__ import annotations

import json as _json
from typing import Any

from ._qpipe import Producer as _Producer, Consumer as _Consumer, QpipeError

__all__ = ["Producer", "Consumer", "QpipeError", "__version__"]
__version__ = "1.4.0"


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
    """Push frames to an orchestrator. `with` closes on exit."""

    def __init__(self, inner: _Producer, codec: Any) -> None:
        self._inner = inner
        self._codec = codec

    @classmethod
    def connect(cls, addr: str, codec: Any = "raw") -> "Producer":
        return cls(_Producer.connect(addr), _resolve_codec(codec))

    def send(self, obj: Any) -> None:
        """Encode `obj` with the codec and send it as one frame."""
        self._inner.send(self._codec.encode(obj))

    def send_bytes(self, data: bytes) -> None:
        """Bypass the codec; send raw frame bytes regardless of codec."""
        self._inner.send(data)

    def close(self) -> None:
        self._inner.close()

    def __enter__(self) -> "Producer":
        return self

    def __exit__(self, *exc: object) -> bool:
        self.close()
        return False


class Consumer:
    """Pull frames from an orchestrator. Iterable; stops when the connection drops."""

    def __init__(self, inner: _Consumer, codec: Any) -> None:
        self._inner = inner
        self._codec = codec

    @classmethod
    def connect(cls, addr: str, codec: Any = "raw") -> "Consumer":
        return cls(_Consumer.connect(addr), _resolve_codec(codec))

    def recv(self) -> Any:
        """Receive one frame and decode it with the codec."""
        return self._codec.decode(self._inner.recv())

    def recv_bytes(self) -> bytes:
        """Bypass the codec; return the raw frame bytes."""
        return self._inner.recv()

    def close(self) -> None:
        self._inner.close()

    def __enter__(self) -> "Consumer":
        return self

    def __exit__(self, *exc: object) -> bool:
        self.close()
        return False

    def __iter__(self) -> "Consumer":
        return self

    def __next__(self) -> Any:
        try:
            return self._codec.decode(self._inner.recv())
        except QpipeError:
            raise StopIteration from None
