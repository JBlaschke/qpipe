#!/usr/bin/env python

# SPDX-License-Identifier: AGPL-3.0-or-later

"""Codec-layer tests. The native _qpipe.Producer/Consumer are mocked, so these
test ONLY the Python wrappers + codecs from qpipe/__init__.py."""

import pytest
from unittest.mock import MagicMock
import qpipe
from qpipe import (
    Producer, Consumer, QpipeError,
    _RawCodec, _JsonCodec, _MsgpackCodec, _resolve_codec,
)


# ---- codec round-trips ----

def test_raw_codec_passes_bytes_through():
    c = _RawCodec()
    assert c.encode(b"\x00\x01\x02") == b"\x00\x01\x02"
    assert c.decode(b"\x00\x01\x02") == b"\x00\x01\x02"

def test_raw_codec_rejects_non_bytes():
    with pytest.raises(TypeError):
        _RawCodec().encode({"not": "bytes"})

def test_json_codec_roundtrip():
    c = _JsonCodec()
    obj = {"bucket": "b", "name": "obj-1", "n": 42, "nested": [1, 2, 3]}
    assert c.decode(c.encode(obj)) == obj

def test_json_codec_is_newline_free():
    # compact separators keep payloads safe for a --jsonl consumer
    assert b"\n" not in _JsonCodec().encode({"a": 1, "b": {"c": 2}})

def test_msgpack_codec_roundtrip():
    pytest.importorskip("msgpack")
    c = _MsgpackCodec()
    obj = {"event": "login", "user": "alice", "tags": ["x", "y"]}
    assert c.decode(c.encode(obj)) == obj

def test_msgpack_codec_preserves_bytes():
    pytest.importorskip("msgpack")
    c = _MsgpackCodec()
    # use_bin_type=True / raw=False => bytes stay bytes, str stays str
    assert c.decode(c.encode({"blob": b"\x00\xff"})) == {"blob": b"\x00\xff"}


# ---- codec resolution ----

def test_resolve_default_is_raw():
    assert isinstance(_resolve_codec(None), _RawCodec)
    assert isinstance(_resolve_codec("raw"), _RawCodec)

def test_resolve_by_name():
    assert isinstance(_resolve_codec("json"), _JsonCodec)

def test_resolve_unknown_name_raises_valueerror():
    with pytest.raises(ValueError):
        _resolve_codec("yaml")

def test_resolve_accepts_duck_typed_codec():
    custom = MagicMock(spec=["encode", "decode"])
    assert _resolve_codec(custom) is custom

def test_resolve_rejects_object_without_codec_methods():
    with pytest.raises(TypeError):
        _resolve_codec(object())


# ---- wrapper delegation (native layer mocked) ----

def test_producer_send_encodes_then_delegates():
    inner = MagicMock()
    p = Producer(inner, _JsonCodec())
    p.send({"x": 1})
    inner.send.assert_called_once_with(b'{"x":1}')

def test_producer_send_bytes_bypasses_codec():
    inner = MagicMock()
    p = Producer(inner, _JsonCodec())  # codec present...
    p.send_bytes(b"\x00raw\xff")        # ...but send_bytes ignores it
    inner.send.assert_called_once_with(b"\x00raw\xff")

def test_consumer_recv_decodes_inner_bytes():
    inner = MagicMock()
    inner.recv.return_value = b'{"y":2}'
    c = Consumer(inner, _JsonCodec())
    assert c.recv() == {"y": 2}

def test_consumer_recv_bytes_bypasses_codec():
    inner = MagicMock()
    inner.recv.return_value = b"\x00raw"
    c = Consumer(inner, _JsonCodec())
    assert c.recv_bytes() == b"\x00raw"

def test_producer_context_manager_closes():
    inner = MagicMock()
    with Producer(inner, _RawCodec()) as p:
        p.send_bytes(b"x")
    inner.close.assert_called_once()


# ---- iterator semantics (the StopIteration-on-QpipeError behavior) ----

def test_iterator_yields_until_transport_error():
    inner = MagicMock()
    # two good frames, then connection drops
    inner.recv.side_effect = [b'{"i":0}', b'{"i":1}', QpipeError("closed")]
    c = Consumer(inner, _JsonCodec())
    assert list(c) == [{"i": 0}, {"i": 1}]

def test_iterator_empty_on_immediate_close():
    inner = MagicMock()
    inner.recv.side_effect = QpipeError("closed immediately")
    c = Consumer(inner, _resolve_codec("raw"))
    assert list(c) == []

def test_iterator_does_not_swallow_other_errors():
    # A non-QpipeError must propagate, not become StopIteration.
    inner = MagicMock()
    inner.recv.side_effect = ValueError("bug, not a transport error")
    c = Consumer(inner, _JsonCodec())
    with pytest.raises(ValueError):
        next(iter(c))
