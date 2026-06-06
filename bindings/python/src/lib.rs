// SPDX-License-Identifier: AGPL-3.0-or-later
//
// PyO3 bindings for qpipe. Exposes Producer and Consumer as Python classes
// with context-manager + iterator support. Releases the GIL around all
// blocking network I/O so other Python threads can run.
//
// Multi-frame messages are transparent here exactly as in the Rust API:
// send() chunks payloads > MAX_FRAME_SIZE, recv() reassembles and returns
// complete messages in completion order. Consumer additionally exposes
// gc_partials()/pending_partials() to manage partial messages orphaned by
// producer/orchestrator failures, and the module exports the framing
// constants so Python code never hardcodes them.
use std::time::Duration;

use pyo3::create_exception;
use pyo3::exceptions::{
    PyConnectionError, PyRuntimeError, PyStopIteration, PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

// qpipe-specific error type. Subclasses ConnectionError, so existing
// `except ConnectionError` handlers still catch it, while `except QpipeError`
// can target qpipe's transport errors specifically.
create_exception!(
    _qpipe,
    QpipeError,
    PyConnectionError,
    "Error raised by qpipe transport operations (connect/send/recv)."
);

// ---------- Producer ----------

#[pyclass(module = "qpipe._qpipe")]
struct Producer {
    inner: Option<qpipe::Producer>,
}

#[pymethods]
impl Producer {
    /// Connect to an orchestrator. `addr` is "host:port".
    #[staticmethod]
    fn connect(addr: &str) -> PyResult<Self> {
        let inner = qpipe::Producer::connect(addr)
            .map_err(|e| QpipeError::new_err(format!("connect failed: {e}")))?;
        Ok(Producer { inner: Some(inner) })
    }

    /// Send one message. Blocks until the orchestrator ACKs receipt (every
    /// frame of a chunked message, for payloads larger than MAX_FRAME_SIZE).
    /// Accepts bytes, bytearray, or memoryview.
    fn send(&mut self, py: Python<'_>, data: &[u8]) -> PyResult<()> {
        // Copy out of Python memory before releasing the GIL — &[u8] from
        // PyBytes is only valid while the GIL is held.
        let buf = data.to_vec();
        let inner = self
            .inner
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("producer is closed"))?;
        py.detach(|| inner.send(&buf))
            .map_err(|e| QpipeError::new_err(format!("send failed: {e}")))?;
        Ok(())
    }

    /// Drop the underlying connection. Subsequent calls raise RuntimeError.
    fn close(&mut self) {
        self.inner = None;
    }

    /// Context-manager support: `with Producer.connect(...) as p: ...`
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: &Bound<'_, PyAny>,
        _exc_val: &Bound<'_, PyAny>,
        _exc_tb: &Bound<'_, PyAny>,
    ) -> bool {
        self.close();
        false // don't suppress exceptions
    }

    fn __repr__(&self) -> String {
        match &self.inner {
            Some(_) => "<qpipe.Producer connected>".into(),
            None => "<qpipe.Producer closed>".into(),
        }
    }
}

// ---------- Consumer ----------

#[pyclass(module = "qpipe._qpipe")]
struct Consumer {
    inner: Option<qpipe::Consumer>,
}

#[pymethods]
impl Consumer {
    /// Connect to an orchestrator. `addr` is "host:port".
    #[staticmethod]
    fn connect(addr: &str) -> PyResult<Self> {
        let inner = qpipe::Consumer::connect(addr)
            .map_err(|e| QpipeError::new_err(format!("connect failed: {e}")))?;
        Ok(Consumer { inner: Some(inner) })
    }

    /// Receive one complete message. Blocks until one is available; chunks
    /// of multi-frame messages are buffered internally, so messages are
    /// returned in COMPLETION order. Returns `bytes`.
    fn recv<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let inner = self
            .inner
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("consumer is closed"))?;
        let data = py
            .detach(|| inner.recv())
            .map_err(|e| QpipeError::new_err(format!("recv failed: {e}")))?;
        Ok(PyBytes::new(py, &data))
    }

    /// Discard partial multi-frame messages that haven't received a chunk
    /// for at least `idle_secs` seconds. Returns how many were discarded.
    /// Partials are orphaned when a producer dies mid-message (or the
    /// orchestrator expires a stale message); they cost memory until dropped.
    fn gc_partials(&mut self, idle_secs: f64) -> PyResult<usize> {
        let inner = self
            .inner
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("consumer is closed"))?;
        // try_from_secs_f64 rejects negative / NaN / overflow as an Err
        // instead of panicking across the FFI boundary.
        let idle = Duration::try_from_secs_f64(idle_secs)
            .map_err(|e| PyValueError::new_err(format!("invalid idle_secs: {e}")))?;
        Ok(inner.gc_partials(idle))
    }

    /// (message count, buffered bytes) of incomplete multi-frame messages
    /// currently held for reassembly.
    fn pending_partials(&self) -> PyResult<(usize, usize)> {
        let inner = self
            .inner
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("consumer is closed"))?;
        Ok(inner.pending_partials())
    }

    fn close(&mut self) {
        self.inner = None;
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: &Bound<'_, PyAny>,
        _exc_val: &Bound<'_, PyAny>,
        _exc_tb: &Bound<'_, PyAny>,
    ) -> bool {
        self.close();
        false
    }

    /// `for frame in consumer: ...` — runs until the connection drops.
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        // Convert a qpipe transport error (connection closed) into StopIteration
        // so `for` loops end cleanly. A closed-consumer RuntimeError still
        // propagates, as do any other errors.
        match self.recv(py) {
            Ok(b) => Ok(b),
            Err(e) if e.is_instance_of::<QpipeError>(py) => {
                Err(PyStopIteration::new_err(()))
            }
            Err(e) => Err(e),
        }
    }

    fn __repr__(&self) -> String {
        match &self.inner {
            Some(_) => "<qpipe.Consumer connected>".into(),
            None => "<qpipe.Consumer closed>".into(),
        }
    }
}

// ---------- orchestrator control ----------

/// Liveness probe: Ok(()) iff an orchestrator answers at `addr`.
#[pyfunction]
fn healthcheck(py: Python<'_>, addr: &str) -> PyResult<()> {
    let a = addr.to_owned();
    py.detach(move || qpipe::healthcheck(&a))
        .map_err(|e| QpipeError::new_err(format!("healthcheck failed: {e}")))
}

/// Block until healthy, or until `timeout` seconds elapse (None = forever).
#[pyfunction]
#[pyo3(signature = (addr, timeout=None))]
fn wait_until_healthy(py: Python<'_>, addr: &str, timeout: Option<f64>) -> PyResult<()> {
    let a = addr.to_owned();
    let t = timeout.map(Duration::from_secs_f64);
    py.detach(move || qpipe::wait_until_healthy(&a, t))
        .map_err(|e| QpipeError::new_err(format!("wait_until_healthy failed: {e}")))
}

/// Enter drain mode. ACK-ON-RECEIPT: returns when the orchestrator
/// acknowledges the request, NOT when draining completes.
#[pyfunction]
fn request_drain(py: Python<'_>, addr: &str) -> PyResult<()> {
    let a = addr.to_owned();
    py.detach(move || qpipe::request_drain(&a))
        .map_err(|e| QpipeError::new_err(format!("drain request failed: {e}")))
}

/// Graceful shutdown (server-side timeout applies; overrides a drain). Ack-on-receipt.
#[pyfunction]
fn request_shutdown(py: Python<'_>, addr: &str) -> PyResult<()> {
    let a = addr.to_owned();
    py.detach(move || qpipe::request_shutdown(&a))
        .map_err(|e| QpipeError::new_err(format!("shutdown request failed: {e}")))
}

// ---------- module ----------

#[pymodule]
fn _qpipe(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Producer>()?;
    m.add_class::<Consumer>()?;
    // Distinct qpipe error type (subclass of ConnectionError).
    m.add("QpipeError", m.py().get_type::<QpipeError>())?;
    // Orchestrator control
    m.add_function(wrap_pyfunction!(healthcheck, m)?)?;
    m.add_function(wrap_pyfunction!(wait_until_healthy, m)?)?;
    m.add_function(wrap_pyfunction!(request_drain, m)?)?;
    m.add_function(wrap_pyfunction!(request_shutdown, m)?)?;
    // Framing constants (mirror qpipe's Rust consts) so Python code — tests,
    // payload size pre-checks — never hardcodes them.
    m.add("MAX_FRAME_SIZE", qpipe::MAX_FRAME_SIZE)?;
    m.add("CHUNK_HEADER_LEN", qpipe::CHUNK_HEADER_LEN)?;
    m.add("MAX_CHUNK_PAYLOAD", qpipe::MAX_CHUNK_PAYLOAD)?;
    m.add("MAX_CHUNKS", qpipe::MAX_CHUNKS)?;
    m.add("MAX_MESSAGE_SIZE", qpipe::MAX_MESSAGE_SIZE)?;
    m.add("FRAME_FLAG_CHUNK", qpipe::FRAME_FLAG_CHUNK)?;
    Ok(())
}
