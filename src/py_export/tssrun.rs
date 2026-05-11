//! pyo3 wrappers for the `slurm_async_runner._slurm_async_runner_core.tssrun` submodule.

#![cfg(feature = "pyo3")]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use uuid::Uuid;

use tokio::sync::watch;

use crate::py_export::entities::slurm::sbatch_options::resource_spec::PyResourceSpec;
use crate::py_export::entities::slurm::sbatch_options::time_limit::PyJobTimeLimit;
use crate::tssrun::cmd::TssrunCmd;

use crate::store::JobStateStore;
use crate::tssrun::handle::{TssrunJobHandle, TssrunJobSnapshot, read_live_env_for_pid};
use crate::tssrun::log::{FileLogSink, JobLogSink, NullLogSink, StdLogSink};
use crate::tssrun::manager::{AttachKey, TssrunManager};
use crate::tssrun::store::{FileSystemStateStore, InMemoryStateStore};

// ---------- TssrunCmd ----------

#[pyclass(
    name = "TssrunCmd",
    module = "slurm_async_runner._slurm_async_runner_core.tssrun",
    from_py_object
)]
#[derive(Clone)]
pub struct PyTssrunCmd(pub TssrunCmd);

#[pymethods]
impl PyTssrunCmd {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        program,
        args = Vec::new(),
        partition = None,
        time_limit = None,
        rsc = None,
        x11 = false,
        env = HashMap::new(),
        cwd = None,
        tssrun_bin = "tssrun".to_string(),
    ))]
    fn new(
        program: PathBuf,
        args: Vec<String>,
        partition: Option<String>,
        time_limit: Option<PyJobTimeLimit>,
        rsc: Option<PyResourceSpec>,
        x11: bool,
        env: HashMap<String, String>,
        cwd: Option<PathBuf>,
        tssrun_bin: String,
    ) -> Self {
        Self(TssrunCmd {
            tssrun_bin,
            partition,
            time_limit: time_limit.map(|t| t.0),
            rsc: rsc.map(|r| r.0),
            x11,
            program,
            args,
            env,
            cwd,
        })
    }

    fn build_argv(&self) -> PyResult<Vec<String>> {
        self.0
            .build_argv()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
}

// ---------- LogSink ----------

#[pyclass(
    name = "LogSink",
    module = "slurm_async_runner._slurm_async_runner_core.tssrun",
    from_py_object,
    frozen
)]
#[derive(Clone)]
pub struct PyLogSink(pub Arc<dyn JobLogSink>);

#[pyfunction]
#[pyo3(name = "null_log_sink")]
fn null_log_sink() -> PyLogSink {
    PyLogSink(Arc::new(NullLogSink))
}

#[pyfunction]
#[pyo3(name = "std_log_sink")]
fn std_log_sink() -> PyLogSink {
    PyLogSink(Arc::new(StdLogSink))
}

#[pyfunction]
#[pyo3(name = "file_log_sink")]
fn file_log_sink<'py>(
    py: Python<'py>,
    stdout: PathBuf,
    stderr: PathBuf,
) -> PyResult<Bound<'py, PyAny>> {
    future_into_py(py, async move {
        let sink = FileLogSink::create(stdout, stderr)
            .await
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyLogSink(Arc::new(sink)))
    })
}

// ---------- JobStateStore ----------

/// Opaque Python handle to an [`Arc<dyn JobStateStore<TssrunJobSnapshot>>`]. Construct via
/// the `in_memory_state_store` / `file_system_state_store` factories and
/// pass the result to ``TssrunManager(cmd, store=...)``.
///
/// The trait is intentionally not subclassable from Python — Rust
/// requires the backend to implement [`async_trait`] methods that pyo3
/// cannot dispatch back into Python without a dedicated bridge. New
/// backends should be added in Rust and re-exported here.
#[pyclass(
    name = "JobStateStore",
    module = "slurm_async_runner._slurm_async_runner_core.tssrun",
    from_py_object,
    frozen
)]
#[derive(Clone)]
pub struct PyJobStateStore(pub Arc<dyn JobStateStore<TssrunJobSnapshot>>);

/// Build an in-process, in-memory state store.
///
/// This is the default backend used when ``TssrunManager`` is
/// constructed without a ``store=`` argument; the factory exists so
/// callers can wire the *same* store into multiple managers within a
/// single process (each ``Arc`` clone shares the underlying map).
#[pyfunction]
#[pyo3(name = "in_memory_state_store")]
fn in_memory_state_store() -> PyJobStateStore {
    PyJobStateStore(Arc::new(InMemoryStateStore::new()))
}

/// Build an on-disk state store rooted at ``dir``.
///
/// Snapshots are written as ``{dir}/{uuid}.json`` via atomic rename.
/// The directory is created lazily on first save, so it is fine to pass
/// a path that does not yet exist — but the parent must be writable when
/// the first save fires.
#[pyfunction]
#[pyo3(name = "file_system_state_store")]
fn file_system_state_store(dir: PathBuf) -> PyJobStateStore {
    PyJobStateStore(Arc::new(FileSystemStateStore::new(dir)))
}

// ---------- TssrunJobHandle ----------

/// Python view onto a [`TssrunJobHandle`].
///
/// Holds two pieces of state with **deliberately different sharing**:
///
/// - `rx`: a cloned `watch::Receiver` over the snapshot. Snapshot getters
///   (`pid`, `jobid`, `node`, `sent_env`, `is_running`, `exit_code`) read
///   from this receiver synchronously — no TssrunJobHandle mutex, so they are
///   not blocked by an in-flight `wait()` (issue H1 in the original
///   review).
/// - `inner`: a `Mutex<TssrunJobHandle>` that exists solely so `wait()` can
///   `.take()` the join handle once. Snapshot getters never touch it.
#[pyclass(
    name = "TssrunJobHandle",
    module = "slurm_async_runner._slurm_async_runner_core.tssrun"
)]
pub struct PyTssrunJobHandle {
    rx: watch::Receiver<TssrunJobSnapshot>,
    inner: Arc<tokio::sync::Mutex<TssrunJobHandle>>,
}

impl PyTssrunJobHandle {
    fn from_handle(handle: TssrunJobHandle) -> Self {
        let rx = handle.watch();
        Self {
            rx,
            inner: Arc::new(tokio::sync::Mutex::new(handle)),
        }
    }
}

#[pymethods]
impl PyTssrunJobHandle {
    #[getter]
    fn pid<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pid = self.rx.borrow().pid;
        future_into_py(py, async move { Ok(pid) })
    }

    #[getter]
    fn jobid<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let jobid = self.rx.borrow().jobid;
        future_into_py(py, async move { Ok(jobid) })
    }

    /// UUID v7 primary key for this job. Returned as the canonical
    /// hyphenated string (e.g. ``"0190cc1c-7a48-7c0e-a0a0-1234567890ab"``)
    /// so it can be passed straight back to ``attach_uuid`` or used as a
    /// dict key without needing to depend on Python's ``uuid`` module.
    #[getter]
    fn uuid<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let uuid = self.rx.borrow().uuid.to_string();
        future_into_py(py, async move { Ok(uuid) })
    }

    #[getter]
    fn node<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let node = self.rx.borrow().node.clone();
        future_into_py(py, async move { Ok(node) })
    }

    #[getter]
    fn sent_env<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let env = self.rx.borrow().sent_env.clone();
        future_into_py(py, async move { Ok(env) })
    }

    fn live_env<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pid = self.rx.borrow().pid;
        future_into_py(py, async move {
            read_live_env_for_pid(pid)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    fn is_running<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let running = self.rx.borrow().finished.is_none();
        future_into_py(py, async move { Ok(running) })
    }

    fn exit_code<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let code = self.rx.borrow().finished.as_ref().and_then(|f| f.exit_code);
        future_into_py(py, async move { Ok(code) })
    }

    /// Wait for the child to exit. Returns:
    ///
    /// - `int` — the exit code (0 = clean success).
    /// - `None` — the child was terminated by a signal (e.g. SLURM time-
    ///   limit kill, OOM). The persisted snapshot also records this with
    ///   `finished.exit_code = None`.
    ///
    /// Raises ``RuntimeError`` if invoked on an attached handle or after a
    /// previous successful wait.
    fn wait<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .lock()
                .await
                .wait()
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Re-read this handle's snapshot from the configured store and
    /// broadcast it to all snapshot subscribers (so subsequent ``pid``
    /// / ``jobid`` / ``node`` getters reflect the persisted state).
    ///
    /// Useful for an attached handle whose owner is in another process
    /// — the local watch channel only sees mutations the local tee /
    /// wait tasks make, so a cross-process update is not visible until
    /// ``refresh()`` pulls it in. Raises ``RuntimeError`` when no store
    /// is wired or when the store has no record for this handle's uuid.
    fn refresh<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .lock()
                .await
                .refresh()
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }
}

// ---------- TssrunManager ----------

#[pyclass(
    name = "TssrunManager",
    module = "slurm_async_runner._slurm_async_runner_core.tssrun",
    from_py_object
)]
#[derive(Clone)]
pub struct PyTssrunManager(pub Arc<TssrunManager>);

#[pymethods]
impl PyTssrunManager {
    /// Construct a manager.
    ///
    /// Parameters
    /// ----------
    /// cmd
    ///     The ``TssrunCmd`` spec to spawn.
    /// store
    ///     A ``JobStateStore`` for snapshot persistence. When ``None``
    ///     (the default), an in-memory store is used — sufficient for
    ///     single-process workflows on read-only filesystems. To
    ///     persist across processes, pass
    ///     ``store=file_system_state_store(path)``.
    /// log_sink
    ///     Optional ``LogSink``. Defaults to the standard-stream sink.
    #[new]
    #[pyo3(signature = (cmd, store = None, log_sink = None))]
    fn new(cmd: PyTssrunCmd, store: Option<PyJobStateStore>, log_sink: Option<PyLogSink>) -> Self {
        let mut m = TssrunManager::new(cmd.0);
        if let Some(s) = store {
            m = m.with_state_store(s.0);
        }
        if let Some(s) = log_sink {
            m = m.with_log_sink(s.0);
        }
        Self(Arc::new(m))
    }

    fn spawn<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let m = self.0.clone();
        future_into_py(py, async move {
            let handle = m
                .spawn()
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Ok(PyTssrunJobHandle::from_handle(handle))
        })
    }

    /// Attach to a previously persisted handle by its UUID v7 primary key.
    /// Accepts the canonical hyphenated string returned by the ``uuid``
    /// getter — invalid input raises ``RuntimeError``.
    fn attach_uuid<'py>(&self, py: Python<'py>, uuid: &str) -> PyResult<Bound<'py, PyAny>> {
        let parsed = Uuid::parse_str(uuid)
            .map_err(|e| PyRuntimeError::new_err(format!("invalid uuid {uuid:?}: {e}")))?;
        let m = self.0.clone();
        future_into_py(py, async move {
            let h = m
                .attach(AttachKey::Uuid(parsed))
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Ok(PyTssrunJobHandle::from_handle(h))
        })
    }

    fn attach_pid<'py>(&self, py: Python<'py>, pid: u32) -> PyResult<Bound<'py, PyAny>> {
        let m = self.0.clone();
        future_into_py(py, async move {
            let h = m
                .attach(AttachKey::Pid(pid))
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Ok(PyTssrunJobHandle::from_handle(h))
        })
    }

    fn attach_jobid<'py>(&self, py: Python<'py>, jobid: u64) -> PyResult<Bound<'py, PyAny>> {
        let m = self.0.clone();
        future_into_py(py, async move {
            let h = m
                .attach(AttachKey::JobId(jobid))
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Ok(PyTssrunJobHandle::from_handle(h))
        })
    }

    fn attach_file<'py>(&self, py: Python<'py>, path: PathBuf) -> PyResult<Bound<'py, PyAny>> {
        let m = self.0.clone();
        future_into_py(py, async move {
            let h = m
                .attach(AttachKey::File(path))
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Ok(PyTssrunJobHandle::from_handle(h))
        })
    }
}

// ---------- submodule wiring ----------

#[pymodule]
#[pyo3(name = "tssrun")]
pub mod inner_module {
    use pyo3::prelude::*;

    const PYTHON_MODULE_NAME: &str = "slurm_async_runner._slurm_async_runner_core.tssrun";

    #[pymodule_export]
    use super::PyJobStateStore;
    #[pymodule_export]
    use super::PyLogSink;
    // Re-export the crate-local ResourceSpec / JobTimeLimit pyclass wrappers
    // so Python callers can construct them via the same import path
    // as TssrunCmd. This saves an extra import for the common tssrun use case.
    #[pymodule_export]
    use super::PyTssrunCmd;
    #[pymodule_export]
    use super::PyTssrunJobHandle;
    #[pymodule_export]
    use super::PyTssrunManager;
    #[pymodule_export]
    use super::file_log_sink;
    #[pymodule_export]
    use super::file_system_state_store;
    #[pymodule_export]
    use super::in_memory_state_store;
    #[pymodule_export]
    use super::null_log_sink;
    #[pymodule_export]
    use super::std_log_sink;
    #[pymodule_export]
    use crate::py_export::entities::slurm::sbatch_options::resource_spec::PyResourceSpec;
    #[pymodule_export]
    use crate::py_export::entities::slurm::sbatch_options::time_limit::PyJobTimeLimit;

    #[pymodule_init]
    fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
        let py = m.py();
        py.import("sys")?
            .getattr("modules")?
            .set_item(PYTHON_MODULE_NAME, m)?;
        log::debug!("{} Rust module initialized", PYTHON_MODULE_NAME);
        Ok(())
    }
}
