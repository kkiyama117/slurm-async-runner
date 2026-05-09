//! pyo3 wrappers for the `slurm_async_runner._core.tssrun` submodule.

#![cfg(feature = "pyo3")]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;

use crate::tssrun::cmd::{Resource, TssrunCmd};
use crate::tssrun::handle::JobHandle;
use crate::tssrun::log::{FileLogSink, JobLogSink, NullLogSink, StdLogSink};
use crate::tssrun::manager::{AttachKey, TssrunManager};

// ---------- Resource ----------

#[pyclass(
    name = "Resource",
    module = "slurm_async_runner._core.tssrun",
    from_py_object,
    frozen
)]
#[derive(Clone)]
pub struct PyResource(pub Resource);

#[pymethods]
impl PyResource {
    #[new]
    #[pyo3(signature = (processes = None, threads = None, cores = None, memory = None, gpus = None))]
    fn new(
        processes: Option<u32>,
        threads: Option<u32>,
        cores: Option<u32>,
        memory: Option<String>,
        gpus: Option<u32>,
    ) -> Self {
        Self(Resource {
            processes,
            threads,
            cores,
            memory,
            gpus,
        })
    }
    #[getter]
    fn processes(&self) -> Option<u32> {
        self.0.processes
    }
    #[getter]
    fn threads(&self) -> Option<u32> {
        self.0.threads
    }
    #[getter]
    fn cores(&self) -> Option<u32> {
        self.0.cores
    }
    #[getter]
    fn memory(&self) -> Option<String> {
        self.0.memory.clone()
    }
    #[getter]
    fn gpus(&self) -> Option<u32> {
        self.0.gpus
    }
}

// ---------- TssrunCmd ----------

#[pyclass(
    name = "TssrunCmd",
    module = "slurm_async_runner._core.tssrun",
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
        queue = None,
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
        queue: Option<String>,
        time_limit: Option<String>,
        rsc: Option<PyResource>,
        x11: bool,
        env: HashMap<String, String>,
        cwd: Option<PathBuf>,
        tssrun_bin: String,
    ) -> Self {
        Self(TssrunCmd {
            tssrun_bin,
            queue,
            time_limit,
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
    module = "slurm_async_runner._core.tssrun",
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

// ---------- JobHandle ----------

#[pyclass(name = "TssrunJobHandle", module = "slurm_async_runner._core.tssrun")]
pub struct PyTssrunJobHandle {
    inner: Arc<tokio::sync::Mutex<JobHandle>>,
}

#[pymethods]
impl PyTssrunJobHandle {
    #[getter]
    fn pid<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move { Ok(inner.lock().await.pid()) })
    }

    #[getter]
    fn jobid<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move { Ok(inner.lock().await.jobid()) })
    }

    #[getter]
    fn node<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move { Ok(inner.lock().await.node()) })
    }

    #[getter]
    fn sent_env<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move { Ok(inner.lock().await.sent_env()) })
    }

    fn live_env<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .lock()
                .await
                .live_env()
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    fn is_running<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move { Ok(inner.lock().await.is_running()) })
    }

    fn exit_code<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move { Ok(inner.lock().await.exit_code()) })
    }

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
}

// ---------- TssrunManager ----------

#[pyclass(
    name = "TssrunManager",
    module = "slurm_async_runner._core.tssrun",
    from_py_object
)]
#[derive(Clone)]
pub struct PyTssrunManager(pub Arc<TssrunManager>);

#[pymethods]
impl PyTssrunManager {
    #[new]
    #[pyo3(signature = (cmd, state_dir = None, log_sink = None))]
    fn new(cmd: PyTssrunCmd, state_dir: Option<PathBuf>, log_sink: Option<PyLogSink>) -> Self {
        let mut m = TssrunManager::new(cmd.0);
        if let Some(d) = state_dir {
            m = m.with_state_dir(d);
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
            Ok(PyTssrunJobHandle {
                inner: Arc::new(tokio::sync::Mutex::new(handle)),
            })
        })
    }

    fn attach_pid<'py>(&self, py: Python<'py>, pid: u32) -> PyResult<Bound<'py, PyAny>> {
        let m = self.0.clone();
        future_into_py(py, async move {
            let h = m
                .attach(AttachKey::Pid(pid))
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Ok(PyTssrunJobHandle {
                inner: Arc::new(tokio::sync::Mutex::new(h)),
            })
        })
    }

    fn attach_jobid<'py>(&self, py: Python<'py>, jobid: u64) -> PyResult<Bound<'py, PyAny>> {
        let m = self.0.clone();
        future_into_py(py, async move {
            let h = m
                .attach(AttachKey::JobId(jobid))
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Ok(PyTssrunJobHandle {
                inner: Arc::new(tokio::sync::Mutex::new(h)),
            })
        })
    }

    fn attach_file<'py>(&self, py: Python<'py>, path: PathBuf) -> PyResult<Bound<'py, PyAny>> {
        let m = self.0.clone();
        future_into_py(py, async move {
            let h = m
                .attach(AttachKey::File(path))
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Ok(PyTssrunJobHandle {
                inner: Arc::new(tokio::sync::Mutex::new(h)),
            })
        })
    }
}

// ---------- submodule wiring ----------

#[pymodule]
#[pyo3(name = "tssrun")]
pub mod inner_module {
    use pyo3::prelude::*;

    const PYTHON_MODULE_NAME: &str = "slurm_async_runner._core.tssrun";

    #[pymodule_export]
    use super::PyLogSink;
    #[pymodule_export]
    use super::PyResource;
    #[pymodule_export]
    use super::PyTssrunCmd;
    #[pymodule_export]
    use super::PyTssrunJobHandle;
    #[pymodule_export]
    use super::PyTssrunManager;
    #[pymodule_export]
    use super::file_log_sink;
    #[pymodule_export]
    use super::null_log_sink;
    #[pymodule_export]
    use super::std_log_sink;

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
