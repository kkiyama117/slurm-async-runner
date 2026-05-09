//! pyo3 wrappers for [`crate::manager::{SlurmCmd, SlurmManager}`].
//!
//! Exposed at `slurm_async_runner._slurm_async_runner_core.manager`:
//!
//! - `SlurmCmd(srun_cmd="srun")` — frozen wrapper around the launcher
//!   binary name.
//! - `SlurmManager(slurm_cmd=None)` — async dispatcher. `run_job`,
//!   `query_job_state`, and `query_job_states_batch` return Python
//!   coroutines via pyo3-async-runtimes (tokio runtime).
//!
//! The async return values are dispatched through the same
//! `PyOnceLock`-cached cross-module import pattern as
//! `runner::query_job_states_batch` so `JobStatus` results stay typed
//! end-to-end (`gaussian_job_shared._gaussian_job_shared_core.entities.slurm.status.JobStatus`).

#![cfg(feature = "pyo3")]

use std::path::PathBuf;
use std::sync::Arc;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyDict;

use crate::manager::{SlurmCmd, SlurmManager};

// ---------------------------------------------------------------- PySlurmCmd

#[pyclass(
    name = "SlurmCmd",
    module = "slurm_async_runner._slurm_async_runner_core.manager",
    from_py_object,
    frozen,
    eq,
    hash
)]
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct PySlurmCmd(pub SlurmCmd);

#[pymethods]
impl PySlurmCmd {
    #[new]
    #[pyo3(signature = (srun_cmd = "srun".to_string()))]
    fn new(srun_cmd: String) -> Self {
        Self(SlurmCmd { srun_cmd })
    }

    #[getter]
    fn srun_cmd(&self) -> &str {
        &self.0.srun_cmd
    }

    fn __repr__(&self) -> String {
        format!("SlurmCmd(srun_cmd={:?})", self.0.srun_cmd)
    }
}

// ------------------------------------------------------------ PySlurmManager

#[pyclass(
    name = "SlurmManager",
    module = "slurm_async_runner._slurm_async_runner_core.manager",
    skip_from_py_object
)]
#[derive(Clone)]
pub struct PySlurmManager(pub Arc<SlurmManager>);

#[pymethods]
impl PySlurmManager {
    #[new]
    #[pyo3(signature = (slurm_cmd = None))]
    fn new(slurm_cmd: Option<PySlurmCmd>) -> Self {
        Self(Arc::new(SlurmManager::new(slurm_cmd.map(|c| c.0))))
    }

    #[getter]
    fn slurm_cmd(&self) -> PySlurmCmd {
        PySlurmCmd(self.0.slurm_cmd.clone())
    }

    /// Build the argv that would dispatch `batch_file`.
    fn build_argv(&self, batch_file: PathBuf) -> PyResult<Vec<String>> {
        self.0
            .build_argv(&batch_file)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Dispatch `batch_file` via the configured launcher. Returns the
    /// subprocess exit code (or `0` for `dry_run=True`).
    #[pyo3(signature = (batch_file, dry_run = false))]
    fn run_job<'py>(
        &self,
        py: Python<'py>,
        batch_file: PathBuf,
        dry_run: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .run_job(&batch_file, dry_run)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Single-jobid form. Returns a `JobStatus`.
    fn query_job_state<'py>(&self, py: Python<'py>, jobid: u64) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let status = inner
                .query_job_state(jobid)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Python::attach(|py| {
                let cls = job_status_class(py)?;
                let parse = cls.getattr("parse")?;
                let py_status = parse.call1((status.state.as_token(), status.reason.as_str()))?;
                Ok::<Py<PyAny>, PyErr>(py_status.unbind())
            })
        })
    }

    /// Bulk form. Returns `dict[int, JobStatus]`.
    fn query_job_states_batch<'py>(
        &self,
        py: Python<'py>,
        jobids: Vec<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = inner
                .query_job_states_batch(&jobids)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Python::attach(|py| {
                let dict = PyDict::new(py);
                if !result.is_empty() {
                    let cls = job_status_class(py)?;
                    let parse = cls.getattr("parse")?;
                    for (jid, status) in result {
                        let py_status =
                            parse.call1((status.state.as_token(), status.reason.as_str()))?;
                        dict.set_item(jid, py_status)?;
                    }
                }
                Ok::<Py<PyAny>, PyErr>(dict.into_any().unbind())
            })
        })
    }

    fn __repr__(&self) -> String {
        format!("SlurmManager(slurm_cmd={:?})", self.0.slurm_cmd.srun_cmd)
    }
}

// --------------------------------------------- shared cross-module-import cache

const UPSTREAM_STATUS_MODULE: &str =
    "gaussian_job_shared._gaussian_job_shared_core.entities.slurm.status";

static JOB_STATUS_CLS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

fn job_status_class<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    let cached = JOB_STATUS_CLS.get_or_try_init(py, || -> PyResult<Py<PyAny>> {
        let module = py.import(UPSTREAM_STATUS_MODULE)?;
        Ok(module.getattr("JobStatus")?.unbind())
    })?;
    Ok(cached.bind(py).clone())
}

// ----------------------------------------------------- Python sub-module glue

#[pymodule(name = "manager")]
pub(crate) mod inner_module {
    use super::*;

    const PYTHON_MODULE_NAME: &str = "slurm_async_runner._slurm_async_runner_core.manager";

    #[pymodule_export]
    use super::{PySlurmCmd, PySlurmManager};

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
