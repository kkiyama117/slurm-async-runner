//! pyo3 async wrapper for [`crate::runner::query_job_states_batch`].
//!
//! Exposes a Python coroutine that returns `dict[int, JobStatus]` where
//! `JobStatus` is the upstream `gaussian_job_shared` Python class —
//! cross-module imported at first call and cached via [`PyOnceLock`]
//! so the import cost is paid once per interpreter.

#![cfg(feature = "pyo3")]

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyDict;

use crate::runner::query_job_states_batch as rust_query;

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

/// Bulk-query SLURM for the `(state, reason)` of every input jobid.
///
/// Issues at most one `squeue` and one `sacct` subprocess. Empty input
/// short-circuits to an empty dict. Every input id appears in the
/// returned dict — ids absent from both backends map to a default
/// `JobStatus` (state=`Unknown`, reason=`None`).
#[pyfunction]
fn query_job_states_batch<'py>(py: Python<'py>, jobids: Vec<u64>) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let result = rust_query(&jobids)
            .await
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Python::attach(|py| {
            let dict = PyDict::new(py);
            // Defer the cross-module import until we actually have a row to
            // build — keeps `[] -> {}` working even when the upstream Python
            // wheel isn't installed (e.g. on test/dev machines without it).
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

#[pymodule(name = "runner")]
pub(crate) mod inner_module {
    use super::*;

    const PYTHON_MODULE_NAME: &str = "slurm_async_runner._slurm_async_runner_core.runner";

    #[pymodule_export]
    use super::query_job_states_batch;

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
