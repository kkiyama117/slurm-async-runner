//! pyo3 bindings for the sbatch module. Lives at
//! `slurm_async_runner._slurm_async_runner_core.sbatch` in the Python namespace.

#![cfg(feature = "pyo3")]

use std::collections::HashMap;
use std::path::PathBuf;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;

use crate::entities::slurm::{JobTimeLimit, ResourceSpec};
use crate::sbatch::cmd::SbatchCmd;
use crate::sbatch::error::SbatchSpawnError;
use crate::sbatch::handle::SbatchJobHandle;
use crate::sbatch::manager::SbatchManager;

// ---------- SbatchCmd ----------

#[pyclass(
    name = "SbatchCmd",
    module = "slurm_async_runner._slurm_async_runner_core.sbatch",
    from_py_object
)]
#[derive(Clone)]
pub struct PySbatchCmd(pub SbatchCmd);

#[pymethods]
impl PySbatchCmd {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        script,
        *,
        sbatch_bin = "sbatch".to_string(),
        job_name = None,
        partition = None,
        time_limit = None,
        rsc = None,
        output = None,
        error = None,
        chdir = None,
        env = None,
        args = None,
    ))]
    fn new(
        script: PathBuf,
        sbatch_bin: String,
        job_name: Option<String>,
        partition: Option<String>,
        time_limit: Option<String>,
        rsc: Option<String>,
        output: Option<String>,
        error: Option<String>,
        chdir: Option<PathBuf>,
        env: Option<HashMap<String, String>>,
        args: Option<Vec<String>>,
    ) -> PyResult<Self> {
        let mut cmd = SbatchCmd::new(script);
        cmd.sbatch_bin = sbatch_bin;
        cmd.job_name = job_name;
        cmd.partition = partition;
        if let Some(s) = time_limit {
            cmd.time_limit = Some(s.parse::<JobTimeLimit>().map_err(py_err)?);
        }
        if let Some(s) = rsc {
            cmd.rsc = Some(s.parse::<ResourceSpec>().map_err(py_err)?);
        }
        cmd.output = output;
        cmd.error = error;
        cmd.chdir = chdir;
        cmd.env = env.unwrap_or_default();
        cmd.args = args.unwrap_or_default();
        Ok(Self(cmd))
    }

    fn build_argv(&self) -> PyResult<Vec<String>> {
        self.0
            .build_argv()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
}

// ---------- SbatchManager ----------

#[pyclass(
    name = "SbatchManager",
    module = "slurm_async_runner._slurm_async_runner_core.sbatch",
    from_py_object
)]
#[derive(Clone)]
pub struct PySbatchManager(pub SbatchManager);

#[pymethods]
impl PySbatchManager {
    #[new]
    #[pyo3(signature = (cmd, *, state_dir = None))]
    fn new(cmd: PySbatchCmd, state_dir: Option<PathBuf>) -> Self {
        let mut mgr = SbatchManager::new(cmd.0);
        if let Some(d) = state_dir {
            mgr = mgr.with_state_dir(d);
        }
        Self(mgr)
    }

    fn spawn<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mgr = self.0.clone();
        future_into_py(py, async move {
            let h = mgr.spawn().await.map_err(|e| match e {
                SbatchSpawnError::SubmittedButUnpersisted { jobid, source } => {
                    PyRuntimeError::new_err(format!(
                        "submitted but unpersisted: jobid={jobid}, source={source}"
                    ))
                }
                other => PyRuntimeError::new_err(other.to_string()),
            })?;
            Ok(PySbatchJobHandle(h))
        })
    }

    fn attach_uuid<'py>(&self, py: Python<'py>, uuid: String) -> PyResult<Bound<'py, PyAny>> {
        let mgr = self.0.clone();
        let u = uuid::Uuid::parse_str(&uuid).map_err(py_err)?;
        future_into_py(py, async move {
            let h = mgr.attach_uuid(u).await.map_err(py_err)?;
            Ok(PySbatchJobHandle(h))
        })
    }

    fn attach_jobid<'py>(&self, py: Python<'py>, jobid: u64) -> PyResult<Bound<'py, PyAny>> {
        let mgr = self.0.clone();
        future_into_py(py, async move {
            let h = mgr.attach_jobid(jobid).await.map_err(py_err)?;
            Ok(PySbatchJobHandle(h))
        })
    }

    fn attach_file<'py>(&self, py: Python<'py>, path: PathBuf) -> PyResult<Bound<'py, PyAny>> {
        let mgr = self.0.clone();
        future_into_py(py, async move {
            let h = mgr.attach_file(path).await.map_err(py_err)?;
            Ok(PySbatchJobHandle(h))
        })
    }
}

// ---------- SbatchJobHandle ----------

#[pyclass(
    name = "SbatchJobHandle",
    module = "slurm_async_runner._slurm_async_runner_core.sbatch",
    from_py_object
)]
#[derive(Clone)]
pub struct PySbatchJobHandle(pub SbatchJobHandle);

#[pymethods]
impl PySbatchJobHandle {
    #[getter]
    fn uuid(&self) -> String {
        self.0.uuid().to_string()
    }

    #[getter]
    fn jobid(&self) -> Option<u64> {
        self.0.jobid()
    }

    #[getter]
    fn partition(&self) -> Option<String> {
        self.0.partition().map(|p| p.to_string())
    }

    #[getter]
    fn job_name(&self) -> Option<String> {
        self.0.job_name()
    }

    #[getter]
    fn sent_env(&self) -> HashMap<String, String> {
        self.0.sent_env()
    }

    #[getter]
    fn output_template(&self) -> Option<String> {
        self.0.output_template()
    }

    #[getter]
    fn error_template(&self) -> Option<String> {
        self.0.error_template()
    }

    #[getter]
    fn output_path(&self) -> Option<PathBuf> {
        self.0.output_path()
    }

    #[getter]
    fn error_path(&self) -> Option<PathBuf> {
        self.0.error_path()
    }

    fn is_running(&self) -> bool {
        self.0.is_running()
    }

    fn is_finished(&self) -> bool {
        self.0.is_finished()
    }

    fn exit_code(&self) -> Option<i32> {
        self.0.exit_code()
    }

    fn refresh<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let h = self.0.clone();
        future_into_py(py, async move {
            h.refresh().await.map_err(py_err)?;
            Ok(())
        })
    }

    fn refresh_with_sacct<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let h = self.0.clone();
        future_into_py(py, async move {
            h.refresh_with_sacct().await.map_err(py_err)?;
            Ok(())
        })
    }

    #[pyo3(signature = (poll_interval_secs))]
    fn wait_terminal<'py>(
        &self,
        py: Python<'py>,
        poll_interval_secs: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let h = self.0.clone();
        future_into_py(py, async move {
            h.wait_terminal(std::time::Duration::from_secs_f64(poll_interval_secs))
                .await
                .map_err(py_err)?;
            Ok(())
        })
    }
}

fn py_err<E: std::fmt::Display>(e: E) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

// ---------- submodule wiring ----------

#[pymodule]
#[pyo3(name = "sbatch")]
pub mod inner_module {
    use pyo3::prelude::*;

    const PYTHON_MODULE_NAME: &str = "slurm_async_runner._slurm_async_runner_core.sbatch";

    #[pymodule_export]
    use super::PySbatchCmd;
    #[pymodule_export]
    use super::PySbatchJobHandle;
    #[pymodule_export]
    use super::PySbatchManager;

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
