//! PyO3 wrappers for `entities::slurm::{MailType, MailTypeInput, SlurmJobConfig}`.

use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_enum, gen_stub_pymethods};

use crate::entities::slurm as inner;

use super::array_spec::PySlurmArraySpec;
use super::dependency::PySlurmDependency;
use super::resource_spec::PyResourceSpec;
use super::time_limit::PyJobTimeLimit;

// ------------------------------------------------------------------ MailType
#[gen_stub_pyclass_enum]
#[pyclass(
    name = "MailType",
    module = "gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options",
    from_py_object,
    eq,
    eq_int,
    hash,
    frozen
)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[allow(clippy::upper_case_acronyms)]
pub enum PyMailType {
    BEGIN,
    END,
    FAIL,
    REQUEUE,
    ALL,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyMailType {
    fn __str__(&self) -> &'static str {
        match self {
            Self::BEGIN => "BEGIN",
            Self::END => "END",
            Self::FAIL => "FAIL",
            Self::REQUEUE => "REQUEUE",
            Self::ALL => "ALL",
        }
    }

    fn __repr__(&self) -> String {
        format!("MailType.{}", self.__str__())
    }
}

impl From<inner::MailType> for PyMailType {
    fn from(v: inner::MailType) -> Self {
        match v {
            inner::MailType::BEGIN => Self::BEGIN,
            inner::MailType::END => Self::END,
            inner::MailType::FAIL => Self::FAIL,
            inner::MailType::REQUEUE => Self::REQUEUE,
            inner::MailType::ALL => Self::ALL,
        }
    }
}

impl From<PyMailType> for inner::MailType {
    fn from(v: PyMailType) -> Self {
        match v {
            PyMailType::BEGIN => Self::BEGIN,
            PyMailType::END => Self::END,
            PyMailType::FAIL => Self::FAIL,
            PyMailType::REQUEUE => Self::REQUEUE,
            PyMailType::ALL => Self::ALL,
        }
    }
}

// ------------------------------------------------------------- MailTypeInput
#[gen_stub_pyclass]
#[pyclass(
    name = "MailTypeInput",
    module = "gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options",
    from_py_object,
    eq,
    hash,
    frozen
)]
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PyMailTypeInput(pub inner::MailTypeInput);

#[gen_stub_pymethods]
#[pymethods]
impl PyMailTypeInput {
    /// Build from a non-empty list of `MailType` variants. Empty list raises.
    #[new]
    fn new(types: Vec<PyMailType>) -> PyResult<Self> {
        if types.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "MailTypeInput requires at least one MailType",
            ));
        }
        let s = types
            .into_iter()
            .map(|t| match inner::MailType::from(t) {
                inner::MailType::BEGIN => "BEGIN",
                inner::MailType::END => "END",
                inner::MailType::FAIL => "FAIL",
                inner::MailType::REQUEUE => "REQUEUE",
                inner::MailType::ALL => "ALL",
            })
            .collect::<Vec<_>>()
            .join(",");
        inner::MailTypeInput::try_from(s)
            .map(Self)
            .map_err(Into::into)
    }

    /// Parse from the comma-separated Slurm form (`"BEGIN,END"`).
    #[staticmethod]
    fn parse(s: String) -> PyResult<Self> {
        inner::MailTypeInput::try_from(s)
            .map(Self)
            .map_err(Into::into)
    }

    fn __repr__(&self) -> String {
        format!("MailTypeInput({:?})", self.0)
    }
}

impl From<inner::MailTypeInput> for PyMailTypeInput {
    fn from(v: inner::MailTypeInput) -> Self {
        Self(v)
    }
}

impl From<PyMailTypeInput> for inner::MailTypeInput {
    fn from(v: PyMailTypeInput) -> Self {
        v.0
    }
}

// ----------------------------------------------------------- SlurmJobConfig
#[gen_stub_pyclass]
#[pyclass(
    name = "SlurmJobConfig",
    module = "gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options",
    from_py_object
)]
#[derive(Clone)]
pub struct PySlurmJobConfig(pub inner::SlurmJobConfig);

#[gen_stub_pymethods]
#[pymethods]
impl PySlurmJobConfig {
    #[new]
    #[pyo3(signature = (
        partition,
        time_limit=None,
        log_stdout=None,
        log_stderr=None,
        comment=None,
        job_name=None,
        array_spec=None,
        dependency=None,
        mail_user=None,
        mail_types=None,
        resource_spec=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        partition: String,
        time_limit: Option<PyJobTimeLimit>,
        log_stdout: Option<PathBuf>,
        log_stderr: Option<PathBuf>,
        comment: Option<String>,
        job_name: Option<String>,
        array_spec: Option<PySlurmArraySpec>,
        dependency: Option<PySlurmDependency>,
        mail_user: Option<String>,
        mail_types: Option<PyMailTypeInput>,
        resource_spec: Option<PyResourceSpec>,
    ) -> Self {
        Self(inner::SlurmJobConfig {
            partition,
            time_limit: time_limit.map(|v| v.0),
            log_stdout,
            log_stderr,
            comment,
            job_name,
            array_spec: array_spec.map(|v| v.0),
            dependency: dependency.map(|v| v.0),
            mail_user,
            mail_types: mail_types.map(|v| v.0),
            resource_spec: resource_spec.map(|v| v.0),
        })
    }

    #[getter]
    fn partition(&self) -> String {
        self.0.partition.clone()
    }

    #[setter]
    fn set_partition(&mut self, v: String) {
        self.0.partition = v;
    }

    #[getter]
    fn time_limit(&self) -> Option<PyJobTimeLimit> {
        self.0.time_limit.map(PyJobTimeLimit)
    }

    #[setter]
    fn set_time_limit(&mut self, v: Option<PyJobTimeLimit>) {
        self.0.time_limit = v.map(|v| v.0);
    }

    #[getter]
    fn log_stdout(&self) -> Option<PathBuf> {
        self.0.log_stdout.clone()
    }

    #[setter]
    fn set_log_stdout(&mut self, v: Option<PathBuf>) {
        self.0.log_stdout = v;
    }

    #[getter]
    fn log_stderr(&self) -> Option<PathBuf> {
        self.0.log_stderr.clone()
    }

    #[setter]
    fn set_log_stderr(&mut self, v: Option<PathBuf>) {
        self.0.log_stderr = v;
    }

    #[getter]
    fn comment(&self) -> Option<String> {
        self.0.comment.clone()
    }

    #[setter]
    fn set_comment(&mut self, v: Option<String>) {
        self.0.comment = v;
    }

    #[getter]
    fn job_name(&self) -> Option<String> {
        self.0.job_name.clone()
    }

    #[setter]
    fn set_job_name(&mut self, v: Option<String>) {
        self.0.job_name = v;
    }

    #[getter]
    fn array_spec(&self) -> Option<PySlurmArraySpec> {
        self.0.array_spec.clone().map(PySlurmArraySpec)
    }

    #[setter]
    fn set_array_spec(&mut self, v: Option<PySlurmArraySpec>) {
        self.0.array_spec = v.map(|v| v.0);
    }

    #[getter]
    fn dependency(&self) -> Option<PySlurmDependency> {
        self.0.dependency.clone().map(PySlurmDependency)
    }

    #[setter]
    fn set_dependency(&mut self, v: Option<PySlurmDependency>) {
        self.0.dependency = v.map(|v| v.0);
    }

    #[getter]
    fn mail_user(&self) -> Option<String> {
        self.0.mail_user.clone()
    }

    #[setter]
    fn set_mail_user(&mut self, v: Option<String>) {
        self.0.mail_user = v;
    }

    #[getter]
    fn mail_types(&self) -> Option<PyMailTypeInput> {
        self.0.mail_types.clone().map(PyMailTypeInput)
    }

    #[setter]
    fn set_mail_types(&mut self, v: Option<PyMailTypeInput>) {
        self.0.mail_types = v.map(|v| v.0);
    }

    #[getter]
    fn resource_spec(&self) -> Option<PyResourceSpec> {
        self.0.resource_spec.clone().map(PyResourceSpec)
    }

    #[setter]
    fn set_resource_spec(&mut self, v: Option<PyResourceSpec>) {
        self.0.resource_spec = v.map(|v| v.0);
    }

    fn __repr__(&self) -> String {
        format!("SlurmJobConfig(partition={:?})", self.0.partition)
    }
}

impl From<inner::SlurmJobConfig> for PySlurmJobConfig {
    fn from(v: inner::SlurmJobConfig) -> Self {
        Self(v)
    }
}

impl From<PySlurmJobConfig> for inner::SlurmJobConfig {
    fn from(v: PySlurmJobConfig) -> Self {
        v.0
    }
}
