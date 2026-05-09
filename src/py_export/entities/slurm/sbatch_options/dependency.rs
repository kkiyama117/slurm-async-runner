//! PyO3 wrappers for `entities::slurm::dependency::*`.

use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_enum, gen_stub_pymethods};

use crate::entities::slurm as inner;

// ---------------------------------------------------------- DependencyType
#[gen_stub_pyclass_enum]
#[pyclass(
    name = "DependencyType",
    module = "gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options",
    from_py_object,
    eq,
    eq_int,
    hash,
    frozen
)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum PyDependencyType {
    After,
    AfterAny,
    AfterBurstBuffer,
    AfterCorr,
    AfterNotOk,
    AfterOk,
    Singleton,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyDependencyType {
    fn __str__(&self) -> &'static str {
        inner::DependencyType::from(*self).as_keyword()
    }

    fn __repr__(&self) -> String {
        format!("DependencyType.{:?}", self)
    }
}

impl From<inner::DependencyType> for PyDependencyType {
    fn from(v: inner::DependencyType) -> Self {
        match v {
            inner::DependencyType::After => Self::After,
            inner::DependencyType::AfterAny => Self::AfterAny,
            inner::DependencyType::AfterBurstBuffer => Self::AfterBurstBuffer,
            inner::DependencyType::AfterCorr => Self::AfterCorr,
            inner::DependencyType::AfterNotOk => Self::AfterNotOk,
            inner::DependencyType::AfterOk => Self::AfterOk,
            inner::DependencyType::Singleton => Self::Singleton,
        }
    }
}

impl From<PyDependencyType> for inner::DependencyType {
    fn from(v: PyDependencyType) -> Self {
        match v {
            PyDependencyType::After => Self::After,
            PyDependencyType::AfterAny => Self::AfterAny,
            PyDependencyType::AfterBurstBuffer => Self::AfterBurstBuffer,
            PyDependencyType::AfterCorr => Self::AfterCorr,
            PyDependencyType::AfterNotOk => Self::AfterNotOk,
            PyDependencyType::AfterOk => Self::AfterOk,
            PyDependencyType::Singleton => Self::Singleton,
        }
    }
}

// ----------------------------------------------------------- DependencyJoin
#[gen_stub_pyclass_enum]
#[pyclass(
    name = "DependencyJoin",
    module = "gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options",
    from_py_object,
    eq,
    eq_int,
    hash,
    frozen
)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum PyDependencyJoin {
    /// Comma-separated — every clause must be satisfied.
    And,
    /// `?`-separated — any one clause being satisfied releases the job.
    Or,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyDependencyJoin {
    fn __str__(&self) -> &'static str {
        match self {
            Self::And => ",",
            Self::Or => "?",
        }
    }

    fn __repr__(&self) -> String {
        format!("DependencyJoin.{:?}", self)
    }
}

impl From<inner::DependencyJoin> for PyDependencyJoin {
    fn from(v: inner::DependencyJoin) -> Self {
        match v {
            inner::DependencyJoin::And => Self::And,
            inner::DependencyJoin::Or => Self::Or,
        }
    }
}

impl From<PyDependencyJoin> for inner::DependencyJoin {
    fn from(v: PyDependencyJoin) -> Self {
        match v {
            PyDependencyJoin::And => Self::And,
            PyDependencyJoin::Or => Self::Or,
        }
    }
}

// ---------------------------------------------------------- DependencyJobRef
#[gen_stub_pyclass]
#[pyclass(
    name = "DependencyJobRef",
    module = "gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options",
    from_py_object,
    eq,
    hash,
    frozen
)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PyDependencyJobRef(pub inner::DependencyJobRef);

#[gen_stub_pymethods]
#[pymethods]
impl PyDependencyJobRef {
    #[new]
    #[pyo3(signature = (job_id, delay_minutes=None))]
    fn new(job_id: u32, delay_minutes: Option<u32>) -> Self {
        Self(inner::DependencyJobRef {
            job_id,
            delay_minutes,
        })
    }

    #[getter]
    fn job_id(&self) -> u32 {
        self.0.job_id
    }

    #[getter]
    fn delay_minutes(&self) -> Option<u32> {
        self.0.delay_minutes
    }

    fn __str__(&self) -> String {
        self.0.to_string()
    }

    fn __repr__(&self) -> String {
        format!(
            "DependencyJobRef(job_id={}, delay_minutes={:?})",
            self.0.job_id, self.0.delay_minutes
        )
    }
}

impl From<inner::DependencyJobRef> for PyDependencyJobRef {
    fn from(v: inner::DependencyJobRef) -> Self {
        Self(v)
    }
}

impl From<PyDependencyJobRef> for inner::DependencyJobRef {
    fn from(v: PyDependencyJobRef) -> Self {
        v.0
    }
}

// ---------------------------------------------------------- DependencyClause
#[gen_stub_pyclass]
#[pyclass(
    name = "DependencyClause",
    module = "gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options",
    from_py_object,
    eq
)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyDependencyClause(pub inner::DependencyClause);

#[gen_stub_pymethods]
#[pymethods]
impl PyDependencyClause {
    #[new]
    #[pyo3(signature = (dep_type, job_refs=Vec::new()))]
    fn new(dep_type: PyDependencyType, job_refs: Vec<PyDependencyJobRef>) -> Self {
        Self(inner::DependencyClause {
            dep_type: dep_type.into(),
            job_refs: job_refs.into_iter().map(|r| r.0).collect(),
        })
    }

    #[getter]
    fn dep_type(&self) -> PyDependencyType {
        self.0.dep_type.into()
    }

    #[setter]
    fn set_dep_type(&mut self, v: PyDependencyType) {
        self.0.dep_type = v.into();
    }

    #[getter]
    fn job_refs(&self) -> Vec<PyDependencyJobRef> {
        self.0
            .job_refs
            .iter()
            .copied()
            .map(PyDependencyJobRef)
            .collect()
    }

    #[setter]
    fn set_job_refs(&mut self, v: Vec<PyDependencyJobRef>) {
        self.0.job_refs = v.into_iter().map(|r| r.0).collect();
    }

    fn __str__(&self) -> String {
        self.0.to_string()
    }

    fn __repr__(&self) -> String {
        format!("DependencyClause({})", self.0)
    }
}

impl From<inner::DependencyClause> for PyDependencyClause {
    fn from(v: inner::DependencyClause) -> Self {
        Self(v)
    }
}

impl From<PyDependencyClause> for inner::DependencyClause {
    fn from(v: PyDependencyClause) -> Self {
        v.0
    }
}

// ---------------------------------------------------------- SlurmDependency
#[gen_stub_pyclass]
#[pyclass(
    name = "SlurmDependency",
    module = "gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options",
    from_py_object,
    eq
)]
#[derive(Clone, PartialEq, Eq)]
pub struct PySlurmDependency(pub inner::SlurmDependency);

#[gen_stub_pymethods]
#[pymethods]
impl PySlurmDependency {
    /// Parse a Slurm `--dependency` spec string, e.g. `"afterok:200"`.
    #[new]
    fn new(s: &str) -> PyResult<Self> {
        s.parse::<inner::SlurmDependency>()
            .map(Self)
            .map_err(Into::into)
    }

    #[staticmethod]
    fn parse(s: &str) -> PyResult<Self> {
        Self::new(s)
    }

    /// Build directly from clauses + join.
    #[staticmethod]
    #[pyo3(signature = (clauses, join=PyDependencyJoin::And))]
    fn from_clauses(clauses: Vec<PyDependencyClause>, join: PyDependencyJoin) -> Self {
        Self(inner::SlurmDependency {
            clauses: clauses.into_iter().map(|c| c.0).collect(),
            join: join.into(),
        })
    }

    #[getter]
    fn clauses(&self) -> Vec<PyDependencyClause> {
        self.0
            .clauses
            .iter()
            .cloned()
            .map(PyDependencyClause)
            .collect()
    }

    #[setter]
    fn set_clauses(&mut self, v: Vec<PyDependencyClause>) {
        self.0.clauses = v.into_iter().map(|c| c.0).collect();
    }

    #[getter]
    fn join(&self) -> PyDependencyJoin {
        self.0.join.into()
    }

    #[setter]
    fn set_join(&mut self, v: PyDependencyJoin) {
        self.0.join = v.into();
    }

    fn __str__(&self) -> String {
        self.0.to_string()
    }

    fn __repr__(&self) -> String {
        format!("SlurmDependency({:?})", self.0.to_string())
    }
}

impl From<inner::SlurmDependency> for PySlurmDependency {
    fn from(v: inner::SlurmDependency) -> Self {
        Self(v)
    }
}

impl From<PySlurmDependency> for inner::SlurmDependency {
    fn from(v: PySlurmDependency) -> Self {
        v.0
    }
}
