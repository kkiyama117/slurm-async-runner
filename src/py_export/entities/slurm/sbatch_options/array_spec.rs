//! PyO3 wrappers for `entities::slurm::array_spec::*`.

use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

use crate::entities::slurm as inner;

// ----------------------------------------------------------------- ArrayIndex
/// Wraps the [`ArrayIndex`] sum type. Construct one of the three variants
/// via the `single`/`range`/`stepped` classmethods. Inspect via `kind` plus
/// the matching `value`/`start`/`end`/`step` accessors.
#[gen_stub_pyclass]
#[pyclass(
    name = "ArrayIndex",
    module = "gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options",
    from_py_object,
    eq,
    frozen
)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyArrayIndex(pub inner::ArrayIndex);

#[gen_stub_pymethods]
#[pymethods]
impl PyArrayIndex {
    /// Single-index entry, e.g. `5`.
    #[staticmethod]
    fn single(i: u32) -> Self {
        Self(inner::ArrayIndex::Single(i))
    }

    /// Inclusive range entry, e.g. `0-15`.
    #[staticmethod]
    fn range(start: u32, end: u32) -> Self {
        Self(inner::ArrayIndex::Range { start, end })
    }

    /// Inclusive range with step, e.g. `0-15:4` (= 0, 4, 8, 12).
    #[staticmethod]
    fn stepped(start: u32, end: u32, step: u32) -> Self {
        Self(inner::ArrayIndex::Stepped { start, end, step })
    }

    /// Discriminant: `"single"`, `"range"`, or `"stepped"`.
    #[getter]
    fn kind(&self) -> &'static str {
        match self.0 {
            inner::ArrayIndex::Single(_) => "single",
            inner::ArrayIndex::Range { .. } => "range",
            inner::ArrayIndex::Stepped { .. } => "stepped",
        }
    }

    /// `Some(i)` for `single`, else `None`.
    #[getter]
    fn value(&self) -> Option<u32> {
        match self.0 {
            inner::ArrayIndex::Single(i) => Some(i),
            _ => None,
        }
    }

    /// `Some(start)` for `range`/`stepped`, else `None`.
    #[getter]
    fn start(&self) -> Option<u32> {
        match self.0 {
            inner::ArrayIndex::Range { start, .. } | inner::ArrayIndex::Stepped { start, .. } => {
                Some(start)
            }
            _ => None,
        }
    }

    /// `Some(end)` for `range`/`stepped`, else `None`.
    #[getter]
    fn end(&self) -> Option<u32> {
        match self.0 {
            inner::ArrayIndex::Range { end, .. } | inner::ArrayIndex::Stepped { end, .. } => {
                Some(end)
            }
            _ => None,
        }
    }

    /// `Some(step)` for `stepped`, else `None`.
    #[getter]
    fn step(&self) -> Option<u32> {
        match self.0 {
            inner::ArrayIndex::Stepped { step, .. } => Some(step),
            _ => None,
        }
    }

    fn __str__(&self) -> String {
        self.0.to_string()
    }

    fn __repr__(&self) -> String {
        format!("ArrayIndex({:?})", self.0.to_string())
    }
}

impl From<inner::ArrayIndex> for PyArrayIndex {
    fn from(v: inner::ArrayIndex) -> Self {
        Self(v)
    }
}

impl From<PyArrayIndex> for inner::ArrayIndex {
    fn from(v: PyArrayIndex) -> Self {
        v.0
    }
}

// -------------------------------------------------------------- SlurmArraySpec
#[gen_stub_pyclass]
#[pyclass(
    name = "SlurmArraySpec",
    module = "gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options",
    from_py_object,
    eq
)]
#[derive(Clone, PartialEq, Eq)]
pub struct PySlurmArraySpec(pub inner::SlurmArraySpec);

#[gen_stub_pymethods]
#[pymethods]
impl PySlurmArraySpec {
    /// Parse a Slurm `--array` spec string, e.g. `"0-15:4%2"`.
    #[new]
    fn new(s: &str) -> PyResult<Self> {
        s.parse::<inner::SlurmArraySpec>()
            .map(Self)
            .map_err(Into::into)
    }

    #[staticmethod]
    fn parse(s: &str) -> PyResult<Self> {
        Self::new(s)
    }

    /// Build from an explicit list of entries plus an optional concurrency cap.
    #[staticmethod]
    #[pyo3(signature = (indices, max_concurrent=None))]
    fn from_indices(indices: Vec<PyArrayIndex>, max_concurrent: Option<u32>) -> Self {
        Self(inner::SlurmArraySpec {
            indices: indices.into_iter().map(|i| i.0).collect(),
            max_concurrent,
        })
    }

    #[getter]
    fn indices(&self) -> Vec<PyArrayIndex> {
        self.0.indices.iter().cloned().map(PyArrayIndex).collect()
    }

    #[setter]
    fn set_indices(&mut self, v: Vec<PyArrayIndex>) {
        self.0.indices = v.into_iter().map(|i| i.0).collect();
    }

    #[getter]
    fn max_concurrent(&self) -> Option<u32> {
        self.0.max_concurrent
    }

    #[setter]
    fn set_max_concurrent(&mut self, v: Option<u32>) {
        self.0.max_concurrent = v;
    }

    fn __str__(&self) -> String {
        self.0.to_string()
    }

    fn __repr__(&self) -> String {
        format!("SlurmArraySpec({:?})", self.0.to_string())
    }
}

impl From<inner::SlurmArraySpec> for PySlurmArraySpec {
    fn from(v: inner::SlurmArraySpec) -> Self {
        Self(v)
    }
}

impl From<PySlurmArraySpec> for inner::SlurmArraySpec {
    fn from(v: PySlurmArraySpec) -> Self {
        v.0
    }
}
