//! PyO3 wrapper for `entities::slurm::time_limit::JobTimeLimit`.

use std::num::NonZeroU32;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

use crate::entities::slurm as inner;

#[gen_stub_pyclass]
#[pyclass(
    name = "JobTimeLimit",
    module = "gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options",
    from_py_object,
    eq,
    hash,
    frozen
)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PyJobTimeLimit(pub inner::JobTimeLimit);

#[gen_stub_pymethods]
#[pymethods]
impl PyJobTimeLimit {
    /// Parse a Slurm `--time` spec string, e.g. `"01:00:00"` or `"3-12:00:00"`.
    #[new]
    fn new(s: &str) -> PyResult<Self> {
        s.parse::<inner::JobTimeLimit>()
            .map(Self)
            .map_err(Into::into)
    }

    #[staticmethod]
    fn parse(s: &str) -> PyResult<Self> {
        Self::new(s)
    }

    #[staticmethod]
    fn from_seconds(seconds: u32) -> PyResult<Self> {
        let nz =
            NonZeroU32::new(seconds).ok_or_else(|| PyValueError::new_err("seconds must be > 0"))?;
        Ok(Self(inner::JobTimeLimit::from_seconds(nz)))
    }

    #[getter]
    fn total_seconds(&self) -> u32 {
        self.0.total_seconds()
    }

    #[getter]
    fn hours(&self) -> u32 {
        self.0.hours()
    }

    #[getter]
    fn minutes(&self) -> u32 {
        self.0.minutes()
    }

    #[getter]
    fn seconds_part(&self) -> u32 {
        self.0.seconds_part()
    }

    fn __str__(&self) -> String {
        self.0.to_string()
    }

    fn __repr__(&self) -> String {
        format!("JobTimeLimit({:?})", self.0.to_string())
    }
}

impl From<inner::JobTimeLimit> for PyJobTimeLimit {
    fn from(v: inner::JobTimeLimit) -> Self {
        Self(v)
    }
}

impl From<PyJobTimeLimit> for inner::JobTimeLimit {
    fn from(v: PyJobTimeLimit) -> Self {
        v.0
    }
}
