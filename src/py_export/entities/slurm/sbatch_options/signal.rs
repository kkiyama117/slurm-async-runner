//! PyO3 wrappers for `entities::slurm::sbatch_options::signal::*`.

use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

use crate::entities::slurm::sbatch_options::signal as inner;

#[gen_stub_pyclass]
#[pyclass(
    name = "SlurmSignalSpec",
    module = "slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options",
    from_py_object,
    eq
)]
#[derive(Clone, PartialEq, Eq)]
pub struct PySlurmSignalSpec(pub inner::SlurmSignalSpec);

#[gen_stub_pymethods]
#[pymethods]
impl PySlurmSignalSpec {
    /// Parse a Slurm `--signal` spec string, e.g. `"USR1@60"` or `"R:SIGTERM@30"`.
    #[new]
    fn new(s: &str) -> PyResult<Self> {
        s.parse::<inner::SlurmSignalSpec>()
            .map(Self)
            .map_err(Into::into)
    }

    #[staticmethod]
    fn parse(s: &str) -> PyResult<Self> {
        Self::new(s)
    }

    #[getter]
    fn allow_resignal(&self) -> bool {
        self.0.allow_resignal
    }

    #[getter]
    fn signal(&self) -> String {
        // Render the inner SignalIdent as its Display form, so Python sees
        // a uniform string rather than an opaque enum. Round-trips via
        // SlurmSignalSpec.parse.
        self.0.signal.to_string()
    }

    #[getter]
    fn seconds_before_end(&self) -> Option<u16> {
        self.0.seconds_before_end
    }

    fn __str__(&self) -> String {
        self.0.to_string()
    }

    fn __repr__(&self) -> String {
        format!("SlurmSignalSpec({:?})", self.0.to_string())
    }
}

impl From<inner::SlurmSignalSpec> for PySlurmSignalSpec {
    fn from(v: inner::SlurmSignalSpec) -> Self {
        Self(v)
    }
}

impl From<PySlurmSignalSpec> for inner::SlurmSignalSpec {
    fn from(v: PySlurmSignalSpec) -> Self {
        v.0
    }
}
