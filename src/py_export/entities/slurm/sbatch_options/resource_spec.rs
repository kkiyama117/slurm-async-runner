//! PyO3 wrappers for `entities::slurm::resource_spec::*`.

use std::num::NonZeroU32;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_enum, gen_stub_pymethods};

use crate::entities::slurm as inner;

// --------------------------------------------------------------- MemoryUnit
#[gen_stub_pyclass_enum]
#[pyclass(
    name = "MemoryUnit",
    module = "gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options",
    from_py_object,
    eq,
    eq_int,
    hash,
    frozen
)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum PyMemoryUnit {
    Kilo,
    Mega,
    Giga,
    Tera,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyMemoryUnit {
    fn __str__(&self) -> &'static str {
        match self {
            Self::Kilo => "K",
            Self::Mega => "M",
            Self::Giga => "G",
            Self::Tera => "T",
        }
    }

    fn __repr__(&self) -> String {
        format!("MemoryUnit.{:?}", self)
    }
}

impl From<inner::MemoryUnit> for PyMemoryUnit {
    fn from(v: inner::MemoryUnit) -> Self {
        match v {
            inner::MemoryUnit::Kilo => Self::Kilo,
            inner::MemoryUnit::Mega => Self::Mega,
            inner::MemoryUnit::Giga => Self::Giga,
            inner::MemoryUnit::Tera => Self::Tera,
        }
    }
}

impl From<PyMemoryUnit> for inner::MemoryUnit {
    fn from(v: PyMemoryUnit) -> Self {
        match v {
            PyMemoryUnit::Kilo => Self::Kilo,
            PyMemoryUnit::Mega => Self::Mega,
            PyMemoryUnit::Giga => Self::Giga,
            PyMemoryUnit::Tera => Self::Tera,
        }
    }
}

// ------------------------------------------------------------------- Memory
#[gen_stub_pyclass]
#[pyclass(
    name = "Memory",
    module = "gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options",
    from_py_object,
    eq,
    hash,
    frozen
)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PyMemory(pub inner::Memory);

#[gen_stub_pymethods]
#[pymethods]
impl PyMemory {
    /// Parse a Slurm memory token, e.g. `"8G"` (or unit-less `"1024"` =
    /// mebibytes per Slurm `--mem` default).
    #[new]
    fn new(s: &str) -> PyResult<Self> {
        s.parse::<inner::Memory>().map(Self).map_err(Into::into)
    }

    #[staticmethod]
    fn parse(s: &str) -> PyResult<Self> {
        Self::new(s)
    }

    #[staticmethod]
    fn from_value(value: u32, unit: PyMemoryUnit) -> PyResult<Self> {
        let value = NonZeroU32::new(value)
            .ok_or_else(|| PyValueError::new_err("memory value must be > 0"))?;
        Ok(Self(inner::Memory {
            value,
            unit: unit.into(),
        }))
    }

    #[getter]
    fn value(&self) -> u32 {
        self.0.value.get()
    }

    #[getter]
    fn unit(&self) -> PyMemoryUnit {
        self.0.unit.into()
    }

    fn __str__(&self) -> String {
        self.0.to_string()
    }

    fn __repr__(&self) -> String {
        format!("Memory({:?})", self.0.to_string())
    }
}

impl From<inner::Memory> for PyMemory {
    fn from(v: inner::Memory) -> Self {
        Self(v)
    }
}

impl From<PyMemory> for inner::Memory {
    fn from(v: PyMemory) -> Self {
        v.0
    }
}

// ------------------------------------------------------------ ResourceSpecCPU
#[gen_stub_pyclass]
#[pyclass(
    name = "ResourceSpecCPU",
    module = "gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options",
    from_py_object,
    eq,
    frozen
)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyResourceSpecCPU(pub inner::ResourceSpecCPU);

#[gen_stub_pymethods]
#[pymethods]
impl PyResourceSpecCPU {
    /// Construct a fully-specified CPU resource spec — all four of
    /// (`p`, `t`, `c`, `m`) are required positional arguments.
    /// For partial specs (e.g. `p=60:t=1:c=1` per the KUDPC manual),
    /// use [`PyResourceSpec`]'s positional/kwargs constructor instead.
    #[new]
    fn new(p: u32, t: u32, c: u32, m: PyMemory) -> PyResult<Self> {
        let p = NonZeroU32::new(p).ok_or_else(|| PyValueError::new_err("p must be > 0"))?;
        let t = NonZeroU32::new(t).ok_or_else(|| PyValueError::new_err("t must be > 0"))?;
        let c = NonZeroU32::new(c).ok_or_else(|| PyValueError::new_err("c must be > 0"))?;
        Ok(Self(inner::ResourceSpecCPU {
            p: Some(p),
            t: Some(t),
            c: Some(c),
            m: Some(m.0),
        }))
    }

    /// Returns `None` if `p` was not specified.
    #[getter]
    fn p(&self) -> Option<u32> {
        self.0.p.map(NonZeroU32::get)
    }

    /// Returns `None` if `t` was not specified.
    #[getter]
    fn t(&self) -> Option<u32> {
        self.0.t.map(NonZeroU32::get)
    }

    /// Returns `None` if `c` was not specified.
    #[getter]
    fn c(&self) -> Option<u32> {
        self.0.c.map(NonZeroU32::get)
    }

    /// Returns `None` if `m` was not specified.
    #[getter]
    fn m(&self) -> Option<PyMemory> {
        self.0.m.map(PyMemory)
    }

    fn __repr__(&self) -> String {
        // Render unset fields as `None` so the repr round-trips
        // visually with the relaxed Option<...> shape.
        let p = self.0.p.map(NonZeroU32::get);
        let t = self.0.t.map(NonZeroU32::get);
        let c = self.0.c.map(NonZeroU32::get);
        let m = self.0.m.map(|m| m.to_string());
        format!("ResourceSpecCPU(p={p:?}, t={t:?}, c={c:?}, m={m:?})")
    }
}

impl From<inner::ResourceSpecCPU> for PyResourceSpecCPU {
    fn from(v: inner::ResourceSpecCPU) -> Self {
        Self(v)
    }
}

impl From<PyResourceSpecCPU> for inner::ResourceSpecCPU {
    fn from(v: PyResourceSpecCPU) -> Self {
        v.0
    }
}

// ------------------------------------------------------------ ResourceSpecGPU
#[gen_stub_pyclass]
#[pyclass(
    name = "ResourceSpecGPU",
    module = "gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options",
    from_py_object,
    eq,
    frozen
)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyResourceSpecGPU(pub inner::ResourceSpecGPU);

#[gen_stub_pymethods]
#[pymethods]
impl PyResourceSpecGPU {
    #[new]
    fn new(g: u32) -> PyResult<Self> {
        let g = NonZeroU32::new(g).ok_or_else(|| PyValueError::new_err("g must be > 0"))?;
        Ok(Self(inner::ResourceSpecGPU { g }))
    }

    #[getter]
    fn g(&self) -> u32 {
        self.0.g.get()
    }

    fn __repr__(&self) -> String {
        format!("ResourceSpecGPU(g={})", self.0.g.get())
    }
}

impl From<inner::ResourceSpecGPU> for PyResourceSpecGPU {
    fn from(v: inner::ResourceSpecGPU) -> Self {
        Self(v)
    }
}

impl From<PyResourceSpecGPU> for inner::ResourceSpecGPU {
    fn from(v: PyResourceSpecGPU) -> Self {
        v.0
    }
}

// --------------------------------------------------------------- ResourceSpec
#[gen_stub_pyclass]
#[pyclass(
    name = "ResourceSpec",
    module = "gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options",
    from_py_object,
    eq,
    frozen
)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyResourceSpec(pub inner::ResourceSpec);

#[gen_stub_pymethods]
#[pymethods]
impl PyResourceSpec {
    /// Build a `ResourceSpec` from individual KUDPC `--rsc` keys.
    ///
    /// All keyword arguments are optional. CPU keys
    /// (`processes`, `threads`, `cores`, `memory`) and the GPU key
    /// (`gpus`) are mutually exclusive — passing any of the former
    /// together with the latter raises `ValueError`. Each integer
    /// key must be `>= 1`. The `memory` parameter must be a
    /// [`PyMemory`] instance — wrap a string with `Memory("2G")` or
    /// `Memory.from_value(2, MemoryUnit.Giga)` first.
    #[new]
    #[pyo3(signature = (
        processes = None, threads = None, cores = None,
        memory = None, gpus = None,
    ))]
    fn new(
        processes: Option<u32>,
        threads: Option<u32>,
        cores: Option<u32>,
        memory: Option<PyMemory>,
        gpus: Option<u32>,
    ) -> PyResult<Self> {
        let to_nz = |v: u32, key: &'static str| {
            NonZeroU32::new(v)
                .ok_or_else(|| PyValueError::new_err(format!("ResourceSpec/{key} must be > 0")))
        };
        let p = processes.map(|v| to_nz(v, "processes")).transpose()?;
        let t = threads.map(|v| to_nz(v, "threads")).transpose()?;
        let c = cores.map(|v| to_nz(v, "cores")).transpose()?;
        let m = memory.map(|pm| pm.0);
        let g = gpus.map(|v| to_nz(v, "gpus")).transpose()?;

        let cpu_keys_present = p.is_some() || t.is_some() || c.is_some() || m.is_some();
        match (cpu_keys_present, g) {
            (true, Some(_)) => Err(PyValueError::new_err(
                "CPU keys (processes/threads/cores/memory) and gpus \
                 are mutually exclusive — pass one group or the other",
            )),
            (false, Some(g)) => Ok(Self(inner::ResourceSpec::GPU(inner::ResourceSpecGPU { g }))),
            (true, None) => Ok(Self(inner::ResourceSpec::CPU(inner::ResourceSpecCPU {
                p,
                t,
                c,
                m,
            }))),
            // No CPU keys, no GPU — the all-None CPU is intentionally valid.
            (false, None) => Ok(Self(inner::ResourceSpec::CPU(
                inner::ResourceSpecCPU::default(),
            ))),
        }
    }

    /// Parse a Slurm `--rsc` spec, e.g. `"p=4:t=8:c=8:m=8G"` or `"g=1"`.
    #[staticmethod]
    fn from_str(s: &str) -> PyResult<Self> {
        s.parse::<inner::ResourceSpec>()
            .map(Self)
            .map_err(Into::into)
    }

    /// Backwards-compatible alias for [`Self::from_str`].
    #[staticmethod]
    fn parse(s: &str) -> PyResult<Self> {
        Self::from_str(s)
    }

    #[staticmethod]
    fn cpu(spec: PyResourceSpecCPU) -> Self {
        Self(inner::ResourceSpec::CPU(spec.0))
    }

    #[staticmethod]
    fn gpu(spec: PyResourceSpecGPU) -> Self {
        Self(inner::ResourceSpec::GPU(spec.0))
    }

    /// `"cpu"` or `"gpu"`.
    #[getter]
    fn kind(&self) -> &'static str {
        match self.0 {
            inner::ResourceSpec::CPU(_) => "cpu",
            inner::ResourceSpec::GPU(_) => "gpu",
        }
    }

    #[getter]
    fn cpu_spec(&self) -> Option<PyResourceSpecCPU> {
        match &self.0 {
            inner::ResourceSpec::CPU(c) => Some(PyResourceSpecCPU(c.clone())),
            _ => None,
        }
    }

    #[getter]
    fn gpu_spec(&self) -> Option<PyResourceSpecGPU> {
        match &self.0 {
            inner::ResourceSpec::GPU(g) => Some(PyResourceSpecGPU(g.clone())),
            _ => None,
        }
    }

    fn __str__(&self) -> String {
        self.0.to_string()
    }

    fn __repr__(&self) -> String {
        format!("ResourceSpec({:?})", self.0.to_string())
    }
}

impl From<inner::ResourceSpec> for PyResourceSpec {
    fn from(v: inner::ResourceSpec) -> Self {
        Self(v)
    }
}

impl From<PyResourceSpec> for inner::ResourceSpec {
    fn from(v: PyResourceSpec) -> Self {
        v.0
    }
}
