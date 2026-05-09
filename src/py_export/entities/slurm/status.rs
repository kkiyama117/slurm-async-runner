//! PyO3 wrappers for `entities::slurm::status::*`.
//!
//! - [`PyJobState`] — flat pyenum mirroring [`crate::entities::slurm::status::JobState`].
//! - [`PyJobReason`] — newtype around [`crate::entities::slurm::status::JobReason`]
//!   exposing common-variant constructors plus `parse` / `other` escape
//!   hatches (Python cannot represent a `String`-bearing variant as a
//!   flat pyenum).
//! - [`PyJobStatus`] — newtype around [`crate::entities::slurm::status::JobStatus`]
//!   exposing `state` and `reason` as get fields.

use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_enum, gen_stub_pymethods};

use crate::entities::slurm::status as inner;

// ----------------------------------------------------------------- JobState

#[gen_stub_pyclass_enum]
#[pyclass(
    name = "JobState",
    module = "gaussian_job_shared._gaussian_job_shared_core.entities.slurm.status",
    from_py_object,
    eq,
    eq_int,
    hash,
    frozen
)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum PyJobState {
    Pending,
    Configuring,
    Requeued,
    RequeueFed,
    RequeueHold,
    ResvDelHold,
    Suspended,
    Stopped,
    Running,
    Completing,
    Resizing,
    Signaling,
    StageOut,
    Completed,
    BootFail,
    Cancelled,
    Deadline,
    Failed,
    NodeFail,
    OutOfMemory,
    Preempted,
    Revoked,
    SpecialExit,
    Timeout,
    Unknown,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyJobState {
    /// Parse a raw SLURM state token (long form, compact code, trailing
    /// context, or any case). Falls back to `Unknown`.
    #[staticmethod]
    fn parse(raw: &str) -> Self {
        inner::JobState::parse(raw).into()
    }

    /// SLURM long-form token, e.g. `"PENDING"`, `"OUT_OF_MEMORY"`.
    #[getter]
    fn token(&self) -> &'static str {
        inner::JobState::from(*self).as_token()
    }

    fn __str__(&self) -> &'static str {
        inner::JobState::from(*self).as_token()
    }

    fn __repr__(&self) -> String {
        format!("JobState.{:?}", self)
    }
}

impl From<inner::JobState> for PyJobState {
    fn from(v: inner::JobState) -> Self {
        match v {
            inner::JobState::Pending => Self::Pending,
            inner::JobState::Configuring => Self::Configuring,
            inner::JobState::Requeued => Self::Requeued,
            inner::JobState::RequeueFed => Self::RequeueFed,
            inner::JobState::RequeueHold => Self::RequeueHold,
            inner::JobState::ResvDelHold => Self::ResvDelHold,
            inner::JobState::Suspended => Self::Suspended,
            inner::JobState::Stopped => Self::Stopped,
            inner::JobState::Running => Self::Running,
            inner::JobState::Completing => Self::Completing,
            inner::JobState::Resizing => Self::Resizing,
            inner::JobState::Signaling => Self::Signaling,
            inner::JobState::StageOut => Self::StageOut,
            inner::JobState::Completed => Self::Completed,
            inner::JobState::BootFail => Self::BootFail,
            inner::JobState::Cancelled => Self::Cancelled,
            inner::JobState::Deadline => Self::Deadline,
            inner::JobState::Failed => Self::Failed,
            inner::JobState::NodeFail => Self::NodeFail,
            inner::JobState::OutOfMemory => Self::OutOfMemory,
            inner::JobState::Preempted => Self::Preempted,
            inner::JobState::Revoked => Self::Revoked,
            inner::JobState::SpecialExit => Self::SpecialExit,
            inner::JobState::Timeout => Self::Timeout,
            inner::JobState::Unknown => Self::Unknown,
        }
    }
}

impl From<PyJobState> for inner::JobState {
    fn from(v: PyJobState) -> Self {
        match v {
            PyJobState::Pending => Self::Pending,
            PyJobState::Configuring => Self::Configuring,
            PyJobState::Requeued => Self::Requeued,
            PyJobState::RequeueFed => Self::RequeueFed,
            PyJobState::RequeueHold => Self::RequeueHold,
            PyJobState::ResvDelHold => Self::ResvDelHold,
            PyJobState::Suspended => Self::Suspended,
            PyJobState::Stopped => Self::Stopped,
            PyJobState::Running => Self::Running,
            PyJobState::Completing => Self::Completing,
            PyJobState::Resizing => Self::Resizing,
            PyJobState::Signaling => Self::Signaling,
            PyJobState::StageOut => Self::StageOut,
            PyJobState::Completed => Self::Completed,
            PyJobState::BootFail => Self::BootFail,
            PyJobState::Cancelled => Self::Cancelled,
            PyJobState::Deadline => Self::Deadline,
            PyJobState::Failed => Self::Failed,
            PyJobState::NodeFail => Self::NodeFail,
            PyJobState::OutOfMemory => Self::OutOfMemory,
            PyJobState::Preempted => Self::Preempted,
            PyJobState::Revoked => Self::Revoked,
            PyJobState::SpecialExit => Self::SpecialExit,
            PyJobState::Timeout => Self::Timeout,
            PyJobState::Unknown => Self::Unknown,
        }
    }
}

// ---------------------------------------------------------------- JobReason

/// Wraps [`inner::JobReason`]. Construct common variants via class
/// methods (`none()`, `priority()`, …), supply an arbitrary SLURM
/// reason string via `parse(raw)`, or attach an unknown raw string with
/// `other(raw)`.
///
/// Inspect with `name` (variant identifier — `"None"`, `"Priority"`, or
/// `"Other"` for an unrecognized reason), `value` (the canonical SLURM
/// string for known variants, or the raw stored string for `Other`),
/// or `__str__` (same as `value`).
#[gen_stub_pyclass]
#[pyclass(
    name = "JobReason",
    module = "gaussian_job_shared._gaussian_job_shared_core.entities.slurm.status",
    from_py_object,
    eq,
    hash,
    frozen
)]
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct PyJobReason(pub inner::JobReason);

#[gen_stub_pymethods]
#[pymethods]
impl PyJobReason {
    /// SLURM `"None"` — typically a running job, no waiting reason.
    #[staticmethod]
    fn none() -> Self {
        Self(inner::JobReason::None)
    }

    /// `"Priority"` — waiting on a higher-priority job.
    #[staticmethod]
    fn priority() -> Self {
        Self(inner::JobReason::Priority)
    }

    /// `"Resources"` — waiting on resource availability.
    #[staticmethod]
    fn resources() -> Self {
        Self(inner::JobReason::Resources)
    }

    /// `"Dependency"` — waiting on a parent job to finish.
    #[staticmethod]
    fn dependency() -> Self {
        Self(inner::JobReason::Dependency)
    }

    /// `"BeginTime"` — held until the job's `--begin` time.
    #[staticmethod]
    fn begin_time() -> Self {
        Self(inner::JobReason::BeginTime)
    }

    /// `"TimeLimit"` — terminated for hitting wall-time.
    #[staticmethod]
    fn time_limit() -> Self {
        Self(inner::JobReason::TimeLimit)
    }

    /// `"OutOfMemory"` — terminated by an OOM kill.
    #[staticmethod]
    fn out_of_memory() -> Self {
        Self(inner::JobReason::OutOfMemory)
    }

    /// `"NonZeroExitCode"` — terminated by a non-zero exit.
    #[staticmethod]
    fn non_zero_exit_code() -> Self {
        Self(inner::JobReason::NonZeroExitCode)
    }

    /// `"JobHeldUser"` — held by user (`scontrol hold`).
    #[staticmethod]
    fn job_held_user() -> Self {
        Self(inner::JobReason::JobHeldUser)
    }

    /// `"JobHeldAdmin"` — held by admin.
    #[staticmethod]
    fn job_held_admin() -> Self {
        Self(inner::JobReason::JobHeldAdmin)
    }

    /// Parse any SLURM reason string. Empty / `"None"` → `None`;
    /// known PascalCase strings → matching variant; anything else →
    /// `Other(raw)`.
    #[staticmethod]
    fn parse(raw: &str) -> Self {
        Self(inner::JobReason::parse(raw))
    }

    /// Construct an explicit `Other(raw)` variant. The raw string is
    /// stored verbatim, even if it matches a known canonical name —
    /// use `parse` if you want auto-routing to the matching variant.
    #[staticmethod]
    fn other(raw: String) -> Self {
        Self(inner::JobReason::Other(raw))
    }

    /// Variant name. `"None"`, `"Priority"`, `"Resources"`, `…`, or
    /// `"Other"` (use `value` to get the raw string of an `Other`).
    #[getter]
    fn name(&self) -> &'static str {
        self.0.variant_name()
    }

    /// Canonical SLURM reason string (`"None"`, `"Priority"`, …). For
    /// `Other(raw)` returns the stored raw string verbatim.
    #[getter]
    fn value(&self) -> String {
        self.0.as_str().to_string()
    }

    fn __str__(&self) -> String {
        self.0.as_str().to_string()
    }

    fn __repr__(&self) -> String {
        match &self.0 {
            inner::JobReason::Other(s) => format!("JobReason.other({s:?})"),
            other => format!("JobReason.{}", other.variant_name()),
        }
    }
}

impl From<inner::JobReason> for PyJobReason {
    fn from(v: inner::JobReason) -> Self {
        Self(v)
    }
}

impl From<PyJobReason> for inner::JobReason {
    fn from(v: PyJobReason) -> Self {
        v.0
    }
}

// ---------------------------------------------------------------- JobStatus

/// Wraps [`inner::JobStatus`] — a `(state, reason)` pair mirroring one
/// `squeue %T %r` row. Build with `JobStatus(state, reason)` (reason
/// defaults to `JobReason.none()`), or via the convenience constructor
/// `JobStatus.parse(state_token, reason_str)`.
#[gen_stub_pyclass]
#[pyclass(
    name = "JobStatus",
    module = "gaussian_job_shared._gaussian_job_shared_core.entities.slurm.status",
    from_py_object,
    eq,
    hash,
    frozen
)]
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct PyJobStatus(pub inner::JobStatus);

#[gen_stub_pymethods]
#[pymethods]
impl PyJobStatus {
    #[new]
    #[pyo3(signature = (state, reason=PyJobReason(inner::JobReason::None)))]
    fn new(state: PyJobState, reason: PyJobReason) -> Self {
        Self(inner::JobStatus {
            state: state.into(),
            reason: reason.0,
        })
    }

    /// Build a status by parsing the `(state, reason)` pair as raw
    /// SLURM strings (e.g. `JobStatus.parse("PD", "Priority")`).
    #[staticmethod]
    #[pyo3(signature = (state, reason=""))]
    fn parse(state: &str, reason: &str) -> Self {
        Self(inner::JobStatus {
            state: inner::JobState::parse(state),
            reason: inner::JobReason::parse(reason),
        })
    }

    #[getter]
    fn state(&self) -> PyJobState {
        self.0.state.into()
    }

    #[getter]
    fn reason(&self) -> PyJobReason {
        PyJobReason(self.0.reason.clone())
    }

    fn __str__(&self) -> String {
        format!("{} ({})", self.0.state.as_token(), self.0.reason.as_str())
    }

    fn __repr__(&self) -> String {
        format!(
            "JobStatus(state=JobState.{:?}, reason={})",
            self.0.state,
            PyJobReason(self.0.reason.clone()).__repr__(),
        )
    }
}

impl From<inner::JobStatus> for PyJobStatus {
    fn from(v: inner::JobStatus) -> Self {
        Self(v)
    }
}

impl From<PyJobStatus> for inner::JobStatus {
    fn from(v: PyJobStatus) -> Self {
        v.0
    }
}

// ----------------------------------------------------- Python sub-module glue

#[pymodule(name = "status")]
pub(crate) mod inner_module {
    use super::*;

    const PYTHON_MODULE_NAME: &str =
        "gaussian_job_shared._gaussian_job_shared_core.entities.slurm.status";

    #[pymodule_export]
    use super::{PyJobReason, PyJobState, PyJobStatus};

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
