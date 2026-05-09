//! Lifecycle status of a Job, mirroring the SLURM scheduler's own
//! `(state, reason)` pair surfaced by `squeue %T %r` / `sacct %State %Reason`.
//!
//! - [`JobState`] — flat enum over the 24 official SLURM job state codes
//!   plus `Unknown` for forward-compat. See
//!   <https://slurm.schedmd.com/squeue.html#JOB_STATE_CODES>.
//! - [`JobReason`] — flat enum over the SLURM `slurm_reason_string` table
//!   (with `Other(String)` as a forward-compat escape hatch).
//! - [`JobStatus`] — `(state, reason)` pair, the Rust mirror of what a
//!   single `squeue` row reports.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

// ---------------------------------------------------------------- JobStatus

/// SLURM job status snapshot: state code paired with scheduler reason.
///
/// `state` mirrors the SLURM `%T` (job state) column;
/// `reason` mirrors the `%r` (reason) column.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobStatus {
    pub state: JobState,
    pub reason: JobReason,
}

impl JobStatus {
    /// Build a status with `reason = JobReason::None`.
    pub fn new(state: JobState) -> Self {
        Self {
            state,
            reason: JobReason::None,
        }
    }

    /// Build a status from `(state, reason)`.
    pub fn with_reason(state: JobState, reason: JobReason) -> Self {
        Self { state, reason }
    }
}

impl Default for JobStatus {
    fn default() -> Self {
        Self {
            state: JobState::Unknown,
            reason: JobReason::None,
        }
    }
}

// ----------------------------------------------------------------- JobState

/// SLURM job state code. Flat enum over the 24 official tokens plus
/// `Unknown` for forward-compat.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JobState {
    // ---- Queued / not progressing ----
    Pending,
    Configuring,
    Requeued,
    RequeueFed,
    RequeueHold,
    ResvDelHold,
    Suspended,
    Stopped,
    // ---- Alive / progressing ----
    Running,
    Completing,
    Resizing,
    Signaling,
    StageOut,
    // ---- Terminal success ----
    Completed,
    // ---- Terminal failure ----
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
    // ---- Sentinel ----
    Unknown,
}

impl JobState {
    /// Parse a raw `squeue` / `sacct` state token.
    ///
    /// Accepts SLURM long forms (`"PENDING"`, `"OUT_OF_MEMORY"`, …),
    /// SLURM compact codes (`"PD"`, `"OOM"`, …), and trailing context
    /// (`"CANCELLED by 1234"` — first whitespace-separated token wins).
    /// Matching is case-insensitive. Falls back to `Unknown`.
    pub fn parse(raw: &str) -> Self {
        let token = raw.split_whitespace().next().unwrap_or("");
        match token.to_ascii_uppercase().as_str() {
            "PENDING" | "PD" => Self::Pending,
            "CONFIGURING" | "CF" => Self::Configuring,
            "REQUEUED" | "RQ" => Self::Requeued,
            "REQUEUE_FED" | "RF" => Self::RequeueFed,
            "REQUEUE_HOLD" | "RH" => Self::RequeueHold,
            "RESV_DEL_HOLD" | "RD" => Self::ResvDelHold,
            "SUSPENDED" | "S" => Self::Suspended,
            "STOPPED" | "ST" => Self::Stopped,

            "RUNNING" | "R" => Self::Running,
            "COMPLETING" | "CG" => Self::Completing,
            "RESIZING" | "RS" => Self::Resizing,
            "SIGNALING" | "SI" => Self::Signaling,
            "STAGE_OUT" | "SO" => Self::StageOut,

            "COMPLETED" | "CD" => Self::Completed,

            "BOOT_FAIL" | "BF" => Self::BootFail,
            "CANCELLED" | "CA" => Self::Cancelled,
            "DEADLINE" | "DL" => Self::Deadline,
            "FAILED" | "F" => Self::Failed,
            "NODE_FAIL" | "NF" => Self::NodeFail,
            "OUT_OF_MEMORY" | "OOM" => Self::OutOfMemory,
            "PREEMPTED" | "PR" => Self::Preempted,
            "REVOKED" | "RV" => Self::Revoked,
            "SPECIAL_EXIT" | "SE" => Self::SpecialExit,
            "TIMEOUT" | "TO" => Self::Timeout,

            _ => Self::Unknown,
        }
    }

    /// SLURM long-form token (UPPERCASE, e.g. `"PENDING"`,
    /// `"OUT_OF_MEMORY"`). `parse(self.as_token())` round-trips for
    /// every variant.
    pub fn as_token(&self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Configuring => "CONFIGURING",
            Self::Requeued => "REQUEUED",
            Self::RequeueFed => "REQUEUE_FED",
            Self::RequeueHold => "REQUEUE_HOLD",
            Self::ResvDelHold => "RESV_DEL_HOLD",
            Self::Suspended => "SUSPENDED",
            Self::Stopped => "STOPPED",

            Self::Running => "RUNNING",
            Self::Completing => "COMPLETING",
            Self::Resizing => "RESIZING",
            Self::Signaling => "SIGNALING",
            Self::StageOut => "STAGE_OUT",

            Self::Completed => "COMPLETED",

            Self::BootFail => "BOOT_FAIL",
            Self::Cancelled => "CANCELLED",
            Self::Deadline => "DEADLINE",
            Self::Failed => "FAILED",
            Self::NodeFail => "NODE_FAIL",
            Self::OutOfMemory => "OUT_OF_MEMORY",
            Self::Preempted => "PREEMPTED",
            Self::Revoked => "REVOKED",
            Self::SpecialExit => "SPECIAL_EXIT",
            Self::Timeout => "TIMEOUT",

            Self::Unknown => "UNKNOWN",
        }
    }
}

impl Serialize for JobState {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_token())
    }
}

impl<'de> Deserialize<'de> for JobState {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Ok(Self::parse(&s))
    }
}

// ---------------------------------------------------------------- JobReason

/// Generates `JobReason` enum + `parse` + `as_str` from a single
/// `(VariantIdent => "SlurmString",)` table. Keeps the bidirectional
/// mapping in lockstep.
macro_rules! job_reason_codes {
    ($($variant:ident => $token:literal,)+) => {
        /// SLURM job reason code (the `%r` column from `squeue` /
        /// `sacct`). `None` means "no waiting reason"; unknown strings
        /// map to `Other(String)` for forward-compat.
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub enum JobReason {
            /// SLURM `"None"` — typically a running job, no waiting reason.
            None,
            $($variant,)+
            /// Unrecognized reason — preserves the raw scheduler string.
            Other(String),
        }

        impl JobReason {
            /// Parse a raw SLURM reason string (PascalCase from `squeue`).
            /// Empty / `"None"` map to [`Self::None`]; unrecognized strings
            /// map to [`Self::Other`].
            pub fn parse(raw: &str) -> Self {
                let trimmed = raw.trim();
                if trimmed.is_empty() || trimmed == "None" {
                    return Self::None;
                }
                match trimmed {
                    $($token => Self::$variant,)+
                    other => Self::Other(other.to_string()),
                }
            }

            /// Canonical SLURM reason string. `Other(s)` returns the
            /// stored raw string verbatim.
            pub fn as_str(&self) -> &str {
                match self {
                    Self::None => "None",
                    $(Self::$variant => $token,)+
                    Self::Other(s) => s.as_str(),
                }
            }

            /// Variant name for pattern-matching (e.g. `"Priority"`,
            /// `"None"`, `"Other"`). For known variants this matches
            /// `as_str`; for [`Self::Other`] it returns the literal
            /// `"Other"` (use `as_str` to get the raw string).
            pub fn variant_name(&self) -> &'static str {
                match self {
                    Self::None => "None",
                    $(Self::$variant => stringify!($variant),)+
                    Self::Other(_) => "Other",
                }
            }
        }
    };
}

job_reason_codes! {
    // Common waiting reasons
    Priority => "Priority",
    Dependency => "Dependency",
    DependencyNeverSatisfied => "DependencyNeverSatisfied",
    Resources => "Resources",
    Reservation => "Reservation",
    ResvDeleted => "ResvDeleted",
    BeginTime => "BeginTime",
    Licenses => "Licenses",
    NodeNotAvail => "ReqNodeNotAvail",
    JobHeldUser => "JobHeldUser",
    JobHeldAdmin => "JobHeldAdmin",
    BadConstraints => "BadConstraints",
    Cleaning => "Cleaning",
    Prolog => "Prolog",
    JobArrayTaskLimit => "JobArrayTaskLimit",
    BurstBufferResources => "BurstBufferResources",
    BurstBufferStageIn => "BurstBufferStageIn",
    NodeReboot => "NodeReboot",
    MaxRequeueExceeded => "MaxRequeueExceeded",
    PlannedReservation => "PlannedReservation",
    FedJobLock => "FedJobLock",
    OutOfMemory => "OutOfMemory",
    Preemption => "Preemption",
    WaitingForScheduling => "WaitingForScheduling",
    AdminHold => "AdminHold",

    // Partition
    PartitionDown => "PartitionDown",
    PartitionInactive => "PartitionInactive",
    PartitionNodeLimit => "PartitionNodeLimit",
    PartitionTimeLimit => "PartitionTimeLimit",
    PartitionConfig => "PartitionConfig",

    // Front-end
    FrontEndDown => "FrontEndDown",
    FrontEndUserDown => "FrontEndUserDown",
    DownNodes => "DownNodes",

    // Termination / failure reasons
    SystemFailure => "SystemFailure",
    JobLaunchFailure => "JobLaunchFailure",
    NonZeroExitCode => "NonZeroExitCode",
    RaisedSignal => "RaisedSignal",
    TimeLimit => "TimeLimit",
    InactiveLimit => "InactiveLimit",
    Deadline => "Deadline",

    // Account / QOS
    InvalidAccount => "InvalidAccount",
    InvalidQOS => "InvalidQOS",
    AccountNotAllowed => "AccountNotAllowed",
    QOSNotAllowed => "QOSNotAllowed",

    // QOS limits
    QOSUsageThreshold => "QOSUsageThreshold",
    QOSJobLimit => "QOSJobLimit",
    QOSResourceLimit => "QOSResourceLimit",
    QOSTimeLimit => "QOSTimeLimit",
    QOSGrpCpuLimit => "QOSGrpCpuLimit",
    QOSGrpCpuMinutesLimit => "QOSGrpCpuMinutesLimit",
    QOSGrpCpuRunMinutesLimit => "QOSGrpCpuRunMinutesLimit",
    QOSGrpJobsLimit => "QOSGrpJobsLimit",
    QOSGrpMemoryLimit => "QOSGrpMemoryLimit",
    QOSGrpNodeLimit => "QOSGrpNodeLimit",
    QOSGrpSubmitJobsLimit => "QOSGrpSubmitJobsLimit",
    QOSGrpWallLimit => "QOSGrpWallLimit",
    QOSMaxCpuMinutesPerJobLimit => "QOSMaxCpuMinutesPerJobLimit",
    QOSMaxCpuPerJobLimit => "QOSMaxCpuPerJobLimit",
    QOSMaxCpuPerNodeLimit => "QOSMaxCpuPerNodeLimit",
    QOSMaxCpuPerUserLimit => "QOSMaxCpuPerUserLimit",
    QOSMaxJobsPerAccountLimit => "QOSMaxJobsPerAccountLimit",
    QOSMaxJobsPerUserLimit => "QOSMaxJobsPerUserLimit",
    QOSMaxNodePerJobLimit => "QOSMaxNodePerJobLimit",
    QOSMaxNodePerUserLimit => "QOSMaxNodePerUserLimit",
    QOSMaxSubmitJobPerAccountLimit => "QOSMaxSubmitJobPerAccountLimit",
    QOSMaxSubmitJobPerUserLimit => "QOSMaxSubmitJobPerUserLimit",
    QOSMaxWallDurationPerJobLimit => "QOSMaxWallDurationPerJobLimit",
    QOSMinCpuNotSatisfied => "QOSMinCpuNotSatisfied",

    // Association limits
    AssociationJobLimit => "AssociationJobLimit",
    AssociationResourceLimit => "AssociationResourceLimit",
    AssociationTimeLimit => "AssociationTimeLimit",
    AssocGrpCpuLimit => "AssocGrpCpuLimit",
    AssocGrpCpuMinutesLimit => "AssocGrpCpuMinutesLimit",
    AssocGrpCpuRunMinutesLimit => "AssocGrpCpuRunMinutesLimit",
    AssocGrpJobsLimit => "AssocGrpJobsLimit",
    AssocGrpMemoryLimit => "AssocGrpMemoryLimit",
    AssocGrpNodeLimit => "AssocGrpNodeLimit",
    AssocGrpSubmitJobsLimit => "AssocGrpSubmitJobsLimit",
    AssocGrpWallLimit => "AssocGrpWallLimit",
    AssocMaxJobsLimit => "AssocMaxJobsLimit",
    AssocMaxCpuPerJobLimit => "AssocMaxCpuPerJobLimit",
    AssocMaxCpuMinutesPerJobLimit => "AssocMaxCpuMinutesPerJobLimit",
    AssocMaxNodePerJobLimit => "AssocMaxNodePerJobLimit",
    AssocMaxWallDurationPerJobLimit => "AssocMaxWallDurationPerJobLimit",
    AssocMaxSubmitJobLimit => "AssocMaxSubmitJobLimit",

    // Block (Bluegene legacy)
    BlockMaxError => "BlockMaxError",
    BlockFreeAction => "BlockFreeAction",
}

impl Serialize for JobReason {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for JobReason {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Ok(Self::parse(&s))
    }
}

// --------------------------------------------------------------------- tests

#[cfg(test)]
mod tests {
    use super::*;

    fn all_states() -> Vec<JobState> {
        vec![
            JobState::Pending,
            JobState::Configuring,
            JobState::Requeued,
            JobState::RequeueFed,
            JobState::RequeueHold,
            JobState::ResvDelHold,
            JobState::Suspended,
            JobState::Stopped,
            JobState::Running,
            JobState::Completing,
            JobState::Resizing,
            JobState::Signaling,
            JobState::StageOut,
            JobState::Completed,
            JobState::BootFail,
            JobState::Cancelled,
            JobState::Deadline,
            JobState::Failed,
            JobState::NodeFail,
            JobState::OutOfMemory,
            JobState::Preempted,
            JobState::Revoked,
            JobState::SpecialExit,
            JobState::Timeout,
            JobState::Unknown,
        ]
    }

    // ---- JobState::parse ----
    #[test]
    fn job_state_parse_long_forms() {
        assert_eq!(JobState::parse("PENDING"), JobState::Pending);
        assert_eq!(JobState::parse("RUNNING"), JobState::Running);
        assert_eq!(JobState::parse("COMPLETED"), JobState::Completed);
        assert_eq!(JobState::parse("OUT_OF_MEMORY"), JobState::OutOfMemory);
        assert_eq!(JobState::parse("SPECIAL_EXIT"), JobState::SpecialExit);
    }

    #[test]
    fn job_state_parse_compact_covers_every_variant() {
        for (code, expected) in [
            ("PD", JobState::Pending),
            ("CF", JobState::Configuring),
            ("RQ", JobState::Requeued),
            ("RF", JobState::RequeueFed),
            ("RH", JobState::RequeueHold),
            ("RD", JobState::ResvDelHold),
            ("S", JobState::Suspended),
            ("ST", JobState::Stopped),
            ("R", JobState::Running),
            ("CG", JobState::Completing),
            ("RS", JobState::Resizing),
            ("SI", JobState::Signaling),
            ("SO", JobState::StageOut),
            ("CD", JobState::Completed),
            ("BF", JobState::BootFail),
            ("CA", JobState::Cancelled),
            ("DL", JobState::Deadline),
            ("F", JobState::Failed),
            ("NF", JobState::NodeFail),
            ("OOM", JobState::OutOfMemory),
            ("PR", JobState::Preempted),
            ("RV", JobState::Revoked),
            ("SE", JobState::SpecialExit),
            ("TO", JobState::Timeout),
        ] {
            assert_eq!(
                JobState::parse(code),
                expected,
                "compact code {code:?} did not parse as expected",
            );
        }
    }

    #[test]
    fn job_state_parse_trailing_context() {
        assert_eq!(JobState::parse("CANCELLED by 1234"), JobState::Cancelled);
    }

    #[test]
    fn job_state_parse_padding_and_case() {
        assert_eq!(JobState::parse("  RUNNING  "), JobState::Running);
        assert_eq!(JobState::parse("pending"), JobState::Pending);
        assert_eq!(JobState::parse("Running"), JobState::Running);
    }

    #[test]
    fn job_state_parse_unknown() {
        assert_eq!(JobState::parse(""), JobState::Unknown);
        assert_eq!(JobState::parse("   "), JobState::Unknown);
        assert_eq!(JobState::parse("FOO_BAR"), JobState::Unknown);
        // Legacy lowercase tokens are NOT mapped any more.
        assert_eq!(JobState::parse("queued"), JobState::Unknown);
        assert_eq!(JobState::parse("done"), JobState::Unknown);
        // "failed" matches "FAILED" via case-insensitive parse — that is
        // a coincidence of the SLURM-token name, not legacy compat.
        assert_eq!(JobState::parse("failed"), JobState::Failed);
    }

    #[test]
    fn job_state_as_token_round_trips() {
        for s in all_states() {
            assert_eq!(JobState::parse(s.as_token()), s, "round-trip for {s:?}");
        }
    }

    // ---- JobReason::parse ----
    #[test]
    fn job_reason_parse_known_variants() {
        assert_eq!(JobReason::parse("None"), JobReason::None);
        assert_eq!(JobReason::parse("Priority"), JobReason::Priority);
        assert_eq!(JobReason::parse("Resources"), JobReason::Resources);
        assert_eq!(JobReason::parse("Dependency"), JobReason::Dependency);
        assert_eq!(JobReason::parse("BeginTime"), JobReason::BeginTime);
        assert_eq!(JobReason::parse("ReqNodeNotAvail"), JobReason::NodeNotAvail);
        assert_eq!(JobReason::parse("TimeLimit"), JobReason::TimeLimit);
        assert_eq!(JobReason::parse("OutOfMemory"), JobReason::OutOfMemory);
        assert_eq!(
            JobReason::parse("AssocGrpCpuLimit"),
            JobReason::AssocGrpCpuLimit
        );
    }

    #[test]
    fn job_reason_parse_empty_is_none() {
        assert_eq!(JobReason::parse(""), JobReason::None);
        assert_eq!(JobReason::parse("   "), JobReason::None);
    }

    #[test]
    fn job_reason_parse_unknown_falls_back_to_other() {
        assert_eq!(
            JobReason::parse("MysteryNewSlurmReason"),
            JobReason::Other("MysteryNewSlurmReason".to_string())
        );
    }

    #[test]
    fn job_reason_parse_is_case_sensitive() {
        // SLURM emits canonical PascalCase — anything else is `Other`.
        assert_eq!(
            JobReason::parse("priority"),
            JobReason::Other("priority".to_string())
        );
        assert_eq!(
            JobReason::parse("PRIORITY"),
            JobReason::Other("PRIORITY".to_string())
        );
    }

    #[test]
    fn job_reason_as_str_round_trip() {
        for r in [
            JobReason::None,
            JobReason::Priority,
            JobReason::Dependency,
            JobReason::Resources,
            JobReason::TimeLimit,
            JobReason::OutOfMemory,
            JobReason::QOSGrpCpuLimit,
            JobReason::AssocMaxJobsLimit,
            JobReason::Other("CustomThing".to_string()),
        ] {
            assert_eq!(JobReason::parse(r.as_str()), r, "round-trip for {r:?}");
        }
    }

    #[test]
    fn job_reason_variant_name() {
        assert_eq!(JobReason::None.variant_name(), "None");
        assert_eq!(JobReason::Priority.variant_name(), "Priority");
        assert_eq!(
            JobReason::Other("anything".to_string()).variant_name(),
            "Other"
        );
    }

    // ---- JobStatus serde ----
    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Holder {
        status: JobStatus,
    }

    #[test]
    fn job_status_default_is_unknown_none() {
        let s = JobStatus::default();
        assert_eq!(s.state, JobState::Unknown);
        assert_eq!(s.reason, JobReason::None);
    }

    #[test]
    fn job_status_constructors() {
        let s = JobStatus::new(JobState::Pending);
        assert_eq!(s.state, JobState::Pending);
        assert_eq!(s.reason, JobReason::None);

        let s = JobStatus::with_reason(JobState::Pending, JobReason::Priority);
        assert_eq!(s.state, JobState::Pending);
        assert_eq!(s.reason, JobReason::Priority);
    }

    #[test]
    fn job_status_toml_round_trip_running_none() {
        let h = Holder {
            status: JobStatus::new(JobState::Running),
        };
        let s = toml::to_string(&h).unwrap();
        let back: Holder = toml::from_str(&s).unwrap();
        assert_eq!(back, h);
    }

    #[test]
    fn job_status_toml_round_trip_pending_priority() {
        let h = Holder {
            status: JobStatus::with_reason(JobState::Pending, JobReason::Priority),
        };
        let s = toml::to_string(&h).unwrap();
        assert!(s.contains(r#"state = "PENDING""#), "actual TOML: {s}");
        assert!(s.contains(r#"reason = "Priority""#), "actual TOML: {s}");
        let back: Holder = toml::from_str(&s).unwrap();
        assert_eq!(back, h);
    }

    #[test]
    fn job_status_toml_round_trip_failed_oom() {
        let h = Holder {
            status: JobStatus::with_reason(JobState::OutOfMemory, JobReason::OutOfMemory),
        };
        let s = toml::to_string(&h).unwrap();
        let back: Holder = toml::from_str(&s).unwrap();
        assert_eq!(back, h);
    }

    #[test]
    fn job_status_toml_round_trip_with_other_reason() {
        let h = Holder {
            status: JobStatus::with_reason(
                JobState::Pending,
                JobReason::Other("BrandNewReasonFromFutureSlurm".to_string()),
            ),
        };
        let s = toml::to_string(&h).unwrap();
        let back: Holder = toml::from_str(&s).unwrap();
        assert_eq!(back, h);
    }

    #[test]
    fn job_status_toml_serializes_state_uppercase_reason_pascalcase() {
        let h = Holder {
            status: JobStatus::with_reason(JobState::Cancelled, JobReason::JobHeldUser),
        };
        let s = toml::to_string(&h).unwrap();
        assert!(s.contains(r#"state = "CANCELLED""#), "actual TOML: {s}");
        assert!(s.contains(r#"reason = "JobHeldUser""#), "actual TOML: {s}");
    }
}
