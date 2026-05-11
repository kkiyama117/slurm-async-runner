pub mod entities;
pub mod error;
pub mod store;
// Crate-internal utilities — not part of the public API.
mod util;

#[cfg(feature = "pyo3")]
pub mod py_export;
#[cfg(feature = "pyo3")]
pub use py_export::stub_info;

pub mod dispatcher;
pub mod handle;
pub mod manager;
pub mod runner;
pub mod sbatch;
pub mod tssrun;

// Re-export the SLURM lifecycle types from the crate-local entities module.
pub use crate::entities::slurm::status::{JobReason, JobState, JobStatus};

// Re-export the async batch-query so callers can write
// `use slurm_async_runner::query_job_states_batch`.
pub use runner::{
    JobOutcome, query_job_states_batch, query_job_states_batch_with,
    query_job_states_with_exit_code_with,
};

// Re-export the manager + launcher so callers can write
// `use slurm_async_runner::{SlurmCmd, SlurmManager}`.
pub use manager::{SlurmCmd, SlurmManager};

// Re-export the dispatcher abstraction so callers can plug in custom
// runtime implementations or use the shipped Tokio / dry-run flavors.
pub use dispatcher::{
    DryRunDispatcher, DynDispatcherAdapter, DynJobDispatcher, DynView, JobDispatcher,
    TokioDispatcher, into_dyn,
};

// Re-export background dispatcher types.
pub use dispatcher::{BackgroundDispatcher, SpawnedChild, TokioBackgroundDispatcher};

// Re-export tssrun public API.
pub use tssrun::cmd::TssrunCmd;
// Re-export the SLURM resource/partition/time-limit vocabulary from the crate-local entities module.
pub use crate::entities::slurm::{
    JobPartition, JobTimeLimit, Memory, MemoryUnit, ResourceSpec, ResourceSpecCPU, ResourceSpecGPU,
};
pub use tssrun::handle::{FinishedInfo, LogLocations, TssrunJobHandle, TssrunJobSnapshot};
// Deprecated re-exports (Phase 3 P1 rename); remove next major.
#[allow(deprecated)]
pub use tssrun::handle::{JobHandle, JobHandleSnapshot};
pub use tssrun::log::{
    FileLogSink, InMemoryLogSink, JobLogSink, LogStream, NullLogSink, StdLogSink,
};
pub use tssrun::manager::{AttachKey, TssrunManager};
pub use tssrun::parse::{parse_salloc_jobid, parse_salloc_node};

// sbatch module re-exports
pub use sbatch::cmd::SbatchCmd;
pub use sbatch::error::SbatchSpawnError;
pub use sbatch::handle::{
    FinishedInfo as SbatchFinishedInfo, LogPathSpec, LogReadError as SbatchLogReadError,
    LogStream as SbatchLogStream, SbatchAttachKey, SbatchJobHandle, SbatchJobSnapshot,
    SbatchLifecycle,
};
pub use sbatch::manager::SbatchManager;
pub use sbatch::parse::{parse_submitted_jobid, resolve_log_path};

// Generic store re-exports (replaces tssrun-specific ones)
pub use store::{FileSystemStateStore, InMemoryStateStore, JobSnapshot, JobStateStore};

// Phase 3 P3: cross-backend handle trait.
// Phase 3 P4: dyn-safe companion + type-erasure adapter. The
// `handle::into_dyn` free function is intentionally NOT re-exported at
// the crate root because `dispatcher::into_dyn` already lives there;
// callers should write `slurm_async_runner::handle::into_dyn(h)` or
// `use slurm_async_runner::handle::into_dyn as into_dyn_handle;`.
pub use handle::{DynHandleAdapter, DynJobHandleCommon, JobHandleCommon};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reexports_resolve_and_parse_from_slurm_tokens() {
        // The whole point of S3'' — proves crate-root paths resolve and that
        // upstream's `parse` flows are reachable through them.
        assert_eq!(JobState::parse("PD"), JobState::Pending);
        assert_eq!(JobState::parse("OUT_OF_MEMORY"), JobState::OutOfMemory);

        let r = JobReason::parse("Priority");
        assert_eq!(r, JobReason::Priority);

        let s = JobStatus::with_reason(JobState::Pending, JobReason::Priority);
        assert_eq!(s.state, JobState::Pending);
        assert_eq!(s.reason, JobReason::Priority);

        // Unknown / forward-compat fallbacks survive the re-export.
        assert_eq!(JobState::parse("FUTURE_TOKEN"), JobState::Unknown);
        assert!(matches!(
            JobReason::parse("FutureReason"),
            JobReason::Other(ref s) if s == "FutureReason"
        ));
    }
}
