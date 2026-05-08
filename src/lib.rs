#[cfg(feature = "pyo3")]
pub mod py_export;
#[cfg(feature = "pyo3")]
pub use py_export::stub_info;

pub mod manager;
pub mod runner;

// Re-export the SLURM lifecycle types so downstream Rust crates can write
// `use slurm_async_runner::{JobStatus, JobState, JobReason}` without taking
// a direct `gaussian_job_shared` dependency. Python users continue to import
// from `gaussian_job_shared._core.entities.slurm.status`.
pub use gaussian_job_shared::entities::slurm::status::{JobReason, JobState, JobStatus};

// Re-export the async batch-query so callers can write
// `use slurm_async_runner::query_job_states_batch`.
pub use runner::query_job_states_batch;

// Re-export the manager + launcher so callers can write
// `use slurm_async_runner::{SlurmCmd, SlurmManager}`.
pub use manager::{SlurmCmd, SlurmManager};

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
