//! sbatch — fire-and-forget SLURM batch submission with KUDPC-aware polling.
//!
//! See `docs/superpowers/specs/2026-05-10-sbatch-module-design.md` for
//! the full design rationale (Approach A: passive handle + watch).

pub mod cmd;
pub mod error;
pub mod handle;
pub mod manager;
pub mod parse;
pub mod store;

#[cfg(test)]
pub(crate) mod test_util {
    //! Shared test fixtures for sbatch module tests.

    use std::sync::Arc;

    use anyhow::Result;

    use crate::dispatcher::{CaptureOutput, JobDispatcher};

    /// Wraps an `Arc<D: JobDispatcher>` so the wrapper can be moved into
    /// `dispatcher::into_dyn` while the caller keeps the original `Arc`
    /// to inspect recorded state after the manager runs (argv recordings,
    /// sacct call counts, etc.).
    ///
    /// Replaces the previously-duplicated `MoveDispatcher` /
    /// `MoveRecording` newtypes that lived in both `handle.rs` and
    /// `manager.rs` test modules.
    pub(crate) struct ArcDispatcher<D: JobDispatcher>(pub Arc<D>);

    impl<D: JobDispatcher> JobDispatcher for ArcDispatcher<D> {
        async fn run(&self, argv: &[String]) -> Result<i32> {
            self.0.run(argv).await
        }
        async fn capture(&self, argv: &[String]) -> Result<CaptureOutput> {
            self.0.capture(argv).await
        }
    }
}
