//! Async dispatcher for the rendered SLURM batch script.
//!
//! Ported from the original Python `SlurmManager`/`SlurmCmd`, then
//! split along two axes:
//!
//! - **Cmd spec vs. execution.** [`SlurmCmd`] only describes the
//!   command (which launcher binary, which batch file). Actual
//!   subprocess work is delegated to a [`crate::dispatcher::JobDispatcher`].
//! - **Language-/runtime-agnostic spec vs. Rust-runtime impl.** The
//!   spec types here are pure data; the Rust-runtime impls
//!   ([`crate::dispatcher::TokioDispatcher`], etc.) live in
//!   `dispatcher.rs`. Other languages (or test harnesses) plug their
//!   own dispatcher in via the trait.
//!
//! No more shell wrapping (`$SHELL -c "..."`) — both Python's
//! `asyncio.create_subprocess_exec` and Rust's `tokio::process::Command`
//! accept argv directly, so we never go through the shell.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};

use crate::JobStatus;
use crate::dispatcher::{DryRunDispatcher, JobDispatcher, TokioDispatcher};
use crate::runner;

// ----------------------------------------------------------------- SlurmCmd

/// The launcher binary used to submit the rendered batch script.
///
/// Defaults to `srun` for production. Tests can pass a different
/// command (e.g. `"bash"` or `"true"`) without going through SLURM.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SlurmCmd {
    pub srun_cmd: String,
}

impl Default for SlurmCmd {
    fn default() -> Self {
        Self {
            srun_cmd: "srun".to_string(),
        }
    }
}

impl SlurmCmd {
    /// Construct with an explicit launcher binary.
    pub fn new(srun_cmd: impl Into<String>) -> Self {
        Self {
            srun_cmd: srun_cmd.into(),
        }
    }

    /// Build the argv that would dispatch `batch_file`.
    ///
    /// The path is resolved to absolute form via `std::path::absolute`
    /// (matches Python `Path.absolute()` — no symlink resolution, no
    /// existence requirement).
    pub fn build_argv(&self, batch_file: &Path) -> Result<Vec<String>> {
        let abs = std::path::absolute(batch_file)
            .with_context(|| format!("failed to absolutize {}", batch_file.display()))?;
        let abs_str = abs
            .into_os_string()
            .into_string()
            .map_err(|os| anyhow::anyhow!("non-UTF8 batch path: {os:?}"))?;
        Ok(vec![self.srun_cmd.clone(), abs_str])
    }
}

// -------------------------------------------------------------- SlurmManager

/// High-level coordinator: holds the [`SlurmCmd`] and dispatches via a
/// [`JobDispatcher`]. Spec-only — no I/O, no subprocess, no env probing.
#[derive(Debug, Clone, Default)]
pub struct SlurmManager {
    pub slurm_cmd: SlurmCmd,
}

impl SlurmManager {
    /// Construct a manager. Pass `None` for `slurm_cmd` to default to
    /// `srun`.
    pub fn new(slurm_cmd: Option<SlurmCmd>) -> Self {
        Self {
            slurm_cmd: slurm_cmd.unwrap_or_default(),
        }
    }

    /// Build the argv that would dispatch `batch_file`. Delegates to
    /// [`SlurmCmd::build_argv`].
    pub fn build_argv(&self, batch_file: &Path) -> Result<Vec<String>> {
        self.slurm_cmd.build_argv(batch_file)
    }

    /// Dispatch `batch_file` via the configured launcher. Returns the
    /// child exit code.
    ///
    /// `dry_run = true` runs through [`DryRunDispatcher`] (echoes argv
    /// without spawning); `false` uses [`TokioDispatcher`]. For a custom
    /// dispatcher, call [`Self::run_job_with`].
    pub async fn run_job(&self, batch_file: &Path, dry_run: bool) -> Result<i32> {
        if dry_run {
            self.run_job_with(&DryRunDispatcher, batch_file).await
        } else {
            self.run_job_with(&TokioDispatcher, batch_file).await
        }
    }

    /// Dispatch `batch_file` via an explicit dispatcher.
    pub async fn run_job_with<D: JobDispatcher>(
        &self,
        dispatcher: &D,
        batch_file: &Path,
    ) -> Result<i32> {
        let argv = self.build_argv(batch_file)?;
        dispatcher.run(&argv).await
    }

    /// Single-jobid form of [`Self::query_job_states_batch`].
    ///
    /// Returns a default `JobStatus` (state=Unknown, reason=None) when
    /// neither `squeue` nor `sacct` reports the id.
    pub async fn query_job_state(&self, jobid: u64) -> Result<JobStatus> {
        let states = self.query_job_states_batch(&[jobid]).await?;
        Ok(states.get(&jobid).cloned().unwrap_or_default())
    }

    /// Bulk-query SLURM for the `(state, reason)` of every input id.
    /// Uses [`TokioDispatcher`]; for a custom dispatcher use
    /// [`runner::query_job_states_batch_with`].
    pub async fn query_job_states_batch(&self, jobids: &[u64]) -> Result<HashMap<u64, JobStatus>> {
        runner::query_job_states_batch(jobids).await
    }
}

// --------------------------------------------------------------------- tests

#[cfg(test)]
mod tests {
    use super::*;

    // ---- SlurmCmd ----

    #[test]
    fn slurm_cmd_default_is_srun() {
        assert_eq!(SlurmCmd::default().srun_cmd, "srun");
    }

    #[test]
    fn slurm_cmd_new_overrides_launcher() {
        assert_eq!(SlurmCmd::new("bash").srun_cmd, "bash");
    }

    #[test]
    fn build_argv_uses_absolute_path() {
        let c = SlurmCmd::default();
        let argv = c.build_argv(Path::new("/tmp/example.sh")).unwrap();
        assert_eq!(
            argv,
            vec!["srun".to_string(), "/tmp/example.sh".to_string()]
        );
    }

    #[test]
    fn build_argv_relative_path_becomes_absolute() {
        let c = SlurmCmd::default();
        let cwd = std::env::current_dir().unwrap();
        let argv = c.build_argv(Path::new("foo.sh")).unwrap();
        assert_eq!(argv[0], "srun");
        assert_eq!(argv[1], format!("{}/foo.sh", cwd.display()));
    }

    // ---- SlurmManager ----

    #[test]
    fn manager_default_uses_srun() {
        assert_eq!(SlurmManager::default().slurm_cmd.srun_cmd, "srun");
    }

    #[test]
    fn manager_new_with_explicit_cmd() {
        let m = SlurmManager::new(Some(SlurmCmd::new("bash")));
        assert_eq!(m.slurm_cmd.srun_cmd, "bash");
    }

    #[test]
    fn manager_build_argv_delegates() {
        let m = SlurmManager::default();
        let argv = m.build_argv(Path::new("/tmp/x.sh")).unwrap();
        assert_eq!(argv, vec!["srun".to_string(), "/tmp/x.sh".to_string()]);
    }

    // ---- run_job ----

    #[tokio::test]
    async fn run_job_dry_run_returns_zero() {
        let m = SlurmManager::default();
        let code = m.run_job(Path::new("/tmp/dummy.sh"), true).await.unwrap();
        assert_eq!(code, 0);
    }

    #[tokio::test]
    async fn run_job_with_true_binary_returns_zero() {
        let m = SlurmManager::new(Some(SlurmCmd::new("true")));
        let code = m.run_job(Path::new("/tmp/dummy.sh"), false).await.unwrap();
        assert_eq!(code, 0);
    }

    #[tokio::test]
    async fn run_job_with_false_binary_returns_one() {
        let m = SlurmManager::new(Some(SlurmCmd::new("false")));
        let code = m.run_job(Path::new("/tmp/dummy.sh"), false).await.unwrap();
        assert_eq!(code, 1);
    }

    #[tokio::test]
    async fn run_job_with_custom_dispatcher_records_argv() {
        // Mock dispatcher to verify argv plumbing without spawning.
        use std::sync::Mutex;
        struct Mock {
            received: Mutex<Vec<Vec<String>>>,
        }
        impl JobDispatcher for Mock {
            async fn run(&self, argv: &[String]) -> Result<i32> {
                self.received.lock().unwrap().push(argv.to_vec());
                Ok(42)
            }
            async fn capture(&self, _argv: &[String]) -> Result<(i32, String)> {
                unimplemented!()
            }
        }
        let mock = Mock {
            received: Mutex::new(vec![]),
        };
        let m = SlurmManager::default();
        let code = m.run_job_with(&mock, Path::new("/tmp/x.sh")).await.unwrap();
        assert_eq!(code, 42);
        assert_eq!(
            mock.received.lock().unwrap()[0],
            vec!["srun".to_string(), "/tmp/x.sh".to_string()]
        );
    }
}
