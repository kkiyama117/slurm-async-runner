//! Async dispatcher for the rendered SLURM batch script.
//!
//! Ported from the original Python `SlurmManager`/`SlurmCmd`. The
//! contract:
//!
//! - [`SlurmCmd`] names the launcher binary (`srun` in production; tests
//!   pass `"bash"` or `"true"` for dry runs against no real cluster).
//! - [`SlurmManager`] composes the shell string (`$SHELL -c "<inner>"`
//!   when `$SHELL` is set, otherwise the verbatim launcher invocation)
//!   and dispatches it via the system shell.
//!
//! The artifact actually submitted to SLURM is the bash file produced
//! by upstream rendering — this module only dispatches it.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context, Result};
use tokio::process::Command;

use crate::JobStatus;
use crate::runner;

// ----------------------------------------------------------------- SlurmCmd

/// The launcher binary used to submit the rendered batch script.
///
/// Defaults to `srun` for production. Pass a different command (e.g.
/// `"bash"` or `"true"`) for tests and local dry runs.
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
}

// -------------------------------------------------------------- SlurmManager

/// Compose and dispatch `<srun_cmd> <batch_file>` asynchronously.
///
/// `default_shell` mirrors `$SHELL` at construction time. When it is
/// `Some(...)`, the inner command is wrapped as `<shell> -c "<inner>"`
/// to mimic an interactive launch; when it is `None`, the inner command
/// is joined verbatim. Tests can mutate `default_shell` directly to
/// keep behavior deterministic regardless of the environment.
#[derive(Debug, Clone)]
pub struct SlurmManager {
    pub default_shell: Option<String>,
    pub slurm_cmd: SlurmCmd,
}

impl Default for SlurmManager {
    fn default() -> Self {
        Self::new(None)
    }
}

impl SlurmManager {
    /// Construct a manager. `default_shell` is read from `$SHELL` once
    /// at construction; pass `None` for `slurm_cmd` to default to `srun`.
    pub fn new(slurm_cmd: Option<SlurmCmd>) -> Self {
        Self {
            default_shell: std::env::var("SHELL").ok(),
            slurm_cmd: slurm_cmd.unwrap_or_default(),
        }
    }

    /// Join `cmd` into a single shell string. When `default_shell` is
    /// `Some(_)`, wrap the inner command as
    /// `<shell> -c " <inner> "` (matches the Python reference's
    /// space-padded inner quotes verbatim). When `None`, join verbatim.
    pub fn build_shell_str(&self, cmd: &[&str]) -> String {
        match &self.default_shell {
            Some(shell) => {
                let mut parts: Vec<&str> = Vec::with_capacity(cmd.len() + 4);
                parts.push(shell);
                parts.push("-c");
                parts.push("\"");
                parts.extend(cmd.iter().copied());
                parts.push("\"");
                parts.join(" ")
            }
            None => cmd.join(" "),
        }
    }

    /// Return the shell string that would dispatch `batch_file`, with
    /// the path resolved to absolute form (matches Python's
    /// `Path.absolute()` — no symlink resolution).
    pub fn srun_command(&self, batch_file: &Path) -> Result<String> {
        let abs = absolute_path(batch_file)
            .with_context(|| format!("failed to absolutize {}", batch_file.display()))?;
        let abs_str = abs.to_string_lossy().into_owned();
        Ok(self.build_shell_str(&[&self.slurm_cmd.srun_cmd, &abs_str]))
    }

    /// Dispatch `batch_file` via the configured launcher.
    ///
    /// `dry_run = true` prints the composed shell string and returns
    /// `0` without spawning a subprocess. Otherwise spawns via
    /// `/bin/sh -c <shell_str>` (matches Python's
    /// `asyncio.create_subprocess_shell`), echoes captured
    /// stdout/stderr, and returns the subprocess exit code (0 when
    /// signal-killed, matching the Python `proc.returncode or 0`
    /// fallback).
    pub async fn run_job(&self, batch_file: &Path, dry_run: bool) -> Result<i32> {
        let shell_str = self.srun_command(batch_file)?;
        if dry_run {
            println!("{shell_str}");
            return Ok(0);
        }

        let output = Command::new("sh")
            .arg("-c")
            .arg(&shell_str)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .with_context(|| format!("failed to spawn `{shell_str}`"))?;

        let code = output.status.code().unwrap_or(0);
        println!("[{shell_str:?} exited with {code}]");
        if !output.stdout.is_empty() {
            println!("[stdout]\n{}", String::from_utf8_lossy(&output.stdout));
        }
        if !output.stderr.is_empty() {
            println!("[stderr]\n{}", String::from_utf8_lossy(&output.stderr));
        }
        Ok(code)
    }

    /// Single-jobid form of [`Self::query_job_states_batch`].
    ///
    /// Convenience wrapper — issues at most one `squeue` and one
    /// `sacct` subprocess. Returns a default `JobStatus` (state=Unknown,
    /// reason=None) when neither command reports the id.
    pub async fn query_job_state(&self, jobid: u64) -> Result<JobStatus> {
        let states = self.query_job_states_batch(&[jobid]).await?;
        Ok(states.get(&jobid).cloned().unwrap_or_default())
    }

    /// Bulk-query SLURM for the `(state, reason)` of every input id.
    /// Delegates to [`crate::runner::query_job_states_batch`].
    pub async fn query_job_states_batch(
        &self,
        jobids: &[u64],
    ) -> Result<HashMap<u64, JobStatus>> {
        runner::query_job_states_batch(jobids).await
    }
}

// ---------------------------------------------------------------- helpers

/// Make `p` absolute without resolving symlinks (matches Python
/// `Path.absolute()`). `std::path::absolute` is stable since 1.79 and
/// is the right primitive for this — `canonicalize` would resolve
/// symlinks and require the file to exist, neither of which we want.
fn absolute_path(p: &Path) -> Result<PathBuf> {
    Ok(std::path::absolute(p)?)
}

// --------------------------------------------------------------------- tests

#[cfg(test)]
mod tests {
    use super::*;

    fn manager_with_shell(shell: Option<&str>, srun: &str) -> SlurmManager {
        SlurmManager {
            default_shell: shell.map(String::from),
            slurm_cmd: SlurmCmd::new(srun),
        }
    }

    // ---- SlurmCmd ----

    #[test]
    fn slurm_cmd_default_is_srun() {
        assert_eq!(SlurmCmd::default().srun_cmd, "srun");
    }

    #[test]
    fn slurm_cmd_new_overrides_launcher() {
        assert_eq!(SlurmCmd::new("bash").srun_cmd, "bash");
    }

    // ---- build_shell_str ----

    #[test]
    fn build_shell_str_with_shell_wraps_in_dash_c() {
        let m = manager_with_shell(Some("/bin/bash"), "srun");
        // Matches the Python reference's space-padded quote behavior:
        // `' '.join(["/bin/bash", "-c", '"', "srun", "/tmp/x", '"'])`.
        assert_eq!(
            m.build_shell_str(&["srun", "/tmp/x"]),
            r#"/bin/bash -c " srun /tmp/x ""#
        );
    }

    #[test]
    fn build_shell_str_without_shell_joins_verbatim() {
        let m = manager_with_shell(None, "srun");
        assert_eq!(m.build_shell_str(&["srun", "/tmp/x"]), "srun /tmp/x");
    }

    #[test]
    fn build_shell_str_empty_input() {
        let m = manager_with_shell(None, "srun");
        assert_eq!(m.build_shell_str(&[]), "");
    }

    // ---- srun_command ----

    #[test]
    fn srun_command_uses_absolute_path() {
        let m = manager_with_shell(None, "srun");
        let s = m.srun_command(Path::new("/tmp/example.sh")).unwrap();
        assert_eq!(s, "srun /tmp/example.sh");
    }

    #[test]
    fn srun_command_with_shell_wraps() {
        let m = manager_with_shell(Some("/bin/bash"), "srun");
        let s = m.srun_command(Path::new("/tmp/example.sh")).unwrap();
        assert_eq!(s, r#"/bin/bash -c " srun /tmp/example.sh ""#);
    }

    #[test]
    fn srun_command_relative_path_becomes_absolute() {
        let m = manager_with_shell(None, "srun");
        let cwd = std::env::current_dir().unwrap();
        let s = m.srun_command(Path::new("foo.sh")).unwrap();
        let expected = format!("srun {}/foo.sh", cwd.display());
        assert_eq!(s, expected);
    }

    // ---- run_job ----

    #[tokio::test]
    async fn run_job_dry_run_returns_zero() {
        let m = manager_with_shell(None, "srun");
        let code = m
            .run_job(Path::new("/tmp/dummy.sh"), true)
            .await
            .unwrap();
        assert_eq!(code, 0);
    }

    #[tokio::test]
    async fn run_job_real_subprocess_with_true_returns_zero() {
        // `true` ignores all args and exits 0 — perfect substitute for
        // a real `srun` in CI.
        let m = manager_with_shell(None, "true");
        let code = m
            .run_job(Path::new("/tmp/dummy.sh"), false)
            .await
            .unwrap();
        assert_eq!(code, 0);
    }

    #[tokio::test]
    async fn run_job_real_subprocess_with_false_returns_nonzero() {
        // `false` ignores all args and exits 1 — verifies non-zero
        // exit codes propagate up through the shell wrapper.
        let m = manager_with_shell(None, "false");
        let code = m
            .run_job(Path::new("/tmp/dummy.sh"), false)
            .await
            .unwrap();
        assert_eq!(code, 1);
    }

    // ---- query_job_state ----
    //
    // We deliberately do not unit-test `query_job_state` against a real
    // SLURM controller — the underlying parsers in `runner.rs` are
    // already covered there and `query_job_state` is a thin one-line
    // delegate.
}
