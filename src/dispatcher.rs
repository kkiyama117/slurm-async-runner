//! Subprocess-execution abstraction.
//!
//! Splits the *runtime-specific* concern (how do I actually spawn a
//! process?) from the *spec* concern (what argv should I run?). The
//! `SlurmCmd` and `SlurmManager` types in [`crate::manager`] only build
//! argv; this module provides the [`JobDispatcher`] trait that turns
//! argv into actual subprocess work.
//!
//! Two implementations are shipped:
//!
//! - [`TokioDispatcher`] — production. Uses `tokio::process::Command`,
//!   pipes both stdout and stderr, and (for `run`) echoes them after
//!   the child exits.
//! - [`DryRunDispatcher`] — testing / dry-run. Prints the argv that
//!   *would* have been spawned and returns success without touching
//!   the OS.

use std::future::Future;
use std::process::Stdio;

use anyhow::{Context, Result};
use tokio::process::Command;

/// Abstract subprocess launcher used by the SLURM glue.
///
/// `run` is for fire-and-forget work (`srun job.sh`) where only the
/// exit code matters and stdout/stderr should reach the user; `capture`
/// is for read-only commands (`squeue`, `sacct`) where the caller needs
/// the stdout text.
///
/// Implementors must be `Send + Sync` so a single dispatcher instance
/// can be shared across tokio worker threads. The returned futures are
/// also `Send` so they can be `await`-ed across `.await` points in
/// multi-threaded runtimes.
pub trait JobDispatcher: Send + Sync {
    /// Spawn `argv`, echo any stdout/stderr to the parent's
    /// stdout/stderr, and return the child exit code (or `0` if the
    /// child was signal-killed).
    fn run(&self, argv: &[String]) -> impl Future<Output = Result<i32>> + Send;

    /// Spawn `argv` and capture stdout. Returns `(exit_code, stdout)`;
    /// stderr is discarded. Used for `squeue` / `sacct` style queries.
    fn capture(&self, argv: &[String]) -> impl Future<Output = Result<(i32, String)>> + Send;
}

// --------------------------------------------------------- TokioDispatcher

/// Production [`JobDispatcher`] backed by `tokio::process::Command`.
#[derive(Debug, Default, Clone, Copy)]
pub struct TokioDispatcher;

impl JobDispatcher for TokioDispatcher {
    async fn run(&self, argv: &[String]) -> Result<i32> {
        let (program, args) = argv
            .split_first()
            .context("TokioDispatcher::run called with empty argv")?;
        let output = Command::new(program)
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .with_context(|| format!("failed to spawn `{program}`"))?;
        let code = output.status.code().unwrap_or(0);
        println!("[{} exited with {code}]", argv.join(" "));
        if !output.stdout.is_empty() {
            println!("[stdout]\n{}", String::from_utf8_lossy(&output.stdout));
        }
        if !output.stderr.is_empty() {
            println!("[stderr]\n{}", String::from_utf8_lossy(&output.stderr));
        }
        Ok(code)
    }

    async fn capture(&self, argv: &[String]) -> Result<(i32, String)> {
        let (program, args) = argv
            .split_first()
            .context("TokioDispatcher::capture called with empty argv")?;
        let output = Command::new(program)
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .with_context(|| format!("failed to spawn `{program}`"))?;
        let code = output.status.code().unwrap_or(0);
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        Ok((code, stdout))
    }
}

// -------------------------------------------------------- DryRunDispatcher

/// Spawnless [`JobDispatcher`] for dry runs / unit tests. Prints the
/// argv that *would* have run and returns success.
#[derive(Debug, Default, Clone, Copy)]
pub struct DryRunDispatcher;

impl JobDispatcher for DryRunDispatcher {
    async fn run(&self, argv: &[String]) -> Result<i32> {
        println!("{}", argv.join(" "));
        Ok(0)
    }

    async fn capture(&self, argv: &[String]) -> Result<(i32, String)> {
        println!("{}", argv.join(" "));
        Ok((0, String::new()))
    }
}

// --------------------------------------------------------------------- tests

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dry_run_run_returns_zero_with_no_spawn() {
        let d = DryRunDispatcher;
        let code = d.run(&["does-not-exist".into()]).await.unwrap();
        assert_eq!(code, 0);
    }

    #[tokio::test]
    async fn dry_run_capture_returns_empty_stdout() {
        let d = DryRunDispatcher;
        let (code, out) = d.capture(&["does-not-exist".into()]).await.unwrap();
        assert_eq!(code, 0);
        assert_eq!(out, "");
    }

    #[tokio::test]
    async fn tokio_run_true_returns_zero() {
        // `true` is a coreutils binary that ignores all args and exits
        // 0 — perfect substitute for a real `srun` in CI.
        let d = TokioDispatcher;
        let code = d.run(&["true".into(), "ignored-arg".into()]).await.unwrap();
        assert_eq!(code, 0);
    }

    #[tokio::test]
    async fn tokio_run_false_returns_nonzero() {
        let d = TokioDispatcher;
        let code = d
            .run(&["false".into(), "ignored-arg".into()])
            .await
            .unwrap();
        assert_eq!(code, 1);
    }

    #[tokio::test]
    async fn tokio_capture_echo_returns_stdout() {
        let d = TokioDispatcher;
        let (code, out) = d
            .capture(&["echo".into(), "hello world".into()])
            .await
            .unwrap();
        assert_eq!(code, 0);
        assert_eq!(out.trim(), "hello world");
    }

    #[tokio::test]
    async fn empty_argv_errors() {
        let d = TokioDispatcher;
        let err = d.run(&[]).await.unwrap_err();
        assert!(err.to_string().contains("empty argv"));
    }
}
