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
//!   the child exits. For `capture`, stderr is appended after stdout
//!   with a `\n[stderr]\n` marker so failure diagnostics (`sbatch: error:
//!   …`) reach the caller's error message. Line-based parsers
//!   (`parse_submitted_jobid`, squeue / sacct / qgroup parsers) are
//!   unaffected because they ignore lines that don't match their prefix.
//! - [`DryRunDispatcher`] — testing / dry-run. Prints the argv that
//!   *would* have been spawned and returns success without touching
//!   the OS.

use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::process::Stdio;

use anyhow::{Context, Result};
use tokio::process::Command;

/// Boxed future returned by [`DynJobDispatcher::run`].
type DynRunFut<'a> = Pin<Box<dyn Future<Output = Result<i32>> + Send + 'a>>;

/// Boxed future returned by [`DynJobDispatcher::capture`].
type DynCaptureFut<'a> = Pin<Box<dyn Future<Output = Result<(i32, String)>> + Send + 'a>>;

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

    /// Spawn `argv` and capture its output. Returns `(exit_code, output)`.
    ///
    /// Production implementations (e.g. [`TokioDispatcher`]) merge stderr
    /// into the returned `output`: if stderr is non-empty, the second
    /// tuple element is `"{stdout}\n[stderr]\n{stderr}"`; otherwise it is
    /// just `stdout`. This contract exists so that command failures
    /// (e.g. `sbatch: error: invalid partition`) surface in
    /// [`crate::sbatch::error::SbatchSpawnError::SubmitFailed::output`]
    /// rather than being silently dropped.
    ///
    /// Used for `sbatch`, `scancel`, `squeue`, `sacct`, and `qgroup` style
    /// queries. Their line-based parsers ignore the `[stderr]` marker line
    /// and any subsequent stderr lines that don't match the expected prefix.
    fn capture(&self, argv: &[String]) -> impl Future<Output = Result<(i32, String)>> + Send;
}

/// Dyn-compatible facade over [`JobDispatcher`] so callers can hold
/// `Arc<dyn DynJobDispatcher>` (RPITIT prevents `Arc<dyn JobDispatcher>`
/// directly).
///
/// Use [`into_dyn`] to convert any `D: JobDispatcher` into an
/// `Arc<dyn DynJobDispatcher>`.
pub trait DynJobDispatcher: Send + Sync {
    fn run<'a>(&'a self, argv: &'a [String]) -> DynRunFut<'a>;

    fn capture<'a>(&'a self, argv: &'a [String]) -> DynCaptureFut<'a>;
}

/// Newtype adapter that wraps any [`JobDispatcher`] impl and exposes the
/// dyn-compatible [`DynJobDispatcher`] interface.  Use [`into_dyn`] as the
/// ergonomic constructor.
pub struct DynDispatcherAdapter<D: JobDispatcher>(pub D);

impl<D: JobDispatcher + Send + Sync> DynJobDispatcher for DynDispatcherAdapter<D> {
    fn run<'a>(&'a self, argv: &'a [String]) -> DynRunFut<'a> {
        Box::pin(<D as JobDispatcher>::run(&self.0, argv))
    }

    fn capture<'a>(&'a self, argv: &'a [String]) -> DynCaptureFut<'a> {
        Box::pin(<D as JobDispatcher>::capture(&self.0, argv))
    }
}

/// Wrap a [`JobDispatcher`] into an `Arc<dyn DynJobDispatcher>`.
///
/// ```rust,ignore
/// let d: Arc<dyn DynJobDispatcher> = into_dyn(DryRunDispatcher);
/// ```
pub fn into_dyn<D: JobDispatcher + Send + Sync + 'static>(
    d: D,
) -> std::sync::Arc<dyn DynJobDispatcher> {
    std::sync::Arc::new(DynDispatcherAdapter(d))
}

/// Borrowed view that implements [`JobDispatcher`] for a `&dyn DynJobDispatcher`.
///
/// Used to cross from the dyn-erased dispatcher held by handles back to
/// the generic-bound `JobDispatcher` API expected by `runner::query_*`.
///
/// ```rust,ignore
/// let view = DynView(&*inner.dispatcher);
/// let result = query_job_states_via_qgroup_with(&view, &[jobid]).await?;
/// ```
pub struct DynView<'a>(pub &'a dyn DynJobDispatcher);

impl<'a> JobDispatcher for DynView<'a> {
    // The `impl Future` return form is intentional here: `async fn` would
    // capture `self` instead of only the inner pointer, which causes
    // lifetime conflicts when bridging `&dyn DynJobDispatcher`.
    #[allow(clippy::manual_async_fn)]
    fn run(&self, argv: &[String]) -> impl Future<Output = Result<i32>> + Send {
        async move { self.0.run(argv).await }
    }

    #[allow(clippy::manual_async_fn)]
    fn capture(&self, argv: &[String]) -> impl Future<Output = Result<(i32, String)>> + Send {
        async move { self.0.capture(argv).await }
    }
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
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let combined = if stderr.trim().is_empty() {
            stdout.into_owned()
        } else if stdout.is_empty() {
            format!("[stderr]\n{stderr}")
        } else {
            format!("{stdout}\n[stderr]\n{stderr}")
        };
        Ok((code, combined))
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

// -------------------------------------------------- BackgroundDispatcher

/// A child spawned but **not awaited**. Caller owns the `Child` and must
/// `wait()` it (or move it into a task that does) to avoid zombies.
pub struct SpawnedChild {
    pub pid: u32,
    pub child: tokio::process::Child,
}

impl std::fmt::Debug for SpawnedChild {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpawnedChild")
            .field("pid", &self.pid)
            .finish_non_exhaustive()
    }
}

/// Non-blocking variant of [`JobDispatcher`]: returns immediately with a
/// child handle whose stdout/stderr are piped.
pub trait BackgroundDispatcher: JobDispatcher {
    fn spawn(
        &self,
        argv: &[String],
        env: &HashMap<String, String>,
        cwd: Option<&Path>,
    ) -> impl std::future::Future<Output = anyhow::Result<SpawnedChild>> + Send;
}

/// Production [`BackgroundDispatcher`] backed by `tokio::process::Command`.
#[derive(Debug, Default, Clone, Copy)]
pub struct TokioBackgroundDispatcher;

impl JobDispatcher for TokioBackgroundDispatcher {
    async fn run(&self, argv: &[String]) -> anyhow::Result<i32> {
        TokioDispatcher.run(argv).await
    }
    async fn capture(&self, argv: &[String]) -> anyhow::Result<(i32, String)> {
        TokioDispatcher.capture(argv).await
    }
}

impl BackgroundDispatcher for TokioBackgroundDispatcher {
    async fn spawn(
        &self,
        argv: &[String],
        env: &HashMap<String, String>,
        cwd: Option<&Path>,
    ) -> anyhow::Result<SpawnedChild> {
        use anyhow::Context as _;
        let (program, args) = argv
            .split_first()
            .context("TokioBackgroundDispatcher::spawn called with empty argv")?;
        let mut cmd = tokio::process::Command::new(program);
        cmd.args(args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .envs(env);
        if let Some(d) = cwd {
            cmd.current_dir(d);
        }
        let child = cmd
            .spawn()
            .with_context(|| format!("failed to spawn `{program}`"))?;
        let pid = child.id().context("spawned child has no pid")?;
        Ok(SpawnedChild { pid, child })
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

    /// Failure-mode diagnostic contract: when a child writes to stderr
    /// (e.g. `sbatch: error: invalid partition`), `capture` must surface
    /// it via the returned `output` string. Regression guard for the bug
    /// where `RuntimeError: sbatch invocation failed (exit=1):` reached
    /// Python with an empty message.
    #[tokio::test]
    async fn tokio_capture_merges_stderr_after_stdout() {
        let d = TokioDispatcher;
        let (code, out) = d
            .capture(&[
                "sh".into(),
                "-c".into(),
                "echo on-stdout; echo on-stderr 1>&2; exit 1".into(),
            ])
            .await
            .unwrap();
        assert_eq!(code, 1);
        assert!(
            out.contains("on-stdout"),
            "stdout content must be preserved, got: {out:?}"
        );
        assert!(
            out.contains("on-stderr"),
            "stderr content must be appended for diagnostics, got: {out:?}"
        );
        assert!(
            out.contains("[stderr]"),
            "stderr section must be marked, got: {out:?}"
        );
    }

    /// stdout-only commands must not gain a spurious `[stderr]` marker.
    /// Pins the success path so existing line-based parsers
    /// (`parse_submitted_jobid` etc.) stay unaffected.
    #[tokio::test]
    async fn tokio_capture_stderr_empty_leaves_stdout_unchanged() {
        let d = TokioDispatcher;
        let (code, out) = d
            .capture(&["sh".into(), "-c".into(), "echo only-stdout".into()])
            .await
            .unwrap();
        assert_eq!(code, 0);
        assert_eq!(out.trim(), "only-stdout");
        assert!(
            !out.contains("[stderr]"),
            "no stderr marker when stderr empty, got: {out:?}"
        );
    }

    #[tokio::test]
    async fn empty_argv_errors() {
        let d = TokioDispatcher;
        let err = d.run(&[]).await.unwrap_err();
        assert!(err.to_string().contains("empty argv"));
    }

    #[tokio::test]
    async fn tokio_background_spawn_runs_to_zero() {
        use std::collections::HashMap;
        let d = TokioBackgroundDispatcher;
        let argv = vec!["bash".to_string(), "-c".to_string(), "exit 0".to_string()];
        let mut spawned = d.spawn(&argv, &HashMap::new(), None).await.unwrap();
        assert!(spawned.pid > 0);
        let status = spawned.child.wait().await.unwrap();
        assert_eq!(status.code(), Some(0));
    }

    #[tokio::test]
    async fn tokio_background_spawn_missing_binary_errors() {
        use std::collections::HashMap;
        let d = TokioBackgroundDispatcher;
        let err = d
            .spawn(
                &["definitely-not-a-real-binary-xyz".to_string()],
                &HashMap::new(),
                None,
            )
            .await
            .unwrap_err();
        let s = err.to_string();
        assert!(
            s.contains("failed to spawn") || s.contains("No such file"),
            "unexpected error: {s}"
        );
    }
}
