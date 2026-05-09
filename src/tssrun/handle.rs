//! [`JobHandleSnapshot`] (Serde) and [`JobHandle`] (in-process state) plus
//! the free function [`read_live_env_for_pid`] for callers that only have
//! a pid but need to inspect the child's `/proc/<pid>/environ`.
//!
//! ## Snapshot vs. owner split
//!
//! [`JobHandle`] keeps two pieces of state:
//!
//! - A [`tokio::sync::watch`] channel whose value is the current
//!   [`JobHandleSnapshot`]. Cloned via [`JobHandle::watch`] for any reader
//!   that wants lock-free polling — including the pyo3 binding.
//! - Three `Option<JoinHandle<…>>` fields (wait + tee_stdout + tee_stderr)
//!   that are `.take()`-d exactly once during [`JobHandle::wait`]. After
//!   wait completes, the handle is "drained" and can no longer wait again.
//!
//! Attached handles ([`JobHandle::attach_snapshot`]) skip the join handles
//! and only carry the receiver — `wait()` on them returns
//! `Err("not owner of the child / already waited")`.
//!
//! ## Persistence
//!
//! When `persist_path = Some(p)`, every mutation that changes `jobid`,
//! `node`, or `finished` triggers an atomic-rename write of `p` using
//! `tempfile::NamedTempFile::persist`. Cross-process attach reads the
//! same file back via
//! [`crate::tssrun::manager::TssrunManager::attach`].

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{ChildStderr, ChildStdout};
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::dispatcher::SpawnedChild;
use crate::tssrun::log::{JobLogSink, LogStream};
use crate::tssrun::parse::{parse_salloc_jobid, parse_salloc_node};

/// Where the tee task is writing the child's logs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum LogLocations {
    /// Either no sink is attached or the sink is non-file-backed.
    None,
    /// Two append-only files on the local filesystem.
    Files { stdout: PathBuf, stderr: PathBuf },
    // Future: Sqlite { db_path: PathBuf, run_id: u64 }
}

/// Recorded once the child exits.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FinishedInfo {
    pub exit_code: Option<i32>,
    pub finished_at_unix: i64,
}

/// Persistable snapshot of a tssrun job. Updated by the tee task as the
/// `salloc:` lines arrive and by the wait task on child exit.
///
/// `uuid` is the stable primary key for a running job. It is generated at
/// spawn time as a UUID v7 (time-ordered, monotonic-friendly) and is the
/// only identifier that survives reuse hazards: `pid` may be recycled by
/// the kernel after the child exits, and `jobid` is only known after
/// SLURM emits the `salloc:` banner. Persisted snapshots are stored as
/// `{state_dir}/{uuid}.json`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobHandleSnapshot {
    pub uuid: Uuid,
    pub pid: u32,
    pub argv: Vec<String>,
    pub sent_env: HashMap<String, String>,
    pub cwd: Option<PathBuf>,
    pub started_at_unix: i64,
    pub log_locations: LogLocations,
    pub jobid: Option<u64>,
    pub node: Option<String>,
    pub finished: Option<FinishedInfo>,
}

/// In-process handle to a spawned `tssrun` child plus the tee/wait tasks
/// that keep its [`JobHandleSnapshot`] up to date.
pub struct JobHandle {
    snapshot_rx: watch::Receiver<JobHandleSnapshot>,
    snapshot_tx: watch::Sender<JobHandleSnapshot>,
    wait_handle: Option<JoinHandle<Result<Option<i32>>>>,
    tee_stdout_handle: Option<JoinHandle<()>>,
    tee_stderr_handle: Option<JoinHandle<()>>,
    persist_path: Option<PathBuf>,
}

impl JobHandle {
    /// Build a handle from a freshly spawned child. Spawns the tee tasks
    /// for stdout/stderr and the wait task for `child.wait()`.
    pub async fn from_spawn(
        mut spawned: SpawnedChild,
        init: JobHandleSnapshot,
        log_sink: Arc<dyn JobLogSink>,
        persist_path: Option<PathBuf>,
    ) -> Result<Self> {
        let (tx, rx) = watch::channel(init);

        if let Some(p) = &persist_path
            && let Err(e) = write_atomic_json(p, &tx.borrow())
        {
            tracing::warn!(error = %e, path = %p.display(), "initial persist failed");
        }

        let stdout = spawned
            .child
            .stdout
            .take()
            .context("BackgroundDispatcher returned a child without piped stdout")?;
        let stderr = spawned
            .child
            .stderr
            .take()
            .context("BackgroundDispatcher returned a child without piped stderr")?;

        let tee_stdout_handle = tokio::spawn(tee_stdout(
            stdout,
            log_sink.clone(),
            tx.clone(),
            persist_path.clone(),
        ));
        let tee_stderr_handle = tokio::spawn(tee_stderr(
            stderr,
            log_sink.clone(),
            tx.clone(),
            persist_path.clone(),
        ));

        let child_arc = Arc::new(Mutex::new(spawned.child));
        let wait_handle = {
            let child = child_arc.clone();
            let tx = tx.clone();
            let persist_path = persist_path.clone();
            let log_sink = log_sink.clone();
            tokio::spawn(async move {
                let status = child.lock().await.wait().await?;
                // status.code() is None when the child was killed by a signal
                // (SIGTERM on SLURM time-limit kill, SIGKILL on OOM, …). We
                // surface that as Ok(None) so callers can branch on it; the
                // previous behaviour of returning Ok(0) silently masked
                // failures and was the source of issue H2.
                let code = status.code();
                tx.send_modify(|s| {
                    s.finished = Some(FinishedInfo {
                        exit_code: code,
                        finished_at_unix: now_unix(),
                    });
                });
                if let Some(p) = &persist_path
                    && let Err(e) = write_atomic_json(p, &tx.borrow())
                {
                    tracing::warn!(error = %e, path = %p.display(), "post-exit persist failed");
                }
                let _ = log_sink.flush().await;
                Ok(code)
            })
        };

        Ok(Self {
            snapshot_rx: rx,
            snapshot_tx: tx,
            wait_handle: Some(wait_handle),
            tee_stdout_handle: Some(tee_stdout_handle),
            tee_stderr_handle: Some(tee_stderr_handle),
            persist_path,
        })
    }

    /// Build a read-only handle from a previously persisted snapshot.
    pub fn attach_snapshot(snap: JobHandleSnapshot, persist_path: Option<PathBuf>) -> Self {
        let (tx, rx) = watch::channel(snap);
        Self {
            snapshot_rx: rx,
            snapshot_tx: tx,
            wait_handle: None,
            tee_stdout_handle: None,
            tee_stderr_handle: None,
            persist_path,
        }
    }

    /// Clone the underlying `watch::Receiver` so callers can read snapshots
    /// without taking exclusive ownership of the handle. This is what the
    /// pyo3 wrapper uses to keep `pid` / `jobid` / `is_running` polls cheap
    /// and lock-free against an in-flight `wait()` (issue H1).
    pub fn watch(&self) -> watch::Receiver<JobHandleSnapshot> {
        self.snapshot_rx.clone()
    }

    pub fn snapshot(&self) -> JobHandleSnapshot {
        self.snapshot_rx.borrow().clone()
    }
    pub fn uuid(&self) -> Uuid {
        self.snapshot_rx.borrow().uuid
    }
    pub fn pid(&self) -> u32 {
        self.snapshot_rx.borrow().pid
    }
    pub fn jobid(&self) -> Option<u64> {
        self.snapshot_rx.borrow().jobid
    }
    pub fn node(&self) -> Option<String> {
        self.snapshot_rx.borrow().node.clone()
    }
    pub fn sent_env(&self) -> HashMap<String, String> {
        self.snapshot_rx.borrow().sent_env.clone()
    }
    pub fn is_running(&self) -> bool {
        self.snapshot_rx.borrow().finished.is_none()
    }
    pub fn exit_code(&self) -> Option<i32> {
        self.snapshot_rx
            .borrow()
            .finished
            .as_ref()
            .and_then(|f| f.exit_code)
    }

    /// Wait for the child to exit and return its exit status code.
    ///
    /// - `Ok(Some(n))` — the child exited normally with code `n` (0 on
    ///   clean success, non-zero on application error).
    /// - `Ok(None)` — the child was terminated by a signal (e.g. SIGTERM
    ///   from SLURM time-limit kill, SIGKILL from OOM). Inspect
    ///   `snapshot().finished` for the recorded `exit_code: None`.
    /// - `Err(_)` — `wait()` itself failed, or the handle was attached /
    ///   already consumed.
    pub async fn wait(&mut self) -> Result<Option<i32>> {
        let h = self
            .wait_handle
            .take()
            .ok_or_else(|| anyhow!("not owner of the child / already waited"))?;
        // Drain stdout/stderr completely before returning, so any salloc:
        // lines emitted before the child exited are guaranteed to be
        // parsed into the snapshot.
        if let Some(t) = self.tee_stdout_handle.take() {
            let _ = t.await;
        }
        if let Some(t) = self.tee_stderr_handle.take() {
            let _ = t.await;
        }
        h.await?
    }

    /// Re-read the persisted snapshot from disk and broadcast it.
    pub async fn refresh_from_disk(&self) -> Result<()> {
        let p = self
            .persist_path
            .as_ref()
            .ok_or_else(|| anyhow!("no persist_path on this handle"))?;
        let bytes = tokio::fs::read(p).await?;
        let snap: JobHandleSnapshot = serde_json::from_slice(&bytes)?;
        let _ = self.snapshot_tx.send(snap);
        Ok(())
    }

    /// Read `/proc/<pid>/environ` (Linux only). Returns `Ok(None)` on
    /// other platforms or when the directory is gone.
    pub async fn live_env(&self) -> Result<Option<HashMap<String, String>>> {
        read_live_env_for_pid(self.pid()).await
    }
}

/// Read `/proc/<pid>/environ` for an arbitrary pid without holding a
/// `JobHandle`. Used by the pyo3 wrapper to bypass the handle mutex.
///
/// This is best-effort introspection: it returns `Ok(None)` whenever
/// `/proc/<pid>/environ` is unobservable for any of the following reasons,
/// none of which indicate a real failure:
///
/// - non-Linux platform (no `/proc` filesystem),
/// - the child has already exited (`ENOENT`),
/// - the kernel refuses the read (`EACCES`) because the target's
///   `PR_SET_DUMPABLE` flag is cleared. This happens routinely when the
///   spawned binary is setuid/setgid (e.g. the kudpc/ECCS `tssrun` wrapper):
///   in that case `/proc/<pid>/environ` is owned by `root:root` with mode
///   `0400` and is only readable with `CAP_SYS_PTRACE`.
///
/// Other I/O errors are propagated.
pub async fn read_live_env_for_pid(pid: u32) -> Result<Option<HashMap<String, String>>> {
    if !cfg!(target_os = "linux") {
        return Ok(None);
    }
    let path = format!("/proc/{pid}/environ");
    match tokio::fs::read(&path).await {
        Ok(bytes) => Ok(Some(parse_environ(&bytes))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            tracing::debug!(
                pid,
                "live_env: /proc/<pid>/environ unreadable (EACCES) — \
                 likely a non-dumpable setuid child; returning None"
            );
            Ok(None)
        }
        Err(e) => Err(e).with_context(|| format!("failed to read {path}")),
    }
}

fn parse_environ(bytes: &[u8]) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let mut dropped_non_utf8: usize = 0;
    let mut dropped_no_eq: usize = 0;
    for raw in bytes.split(|&b| b == 0) {
        if raw.is_empty() {
            continue;
        }
        match std::str::from_utf8(raw) {
            Ok(s) => match s.split_once('=') {
                Some((k, v)) => {
                    out.insert(k.to_string(), v.to_string());
                }
                None => dropped_no_eq += 1,
            },
            Err(_) => dropped_non_utf8 += 1,
        }
    }
    if dropped_non_utf8 > 0 || dropped_no_eq > 0 {
        tracing::warn!(
            non_utf8 = dropped_non_utf8,
            no_eq = dropped_no_eq,
            "parse_environ dropped entries"
        );
    }
    out
}

async fn tee_stdout(
    stdout: ChildStdout,
    sink: Arc<dyn JobLogSink>,
    tx: watch::Sender<JobHandleSnapshot>,
    persist_path: Option<PathBuf>,
) {
    tee_lines(stdout, LogStream::Stdout, sink, tx, persist_path).await;
}

async fn tee_stderr(
    stderr: ChildStderr,
    sink: Arc<dyn JobLogSink>,
    tx: watch::Sender<JobHandleSnapshot>,
    persist_path: Option<PathBuf>,
) {
    tee_lines(stderr, LogStream::Stderr, sink, tx, persist_path).await;
}

async fn tee_lines<R>(
    stream: R,
    stream_kind: LogStream,
    sink: Arc<dyn JobLogSink>,
    tx: watch::Sender<JobHandleSnapshot>,
    persist_path: Option<PathBuf>,
) where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let mut lines = BufReader::new(stream).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                if let Err(e) = sink.append(stream_kind, &line).await {
                    tracing::warn!(error = %e, "log sink append failed");
                }
                let mut updated = false;
                if let Some(jid) = parse_salloc_jobid(&line) {
                    tx.send_modify(|s| {
                        if s.jobid.is_none() {
                            s.jobid = Some(jid);
                            updated = true;
                        }
                    });
                }
                if let Some(node) = parse_salloc_node(&line) {
                    tx.send_modify(|s| {
                        if s.node.is_none() {
                            s.node = Some(node);
                            updated = true;
                        }
                    });
                }
                if updated
                    && let Some(p) = &persist_path
                    && let Err(e) = write_atomic_json(p, &tx.borrow())
                {
                    tracing::warn!(error = %e, "persist after parse failed");
                }
            }
            Ok(None) => break,
            Err(e) => {
                tracing::warn!(error = %e, "tee_lines read error");
                break;
            }
        }
    }
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn write_atomic_json(path: &Path, snap: &JobHandleSnapshot) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("persist_path has no parent: {}", path.display()))?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("failed to mkdir -p {}", parent.display()))?;
    let mut tmp = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(&mut tmp, snap)?;
    tmp.persist(path)
        .map_err(|e| anyhow!("persist rename failed: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;

    use crate::dispatcher::{BackgroundDispatcher, TokioBackgroundDispatcher};
    use crate::tssrun::log::InMemoryLogSink;
    use std::sync::Arc;

    fn snap_running() -> JobHandleSnapshot {
        JobHandleSnapshot {
            uuid: Uuid::now_v7(),
            pid: 31415,
            argv: vec!["tssrun".into(), "/work/job.sh".into()],
            sent_env: HashMap::from([("OMP_NUM_THREADS".into(), "8".into())]),
            cwd: Some(PathBuf::from("/work")),
            started_at_unix: 1746345600,
            log_locations: LogLocations::Files {
                stdout: PathBuf::from("/var/log/x/o"),
                stderr: PathBuf::from("/var/log/x/e"),
            },
            jobid: Some(102362),
            node: Some("cnode3".into()),
            finished: None,
        }
    }

    #[test]
    fn snapshot_round_trip_running() {
        let s = snap_running();
        let json = serde_json::to_string(&s).unwrap();
        let back: JobHandleSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }

    #[test]
    fn snapshot_round_trip_finished_none_loglocations() {
        let mut s = snap_running();
        s.log_locations = LogLocations::None;
        s.finished = Some(FinishedInfo {
            exit_code: Some(0),
            finished_at_unix: 1746349200,
        });
        let json = serde_json::to_string(&s).unwrap();
        let back: JobHandleSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }

    #[tokio::test]
    async fn handle_from_spawn_parses_jobid_node_and_waits_zero() {
        let argv = vec![
            "bash".to_string(),
            "-c".to_string(),
            r#"echo "salloc: Granted job allocation 999"
echo "salloc: Nodes node-x are ready for job"
sleep 0.05
echo done"#
                .into(),
        ];
        let env = std::collections::HashMap::new();
        let spawned = TokioBackgroundDispatcher
            .spawn(&argv, &env, None)
            .await
            .unwrap();

        let typed_sink: Arc<InMemoryLogSink> = Arc::new(InMemoryLogSink::default());
        let sink_for_handle: Arc<dyn crate::tssrun::log::JobLogSink> =
            Arc::clone(&typed_sink) as Arc<dyn crate::tssrun::log::JobLogSink>;

        let init = JobHandleSnapshot {
            uuid: Uuid::now_v7(),
            pid: spawned.pid,
            argv: argv.clone(),
            sent_env: env,
            cwd: None,
            started_at_unix: 0,
            log_locations: LogLocations::None,
            jobid: None,
            node: None,
            finished: None,
        };
        let mut handle = JobHandle::from_spawn(spawned, init, sink_for_handle, None)
            .await
            .unwrap();

        let code = handle.wait().await.unwrap();
        assert_eq!(code, Some(0));

        let snap = handle.snapshot();
        assert_eq!(snap.jobid, Some(999));
        assert_eq!(snap.node.as_deref(), Some("node-x"));
        assert!(snap.finished.is_some());

        let lines: Vec<String> = typed_sink.snapshot().into_iter().map(|(_, l)| l).collect();
        assert!(lines.iter().any(|l| l == "done"), "sink lines: {lines:?}");
    }

    #[tokio::test]
    async fn attached_handle_wait_errors_with_not_owner() {
        let snap = snap_running();
        let mut h = JobHandle::attach_snapshot(snap, None);
        let err = h.wait().await.unwrap_err().to_string();
        assert!(err.contains("not owner"), "unexpected: {err}");
    }

    #[tokio::test]
    async fn wait_returns_none_when_child_is_signal_killed() {
        // Regression test for H2 — wait() must distinguish a signal kill
        // (Ok(None)) from a clean exit (Ok(Some(0))). bash's `kill -9 $$`
        // makes the child SIGKILL itself synchronously, so `Child::wait`
        // resolves with `ExitStatus::code() == None`.
        let argv = vec!["bash".to_string(), "-c".to_string(), "kill -9 $$".into()];
        let env = std::collections::HashMap::new();
        let spawned = TokioBackgroundDispatcher
            .spawn(&argv, &env, None)
            .await
            .unwrap();

        let init = JobHandleSnapshot {
            uuid: Uuid::now_v7(),
            pid: spawned.pid,
            argv,
            sent_env: env,
            cwd: None,
            started_at_unix: 0,
            log_locations: LogLocations::None,
            jobid: None,
            node: None,
            finished: None,
        };
        let sink: Arc<dyn crate::tssrun::log::JobLogSink> =
            Arc::new(crate::tssrun::log::NullLogSink);
        let mut handle = JobHandle::from_spawn(spawned, init, sink, None)
            .await
            .unwrap();
        let code = handle.wait().await.unwrap();
        assert_eq!(code, None, "signal-killed child should return Ok(None)");
        let snap = handle.snapshot();
        assert!(snap.finished.is_some(), "finished must be recorded");
        assert_eq!(
            snap.finished.unwrap().exit_code,
            None,
            "snapshot must record None exit_code for signal kill"
        );
    }

    #[tokio::test]
    async fn live_env_returns_some_for_running_self() {
        if !cfg!(target_os = "linux") {
            return;
        }
        let argv = vec!["bash".to_string(), "-c".to_string(), "sleep 0.5".into()];
        let mut env = std::collections::HashMap::new();
        env.insert("MY_TEST_VAR".to_string(), "ok".to_string());
        let spawned = TokioBackgroundDispatcher
            .spawn(&argv, &env, None)
            .await
            .unwrap();
        let init = JobHandleSnapshot {
            uuid: Uuid::now_v7(),
            pid: spawned.pid,
            argv,
            sent_env: env,
            cwd: None,
            started_at_unix: 0,
            log_locations: LogLocations::None,
            jobid: None,
            node: None,
            finished: None,
        };
        let sink: std::sync::Arc<dyn crate::tssrun::log::JobLogSink> =
            std::sync::Arc::new(crate::tssrun::log::NullLogSink);
        let mut h = JobHandle::from_spawn(spawned, init, sink, None)
            .await
            .unwrap();

        // Tokio's `Command::spawn` returns immediately after fork(). On a
        // busy test runner there is a brief window before the child has
        // finished its execve(), during which `/proc/<pid>/environ` still
        // reflects the *parent's* environment — without our `MY_TEST_VAR`
        // override. Wait briefly so the assertion observes the post-exec
        // state deterministically.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let live = h.live_env().await.unwrap();
        let live = live.expect("live env should be readable on linux for live child");
        assert_eq!(live.get("MY_TEST_VAR").map(String::as_str), Some("ok"));

        let _ = h.wait().await;
    }

    #[tokio::test]
    async fn read_live_env_for_pid_returns_none_when_pid_is_gone() {
        // ENOENT path: best-effort is None, never an error. We use
        // u32::MAX as a pid that the kernel will not have allocated.
        // On non-Linux the early return makes this trivially Ok(None).
        let res = read_live_env_for_pid(u32::MAX).await;
        assert!(matches!(res, Ok(None)), "unexpected: {res:?}");
    }
}
