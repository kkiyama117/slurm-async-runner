//! [`TssrunJobSnapshot`] (Serde) and [`TssrunJobHandle`] (in-process state) plus
//! the free function [`read_live_env_for_pid`] for callers that only have
//! a pid but need to inspect the child's `/proc/<pid>/environ`.
//!
//! ## Snapshot vs. owner split
//!
//! [`TssrunJobHandle`] keeps two pieces of state:
//!
//! - A [`tokio::sync::watch`] channel whose value is the current
//!   [`TssrunJobSnapshot`]. Cloned via [`TssrunJobHandle::watch`] for any reader
//!   that wants lock-free polling — including the pyo3 binding.
//! - Three `Option<JoinHandle<…>>` fields (wait + tee_stdout + tee_stderr)
//!   that are `.take()`-d exactly once during [`TssrunJobHandle::wait`]. After
//!   wait completes, the handle is "drained" and can no longer wait again.
//!
//! Attached handles ([`TssrunJobHandle::attach_snapshot`]) skip the join handles
//! and only carry the receiver — `wait()` on them returns
//! `Err("not owner of the child / already waited")`.
//!
//! ## Persistence
//!
//! When `store = Some(s)`, every mutation that changes `jobid`, `node`,
//! or `finished` triggers `s.save(&snapshot).await`. The shipped
//! [`FileSystemStateStore`] writes `{dir}/{uuid}.json` via atomic
//! rename; the [`InMemoryStateStore`] just inserts into a HashMap.
//! Save errors are logged via `tracing::warn!` rather than propagated,
//! because losing one mid-flight persistence write must not crash an
//! otherwise-healthy job.
//!
//! Cross-process attach reads the same store back via
//! [`crate::tssrun::manager::TssrunManager::attach`].
//!
//! [`FileSystemStateStore`]: crate::tssrun::store::FileSystemStateStore
//! [`InMemoryStateStore`]: crate::tssrun::store::InMemoryStateStore

use std::collections::HashMap;
use std::path::PathBuf;
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
use crate::store::JobStateStore;
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
pub struct TssrunJobSnapshot {
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

impl TssrunJobSnapshot {
    /// True while the child process is still alive (no `finished` recorded).
    pub fn is_running(&self) -> bool {
        self.finished.is_none()
    }

    /// True once the child has exited (regardless of how).
    pub fn is_finished(&self) -> bool {
        self.finished.is_some()
    }

    /// Exit code if the child exited normally; `None` if killed by signal
    /// or if `finished` is not yet recorded.
    pub fn exit_code(&self) -> Option<i32> {
        self.finished.as_ref().and_then(|f| f.exit_code)
    }
}

/// In-process handle to a spawned `tssrun` child plus the tee/wait tasks
/// that keep its [`TssrunJobSnapshot`] up to date.
pub struct TssrunJobHandle {
    snapshot_rx: watch::Receiver<TssrunJobSnapshot>,
    snapshot_tx: watch::Sender<TssrunJobSnapshot>,
    wait_handle: Option<JoinHandle<Result<Option<i32>>>>,
    tee_stdout_handle: Option<JoinHandle<()>>,
    tee_stderr_handle: Option<JoinHandle<()>>,
    store: Option<Arc<dyn JobStateStore<TssrunJobSnapshot>>>,
}

impl TssrunJobHandle {
    /// Build a handle from a freshly spawned child. Spawns the tee tasks
    /// for stdout/stderr and the wait task for `child.wait()`.
    ///
    /// When `store` is `Some`, every mutation that changes `jobid`,
    /// `node`, or `finished` is also written through. Save failures are
    /// downgraded to `tracing::warn!` so a transient backend hiccup does
    /// not abort an otherwise-healthy job.
    pub async fn from_spawn(
        mut spawned: SpawnedChild,
        init: TssrunJobSnapshot,
        log_sink: Arc<dyn JobLogSink>,
        store: Option<Arc<dyn JobStateStore<TssrunJobSnapshot>>>,
    ) -> Result<Self> {
        let (tx, rx) = watch::channel(init);

        if let Some(s) = &store {
            // Clone before awaiting: `tx.borrow()` returns a non-Send guard,
            // which would make the surrounding future non-Send and break the
            // pyo3-async-runtimes `future_into_py` Send bound downstream.
            let snap = tx.borrow().clone();
            persist_warn(s.as_ref(), &snap, "initial").await;
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
            store.clone(),
        ));
        let tee_stderr_handle = tokio::spawn(tee_stderr(
            stderr,
            log_sink.clone(),
            tx.clone(),
            store.clone(),
        ));

        let child_arc = Arc::new(Mutex::new(spawned.child));
        let wait_handle = {
            let child = child_arc.clone();
            let tx = tx.clone();
            let store = store.clone();
            let log_sink = log_sink.clone();
            tokio::spawn(async move {
                let status = child.lock().await.wait().await?;
                // status.code() is None when the child was killed by a signal
                // (SIGTERM on SLURM time-limit kill, SIGKILL on OOM, …). We
                // surface that as Ok(None) so callers can branch on it; the
                // previous behaviour of returning Ok(0) silently masked
                // failures and was the source of issue H2.
                let code = status.code();
                let snap_to_persist = {
                    tx.send_modify(|s| {
                        s.finished = Some(FinishedInfo {
                            exit_code: code,
                            finished_at_unix: now_unix(),
                        });
                    });
                    tx.borrow().clone()
                };
                if let Some(s) = &store {
                    persist_warn(s.as_ref(), &snap_to_persist, "post-exit").await;
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
            store,
        })
    }

    /// Build a read-only handle from a previously persisted snapshot.
    pub fn attach_snapshot(
        snap: TssrunJobSnapshot,
        store: Option<Arc<dyn JobStateStore<TssrunJobSnapshot>>>,
    ) -> Self {
        let (tx, rx) = watch::channel(snap);
        Self {
            snapshot_rx: rx,
            snapshot_tx: tx,
            wait_handle: None,
            tee_stdout_handle: None,
            tee_stderr_handle: None,
            store,
        }
    }

    /// Clone the underlying `watch::Receiver` so callers can read snapshots
    /// without taking exclusive ownership of the handle. This is what the
    /// pyo3 wrapper uses to keep `pid` / `jobid` / `is_running` polls cheap
    /// and lock-free against an in-flight `wait()` (issue H1).
    pub fn watch(&self) -> watch::Receiver<TssrunJobSnapshot> {
        self.snapshot_rx.clone()
    }

    pub fn snapshot(&self) -> TssrunJobSnapshot {
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
        self.snapshot_rx.borrow().is_running()
    }

    pub fn is_finished(&self) -> bool {
        self.snapshot_rx.borrow().is_finished()
    }

    pub fn exit_code(&self) -> Option<i32> {
        self.snapshot_rx.borrow().exit_code()
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

    /// Re-read the persisted snapshot from the store, broadcast it to all
    /// `watch::Receiver` subscribers, and return it.
    ///
    /// Phase 3 P2: return type changed from `Result<()>` to
    /// `Result<TssrunJobSnapshot>` so this method matches the
    /// [`crate::SbatchJobHandle::refresh`] shape and the upcoming
    /// `JobHandleCommon` trait. Existing callers that wrote
    /// `let _ = handle.refresh().await?;` continue to compile because Rust
    /// does not warn on a discarded `Ok(T)`.
    pub async fn refresh(&self) -> Result<TssrunJobSnapshot> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| anyhow!("no store on this handle"))?;
        let uuid = self.snapshot_rx.borrow().uuid;
        let snap = store
            .load(uuid)
            .await?
            .ok_or_else(|| anyhow!("uuid {uuid} not found in store"))?;
        let _ = self.snapshot_tx.send(snap.clone());
        Ok(snap)
    }

    /// Block (asynchronously) until [`TssrunJobSnapshot::is_finished`] is true.
    /// Polls via [`Self::refresh`] every `poll_interval`. The returned snapshot
    /// is the first refreshed snapshot that satisfies `is_finished()`.
    ///
    /// Mirrors [`crate::SbatchJobHandle::wait_terminal`] but takes `&self`
    /// (not `self`) — tssrun handles are designed for post-wait reuse and
    /// have no `Drop`-warn pattern that would justify consuming `self`.
    ///
    /// Returns immediately when the current snapshot is already terminal
    /// (no sleep / no first refresh on stale state).
    pub async fn wait_terminal(
        &self,
        poll_interval: std::time::Duration,
    ) -> Result<TssrunJobSnapshot> {
        loop {
            let snap = self.refresh().await?;
            if snap.is_finished() {
                return Ok(snap);
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    /// Read `/proc/<pid>/environ` (Linux only). Returns `Ok(None)` on
    /// other platforms or when the directory is gone.
    pub async fn live_env(&self) -> Result<Option<HashMap<String, String>>> {
        read_live_env_for_pid(self.pid()).await
    }
}

/// Read `/proc/<pid>/environ` for an arbitrary pid without holding a
/// `TssrunJobHandle`. Used by the pyo3 wrapper to bypass the handle mutex.
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
    tx: watch::Sender<TssrunJobSnapshot>,
    store: Option<Arc<dyn JobStateStore<TssrunJobSnapshot>>>,
) {
    tee_lines(stdout, LogStream::Stdout, sink, tx, store).await;
}

async fn tee_stderr(
    stderr: ChildStderr,
    sink: Arc<dyn JobLogSink>,
    tx: watch::Sender<TssrunJobSnapshot>,
    store: Option<Arc<dyn JobStateStore<TssrunJobSnapshot>>>,
) {
    tee_lines(stderr, LogStream::Stderr, sink, tx, store).await;
}

async fn tee_lines<R>(
    stream: R,
    stream_kind: LogStream,
    sink: Arc<dyn JobLogSink>,
    tx: watch::Sender<TssrunJobSnapshot>,
    store: Option<Arc<dyn JobStateStore<TssrunJobSnapshot>>>,
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
                if updated && let Some(s) = &store {
                    // Clone the snapshot under the lock window of `borrow()`
                    // and release before awaiting the store — avoids holding
                    // the watch borrow across an `.await` point.
                    let snap = tx.borrow().clone();
                    persist_warn(s.as_ref(), &snap, "after parse").await;
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

/// Save `snap` through `store`, downgrading any error to a warning.
/// The wait/tee tasks call this as the child progresses; a transient
/// store outage must not crash the job, just leave the persisted view
/// slightly stale until the next mutation.
async fn persist_warn(
    store: &dyn JobStateStore<TssrunJobSnapshot>,
    snap: &TssrunJobSnapshot,
    when: &str,
) {
    if let Err(e) = store.save(snap).await {
        tracing::warn!(error = %e, when, uuid = %snap.uuid, "store.save failed");
    }
}

/// Deprecated alias for [`TssrunJobSnapshot`]. Phase 3 P1 renamed the
/// canonical struct to be naming-symmetric with [`crate::SbatchJobSnapshot`].
/// Remove in the next major.
#[deprecated(
    since = "0.1.0",
    note = "use TssrunJobSnapshot — Phase 3 P1 rename for naming symmetry with SbatchJobSnapshot"
)]
pub type JobHandleSnapshot = TssrunJobSnapshot;

/// Deprecated alias for [`TssrunJobHandle`]. Phase 3 P1 renamed the
/// canonical struct to be naming-symmetric with [`crate::SbatchJobHandle`].
/// Remove in the next major.
#[deprecated(
    since = "0.1.0",
    note = "use TssrunJobHandle — Phase 3 P1 rename for naming symmetry with SbatchJobHandle"
)]
pub type JobHandle = TssrunJobHandle;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;

    use crate::dispatcher::{BackgroundDispatcher, TokioBackgroundDispatcher};
    use crate::tssrun::log::InMemoryLogSink;
    use std::sync::Arc;

    fn snap_running() -> TssrunJobSnapshot {
        TssrunJobSnapshot {
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
        let back: TssrunJobSnapshot = serde_json::from_str(&json).unwrap();
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
        let back: TssrunJobSnapshot = serde_json::from_str(&json).unwrap();
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

        let init = TssrunJobSnapshot {
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
        let mut handle = TssrunJobHandle::from_spawn(spawned, init, sink_for_handle, None)
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
        let mut h = TssrunJobHandle::attach_snapshot(snap, None);
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

        let init = TssrunJobSnapshot {
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
        let mut handle = TssrunJobHandle::from_spawn(spawned, init, sink, None)
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
        let init = TssrunJobSnapshot {
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
        let mut h = TssrunJobHandle::from_spawn(spawned, init, sink, None)
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

    #[test]
    fn snapshot_is_running_when_finished_is_none() {
        let s = snap_running();
        assert!(s.is_running());
        assert!(!s.is_finished());
        assert_eq!(s.exit_code(), None);
    }

    #[test]
    fn snapshot_is_finished_after_normal_exit() {
        let mut s = snap_running();
        s.finished = Some(FinishedInfo {
            exit_code: Some(0),
            finished_at_unix: 1,
        });
        assert!(!s.is_running());
        assert!(s.is_finished());
        assert_eq!(s.exit_code(), Some(0));
    }

    #[test]
    fn snapshot_signal_killed_has_none_exit_code() {
        let mut s = snap_running();
        s.finished = Some(FinishedInfo {
            exit_code: None,
            finished_at_unix: 1,
        });
        assert!(!s.is_running());
        assert!(s.is_finished());
        assert_eq!(s.exit_code(), None);
    }

    #[test]
    fn tssrun_job_snapshot_alias_for_jobhandlesnapshot_resolves() {
        // Deprecated alias must continue to compile after the Phase 3 P1 rename.
        let snap = snap_running();
        #[allow(deprecated)]
        let _: super::JobHandleSnapshot = snap.clone();
    }

    #[test]
    fn tssrun_job_handle_alias_for_jobhandle_resolves() {
        // Type-level assertion that both the new name and the deprecated
        // alias refer to the same Send + Sync type.
        fn _assert_send_sync<T: Send + Sync>() {}
        _assert_send_sync::<TssrunJobHandle>();
        #[allow(deprecated)]
        _assert_send_sync::<super::JobHandle>();
    }

    #[tokio::test]
    async fn refresh_returns_snapshot_from_store() {
        // Phase 3 P2: refresh() returns the freshly-loaded snapshot so
        // callers don't need a follow-up snapshot() borrow.
        let store: Arc<dyn JobStateStore<TssrunJobSnapshot>> =
            Arc::new(crate::store::InMemoryStateStore::<TssrunJobSnapshot>::new());
        let snap = snap_running();
        store.save(&snap).await.unwrap();

        let h = TssrunJobHandle::attach_snapshot(snap.clone(), Some(store));
        let got = h.refresh().await.unwrap();

        assert_eq!(got.uuid, snap.uuid);
        assert_eq!(got, snap);
        assert_eq!(
            h.snapshot(),
            snap,
            "broadcast must update the watch channel"
        );
    }

    #[tokio::test]
    async fn wait_terminal_returns_immediately_when_already_finished() {
        let store: Arc<dyn JobStateStore<TssrunJobSnapshot>> =
            Arc::new(crate::store::InMemoryStateStore::<TssrunJobSnapshot>::new());
        let mut snap = snap_running();
        snap.finished = Some(FinishedInfo {
            exit_code: Some(0),
            finished_at_unix: 1,
        });
        store.save(&snap).await.unwrap();

        let h = TssrunJobHandle::attach_snapshot(snap.clone(), Some(store));
        let got = h
            .wait_terminal(std::time::Duration::from_secs(60))
            .await
            .unwrap();

        assert!(got.is_finished());
        assert_eq!(got.exit_code(), Some(0));
    }

    #[tokio::test]
    async fn wait_terminal_returns_when_store_flips_to_finished() {
        let store: Arc<dyn JobStateStore<TssrunJobSnapshot>> =
            Arc::new(crate::store::InMemoryStateStore::<TssrunJobSnapshot>::new());
        let snap = snap_running();
        store.save(&snap).await.unwrap();

        let h = TssrunJobHandle::attach_snapshot(snap.clone(), Some(store.clone()));

        let store2 = store.clone();
        let snap2 = snap.clone();
        let flipper = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let updated = TssrunJobSnapshot {
                finished: Some(FinishedInfo {
                    exit_code: Some(7),
                    finished_at_unix: 2,
                }),
                ..snap2
            };
            store2.save(&updated).await.unwrap();
        });

        let got = h
            .wait_terminal(std::time::Duration::from_millis(5))
            .await
            .unwrap();
        flipper.await.unwrap();

        assert!(got.is_finished());
        assert_eq!(got.exit_code(), Some(7));
    }
}
