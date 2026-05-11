//! `SbatchJobSnapshot` and supporting lifecycle types. The runtime
//! handle (`SbatchJobHandle`) is appended in Task 9.
//!
//! See `docs/superpowers/specs/2026-05-10-sbatch-module-design.md` §6
//! and §9 for the full design.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::{Mutex as TokioMutex, watch};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::dispatcher::DynJobDispatcher;
use crate::entities::slurm::JobPartition;
use crate::sbatch::parse::resolve_log_path;
use crate::store::JobStateStore;
use crate::{JobReason, JobState, JobStatus};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SbatchJobSnapshot {
    pub uuid: Uuid,
    pub jobid: u64,

    /// Per-task index within the array (e.g. `0`, `1`, `4` for `-a 0-1,4`).
    /// `None` for single (non-array) jobs — this is the sole discriminator
    /// between array tasks and singles (the `jobid` field stores the master
    /// jobid for array tasks too, so it cannot be used to distinguish).
    /// SLURM prints array task identity as `<master>_<idx>` in
    /// `squeue -t`.
    ///
    /// Phase 3 (#8 B1) removed the previously-mirrored `array_jobid`
    /// field — old Phase 2 on-disk JSON files containing `array_jobid:
    /// <N>` still load fine because this struct does not set
    /// `#[serde(deny_unknown_fields)]`; the surplus field is silently
    /// discarded. New writes omit it.
    #[serde(default)]
    pub array_task_id: Option<u32>,

    pub argv: Vec<String>,
    pub sent_env: HashMap<String, String>,
    pub script_path: PathBuf,
    pub chdir: Option<PathBuf>,
    pub partition: Option<JobPartition>,
    pub job_name: Option<String>,
    pub submitted_at: DateTime<Utc>,

    pub log: LogPathSpec,

    pub lifecycle: SbatchLifecycle,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogPathSpec {
    pub output_template: Option<String>,
    pub error_template: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SbatchLifecycle {
    pub last_observed_state: Option<JobStatus>,
    pub last_observed_at: Option<DateTime<Utc>>,
    pub left_active_listing: bool,
    pub finished: Option<FinishedInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinishedInfo {
    pub final_state: JobState,
    pub final_reason: JobReason,
    pub exit_code: Option<i32>,
    pub finished_at: DateTime<Utc>,
}

impl SbatchLifecycle {
    pub fn is_running(&self) -> bool {
        if self.left_active_listing {
            return false;
        }
        self.last_observed_state
            .as_ref()
            .map(|s| s.state.is_running())
            .unwrap_or(false)
    }

    pub fn is_finished(&self) -> bool {
        self.finished.is_some()
    }

    /// Exit code if the child exited normally; `None` if killed by signal,
    /// or if `finished` is not yet recorded.
    pub fn exit_code(&self) -> Option<i32> {
        self.finished.as_ref().and_then(|f| f.exit_code)
    }
}

impl SbatchJobSnapshot {
    pub fn output_path(&self) -> Option<PathBuf> {
        self.log
            .output_template
            .as_deref()
            .map(|t| resolve_log_path(t, self.jobid, self.array_task_id, self.job_name.as_deref()))
    }

    pub fn error_path(&self) -> Option<PathBuf> {
        self.log
            .error_template
            .as_deref()
            .map(|t| resolve_log_path(t, self.jobid, self.array_task_id, self.job_name.as_deref()))
    }

    pub fn is_running(&self) -> bool {
        self.lifecycle.is_running()
    }
    pub fn is_finished(&self) -> bool {
        self.lifecycle.is_finished()
    }
    /// Exit code if the child exited normally; `None` if killed by signal,
    /// or if `finished` is not yet recorded.
    pub fn exit_code(&self) -> Option<i32> {
        self.lifecycle.exit_code()
    }
}

/// Which job log stream to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogStream {
    Stdout,
    Stderr,
}

/// Errors that can occur while reading a job's log file.
#[derive(Debug, Error)]
pub enum LogReadError {
    #[error("log path not resolved on snapshot (template missing)")]
    PathNotResolved,
    #[error("io error reading log: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
pub enum SbatchAttachKey {
    Uuid(Uuid),
    JobId(u64),
    File(PathBuf),
}

/// Cheap-to-clone handle to an in-flight or attached sbatch job. All
/// snapshot reads are lock-free; `refresh` / `refresh_with_sacct` /
/// `wait_terminal` serialize through `refresh_lock`.
#[derive(Clone)]
pub struct SbatchJobHandle(pub(crate) Arc<SbatchJobHandleInner>);

pub(crate) struct SbatchJobHandleInner {
    pub(crate) snapshot_tx: watch::Sender<SbatchJobSnapshot>,
    pub(crate) store: Arc<dyn JobStateStore<SbatchJobSnapshot>>,
    pub(crate) dispatcher: Arc<dyn DynJobDispatcher>,
    pub(crate) refresh_lock: TokioMutex<()>,
}

/// Drop on the inner (Arc-shared) state — fires once when the last
/// clone of `SbatchJobHandle` is released. Per spec §6.5, dropping a
/// handle whose snapshot still appears to be running is treated as a
/// silent bug magnet and earns a `tracing::warn!`. The drop is
/// deliberately NOT auto-cancelling: SLURM jobs survive their owning
/// handles, and recovering one via `SbatchManager::attach_jobid` /
/// `attach_uuid` is the expected workflow. Use
/// `SbatchManager::cancel(jobid)` explicitly when termination is
/// intended.
impl Drop for SbatchJobHandleInner {
    fn drop(&mut self) {
        let snap = self.snapshot_tx.borrow();
        if snap.is_running() {
            tracing::warn!(
                jobid = snap.jobid,
                uuid = %snap.uuid,
                "SbatchJobHandle dropped while job still appears to be running; \
                 the job was NOT auto-cancelled. Call SbatchManager::cancel(jobid) \
                 explicitly if termination is intended."
            );
        }
    }
}

impl SbatchJobHandle {
    pub(crate) fn new(
        snapshot: SbatchJobSnapshot,
        store: Arc<dyn JobStateStore<SbatchJobSnapshot>>,
        dispatcher: Arc<dyn DynJobDispatcher>,
    ) -> Self {
        let (tx, _rx) = watch::channel(snapshot);
        Self(Arc::new(SbatchJobHandleInner {
            snapshot_tx: tx,
            store,
            dispatcher,
            refresh_lock: TokioMutex::new(()),
        }))
    }

    // -------- Lock-free snapshot reads --------

    pub fn snapshot(&self) -> SbatchJobSnapshot {
        self.0.snapshot_tx.borrow().clone()
    }

    pub fn watch(&self) -> watch::Receiver<SbatchJobSnapshot> {
        self.0.snapshot_tx.subscribe()
    }

    pub fn uuid(&self) -> Uuid {
        self.0.snapshot_tx.borrow().uuid
    }

    pub fn jobid(&self) -> Option<u64> {
        Some(self.0.snapshot_tx.borrow().jobid)
    }

    pub fn array_task_id(&self) -> Option<u32> {
        self.snapshot().array_task_id
    }

    pub fn partition(&self) -> Option<JobPartition> {
        self.0.snapshot_tx.borrow().partition.clone()
    }

    pub fn job_name(&self) -> Option<String> {
        self.0.snapshot_tx.borrow().job_name.clone()
    }

    pub fn sent_env(&self) -> HashMap<String, String> {
        self.0.snapshot_tx.borrow().sent_env.clone()
    }

    pub fn output_template(&self) -> Option<String> {
        self.0.snapshot_tx.borrow().log.output_template.clone()
    }

    pub fn error_template(&self) -> Option<String> {
        self.0.snapshot_tx.borrow().log.error_template.clone()
    }

    pub fn output_path(&self) -> Option<PathBuf> {
        self.0.snapshot_tx.borrow().output_path()
    }

    pub fn error_path(&self) -> Option<PathBuf> {
        self.0.snapshot_tx.borrow().error_path()
    }

    pub fn is_running(&self) -> bool {
        self.0.snapshot_tx.borrow().is_running()
    }

    pub fn is_finished(&self) -> bool {
        self.0.snapshot_tx.borrow().is_finished()
    }

    /// Exit code if the child exited normally; `None` if killed by signal,
    /// or if `finished` is not yet recorded.
    pub fn exit_code(&self) -> Option<i32> {
        self.0.snapshot_tx.borrow().exit_code()
    }

    // -------- Log read API (Phase 2 P1) --------

    /// Read the last `n` lines of the job's stdout/stderr log file.
    ///
    /// Returns an empty `Vec` if the log file does not yet exist (job
    /// pending or just submitted). Returns `LogReadError::PathNotResolved`
    /// if the snapshot does not carry the corresponding log template.
    /// Other I/O errors are propagated as `LogReadError::Io`.
    ///
    /// Phase 2 P1 implements this with a full read of the file followed
    /// by line splitting; for very large logs (> ~10MB) consider Phase 3
    /// optimization with reverse seek.
    pub async fn log_lines(
        &self,
        stream: LogStream,
        n: usize,
    ) -> Result<Vec<String>, LogReadError> {
        let snap = self.0.snapshot_tx.borrow().clone();
        let path = match stream {
            LogStream::Stdout => snap.output_path(),
            LogStream::Stderr => snap.error_path(),
        };
        let path = path.ok_or(LogReadError::PathNotResolved)?;
        match tokio::fs::read_to_string(&path).await {
            Ok(content) => {
                let lines: Vec<String> = content.lines().map(|s| s.to_string()).collect();
                let start = lines.len().saturating_sub(n);
                Ok(lines[start..].to_vec())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(LogReadError::Io(e)),
        }
    }

    /// Read the full contents of the job's stdout/stderr log file.
    ///
    /// Returns an empty string if the log file does not yet exist.
    /// Same error semantics as [`SbatchJobHandle::log_lines`] otherwise.
    pub async fn read_log_to_end(&self, stream: LogStream) -> Result<String, LogReadError> {
        let snap = self.0.snapshot_tx.borrow().clone();
        let path = match stream {
            LogStream::Stdout => snap.output_path(),
            LogStream::Stderr => snap.error_path(),
        };
        let path = path.ok_or(LogReadError::PathNotResolved)?;
        match tokio::fs::read_to_string(&path).await {
            Ok(content) => Ok(content),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
            Err(e) => Err(LogReadError::Io(e)),
        }
    }

    /// Lightweight polling: `qgroup -l` → `squeue` fallback. **Never** calls
    /// sacct. If both lookups miss, sets `lifecycle.left_active_listing = true`.
    pub async fn refresh(&self) -> anyhow::Result<SbatchJobSnapshot> {
        let inner = &*self.0;
        let _guard = inner.refresh_lock.lock().await;

        let mut snap = inner.snapshot_tx.borrow().clone();
        let now = chrono::Utc::now();
        let view = crate::dispatcher::DynView(&*inner.dispatcher);

        let qgroup = crate::runner::query_job_states_via_qgroup_with(&view, &[snap.jobid]).await?;
        if let Some(status) = qgroup.get(&snap.jobid) {
            snap.lifecycle.last_observed_state = Some(status.clone());
            snap.lifecycle.last_observed_at = Some(now);
            inner.store.save(&snap).await?;
            let _ = inner.snapshot_tx.send(snap.clone());
            return Ok(snap);
        }

        let squeue = crate::runner::query_job_states_squeue_only_with(&view, &[snap.jobid]).await?;
        if let Some(status) = squeue.get(&snap.jobid) {
            snap.lifecycle.last_observed_state = Some(status.clone());
            snap.lifecycle.last_observed_at = Some(now);
            inner.store.save(&snap).await?;
            let _ = inner.snapshot_tx.send(snap.clone());
            return Ok(snap);
        }

        snap.lifecycle.left_active_listing = true;
        snap.lifecycle.last_observed_at = Some(now);
        inner.store.save(&snap).await?;
        let _ = inner.snapshot_tx.send(snap.clone());
        Ok(snap)
    }

    /// Heavyweight finalizer. Calls `refresh()` first; only invokes
    /// sacct if the job has actually left both `qgroup -l` and `squeue`
    /// **and** `lifecycle.finished` is still None. Otherwise behaves
    /// identically to `refresh()`.
    pub async fn refresh_with_sacct(&self) -> anyhow::Result<SbatchJobSnapshot> {
        let mut snap = self.refresh().await?;
        if snap.lifecycle.finished.is_some() {
            return Ok(snap);
        }
        if !snap.lifecycle.left_active_listing {
            return Ok(snap);
        }

        let inner = &*self.0;
        // Re-acquire refresh_lock here to serialize the sacct call itself.
        // The inner `refresh()` above already took and released the lock
        // for the qgroup/squeue probes; this second acquisition guards the
        // heavier sacct invocation and the finished-info write.
        let _guard = inner.refresh_lock.lock().await;
        let view = crate::dispatcher::DynView(&*inner.dispatcher);

        // Phase 2 P1: switch to the exit-code-aware query so we can populate
        // FinishedInfo::exit_code instead of leaving it None.
        let map = crate::runner::query_job_states_with_exit_code_with(&view, &[snap.jobid]).await?;
        let outcome = map
            .get(&snap.jobid)
            .cloned()
            .unwrap_or(crate::runner::JobOutcome {
                status: JobStatus::default(),
                exit_code: None,
            });
        snap.lifecycle.finished = Some(FinishedInfo {
            final_state: outcome.status.state,
            final_reason: outcome.status.reason,
            exit_code: outcome.exit_code,
            finished_at: chrono::Utc::now(),
        });
        inner.store.save(&snap).await?;
        let _ = inner.snapshot_tx.send(snap.clone());
        Ok(snap)
    }

    /// Lightweight polling loop. Calls `refresh()` (sacct-free) at the
    /// supplied interval until either (a) the observed state is terminal,
    /// or (b) the job leaves both active listings. Caller may follow up
    /// with one `refresh_with_sacct()` if exit_code resolution is needed.
    pub async fn wait_terminal(
        &self,
        poll_interval: std::time::Duration,
    ) -> anyhow::Result<SbatchJobSnapshot> {
        loop {
            let snap = self.refresh().await?;
            if let Some(state) = &snap.lifecycle.last_observed_state
                && state.state.is_terminal()
            {
                return Ok(snap);
            }
            if snap.lifecycle.left_active_listing {
                return Ok(snap);
            }
            tokio::time::sleep(poll_interval).await;
        }
    }
}

/// Phase 3 P3: cross-backend trait implementation. All methods delegate
/// to the existing inherent `SbatchJobHandle` API — there is no behavior
/// change, just a uniform contract that callers can hold via
/// `H: JobHandleCommon`.
#[async_trait::async_trait]
impl crate::handle::JobHandleCommon for SbatchJobHandle {
    type Snapshot = SbatchJobSnapshot;

    fn uuid(&self) -> Uuid {
        Self::uuid(self)
    }
    fn jobid(&self) -> Option<u64> {
        Self::jobid(self)
    }
    fn is_running(&self) -> bool {
        Self::is_running(self)
    }
    fn is_finished(&self) -> bool {
        Self::is_finished(self)
    }
    fn exit_code(&self) -> Option<i32> {
        Self::exit_code(self)
    }

    fn snapshot(&self) -> SbatchJobSnapshot {
        Self::snapshot(self)
    }
    fn watch(&self) -> watch::Receiver<SbatchJobSnapshot> {
        Self::watch(self)
    }

    async fn refresh(&self) -> anyhow::Result<SbatchJobSnapshot> {
        Self::refresh(self).await
    }

    async fn wait_terminal(
        &self,
        poll_interval: std::time::Duration,
    ) -> anyhow::Result<SbatchJobSnapshot> {
        Self::wait_terminal(self, poll_interval).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(jobid: u64) -> SbatchJobSnapshot {
        SbatchJobSnapshot {
            uuid: Uuid::now_v7(),
            jobid,
            array_task_id: None,
            argv: vec!["sbatch".into(), "/w/job.sh".into()],
            sent_env: HashMap::from([("FOO".into(), "bar".into())]),
            script_path: PathBuf::from("/w/job.sh"),
            chdir: Some(PathBuf::from("/w")),
            partition: Some("gr19999b".into()),
            job_name: Some("g09run".into()),
            submitted_at: chrono::Utc::now(),
            log: LogPathSpec {
                output_template: Some("slurm-%j.out".into()),
                error_template: Some("slurm-%j.err".into()),
            },
            lifecycle: SbatchLifecycle::default(),
        }
    }

    #[test]
    fn snapshot_round_trips_through_json() {
        let s = snap(12345);
        let raw = serde_json::to_string(&s).unwrap();
        let back: SbatchJobSnapshot = serde_json::from_str(&raw).unwrap();
        assert_eq!(s, back);
    }

    #[test]
    fn output_path_substitutes_jobid_lazily() {
        let s = snap(12345);
        assert_eq!(s.output_path(), Some(PathBuf::from("slurm-12345.out")));
        assert_eq!(s.error_path(), Some(PathBuf::from("slurm-12345.err")));
    }

    #[test]
    fn lifecycle_running_requires_state_and_no_vanish() {
        let mut s = snap(1);
        assert!(!s.is_running());
        s.lifecycle.last_observed_state = Some(JobStatus {
            state: JobState::Running,
            reason: JobReason::None,
        });
        assert!(s.is_running());
        s.lifecycle.left_active_listing = true;
        assert!(!s.is_running());
    }

    #[test]
    fn lifecycle_finished_records_exit_code() {
        let mut s = snap(1);
        assert!(!s.is_finished());
        assert_eq!(s.exit_code(), None);
        s.lifecycle.finished = Some(FinishedInfo {
            final_state: JobState::Completed,
            final_reason: JobReason::None,
            exit_code: Some(0),
            finished_at: chrono::Utc::now(),
        });
        assert!(s.is_finished());
        assert_eq!(s.exit_code(), Some(0));
    }

    #[tokio::test]
    async fn handle_lock_free_getters_return_initial_snapshot() {
        use crate::dispatcher::{DryRunDispatcher, into_dyn};
        use crate::store::InMemoryStateStore;

        let s = snap(99);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(DryRunDispatcher);
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);

        assert_eq!(h.uuid(), s.uuid);
        assert_eq!(h.jobid(), Some(99));
        assert_eq!(h.job_name().as_deref(), Some("g09run"));
        assert_eq!(h.output_path(), Some(PathBuf::from("slurm-99.out")));
        assert!(!h.is_running());
        assert!(!h.is_finished());
    }

    #[tokio::test]
    async fn handle_clone_shares_inner() {
        use crate::dispatcher::{DryRunDispatcher, into_dyn};
        use crate::store::InMemoryStateStore;

        let s = snap(1);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(DryRunDispatcher);
        let h1 = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let h2 = h1.clone();
        assert_eq!(h1.uuid(), h2.uuid());
        let _r1 = h1.watch();
        let _r2 = h2.watch();
    }

    // ---- CannedDispatcher mock for refresh tests ----

    /// Mock dispatcher: returns canned outputs keyed by argv[0].
    /// argv[0] = "qgroup" → first canned, "squeue" → second, "sacct" → third.
    struct CannedDispatcher {
        qgroup: std::sync::Mutex<String>,
        squeue: std::sync::Mutex<String>,
        sacct: std::sync::Mutex<String>,
        sacct_call_count: std::sync::Mutex<u32>,
    }

    impl CannedDispatcher {
        fn new(qgroup: &str, squeue: &str, sacct: &str) -> Self {
            Self {
                qgroup: std::sync::Mutex::new(qgroup.to_string()),
                squeue: std::sync::Mutex::new(squeue.to_string()),
                sacct: std::sync::Mutex::new(sacct.to_string()),
                sacct_call_count: std::sync::Mutex::new(0),
            }
        }

        fn sacct_calls(&self) -> u32 {
            *self.sacct_call_count.lock().unwrap()
        }
    }

    impl crate::dispatcher::JobDispatcher for CannedDispatcher {
        async fn run(&self, _argv: &[String]) -> anyhow::Result<i32> {
            unimplemented!()
        }

        async fn capture(&self, argv: &[String]) -> anyhow::Result<(i32, String)> {
            let bin = argv[0].as_str();
            let out = match bin {
                "qgroup" => self.qgroup.lock().unwrap().clone(),
                "squeue" => self.squeue.lock().unwrap().clone(),
                "sacct" => {
                    *self.sacct_call_count.lock().unwrap() += 1;
                    self.sacct.lock().unwrap().clone()
                }
                _ => String::new(),
            };
            Ok((0, out))
        }
    }

    // Shared Arc-wrapper for `Arc<D>` → `dyn JobDispatcher` coercion
    // lives in `crate::sbatch::test_util` so handle.rs and manager.rs
    // both consume the same generic helper.
    use crate::sbatch::test_util::ArcDispatcher as MoveDispatcher;

    #[tokio::test]
    async fn refresh_uses_qgroup_when_jobid_present() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;

        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(CannedDispatcher::new(
            "QUEUE USER JOBID STATUS PROC\ngr u 12345 RUN 1\n",
            "",
            "",
        ));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh().await.unwrap();
        assert_eq!(
            after.lifecycle.last_observed_state.unwrap().state,
            crate::JobState::Running
        );
        assert!(!after.lifecycle.left_active_listing);
    }

    #[tokio::test]
    async fn refresh_falls_back_to_squeue_when_qgroup_misses() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;

        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(CannedDispatcher::new("", "12345 RUNNING None\n", ""));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh().await.unwrap();
        assert_eq!(
            after.lifecycle.last_observed_state.unwrap().state,
            crate::JobState::Running
        );
    }

    #[tokio::test]
    async fn refresh_marks_left_active_listing_when_both_miss() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;

        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let canned = std::sync::Arc::new(CannedDispatcher::new("", "", ""));
        // Wrap a clone of the Arc in into_dyn; keep the original Arc to
        // observe sacct call count after refresh.
        let dispatcher = {
            let canned = canned.clone();
            into_dyn(MoveDispatcher(canned))
        };
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh().await.unwrap();
        assert!(after.lifecycle.left_active_listing);
        assert_eq!(canned.sacct_calls(), 0, "refresh must NOT call sacct");
    }

    #[tokio::test]
    async fn refresh_with_sacct_skips_sacct_when_qgroup_hits() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;
        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let canned = std::sync::Arc::new(CannedDispatcher::new(
            "QUEUE USER JOBID STATUS PROC\ngr u 12345 RUN 1\n",
            "",
            "",
        ));
        let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh_with_sacct().await.unwrap();
        assert!(after.lifecycle.finished.is_none());
        assert_eq!(canned.sacct_calls(), 0);
    }

    #[tokio::test]
    async fn refresh_with_sacct_calls_sacct_once_after_vanish() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;
        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let canned =
            std::sync::Arc::new(CannedDispatcher::new("", "", "12345|COMPLETED|None|0:0\n"));
        let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh_with_sacct().await.unwrap();
        assert!(after.lifecycle.finished.is_some());
        assert_eq!(canned.sacct_calls(), 1);
    }

    #[tokio::test]
    async fn refresh_with_sacct_populates_exit_code_on_completed() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;
        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        // sacct now emits 4 columns (JobID|State|Reason|ExitCode)
        let canned =
            std::sync::Arc::new(CannedDispatcher::new("", "", "12345|COMPLETED|None|0:0\n"));
        let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh_with_sacct().await.unwrap();
        let finished = after.lifecycle.finished.expect("finished should be Some");
        assert_eq!(finished.final_state, crate::JobState::Completed);
        assert_eq!(finished.exit_code, Some(0));
    }

    #[tokio::test]
    async fn refresh_with_sacct_populates_exit_code_on_signaled() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;
        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let canned =
            std::sync::Arc::new(CannedDispatcher::new("", "", "12345|CANCELLED|None|0:9\n"));
        let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh_with_sacct().await.unwrap();
        let finished = after.lifecycle.finished.expect("finished should be Some");
        // SIGKILL = 9 -> 128 + 9 = 137
        assert_eq!(finished.exit_code, Some(137));
    }

    #[tokio::test]
    async fn wait_terminal_returns_when_state_is_terminal() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;
        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(CannedDispatcher::new(
            "QUEUE USER JOBID STATUS PROC\ngr u 12345 CMP 1\n",
            "",
            "",
        ));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h
            .wait_terminal(std::time::Duration::from_millis(10))
            .await
            .unwrap();
        assert_eq!(
            after.lifecycle.last_observed_state.unwrap().state,
            crate::JobState::Completed
        );
    }

    #[tokio::test]
    async fn wait_terminal_returns_on_left_active_listing() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;
        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let canned = std::sync::Arc::new(CannedDispatcher::new("", "", ""));
        let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h
            .wait_terminal(std::time::Duration::from_millis(10))
            .await
            .unwrap();
        assert!(after.lifecycle.left_active_listing);
        assert_eq!(
            canned.sacct_calls(),
            0,
            "wait_terminal must NEVER call sacct"
        );
    }

    // ---- log_lines / read_log_to_end ----

    use std::io::Write as _;

    fn snap_with_log_path(jobid: u64, stdout_path: &str, stderr_path: &str) -> SbatchJobSnapshot {
        let mut s = snap(jobid);
        s.log = LogPathSpec {
            output_template: Some(stdout_path.to_string()),
            error_template: Some(stderr_path.to_string()),
        };
        s
    }

    #[tokio::test]
    async fn log_lines_returns_empty_when_file_missing() {
        use crate::dispatcher::{DryRunDispatcher, into_dyn};
        use crate::store::InMemoryStateStore;
        let s = snap_with_log_path(
            12345,
            "/nonexistent/stdout-%j.out",
            "/nonexistent/stderr-%j.err",
        );
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(DryRunDispatcher);
        let h = SbatchJobHandle::new(s, store, dispatcher);
        let lines = h.log_lines(LogStream::Stdout, 5).await.unwrap();
        assert_eq!(lines, Vec::<String>::new());
    }

    #[tokio::test]
    async fn log_lines_returns_path_not_resolved_when_no_template() {
        use crate::dispatcher::{DryRunDispatcher, into_dyn};
        use crate::store::InMemoryStateStore;
        let mut s = snap(12345);
        s.log = LogPathSpec::default();
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(DryRunDispatcher);
        let h = SbatchJobHandle::new(s, store, dispatcher);
        let err = h.log_lines(LogStream::Stdout, 5).await.unwrap_err();
        assert!(matches!(err, LogReadError::PathNotResolved));
    }

    #[tokio::test]
    async fn log_lines_returns_last_n_lines() {
        use crate::dispatcher::{DryRunDispatcher, into_dyn};
        use crate::store::InMemoryStateStore;

        let dir = tempfile::tempdir().unwrap();
        let stdout_path = dir.path().join("stdout-12345.out");
        let mut f = std::fs::File::create(&stdout_path).unwrap();
        for i in 0..20 {
            writeln!(f, "line {i}").unwrap();
        }
        drop(f);

        let s = snap_with_log_path(12345, stdout_path.to_str().unwrap(), "ignored.err");
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(DryRunDispatcher);
        let h = SbatchJobHandle::new(s, store, dispatcher);

        let lines = h.log_lines(LogStream::Stdout, 5).await.unwrap();
        assert_eq!(
            lines,
            vec!["line 15", "line 16", "line 17", "line 18", "line 19"]
        );
    }

    #[tokio::test]
    async fn read_log_to_end_returns_full_content() {
        use crate::dispatcher::{DryRunDispatcher, into_dyn};
        use crate::store::InMemoryStateStore;

        let dir = tempfile::tempdir().unwrap();
        let stdout_path = dir.path().join("stdout-12345.out");
        std::fs::write(&stdout_path, "hello\nworld\n").unwrap();

        let s = snap_with_log_path(12345, stdout_path.to_str().unwrap(), "ignored.err");
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(DryRunDispatcher);
        let h = SbatchJobHandle::new(s, store, dispatcher);

        let content = h.read_log_to_end(LogStream::Stdout).await.unwrap();
        assert_eq!(content, "hello\nworld\n");
    }

    /// Drop on the inner Arc-shared state must not panic, regardless of
    /// whether the last-observed state says the job is still running.
    /// This is a smoke test for spec §6.5 — the Drop emits a
    /// `tracing::warn!` when running, and is a no-op otherwise. We
    /// cannot easily assert on the warn without a captive subscriber,
    /// but we can guarantee the Drop path is panic-free.
    #[tokio::test]
    async fn drop_does_not_panic_for_running_or_idle_handle() {
        use crate::dispatcher::{DryRunDispatcher, into_dyn};
        use crate::store::InMemoryStateStore;

        // Idle: no observed state → Drop is a silent no-op.
        {
            let s = snap(1);
            let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> =
                Arc::new(InMemoryStateStore::new());
            let dispatcher = into_dyn(DryRunDispatcher);
            let h = SbatchJobHandle::new(s, store, dispatcher);
            drop(h);
        }

        // Running: last_observed_state = Running → Drop emits warn.
        {
            let mut s = snap(2);
            s.lifecycle.last_observed_state = Some(JobStatus {
                state: JobState::Running,
                reason: JobReason::None,
            });
            let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> =
                Arc::new(InMemoryStateStore::new());
            let dispatcher = into_dyn(DryRunDispatcher);
            let h = SbatchJobHandle::new(s, store, dispatcher);
            drop(h);
        }
    }

    /// Cloning a handle and dropping each clone must not double-warn:
    /// the Drop impl lives on `SbatchJobHandleInner` (Arc-shared), so
    /// it fires exactly once when the last clone goes away. This test
    /// confirms the Arc semantics by simply not panicking under a
    /// 2-clone drop pattern with a running snapshot.
    #[tokio::test]
    async fn drop_runs_once_per_arc_not_per_clone() {
        use crate::dispatcher::{DryRunDispatcher, into_dyn};
        use crate::store::InMemoryStateStore;

        let mut s = snap(3);
        s.lifecycle.last_observed_state = Some(JobStatus {
            state: JobState::Running,
            reason: JobReason::None,
        });
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(DryRunDispatcher);
        let h1 = SbatchJobHandle::new(s, store, dispatcher);
        let h2 = h1.clone();
        drop(h1);
        drop(h2);
    }
}
