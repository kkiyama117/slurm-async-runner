//! `SbatchJobSnapshot` and supporting lifecycle types. The runtime
//! handle (`SbatchJobHandle`) is appended in Task 9.
//!
//! See `docs/superpowers/specs/2026-05-10-sbatch-module-design.md` §6
//! and §9 for the full design.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

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

    pub fn exit_code(&self) -> Option<i32> {
        self.finished.as_ref().and_then(|f| f.exit_code)
    }
}

impl SbatchJobSnapshot {
    pub fn output_path(&self) -> Option<PathBuf> {
        self.log
            .output_template
            .as_deref()
            .map(|t| resolve_log_path(t, self.jobid, self.job_name.as_deref()))
    }

    pub fn error_path(&self) -> Option<PathBuf> {
        self.log
            .error_template
            .as_deref()
            .map(|t| resolve_log_path(t, self.jobid, self.job_name.as_deref()))
    }

    pub fn is_running(&self) -> bool {
        self.lifecycle.is_running()
    }
    pub fn is_finished(&self) -> bool {
        self.lifecycle.is_finished()
    }
    pub fn exit_code(&self) -> Option<i32> {
        self.lifecycle.exit_code()
    }
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

    pub fn exit_code(&self) -> Option<i32> {
        self.0.snapshot_tx.borrow().exit_code()
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(jobid: u64) -> SbatchJobSnapshot {
        SbatchJobSnapshot {
            uuid: Uuid::now_v7(),
            jobid,
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

    /// Newtype that wraps an `Arc<CannedDispatcher>` so it can be passed
    /// to `into_dyn` (which requires `D: JobDispatcher + Send + Sync + 'static`)
    /// while leaving the original Arc available for assertion.
    struct MoveDispatcher(std::sync::Arc<CannedDispatcher>);

    impl crate::dispatcher::JobDispatcher for MoveDispatcher {
        async fn run(&self, argv: &[String]) -> anyhow::Result<i32> {
            self.0.run(argv).await
        }

        async fn capture(&self, argv: &[String]) -> anyhow::Result<(i32, String)> {
            self.0.capture(argv).await
        }
    }

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
}
