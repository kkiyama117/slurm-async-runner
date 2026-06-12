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

use crate::dispatcher::CaptureOutput;

/// stdout-only success [`CaptureOutput`] — the common canned shape.
fn cap_ok(stdout: &str) -> CaptureOutput {
    CaptureOutput {
        exit_code: 0,
        stdout: stdout.to_string(),
        stderr: String::new(),
    }
}

/// Mock dispatcher: returns canned outputs keyed by argv[0].
/// argv[0] = "qgroup" → first canned, "squeue" → second, "sacct" → third.
/// The constructor takes stdout-only `&str` values for ergonomics;
/// tests that need a nonzero exit / stderr replace the whole
/// `CaptureOutput` through the field's `Mutex`.
struct CannedDispatcher {
    qgroup: std::sync::Mutex<CaptureOutput>,
    squeue: std::sync::Mutex<CaptureOutput>,
    sacct: std::sync::Mutex<CaptureOutput>,
    sacct_call_count: std::sync::Mutex<u32>,
}

impl CannedDispatcher {
    fn new(qgroup: &str, squeue: &str, sacct: &str) -> Self {
        Self {
            qgroup: std::sync::Mutex::new(cap_ok(qgroup)),
            squeue: std::sync::Mutex::new(cap_ok(squeue)),
            sacct: std::sync::Mutex::new(cap_ok(sacct)),
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

    async fn capture(&self, argv: &[String]) -> anyhow::Result<CaptureOutput> {
        let bin = argv[0].as_str();
        let out = match bin {
            "qgroup" => self.qgroup.lock().unwrap().clone(),
            "squeue" => self.squeue.lock().unwrap().clone(),
            "sacct" => {
                *self.sacct_call_count.lock().unwrap() += 1;
                self.sacct.lock().unwrap().clone()
            }
            _ => CaptureOutput::default(),
        };
        Ok(out)
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

/// Idempotency: once `finished` is populated and re-readable from the
/// watch channel (requires `send_replace`, not `send`), subsequent
/// `refresh_with_sacct` calls must short-circuit and not re-invoke
/// sacct. Regression guard for the live KUDPC failure where every
/// call burned a sacct invocation because the watch channel never
/// updated due to receiver_count == 0.
#[tokio::test]
async fn refresh_with_sacct_is_idempotent_when_finished_already_set() {
    use crate::dispatcher::into_dyn;
    use crate::store::InMemoryStateStore;
    let s = snap(12345);
    let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
    let canned = std::sync::Arc::new(CannedDispatcher::new("", "", "12345|COMPLETED|None|0:0\n"));
    let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
    let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
    let _first = h.refresh_with_sacct().await.unwrap();
    assert_eq!(canned.sacct_calls(), 1);
    let _second = h.refresh_with_sacct().await.unwrap();
    assert_eq!(
        canned.sacct_calls(),
        1,
        "second call must not re-invoke sacct (idempotency via send_replace)",
    );
}

/// Regression: when qgroup -l reports a *terminal* state (FINI / CMP
/// → `Completed`), `refresh_with_sacct` must still call sacct to
/// resolve `exit_code`. Previously it short-circuited on
/// `!left_active_listing`, leaving `lifecycle.finished == None` and
/// `is_finished() == false` even after the post-`wait_terminal` call.
/// Live KUDPC reproducer: a 5-second job whose qgroup row reads
/// `FINI` while still in the listing.
#[tokio::test]
async fn refresh_with_sacct_calls_sacct_when_qgroup_reports_terminal_fini() {
    use crate::dispatcher::into_dyn;
    use crate::store::InMemoryStateStore;
    let s = snap(12345);
    let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
    let canned = std::sync::Arc::new(CannedDispatcher::new(
        // KUDPC pipe layout, terminal token FINI — job still listed
        // (so left_active_listing stays false through refresh()).
        " gr u 12345 | FINI 2026-05-12 01:48 | 1 | 1 1 1M 00:00:05\n",
        "",
        "12345|COMPLETED|None|0:0\n",
    ));
    let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
    let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
    let after = h.refresh_with_sacct().await.unwrap();
    assert!(
        !after.lifecycle.left_active_listing,
        "qgroup still listed the job; left_active_listing must stay false",
    );
    assert_eq!(
        canned.sacct_calls(),
        1,
        "terminal qgroup state must still trigger sacct for exit_code resolution",
    );
    let finished = after
        .lifecycle
        .finished
        .expect("FinishedInfo must be populated after sacct");
    assert_eq!(finished.final_state, crate::JobState::Completed);
    assert_eq!(finished.exit_code, Some(0));
}

#[tokio::test]
async fn refresh_with_sacct_calls_sacct_once_after_vanish() {
    use crate::dispatcher::into_dyn;
    use crate::store::InMemoryStateStore;
    let s = snap(12345);
    let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
    let canned = std::sync::Arc::new(CannedDispatcher::new("", "", "12345|COMPLETED|None|0:0\n"));
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
    let canned = std::sync::Arc::new(CannedDispatcher::new("", "", "12345|COMPLETED|None|0:0\n"));
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
    let canned = std::sync::Arc::new(CannedDispatcher::new("", "", "12345|CANCELLED|None|0:9\n"));
    let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
    let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
    let after = h.refresh_with_sacct().await.unwrap();
    let finished = after.lifecycle.finished.expect("finished should be Some");
    // SIGKILL = 9 -> 128 + 9 = 137
    assert_eq!(finished.exit_code, Some(137));
}

// ---- Array-task refresh (issue #8 A5, spec §5.5) ----

/// Build an array-task snapshot rooted at `master_jobid` with task
/// index `idx`. The `CannedDispatcher` does not inspect argv beyond
/// `argv[0]`, so the test rig does not need to know about the
/// `<master>_<idx>` key — the snapshot wiring carries it.
fn snap_array_task(master_jobid: u64, idx: u32) -> SbatchJobSnapshot {
    let mut s = snap(master_jobid);
    s.array_task_id = Some(idx);
    s
}

#[tokio::test]
async fn refresh_array_task_uses_squeue_and_skips_qgroup() {
    use crate::dispatcher::into_dyn;
    use crate::store::InMemoryStateStore;

    // qgroup output deliberately contains the master jobid — proves
    // the array path takes the squeue branch instead of falling
    // through to qgroup.
    let s = snap_array_task(12345, 3);
    let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
    let dispatcher = into_dyn(CannedDispatcher::new(
        "QUEUE USER JOBID STATUS PROC\ngr u 12345 RUN 1\n",
        "RUNNING None\n",
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
async fn refresh_array_task_marks_vanished_when_squeue_empty() {
    use crate::dispatcher::into_dyn;
    use crate::store::InMemoryStateStore;

    // Empty squeue output means the task has left the active queue.
    let s = snap_array_task(12345, 3);
    let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
    let canned = std::sync::Arc::new(CannedDispatcher::new("", "", ""));
    let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
    let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
    let after = h.refresh().await.unwrap();
    assert!(after.lifecycle.left_active_listing);
    assert_eq!(
        canned.sacct_calls(),
        0,
        "refresh on an array task must NOT call sacct"
    );
}

#[tokio::test]
async fn refresh_with_sacct_array_task_populates_exit_code() {
    use crate::dispatcher::into_dyn;
    use crate::store::InMemoryStateStore;

    // The canned sacct returns a single parent row keyed by
    // `<master>_<idx>` (12345_3). The summary-mode parser would
    // discard this row because the jobid column is not a `u64`;
    // verify the array-task parser keeps it.
    let s = snap_array_task(12345, 3);
    let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
    let canned = std::sync::Arc::new(CannedDispatcher::new(
        "",
        "",
        "12345_3|COMPLETED|None|0:0\n12345_3.batch|COMPLETED|None|0:0\n",
    ));
    let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
    let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
    let after = h.refresh_with_sacct().await.unwrap();
    let finished = after.lifecycle.finished.expect("finished should be Some");
    assert_eq!(finished.final_state, crate::JobState::Completed);
    assert_eq!(finished.exit_code, Some(0));
    assert_eq!(canned.sacct_calls(), 1);
}

/// Regression (vanished-jobid classification): once the array master
/// has left squeue entirely, `squeue -j <master>_<idx>` prints
/// `slurm_load_jobs error: Invalid job id specified` on stderr and
/// exits 1 (KUDPC purges terminated jobs from the queue immediately).
/// That combination is the *vanish* signal — the refresh must set
/// `left_active_listing = true`, never record a bogus observation,
/// and never treat it as a query failure.
#[tokio::test]
async fn refresh_array_task_treats_stderr_only_squeue_as_vanished() {
    use crate::dispatcher::into_dyn;
    use crate::store::InMemoryStateStore;

    let s = snap_array_task(12345, 3);
    let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
    let canned = std::sync::Arc::new(CannedDispatcher::new("", "", ""));
    *canned.squeue.lock().unwrap() = CaptureOutput {
        exit_code: 1,
        stdout: String::new(),
        stderr: "slurm_load_jobs error: Invalid job id specified\n".into(),
    };
    let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
    let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
    let after = h.refresh().await.unwrap();
    assert!(
        after.lifecycle.left_active_listing,
        "vanished-jobid squeue failure must mark the task as vanished, got {:?}",
        after.lifecycle
    );
    assert_eq!(canned.sacct_calls(), 0, "refresh must NOT call sacct");
}

fn transient_squeue_failure() -> CaptureOutput {
    CaptureOutput {
        exit_code: 1,
        stdout: String::new(),
        stderr: "slurm_load_jobs error: Socket timed out on send/recv operation\n".into(),
    }
}

/// A transient squeue failure (controller overload prints `Socket
/// timed out` on stderr, exits 1) must NOT be misread as a vanish:
/// `refresh()` propagates the error and the persisted snapshot keeps
/// `left_active_listing == false`. This was the root cause of the
/// false-vanish regressions — previously the failure looked like an
/// empty listing.
#[tokio::test]
async fn refresh_propagates_transient_squeue_failure() {
    use crate::dispatcher::into_dyn;
    use crate::store::InMemoryStateStore;

    let s = snap(12345);
    let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
    // qgroup: empty success (a miss) → falls through to squeue, which
    // fails transiently.
    let canned = std::sync::Arc::new(CannedDispatcher::new("", "", ""));
    *canned.squeue.lock().unwrap() = transient_squeue_failure();
    let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
    let h = SbatchJobHandle::new(s.clone(), store, dispatcher);

    let err = h.refresh().await.expect_err("transient failure must error");
    let msg = format!("{err:#}");
    assert!(msg.contains("squeue"), "tool name expected, got: {msg}");
    assert!(
        !h.snapshot().lifecycle.left_active_listing,
        "transient failure must not persist a fake vanish"
    );
}

/// Array-task variant of the transient-failure propagation.
#[tokio::test]
async fn refresh_array_task_propagates_transient_squeue_failure() {
    use crate::dispatcher::into_dyn;
    use crate::store::InMemoryStateStore;

    let s = snap_array_task(12345, 3);
    let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
    let canned = std::sync::Arc::new(CannedDispatcher::new("", "", ""));
    *canned.squeue.lock().unwrap() = transient_squeue_failure();
    let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
    let h = SbatchJobHandle::new(s.clone(), store, dispatcher);

    let err = h.refresh().await.expect_err("transient failure must error");
    let msg = format!("{err:#}");
    assert!(msg.contains("squeue"), "tool name expected, got: {msg}");
    assert!(
        !h.snapshot().lifecycle.left_active_listing,
        "transient failure must not persist a fake vanish"
    );
    assert_eq!(canned.sacct_calls(), 0, "refresh must NOT call sacct");
}

/// Regression (sacct-lag freeze): when the job has vanished from the
/// active listings but sacct has no row *yet* (accounting flush lag),
/// `refresh_with_sacct` must leave `finished` unset so a later call
/// can retry — previously it stamped
/// `FinishedInfo { final_state: Unknown, exit_code: None }` and the
/// idempotency short-circuit froze that fabricated outcome forever.
#[tokio::test]
async fn refresh_with_sacct_leaves_finished_unset_until_sacct_reports() {
    use crate::dispatcher::into_dyn;
    use crate::store::InMemoryStateStore;

    let s = snap(12345);
    let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
    // qgroup & squeue both miss (vanish) AND sacct is still empty.
    let canned = std::sync::Arc::new(CannedDispatcher::new("", "", ""));
    let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
    let h = SbatchJobHandle::new(s.clone(), store, dispatcher);

    let first = h.refresh_with_sacct().await.unwrap();
    assert!(first.lifecycle.left_active_listing);
    assert!(
        first.lifecycle.finished.is_none(),
        "sacct miss must not freeze a fabricated Unknown outcome"
    );

    // Accounting catches up — the next call must resolve for real.
    *canned.sacct.lock().unwrap() = cap_ok("12345|COMPLETED|None|0:0\n");
    let second = h.refresh_with_sacct().await.unwrap();
    let finished = second
        .lifecycle
        .finished
        .expect("finished must resolve once sacct reports the row");
    assert_eq!(finished.final_state, crate::JobState::Completed);
    assert_eq!(finished.exit_code, Some(0));
    assert_eq!(
        canned.sacct_calls(),
        2,
        "each unresolved call retries sacct"
    );
}

/// Array-task variant of the sacct-lag retry: empty sacct leaves
/// `finished` unset; a later call resolves from the per-task row.
#[tokio::test]
async fn refresh_with_sacct_array_task_retries_after_sacct_lag() {
    use crate::dispatcher::into_dyn;
    use crate::store::InMemoryStateStore;

    let s = snap_array_task(12345, 3);
    let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
    let canned = std::sync::Arc::new(CannedDispatcher::new("", "", ""));
    let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
    let h = SbatchJobHandle::new(s.clone(), store, dispatcher);

    let first = h.refresh_with_sacct().await.unwrap();
    assert!(first.lifecycle.finished.is_none());

    *canned.sacct.lock().unwrap() = cap_ok("12345_3|COMPLETED|None|0:0\n");
    let second = h.refresh_with_sacct().await.unwrap();
    let finished = second.lifecycle.finished.expect("resolved after lag");
    assert_eq!(finished.final_state, crate::JobState::Completed);
    assert_eq!(finished.exit_code, Some(0));
}

// -------- read_last_lines (tail read) --------

/// Equivalence with the naive whole-file implementation for files
/// smaller than one chunk.
#[tokio::test]
async fn read_last_lines_small_file_matches_naive_tail() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log.out");
    tokio::fs::write(&path, "a\nb\nc\nd\n").await.unwrap();
    assert_eq!(read_last_lines(&path, 2).await.unwrap(), vec!["c", "d"]);
    assert_eq!(
        read_last_lines(&path, 10).await.unwrap(),
        vec!["a", "b", "c", "d"],
        "n larger than the file returns every line"
    );
    assert_eq!(
        read_last_lines(&path, 0).await.unwrap(),
        Vec::<String>::new()
    );
}

/// A file spanning many read chunks must yield exactly the last `n`
/// lines without loading the whole file (correctness side; the memory
/// bound is structural — the loop stops once `n` newlines are seen).
#[tokio::test]
async fn read_last_lines_large_file_returns_exact_tail() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("big.out");
    // ~100 bytes × 2000 lines ≈ 200 KiB — dozens of 8 KiB chunks.
    let mut content = String::new();
    for i in 0..2000 {
        content.push_str(&format!("line-{i:05} {}\n", "x".repeat(90)));
    }
    tokio::fs::write(&path, &content).await.unwrap();
    let tail = read_last_lines(&path, 3).await.unwrap();
    assert_eq!(tail.len(), 3);
    assert!(tail[0].starts_with("line-01997"));
    assert!(tail[2].starts_with("line-01999"));
}

/// `lines()` semantics: a final line without a trailing newline still
/// counts as a line (matches the previous whole-file implementation).
#[tokio::test]
async fn read_last_lines_includes_unterminated_final_line() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log.out");
    tokio::fs::write(&path, "first\nsecond\nno-newline-tail")
        .await
        .unwrap();
    assert_eq!(
        read_last_lines(&path, 2).await.unwrap(),
        vec!["second", "no-newline-tail"]
    );
}

/// Multibyte (UTF-8) content must survive chunk-boundary splits —
/// the buffer is only decoded after assembly and the potentially
/// broken leading line is always outside the returned tail.
#[tokio::test]
async fn read_last_lines_multibyte_content_across_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("jp.out");
    let mut content = String::new();
    for i in 0..2000 {
        content.push_str(&format!("行{i:05}-日本語テキスト{}\n", "あ".repeat(30)));
    }
    tokio::fs::write(&path, &content).await.unwrap();
    let tail = read_last_lines(&path, 2).await.unwrap();
    assert_eq!(tail.len(), 2);
    assert!(tail[0].starts_with("行01998"));
    assert!(tail[1].starts_with("行01999"));
    assert!(
        !tail[0].contains('\u{FFFD}'),
        "no replacement chars: {tail:?}"
    );
}

#[tokio::test]
async fn read_last_lines_empty_file_returns_empty() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty.out");
    tokio::fs::write(&path, "").await.unwrap();
    assert_eq!(
        read_last_lines(&path, 5).await.unwrap(),
        Vec::<String>::new()
    );
}

/// Regression (non-terminal sacct row freeze): a transient squeue
/// failure (controller overload prints `Socket timed out` on stderr,
/// exits 1, stdout empty) is indistinguishable from a vanish, so
/// `left_active_listing` flips to `true` while the job is still
/// running. A subsequent `refresh_with_sacct` then gets a live
/// `RUNNING|…|0:0` row — which slips past the no-row sentinel
/// (`Unknown && exit_code.is_none()`) and froze
/// `FinishedInfo { final_state: Running, exit_code: Some(0) }`
/// behind the idempotency short-circuit. The fix: only a terminal
/// state may be stamped; a live row instead rolls back the vanish
/// flag so normal polling resumes.
#[tokio::test]
async fn refresh_with_sacct_rolls_back_false_vanish_on_live_sacct_row() {
    use crate::dispatcher::into_dyn;
    use crate::store::InMemoryStateStore;

    let s = snap(12345);
    let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
    // qgroup & squeue empty (transient miss) but sacct proves the job
    // is alive.
    let canned = std::sync::Arc::new(CannedDispatcher::new(
        "",
        "",
        "12345|RUNNING|None|0:0\n12345.batch|RUNNING|None|0:0\n",
    ));
    let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
    let h = SbatchJobHandle::new(s.clone(), store, dispatcher);

    let first = h.refresh_with_sacct().await.unwrap();
    assert!(
        first.lifecycle.finished.is_none(),
        "a non-terminal sacct row must never be stamped as FinishedInfo, got {:?}",
        first.lifecycle.finished
    );
    assert!(
        !first.lifecycle.left_active_listing,
        "a live sacct row must roll back the false vanish flag"
    );
    assert_eq!(
        first.lifecycle.last_observed_state.as_ref().unwrap().state,
        crate::JobState::Running,
        "the live observation must be recorded"
    );
    assert!(h.is_running(), "handle must report running again");

    // The job later finishes for real: squeue stays empty, sacct now
    // reports the terminal row — the normal resolve path must work.
    *canned.sacct.lock().unwrap() = cap_ok("12345|COMPLETED|None|0:0\n");
    let second = h.refresh_with_sacct().await.unwrap();
    let finished = second
        .lifecycle
        .finished
        .expect("terminal sacct row must resolve normally");
    assert_eq!(finished.final_state, crate::JobState::Completed);
    assert_eq!(finished.exit_code, Some(0));
}

/// Array-task variant of the non-terminal-row rollback.
#[tokio::test]
async fn refresh_with_sacct_array_task_rolls_back_false_vanish_on_live_row() {
    use crate::dispatcher::into_dyn;
    use crate::store::InMemoryStateStore;

    let s = snap_array_task(12345, 3);
    let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
    let canned = std::sync::Arc::new(CannedDispatcher::new(
        "",
        "",
        "12345_3|RUNNING|None|0:0\n12345_3.batch|RUNNING|None|0:0\n",
    ));
    let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
    let h = SbatchJobHandle::new(s.clone(), store, dispatcher);

    let first = h.refresh_with_sacct().await.unwrap();
    assert!(first.lifecycle.finished.is_none());
    assert!(!first.lifecycle.left_active_listing);

    *canned.sacct.lock().unwrap() = cap_ok("12345_3|FAILED|NonZeroExitCode|3:0\n");
    let second = h.refresh_with_sacct().await.unwrap();
    let finished = second.lifecycle.finished.expect("terminal row resolves");
    assert_eq!(finished.final_state, crate::JobState::Failed);
    assert_eq!(finished.exit_code, Some(3));
}

/// Forward-compat lock-in: an *unrecognized* state token with a real
/// exit code (a future SLURM terminal state we don't know yet) must
/// still be stamped — the terminal guard only blocks *known*
/// non-terminal states. The no-row case stays covered by the
/// `Unknown && exit_code.is_none()` sentinel.
#[tokio::test]
async fn refresh_with_sacct_stamps_unknown_token_with_exit_code() {
    use crate::dispatcher::into_dyn;
    use crate::store::InMemoryStateStore;

    let s = snap(12345);
    let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
    let canned = std::sync::Arc::new(CannedDispatcher::new(
        "",
        "",
        "12345|SOME_FUTURE_STATE|None|0:0\n",
    ));
    let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
    let h = SbatchJobHandle::new(s.clone(), store, dispatcher);

    let after = h.refresh_with_sacct().await.unwrap();
    let finished = after
        .lifecycle
        .finished
        .expect("unknown token + exit code must still resolve (forward-compat)");
    assert_eq!(finished.final_state, crate::JobState::Unknown);
    assert_eq!(finished.exit_code, Some(0));
}

/// `qgroup` is a KUDPC-ism: on clusters where the binary does not
/// exist, spawning it fails with an `Err` (not a nonzero exit).
/// `refresh()` must treat that as a qgroup miss and fall back to
/// squeue instead of propagating the error to the caller.
#[tokio::test]
async fn refresh_falls_back_to_squeue_when_qgroup_spawn_fails() {
    use crate::dispatcher::{JobDispatcher, into_dyn};
    use crate::store::InMemoryStateStore;

    struct QgroupErrDispatcher;
    impl JobDispatcher for QgroupErrDispatcher {
        async fn run(&self, _argv: &[String]) -> anyhow::Result<i32> {
            unimplemented!()
        }
        async fn capture(&self, argv: &[String]) -> anyhow::Result<CaptureOutput> {
            match argv[0].as_str() {
                "qgroup" => Err(anyhow::anyhow!("failed to spawn `qgroup`")),
                "squeue" => Ok(cap_ok("12345 RUNNING None\n")),
                _ => Ok(CaptureOutput::default()),
            }
        }
    }

    let s = snap(12345);
    let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
    let dispatcher = into_dyn(QgroupErrDispatcher);
    let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
    let after = h
        .refresh()
        .await
        .expect("qgroup spawn failure must not propagate");
    assert_eq!(
        after.lifecycle.last_observed_state.unwrap().state,
        crate::JobState::Running
    );
    assert!(!after.lifecycle.left_active_listing);
}

/// Per-task tasks of one master submission must observe distinct
/// states even when the master would only have a single qgroup
/// summary row. This is the regression test for issue #8 A5: before
/// the fix, two handles cloned from the same master snapshot would
/// both see the same observed state on refresh.
#[tokio::test]
async fn refresh_array_tasks_observe_per_task_states() {
    use crate::dispatcher::{JobDispatcher, into_dyn};
    use crate::store::InMemoryStateStore;

    // Dispatcher: route squeue calls based on the `-j <key>` argv to
    // produce per-task responses. `argv[3]` carries the `<master>_<idx>`
    // key (after "squeue", "-h", "-j").
    struct PerTaskSqueue;
    impl JobDispatcher for PerTaskSqueue {
        async fn run(&self, _argv: &[String]) -> anyhow::Result<i32> {
            unimplemented!()
        }
        async fn capture(&self, argv: &[String]) -> anyhow::Result<CaptureOutput> {
            if argv[0] == "squeue" {
                let key = argv[3].as_str();
                return Ok(cap_ok(match key {
                    "12345_0" => "RUNNING None\n",
                    "12345_1" => "PENDING Priority\n",
                    _ => "",
                }));
            }
            Ok(CaptureOutput::default())
        }
    }

    let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
    let h0 = SbatchJobHandle::new(
        snap_array_task(12345, 0),
        store.clone(),
        into_dyn(PerTaskSqueue),
    );
    let h1 = SbatchJobHandle::new(
        snap_array_task(12345, 1),
        store.clone(),
        into_dyn(PerTaskSqueue),
    );

    let s0 = h0.refresh().await.unwrap();
    let s1 = h1.refresh().await.unwrap();
    assert_eq!(
        s0.lifecycle.last_observed_state.unwrap().state,
        crate::JobState::Running
    );
    let s1_state = s1.lifecycle.last_observed_state.unwrap();
    assert_eq!(s1_state.state, crate::JobState::Pending);
    assert_eq!(s1_state.reason, crate::JobReason::Priority);
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

/// Dispatcher whose squeue responses follow a script: pops the next
/// canned [`CaptureOutput`] per squeue call (the last entry repeats
/// once the script is exhausted). Counts squeue calls so tests can
/// assert exactly how many refresh attempts happened.
struct ScriptedSqueue {
    script: std::sync::Mutex<std::collections::VecDeque<CaptureOutput>>,
    last: CaptureOutput,
    squeue_calls: std::sync::Mutex<u32>,
}

impl ScriptedSqueue {
    fn new(script: Vec<CaptureOutput>) -> Self {
        let last = script.last().cloned().unwrap_or_default();
        Self {
            script: std::sync::Mutex::new(script.into_iter().collect()),
            last,
            squeue_calls: std::sync::Mutex::new(0),
        }
    }

    fn squeue_calls(&self) -> u32 {
        *self.squeue_calls.lock().unwrap()
    }
}

impl crate::dispatcher::JobDispatcher for ScriptedSqueue {
    async fn run(&self, _argv: &[String]) -> anyhow::Result<i32> {
        unimplemented!()
    }
    async fn capture(&self, argv: &[String]) -> anyhow::Result<CaptureOutput> {
        if argv[0] == "squeue" {
            *self.squeue_calls.lock().unwrap() += 1;
            let next = self.script.lock().unwrap().pop_front();
            return Ok(next.unwrap_or_else(|| self.last.clone()));
        }
        Ok(CaptureOutput::default())
    }
}

fn vanished_squeue_failure() -> CaptureOutput {
    CaptureOutput {
        exit_code: 1,
        stdout: String::new(),
        stderr: "slurm_load_jobs error: Invalid job id specified\n".into(),
    }
}

/// Phase C: a couple of transient `Socket timed out` hiccups during a
/// long `wait_terminal` poll must not abort the wait — the loop keeps
/// polling and succeeds once squeue reports the terminal vanish.
#[tokio::test]
async fn wait_terminal_tolerates_transient_refresh_failures() {
    use crate::dispatcher::into_dyn;
    use crate::store::InMemoryStateStore;

    let s = snap_array_task(12345, 3);
    let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
    let scripted = std::sync::Arc::new(ScriptedSqueue::new(vec![
        transient_squeue_failure(),
        transient_squeue_failure(),
        vanished_squeue_failure(),
    ]));
    let dispatcher = into_dyn(MoveDispatcher(scripted.clone()));
    let h = SbatchJobHandle::new(s.clone(), store, dispatcher);

    let after = h
        .wait_terminal(std::time::Duration::from_millis(1))
        .await
        .expect("two transient failures then a vanish must succeed");
    assert!(after.lifecycle.left_active_listing);
    assert_eq!(scripted.squeue_calls(), 3);
}

/// Phase C: persistent refresh failure must still propagate — after
/// exactly `MAX_CONSECUTIVE_REFRESH_FAILURES` (5) consecutive failed
/// refresh attempts, `wait_terminal` returns the error instead of
/// polling forever against a dead controller.
#[tokio::test]
async fn wait_terminal_propagates_persistent_refresh_failure() {
    use crate::dispatcher::into_dyn;
    use crate::store::InMemoryStateStore;

    let s = snap_array_task(12345, 3);
    let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
    let scripted = std::sync::Arc::new(ScriptedSqueue::new(vec![transient_squeue_failure()]));
    let dispatcher = into_dyn(MoveDispatcher(scripted.clone()));
    let h = SbatchJobHandle::new(s.clone(), store, dispatcher);

    let err = h
        .wait_terminal(std::time::Duration::from_millis(1))
        .await
        .expect_err("persistent failure must propagate");
    let msg = format!("{err:#}");
    assert!(msg.contains("squeue"), "tool name expected, got: {msg}");
    assert_eq!(
        scripted.squeue_calls(),
        5,
        "exactly MAX_CONSECUTIVE_REFRESH_FAILURES attempts before giving up"
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
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
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
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
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
