//! Phase 3 P3 cross-backend integration test for `JobHandleCommon`.
//!
//! Verifies that:
//! 1. Both `SbatchJobHandle` and `TssrunJobHandle` satisfy
//!    `slurm_async_runner::JobHandleCommon` (compile-time check).
//! 2. The trait getters produce the same observations as the inherent
//!    methods on each backend (runtime check via a generic helper).
//!
//! The async portion of the trait (`refresh` / `wait_terminal`) is exercised
//! by the per-backend unit tests in `src/sbatch/handle.rs` and
//! `src/tssrun/handle.rs`. This integration test focuses on the shape
//! contract that the trait enforces on the public crate API.
//!
//! See `docs/superpowers/specs/2026-05-11-sbatch-phase3-design.md` §3.2.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use uuid::Uuid;

use slurm_async_runner::sbatch::handle::{LogPathSpec, SbatchLifecycle};
use slurm_async_runner::tssrun::handle::{FinishedInfo as TssrunFinishedInfo, LogLocations};
use slurm_async_runner::{
    FileSystemStateStore, InMemoryStateStore, JobHandleCommon, JobReason, JobState, JobStateStore,
    JobStatus, SbatchCmd, SbatchFinishedInfo, SbatchJobHandle, SbatchJobSnapshot, SbatchManager,
    TssrunJobHandle, TssrunJobSnapshot,
};

/// Compile-time assertion that the named type implements `JobHandleCommon`.
/// Adding a new backend? Add another call here.
#[allow(dead_code)]
fn _assert_impls() {
    fn _check<H: JobHandleCommon>() {}
    _check::<SbatchJobHandle>();
    _check::<TssrunJobHandle>();
}

/// Generic runtime contract: every `JobHandleCommon` impl must expose the
/// same observations through the trait as through its inherent methods.
async fn assert_common_contract<H>(handle: H, expected_uuid: Uuid, expected_jobid: Option<u64>)
where
    H: JobHandleCommon,
{
    // sync getters
    assert_eq!(JobHandleCommon::uuid(&handle), expected_uuid);
    assert_eq!(JobHandleCommon::jobid(&handle), expected_jobid);
    assert!(JobHandleCommon::is_finished(&handle));
    assert!(!JobHandleCommon::is_running(&handle));
    assert_eq!(JobHandleCommon::exit_code(&handle), Some(0));

    // snapshot accessor — round-trips JobSnapshot trait methods
    let snap = JobHandleCommon::snapshot(&handle);
    assert_eq!(
        <H::Snapshot as slurm_async_runner::JobSnapshot>::uuid(&snap),
        expected_uuid
    );
    assert_eq!(
        <H::Snapshot as slurm_async_runner::JobSnapshot>::jobid(&snap),
        expected_jobid
    );

    // watch subscriber sees the same snapshot
    let rx = JobHandleCommon::watch(&handle);
    assert_eq!(
        <H::Snapshot as slurm_async_runner::JobSnapshot>::uuid(&rx.borrow()),
        expected_uuid
    );
}

// ─── Tssrun fixture ────────────────────────────────────────────────

fn finished_tssrun_snapshot(uuid: Uuid, jobid: u64) -> TssrunJobSnapshot {
    TssrunJobSnapshot {
        uuid,
        pid: 31415,
        argv: vec!["bash".into(), "/tmp/ok.sh".into()],
        sent_env: HashMap::new(),
        cwd: None,
        started_at_unix: 0,
        log_locations: LogLocations::None,
        jobid: Some(jobid),
        node: Some("cnode1".into()),
        finished: Some(TssrunFinishedInfo {
            exit_code: Some(0),
            finished_at_unix: 1,
        }),
    }
}

#[tokio::test]
async fn tssrun_handle_satisfies_common_contract() {
    let uuid = Uuid::now_v7();
    let jobid = 42_u64;
    let store: Arc<dyn JobStateStore<TssrunJobSnapshot>> =
        Arc::new(InMemoryStateStore::<TssrunJobSnapshot>::new());
    let snap = finished_tssrun_snapshot(uuid, jobid);
    store.save(&snap).await.unwrap();

    let handle = TssrunJobHandle::attach_snapshot(snap, Some(store));
    assert_common_contract(handle, uuid, Some(jobid)).await;
}

// ─── Sbatch fixture ────────────────────────────────────────────────

fn finished_sbatch_snapshot(uuid: Uuid, jobid: u64) -> SbatchJobSnapshot {
    SbatchJobSnapshot {
        uuid,
        jobid,
        array_jobid: None,
        array_task_id: None,
        argv: vec!["sbatch".into(), "/tmp/job.sh".into()],
        sent_env: HashMap::new(),
        script_path: PathBuf::from("/tmp/job.sh"),
        chdir: None,
        partition: None,
        job_name: None,
        submitted_at: Utc::now(),
        log: LogPathSpec::default(),
        lifecycle: SbatchLifecycle {
            last_observed_state: Some(JobStatus::with_reason(JobState::Completed, JobReason::None)),
            last_observed_at: Some(Utc::now()),
            left_active_listing: true,
            finished: Some(SbatchFinishedInfo {
                final_state: JobState::Completed,
                final_reason: JobReason::None,
                exit_code: Some(0),
                finished_at: Utc::now(),
            }),
        },
    }
}

#[tokio::test]
async fn sbatch_handle_satisfies_common_contract() {
    let uuid = Uuid::now_v7();
    let jobid = 7777_u64;

    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> =
        Arc::new(FileSystemStateStore::<SbatchJobSnapshot>::new(tmp.path()));
    let snap = finished_sbatch_snapshot(uuid, jobid);
    store.save(&snap).await.unwrap();

    let cmd = SbatchCmd::new(PathBuf::from("/tmp/job.sh"));
    let manager = SbatchManager::new(cmd).with_state_store(store);
    let handle = manager.attach_uuid(uuid).await.unwrap();

    assert_common_contract(handle, uuid, Some(jobid)).await;
}
