//! Cross-backend integration test for `JobManager` (issue #16 item 2).
//!
//! Mirrors `tests/job_handle_common.rs`: a compile-time check that both
//! managers satisfy the trait (including the `spawn` signature), plus a
//! generic runtime contract for the attach pair. `spawn` runtime behavior
//! is covered by the per-backend unit tests in `src/tssrun/manager.rs` and
//! `src/sbatch/manager/tests.rs` — this file focuses on the shape contract
//! the trait enforces on the public crate API.
//!
//! Adding a new backend? Add a `_check::<YourManager>()` line and a fixture
//! invocation of `assert_manager_attach_contract`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use uuid::Uuid;

use slurm_async_runner::sbatch::handle::{LogPathSpec, SbatchLifecycle};
use slurm_async_runner::tssrun::handle::{FinishedInfo as TssrunFinishedInfo, LogLocations};
use slurm_async_runner::{
    FileSystemStateStore, InMemoryStateStore, JobHandleCommon, JobManager, JobReason, JobState,
    JobStateStore, JobStatus, SbatchCmd, SbatchFinishedInfo, SbatchJobSnapshot, SbatchManager,
    TssrunCmd, TssrunJobSnapshot, TssrunManager,
};

/// Compile-time assertion that the named type implements `JobManager`
/// (which includes the `spawn` method signature and the typed error
/// associated types).
#[allow(dead_code)]
fn _assert_impls() {
    fn _check<M: JobManager>() {}
    _check::<SbatchManager>();
    _check::<TssrunManager>();
}

/// Generic runtime contract for the attach pair: `attach_uuid` is the
/// primary-key lookup, `attach_jobid` resolves to the same job, and an
/// unknown uuid fails with the backend's typed `AttachError`.
async fn assert_manager_attach_contract<M>(mgr: &M, uuid: Uuid, jobid: u64)
where
    M: JobManager,
{
    let Ok(by_uuid) = mgr.attach_uuid(uuid).await else {
        panic!("attach_uuid({uuid}) should resolve the persisted snapshot")
    };
    assert_eq!(JobHandleCommon::uuid(&by_uuid), uuid);
    assert_eq!(JobHandleCommon::jobid(&by_uuid), Some(jobid));

    let Ok(by_jobid) = mgr.attach_jobid(jobid).await else {
        panic!("attach_jobid({jobid}) should resolve the persisted snapshot")
    };
    assert_eq!(JobHandleCommon::uuid(&by_jobid), uuid);

    let missing = Uuid::now_v7();
    let err = match mgr.attach_uuid(missing).await {
        Ok(_) => panic!("attach_uuid({missing}) must fail for an unknown uuid"),
        Err(e) => e,
    };
    // The typed error must carry a useful Display (trait bound guarantees
    // std::error::Error; this pins non-empty messaging).
    assert!(
        !err.to_string().is_empty(),
        "AttachError Display must not be empty"
    );
}

// ─── Tssrun fixture ────────────────────────────────────────────────

#[tokio::test]
async fn tssrun_manager_satisfies_attach_contract() {
    let uuid = Uuid::now_v7();
    let jobid = 42_u64;
    let snap = TssrunJobSnapshot {
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
    };
    let store: Arc<dyn JobStateStore<TssrunJobSnapshot>> =
        Arc::new(InMemoryStateStore::<TssrunJobSnapshot>::new());
    store.save(&snap).await.unwrap();
    let mgr = TssrunManager::new(TssrunCmd::new("/bin/true")).with_state_store(store);

    assert_manager_attach_contract(&mgr, uuid, jobid).await;
}

// ─── Sbatch fixture ────────────────────────────────────────────────

#[tokio::test]
async fn sbatch_manager_satisfies_attach_contract() {
    let uuid = Uuid::now_v7();
    let jobid = 7777_u64;
    let snap = SbatchJobSnapshot {
        uuid,
        jobid,
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
    };
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> =
        Arc::new(FileSystemStateStore::<SbatchJobSnapshot>::new(tmp.path()));
    store.save(&snap).await.unwrap();
    let mgr =
        SbatchManager::new(SbatchCmd::new(PathBuf::from("/tmp/job.sh"))).with_state_store(store);

    assert_manager_attach_contract(&mgr, uuid, jobid).await;
}
