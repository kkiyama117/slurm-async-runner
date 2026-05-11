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

use slurm_async_runner::handle::into_dyn;
use slurm_async_runner::sbatch::handle::{LogPathSpec, SbatchLifecycle};
use slurm_async_runner::tssrun::handle::{FinishedInfo as TssrunFinishedInfo, LogLocations};
use slurm_async_runner::{
    DynJobHandleCommon, FileSystemStateStore, InMemoryStateStore, JobHandleCommon, JobReason,
    JobState, JobStateStore, JobStatus, SbatchCmd, SbatchFinishedInfo, SbatchJobHandle,
    SbatchJobSnapshot, SbatchManager, TssrunJobHandle, TssrunJobSnapshot,
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

async fn build_finished_tssrun_handle(uuid: Uuid, jobid: u64) -> TssrunJobHandle {
    let store: Arc<dyn JobStateStore<TssrunJobSnapshot>> =
        Arc::new(InMemoryStateStore::<TssrunJobSnapshot>::new());
    let snap = finished_tssrun_snapshot(uuid, jobid);
    store.save(&snap).await.unwrap();
    TssrunJobHandle::attach_snapshot(snap, Some(store))
}

#[tokio::test]
async fn tssrun_handle_satisfies_common_contract() {
    let uuid = Uuid::now_v7();
    let jobid = 42_u64;
    let handle = build_finished_tssrun_handle(uuid, jobid).await;
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

/// Build a finished sbatch handle backed by a fresh tempdir + filesystem
/// store. Returns `(handle, tempdir_guard)` — the guard must outlive the
/// handle to keep the state files on disk.
async fn build_finished_sbatch_handle(
    uuid: Uuid,
    jobid: u64,
) -> (SbatchJobHandle, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> =
        Arc::new(FileSystemStateStore::<SbatchJobSnapshot>::new(tmp.path()));
    let snap = finished_sbatch_snapshot(uuid, jobid);
    store.save(&snap).await.unwrap();

    let cmd = SbatchCmd::new(PathBuf::from("/tmp/job.sh"));
    let manager = SbatchManager::new(cmd).with_state_store(store);
    let handle = manager.attach_uuid(uuid).await.unwrap();
    (handle, tmp)
}

#[tokio::test]
async fn sbatch_handle_satisfies_common_contract() {
    let uuid = Uuid::now_v7();
    let jobid = 7777_u64;
    let (handle, _tmp) = build_finished_sbatch_handle(uuid, jobid).await;
    assert_common_contract(handle, uuid, Some(jobid)).await;
}

// ─── Phase 3 P4: type-erased DynJobHandleCommon round-trip ────────────

#[tokio::test]
async fn into_dyn_sbatch_preserves_kind_and_getters() {
    let uuid = Uuid::now_v7();
    let jobid = 8888_u64;
    let (h, _tmp) = build_finished_sbatch_handle(uuid, jobid).await;
    let expected_uuid = h.uuid();
    let expected_jobid = h.jobid();

    let dyn_h: Arc<dyn DynJobHandleCommon> = into_dyn(h);

    assert_eq!(dyn_h.uuid(), expected_uuid);
    assert_eq!(dyn_h.jobid(), expected_jobid);
    assert_eq!(dyn_h.kind(), "sbatch");
    assert!(dyn_h.is_finished());
    assert!(!dyn_h.is_running());
    assert_eq!(dyn_h.exit_code(), Some(0));

    let json = dyn_h.snapshot_json();
    assert!(json.is_object(), "snapshot_json should be a JSON object");
    assert_eq!(
        json.get("uuid").and_then(|v| v.as_str()),
        Some(expected_uuid.to_string().as_str()),
        "uuid must round-trip into JSON: {json}"
    );
    assert_eq!(json.get("jobid").and_then(|v| v.as_u64()), Some(jobid));
    // The static `kind` discriminator is exposed separately via `kind()`
    // and is NOT part of the snapshot's serde body — the file-store writes
    // it as an envelope sibling, not nested inside the snapshot fields.
}

#[tokio::test]
async fn into_dyn_tssrun_preserves_kind_and_getters() {
    let uuid = Uuid::now_v7();
    let jobid = 99_u64;
    let h = build_finished_tssrun_handle(uuid, jobid).await;
    let expected_uuid = h.uuid();

    let dyn_h: Arc<dyn DynJobHandleCommon> = into_dyn(h);

    assert_eq!(dyn_h.uuid(), expected_uuid);
    assert_eq!(dyn_h.jobid(), Some(jobid));
    assert_eq!(dyn_h.kind(), "tssrun");
    assert!(dyn_h.is_finished());

    let json = dyn_h.snapshot_json();
    assert!(json.is_object());
    assert_eq!(
        json.get("uuid").and_then(|v| v.as_str()),
        Some(expected_uuid.to_string().as_str()),
        "uuid must round-trip into JSON: {json}"
    );
    assert_eq!(json.get("jobid").and_then(|v| v.as_u64()), Some(jobid));
}

#[tokio::test]
async fn dyn_handles_can_be_held_in_a_heterogenous_vec() {
    let sbatch_uuid = Uuid::now_v7();
    let tssrun_uuid = Uuid::now_v7();
    let (s, _tmp) = build_finished_sbatch_handle(sbatch_uuid, 1234).await;
    let t = build_finished_tssrun_handle(tssrun_uuid, 56).await;

    let handles: Vec<Arc<dyn DynJobHandleCommon>> = vec![into_dyn(s), into_dyn(t)];

    let kinds: Vec<&'static str> = handles.iter().map(|h| h.kind()).collect();
    assert_eq!(kinds, vec!["sbatch", "tssrun"]);

    let uuids: Vec<Uuid> = handles.iter().map(|h| h.uuid()).collect();
    assert_eq!(uuids, vec![sbatch_uuid, tssrun_uuid]);
}
