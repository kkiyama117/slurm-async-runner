use super::*;
use crate::dispatcher::{CaptureOutput, JobDispatcher, into_dyn};
use std::sync::Mutex;

/// stdout-only [`CaptureOutput`] with the given exit code — the
/// common shape for canned test responses.
fn cap(exit_code: i32, stdout: &str) -> CaptureOutput {
    CaptureOutput {
        exit_code,
        stdout: stdout.to_string(),
        stderr: String::new(),
    }
}

struct CannedSbatch {
    stdout: Mutex<String>,
    exit: Mutex<i32>,
}
impl CannedSbatch {
    fn ok(jobid: u64) -> Self {
        Self {
            stdout: Mutex::new(format!("Submitted batch job {jobid}\n")),
            exit: Mutex::new(0),
        }
    }
    fn failed() -> Self {
        Self {
            stdout: Mutex::new("error: bad partition\n".into()),
            exit: Mutex::new(1),
        }
    }
    fn ok_no_jobid() -> Self {
        Self {
            stdout: Mutex::new("warning but no parseable id\n".into()),
            exit: Mutex::new(0),
        }
    }
}
impl JobDispatcher for CannedSbatch {
    async fn run(&self, _argv: &[String]) -> Result<i32> {
        unimplemented!()
    }
    async fn capture(&self, _argv: &[String]) -> Result<CaptureOutput> {
        Ok(cap(
            *self.exit.lock().unwrap(),
            &self.stdout.lock().unwrap(),
        ))
    }
}

#[tokio::test]
async fn spawn_happy_path_returns_handle_with_jobid() {
    let cmd = SbatchCmd::new("/w/job.sh");
    let dispatcher = into_dyn(CannedSbatch::ok(12345));
    let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);
    let h = mgr.spawn().await.unwrap();
    assert_eq!(h.jobid(), Some(12345));
    assert_eq!(h.snapshot().argv[0], "sbatch");
}

#[tokio::test]
async fn spawn_returns_submit_failed_on_nonzero_exit() {
    let cmd = SbatchCmd::new("/w/job.sh");
    let dispatcher = into_dyn(CannedSbatch::failed());
    let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);
    let Err(err) = mgr.spawn().await else {
        panic!("spawn should fail")
    };
    assert!(matches!(
        err,
        SbatchSpawnError::SubmitFailed { exit_code: 1, .. }
    ));
}

#[tokio::test]
async fn spawn_returns_jobid_parse_error_when_stdout_is_garbage() {
    let cmd = SbatchCmd::new("/w/job.sh");
    let dispatcher = into_dyn(CannedSbatch::ok_no_jobid());
    let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);
    let Err(err) = mgr.spawn().await else {
        panic!("spawn should fail")
    };
    assert!(matches!(err, SbatchSpawnError::JobidParseError { .. }));
}

#[tokio::test]
async fn attach_uuid_round_trips_through_in_memory_store() {
    let cmd = SbatchCmd::new("/w/job.sh");
    let dispatcher = into_dyn(CannedSbatch::ok(99));
    let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);
    let h = mgr.spawn().await.unwrap();
    let attached = mgr.attach_uuid(h.uuid()).await.unwrap();
    assert_eq!(attached.jobid(), Some(99));
}

#[tokio::test]
async fn attach_jobid_finds_via_default_trait_impl() {
    let cmd = SbatchCmd::new("/w/job.sh");
    let dispatcher = into_dyn(CannedSbatch::ok(77));
    let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);
    let _ = mgr.spawn().await.unwrap();
    let attached = mgr.attach_jobid(77).await.unwrap();
    assert_eq!(attached.jobid(), Some(77));
}

#[tokio::test]
async fn attach_file_rejects_wrong_kind_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("wrong.json");
    std::fs::write(&path, r#"{"kind":"tssrun"}"#).unwrap();

    let cmd = SbatchCmd::new("/w/job.sh");
    let dispatcher = into_dyn(CannedSbatch::ok(1));
    let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);
    match mgr.attach_file(&path).await {
        Ok(_) => panic!("attach_file should fail on wrong kind"),
        Err(SbatchAttachError::KindMismatch { expected, got }) => {
            assert_eq!(expected, "sbatch");
            assert_eq!(got, "tssrun");
        }
        Err(other) => panic!("expected KindMismatch, got {other:?}"),
    }
}

#[tokio::test]
async fn attach_file_reads_disk_snapshot_directly() {
    let tmp = tempfile::tempdir().unwrap();
    let cmd = SbatchCmd::new("/w/job.sh");
    let dispatcher = into_dyn(CannedSbatch::ok(55));
    let mgr = SbatchManager::new(cmd)
        .with_state_dir(tmp.path())
        .with_dispatcher(dispatcher);
    let h = mgr.spawn().await.unwrap();
    let path = tmp.path().join(format!("{}.json", h.uuid()));
    assert!(path.exists());
    let attached = mgr.attach_file(&path).await.unwrap();
    assert_eq!(attached.jobid(), Some(55));
}

#[tokio::test]
async fn spawn_array_creates_one_snapshot_per_task() {
    use crate::entities::slurm::SlurmArraySpec;

    let cmd = SbatchCmd::new("/w/job.sh");
    let dispatcher = into_dyn(CannedSbatch::ok(50000));
    let tmp = tempfile::tempdir().unwrap();
    let mgr = SbatchManager::new(cmd)
        .with_state_dir(tmp.path())
        .with_dispatcher(dispatcher);

    let spec: SlurmArraySpec = "0-3".parse().unwrap();
    let handles = mgr.spawn_array(spec).await.unwrap();

    assert_eq!(handles.len(), 4);
    for (i, h) in handles.iter().enumerate() {
        let snap = h.snapshot();
        assert_eq!(snap.jobid, 50000);
        assert_eq!(snap.array_task_id, Some(i as u32));
        for (j, other) in handles.iter().enumerate() {
            if i != j {
                assert_ne!(h.uuid(), other.uuid());
            }
        }
    }

    let count = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .map(|e| e.path().extension().and_then(|s| s.to_str()) == Some("json"))
                .unwrap_or(false)
        })
        .count();
    assert_eq!(count, 4);
}

#[tokio::test]
async fn spawn_array_returns_submit_failed_on_nonzero_exit() {
    use crate::entities::slurm::SlurmArraySpec;

    let cmd = SbatchCmd::new("/w/job.sh");
    let dispatcher = into_dyn(CannedSbatch::failed());
    let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);

    let spec: SlurmArraySpec = "0-2".parse().unwrap();
    let Err(err) = mgr.spawn_array(spec).await else {
        panic!("spawn_array should fail");
    };
    assert!(matches!(
        err,
        SbatchSpawnError::SubmitFailed { exit_code: 1, .. }
    ));
}

#[tokio::test]
async fn attach_array_jobid_returns_all_tasks_sorted() {
    use crate::entities::slurm::SlurmArraySpec;

    let cmd = SbatchCmd::new("/w/job.sh");
    let dispatcher = into_dyn(CannedSbatch::ok(60000));
    let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);

    let spec: SlurmArraySpec = "0-2".parse().unwrap();
    let _spawned = mgr.spawn_array(spec).await.unwrap();

    let attached = mgr.attach_array_jobid(60000).await.unwrap();
    assert_eq!(attached.len(), 3);
    assert_eq!(attached[0].snapshot().array_task_id, Some(0));
    assert_eq!(attached[1].snapshot().array_task_id, Some(1));
    assert_eq!(attached[2].snapshot().array_task_id, Some(2));
}

#[tokio::test]
async fn attach_array_jobid_empty_when_no_match() {
    let cmd = SbatchCmd::new("/w/job.sh");
    let dispatcher = into_dyn(CannedSbatch::ok(1));
    let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);
    let attached = mgr.attach_array_jobid(99999).await.unwrap();
    assert!(attached.is_empty());
}

/// spec §5.6 / issue #8 A6: `attach_array_jobid` must inherit the
/// `kind = "sbatch"` peek that already guards the single-snapshot
/// attach paths. A `{"kind":"tssrun"}` JSON file sitting in the same
/// state dir with the same master jobid must be silently skipped —
/// never panicking, never being returned as a snapshot.
///
/// The kind check itself lives in
/// [`crate::store::decode_with_kind_check`]; this test just pins the
/// behaviour at the `attach_array_jobid` entry point so a future
/// refactor cannot accidentally bypass it.
#[tokio::test]
async fn attach_array_jobid_silently_skips_wrong_kind_file() {
    use crate::entities::slurm::SlurmArraySpec;

    let tmp = tempfile::tempdir().unwrap();
    let cmd = SbatchCmd::new("/w/job.sh");
    let dispatcher = into_dyn(CannedSbatch::ok(90001));
    let mgr = SbatchManager::new(cmd)
        .with_state_dir(tmp.path())
        .with_dispatcher(dispatcher);

    // Plant a tssrun-kind JSON file sharing the master jobid in the
    // state dir, BEFORE the array submission. `attach_array_jobid`
    // must skip it via the kind-peek filter and not surface it as a
    // snapshot.
    let intruder = tmp.path().join("intruder.json");
    std::fs::write(
        &intruder,
        r#"{"kind":"tssrun","jobid":90001,"uuid":"00000000-0000-0000-0000-000000000000"}"#,
    )
    .unwrap();

    // Now spawn a real 2-task array job under the same master jobid.
    let spec: SlurmArraySpec = "0-1".parse().unwrap();
    let _ = mgr.spawn_array(spec).await.unwrap();

    let attached = mgr.attach_array_jobid(90001).await.unwrap();
    assert_eq!(
        attached.len(),
        2,
        "tssrun-kind intruder must be silently skipped; only the 2 array tasks should attach"
    );
    // Sanity: both attached handles are real sbatch array tasks.
    assert!(attached[0].snapshot().array_task_id.is_some());
    assert!(attached[1].snapshot().array_task_id.is_some());
}

#[tokio::test]
async fn attach_jobid_returns_multiple_match_for_array_master() {
    use crate::entities::slurm::SlurmArraySpec;

    let cmd = SbatchCmd::new("/w/job.sh");
    let dispatcher = into_dyn(CannedSbatch::ok(70000));
    let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);

    let spec: SlurmArraySpec = "0-1".parse().unwrap();
    let _ = mgr.spawn_array(spec).await.unwrap();

    let Err(err) = mgr.attach_jobid(70000).await else {
        panic!("attach_jobid on array master should error")
    };
    match err {
        SbatchAttachError::MultipleMatch { jobid, count } => {
            assert_eq!(jobid, 70000);
            assert_eq!(count, 2);
        }
        other => panic!("expected MultipleMatch, got {other:?}"),
    }
}

/// Dispatcher that records every captured argv and returns canned exit/stdout.
struct RecordingDispatcher {
    responses: Mutex<std::collections::VecDeque<(i32, String)>>,
    seen: Mutex<Vec<Vec<String>>>,
}
impl RecordingDispatcher {
    fn new(responses: Vec<(i32, String)>) -> Self {
        Self {
            responses: Mutex::new(responses.into_iter().collect()),
            seen: Mutex::new(Vec::new()),
        }
    }
    fn seen(&self) -> Vec<Vec<String>> {
        self.seen.lock().unwrap().clone()
    }
}
impl JobDispatcher for RecordingDispatcher {
    async fn run(&self, _argv: &[String]) -> Result<i32> {
        unimplemented!()
    }
    async fn capture(&self, argv: &[String]) -> Result<CaptureOutput> {
        self.seen.lock().unwrap().push(argv.to_vec());
        let (exit_code, stdout) = self
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or((0, String::new()));
        Ok(cap(exit_code, &stdout))
    }
}

use crate::sbatch::test_util::ArcDispatcher;

#[tokio::test]
async fn cancel_invokes_scancel_with_jobid() {
    let cmd = SbatchCmd::new("/w/job.sh");
    let recorder = std::sync::Arc::new(RecordingDispatcher::new(vec![(0, String::new())]));
    let dispatcher = into_dyn(ArcDispatcher(recorder.clone()));
    let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);

    mgr.cancel(12345).await.expect("cancel should succeed");

    let seen = recorder.seen();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0][0], "scancel");
    assert_eq!(seen[0][1], "12345");
}

#[tokio::test]
async fn cancel_uses_scancel_bin_override_when_set() {
    let cmd = SbatchCmd::new("/w/job.sh");
    let recorder = std::sync::Arc::new(RecordingDispatcher::new(vec![(0, String::new())]));
    let dispatcher = into_dyn(ArcDispatcher(recorder.clone()));
    let mgr = SbatchManager::new(cmd)
        .with_dispatcher(dispatcher)
        .with_scancel_bin("/tmp/fake-scancel");

    mgr.cancel(99).await.expect("cancel should succeed");

    let seen = recorder.seen();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0][0], "/tmp/fake-scancel");
    assert_eq!(seen[0][1], "99");
}

#[tokio::test]
async fn cancel_returns_scancel_error_on_nonzero_exit() {
    let cmd = SbatchCmd::new("/w/job.sh");
    let recorder = std::sync::Arc::new(RecordingDispatcher::new(vec![(
        1,
        "scancel: error: Invalid job id".to_string(),
    )]));
    let dispatcher = into_dyn(ArcDispatcher(recorder));
    let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);

    match mgr.cancel(99).await {
        Err(SbatchCancelError::Scancel { exit_code, output }) => {
            assert_eq!(exit_code, 1);
            assert!(output.contains("Invalid job id"));
        }
        other => panic!("expected Scancel error, got {other:?}"),
    }
}

use crate::sbatch::error::SbatchRunError;

#[tokio::test]
async fn run_rejects_array_spec_before_spawn() {
    use crate::entities::slurm::SlurmArraySpec;

    let mut cmd = SbatchCmd::new("/w/job.sh");
    cmd.array_spec = Some("0-2".parse::<SlurmArraySpec>().unwrap());

    // Recorder must remain unused — guard fires before spawn touches sbatch.
    let recorder = std::sync::Arc::new(RecordingDispatcher::new(vec![]));
    let dispatcher = into_dyn(ArcDispatcher(recorder.clone()));
    let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);

    match mgr.run().await {
        Err(SbatchRunError::ArrayNotSupported) => {}
        other => panic!("expected ArrayNotSupported, got {other:?}"),
    }
    assert!(recorder.seen().is_empty(), "spawn must not be called");
}

/// Dispatcher that routes by argv[0]:
/// - "sbatch": returns canned spawn output
/// - "qgroup": returns canned qgroup output (CMP / RUN / empty)
/// - "squeue": returns canned squeue output
/// - "sacct":  returns canned sacct output (pipe-separated)
/// - any other: (0, "")
struct RunCannedDispatcher {
    sbatch: Mutex<(i32, String)>,
    qgroup: Mutex<String>,
    squeue: Mutex<String>,
    sacct: Mutex<String>,
}
impl RunCannedDispatcher {
    fn new(sbatch_stdout: &str, qgroup: &str, sacct: &str) -> Self {
        Self {
            sbatch: Mutex::new((0, sbatch_stdout.to_string())),
            qgroup: Mutex::new(qgroup.to_string()),
            squeue: Mutex::new(String::new()),
            sacct: Mutex::new(sacct.to_string()),
        }
    }
}
impl JobDispatcher for RunCannedDispatcher {
    async fn run(&self, _argv: &[String]) -> Result<i32> {
        unimplemented!()
    }
    async fn capture(&self, argv: &[String]) -> Result<CaptureOutput> {
        let out = match argv[0].as_str() {
            "sbatch" => {
                let (e, s) = self.sbatch.lock().unwrap().clone();
                return Ok(cap(e, &s));
            }
            "qgroup" => self.qgroup.lock().unwrap().clone(),
            "squeue" => self.squeue.lock().unwrap().clone(),
            "sacct" => self.sacct.lock().unwrap().clone(),
            _ => String::new(),
        };
        Ok(cap(0, &out))
    }
}

#[tokio::test]
async fn run_happy_path_returns_finished_info_with_exit_code_zero() {
    let cmd = SbatchCmd::new("/w/job.sh");
    // sbatch returns jobid 4242. qgroup misses (empty) so the active
    // listing is left immediately, allowing `refresh_with_sacct` to
    // call sacct, which reports COMPLETED exit 0:0.
    let dispatcher = into_dyn(RunCannedDispatcher::new(
        "Submitted batch job 4242\n",
        "",
        "4242|COMPLETED|None|0:0\n",
    ));
    let mgr = SbatchManager::new(cmd)
        .with_dispatcher(dispatcher)
        .with_poll_interval(std::time::Duration::from_millis(1));

    let finished = mgr.run().await.expect("run should succeed");
    assert_eq!(finished.final_state, crate::JobState::Completed);
    assert_eq!(finished.exit_code, Some(0));
}

#[tokio::test]
async fn run_returns_job_failed_when_sacct_reports_failed_state() {
    let cmd = SbatchCmd::new("/w/job.sh");
    // qgroup empty so wait_terminal flips left_active_listing and
    // refresh_with_sacct invokes sacct, which reports FAILED exit 2.
    let dispatcher = into_dyn(RunCannedDispatcher::new(
        "Submitted batch job 9090\n",
        "",
        "9090|FAILED|NonZeroExit|2:0\n",
    ));
    let mgr = SbatchManager::new(cmd)
        .with_dispatcher(dispatcher)
        .with_poll_interval(std::time::Duration::from_millis(1));

    match mgr.run().await {
        Err(SbatchRunError::JobFailed { state, exit_code }) => {
            assert_eq!(state, crate::JobState::Failed);
            assert_eq!(exit_code, Some(2));
        }
        other => panic!("expected JobFailed, got {other:?}"),
    }
}

#[tokio::test]
async fn run_with_invokes_callback_with_jobid_before_terminal() {
    let cmd = SbatchCmd::new("/w/job.sh");
    let dispatcher = into_dyn(RunCannedDispatcher::new(
        "Submitted batch job 5151\n",
        "",
        "5151|COMPLETED|None|0:0\n",
    ));
    let mgr = SbatchManager::new(cmd)
        .with_dispatcher(dispatcher)
        .with_poll_interval(std::time::Duration::from_millis(1));

    let captured = std::sync::Arc::new(std::sync::Mutex::new(None::<u64>));
    let captured_clone = captured.clone();

    let finished = mgr
        .run_with(move |jid| *captured_clone.lock().unwrap() = Some(jid))
        .await
        .expect("run_with should succeed");

    assert_eq!(finished.final_state, crate::JobState::Completed);
    assert_eq!(
        *captured.lock().unwrap(),
        Some(5151),
        "on_spawn must have fired with the submitted jobid before terminal"
    );
}

#[tokio::test]
async fn run_with_callback_not_invoked_when_array_spec_rejected() {
    use crate::entities::slurm::SlurmArraySpec;

    let mut cmd = SbatchCmd::new("/w/job.sh");
    cmd.array_spec = Some("0-2".parse::<SlurmArraySpec>().unwrap());

    let recorder = std::sync::Arc::new(RecordingDispatcher::new(vec![]));
    let dispatcher = into_dyn(ArcDispatcher(recorder));
    let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);

    let fired = std::sync::Arc::new(std::sync::Mutex::new(false));
    let fired_clone = fired.clone();

    match mgr
        .run_with(move |_| *fired_clone.lock().unwrap() = true)
        .await
    {
        Err(SbatchRunError::ArrayNotSupported) => {}
        other => panic!("expected ArrayNotSupported, got {other:?}"),
    }
    assert!(
        !*fired.lock().unwrap(),
        "callback must NOT fire when array rejection short-circuits before spawn"
    );
}

/// Routes by argv[0]: `sbatch` hands out incrementing jobids,
/// `qgroup` returns a listing with every issued jobid as RUN and
/// counts its invocations. Everything else returns empty success.
struct SharedPollDispatcher {
    next_jobid: std::sync::atomic::AtomicU64,
    issued: Mutex<Vec<u64>>,
    qgroup_calls: std::sync::atomic::AtomicUsize,
}
impl SharedPollDispatcher {
    fn new(first_jobid: u64) -> Self {
        Self {
            next_jobid: std::sync::atomic::AtomicU64::new(first_jobid),
            issued: Mutex::new(Vec::new()),
            qgroup_calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}
impl JobDispatcher for SharedPollDispatcher {
    async fn run(&self, _argv: &[String]) -> Result<i32> {
        unimplemented!()
    }
    async fn capture(&self, argv: &[String]) -> Result<CaptureOutput> {
        use std::sync::atomic::Ordering;
        match argv[0].as_str() {
            "sbatch" => {
                let jobid = self.next_jobid.fetch_add(1, Ordering::SeqCst);
                self.issued.lock().unwrap().push(jobid);
                Ok(cap(0, &format!("Submitted batch job {jobid}\n")))
            }
            "qgroup" => {
                self.qgroup_calls.fetch_add(1, Ordering::SeqCst);
                let rows: String = self
                    .issued
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|j| format!("queue user {j} RUN 1 1 1M 00:00:01\n"))
                    .collect();
                Ok(cap(0, &rows))
            }
            _ => Ok(cap(0, "")),
        }
    }
}

/// The qgroup -l listing is global, so handles spawned by the same
/// manager must share one subprocess per poll cycle instead of each
/// spawning their own (the refresh-multiplexing review item).
#[tokio::test]
async fn handles_from_same_manager_share_one_qgroup_listing_within_ttl() {
    let recorder = std::sync::Arc::new(SharedPollDispatcher::new(101));
    let mgr = SbatchManager::new(SbatchCmd::new("/w/job.sh"))
        .with_dispatcher(into_dyn(ArcDispatcher(recorder.clone())));
    // Default poll_interval (60 s) == cache TTL — both refreshes land
    // well inside it.
    let h1 = mgr.spawn().await.unwrap();
    let h2 = mgr.spawn().await.unwrap();

    let s1 = h1.refresh().await.unwrap();
    let s2 = h2.refresh().await.unwrap();

    use std::sync::atomic::Ordering;
    assert_eq!(
        recorder.qgroup_calls.load(Ordering::SeqCst),
        1,
        "two handles refreshing within poll_interval must share one qgroup -l spawn"
    );
    assert_eq!(
        s1.lifecycle.last_observed_state.as_ref().map(|s| s.state),
        Some(crate::JobState::Running),
    );
    assert_eq!(
        s2.lifecycle.last_observed_state.as_ref().map(|s| s.state),
        Some(crate::JobState::Running),
        "the cached listing must still resolve the second handle's own jobid"
    );
}

/// TTL is wired to `with_poll_interval`: a zero interval means every
/// refresh does a live qgroup query (cache effectively off).
#[tokio::test]
async fn poll_interval_zero_makes_every_refresh_query_qgroup_live() {
    let recorder = std::sync::Arc::new(SharedPollDispatcher::new(201));
    let mgr = SbatchManager::new(SbatchCmd::new("/w/job.sh"))
        .with_dispatcher(into_dyn(ArcDispatcher(recorder.clone())))
        .with_poll_interval(std::time::Duration::ZERO);
    let h = mgr.spawn().await.unwrap();

    h.refresh().await.unwrap();
    h.refresh().await.unwrap();

    use std::sync::atomic::Ordering;
    assert_eq!(
        recorder.qgroup_calls.load(Ordering::SeqCst),
        2,
        "TTL must follow poll_interval; zero interval disables caching"
    );
}

/// Routes by argv[0]: `sbatch` hands out incrementing jobids,
/// `qgroup` returns an empty listing (forcing every handle onto the
/// squeue fallback), `squeue` echoes a RUNNING row per requested
/// `-j` id and counts its invocations.
struct SqueueFallbackDispatcher {
    next_jobid: std::sync::atomic::AtomicU64,
    squeue_calls: std::sync::atomic::AtomicUsize,
}
impl SqueueFallbackDispatcher {
    fn new(first_jobid: u64) -> Self {
        Self {
            next_jobid: std::sync::atomic::AtomicU64::new(first_jobid),
            squeue_calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}
impl JobDispatcher for SqueueFallbackDispatcher {
    async fn run(&self, _argv: &[String]) -> Result<i32> {
        unimplemented!()
    }
    async fn capture(&self, argv: &[String]) -> Result<CaptureOutput> {
        use std::sync::atomic::Ordering;
        match argv[0].as_str() {
            "sbatch" => {
                let jobid = self.next_jobid.fetch_add(1, Ordering::SeqCst);
                Ok(cap(0, &format!("Submitted batch job {jobid}\n")))
            }
            "squeue" => {
                self.squeue_calls.fetch_add(1, Ordering::SeqCst);
                let rows: String = argv[3]
                    .split(',')
                    .map(|id| format!("{id} RUNNING None\n"))
                    .collect();
                Ok(cap(0, &rows))
            }
            _ => Ok(cap(0, "")),
        }
    }
}

/// Squeue-side refresh multiplexing: handles that fall through to
/// the squeue probe must converge onto one shared batched `-j`
/// query per poll cycle. The second handle's first poll forces one
/// live re-batch (its id was not in the first listing — replaying
/// it would fabricate a vanish), after which both replay.
#[tokio::test]
async fn handles_falling_back_to_squeue_share_one_batched_query_within_ttl() {
    let recorder = std::sync::Arc::new(SqueueFallbackDispatcher::new(301));
    let mgr = SbatchManager::new(SbatchCmd::new("/w/job.sh"))
        .with_dispatcher(into_dyn(ArcDispatcher(recorder.clone())));
    let h1 = mgr.spawn().await.unwrap();
    let h2 = mgr.spawn().await.unwrap();

    h1.refresh().await.unwrap();
    h2.refresh().await.unwrap();
    let s1 = h1.refresh().await.unwrap();
    let s2 = h2.refresh().await.unwrap();

    use std::sync::atomic::Ordering;
    assert_eq!(
        recorder.squeue_calls.load(Ordering::SeqCst),
        2,
        "4 refreshes across 2 handles must cost 1 warm-up query + 1 re-batch, then replay"
    );
    for (snap, jobid) in [(&s1, 301), (&s2, 302)] {
        assert_eq!(
            snap.lifecycle.last_observed_state.as_ref().map(|s| s.state),
            Some(crate::JobState::Running),
            "jobid {jobid} must resolve from the shared listing"
        );
        assert!(
            !snap.lifecycle.left_active_listing,
            "jobid {jobid} must not be misread as vanished from the batched listing"
        );
    }
}

#[tokio::test]
async fn run_returns_job_failed_when_cancelled_by_signal() {
    let cmd = SbatchCmd::new("/w/job.sh");
    // qgroup empty so sacct path runs; sacct reports CANCELLED with
    // signal 9 -> exit_code 137 (128 + 9).
    let dispatcher = into_dyn(RunCannedDispatcher::new(
        "Submitted batch job 7777\n",
        "",
        "7777|CANCELLED|None|0:9\n",
    ));
    let mgr = SbatchManager::new(cmd)
        .with_dispatcher(dispatcher)
        .with_poll_interval(std::time::Duration::from_millis(1));

    match mgr.run().await {
        Err(SbatchRunError::JobFailed { state, exit_code }) => {
            assert_eq!(state, crate::JobState::Cancelled);
            assert_eq!(exit_code, Some(137));
        }
        other => panic!("expected JobFailed for cancelled, got {other:?}"),
    }
}
