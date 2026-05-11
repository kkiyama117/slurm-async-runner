//! `SbatchManager` — spawn / attach orchestration.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::Utc;
use uuid::Uuid;

use crate::dispatcher::{DynJobDispatcher, TokioDispatcher, into_dyn};
use crate::sbatch::cmd::SbatchCmd;
use crate::sbatch::error::{SbatchAttachError, SbatchSpawnError};
use crate::sbatch::handle::{
    LogPathSpec, SbatchAttachKey, SbatchJobHandle, SbatchJobSnapshot, SbatchLifecycle,
};
use crate::sbatch::parse::parse_submitted_jobid;
use crate::store::{FileSystemStateStore, InMemoryStateStore, JobSnapshot, JobStateStore};

#[derive(Clone)]
pub struct SbatchManager {
    cmd: SbatchCmd,
    store: Arc<dyn JobStateStore<SbatchJobSnapshot>>,
    dispatcher: Arc<dyn DynJobDispatcher>,
}

impl SbatchManager {
    pub fn new(cmd: SbatchCmd) -> Self {
        Self {
            cmd,
            store: Arc::new(InMemoryStateStore::<SbatchJobSnapshot>::new()),
            dispatcher: into_dyn(TokioDispatcher),
        }
    }

    pub fn with_state_dir(mut self, root: impl Into<PathBuf>) -> Self {
        self.store = Arc::new(FileSystemStateStore::<SbatchJobSnapshot>::new(root));
        self
    }

    pub fn with_state_store(mut self, store: Arc<dyn JobStateStore<SbatchJobSnapshot>>) -> Self {
        self.store = store;
        self
    }

    pub fn with_dispatcher(mut self, dispatcher: Arc<dyn DynJobDispatcher>) -> Self {
        self.dispatcher = dispatcher;
        self
    }

    pub async fn spawn(&self) -> Result<SbatchJobHandle, SbatchSpawnError> {
        let argv = self.cmd.build_argv()?;
        let (exit_code, stdout) = self
            .dispatcher
            .capture(&argv)
            .await
            .map_err(SbatchSpawnError::Other)?;
        if exit_code != 0 {
            return Err(SbatchSpawnError::SubmitFailed { exit_code, stdout });
        }
        let jobid =
            parse_submitted_jobid(&stdout).ok_or_else(|| SbatchSpawnError::JobidParseError {
                stdout: stdout.clone(),
            })?;

        let uuid = Uuid::now_v7();
        let script_path = std::path::absolute(&self.cmd.script)
            .with_context(|| format!("absolutize {}", self.cmd.script.display()))?;
        let snapshot = SbatchJobSnapshot {
            uuid,
            jobid,
            array_jobid: None,
            array_task_id: None,
            argv,
            sent_env: self.cmd.env.clone(),
            script_path,
            chdir: self.cmd.chdir.clone(),
            partition: self.cmd.partition.clone(),
            job_name: self.cmd.job_name.clone(),
            submitted_at: Utc::now(),
            log: LogPathSpec {
                output_template: self.cmd.output.clone(),
                error_template: self.cmd.error.clone(),
            },
            lifecycle: SbatchLifecycle::default(),
        };

        self.store
            .save(&snapshot)
            .await
            .map_err(|source| SbatchSpawnError::SubmittedButUnpersisted { jobid, source })?;
        Ok(SbatchJobHandle::new(
            snapshot,
            self.store.clone(),
            self.dispatcher.clone(),
        ))
    }

    /// Submit an array job in a single `sbatch --array=<spec>` invocation,
    /// then persist one snapshot per task and return one handle per task.
    ///
    /// All returned snapshots share the same master `jobid` (from sbatch's
    /// `Submitted batch job <N>` line) and the same `argv`, but each has a
    /// distinct `uuid` and `array_task_id`. The `array_jobid` field also
    /// holds the master jobid for every task.
    ///
    /// `array_spec` overrides any value already on `self.cmd.array_spec`.
    /// Returns the handles in `expand_array_indices` order (declaration
    /// order — not numerical sort if the spec was e.g. `5,0-2`).
    pub async fn spawn_array(
        &self,
        array_spec: crate::entities::slurm::SlurmArraySpec,
    ) -> Result<Vec<SbatchJobHandle>, SbatchSpawnError> {
        use crate::sbatch::parse::expand_array_indices;
        let task_indices = expand_array_indices(&array_spec);
        assert!(
            !task_indices.is_empty(),
            "SlurmArraySpec FromStr guarantees non-empty indices"
        );

        let mut cmd = self.cmd.clone();
        cmd.array_spec = Some(array_spec);
        let argv = cmd.build_argv()?;

        let (exit_code, stdout) = self
            .dispatcher
            .capture(&argv)
            .await
            .map_err(SbatchSpawnError::Other)?;
        if exit_code != 0 {
            return Err(SbatchSpawnError::SubmitFailed { exit_code, stdout });
        }
        let master_jobid =
            parse_submitted_jobid(&stdout).ok_or_else(|| SbatchSpawnError::JobidParseError {
                stdout: stdout.clone(),
            })?;

        let script_path = std::path::absolute(&cmd.script)
            .with_context(|| format!("absolutize {}", cmd.script.display()))
            .map_err(SbatchSpawnError::Other)?;

        let mut handles = Vec::with_capacity(task_indices.len());
        for idx in task_indices {
            let snapshot = SbatchJobSnapshot {
                uuid: Uuid::now_v7(),
                jobid: master_jobid,
                array_jobid: Some(master_jobid),
                array_task_id: Some(idx),
                argv: argv.clone(),
                sent_env: cmd.env.clone(),
                script_path: script_path.clone(),
                chdir: cmd.chdir.clone(),
                partition: cmd.partition.clone(),
                job_name: cmd.job_name.clone(),
                submitted_at: Utc::now(),
                log: LogPathSpec {
                    output_template: cmd.output.clone(),
                    error_template: cmd.error.clone(),
                },
                lifecycle: SbatchLifecycle::default(),
            };
            self.store.save(&snapshot).await.map_err(|source| {
                SbatchSpawnError::SubmittedButUnpersisted {
                    jobid: master_jobid,
                    source,
                }
            })?;
            handles.push(SbatchJobHandle::new(
                snapshot,
                self.store.clone(),
                self.dispatcher.clone(),
            ));
        }
        Ok(handles)
    }

    pub async fn attach(&self, key: SbatchAttachKey) -> Result<SbatchJobHandle, SbatchAttachError> {
        let key_repr = format!("{key:?}");
        let snapshot = match key {
            SbatchAttachKey::Uuid(u) => self.store.load(u).await.map_err(SbatchAttachError::Io)?,
            SbatchAttachKey::JobId(j) => {
                let snaps = self
                    .store
                    .find_all_by_jobid(j)
                    .await
                    .map_err(SbatchAttachError::Io)?;
                if snaps.len() > 1 {
                    return Err(SbatchAttachError::MultipleMatch {
                        jobid: j,
                        count: snaps.len(),
                    });
                }
                snaps.into_iter().next()
            }
            SbatchAttachKey::File(path) => {
                let bytes = tokio::fs::read(&path)
                    .await
                    .map_err(|e| SbatchAttachError::Io(anyhow::Error::from(e)))?;
                let value: serde_json::Value = serde_json::from_slice(&bytes)
                    .map_err(|e| SbatchAttachError::Io(anyhow::Error::from(e)))?;
                if let Some(k) = value.get("kind").and_then(|v| v.as_str())
                    && k != <SbatchJobSnapshot as JobSnapshot>::kind()
                {
                    return Err(SbatchAttachError::KindMismatch {
                        expected: <SbatchJobSnapshot as JobSnapshot>::kind(),
                        got: k.to_string(),
                    });
                }
                Some(
                    serde_json::from_value(value)
                        .map_err(|e| SbatchAttachError::Io(anyhow::Error::from(e)))?,
                )
            }
        }
        .ok_or_else(|| SbatchAttachError::NotFound { key: key_repr })?;
        Ok(SbatchJobHandle::new(
            snapshot,
            self.store.clone(),
            self.dispatcher.clone(),
        ))
    }

    pub async fn attach_uuid(&self, u: Uuid) -> Result<SbatchJobHandle, SbatchAttachError> {
        self.attach(SbatchAttachKey::Uuid(u)).await
    }
    pub async fn attach_jobid(&self, j: u64) -> Result<SbatchJobHandle, SbatchAttachError> {
        self.attach(SbatchAttachKey::JobId(j)).await
    }
    pub async fn attach_file(
        &self,
        p: impl Into<PathBuf>,
    ) -> Result<SbatchJobHandle, SbatchAttachError> {
        self.attach(SbatchAttachKey::File(p.into())).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatcher::{JobDispatcher, into_dyn};
    use std::sync::Mutex;

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
        async fn capture(&self, _argv: &[String]) -> Result<(i32, String)> {
            Ok((
                *self.exit.lock().unwrap(),
                self.stdout.lock().unwrap().clone(),
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
            assert_eq!(snap.array_jobid, Some(50000));
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
}
