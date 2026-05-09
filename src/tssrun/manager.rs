//! [`TssrunManager`] orchestrates `spawn` / `attach` / `query_state` for a
//! single [`TssrunCmd`].
//!
//! ## Responsibilities
//!
//! - Holds the [`TssrunCmd`] spec, an optional `state_dir` for
//!   JSON-snapshot persistence, and a shared [`JobLogSink`] for tee'd
//!   stdout/stderr.
//! - [`TssrunManager::spawn`] launches the child via
//!   [`TokioBackgroundDispatcher`] and returns a [`JobHandle`] whose
//!   snapshot is updated as `salloc:` lines arrive and as the wait task
//!   records exit info. Every spawn generates a fresh UUID v7 that is
//!   the snapshot's primary key. When `state_dir` is set, every snapshot
//!   mutation persists `{state_dir}/{uuid}.json` via atomic rename.
//! - [`TssrunManager::attach`] reconstructs a read-only [`JobHandle`]
//!   from a previously persisted snapshot, identified by [`AttachKey`].
//!   Attach by [`AttachKey::Uuid`] resolves directly to
//!   `{state_dir}/{uuid}.json`; attach by `Pid` / `JobId` falls back to
//!   scanning the directory because those keys are not encoded in the
//!   filename and `pid` in particular may have been reused by the kernel.
//! - [`TssrunManager::query_state`] looks up SLURM lifecycle state via
//!   `sacct` for handles that already parsed a jobid.
//!
//! ## Builder pattern
//!
//! ```ignore
//! let manager = TssrunManager::new(cmd)
//!     .with_state_dir(PathBuf::from("/var/lib/slurm-runner"))
//!     .with_log_sink(Arc::new(StdLogSink));
//! ```
//!
//! Fields are crate-private on purpose — mutating them after a `spawn()`
//! would not retroactively redirect already-running handles' persistence
//! or log routing, so the builders are the only sanctioned construction
//! path.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use uuid::Uuid;

use crate::JobStatus;
use crate::dispatcher::{BackgroundDispatcher, TokioBackgroundDispatcher};
use crate::runner;
use crate::tssrun::cmd::TssrunCmd;
use crate::tssrun::handle::{JobHandle, JobHandleSnapshot, LogLocations};
use crate::tssrun::log::{JobLogSink, StdLogSink};

/// Identifies a previously-persisted handle to attach to.
#[derive(Debug, Clone)]
pub enum AttachKey {
    /// Primary key. Resolves directly to `{state_dir}/{uuid}.json`.
    Uuid(Uuid),
    /// Best-effort lookup that scans `state_dir` for a snapshot whose
    /// `pid` matches. Pids may be recycled, so prefer `Uuid` for
    /// long-lived references.
    Pid(u32),
    /// Best-effort lookup that scans `state_dir` for a snapshot whose
    /// `jobid` matches. Only useful after `salloc:` has been parsed.
    JobId(u64),
    /// Direct path to a `{uuid}.json` file (or any other persisted
    /// snapshot path).
    File(PathBuf),
}

/// Orchestrates one or more tssrun invocations sharing a common log sink
/// and (optional) state directory.
///
/// Fields are crate-private on purpose: mutating them after a `spawn()`
/// would NOT retroactively redirect already-running handles' state-dir
/// persistence or log sinks. Use the [`TssrunManager::new`] constructor
/// plus the [`TssrunManager::with_state_dir`] / [`TssrunManager::with_log_sink`]
/// builders for new managers.
pub struct TssrunManager {
    pub(crate) cmd: TssrunCmd,
    pub(crate) state_dir: Option<PathBuf>,
    pub(crate) log_sink: Arc<dyn JobLogSink>,
}

impl TssrunManager {
    pub fn new(cmd: TssrunCmd) -> Self {
        Self {
            cmd,
            state_dir: None,
            log_sink: Arc::new(StdLogSink),
        }
    }

    pub fn with_state_dir(mut self, dir: PathBuf) -> Self {
        self.state_dir = Some(dir);
        self
    }

    pub fn with_log_sink(mut self, sink: Arc<dyn JobLogSink>) -> Self {
        self.log_sink = sink;
        self
    }

    /// Spawn the configured command via [`TokioBackgroundDispatcher`].
    pub async fn spawn(&self) -> Result<JobHandle> {
        self.spawn_with(&TokioBackgroundDispatcher).await
    }

    /// Spawn via an explicit dispatcher.
    pub async fn spawn_with<D: BackgroundDispatcher>(&self, dispatcher: &D) -> Result<JobHandle> {
        let argv = self.cmd.build_argv()?;
        let cwd = self.cmd.cwd.as_deref();
        let spawned = dispatcher.spawn(&argv, &self.cmd.env, cwd).await?;

        // UUID v7 — time-ordered, per-spawn primary key. Generated *before*
        // building `persist_path` so the on-disk filename and the in-memory
        // snapshot share the exact same value with no second source of truth.
        let uuid = Uuid::now_v7();
        let persist_path = self
            .state_dir
            .as_ref()
            .map(|d| d.join(format!("{uuid}.json")));

        let init = JobHandleSnapshot {
            uuid,
            pid: spawned.pid,
            argv,
            sent_env: self.cmd.env.clone(),
            cwd: self.cmd.cwd.clone(),
            started_at_unix: now_unix(),
            log_locations: LogLocations::None,
            jobid: None,
            node: None,
            finished: None,
        };
        JobHandle::from_spawn(spawned, init, self.log_sink.clone(), persist_path).await
    }

    /// Re-attach to a previously persisted handle.
    pub async fn attach(&self, key: AttachKey) -> Result<JobHandle> {
        let path = match key {
            AttachKey::File(p) => p,
            AttachKey::Uuid(uuid) => {
                let dir = self
                    .state_dir
                    .as_ref()
                    .ok_or_else(|| anyhow!("attach by uuid requires state_dir"))?;
                dir.join(format!("{uuid}.json"))
            }
            AttachKey::Pid(pid) => {
                let dir = self
                    .state_dir
                    .as_ref()
                    .ok_or_else(|| anyhow!("attach by pid requires state_dir"))?;
                find_in_state_dir(dir, |s| s.pid == pid)
                    .await?
                    .ok_or_else(|| {
                        anyhow!("no persisted handle in {} matched pid {pid}", dir.display())
                    })?
            }
            AttachKey::JobId(jobid) => {
                let dir = self
                    .state_dir
                    .as_ref()
                    .ok_or_else(|| anyhow!("attach by jobid requires state_dir"))?;
                find_in_state_dir(dir, |s| s.jobid == Some(jobid))
                    .await?
                    .ok_or_else(|| {
                        anyhow!(
                            "no persisted handle in {} matched jobid {jobid}",
                            dir.display()
                        )
                    })?
            }
        };
        let bytes = tokio::fs::read(&path)
            .await
            .with_context(|| format!("failed to read {}", path.display()))?;
        let snap: JobHandleSnapshot = serde_json::from_slice(&bytes)?;
        Ok(JobHandle::attach_snapshot(snap, Some(path)))
    }

    /// Look up the SLURM lifecycle state via `sacct`.
    /// Returns a default `JobStatus` when the handle has no parsed jobid.
    pub async fn query_state(&self, handle: &JobHandle) -> Result<JobStatus> {
        match handle.jobid() {
            None => Ok(JobStatus::default()),
            Some(jid) => {
                let states = runner::query_job_states_batch(&[jid]).await?;
                Ok(states.get(&jid).cloned().unwrap_or_default())
            }
        }
    }
}

/// Scan `dir` for the first `*.json` whose decoded snapshot matches `pred`.
/// Returns `Ok(None)` when no file matches; only I/O / read errors propagate.
/// Decode errors on unrelated files are ignored — a single malformed entry
/// must not block attach for an otherwise-healthy state directory.
async fn find_in_state_dir<F>(dir: &std::path::Path, pred: F) -> Result<Option<PathBuf>>
where
    F: Fn(&JobHandleSnapshot) -> bool,
{
    let mut rd = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = rd.next_entry().await? {
        if entry.path().extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        if let Ok(bytes) = tokio::fs::read(entry.path()).await
            && let Ok(snap) = serde_json::from_slice::<JobHandleSnapshot>(&bytes)
            && pred(&snap)
        {
            return Ok(Some(entry.path()));
        }
    }
    Ok(None)
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tssrun::cmd::TssrunCmd;
    use crate::tssrun::log::{InMemoryLogSink, JobLogSink};
    use std::sync::Arc;

    #[tokio::test]
    async fn manager_spawn_with_bash_mock_returns_running_handle() {
        // Write a small shell script that emits the salloc lines tssrun would,
        // then use bash as the stand-in binary and the script as `program`.
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("mock_tssrun.sh");
        tokio::fs::write(
            &script,
            r#"#!/usr/bin/env bash
echo "salloc: Granted job allocation 999"
echo "salloc: Nodes node-x are ready for job"
sleep 0.05
echo done
"#,
        )
        .await
        .unwrap();

        let mut cmd = TssrunCmd::new(&script);
        cmd.tssrun_bin = "bash".to_string();
        let dispatcher = crate::dispatcher::TokioBackgroundDispatcher;
        let sink: Arc<dyn JobLogSink> = Arc::new(InMemoryLogSink::default());
        let manager = TssrunManager::new(cmd).with_log_sink(sink);

        let mut handle = manager.spawn_with(&dispatcher).await.unwrap();
        let code = handle.wait().await.unwrap();
        assert_eq!(code, Some(0));
        assert_eq!(handle.snapshot().jobid, Some(999));
        assert_eq!(handle.snapshot().node.as_deref(), Some("node-x"));
    }

    #[tokio::test]
    async fn query_state_with_no_jobid_returns_default() {
        let cmd = TssrunCmd::new("/bin/true");
        let manager = TssrunManager::new(cmd);
        let snap = JobHandleSnapshot {
            uuid: Uuid::now_v7(),
            pid: 1,
            argv: vec![],
            sent_env: Default::default(),
            cwd: None,
            started_at_unix: 0,
            log_locations: LogLocations::None,
            jobid: None,
            node: None,
            finished: None,
        };
        let h = JobHandle::attach_snapshot(snap, None);
        let st = manager.query_state(&h).await.unwrap();
        assert_eq!(st, JobStatus::default());
    }

    #[tokio::test]
    async fn attach_by_file_round_trips_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        let uuid = Uuid::now_v7();
        let path = tmp.path().join(format!("{uuid}.json"));
        let snap = JobHandleSnapshot {
            uuid,
            pid: 42,
            argv: vec!["tssrun".into(), "/x".into()],
            sent_env: Default::default(),
            cwd: None,
            started_at_unix: 0,
            log_locations: LogLocations::None,
            jobid: Some(7),
            node: Some("nA".into()),
            finished: None,
        };
        tokio::fs::write(&path, serde_json::to_vec_pretty(&snap).unwrap())
            .await
            .unwrap();

        let manager = TssrunManager::new(TssrunCmd::new("/bin/true"))
            .with_state_dir(tmp.path().to_path_buf());
        let h = manager.attach(AttachKey::File(path)).await.unwrap();
        assert_eq!(h.uuid(), uuid);
        assert_eq!(h.pid(), 42);
        assert_eq!(h.jobid(), Some(7));
    }

    #[tokio::test]
    async fn attach_by_uuid_resolves_directly_to_filename() {
        let tmp = tempfile::tempdir().unwrap();
        let uuid = Uuid::now_v7();
        let snap = JobHandleSnapshot {
            uuid,
            pid: 1234,
            argv: vec![],
            sent_env: Default::default(),
            cwd: None,
            started_at_unix: 0,
            log_locations: LogLocations::None,
            jobid: Some(42),
            node: None,
            finished: None,
        };
        tokio::fs::write(
            tmp.path().join(format!("{uuid}.json")),
            serde_json::to_vec_pretty(&snap).unwrap(),
        )
        .await
        .unwrap();

        let manager = TssrunManager::new(TssrunCmd::new("/bin/true"))
            .with_state_dir(tmp.path().to_path_buf());
        let h = manager.attach(AttachKey::Uuid(uuid)).await.unwrap();
        assert_eq!(h.uuid(), uuid);
        assert_eq!(h.pid(), 1234);
        assert_eq!(h.jobid(), Some(42));
    }

    #[tokio::test]
    async fn attach_by_jobid_finds_correct_file() {
        let tmp = tempfile::tempdir().unwrap();
        for (pid, jid) in [(10u32, 100u64), (11, 101)] {
            let uuid = Uuid::now_v7();
            let snap = JobHandleSnapshot {
                uuid,
                pid,
                argv: vec![],
                sent_env: Default::default(),
                cwd: None,
                started_at_unix: 0,
                log_locations: LogLocations::None,
                jobid: Some(jid),
                node: None,
                finished: None,
            };
            tokio::fs::write(
                tmp.path().join(format!("{uuid}.json")),
                serde_json::to_vec_pretty(&snap).unwrap(),
            )
            .await
            .unwrap();
        }
        let manager = TssrunManager::new(TssrunCmd::new("/bin/true"))
            .with_state_dir(tmp.path().to_path_buf());
        let h = manager.attach(AttachKey::JobId(101)).await.unwrap();
        assert_eq!(h.pid(), 11);
    }

    #[tokio::test]
    async fn attach_by_pid_scans_state_dir() {
        // Pre-UUID layout used `{pid}.json` as the filename, so attach_pid
        // could just join. The new layout encodes the UUID, so attach_pid
        // must scan. Verify the scan still resolves correctly.
        let tmp = tempfile::tempdir().unwrap();
        for pid in [777u32, 888] {
            let uuid = Uuid::now_v7();
            let snap = JobHandleSnapshot {
                uuid,
                pid,
                argv: vec![],
                sent_env: Default::default(),
                cwd: None,
                started_at_unix: 0,
                log_locations: LogLocations::None,
                jobid: None,
                node: None,
                finished: None,
            };
            tokio::fs::write(
                tmp.path().join(format!("{uuid}.json")),
                serde_json::to_vec_pretty(&snap).unwrap(),
            )
            .await
            .unwrap();
        }
        let manager = TssrunManager::new(TssrunCmd::new("/bin/true"))
            .with_state_dir(tmp.path().to_path_buf());
        let h = manager.attach(AttachKey::Pid(888)).await.unwrap();
        assert_eq!(h.pid(), 888);
    }

    #[tokio::test]
    async fn spawn_persists_snapshot_under_uuid_filename() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("mock.sh");
        tokio::fs::write(
            &script,
            r#"#!/usr/bin/env bash
echo "salloc: Granted job allocation 1"
echo "salloc: Nodes node-uuid are ready for job"
"#,
        )
        .await
        .unwrap();

        let mut cmd = TssrunCmd::new(&script);
        cmd.tssrun_bin = "bash".to_string();
        let manager = TssrunManager::new(cmd).with_state_dir(tmp.path().to_path_buf());

        let mut handle = manager.spawn().await.unwrap();
        let uuid = handle.uuid();
        let _ = handle.wait().await.unwrap();

        let path = tmp.path().join(format!("{uuid}.json"));
        assert!(
            path.exists(),
            "expected persisted snapshot at {}",
            path.display()
        );
        // attach by uuid round-trips the same snapshot we just spawned.
        let attached = manager.attach(AttachKey::Uuid(uuid)).await.unwrap();
        assert_eq!(attached.uuid(), uuid);
    }
}
