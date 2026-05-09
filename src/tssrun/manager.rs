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
//!   records exit info. When `state_dir` is set, every snapshot mutation
//!   persists `{state_dir}/{pid}.json` via atomic rename.
//! - [`TssrunManager::attach`] reconstructs a read-only [`JobHandle`]
//!   from a previously persisted snapshot, identified by [`AttachKey`].
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

use crate::JobStatus;
use crate::dispatcher::{BackgroundDispatcher, TokioBackgroundDispatcher};
use crate::runner;
use crate::tssrun::cmd::TssrunCmd;
use crate::tssrun::handle::{JobHandle, JobHandleSnapshot, LogLocations};
use crate::tssrun::log::{JobLogSink, StdLogSink};

/// Identifies a previously-persisted handle to attach to.
#[derive(Debug, Clone)]
pub enum AttachKey {
    Pid(u32),
    JobId(u64),
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

        let persist_path = self
            .state_dir
            .as_ref()
            .map(|d| d.join(format!("{}.json", spawned.pid)));

        let init = JobHandleSnapshot {
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
            AttachKey::Pid(pid) => {
                let dir = self
                    .state_dir
                    .as_ref()
                    .ok_or_else(|| anyhow!("attach by pid requires state_dir"))?;
                dir.join(format!("{pid}.json"))
            }
            AttachKey::JobId(jobid) => {
                let dir = self
                    .state_dir
                    .as_ref()
                    .ok_or_else(|| anyhow!("attach by jobid requires state_dir"))?;
                find_by_jobid(dir, jobid).await?
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

async fn find_by_jobid(dir: &std::path::Path, jobid: u64) -> Result<PathBuf> {
    let mut rd = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = rd.next_entry().await? {
        if entry.path().extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        if let Ok(bytes) = tokio::fs::read(entry.path()).await
            && let Ok(snap) = serde_json::from_slice::<JobHandleSnapshot>(&bytes)
            && snap.jobid == Some(jobid)
        {
            return Ok(entry.path());
        }
    }
    Err(anyhow!(
        "no persisted handle in {} matched jobid {jobid}",
        dir.display()
    ))
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
        let path = tmp.path().join("42.json");
        let snap = JobHandleSnapshot {
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
        assert_eq!(h.pid(), 42);
        assert_eq!(h.jobid(), Some(7));
    }

    #[tokio::test]
    async fn attach_by_jobid_finds_correct_file() {
        let tmp = tempfile::tempdir().unwrap();
        for (pid, jid) in [(10u32, 100u64), (11, 101)] {
            let snap = JobHandleSnapshot {
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
                tmp.path().join(format!("{pid}.json")),
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
}
