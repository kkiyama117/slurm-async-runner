//! [`TssrunManager`] orchestrates `spawn` / `attach` / `query_state` for a
//! single [`TssrunCmd`].
//!
//! ## Responsibilities
//!
//! - Holds the [`TssrunCmd`] spec, a [`JobStateStore`] for snapshot
//!   persistence, and a shared [`JobLogSink`] for tee'd stdout/stderr.
//! - [`TssrunManager::spawn`] launches the child via
//!   [`TokioBackgroundDispatcher`] and returns a [`TssrunJobHandle`] whose
//!   snapshot is updated as `salloc:` lines arrive and as the wait task
//!   records exit info. Every spawn generates a fresh UUID v7 that is
//!   the snapshot's primary key. Every snapshot mutation is persisted
//!   through the configured store.
//! - [`TssrunManager::attach`] reconstructs a read-only [`TssrunJobHandle`]
//!   from a previously persisted snapshot, identified by [`AttachKey`].
//!   Attach by [`AttachKey::Uuid`] is an O(1) primary-key lookup; attach
//!   by `Pid` / `JobId` may fall back to a scan inside the store
//!   implementation. [`AttachKey::File`] bypasses the store entirely
//!   and reads a JSON file directly — useful for ad-hoc debugging
//!   regardless of which store is configured.
//! - [`TssrunManager::query_state`] looks up SLURM lifecycle state via
//!   `sacct` for handles that already parsed a jobid.
//!
//! ## Builder pattern
//!
//! ```ignore
//! // Default: process-local InMemoryStateStore — works without writable
//! // disk and is enough for single-process workflows.
//! let manager = TssrunManager::new(cmd);
//!
//! // Persist across processes:
//! let manager = TssrunManager::new(cmd)
//!     .with_state_dir("/var/lib/slurm-runner")              // sugar for FS
//!     .with_log_sink(Arc::new(StdLogSink));
//!
//! // Or wire a custom backend (e.g. a Redis impl in another crate):
//! let manager = TssrunManager::new(cmd)
//!     .with_state_store(Arc::new(my_redis_store));
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
use crate::store::JobStateStore;
use crate::tssrun::cmd::TssrunCmd;
use crate::tssrun::handle::{LogLocations, TssrunJobHandle, TssrunJobSnapshot};
use crate::tssrun::log::{JobLogSink, StdLogSink};
use crate::tssrun::store::{self, FileSystemStateStore, InMemoryStateStore};

/// Identifies a previously-persisted handle to attach to.
#[derive(Debug, Clone)]
pub enum AttachKey {
    /// Primary key — resolved through the configured store with an O(1)
    /// load (no scan required).
    Uuid(Uuid),
    /// Best-effort lookup by `pid`. Pids may be recycled by the kernel,
    /// so prefer `Uuid` for long-lived references. Implementations are
    /// allowed to scan all persisted snapshots to satisfy this.
    Pid(u32),
    /// Best-effort lookup by SLURM `jobid`. Only useful after `salloc:`
    /// has been parsed.
    JobId(u64),
    /// Direct path to a JSON snapshot file. Bypasses the store entirely
    /// — primarily for debugging and one-off recovery.
    File(PathBuf),
}

/// Orchestrates one or more tssrun invocations sharing a common log
/// sink and a [`JobStateStore`] for snapshot persistence.
///
/// Fields are crate-private on purpose: mutating them after a `spawn()`
/// would NOT retroactively redirect already-running handles' store
/// persistence or log sinks. Use [`TssrunManager::new`] plus the
/// `with_*` builders for new managers.
pub struct TssrunManager {
    pub(crate) cmd: TssrunCmd,
    pub(crate) store: Arc<dyn JobStateStore<TssrunJobSnapshot>>,
    pub(crate) log_sink: Arc<dyn JobLogSink>,
}

impl TssrunManager {
    /// Construct a manager with the in-memory default store. Suitable
    /// for single-process workflows that don't need cross-process
    /// attach. To persist across processes, chain [`with_state_dir`] or
    /// [`with_state_store`].
    ///
    /// [`with_state_dir`]: TssrunManager::with_state_dir
    /// [`with_state_store`]: TssrunManager::with_state_store
    pub fn new(cmd: TssrunCmd) -> Self {
        Self {
            cmd,
            store: Arc::new(InMemoryStateStore::new()),
            log_sink: Arc::new(StdLogSink),
        }
    }

    /// Sugar for `with_state_store(Arc::new(FileSystemStateStore::new(dir)))`.
    /// The directory does not need to exist yet — it is created lazily
    /// on first save.
    #[must_use = "with_state_dir returns a new TssrunManager; the receiver is unchanged"]
    pub fn with_state_dir(self, dir: impl Into<PathBuf>) -> Self {
        self.with_state_store(Arc::new(FileSystemStateStore::new(dir)))
    }

    /// Wire an arbitrary [`JobStateStore`] backend.
    #[must_use = "with_state_store returns a new TssrunManager; the receiver is unchanged"]
    pub fn with_state_store(mut self, store: Arc<dyn JobStateStore<TssrunJobSnapshot>>) -> Self {
        self.store = store;
        self
    }

    #[must_use = "with_log_sink returns a new TssrunManager; the receiver is unchanged"]
    pub fn with_log_sink(mut self, sink: Arc<dyn JobLogSink>) -> Self {
        self.log_sink = sink;
        self
    }

    /// Borrow the configured store. Useful when a caller wants to save
    /// or load snapshots out-of-band (e.g. a CLI listing all known jobs).
    pub fn store(&self) -> &Arc<dyn JobStateStore<TssrunJobSnapshot>> {
        &self.store
    }

    /// Spawn the configured command via [`TokioBackgroundDispatcher`].
    pub async fn spawn(&self) -> Result<TssrunJobHandle> {
        self.spawn_with(&TokioBackgroundDispatcher).await
    }

    /// Spawn via an explicit dispatcher.
    pub async fn spawn_with<D: BackgroundDispatcher>(
        &self,
        dispatcher: &D,
    ) -> Result<TssrunJobHandle> {
        let argv = self.cmd.build_argv()?;
        let cwd = self.cmd.cwd.as_deref();
        let spawned = dispatcher.spawn(&argv, &self.cmd.env, cwd).await?;

        // UUID v7 — time-ordered, per-spawn primary key. The store keys
        // every snapshot by `init.uuid`, so generating it once here keeps
        // the on-disk filename, the in-memory snapshot, and the in-flight
        // store entry in sync with no second source of truth.
        let uuid = Uuid::now_v7();
        let init = TssrunJobSnapshot {
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
        TssrunJobHandle::from_spawn(
            spawned,
            init,
            self.log_sink.clone(),
            Some(self.store.clone()),
        )
        .await
    }

    /// Re-attach to a previously persisted handle.
    pub async fn attach(&self, key: AttachKey) -> Result<TssrunJobHandle> {
        match key {
            AttachKey::File(p) => {
                let bytes = tokio::fs::read(&p)
                    .await
                    .with_context(|| format!("failed to read {}", p.display()))?;
                let snap: TssrunJobSnapshot = serde_json::from_slice(&bytes)
                    .with_context(|| format!("failed to decode {}", p.display()))?;
                Ok(TssrunJobHandle::attach_snapshot(
                    snap,
                    Some(self.store.clone()),
                ))
            }
            AttachKey::Uuid(uuid) => {
                let snap = self
                    .store
                    .load(uuid)
                    .await?
                    .ok_or_else(|| anyhow!("no persisted handle matched uuid {uuid}"))?;
                Ok(TssrunJobHandle::attach_snapshot(
                    snap,
                    Some(self.store.clone()),
                ))
            }
            AttachKey::Pid(pid) => {
                let snap = store::find_by_pid(self.store.as_ref(), pid)
                    .await?
                    .ok_or_else(|| anyhow!("no persisted handle matched pid {pid}"))?;
                Ok(TssrunJobHandle::attach_snapshot(
                    snap,
                    Some(self.store.clone()),
                ))
            }
            AttachKey::JobId(jobid) => {
                let snap = self
                    .store
                    .find_by_jobid(jobid)
                    .await?
                    .ok_or_else(|| anyhow!("no persisted handle matched jobid {jobid}"))?;
                Ok(TssrunJobHandle::attach_snapshot(
                    snap,
                    Some(self.store.clone()),
                ))
            }
        }
    }

    /// Look up the SLURM lifecycle state via `sacct`.
    /// Returns a default `JobStatus` when the handle has no parsed jobid.
    pub async fn query_state(&self, handle: &TssrunJobHandle) -> Result<JobStatus> {
        match handle.jobid() {
            None => Ok(JobStatus::default()),
            Some(jid) => {
                let states = runner::query_job_states_batch(&[jid]).await?;
                Ok(states.get(&jid).cloned().unwrap_or_default())
            }
        }
    }
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
        let snap = TssrunJobSnapshot {
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
        let h = TssrunJobHandle::attach_snapshot(snap, None);
        let st = manager.query_state(&h).await.unwrap();
        assert_eq!(st, JobStatus::default());
    }

    #[tokio::test]
    async fn attach_by_file_round_trips_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        let uuid = Uuid::now_v7();
        let path = tmp.path().join(format!("{uuid}.json"));
        let snap = TssrunJobSnapshot {
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
        let snap = TssrunJobSnapshot {
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
            let snap = TssrunJobSnapshot {
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
            let snap = TssrunJobSnapshot {
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
    async fn default_in_memory_store_supports_attach_uuid_in_process() {
        // The default InMemoryStateStore lets an in-process workflow
        // round-trip spawn → attach_uuid without ever touching the disk.
        // This is the headline use case for the no-write-permission /
        // single-process scenarios the JobStateStore split was designed for.
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("mock.sh");
        tokio::fs::write(
            &script,
            r#"#!/usr/bin/env bash
echo "salloc: Granted job allocation 5"
echo "salloc: Nodes node-mem are ready for job"
"#,
        )
        .await
        .unwrap();

        let mut cmd = TssrunCmd::new(&script);
        cmd.tssrun_bin = "bash".to_string();
        // Note: no with_state_dir — we are exercising the in-memory default.
        let manager = TssrunManager::new(cmd);

        let mut handle = manager.spawn().await.unwrap();
        let uuid = handle.uuid();
        let _ = handle.wait().await.unwrap();

        let attached = manager.attach(AttachKey::Uuid(uuid)).await.unwrap();
        assert_eq!(attached.uuid(), uuid);
        assert_eq!(attached.jobid(), Some(5));
    }

    #[tokio::test]
    async fn attach_by_jobid_returns_friendly_error_when_dir_missing() {
        // Regression: pre-store, find_by_jobid surfaced ENOENT from a raw
        // read_dir on a not-yet-created state_dir. With the FS store, the
        // missing directory is simply "no entries", so the manager returns
        // the same helpful "no persisted handle matched jobid …" message
        // it would for an empty directory.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("never-created");
        let manager = TssrunManager::new(TssrunCmd::new("/bin/true")).with_state_dir(&dir);
        // TssrunJobHandle isn't Debug, so we can't use unwrap_err() — match
        // explicitly and inspect the error message instead.
        match manager.attach(AttachKey::JobId(999)).await {
            Ok(_) => panic!("attach should have failed for missing dir"),
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("no persisted handle matched jobid"),
                    "expected friendly not-found message, got: {msg}"
                );
            }
        }
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
