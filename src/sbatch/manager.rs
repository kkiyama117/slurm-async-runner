//! `SbatchManager` — spawn / attach orchestration.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::Utc;
use uuid::Uuid;

use crate::dispatcher::{DynJobDispatcher, TokioDispatcher, into_dyn};
use crate::sbatch::cmd::SbatchCmd;
use crate::sbatch::error::{SbatchAttachError, SbatchCancelError, SbatchSpawnError};
use crate::sbatch::handle::{
    LogPathSpec, SbatchAttachKey, SbatchJobHandle, SbatchJobSnapshot, SbatchLifecycle,
};
use crate::sbatch::parse::parse_submitted_jobid;
use crate::sbatch::qgroup_cache::{QgroupCacheState, QgroupCachingDispatcher};
use crate::sbatch::squeue_cache::{SqueueBatchingDispatcher, SqueueCacheState};
use crate::store::{FileSystemStateStore, InMemoryStateStore, JobSnapshot, JobStateStore};

/// Default polling cadence — also the TTL of the shared `qgroup -l`
/// listing cache. See [`SbatchManager::with_poll_interval`] for the
/// rationale behind 60 s.
const DEFAULT_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

#[derive(Clone)]
pub struct SbatchManager {
    cmd: SbatchCmd,
    store: Arc<dyn JobStateStore<SbatchJobSnapshot>>,
    dispatcher: Arc<dyn DynJobDispatcher>,
    poll_interval: std::time::Duration,
    scancel_bin: String,
    /// Shared `qgroup -l` listing cache (TTL = `poll_interval`), layered
    /// into the dispatcher handed to every handle this manager creates so
    /// N concurrently-polling handles spawn one qgroup subprocess per
    /// poll cycle instead of N.
    qgroup_cache: Arc<QgroupCacheState>,
    /// Shared squeue summary-query batch cache (TTL = `poll_interval`),
    /// the squeue-side counterpart of `qgroup_cache`: handles that fall
    /// through to the squeue probe share one batched `-j id1,id2,…`
    /// listing per poll cycle. See [`crate::sbatch::squeue_cache`].
    squeue_cache: Arc<SqueueCacheState>,
}

impl SbatchManager {
    pub fn new(cmd: SbatchCmd) -> Self {
        Self {
            cmd,
            store: Arc::new(InMemoryStateStore::<SbatchJobSnapshot>::new()),
            dispatcher: into_dyn(TokioDispatcher),
            poll_interval: DEFAULT_POLL_INTERVAL,
            scancel_bin: "scancel".to_string(),
            qgroup_cache: Arc::new(QgroupCacheState::new(DEFAULT_POLL_INTERVAL)),
            squeue_cache: Arc::new(SqueueCacheState::new(DEFAULT_POLL_INTERVAL)),
        }
    }

    /// Dispatcher handed to handles: `self.dispatcher` with the shared
    /// `qgroup -l` TTL cache and the squeue batch cache layered in.
    /// The two wrappers intercept disjoint argv shapes, so their order
    /// is immaterial. Submission-side calls (`sbatch` in `spawn`,
    /// `scancel` in `cancel`) keep using `self.dispatcher` directly —
    /// they never issue either query, and the wrappers pass every other
    /// argv through untouched anyway.
    fn handle_dispatcher(&self) -> Arc<dyn DynJobDispatcher> {
        let squeue_batched = into_dyn(SqueueBatchingDispatcher::new(
            self.dispatcher.clone(),
            self.squeue_cache.clone(),
        ));
        into_dyn(QgroupCachingDispatcher::new(
            squeue_batched,
            self.qgroup_cache.clone(),
        ))
    }

    #[must_use = "with_state_dir returns a new SbatchManager; the receiver is unchanged"]
    pub fn with_state_dir(mut self, root: impl Into<PathBuf>) -> Self {
        self.store = Arc::new(FileSystemStateStore::<SbatchJobSnapshot>::new(root));
        self
    }

    #[must_use = "with_state_store returns a new SbatchManager; the receiver is unchanged"]
    pub fn with_state_store(mut self, store: Arc<dyn JobStateStore<SbatchJobSnapshot>>) -> Self {
        self.store = store;
        self
    }

    /// Deliberately leaves `qgroup_cache` / `squeue_cache` untouched: the
    /// cache wrappers are (re)assembled around the *current* dispatcher at
    /// each handle creation (`handle_dispatcher`), so calling this before
    /// or after `with_poll_interval` works equally — the TTLs always come
    /// from `with_poll_interval`, the inner dispatcher always from here.
    #[must_use = "with_dispatcher returns a new SbatchManager; the receiver is unchanged"]
    pub fn with_dispatcher(mut self, dispatcher: Arc<dyn DynJobDispatcher>) -> Self {
        self.dispatcher = dispatcher;
        self
    }

    /// Override the polling cadence used by `run()` between `wait_terminal`
    /// iterations. Default is **60 s** — chosen to stay strictly longer than
    /// SLURM's default 30 s task sampling interval (so two consecutive polls
    /// cannot land inside a single sampling window) and to keep KUDPC
    /// squeue load low. Tests typically use 1–10 ms.
    ///
    /// Also sets the TTL of the shared `qgroup -l` listing cache and of
    /// the squeue batch cache to the same duration, so a manual
    /// `refresh()` is never staler than one poll cycle. Call this
    /// *before* `spawn`/`attach` — handles created earlier keep the
    /// caches built from the previous interval.
    #[must_use = "with_poll_interval returns a new SbatchManager; the receiver is unchanged"]
    pub fn with_poll_interval(mut self, dur: std::time::Duration) -> Self {
        self.poll_interval = dur;
        self.qgroup_cache = Arc::new(QgroupCacheState::new(dur));
        self.squeue_cache = Arc::new(SqueueCacheState::new(dur));
        self
    }

    /// Override the `scancel` binary used by [`Self::cancel`]. Defaults to
    /// `"scancel"` (resolved via `$PATH`). Tests and integration smokes
    /// can pass a fake-scancel script path to exercise the cancel flow
    /// without a real SLURM cluster.
    #[must_use = "with_scancel_bin returns a new SbatchManager; the receiver is unchanged"]
    pub fn with_scancel_bin(mut self, bin: impl Into<String>) -> Self {
        self.scancel_bin = bin.into();
        self
    }

    pub async fn spawn(&self) -> Result<SbatchJobHandle, SbatchSpawnError> {
        let argv = self.cmd.build_argv()?;
        let out = self
            .dispatcher
            .capture(&argv)
            .await
            .map_err(SbatchSpawnError::Other)?;
        if !out.success() {
            return Err(SbatchSpawnError::SubmitFailed {
                exit_code: out.exit_code,
                output: out.diagnostic(),
            });
        }
        let jobid = parse_submitted_jobid(&out.stdout).ok_or_else(|| {
            SbatchSpawnError::JobidParseError {
                stdout: out.stdout.clone(),
            }
        })?;

        let uuid = Uuid::now_v7();
        let script_path = std::path::absolute(&self.cmd.script)
            .with_context(|| format!("absolutize {}", self.cmd.script.display()))?;
        let snapshot = SbatchJobSnapshot {
            uuid,
            jobid,
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
            self.handle_dispatcher(),
        ))
    }

    /// Submit an array job in a single `sbatch --array=<spec>` invocation,
    /// then persist one snapshot per task and return one handle per task.
    ///
    /// All returned snapshots share the same master `jobid` (from sbatch's
    /// `Submitted batch job <N>` line) and the same `argv`, but each has a
    /// distinct `uuid` and `array_task_id`. `array_task_id.is_some()` is
    /// the sole discriminator between array tasks and singles.
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
        if task_indices.is_empty() {
            // FromStr already rejects empty specs; this guards against direct
            // struct construction that bypasses parsing.
            return Err(SbatchSpawnError::Other(anyhow::anyhow!(
                "spawn_array: SlurmArraySpec yielded zero task indices \
                 (constructed via FromStr should be non-empty)"
            )));
        }

        let mut cmd = self.cmd.clone();
        cmd.array_spec = Some(array_spec);
        let argv = cmd.build_argv()?;

        let out = self
            .dispatcher
            .capture(&argv)
            .await
            .map_err(SbatchSpawnError::Other)?;
        if !out.success() {
            return Err(SbatchSpawnError::SubmitFailed {
                exit_code: out.exit_code,
                output: out.diagnostic(),
            });
        }
        let master_jobid = parse_submitted_jobid(&out.stdout).ok_or_else(|| {
            SbatchSpawnError::JobidParseError {
                stdout: out.stdout.clone(),
            }
        })?;

        let script_path = std::path::absolute(&cmd.script)
            .with_context(|| format!("absolutize {}", cmd.script.display()))
            .map_err(SbatchSpawnError::Other)?;

        let mut handles = Vec::with_capacity(task_indices.len());
        for idx in task_indices {
            let snapshot = SbatchJobSnapshot {
                uuid: Uuid::now_v7(),
                jobid: master_jobid,
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
                self.handle_dispatcher(),
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
            self.handle_dispatcher(),
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

    /// Attach to all per-task snapshots of an array job by its master jobid.
    ///
    /// Returns handles sorted by `array_task_id` ascending. Empty result
    /// (`Ok(vec![])`) means "no array-task snapshots stored under this
    /// master jobid"; single-job snapshots (with `array_task_id == None`)
    /// are filtered out even if they share the master jobid.
    pub async fn attach_array_jobid(
        &self,
        master_jobid: u64,
    ) -> Result<Vec<SbatchJobHandle>, SbatchAttachError> {
        let snaps = self
            .store
            .find_all_by_jobid(master_jobid)
            .await
            .map_err(SbatchAttachError::Io)?;
        let mut filtered: Vec<SbatchJobSnapshot> = snaps
            .into_iter()
            .filter(|s| s.array_task_id.is_some())
            .collect();
        filtered.sort_by_key(|s| s.array_task_id);
        Ok(filtered
            .into_iter()
            .map(|snap| SbatchJobHandle::new(snap, self.store.clone(), self.handle_dispatcher()))
            .collect())
    }

    /// Submit a single sbatch job, block until terminal state, return
    /// `FinishedInfo`. See spec §6 for the full design rationale.
    ///
    /// **Not for array submissions.** Use `spawn_array` for those —
    /// `run()` returns `Err(SbatchRunError::ArrayNotSupported)` when
    /// `cmd.array_spec.is_some()`.
    ///
    /// Timeout: caller wraps with `tokio::time::timeout(dur, mgr.run())`,
    /// but the dropped future strands the jobid. To recover the jobid for
    /// `cancel(jobid)` after a timeout, use [`Self::run_with`] and capture
    /// the jobid in the `on_spawn` callback.
    pub async fn run(
        &self,
    ) -> Result<crate::sbatch::handle::FinishedInfo, crate::sbatch::error::SbatchRunError> {
        self.run_with(|_| {}).await
    }

    /// Same as [`Self::run`] but invokes `on_spawn(jobid)` **synchronously**
    /// the moment sbatch returns a parseable jobid. Designed for callers
    /// that need timeout-with-cancel ergonomics — capture the jobid in a
    /// shared cell before wrapping in `tokio::time::timeout`, then call
    /// `mgr.cancel(jobid)` if the timeout fires.
    ///
    /// `on_spawn` runs on the same task as `run_with` itself; it is NOT a
    /// tokio spawn. Keep it cheap — a single `oneshot::Sender::send` or
    /// `Mutex::lock().*=Some(...)` is the intended use.
    ///
    /// Example (spec §6.1 timeout pattern, now jobid-recoverable):
    ///
    /// ```ignore
    /// use std::sync::{Arc, Mutex};
    /// use std::time::Duration;
    ///
    /// let jobid_cell: Arc<Mutex<Option<u64>>> = Arc::default();
    /// let cell = jobid_cell.clone();
    /// match tokio::time::timeout(
    ///     Duration::from_secs(60),
    ///     mgr.run_with(move |j| *cell.lock().unwrap() = Some(j)),
    /// ).await {
    ///     Ok(Ok(info)) => /* happy path */ {},
    ///     Ok(Err(e)) => return Err(e),
    ///     Err(_elapsed) => {
    ///         if let Some(jid) = *jobid_cell.lock().unwrap() {
    ///             let _ = mgr.cancel(jid).await;
    ///         }
    ///         // else: spawn hadn't completed yet — nothing to cancel.
    ///     }
    /// }
    /// ```
    pub async fn run_with<F>(
        &self,
        on_spawn: F,
    ) -> Result<crate::sbatch::handle::FinishedInfo, crate::sbatch::error::SbatchRunError>
    where
        F: FnOnce(u64),
    {
        use crate::sbatch::error::SbatchRunError;
        if self.cmd.array_spec.is_some() {
            return Err(SbatchRunError::ArrayNotSupported);
        }
        let handle = self.spawn().await?;
        let jobid = handle.snapshot().jobid;
        on_spawn(jobid);
        handle
            .wait_terminal(self.poll_interval)
            .await
            .map_err(SbatchRunError::Wait)?;
        let snap = handle
            .refresh_with_sacct()
            .await
            .map_err(SbatchRunError::Sacct)?;
        let finished = snap
            .lifecycle
            .finished
            .ok_or(SbatchRunError::MissingFinished { jobid })?;
        match finished.final_state {
            crate::JobState::Completed => Ok(finished),
            other => Err(SbatchRunError::JobFailed {
                state: other,
                exit_code: finished.exit_code,
            }),
        }
    }

    /// Send `scancel <jobid>`. Idempotent at the SLURM side — sending
    /// scancel to a terminal job returns exit 0. Returns
    /// `SbatchCancelError::Scancel` if scancel itself reports a non-zero exit.
    ///
    /// The binary name comes from [`Self::with_scancel_bin`] (default
    /// `"scancel"`), so tests and integration smokes can substitute a
    /// fake-scancel script.
    pub async fn cancel(&self, jobid: u64) -> Result<(), SbatchCancelError> {
        let argv = vec![self.scancel_bin.clone(), jobid.to_string()];
        let out = self
            .dispatcher
            .capture(&argv)
            .await
            .map_err(SbatchCancelError::Other)?;
        if !out.success() {
            return Err(SbatchCancelError::Scancel {
                exit_code: out.exit_code,
                output: out.diagnostic(),
            });
        }
        Ok(())
    }
}

/// [`crate::job_manager::JobManager`] impl — delegates to the inherent
/// methods; array spawn/attach, `run`, and `cancel` stay sbatch-specific.
#[async_trait::async_trait]
impl crate::job_manager::JobManager for SbatchManager {
    type Handle = SbatchJobHandle;
    type SpawnError = SbatchSpawnError;
    type AttachError = SbatchAttachError;

    async fn spawn(&self) -> Result<SbatchJobHandle, SbatchSpawnError> {
        SbatchManager::spawn(self).await
    }

    async fn attach_uuid(&self, uuid: Uuid) -> Result<SbatchJobHandle, SbatchAttachError> {
        SbatchManager::attach_uuid(self, uuid).await
    }

    async fn attach_jobid(&self, jobid: u64) -> Result<SbatchJobHandle, SbatchAttachError> {
        SbatchManager::attach_jobid(self, jobid).await
    }
}

#[cfg(test)]
mod tests;
