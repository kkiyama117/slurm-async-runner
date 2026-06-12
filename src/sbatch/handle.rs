//! `SbatchJobSnapshot` and supporting lifecycle types. The runtime
//! handle (`SbatchJobHandle`) is appended in Task 9.
//!
//! See `docs/superpowers/specs/2026-05-10-sbatch-module-design.md` §6
//! and §9 for the full design.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::{Mutex as TokioMutex, watch};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::dispatcher::DynJobDispatcher;
use crate::entities::slurm::JobPartition;
use crate::sbatch::parse::resolve_log_path;
use crate::store::JobStateStore;
use crate::{JobReason, JobState, JobStatus};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SbatchJobSnapshot {
    pub uuid: Uuid,
    pub jobid: u64,

    /// Per-task index within the array (e.g. `0`, `1`, `4` for `-a 0-1,4`).
    /// `None` for single (non-array) jobs — this is the sole discriminator
    /// between array tasks and singles (the `jobid` field stores the master
    /// jobid for array tasks too, so it cannot be used to distinguish).
    /// SLURM prints array task identity as `<master>_<idx>` in
    /// `squeue -t`.
    ///
    /// Phase 3 (#8 B1) removed the previously-mirrored `array_jobid`
    /// field — old Phase 2 on-disk JSON files containing `array_jobid:
    /// <N>` still load fine because this struct does not set
    /// `#[serde(deny_unknown_fields)]`; the surplus field is silently
    /// discarded. New writes omit it.
    #[serde(default)]
    pub array_task_id: Option<u32>,

    pub argv: Vec<String>,
    pub sent_env: HashMap<String, String>,
    pub script_path: PathBuf,
    pub chdir: Option<PathBuf>,
    pub partition: Option<JobPartition>,
    pub job_name: Option<String>,
    pub submitted_at: DateTime<Utc>,

    pub log: LogPathSpec,

    pub lifecycle: SbatchLifecycle,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogPathSpec {
    pub output_template: Option<String>,
    pub error_template: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SbatchLifecycle {
    pub last_observed_state: Option<JobStatus>,
    pub last_observed_at: Option<DateTime<Utc>>,
    pub left_active_listing: bool,
    pub finished: Option<FinishedInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinishedInfo {
    pub final_state: JobState,
    pub final_reason: JobReason,
    pub exit_code: Option<i32>,
    pub finished_at: DateTime<Utc>,
}

impl SbatchLifecycle {
    pub fn is_running(&self) -> bool {
        if self.left_active_listing {
            return false;
        }
        self.last_observed_state
            .as_ref()
            .map(|s| s.state.is_running())
            .unwrap_or(false)
    }

    pub fn is_finished(&self) -> bool {
        self.finished.is_some()
    }

    /// Exit code if the child exited normally; `None` if killed by signal,
    /// or if `finished` is not yet recorded.
    pub fn exit_code(&self) -> Option<i32> {
        self.finished.as_ref().and_then(|f| f.exit_code)
    }
}

impl SbatchJobSnapshot {
    pub fn output_path(&self) -> Option<PathBuf> {
        self.log
            .output_template
            .as_deref()
            .map(|t| resolve_log_path(t, self.jobid, self.array_task_id, self.job_name.as_deref()))
    }

    pub fn error_path(&self) -> Option<PathBuf> {
        self.log
            .error_template
            .as_deref()
            .map(|t| resolve_log_path(t, self.jobid, self.array_task_id, self.job_name.as_deref()))
    }

    pub fn is_running(&self) -> bool {
        self.lifecycle.is_running()
    }
    pub fn is_finished(&self) -> bool {
        self.lifecycle.is_finished()
    }
    /// Exit code if the child exited normally; `None` if killed by signal,
    /// or if `finished` is not yet recorded.
    pub fn exit_code(&self) -> Option<i32> {
        self.lifecycle.exit_code()
    }
}

/// Which job log stream to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogStream {
    Stdout,
    Stderr,
}

/// Errors that can occur while reading a job's log file.
#[derive(Debug, Error)]
pub enum LogReadError {
    #[error("log path not resolved on snapshot (template missing)")]
    PathNotResolved,
    #[error("io error reading log: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
pub enum SbatchAttachKey {
    Uuid(Uuid),
    JobId(u64),
    File(PathBuf),
}

/// How many *consecutive* `refresh()` failures `wait_terminal` tolerates
/// before propagating the error.
///
/// Rationale: with failure classification in place (see
/// `crate::runner::ensure_query_success`), a transient `Socket timed
/// out` under SLURM controller overload now surfaces as a refresh `Err`
/// — and a single hiccup must not kill a multi-hour wait. Conversely,
/// 5 × `poll_interval` (5 minutes at the default 60 s cadence) of
/// uninterrupted failure indicates a real outage that the caller should
/// see. The counter resets on every successful refresh, so only an
/// unbroken failure streak trips the cap.
const MAX_CONSECUTIVE_REFRESH_FAILURES: u32 = 5;

/// Cheap-to-clone handle to an in-flight or attached sbatch job. All
/// snapshot reads are lock-free; `refresh` / `refresh_with_sacct` /
/// `wait_terminal` serialize through `refresh_lock`.
#[derive(Clone)]
pub struct SbatchJobHandle(pub(crate) Arc<SbatchJobHandleInner>);

pub(crate) struct SbatchJobHandleInner {
    pub(crate) snapshot_tx: watch::Sender<SbatchJobSnapshot>,
    pub(crate) store: Arc<dyn JobStateStore<SbatchJobSnapshot>>,
    pub(crate) dispatcher: Arc<dyn DynJobDispatcher>,
    pub(crate) refresh_lock: TokioMutex<()>,
}

/// Drop on the inner (Arc-shared) state — fires once when the last
/// clone of `SbatchJobHandle` is released. Per spec §6.5, dropping a
/// handle whose snapshot still appears to be running is treated as a
/// silent bug magnet and earns a `tracing::warn!`. The drop is
/// deliberately NOT auto-cancelling: SLURM jobs survive their owning
/// handles, and recovering one via `SbatchManager::attach_jobid` /
/// `attach_uuid` is the expected workflow. Use
/// `SbatchManager::cancel(jobid)` explicitly when termination is
/// intended.
impl Drop for SbatchJobHandleInner {
    fn drop(&mut self) {
        let snap = self.snapshot_tx.borrow();
        if snap.is_running() {
            tracing::warn!(
                jobid = snap.jobid,
                uuid = %snap.uuid,
                "SbatchJobHandle dropped while job still appears to be running; \
                 the job was NOT auto-cancelled. Call SbatchManager::cancel(jobid) \
                 explicitly if termination is intended."
            );
        }
    }
}

impl SbatchJobHandle {
    pub(crate) fn new(
        snapshot: SbatchJobSnapshot,
        store: Arc<dyn JobStateStore<SbatchJobSnapshot>>,
        dispatcher: Arc<dyn DynJobDispatcher>,
    ) -> Self {
        let (tx, _rx) = watch::channel(snapshot);
        Self(Arc::new(SbatchJobHandleInner {
            snapshot_tx: tx,
            store,
            dispatcher,
            refresh_lock: TokioMutex::new(()),
        }))
    }

    // -------- Lock-free snapshot reads --------

    pub fn snapshot(&self) -> SbatchJobSnapshot {
        self.0.snapshot_tx.borrow().clone()
    }

    pub fn watch(&self) -> watch::Receiver<SbatchJobSnapshot> {
        self.0.snapshot_tx.subscribe()
    }

    pub fn uuid(&self) -> Uuid {
        self.0.snapshot_tx.borrow().uuid
    }

    pub fn jobid(&self) -> Option<u64> {
        Some(self.0.snapshot_tx.borrow().jobid)
    }

    pub fn array_task_id(&self) -> Option<u32> {
        self.snapshot().array_task_id
    }

    pub fn partition(&self) -> Option<JobPartition> {
        self.0.snapshot_tx.borrow().partition.clone()
    }

    pub fn job_name(&self) -> Option<String> {
        self.0.snapshot_tx.borrow().job_name.clone()
    }

    pub fn sent_env(&self) -> HashMap<String, String> {
        self.0.snapshot_tx.borrow().sent_env.clone()
    }

    pub fn output_template(&self) -> Option<String> {
        self.0.snapshot_tx.borrow().log.output_template.clone()
    }

    pub fn error_template(&self) -> Option<String> {
        self.0.snapshot_tx.borrow().log.error_template.clone()
    }

    pub fn output_path(&self) -> Option<PathBuf> {
        self.0.snapshot_tx.borrow().output_path()
    }

    pub fn error_path(&self) -> Option<PathBuf> {
        self.0.snapshot_tx.borrow().error_path()
    }

    pub fn is_running(&self) -> bool {
        self.0.snapshot_tx.borrow().is_running()
    }

    pub fn is_finished(&self) -> bool {
        self.0.snapshot_tx.borrow().is_finished()
    }

    /// Exit code if the child exited normally; `None` if killed by signal,
    /// or if `finished` is not yet recorded.
    pub fn exit_code(&self) -> Option<i32> {
        self.0.snapshot_tx.borrow().exit_code()
    }

    // -------- Log read API (Phase 2 P1) --------

    /// Read the last `n` lines of the job's stdout/stderr log file.
    ///
    /// Returns an empty `Vec` if the log file does not yet exist (job
    /// pending or just submitted). Returns `LogReadError::PathNotResolved`
    /// if the snapshot does not carry the corresponding log template.
    /// Other I/O errors are propagated as `LogReadError::Io`.
    ///
    /// Reads the file *backwards* in fixed-size chunks and stops as soon
    /// as `n` lines are covered, so memory stays bounded by
    /// `O(n × line length)` even for multi-GB HPC job logs (the previous
    /// implementation loaded the whole file).
    pub async fn log_lines(
        &self,
        stream: LogStream,
        n: usize,
    ) -> Result<Vec<String>, LogReadError> {
        let snap = self.0.snapshot_tx.borrow().clone();
        let path = match stream {
            LogStream::Stdout => snap.output_path(),
            LogStream::Stderr => snap.error_path(),
        };
        let path = path.ok_or(LogReadError::PathNotResolved)?;
        match read_last_lines(&path, n).await {
            Ok(lines) => Ok(lines),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(LogReadError::Io(e)),
        }
    }

    /// Read the full contents of the job's stdout/stderr log file.
    ///
    /// Returns an empty string if the log file does not yet exist.
    /// Same error semantics as [`SbatchJobHandle::log_lines`] otherwise.
    /// Unlike `log_lines` this intentionally loads the whole file —
    /// that is its contract; use `log_lines` for tail reads.
    pub async fn read_log_to_end(&self, stream: LogStream) -> Result<String, LogReadError> {
        let snap = self.0.snapshot_tx.borrow().clone();
        let path = match stream {
            LogStream::Stdout => snap.output_path(),
            LogStream::Stderr => snap.error_path(),
        };
        let path = path.ok_or(LogReadError::PathNotResolved)?;
        match tokio::fs::read_to_string(&path).await {
            Ok(content) => Ok(content),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
            Err(e) => Err(LogReadError::Io(e)),
        }
    }

    /// Lightweight polling: `qgroup -l` → `squeue` fallback. **Never** calls
    /// sacct. If both lookups miss, sets `lifecycle.left_active_listing = true`.
    /// A failing `qgroup` invocation (e.g. the binary does not exist on
    /// non-KUDPC clusters) is logged and treated as a miss, not an error.
    ///
    /// For handles produced by `spawn_array` / `attach_array_jobid`
    /// (i.e. `array_task_id.is_some()`), this branches into a per-task
    /// `squeue -j <master>_<idx>` lookup. `qgroup -l` is skipped on
    /// that path because KUDPC's `qgroup` returns one summary row per
    /// submission rather than per task — every per-task handle would
    /// otherwise observe the same master-summary state and drift apart
    /// only on `refresh_with_sacct`. See spec §5.5 and issue #8 A5.
    pub async fn refresh(&self) -> anyhow::Result<SbatchJobSnapshot> {
        let inner = &*self.0;
        let _guard = inner.refresh_lock.lock().await;

        let mut snap = inner.snapshot_tx.borrow().clone();
        let now = chrono::Utc::now();
        let view = crate::dispatcher::DynView(&*inner.dispatcher);

        // Array-task path: query SLURM with `<master>_<idx>` via squeue
        // (qgroup -l would only surface the master summary).
        if let Some(idx) = snap.array_task_id {
            let task = crate::runner::query_array_task_state_with(&view, snap.jobid, idx).await?;
            if let Some(status) = task {
                snap.lifecycle.last_observed_state = Some(status);
                snap.lifecycle.last_observed_at = Some(now);
            } else {
                snap.lifecycle.left_active_listing = true;
                snap.lifecycle.last_observed_at = Some(now);
            }
            inner.store.save(&snap).await?;
            inner.snapshot_tx.send_replace(snap.clone());
            return Ok(snap);
        }

        // `qgroup` is KUDPC-specific: on clusters without the binary the
        // spawn itself fails. Treat any qgroup error as a miss (with a
        // warning so genuine qgroup breakage on KUDPC stays visible) and
        // fall through to the squeue probe instead of failing the refresh.
        let qgroup =
            match crate::runner::query_job_states_via_qgroup_with(&view, &[snap.jobid]).await {
                Ok(map) => map,
                Err(e) => {
                    tracing::warn!(
                        jobid = snap.jobid,
                        error = %e,
                        "qgroup -l query failed; treating as a miss and falling back to squeue"
                    );
                    HashMap::new()
                }
            };
        if let Some(status) = qgroup.get(&snap.jobid) {
            snap.lifecycle.last_observed_state = Some(status.clone());
            snap.lifecycle.last_observed_at = Some(now);
            inner.store.save(&snap).await?;
            inner.snapshot_tx.send_replace(snap.clone());
            return Ok(snap);
        }

        let squeue = crate::runner::query_job_states_squeue_only_with(&view, &[snap.jobid]).await?;
        if let Some(status) = squeue.get(&snap.jobid) {
            snap.lifecycle.last_observed_state = Some(status.clone());
            snap.lifecycle.last_observed_at = Some(now);
            inner.store.save(&snap).await?;
            inner.snapshot_tx.send_replace(snap.clone());
            return Ok(snap);
        }

        snap.lifecycle.left_active_listing = true;
        snap.lifecycle.last_observed_at = Some(now);
        inner.store.save(&snap).await?;
        inner.snapshot_tx.send_replace(snap.clone());
        Ok(snap)
    }

    /// Heavyweight finalizer. Calls `refresh()` first; invokes sacct if
    /// the job has reached a terminal state (observed via `qgroup -l` /
    /// `squeue`) **or** has left both active listings, **and**
    /// `lifecycle.finished` is still None. Otherwise behaves identically
    /// to `refresh()`.
    ///
    /// The terminal-state-from-qgroup branch is required because KUDPC's
    /// `qgroup -l` reports `FINI` (mapped to `JobState::Completed`) for
    /// freshly-finished jobs while they are still in the listing — i.e.
    /// `left_active_listing` is still `false` but the job is provably
    /// done and sacct is the authoritative source for `exit_code`.
    ///
    /// For array-task handles (`array_task_id.is_some()`), the sacct
    /// call queries `sacct -j <master>` (sacct expands the master key
    /// into per-task rows) and extracts this task's `<master>_<idx>`
    /// parent row, so the captured `FinishedInfo` reflects the
    /// individual task — not the master summary. Step rows (`.batch`,
    /// `.0`, …) are filtered. Keying by the master makes every task
    /// finalizer of one array share a single sacct batch-cache entry —
    /// see `crate::runner::query_array_task_outcome_with`. See spec
    /// §5.5 and issue #8 A5.
    ///
    /// If sacct has no usable row yet (accounting flush lag, or the job
    /// was purged from history), `lifecycle.finished` is left unset so a
    /// later call can retry — a fabricated `Unknown` outcome is never
    /// recorded.
    pub async fn refresh_with_sacct(&self) -> anyhow::Result<SbatchJobSnapshot> {
        let mut snap = self.refresh().await?;
        if snap.lifecycle.finished.is_some() {
            return Ok(snap);
        }
        let observed_terminal = snap
            .lifecycle
            .last_observed_state
            .as_ref()
            .is_some_and(|s| s.state.is_terminal());
        if !snap.lifecycle.left_active_listing && !observed_terminal {
            return Ok(snap);
        }

        let inner = &*self.0;
        // Re-acquire refresh_lock here to serialize the sacct call itself.
        // The inner `refresh()` above already took and released the lock
        // for the qgroup/squeue probes; this second acquisition guards the
        // heavier sacct invocation and the finished-info write.
        let _guard = inner.refresh_lock.lock().await;
        let view = crate::dispatcher::DynView(&*inner.dispatcher);

        // Both paths key sacct by the (master) jobid. The array path
        // extracts its own `<master>_<idx>` parent row from the
        // master-expanded listing; the single-job path reads its row
        // from the batched map.
        let outcome = if let Some(idx) = snap.array_task_id {
            crate::runner::query_array_task_outcome_with(&view, snap.jobid, idx)
                .await?
                .unwrap_or(crate::runner::JobOutcome {
                    status: JobStatus::default(),
                    exit_code: None,
                })
        } else {
            // Phase 2 P1: exit-code-aware query so we can populate
            // FinishedInfo::exit_code instead of leaving it None.
            let map =
                crate::runner::query_job_states_with_exit_code_with(&view, &[snap.jobid]).await?;
            map.get(&snap.jobid)
                .cloned()
                .unwrap_or(crate::runner::JobOutcome {
                    status: JobStatus::default(),
                    exit_code: None,
                })
        };

        // sacct may lag behind qgroup/squeue (accounting flush delay) or
        // may have purged the job from history entirely. Both cases yield
        // no usable row — the queries above synthesize a default outcome
        // (state Unknown, no exit code). Leave `finished` unset so a later
        // call can retry once accounting catches up, instead of freezing
        // the fabricated Unknown outcome behind the idempotency
        // short-circuit at the top of this method. Callers can detect the
        // vanished-but-unresolved situation via
        // `left_active_listing == true && finished.is_none()`.
        //
        // This sentinel relies on every sacct query above selecting the
        // `ExitCode` column: a row that actually exists always yields
        // `exit_code: Some(_)` (sacct prints `<exit>:<signal>` for every
        // parent row), so `Unknown + None` can only mean "no usable row".
        // Keep that column in the argv if the queries are ever reworked.
        if outcome.status.state == JobState::Unknown && outcome.exit_code.is_none() {
            return Ok(snap);
        }

        // A *known non-terminal* row (RUNNING/PENDING/…) means the job is
        // provably still alive — the earlier vanish observation was a
        // false positive (e.g. a transient `squeue` failure under
        // controller overload yields empty stdout, indistinguishable from
        // a purge). Never stamp FinishedInfo from it: roll back the
        // vanish flag and record the live observation so `is_running()` /
        // `wait_terminal` resume normal polling. `Unknown` is exempt so
        // an unrecognized future terminal token with a real exit code
        // (forward-compat) still resolves.
        if outcome.status.state != JobState::Unknown && !outcome.status.state.is_terminal() {
            snap.lifecycle.left_active_listing = false;
            snap.lifecycle.last_observed_state = Some(outcome.status);
            snap.lifecycle.last_observed_at = Some(chrono::Utc::now());
            inner.store.save(&snap).await?;
            inner.snapshot_tx.send_replace(snap.clone());
            return Ok(snap);
        }

        snap.lifecycle.finished = Some(FinishedInfo {
            final_state: outcome.status.state,
            final_reason: outcome.status.reason,
            exit_code: outcome.exit_code,
            finished_at: chrono::Utc::now(),
        });
        inner.store.save(&snap).await?;
        inner.snapshot_tx.send_replace(snap.clone());
        Ok(snap)
    }

    /// Lightweight polling loop. Calls `refresh()` (sacct-free) at the
    /// supplied interval until either (a) the observed state is terminal,
    /// or (b) the job leaves both active listings. Caller may follow up
    /// with one `refresh_with_sacct()` if exit_code resolution is needed.
    ///
    /// Up to [`MAX_CONSECUTIVE_REFRESH_FAILURES`] consecutive `refresh()`
    /// errors are tolerated (each is logged via `tracing::warn!` and the
    /// loop keeps polling); the counter resets on any success, and the
    /// error propagates once the cap is hit.
    pub async fn wait_terminal(
        &self,
        poll_interval: std::time::Duration,
    ) -> anyhow::Result<SbatchJobSnapshot> {
        let mut consecutive_failures: u32 = 0;
        loop {
            match self.refresh().await {
                Ok(snap) => {
                    consecutive_failures = 0;
                    if let Some(state) = &snap.lifecycle.last_observed_state
                        && state.state.is_terminal()
                    {
                        return Ok(snap);
                    }
                    if snap.lifecycle.left_active_listing {
                        return Ok(snap);
                    }
                }
                Err(e) => {
                    consecutive_failures += 1;
                    if consecutive_failures >= MAX_CONSECUTIVE_REFRESH_FAILURES {
                        return Err(e);
                    }
                    tracing::warn!(
                        jobid = self.0.snapshot_tx.borrow().jobid,
                        consecutive_failures,
                        max = MAX_CONSECUTIVE_REFRESH_FAILURES,
                        error = %e,
                        "wait_terminal: refresh failed transiently; continuing to poll"
                    );
                }
            }
            tokio::time::sleep(poll_interval).await;
        }
    }
}

/// Phase 3 P3: cross-backend trait implementation. All methods delegate
/// to the existing inherent `SbatchJobHandle` API — there is no behavior
/// change, just a uniform contract that callers can hold via
/// `H: JobHandleCommon`.
#[async_trait::async_trait]
impl crate::handle::JobHandleCommon for SbatchJobHandle {
    type Snapshot = SbatchJobSnapshot;

    fn uuid(&self) -> Uuid {
        Self::uuid(self)
    }
    fn jobid(&self) -> Option<u64> {
        Self::jobid(self)
    }
    fn is_running(&self) -> bool {
        Self::is_running(self)
    }
    fn is_finished(&self) -> bool {
        Self::is_finished(self)
    }
    fn exit_code(&self) -> Option<i32> {
        Self::exit_code(self)
    }

    fn snapshot(&self) -> SbatchJobSnapshot {
        Self::snapshot(self)
    }
    fn watch(&self) -> watch::Receiver<SbatchJobSnapshot> {
        Self::watch(self)
    }

    async fn refresh(&self) -> anyhow::Result<SbatchJobSnapshot> {
        Self::refresh(self).await
    }

    async fn wait_terminal(
        &self,
        poll_interval: std::time::Duration,
    ) -> anyhow::Result<SbatchJobSnapshot> {
        Self::wait_terminal(self, poll_interval).await
    }
}

/// Chunk size for the backwards tail read in [`read_last_lines`]. 8 KiB
/// covers `n × typical-line-length` for the common `log_lines(_, 100)`
/// case in one or two reads while keeping worst-case memory at
/// `O(n × line length)`, not `O(file size)`.
const TAIL_READ_CHUNK: u64 = 8192;

/// Return the last `n` lines of `path` without reading the whole file.
///
/// Reads backwards in [`TAIL_READ_CHUNK`]-sized chunks and stops once the
/// accumulated buffer contains more than `n` newlines (i.e. at least `n`
/// complete lines after the potentially partial first one). Line
/// semantics match [`str::lines`] — a final line without a trailing
/// newline counts as a line — so small files behave identically to the
/// previous whole-file implementation.
///
/// UTF-8 safety: the buffer is only decoded (lossily) after assembly.
/// The buffer can start mid-multibyte-character only when the loop broke
/// early on the newline budget; in that case the buffer holds more than
/// `n` newlines, so `lines.len() > n` is guaranteed and the damaged
/// fragment occupies the leading line that `drain` removes. A buffer
/// that reaches back to the start of the file is never damaged.
pub(crate) async fn read_last_lines(
    path: &std::path::Path,
    n: usize,
) -> std::io::Result<Vec<String>> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    if n == 0 {
        return Ok(Vec::new());
    }
    let mut file = tokio::fs::File::open(path).await?;
    let mut pos = file.metadata().await?.len();
    let mut buf: Vec<u8> = Vec::new();

    while pos > 0 {
        let read_len = TAIL_READ_CHUNK.min(pos);
        pos -= read_len;
        file.seek(std::io::SeekFrom::Start(pos)).await?;
        let mut chunk = vec![0u8; read_len as usize];
        file.read_exact(&mut chunk).await?;
        chunk.extend_from_slice(&buf);
        buf = chunk;
        if buf.iter().filter(|&&b| b == b'\n').count() > n {
            break;
        }
    }

    let text = String::from_utf8_lossy(&buf);
    let mut lines: Vec<String> = text.lines().map(|s| s.to_string()).collect();
    if lines.len() > n {
        lines.drain(..lines.len() - n);
    }
    Ok(lines)
}

#[cfg(test)]
mod tests;
