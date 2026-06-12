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
    /// call queries `sacct -j <master>_<idx>` so the captured
    /// `FinishedInfo` reflects the individual task — not the master
    /// summary. Step rows (`.batch`, `.0`, …) are filtered. See spec
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

        // Array-task path: per-task sacct with `<master>_<idx>` key.
        // Single-job path: existing batched query keyed by master jobid.
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
mod tests {
    use super::*;

    fn snap(jobid: u64) -> SbatchJobSnapshot {
        SbatchJobSnapshot {
            uuid: Uuid::now_v7(),
            jobid,
            array_task_id: None,
            argv: vec!["sbatch".into(), "/w/job.sh".into()],
            sent_env: HashMap::from([("FOO".into(), "bar".into())]),
            script_path: PathBuf::from("/w/job.sh"),
            chdir: Some(PathBuf::from("/w")),
            partition: Some("gr19999b".into()),
            job_name: Some("g09run".into()),
            submitted_at: chrono::Utc::now(),
            log: LogPathSpec {
                output_template: Some("slurm-%j.out".into()),
                error_template: Some("slurm-%j.err".into()),
            },
            lifecycle: SbatchLifecycle::default(),
        }
    }

    #[test]
    fn snapshot_round_trips_through_json() {
        let s = snap(12345);
        let raw = serde_json::to_string(&s).unwrap();
        let back: SbatchJobSnapshot = serde_json::from_str(&raw).unwrap();
        assert_eq!(s, back);
    }

    #[test]
    fn output_path_substitutes_jobid_lazily() {
        let s = snap(12345);
        assert_eq!(s.output_path(), Some(PathBuf::from("slurm-12345.out")));
        assert_eq!(s.error_path(), Some(PathBuf::from("slurm-12345.err")));
    }

    #[test]
    fn lifecycle_running_requires_state_and_no_vanish() {
        let mut s = snap(1);
        assert!(!s.is_running());
        s.lifecycle.last_observed_state = Some(JobStatus {
            state: JobState::Running,
            reason: JobReason::None,
        });
        assert!(s.is_running());
        s.lifecycle.left_active_listing = true;
        assert!(!s.is_running());
    }

    #[test]
    fn lifecycle_finished_records_exit_code() {
        let mut s = snap(1);
        assert!(!s.is_finished());
        assert_eq!(s.exit_code(), None);
        s.lifecycle.finished = Some(FinishedInfo {
            final_state: JobState::Completed,
            final_reason: JobReason::None,
            exit_code: Some(0),
            finished_at: chrono::Utc::now(),
        });
        assert!(s.is_finished());
        assert_eq!(s.exit_code(), Some(0));
    }

    #[tokio::test]
    async fn handle_lock_free_getters_return_initial_snapshot() {
        use crate::dispatcher::{DryRunDispatcher, into_dyn};
        use crate::store::InMemoryStateStore;

        let s = snap(99);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(DryRunDispatcher);
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);

        assert_eq!(h.uuid(), s.uuid);
        assert_eq!(h.jobid(), Some(99));
        assert_eq!(h.job_name().as_deref(), Some("g09run"));
        assert_eq!(h.output_path(), Some(PathBuf::from("slurm-99.out")));
        assert!(!h.is_running());
        assert!(!h.is_finished());
    }

    #[tokio::test]
    async fn handle_clone_shares_inner() {
        use crate::dispatcher::{DryRunDispatcher, into_dyn};
        use crate::store::InMemoryStateStore;

        let s = snap(1);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(DryRunDispatcher);
        let h1 = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let h2 = h1.clone();
        assert_eq!(h1.uuid(), h2.uuid());
        let _r1 = h1.watch();
        let _r2 = h2.watch();
    }

    // ---- CannedDispatcher mock for refresh tests ----

    use crate::dispatcher::CaptureOutput;

    /// stdout-only success [`CaptureOutput`] — the common canned shape.
    fn cap_ok(stdout: &str) -> CaptureOutput {
        CaptureOutput {
            exit_code: 0,
            stdout: stdout.to_string(),
            stderr: String::new(),
        }
    }

    /// Mock dispatcher: returns canned outputs keyed by argv[0].
    /// argv[0] = "qgroup" → first canned, "squeue" → second, "sacct" → third.
    /// The constructor takes stdout-only `&str` values for ergonomics;
    /// tests that need a nonzero exit / stderr replace the whole
    /// `CaptureOutput` through the field's `Mutex`.
    struct CannedDispatcher {
        qgroup: std::sync::Mutex<CaptureOutput>,
        squeue: std::sync::Mutex<CaptureOutput>,
        sacct: std::sync::Mutex<CaptureOutput>,
        sacct_call_count: std::sync::Mutex<u32>,
    }

    impl CannedDispatcher {
        fn new(qgroup: &str, squeue: &str, sacct: &str) -> Self {
            Self {
                qgroup: std::sync::Mutex::new(cap_ok(qgroup)),
                squeue: std::sync::Mutex::new(cap_ok(squeue)),
                sacct: std::sync::Mutex::new(cap_ok(sacct)),
                sacct_call_count: std::sync::Mutex::new(0),
            }
        }

        fn sacct_calls(&self) -> u32 {
            *self.sacct_call_count.lock().unwrap()
        }
    }

    impl crate::dispatcher::JobDispatcher for CannedDispatcher {
        async fn run(&self, _argv: &[String]) -> anyhow::Result<i32> {
            unimplemented!()
        }

        async fn capture(&self, argv: &[String]) -> anyhow::Result<CaptureOutput> {
            let bin = argv[0].as_str();
            let out = match bin {
                "qgroup" => self.qgroup.lock().unwrap().clone(),
                "squeue" => self.squeue.lock().unwrap().clone(),
                "sacct" => {
                    *self.sacct_call_count.lock().unwrap() += 1;
                    self.sacct.lock().unwrap().clone()
                }
                _ => CaptureOutput::default(),
            };
            Ok(out)
        }
    }

    // Shared Arc-wrapper for `Arc<D>` → `dyn JobDispatcher` coercion
    // lives in `crate::sbatch::test_util` so handle.rs and manager.rs
    // both consume the same generic helper.
    use crate::sbatch::test_util::ArcDispatcher as MoveDispatcher;

    #[tokio::test]
    async fn refresh_uses_qgroup_when_jobid_present() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;

        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(CannedDispatcher::new(
            "QUEUE USER JOBID STATUS PROC\ngr u 12345 RUN 1\n",
            "",
            "",
        ));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh().await.unwrap();
        assert_eq!(
            after.lifecycle.last_observed_state.unwrap().state,
            crate::JobState::Running
        );
        assert!(!after.lifecycle.left_active_listing);
    }

    #[tokio::test]
    async fn refresh_falls_back_to_squeue_when_qgroup_misses() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;

        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(CannedDispatcher::new("", "12345 RUNNING None\n", ""));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh().await.unwrap();
        assert_eq!(
            after.lifecycle.last_observed_state.unwrap().state,
            crate::JobState::Running
        );
    }

    #[tokio::test]
    async fn refresh_marks_left_active_listing_when_both_miss() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;

        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let canned = std::sync::Arc::new(CannedDispatcher::new("", "", ""));
        // Wrap a clone of the Arc in into_dyn; keep the original Arc to
        // observe sacct call count after refresh.
        let dispatcher = {
            let canned = canned.clone();
            into_dyn(MoveDispatcher(canned))
        };
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh().await.unwrap();
        assert!(after.lifecycle.left_active_listing);
        assert_eq!(canned.sacct_calls(), 0, "refresh must NOT call sacct");
    }

    #[tokio::test]
    async fn refresh_with_sacct_skips_sacct_when_qgroup_hits() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;
        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let canned = std::sync::Arc::new(CannedDispatcher::new(
            "QUEUE USER JOBID STATUS PROC\ngr u 12345 RUN 1\n",
            "",
            "",
        ));
        let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh_with_sacct().await.unwrap();
        assert!(after.lifecycle.finished.is_none());
        assert_eq!(canned.sacct_calls(), 0);
    }

    /// Idempotency: once `finished` is populated and re-readable from the
    /// watch channel (requires `send_replace`, not `send`), subsequent
    /// `refresh_with_sacct` calls must short-circuit and not re-invoke
    /// sacct. Regression guard for the live KUDPC failure where every
    /// call burned a sacct invocation because the watch channel never
    /// updated due to receiver_count == 0.
    #[tokio::test]
    async fn refresh_with_sacct_is_idempotent_when_finished_already_set() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;
        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let canned =
            std::sync::Arc::new(CannedDispatcher::new("", "", "12345|COMPLETED|None|0:0\n"));
        let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let _first = h.refresh_with_sacct().await.unwrap();
        assert_eq!(canned.sacct_calls(), 1);
        let _second = h.refresh_with_sacct().await.unwrap();
        assert_eq!(
            canned.sacct_calls(),
            1,
            "second call must not re-invoke sacct (idempotency via send_replace)",
        );
    }

    /// Regression: when qgroup -l reports a *terminal* state (FINI / CMP
    /// → `Completed`), `refresh_with_sacct` must still call sacct to
    /// resolve `exit_code`. Previously it short-circuited on
    /// `!left_active_listing`, leaving `lifecycle.finished == None` and
    /// `is_finished() == false` even after the post-`wait_terminal` call.
    /// Live KUDPC reproducer: a 5-second job whose qgroup row reads
    /// `FINI` while still in the listing.
    #[tokio::test]
    async fn refresh_with_sacct_calls_sacct_when_qgroup_reports_terminal_fini() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;
        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let canned = std::sync::Arc::new(CannedDispatcher::new(
            // KUDPC pipe layout, terminal token FINI — job still listed
            // (so left_active_listing stays false through refresh()).
            " gr u 12345 | FINI 2026-05-12 01:48 | 1 | 1 1 1M 00:00:05\n",
            "",
            "12345|COMPLETED|None|0:0\n",
        ));
        let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh_with_sacct().await.unwrap();
        assert!(
            !after.lifecycle.left_active_listing,
            "qgroup still listed the job; left_active_listing must stay false",
        );
        assert_eq!(
            canned.sacct_calls(),
            1,
            "terminal qgroup state must still trigger sacct for exit_code resolution",
        );
        let finished = after
            .lifecycle
            .finished
            .expect("FinishedInfo must be populated after sacct");
        assert_eq!(finished.final_state, crate::JobState::Completed);
        assert_eq!(finished.exit_code, Some(0));
    }

    #[tokio::test]
    async fn refresh_with_sacct_calls_sacct_once_after_vanish() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;
        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let canned =
            std::sync::Arc::new(CannedDispatcher::new("", "", "12345|COMPLETED|None|0:0\n"));
        let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh_with_sacct().await.unwrap();
        assert!(after.lifecycle.finished.is_some());
        assert_eq!(canned.sacct_calls(), 1);
    }

    #[tokio::test]
    async fn refresh_with_sacct_populates_exit_code_on_completed() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;
        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        // sacct now emits 4 columns (JobID|State|Reason|ExitCode)
        let canned =
            std::sync::Arc::new(CannedDispatcher::new("", "", "12345|COMPLETED|None|0:0\n"));
        let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh_with_sacct().await.unwrap();
        let finished = after.lifecycle.finished.expect("finished should be Some");
        assert_eq!(finished.final_state, crate::JobState::Completed);
        assert_eq!(finished.exit_code, Some(0));
    }

    #[tokio::test]
    async fn refresh_with_sacct_populates_exit_code_on_signaled() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;
        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let canned =
            std::sync::Arc::new(CannedDispatcher::new("", "", "12345|CANCELLED|None|0:9\n"));
        let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh_with_sacct().await.unwrap();
        let finished = after.lifecycle.finished.expect("finished should be Some");
        // SIGKILL = 9 -> 128 + 9 = 137
        assert_eq!(finished.exit_code, Some(137));
    }

    // ---- Array-task refresh (issue #8 A5, spec §5.5) ----

    /// Build an array-task snapshot rooted at `master_jobid` with task
    /// index `idx`. The `CannedDispatcher` does not inspect argv beyond
    /// `argv[0]`, so the test rig does not need to know about the
    /// `<master>_<idx>` key — the snapshot wiring carries it.
    fn snap_array_task(master_jobid: u64, idx: u32) -> SbatchJobSnapshot {
        let mut s = snap(master_jobid);
        s.array_task_id = Some(idx);
        s
    }

    #[tokio::test]
    async fn refresh_array_task_uses_squeue_and_skips_qgroup() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;

        // qgroup output deliberately contains the master jobid — proves
        // the array path takes the squeue branch instead of falling
        // through to qgroup.
        let s = snap_array_task(12345, 3);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(CannedDispatcher::new(
            "QUEUE USER JOBID STATUS PROC\ngr u 12345 RUN 1\n",
            "RUNNING None\n",
            "",
        ));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh().await.unwrap();
        assert_eq!(
            after.lifecycle.last_observed_state.unwrap().state,
            crate::JobState::Running
        );
        assert!(!after.lifecycle.left_active_listing);
    }

    #[tokio::test]
    async fn refresh_array_task_marks_vanished_when_squeue_empty() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;

        // Empty squeue output means the task has left the active queue.
        let s = snap_array_task(12345, 3);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let canned = std::sync::Arc::new(CannedDispatcher::new("", "", ""));
        let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh().await.unwrap();
        assert!(after.lifecycle.left_active_listing);
        assert_eq!(
            canned.sacct_calls(),
            0,
            "refresh on an array task must NOT call sacct"
        );
    }

    #[tokio::test]
    async fn refresh_with_sacct_array_task_populates_exit_code() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;

        // The canned sacct returns a single parent row keyed by
        // `<master>_<idx>` (12345_3). The summary-mode parser would
        // discard this row because the jobid column is not a `u64`;
        // verify the array-task parser keeps it.
        let s = snap_array_task(12345, 3);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let canned = std::sync::Arc::new(CannedDispatcher::new(
            "",
            "",
            "12345_3|COMPLETED|None|0:0\n12345_3.batch|COMPLETED|None|0:0\n",
        ));
        let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh_with_sacct().await.unwrap();
        let finished = after.lifecycle.finished.expect("finished should be Some");
        assert_eq!(finished.final_state, crate::JobState::Completed);
        assert_eq!(finished.exit_code, Some(0));
        assert_eq!(canned.sacct_calls(), 1);
    }

    /// Regression (vanished-jobid classification): once the array master
    /// has left squeue entirely, `squeue -j <master>_<idx>` prints
    /// `slurm_load_jobs error: Invalid job id specified` on stderr and
    /// exits 1 (KUDPC purges terminated jobs from the queue immediately).
    /// That combination is the *vanish* signal — the refresh must set
    /// `left_active_listing = true`, never record a bogus observation,
    /// and never treat it as a query failure.
    #[tokio::test]
    async fn refresh_array_task_treats_stderr_only_squeue_as_vanished() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;

        let s = snap_array_task(12345, 3);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let canned = std::sync::Arc::new(CannedDispatcher::new("", "", ""));
        *canned.squeue.lock().unwrap() = CaptureOutput {
            exit_code: 1,
            stdout: String::new(),
            stderr: "slurm_load_jobs error: Invalid job id specified\n".into(),
        };
        let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh().await.unwrap();
        assert!(
            after.lifecycle.left_active_listing,
            "vanished-jobid squeue failure must mark the task as vanished, got {:?}",
            after.lifecycle
        );
        assert_eq!(canned.sacct_calls(), 0, "refresh must NOT call sacct");
    }

    fn transient_squeue_failure() -> CaptureOutput {
        CaptureOutput {
            exit_code: 1,
            stdout: String::new(),
            stderr: "slurm_load_jobs error: Socket timed out on send/recv operation\n".into(),
        }
    }

    /// A transient squeue failure (controller overload prints `Socket
    /// timed out` on stderr, exits 1) must NOT be misread as a vanish:
    /// `refresh()` propagates the error and the persisted snapshot keeps
    /// `left_active_listing == false`. This was the root cause of the
    /// false-vanish regressions — previously the failure looked like an
    /// empty listing.
    #[tokio::test]
    async fn refresh_propagates_transient_squeue_failure() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;

        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        // qgroup: empty success (a miss) → falls through to squeue, which
        // fails transiently.
        let canned = std::sync::Arc::new(CannedDispatcher::new("", "", ""));
        *canned.squeue.lock().unwrap() = transient_squeue_failure();
        let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);

        let err = h.refresh().await.expect_err("transient failure must error");
        let msg = format!("{err:#}");
        assert!(msg.contains("squeue"), "tool name expected, got: {msg}");
        assert!(
            !h.snapshot().lifecycle.left_active_listing,
            "transient failure must not persist a fake vanish"
        );
    }

    /// Array-task variant of the transient-failure propagation.
    #[tokio::test]
    async fn refresh_array_task_propagates_transient_squeue_failure() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;

        let s = snap_array_task(12345, 3);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let canned = std::sync::Arc::new(CannedDispatcher::new("", "", ""));
        *canned.squeue.lock().unwrap() = transient_squeue_failure();
        let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);

        let err = h.refresh().await.expect_err("transient failure must error");
        let msg = format!("{err:#}");
        assert!(msg.contains("squeue"), "tool name expected, got: {msg}");
        assert!(
            !h.snapshot().lifecycle.left_active_listing,
            "transient failure must not persist a fake vanish"
        );
        assert_eq!(canned.sacct_calls(), 0, "refresh must NOT call sacct");
    }

    /// Regression (sacct-lag freeze): when the job has vanished from the
    /// active listings but sacct has no row *yet* (accounting flush lag),
    /// `refresh_with_sacct` must leave `finished` unset so a later call
    /// can retry — previously it stamped
    /// `FinishedInfo { final_state: Unknown, exit_code: None }` and the
    /// idempotency short-circuit froze that fabricated outcome forever.
    #[tokio::test]
    async fn refresh_with_sacct_leaves_finished_unset_until_sacct_reports() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;

        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        // qgroup & squeue both miss (vanish) AND sacct is still empty.
        let canned = std::sync::Arc::new(CannedDispatcher::new("", "", ""));
        let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);

        let first = h.refresh_with_sacct().await.unwrap();
        assert!(first.lifecycle.left_active_listing);
        assert!(
            first.lifecycle.finished.is_none(),
            "sacct miss must not freeze a fabricated Unknown outcome"
        );

        // Accounting catches up — the next call must resolve for real.
        *canned.sacct.lock().unwrap() = cap_ok("12345|COMPLETED|None|0:0\n");
        let second = h.refresh_with_sacct().await.unwrap();
        let finished = second
            .lifecycle
            .finished
            .expect("finished must resolve once sacct reports the row");
        assert_eq!(finished.final_state, crate::JobState::Completed);
        assert_eq!(finished.exit_code, Some(0));
        assert_eq!(
            canned.sacct_calls(),
            2,
            "each unresolved call retries sacct"
        );
    }

    /// Array-task variant of the sacct-lag retry: empty sacct leaves
    /// `finished` unset; a later call resolves from the per-task row.
    #[tokio::test]
    async fn refresh_with_sacct_array_task_retries_after_sacct_lag() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;

        let s = snap_array_task(12345, 3);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let canned = std::sync::Arc::new(CannedDispatcher::new("", "", ""));
        let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);

        let first = h.refresh_with_sacct().await.unwrap();
        assert!(first.lifecycle.finished.is_none());

        *canned.sacct.lock().unwrap() = cap_ok("12345_3|COMPLETED|None|0:0\n");
        let second = h.refresh_with_sacct().await.unwrap();
        let finished = second.lifecycle.finished.expect("resolved after lag");
        assert_eq!(finished.final_state, crate::JobState::Completed);
        assert_eq!(finished.exit_code, Some(0));
    }

    // -------- read_last_lines (tail read) --------

    /// Equivalence with the naive whole-file implementation for files
    /// smaller than one chunk.
    #[tokio::test]
    async fn read_last_lines_small_file_matches_naive_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.out");
        tokio::fs::write(&path, "a\nb\nc\nd\n").await.unwrap();
        assert_eq!(read_last_lines(&path, 2).await.unwrap(), vec!["c", "d"]);
        assert_eq!(
            read_last_lines(&path, 10).await.unwrap(),
            vec!["a", "b", "c", "d"],
            "n larger than the file returns every line"
        );
        assert_eq!(
            read_last_lines(&path, 0).await.unwrap(),
            Vec::<String>::new()
        );
    }

    /// A file spanning many read chunks must yield exactly the last `n`
    /// lines without loading the whole file (correctness side; the memory
    /// bound is structural — the loop stops once `n` newlines are seen).
    #[tokio::test]
    async fn read_last_lines_large_file_returns_exact_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("big.out");
        // ~100 bytes × 2000 lines ≈ 200 KiB — dozens of 8 KiB chunks.
        let mut content = String::new();
        for i in 0..2000 {
            content.push_str(&format!("line-{i:05} {}\n", "x".repeat(90)));
        }
        tokio::fs::write(&path, &content).await.unwrap();
        let tail = read_last_lines(&path, 3).await.unwrap();
        assert_eq!(tail.len(), 3);
        assert!(tail[0].starts_with("line-01997"));
        assert!(tail[2].starts_with("line-01999"));
    }

    /// `lines()` semantics: a final line without a trailing newline still
    /// counts as a line (matches the previous whole-file implementation).
    #[tokio::test]
    async fn read_last_lines_includes_unterminated_final_line() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.out");
        tokio::fs::write(&path, "first\nsecond\nno-newline-tail")
            .await
            .unwrap();
        assert_eq!(
            read_last_lines(&path, 2).await.unwrap(),
            vec!["second", "no-newline-tail"]
        );
    }

    /// Multibyte (UTF-8) content must survive chunk-boundary splits —
    /// the buffer is only decoded after assembly and the potentially
    /// broken leading line is always outside the returned tail.
    #[tokio::test]
    async fn read_last_lines_multibyte_content_across_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("jp.out");
        let mut content = String::new();
        for i in 0..2000 {
            content.push_str(&format!("行{i:05}-日本語テキスト{}\n", "あ".repeat(30)));
        }
        tokio::fs::write(&path, &content).await.unwrap();
        let tail = read_last_lines(&path, 2).await.unwrap();
        assert_eq!(tail.len(), 2);
        assert!(tail[0].starts_with("行01998"));
        assert!(tail[1].starts_with("行01999"));
        assert!(
            !tail[0].contains('\u{FFFD}'),
            "no replacement chars: {tail:?}"
        );
    }

    #[tokio::test]
    async fn read_last_lines_empty_file_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.out");
        tokio::fs::write(&path, "").await.unwrap();
        assert_eq!(
            read_last_lines(&path, 5).await.unwrap(),
            Vec::<String>::new()
        );
    }

    /// Regression (non-terminal sacct row freeze): a transient squeue
    /// failure (controller overload prints `Socket timed out` on stderr,
    /// exits 1, stdout empty) is indistinguishable from a vanish, so
    /// `left_active_listing` flips to `true` while the job is still
    /// running. A subsequent `refresh_with_sacct` then gets a live
    /// `RUNNING|…|0:0` row — which slips past the no-row sentinel
    /// (`Unknown && exit_code.is_none()`) and froze
    /// `FinishedInfo { final_state: Running, exit_code: Some(0) }`
    /// behind the idempotency short-circuit. The fix: only a terminal
    /// state may be stamped; a live row instead rolls back the vanish
    /// flag so normal polling resumes.
    #[tokio::test]
    async fn refresh_with_sacct_rolls_back_false_vanish_on_live_sacct_row() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;

        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        // qgroup & squeue empty (transient miss) but sacct proves the job
        // is alive.
        let canned = std::sync::Arc::new(CannedDispatcher::new(
            "",
            "",
            "12345|RUNNING|None|0:0\n12345.batch|RUNNING|None|0:0\n",
        ));
        let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);

        let first = h.refresh_with_sacct().await.unwrap();
        assert!(
            first.lifecycle.finished.is_none(),
            "a non-terminal sacct row must never be stamped as FinishedInfo, got {:?}",
            first.lifecycle.finished
        );
        assert!(
            !first.lifecycle.left_active_listing,
            "a live sacct row must roll back the false vanish flag"
        );
        assert_eq!(
            first.lifecycle.last_observed_state.as_ref().unwrap().state,
            crate::JobState::Running,
            "the live observation must be recorded"
        );
        assert!(h.is_running(), "handle must report running again");

        // The job later finishes for real: squeue stays empty, sacct now
        // reports the terminal row — the normal resolve path must work.
        *canned.sacct.lock().unwrap() = cap_ok("12345|COMPLETED|None|0:0\n");
        let second = h.refresh_with_sacct().await.unwrap();
        let finished = second
            .lifecycle
            .finished
            .expect("terminal sacct row must resolve normally");
        assert_eq!(finished.final_state, crate::JobState::Completed);
        assert_eq!(finished.exit_code, Some(0));
    }

    /// Array-task variant of the non-terminal-row rollback.
    #[tokio::test]
    async fn refresh_with_sacct_array_task_rolls_back_false_vanish_on_live_row() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;

        let s = snap_array_task(12345, 3);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let canned = std::sync::Arc::new(CannedDispatcher::new(
            "",
            "",
            "12345_3|RUNNING|None|0:0\n12345_3.batch|RUNNING|None|0:0\n",
        ));
        let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);

        let first = h.refresh_with_sacct().await.unwrap();
        assert!(first.lifecycle.finished.is_none());
        assert!(!first.lifecycle.left_active_listing);

        *canned.sacct.lock().unwrap() = cap_ok("12345_3|FAILED|NonZeroExitCode|3:0\n");
        let second = h.refresh_with_sacct().await.unwrap();
        let finished = second.lifecycle.finished.expect("terminal row resolves");
        assert_eq!(finished.final_state, crate::JobState::Failed);
        assert_eq!(finished.exit_code, Some(3));
    }

    /// Forward-compat lock-in: an *unrecognized* state token with a real
    /// exit code (a future SLURM terminal state we don't know yet) must
    /// still be stamped — the terminal guard only blocks *known*
    /// non-terminal states. The no-row case stays covered by the
    /// `Unknown && exit_code.is_none()` sentinel.
    #[tokio::test]
    async fn refresh_with_sacct_stamps_unknown_token_with_exit_code() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;

        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let canned = std::sync::Arc::new(CannedDispatcher::new(
            "",
            "",
            "12345|SOME_FUTURE_STATE|None|0:0\n",
        ));
        let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);

        let after = h.refresh_with_sacct().await.unwrap();
        let finished = after
            .lifecycle
            .finished
            .expect("unknown token + exit code must still resolve (forward-compat)");
        assert_eq!(finished.final_state, crate::JobState::Unknown);
        assert_eq!(finished.exit_code, Some(0));
    }

    /// `qgroup` is a KUDPC-ism: on clusters where the binary does not
    /// exist, spawning it fails with an `Err` (not a nonzero exit).
    /// `refresh()` must treat that as a qgroup miss and fall back to
    /// squeue instead of propagating the error to the caller.
    #[tokio::test]
    async fn refresh_falls_back_to_squeue_when_qgroup_spawn_fails() {
        use crate::dispatcher::{JobDispatcher, into_dyn};
        use crate::store::InMemoryStateStore;

        struct QgroupErrDispatcher;
        impl JobDispatcher for QgroupErrDispatcher {
            async fn run(&self, _argv: &[String]) -> anyhow::Result<i32> {
                unimplemented!()
            }
            async fn capture(&self, argv: &[String]) -> anyhow::Result<CaptureOutput> {
                match argv[0].as_str() {
                    "qgroup" => Err(anyhow::anyhow!("failed to spawn `qgroup`")),
                    "squeue" => Ok(cap_ok("12345 RUNNING None\n")),
                    _ => Ok(CaptureOutput::default()),
                }
            }
        }

        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(QgroupErrDispatcher);
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h
            .refresh()
            .await
            .expect("qgroup spawn failure must not propagate");
        assert_eq!(
            after.lifecycle.last_observed_state.unwrap().state,
            crate::JobState::Running
        );
        assert!(!after.lifecycle.left_active_listing);
    }

    /// Per-task tasks of one master submission must observe distinct
    /// states even when the master would only have a single qgroup
    /// summary row. This is the regression test for issue #8 A5: before
    /// the fix, two handles cloned from the same master snapshot would
    /// both see the same observed state on refresh.
    #[tokio::test]
    async fn refresh_array_tasks_observe_per_task_states() {
        use crate::dispatcher::{JobDispatcher, into_dyn};
        use crate::store::InMemoryStateStore;

        // Dispatcher: route squeue calls based on the `-j <key>` argv to
        // produce per-task responses. `argv[3]` carries the `<master>_<idx>`
        // key (after "squeue", "-h", "-j").
        struct PerTaskSqueue;
        impl JobDispatcher for PerTaskSqueue {
            async fn run(&self, _argv: &[String]) -> anyhow::Result<i32> {
                unimplemented!()
            }
            async fn capture(&self, argv: &[String]) -> anyhow::Result<CaptureOutput> {
                if argv[0] == "squeue" {
                    let key = argv[3].as_str();
                    return Ok(cap_ok(match key {
                        "12345_0" => "RUNNING None\n",
                        "12345_1" => "PENDING Priority\n",
                        _ => "",
                    }));
                }
                Ok(CaptureOutput::default())
            }
        }

        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let h0 = SbatchJobHandle::new(
            snap_array_task(12345, 0),
            store.clone(),
            into_dyn(PerTaskSqueue),
        );
        let h1 = SbatchJobHandle::new(
            snap_array_task(12345, 1),
            store.clone(),
            into_dyn(PerTaskSqueue),
        );

        let s0 = h0.refresh().await.unwrap();
        let s1 = h1.refresh().await.unwrap();
        assert_eq!(
            s0.lifecycle.last_observed_state.unwrap().state,
            crate::JobState::Running
        );
        let s1_state = s1.lifecycle.last_observed_state.unwrap();
        assert_eq!(s1_state.state, crate::JobState::Pending);
        assert_eq!(s1_state.reason, crate::JobReason::Priority);
    }

    #[tokio::test]
    async fn wait_terminal_returns_when_state_is_terminal() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;
        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(CannedDispatcher::new(
            "QUEUE USER JOBID STATUS PROC\ngr u 12345 CMP 1\n",
            "",
            "",
        ));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h
            .wait_terminal(std::time::Duration::from_millis(10))
            .await
            .unwrap();
        assert_eq!(
            after.lifecycle.last_observed_state.unwrap().state,
            crate::JobState::Completed
        );
    }

    #[tokio::test]
    async fn wait_terminal_returns_on_left_active_listing() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;
        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let canned = std::sync::Arc::new(CannedDispatcher::new("", "", ""));
        let dispatcher = into_dyn(MoveDispatcher(canned.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h
            .wait_terminal(std::time::Duration::from_millis(10))
            .await
            .unwrap();
        assert!(after.lifecycle.left_active_listing);
        assert_eq!(
            canned.sacct_calls(),
            0,
            "wait_terminal must NEVER call sacct"
        );
    }

    /// Dispatcher whose squeue responses follow a script: pops the next
    /// canned [`CaptureOutput`] per squeue call (the last entry repeats
    /// once the script is exhausted). Counts squeue calls so tests can
    /// assert exactly how many refresh attempts happened.
    struct ScriptedSqueue {
        script: std::sync::Mutex<std::collections::VecDeque<CaptureOutput>>,
        last: CaptureOutput,
        squeue_calls: std::sync::Mutex<u32>,
    }

    impl ScriptedSqueue {
        fn new(script: Vec<CaptureOutput>) -> Self {
            let last = script.last().cloned().unwrap_or_default();
            Self {
                script: std::sync::Mutex::new(script.into_iter().collect()),
                last,
                squeue_calls: std::sync::Mutex::new(0),
            }
        }

        fn squeue_calls(&self) -> u32 {
            *self.squeue_calls.lock().unwrap()
        }
    }

    impl crate::dispatcher::JobDispatcher for ScriptedSqueue {
        async fn run(&self, _argv: &[String]) -> anyhow::Result<i32> {
            unimplemented!()
        }
        async fn capture(&self, argv: &[String]) -> anyhow::Result<CaptureOutput> {
            if argv[0] == "squeue" {
                *self.squeue_calls.lock().unwrap() += 1;
                let next = self.script.lock().unwrap().pop_front();
                return Ok(next.unwrap_or_else(|| self.last.clone()));
            }
            Ok(CaptureOutput::default())
        }
    }

    fn vanished_squeue_failure() -> CaptureOutput {
        CaptureOutput {
            exit_code: 1,
            stdout: String::new(),
            stderr: "slurm_load_jobs error: Invalid job id specified\n".into(),
        }
    }

    /// Phase C: a couple of transient `Socket timed out` hiccups during a
    /// long `wait_terminal` poll must not abort the wait — the loop keeps
    /// polling and succeeds once squeue reports the terminal vanish.
    #[tokio::test]
    async fn wait_terminal_tolerates_transient_refresh_failures() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;

        let s = snap_array_task(12345, 3);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let scripted = std::sync::Arc::new(ScriptedSqueue::new(vec![
            transient_squeue_failure(),
            transient_squeue_failure(),
            vanished_squeue_failure(),
        ]));
        let dispatcher = into_dyn(MoveDispatcher(scripted.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);

        let after = h
            .wait_terminal(std::time::Duration::from_millis(1))
            .await
            .expect("two transient failures then a vanish must succeed");
        assert!(after.lifecycle.left_active_listing);
        assert_eq!(scripted.squeue_calls(), 3);
    }

    /// Phase C: persistent refresh failure must still propagate — after
    /// exactly `MAX_CONSECUTIVE_REFRESH_FAILURES` (5) consecutive failed
    /// refresh attempts, `wait_terminal` returns the error instead of
    /// polling forever against a dead controller.
    #[tokio::test]
    async fn wait_terminal_propagates_persistent_refresh_failure() {
        use crate::dispatcher::into_dyn;
        use crate::store::InMemoryStateStore;

        let s = snap_array_task(12345, 3);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let scripted = std::sync::Arc::new(ScriptedSqueue::new(vec![transient_squeue_failure()]));
        let dispatcher = into_dyn(MoveDispatcher(scripted.clone()));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);

        let err = h
            .wait_terminal(std::time::Duration::from_millis(1))
            .await
            .expect_err("persistent failure must propagate");
        let msg = format!("{err:#}");
        assert!(msg.contains("squeue"), "tool name expected, got: {msg}");
        assert_eq!(
            scripted.squeue_calls(),
            5,
            "exactly MAX_CONSECUTIVE_REFRESH_FAILURES attempts before giving up"
        );
    }

    // ---- log_lines / read_log_to_end ----

    use std::io::Write as _;

    fn snap_with_log_path(jobid: u64, stdout_path: &str, stderr_path: &str) -> SbatchJobSnapshot {
        let mut s = snap(jobid);
        s.log = LogPathSpec {
            output_template: Some(stdout_path.to_string()),
            error_template: Some(stderr_path.to_string()),
        };
        s
    }

    #[tokio::test]
    async fn log_lines_returns_empty_when_file_missing() {
        use crate::dispatcher::{DryRunDispatcher, into_dyn};
        use crate::store::InMemoryStateStore;
        let s = snap_with_log_path(
            12345,
            "/nonexistent/stdout-%j.out",
            "/nonexistent/stderr-%j.err",
        );
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(DryRunDispatcher);
        let h = SbatchJobHandle::new(s, store, dispatcher);
        let lines = h.log_lines(LogStream::Stdout, 5).await.unwrap();
        assert_eq!(lines, Vec::<String>::new());
    }

    #[tokio::test]
    async fn log_lines_returns_path_not_resolved_when_no_template() {
        use crate::dispatcher::{DryRunDispatcher, into_dyn};
        use crate::store::InMemoryStateStore;
        let mut s = snap(12345);
        s.log = LogPathSpec::default();
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(DryRunDispatcher);
        let h = SbatchJobHandle::new(s, store, dispatcher);
        let err = h.log_lines(LogStream::Stdout, 5).await.unwrap_err();
        assert!(matches!(err, LogReadError::PathNotResolved));
    }

    #[tokio::test]
    async fn log_lines_returns_last_n_lines() {
        use crate::dispatcher::{DryRunDispatcher, into_dyn};
        use crate::store::InMemoryStateStore;

        let dir = tempfile::tempdir().unwrap();
        let stdout_path = dir.path().join("stdout-12345.out");
        let mut f = std::fs::File::create(&stdout_path).unwrap();
        for i in 0..20 {
            writeln!(f, "line {i}").unwrap();
        }
        drop(f);

        let s = snap_with_log_path(12345, stdout_path.to_str().unwrap(), "ignored.err");
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(DryRunDispatcher);
        let h = SbatchJobHandle::new(s, store, dispatcher);

        let lines = h.log_lines(LogStream::Stdout, 5).await.unwrap();
        assert_eq!(
            lines,
            vec!["line 15", "line 16", "line 17", "line 18", "line 19"]
        );
    }

    #[tokio::test]
    async fn read_log_to_end_returns_full_content() {
        use crate::dispatcher::{DryRunDispatcher, into_dyn};
        use crate::store::InMemoryStateStore;

        let dir = tempfile::tempdir().unwrap();
        let stdout_path = dir.path().join("stdout-12345.out");
        std::fs::write(&stdout_path, "hello\nworld\n").unwrap();

        let s = snap_with_log_path(12345, stdout_path.to_str().unwrap(), "ignored.err");
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(DryRunDispatcher);
        let h = SbatchJobHandle::new(s, store, dispatcher);

        let content = h.read_log_to_end(LogStream::Stdout).await.unwrap();
        assert_eq!(content, "hello\nworld\n");
    }

    /// Drop on the inner Arc-shared state must not panic, regardless of
    /// whether the last-observed state says the job is still running.
    /// This is a smoke test for spec §6.5 — the Drop emits a
    /// `tracing::warn!` when running, and is a no-op otherwise. We
    /// cannot easily assert on the warn without a captive subscriber,
    /// but we can guarantee the Drop path is panic-free.
    #[tokio::test]
    async fn drop_does_not_panic_for_running_or_idle_handle() {
        use crate::dispatcher::{DryRunDispatcher, into_dyn};
        use crate::store::InMemoryStateStore;

        // Idle: no observed state → Drop is a silent no-op.
        {
            let s = snap(1);
            let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> =
                Arc::new(InMemoryStateStore::new());
            let dispatcher = into_dyn(DryRunDispatcher);
            let h = SbatchJobHandle::new(s, store, dispatcher);
            drop(h);
        }

        // Running: last_observed_state = Running → Drop emits warn.
        {
            let mut s = snap(2);
            s.lifecycle.last_observed_state = Some(JobStatus {
                state: JobState::Running,
                reason: JobReason::None,
            });
            let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> =
                Arc::new(InMemoryStateStore::new());
            let dispatcher = into_dyn(DryRunDispatcher);
            let h = SbatchJobHandle::new(s, store, dispatcher);
            drop(h);
        }
    }

    /// Cloning a handle and dropping each clone must not double-warn:
    /// the Drop impl lives on `SbatchJobHandleInner` (Arc-shared), so
    /// it fires exactly once when the last clone goes away. This test
    /// confirms the Arc semantics by simply not panicking under a
    /// 2-clone drop pattern with a running snapshot.
    #[tokio::test]
    async fn drop_runs_once_per_arc_not_per_clone() {
        use crate::dispatcher::{DryRunDispatcher, into_dyn};
        use crate::store::InMemoryStateStore;

        let mut s = snap(3);
        s.lifecycle.last_observed_state = Some(JobStatus {
            state: JobState::Running,
            reason: JobReason::None,
        });
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> = Arc::new(InMemoryStateStore::new());
        let dispatcher = into_dyn(DryRunDispatcher);
        let h1 = SbatchJobHandle::new(s, store, dispatcher);
        let h2 = h1.clone();
        drop(h1);
        drop(h2);
    }
}
