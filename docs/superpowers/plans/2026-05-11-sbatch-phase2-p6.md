# Phase 2 P6: `SbatchManager::run()` + `cancel()` + Typed Errors

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a sync-style `run()` API to `SbatchManager` that submits a single sbatch job and blocks until terminal state, plus an explicit `cancel(jobid)` API. Both surface typed errors so callers can branch on cause.

**Architecture:** `run()` is a thin composition over the existing `spawn() → wait_terminal() → refresh_with_sacct()` chain. It rejects array submissions up front (`ArrayNotSupported`) so callers receive a clear contract message, and inspects `FinishedInfo.final_state` to distinguish success (`Completed`) from failure (`JobFailed`). `cancel()` shells out to `scancel <jobid>` via the existing `JobDispatcher::capture` seam. Polling cadence is hardcoded to 30 s by default but exposed through `SbatchManager::with_poll_interval()` for tests and tight loops. `--wait` flag is intentionally NOT used — see spec §6.0 for the four-bullet justification (orphan-on-disconnect risk, snapshot-permanence invariant, KUDPC load guideline, simpler timeout).

**Tech Stack:** Rust + `thiserror` + `tokio` + `anyhow` + existing `JobDispatcher` trait + pyo3 (`future_into_py`).

**Spec reference:** `docs/superpowers/specs/2026-05-10-sbatch-phase2-design.md` §6 (run / cancel / SbatchRunError), §6.0 (`--wait` deviation), §6.4 (array guard).

**Deviation from spec literal** — record now so reviewers don't flag:
- spec §6.1 sketches `run(&self, cmd: SbatchCmd)` but every other method on `SbatchManager` (`spawn`, `attach_*`, `spawn_array`) takes the command from `self.cmd`. Following that established pattern, we implement `run(&self) -> ...` with no `cmd` parameter. Callers compose by building a `SbatchManager::new(cmd)` and calling `mgr.run().await`.
- spec §6.2 declares `Wait(std::io::Error)`. Reality: `SbatchJobHandle::wait_terminal` returns `anyhow::Result`, not `io::Result`. We use `Wait(anyhow::Error)` so no information is lost and we don't have to manually unwrap.
- `cancel()` does not pre-check the local snapshot's `is_finished()` state. `scancel <jobid>` is already idempotent on the SLURM side (terminal jobs return exit=0 with a notice on stderr) — pre-checking would race against external state changes and add latency for no benefit.

---

## File Structure

| File | Role |
|---|---|
| `src/sbatch/error.rs` | Add `SbatchRunError`, `SbatchCancelError` (both `#[non_exhaustive]` + `thiserror`). |
| `src/sbatch/manager.rs` | Add `SbatchManager::run()`, `SbatchManager::cancel()`, `with_poll_interval()` builder, default 30 s poll. |
| `src/py_export/sbatch.rs` | Add `PySbatchManager::run` / `PySbatchManager::cancel` async methods, returning `FinishedInfo` as a pyclass (`PyFinishedInfo`). |
| `python/slurm_async_runner/_slurm_async_runner_core/sbatch.pyi` | Sync `run` / `cancel` signatures + `FinishedInfo` stub. |
| `python/tests/test_sbatch.py` | New tests: `run_rejects_array_spec_with_value_error`, `cancel_invokes_scancel_via_dispatcher` (latter skipped — see Task 8 note). |
| `CHANGELOG.md` | `[Unreleased]` → `### Phase 2 P6 …`. |

---

## Task 1: Add `SbatchRunError` + `SbatchCancelError` to `error.rs`

**Files:**
- Modify: `src/sbatch/error.rs`

- [ ] **Step 1: Write the failing tests**

Append to the existing `#[cfg(test)] mod tests` at the bottom of `src/sbatch/error.rs`:

```rust
    #[test]
    fn run_error_array_not_supported_displays_helpful_message() {
        let e = SbatchRunError::ArrayNotSupported;
        let msg = e.to_string();
        assert!(msg.contains("array"), "expected 'array' in message, got: {msg}");
        assert!(
            msg.contains("spawn_array"),
            "should point user at spawn_array, got: {msg}"
        );
    }

    #[test]
    fn run_error_job_failed_carries_state_and_exit_code() {
        let e = SbatchRunError::JobFailed {
            state: crate::JobState::Failed,
            exit_code: Some(2),
        };
        let msg = e.to_string();
        assert!(msg.contains("Failed"), "state should appear, got: {msg}");
        assert!(msg.contains('2'), "exit code should appear, got: {msg}");
    }

    #[test]
    fn run_error_from_spawn_error_preserves_variant() {
        let inner = SbatchSpawnError::SubmitFailed {
            exit_code: 1,
            stdout: "boom".into(),
        };
        let e: SbatchRunError = inner.into();
        assert!(matches!(e, SbatchRunError::Spawn(_)));
    }

    #[test]
    fn cancel_error_scancel_carries_exit_and_stdout() {
        let e = SbatchCancelError::Scancel {
            exit_code: 1,
            stdout: "scancel: error: invalid job id specified".into(),
        };
        let msg = e.to_string();
        assert!(msg.contains("scancel"));
        assert!(msg.contains("invalid job id specified"));
    }
```

- [ ] **Step 2: Run tests, verify they fail to compile**

Run: `cargo test --lib --features pyo3 sbatch::error::`
Expected: FAIL with "cannot find type `SbatchRunError`" / "cannot find type `SbatchCancelError`".

- [ ] **Step 3: Add the two enums to `src/sbatch/error.rs`**

Insert after `SbatchAttachError` (i.e. before the `#[cfg(test)]` block):

```rust
/// Errors that can occur during a blocking submit-and-wait `run()` call.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SbatchRunError {
    #[error("spawn failed: {0}")]
    Spawn(#[from] SbatchSpawnError),

    #[error("wait_terminal io error: {0}")]
    Wait(anyhow::Error),

    #[error("sacct refresh failed: {0}")]
    Sacct(anyhow::Error),

    #[error("sacct returned but finished info was not populated for jobid={jobid}")]
    MissingFinished { jobid: u64 },

    #[error("job ended in non-success terminal state: {state:?}, exit_code={exit_code:?}")]
    JobFailed {
        state: crate::JobState,
        exit_code: Option<i32>,
    },

    #[error(
        "array submission is not supported by run(); use spawn_array() instead \
         and await tasks individually"
    )]
    ArrayNotSupported,
}

/// Errors that can occur while sending `scancel <jobid>`.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SbatchCancelError {
    #[error("scancel failed (exit={exit_code}): {stdout}")]
    Scancel { exit_code: i32, stdout: String },

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
```

- [ ] **Step 4: Run tests, verify they pass**

Run: `cargo test --lib --features pyo3 sbatch::error::`
Expected: PASS (4 new tests + existing tests still green).

- [ ] **Step 5: Run clippy + fmt**

Run: `cargo clippy --all-targets --features pyo3 -- -D warnings && cargo fmt --all --check`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add src/sbatch/error.rs
git commit -m "feat(sbatch): add SbatchRunError + SbatchCancelError typed enums"
```

---

## Task 2: Implement `SbatchManager::cancel()` + poll-interval builder

**Files:**
- Modify: `src/sbatch/manager.rs`

- [ ] **Step 1: Write the failing tests**

Append to `#[cfg(test)] mod tests` in `src/sbatch/manager.rs`:

```rust
    use crate::sbatch::error::SbatchCancelError;

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
        async fn capture(&self, argv: &[String]) -> Result<(i32, String)> {
            self.seen.lock().unwrap().push(argv.to_vec());
            let resp = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or((0, String::new()));
            Ok(resp)
        }
    }

    /// Reuse pattern from handle.rs: Arc-wrapped dispatcher so the test
    /// keeps a handle to inspect `seen()` after the manager calls capture.
    struct MoveRecording(std::sync::Arc<RecordingDispatcher>);
    impl JobDispatcher for MoveRecording {
        async fn run(&self, argv: &[String]) -> Result<i32> {
            self.0.run(argv).await
        }
        async fn capture(&self, argv: &[String]) -> Result<(i32, String)> {
            self.0.capture(argv).await
        }
    }

    #[tokio::test]
    async fn cancel_invokes_scancel_with_jobid() {
        let cmd = SbatchCmd::new("/w/job.sh");
        let recorder = std::sync::Arc::new(RecordingDispatcher::new(vec![(0, String::new())]));
        let dispatcher = into_dyn(MoveRecording(recorder.clone()));
        let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);

        mgr.cancel(12345).await.expect("cancel should succeed");

        let seen = recorder.seen();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0][0], "scancel");
        assert_eq!(seen[0][1], "12345");
    }

    #[tokio::test]
    async fn cancel_returns_scancel_error_on_nonzero_exit() {
        let cmd = SbatchCmd::new("/w/job.sh");
        let recorder = std::sync::Arc::new(RecordingDispatcher::new(vec![(
            1,
            "scancel: error: Invalid job id".to_string(),
        )]));
        let dispatcher = into_dyn(MoveRecording(recorder));
        let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);

        match mgr.cancel(99).await {
            Err(SbatchCancelError::Scancel { exit_code, stdout }) => {
                assert_eq!(exit_code, 1);
                assert!(stdout.contains("Invalid job id"));
            }
            other => panic!("expected Scancel error, got {other:?}"),
        }
    }
```

- [ ] **Step 2: Run tests, verify they fail**

Run: `cargo test --lib --features pyo3 sbatch::manager::tests::cancel_`
Expected: FAIL with "no method named `cancel` found".

- [ ] **Step 3: Add the `poll_interval` field + builder + `cancel()`**

In `src/sbatch/manager.rs`, modify the struct definition:

```rust
#[derive(Clone)]
pub struct SbatchManager {
    cmd: SbatchCmd,
    store: Arc<dyn JobStateStore<SbatchJobSnapshot>>,
    dispatcher: Arc<dyn DynJobDispatcher>,
    poll_interval: std::time::Duration,
}
```

Update `new()` to initialize the new field:

```rust
    pub fn new(cmd: SbatchCmd) -> Self {
        Self {
            cmd,
            store: Arc::new(InMemoryStateStore::<SbatchJobSnapshot>::new()),
            dispatcher: into_dyn(TokioDispatcher),
            poll_interval: std::time::Duration::from_secs(30),
        }
    }
```

Add the builder method adjacent to `with_dispatcher`:

```rust
    /// Override the polling cadence used by `run()` between `wait_terminal`
    /// iterations. Default is 30 s, chosen to keep KUDPC squeue load low.
    /// Tests typically use 1–10 ms.
    pub fn with_poll_interval(mut self, dur: std::time::Duration) -> Self {
        self.poll_interval = dur;
        self
    }
```

Append `cancel()` after `attach_array_jobid`:

```rust
    /// Send `scancel <jobid>`. Idempotent at the SLURM side — sending
    /// scancel to a terminal job returns exit 0. Returns
    /// `SbatchCancelError::Scancel` if scancel itself reports a non-zero exit.
    pub async fn cancel(&self, jobid: u64) -> Result<(), SbatchCancelError> {
        let argv = vec!["scancel".to_string(), jobid.to_string()];
        let (exit_code, stdout) = self
            .dispatcher
            .capture(&argv)
            .await
            .map_err(SbatchCancelError::Other)?;
        if exit_code != 0 {
            return Err(SbatchCancelError::Scancel { exit_code, stdout });
        }
        Ok(())
    }
```

- [ ] **Step 4: Run tests, verify they pass**

Run: `cargo test --lib --features pyo3 sbatch::manager::tests::cancel_`
Expected: PASS (2 new tests).

Run the full manager test module: `cargo test --lib --features pyo3 sbatch::manager`
Expected: PASS (no regressions on existing tests).

- [ ] **Step 5: Run clippy + fmt**

Run: `cargo clippy --all-targets --features pyo3 -- -D warnings && cargo fmt --all --check`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add src/sbatch/manager.rs
git commit -m "feat(sbatch): add SbatchManager::cancel + with_poll_interval builder"
```

---

## Task 3: Implement `SbatchManager::run()` — ArrayNotSupported guard

**Files:**
- Modify: `src/sbatch/manager.rs`

- [ ] **Step 1: Write the failing test**

Append to `#[cfg(test)] mod tests` in `src/sbatch/manager.rs`:

```rust
    use crate::sbatch::error::SbatchRunError;

    #[tokio::test]
    async fn run_rejects_array_spec_before_spawn() {
        use crate::entities::slurm::SlurmArraySpec;

        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.array_spec = Some("0-2".parse::<SlurmArraySpec>().unwrap());

        // Recorder must remain unused — guard fires before spawn touches sbatch.
        let recorder = std::sync::Arc::new(RecordingDispatcher::new(vec![]));
        let dispatcher = into_dyn(MoveRecording(recorder.clone()));
        let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);

        match mgr.run().await {
            Err(SbatchRunError::ArrayNotSupported) => {}
            other => panic!("expected ArrayNotSupported, got {other:?}"),
        }
        assert!(recorder.seen().is_empty(), "spawn must not be called");
    }
```

- [ ] **Step 2: Run test, verify it fails to compile**

Run: `cargo test --lib --features pyo3 sbatch::manager::tests::run_rejects_array_spec_before_spawn`
Expected: FAIL with "no method named `run` found".

- [ ] **Step 3: Add the stub `run()`**

Append to the `impl SbatchManager` block (after `attach_array_jobid`, before `cancel`):

```rust
    /// Submit a single sbatch job, block until terminal state, return
    /// `FinishedInfo`. See spec §6 for the full design rationale.
    ///
    /// **Not for array submissions.** Use `spawn_array` for those —
    /// run() returns `Err(SbatchRunError::ArrayNotSupported)` when
    /// `cmd.array_spec.is_some()`.
    ///
    /// Timeout: caller wraps with `tokio::time::timeout(dur, mgr.run())`.
    /// On timeout, the caller is responsible for calling `mgr.cancel(jobid)`
    /// if they want to stop the SLURM-side job; the timeout itself only
    /// drops the future.
    pub async fn run(
        &self,
    ) -> Result<crate::sbatch::handle::FinishedInfo, SbatchRunError> {
        if self.cmd.array_spec.is_some() {
            return Err(SbatchRunError::ArrayNotSupported);
        }
        // Spawn + wait + sacct chain is added in Task 4. Provide a temporary
        // panic so the guard test can pass while the full path is unimplemented.
        unimplemented!("run() body lands in Task 4")
    }
```

- [ ] **Step 4: Run test, verify it passes**

Run: `cargo test --lib --features pyo3 sbatch::manager::tests::run_rejects_array_spec_before_spawn`
Expected: PASS.

- [ ] **Step 5: Run clippy + fmt**

Run: `cargo clippy --all-targets --features pyo3 -- -D warnings && cargo fmt --all --check`
Expected: clean (the `unimplemented!` is reachable only from Task 4's new tests).

- [ ] **Step 6: Commit**

```bash
git add src/sbatch/manager.rs
git commit -m "feat(sbatch): add SbatchManager::run() with ArrayNotSupported guard"
```

---

## Task 4: Implement `SbatchManager::run()` — happy path (Completed → Ok)

**Files:**
- Modify: `src/sbatch/manager.rs`

- [ ] **Step 1: Write the failing test**

Append a multi-call canned dispatcher and the happy-path test to `#[cfg(test)] mod tests`:

```rust
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
        async fn capture(&self, argv: &[String]) -> Result<(i32, String)> {
            let out = match argv[0].as_str() {
                "sbatch" => {
                    let (e, s) = self.sbatch.lock().unwrap().clone();
                    return Ok((e, s));
                }
                "qgroup" => self.qgroup.lock().unwrap().clone(),
                "squeue" => self.squeue.lock().unwrap().clone(),
                "sacct" => self.sacct.lock().unwrap().clone(),
                _ => String::new(),
            };
            Ok((0, out))
        }
    }

    #[tokio::test]
    async fn run_happy_path_returns_finished_info_with_exit_code_zero() {
        let cmd = SbatchCmd::new("/w/job.sh");
        // sbatch returns jobid 4242, qgroup reports CMP (terminal), sacct
        // reports COMPLETED exit 0:0.
        let dispatcher = into_dyn(RunCannedDispatcher::new(
            "Submitted batch job 4242\n",
            "QUEUE USER JOBID STATUS PROC\ngr u 4242 CMP 1\n",
            "4242|COMPLETED|None|0:0\n",
        ));
        let mgr = SbatchManager::new(cmd)
            .with_dispatcher(dispatcher)
            .with_poll_interval(std::time::Duration::from_millis(1));

        let finished = mgr.run().await.expect("run should succeed");
        assert_eq!(finished.final_state, crate::JobState::Completed);
        assert_eq!(finished.exit_code, Some(0));
    }
```

- [ ] **Step 2: Run test, verify it fails**

Run: `cargo test --lib --features pyo3 sbatch::manager::tests::run_happy_path_returns_finished_info_with_exit_code_zero`
Expected: FAIL with `panicked at 'not implemented: run() body lands in Task 4'`.

- [ ] **Step 3: Replace the `unimplemented!` with the real chain**

In `src/sbatch/manager.rs`, replace the body of `run()`:

```rust
    pub async fn run(
        &self,
    ) -> Result<crate::sbatch::handle::FinishedInfo, SbatchRunError> {
        if self.cmd.array_spec.is_some() {
            return Err(SbatchRunError::ArrayNotSupported);
        }
        let handle = self.spawn().await?;
        let jobid = handle.snapshot().jobid;
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
```

- [ ] **Step 4: Run the happy-path test, verify it passes**

Run: `cargo test --lib --features pyo3 sbatch::manager::tests::run_happy_path`
Expected: PASS.

Re-run the array-guard test: `cargo test --lib --features pyo3 sbatch::manager::tests::run_rejects_array_spec_before_spawn`
Expected: PASS.

- [ ] **Step 5: Run clippy + fmt**

Run: `cargo clippy --all-targets --features pyo3 -- -D warnings && cargo fmt --all --check`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add src/sbatch/manager.rs
git commit -m "feat(sbatch): wire run() spawn -> wait_terminal -> refresh_with_sacct chain"
```

---

## Task 5: `run()` — JobFailed branch (non-Completed terminal state)

**Files:**
- Modify: `src/sbatch/manager.rs`

- [ ] **Step 1: Write the failing test**

Append to `#[cfg(test)] mod tests`:

```rust
    #[tokio::test]
    async fn run_returns_job_failed_when_sacct_reports_failed_state() {
        let cmd = SbatchCmd::new("/w/job.sh");
        // qgroup says CMP (terminal -> wait_terminal returns), sacct says FAILED exit 2.
        let dispatcher = into_dyn(RunCannedDispatcher::new(
            "Submitted batch job 9090\n",
            "QUEUE USER JOBID STATUS PROC\ngr u 9090 CMP 1\n",
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
    async fn run_returns_job_failed_when_cancelled_by_signal() {
        let cmd = SbatchCmd::new("/w/job.sh");
        // sacct: CANCELLED, signal 9 -> exit_code 137.
        let dispatcher = into_dyn(RunCannedDispatcher::new(
            "Submitted batch job 7777\n",
            "QUEUE USER JOBID STATUS PROC\ngr u 7777 CMP 1\n",
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
```

- [ ] **Step 2: Run tests, verify they pass**

The branch already exists from Task 4 (`crate::JobState::Completed => Ok ... | other => Err(JobFailed)`), so these tests should pass on first run.

Run: `cargo test --lib --features pyo3 sbatch::manager::tests::run_returns_job_failed`
Expected: PASS (both tests).

If they fail, debug the `final_state` mapping in `query_job_states_with_exit_code_with`.

- [ ] **Step 3: Run clippy + fmt**

Run: `cargo clippy --all-targets --features pyo3 -- -D warnings && cargo fmt --all --check`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add src/sbatch/manager.rs
git commit -m "test(sbatch): cover run() JobFailed branch for Failed + Cancelled states"
```

---

## Task 6: pyo3 binding — `PySbatchManager::run` / `PySbatchManager::cancel`

**Files:**
- Modify: `src/py_export/sbatch.rs`

- [ ] **Step 1: Read current `PySbatchManager` and `PySbatchJobHandle` shape**

Run: `grep -n "impl PySbatchManager\|fn spawn\|fn spawn_array\|fn attach_array_jobid" src/py_export/sbatch.rs`
Expected: confirms the layout of methods so you append after `attach_array_jobid`.

- [ ] **Step 2: Add imports for the new types**

In `src/py_export/sbatch.rs`, add after the existing `use crate::sbatch::...` lines:

```rust
use crate::sbatch::error::{SbatchCancelError, SbatchRunError};
use crate::sbatch::handle::FinishedInfo;
```

- [ ] **Step 3: Add a `PyFinishedInfo` pyclass**

Insert the pyclass before `PySbatchManager`:

```rust
#[pyclass(
    name = "FinishedInfo",
    module = "slurm_async_runner._slurm_async_runner_core.sbatch",
    frozen
)]
#[derive(Clone)]
pub struct PyFinishedInfo(pub FinishedInfo);

#[pymethods]
impl PyFinishedInfo {
    #[getter]
    fn final_state(&self) -> String {
        self.0.final_state.as_slurm_str().to_string()
    }

    #[getter]
    fn exit_code(&self) -> Option<i32> {
        self.0.exit_code
    }

    #[getter]
    fn finished_at(&self) -> String {
        self.0.finished_at.to_rfc3339()
    }

    fn __repr__(&self) -> String {
        format!(
            "FinishedInfo(state={}, exit_code={:?}, finished_at={})",
            self.0.final_state.as_slurm_str(),
            self.0.exit_code,
            self.0.finished_at.to_rfc3339()
        )
    }
}
```

Confirm `JobState::as_slurm_str` exists: `grep -n "as_slurm_str" src/entities/slurm/status.rs`. If it does not exist on `JobState`, use `format!("{:?}", self.0.final_state)` as a fallback (record this fallback as a follow-up in the CHANGELOG).

- [ ] **Step 4: Add `run` and `cancel` to `impl PySbatchManager`**

Append inside `#[pymethods] impl PySbatchManager` (after `attach_array_jobid`):

```rust
    fn run<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mgr = self.0.clone();
        future_into_py(py, async move {
            let finished = mgr.run().await.map_err(|e| match e {
                SbatchRunError::ArrayNotSupported => {
                    pyo3::exceptions::PyValueError::new_err(e.to_string())
                }
                other => PyRuntimeError::new_err(other.to_string()),
            })?;
            Ok(PyFinishedInfo(finished))
        })
    }

    fn cancel<'py>(&self, py: Python<'py>, jobid: u64) -> PyResult<Bound<'py, PyAny>> {
        let mgr = self.0.clone();
        future_into_py(py, async move {
            mgr.cancel(jobid).await.map_err(|e| match e {
                SbatchCancelError::Scancel { exit_code, stdout } => PyRuntimeError::new_err(
                    format!("scancel failed (exit={exit_code}): {stdout}"),
                ),
                SbatchCancelError::Other(err) => PyRuntimeError::new_err(err.to_string()),
            })?;
            Ok(())
        })
    }
```

- [ ] **Step 5: Wire `PyFinishedInfo` into the module export**

Find the existing pyclass-export wiring. In this repo, look for `add_class::<PySbatch` or `#[pymodule_export]` patterns:

Run: `grep -rn "PySbatchJobHandle\|PySbatchManager" src/py_export/ | grep -v "^src/py_export/sbatch.rs"`
to see how those classes get exposed (mod.rs / main lib.rs / a `pymodule_export!` macro).

Then add `PyFinishedInfo` to the same registration list using the same mechanism (don't invent a new path).

- [ ] **Step 6: Run cargo test**

Run: `cargo test --lib --features pyo3`
Expected: PASS (all existing tests + new tests from Tasks 1–5).

- [ ] **Step 7: Run clippy + fmt**

Run: `cargo clippy --all-targets --features pyo3 -- -D warnings && cargo fmt --all --check`
Expected: clean.

- [ ] **Step 8: Run Python build to verify pyo3 surface compiles**

Run: `uv run maturin develop --features pyo3 --quiet 2>&1 | tail -10`
(Check `pyproject.toml` for the project's chosen invocation if this differs.)
Expected: build succeeds.

- [ ] **Step 9: Commit**

```bash
git add src/py_export/sbatch.rs
git commit -m "feat(py): expose SbatchManager.run / cancel + FinishedInfo pyclass"
```

---

## Task 7: `.pyi` sync

**Files:**
- Modify: `python/slurm_async_runner/_slurm_async_runner_core/sbatch.pyi`

- [ ] **Step 1: Add `FinishedInfo` to `__all__`**

Edit the `__all__` list near the top of the file to read:

```python
__all__ = [
    "SbatchCmd",
    "SbatchManager",
    "SbatchJobHandle",
    "FinishedInfo",
]
```

- [ ] **Step 2: Add `FinishedInfo` class definition**

Insert before the `SbatchManager` class block:

```python
@final
class FinishedInfo:
    """Outcome of a finished sbatch job. Returned by ``SbatchManager.run``.

    ``final_state`` is the SLURM state string (e.g. ``"COMPLETED"``, ``"FAILED"``).
    ``exit_code`` is the conventional Unix exit code: ``None`` if not resolvable,
    ``128 + signum`` if killed by signal.
    ``finished_at`` is an RFC3339 timestamp string.
    """

    @property
    def final_state(self) -> builtins.str: ...
    @property
    def exit_code(self) -> builtins.int | None: ...
    @property
    def finished_at(self) -> builtins.str: ...
```

- [ ] **Step 3: Add `run` and `cancel` method signatures to `SbatchManager`**

In the `SbatchManager` class body, append after `attach_array_jobid`:

```python
    def run(self) -> Awaitable[FinishedInfo]:
        """Submit one job, block until terminal state, return ``FinishedInfo``.

        Raises ``ValueError`` if ``cmd.array_spec`` is set — use ``spawn_array``
        for array submissions. Raises ``RuntimeError`` for spawn / wait / sacct
        failures or non-success terminal states.
        """
        ...

    def cancel(self, jobid: builtins.int) -> Awaitable[None]:
        """Send ``scancel <jobid>``. Idempotent on the SLURM side.

        Raises ``RuntimeError`` if scancel itself reports a non-zero exit.
        """
        ...
```

- [ ] **Step 4: Validate the `.pyi` parses**

Run: `python -c "import ast; ast.parse(open('python/slurm_async_runner/_slurm_async_runner_core/sbatch.pyi').read())"`
Expected: no output (silent success).

- [ ] **Step 5: Commit**

```bash
git add python/slurm_async_runner/_slurm_async_runner_core/sbatch.pyi
git commit -m "docs(py): sync .pyi for SbatchManager.run / cancel / FinishedInfo"
```

---

## Task 8: Python smoke tests

**Files:**
- Modify: `python/tests/test_sbatch.py`

- [ ] **Step 1: Write the failing tests**

Append to `python/tests/test_sbatch.py`:

```python
@pytest.mark.skipif(not _have_bash(), reason="bash required")
def test_run_rejects_array_spec_with_value_error(tmp_path: Path):
    """run() should raise ValueError when cmd.array_spec is set."""
    from slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options import (
        SlurmArraySpec,
    )

    job = tmp_path / "j.sh"
    job.write_text("#!/usr/bin/env bash\n:\n")
    job.chmod(0o755)

    cmd = SbatchCmd(str(job), array_spec=SlurmArraySpec.parse("0-2"))
    mgr = SbatchManager(cmd)

    async def go():
        await mgr.run()

    with pytest.raises(ValueError, match="array"):
        asyncio.run(go())


def test_cancel_smoke_skipped_pending_scancel_bin_override():
    """Placeholder marker for the cancel-via-fake-scancel test.

    Until ``SbatchManager`` exposes a ``scancel_bin`` override mirroring
    ``sbatch_bin``, this Python test cannot inject a controllable scancel
    binary. The Rust unit tests in ``src/sbatch/manager.rs`` cover the
    success + non-zero-exit branches via the dispatcher seam.
    Tracked as Phase 3 follow-up.
    """
    pytest.skip("scancel_bin override not yet implemented; see Phase 3")
```

- [ ] **Step 2: Run tests, verify they pass**

Run: `uv run pytest python/tests/test_sbatch.py -v`
Expected: new array-rejection test PASSES; cancel smoke test is SKIPPED with the documented reason. Existing 39 tests stay green.

- [ ] **Step 3: Commit**

```bash
git add python/tests/test_sbatch.py
git commit -m "test(py): cover SbatchManager.run array-rejection from Python side"
```

---

## Task 9: CHANGELOG + final validation

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add P6 entry**

In the `[Unreleased]` block, insert a new section above the existing P5 section:

```markdown
### Phase 2 P6

- **`SbatchManager::run()`** — submit a single job and block until terminal state, then return `FinishedInfo`. Rejects array submissions early with `SbatchRunError::ArrayNotSupported` (mapped to Python `ValueError`). Polling cadence defaults to 30 s; override via `with_poll_interval` (tests use 1–10 ms).
- **`SbatchManager::cancel(jobid)`** — send `scancel <jobid>` via the existing `JobDispatcher::capture` seam. Idempotent on the SLURM side. Non-zero exit surfaces as `SbatchCancelError::Scancel { exit_code, stdout }`.
- **`SbatchRunError`** — typed errors for the run pipeline: `Spawn`, `Wait(anyhow::Error)`, `Sacct(anyhow::Error)`, `MissingFinished`, `JobFailed { state, exit_code }`, `ArrayNotSupported`. `#[non_exhaustive]`.
- **`SbatchCancelError`** — `Scancel { exit_code, stdout }` and `Other(anyhow::Error)`. `#[non_exhaustive]`.
- **Python**: `SbatchManager.run` / `SbatchManager.cancel` async methods; new `FinishedInfo` pyclass with `final_state` / `exit_code` / `finished_at` getters.

### Notes

- `run()` does not use `sbatch --wait`. The poll-based design avoids orphan-on-disconnect risk on KUDPC and preserves Phase 1's snapshot-permanence invariant. See spec §6.0 for the four-bullet justification.
- `cancel()` does not pre-check local `is_finished()` state. SLURM's own `scancel` is idempotent for terminal jobs; pre-checking would race against external state changes.
- Spec §6.5 (Drop auto-cancel + `tracing::warn!`) is explicitly NOT in P6. `SbatchJobHandle::Drop` is left unchanged; the warning is a Phase 3 add-on.
- The scancel-binary-swap Python smoke test is skipped pending a `scancel_bin` override on `SbatchManager`; tracked as Phase 3.
- spec §6.1 sketched `run(&self, cmd: SbatchCmd)`; the implementation follows the established `SbatchManager::spawn(&self)` pattern (`run(&self)`, no `cmd` parameter) for consistency.
- spec §6.2 declared `Wait(std::io::Error)`; implementation uses `Wait(anyhow::Error)` to match the actual `wait_terminal` return type.
```

- [ ] **Step 2: Run the full validation suite**

```bash
cargo test --lib --features pyo3
cargo clippy --all-targets --features pyo3 -- -D warnings
cargo fmt --all --check
uv run pytest python/tests/ -v
uv run ruff check python/
```

Expected counts:
- `cargo test --lib`: **≥362 passed** (P5 ended at 352; Task 1 adds 4 error tests, Task 2 adds 2 cancel tests, Task 3 adds 1 guard test, Task 4 adds 1 happy-path test, Task 5 adds 2 JobFailed tests → +10 tests).
- `pytest`: **40 passed, 1 skipped** (P5 ended at 39).
- clippy / fmt / ruff: clean.

If counts diverge by ±1 due to e.g. an additional helper test, update the CHANGELOG numbers in the implementer message rather than panicking.

- [ ] **Step 3: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): record Phase 2 P6 run() / cancel() / typed errors"
```

- [ ] **Step 4: Push branch (orchestrator runs)**

```bash
git push origin sbatch-module-phase2
```

- [ ] **Step 5: Update PR #6 body to include P6 (orchestrator runs after final review)**

Defer to the orchestrator: `gh pr edit 6 --body "$(cat ...)"` with a P6 section mirroring the existing P1..P5 structure.

---

## Self-review checklist

- [x] Every requirement in spec §6 has a task:
  - §6.1 `run()` signature → Task 3 (guard) + Task 4 (chain) + Task 6 (pyo3)
  - §6.1 `cancel()` signature → Task 2 (Rust) + Task 6 (pyo3)
  - §6.2 `SbatchRunError` enum → Task 1
  - §6.3 `SbatchCancelError` + scancel idempotency → Task 1 (type) + Task 2 (behavior, with SLURM-side idempotency rather than local pre-check — documented as deviation in Task 9 Notes)
  - §6.4 ArrayNotSupported guard → Task 3
  - §6.5 Drop semantics → explicit non-goal documented in CHANGELOG Notes (Phase 3 follow-up)
- [x] No placeholders: each step has runnable commands and concrete code.
- [x] Type consistency: `SbatchRunError`, `FinishedInfo`, `JobState::Completed` referenced identically across Tasks 1, 4, 5, 6, 7.
- [x] Tests cover both branches: error enums (Task 1), cancel happy + error (Task 2), array guard (Task 3), happy path (Task 4), JobFailed two variants (Task 5), Python guard (Task 8).
- [x] Phase 1 lesson §11 grep-before-plan: confirmed against current branch (`src/runner.rs:95` `query_job_states_with_exit_code_with`, `src/sbatch/handle.rs:378` `wait_terminal`, `src/sbatch/handle.rs:340` `refresh_with_sacct`, `src/entities/slurm/status.rs:183` `as_slurm_str`, `src/sbatch/handle.rs:547` `MoveDispatcher`).
- [x] CHANGELOG entry + .pyi sync + Python tests + spec-deviation documented inline.
