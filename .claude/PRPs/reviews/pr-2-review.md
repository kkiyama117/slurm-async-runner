# PR Review: #2 — feat(tssrun): non-blocking tssrun wrapper with env inspection + cross-process attach

**Reviewed**: 2026-05-09
**Author**: kkiyama117
**Branch**: tssrun-wrapper-env → main
**Decision**: REQUEST CHANGES

## Summary

Solid TDD-driven module with extensive coverage (64 unit + 1 integration + 18 Python tests). Architecture cleanly separates spec (`TssrunCmd`/`Resource`) from runtime (`BackgroundDispatcher`) from state (`JobHandle` + watch channel + atomic JSON snapshot). Two HIGH concurrency / correctness issues in the Python binding and `wait()` exit-code semantics warrant fixes before merge; everything else is MEDIUM/LOW polish.

## Findings

### CRITICAL
None.

### HIGH

**H1. `PyTssrunJobHandle` mutex serializes `wait()` against all snapshot getters — `is_running()` / `pid` / `jobid` polls block until the job exits.**
File: `src/py_export/tssrun.rs:166-230`

`PyTssrunJobHandle` wraps `Arc<tokio::sync::Mutex<JobHandle>>`. Every method (including snapshot-only getters like `pid`, `jobid`, `is_running`, `exit_code`, `sent_env`) takes `inner.lock().await` before calling through. `wait()` holds that same mutex for the entire wait duration:

```rust
fn wait<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    let inner = self.inner.clone();
    future_into_py(py, async move {
        inner.lock().await.wait().await   // mutex held for the full job lifetime
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    })
}
```

A user who legitimately wants to poll while waiting:

```python
asyncio.gather(handle.wait(), watchdog(handle))   # watchdog awaits handle.is_running()
```

…will see the watchdog's `is_running()` block for the entire wait, defeating the "non-blocking with snapshot inspection" value proposition the PR description advertises.

The Rust `JobHandle` is fine: snapshot getters take `&self` and read from `watch::Receiver::borrow()` (cheap, lock-free for readers); only `wait()` is `&mut self`. The Python wrapper coarsens that to a single mutex.

**Suggested fix:** Split the Python handle into a snapshot-reader (clones `watch::Receiver` + `persist_path`, no lock) and a waiter (consumes the `JoinHandle` once via `Option::take()`, behind a small mutex that's only locked for the brief moment of taking the handle). Or stash a `tokio::sync::Mutex<Option<JoinHandle<Result<i32>>>>` and have the snapshot getters bypass it entirely.

**H2. `JobHandle::wait()` swallows signal-killed exits and returns `Ok(0)`, indistinguishable from clean success.**
File: `src/tssrun/handle.rs:124`

```rust
let status = child.lock().await.wait().await?;
let code = status.code();                 // Option<i32>; None on signal kill
tx.send_modify(|s| {
    s.finished = Some(FinishedInfo {
        exit_code: code,                  // honest: stored as None
        finished_at_unix: now_unix(),
    });
});
...
Ok(code.unwrap_or(0))                     // dishonest: returns 0 to wait() caller
```

Signal-killed children (SIGTERM during a SLURM time-limit kill, SIGKILL from OOM, etc.) all surface to the caller as exit code 0. The snapshot itself is correct — `finished.exit_code` stays `None` — but the value returned from `wait().await` lies. Code that branches on `if exit_code != 0 { error }` will silently treat a killed job as success.

**Suggested fix:** Either return `Result<Option<i32>>` (and let callers handle the None), return a typed `JobOutcome { exit_code: Option<i32>, signal: Option<i32> }`, or at minimum use `unwrap_or(-1)` so the convention "negative = abnormal" is preserved. Update `PyTssrunJobHandle::wait` to surface the same truth to Python.

### MEDIUM

**M1. `TssrunManager` exposes all fields as `pub` — mid-flight mutation isn't surfaced to in-flight handles.**
File: `src/tssrun/manager.rs:26-30`

```rust
pub struct TssrunManager {
    pub cmd: TssrunCmd,
    pub state_dir: Option<PathBuf>,
    pub log_sink: Arc<dyn JobLogSink>,
}
```

Spawning takes `&self`, so the values are snapshotted at `spawn()` time — but a Rust caller who later does `manager.state_dir = Some(other_dir)` will be surprised that already-spawned handles keep persisting to the original directory. The Python wrapper insulates Python users (it holds `Arc<TssrunManager>`, immutable after construction), but the Rust API invites confusion.

**Suggested fix:** Demote to `pub(crate)` and rely on `with_state_dir` / `with_log_sink` builders (already provided), or keep `pub` and add a doc comment that mutation after a spawn is intentionally not retroactive.

**M2. `parse_environ` silently drops non-UTF8 entries with no observability.**
File: `src/tssrun/handle.rs:231-235`

```rust
if let Ok(s) = std::str::from_utf8(raw)
    && let Some((k, v)) = s.split_once('=')
{
    out.insert(k.to_string(), v.to_string());
}
```

A non-UTF8 environment variable (rare but legal on Linux) disappears with no warning. Other "best effort" paths in this PR use `tracing::warn!`. For consistency, log a count or sample of the dropped keys. Lossy-UTF8 (`String::from_utf8_lossy`) would also be acceptable since these are diagnostic snapshots.

**M3. `wait()` doc comment doesn't warn about signal-killed semantics (related to H2).**
File: `src/tssrun/handle.rs:177-180`

The current doc only mentions "errors when invoked on an attached handle". After fixing H2, update this doc and the `.pyi` stub (`python/slurm_async_runner/_core/tssrun.pyi:83`) to describe the abnormal-exit case explicitly.

**M4. `scripts/test_tssrun_live.py` `live_env` print is misleading on race.**
File: `scripts/test_tssrun_live.py:161-166`

The child sleeps 1 s and the script reads `/proc/<pid>/environ` immediately after spawn — a queued or quickly-scheduled job could already have exited, in which case the script prints `"likely exited"`. On non-Linux it prints the same message even though the cause is "no /proc". Not a bug, but the log line conflates two distinct outcomes. Differentiate with `sys.platform`-aware handling or distinct log messages.

### LOW

**L1. Per-line double `send_modify` when both `salloc:` markers arrive on separate lines.**
File: `src/tssrun/handle.rs:275-296` — Each tee'd line that updates `jobid` *and* `node` triggers two `send_modify` notifications. Not measurable in practice.

**L2. `tee_stdout` / `tee_stderr` are 2-line trampolines that add no value.**
File: `src/tssrun/handle.rs:240-256` — Inline at call site or replace with a single generic helper passed `LogStream::Stdout/Stderr` directly.

**L3. `BufReader::lines()` is unbounded; a pathological child emitting `'a' * 10GB` without `\n` would balloon RAM.**
File: `src/tssrun/handle.rs:267` — Trust boundary is the user's own SLURM job, so risk is theoretical, but a `take(MAX_LINE_BYTES)` wrapper would harden it.

**L4. `Vec::with_capacity(8 + self.args.len())` magic literal.**
File: `src/tssrun/cmd.rs:84` — Capacity hint of 8 is one of: `tssrun_bin + -p Q + -t T + --rsc S + --x11 + program` = 7 — almost right. Either use 7 or extract `const PRELUDE_SLOTS = 8;` with a comment, or just drop the hint.

**L5. `mkdir_p` runs on every snapshot persist.**
File: `src/tssrun/handle.rs:318` — Cheap because the kernel just stats an existing dir, but for jobs emitting 10⁵+ stdout lines it's still measurable. Cache the "directory exists" bit on the handle.

**L6. `InMemoryLogSink` uses `std::sync::Mutex` inside an async fn.**
File: `src/tssrun/log.rs:50,67` — Lock is held only for `.push()` (no await while held), so fine. Worth a code comment confirming the intent so future readers don't try to "fix" it to `tokio::sync::Mutex`.

**L7. `.pyi` stubs are hand-maintained — drift risk.**
File: `python/slurm_async_runner/_core/tssrun.pyi:1-4` — The header notes pyo3-stub-gen doesn't derive for `#[pymodule_export]`-wired pyclasses. Add a CI check that imports both and diffs `__all__` + signatures so the stub doesn't silently drift from `src/py_export/tssrun.rs`.

## Validation Results

| Check | Result |
|---|---|
| Clippy (`-D warnings`) | Pass |
| Format (`cargo fmt --check`) | Pass |
| Cargo tests (`--all-targets`) | Pass — 64 unit + 1 integration |
| Pytest (`python/tests`) | Pass — 18 passed, 2 skipped (live + sacct, both expected) |
| Ruff (`check`) | Pass |

## Files Reviewed

- Modified: `Cargo.toml`, `Cargo.lock`, `README.md`, `CHANGELOG.md`, `src/lib.rs`, `src/py_export/mod.rs`
- Added (Rust): `src/tssrun/{cmd,handle,log,manager,mod,parse}.rs`, `src/dispatcher.rs` (additions), `src/py_export/tssrun.rs`, `tests/tssrun_integration.rs`
- Added (Python): `python/tests/test_tssrun.py`, `python/tests/test_tssrun_live.py`, `python/slurm_async_runner/_core/tssrun.pyi`, `scripts/test_tssrun_live.py`
- Added (docs): `docs/superpowers/specs/2026-05-09-tssrun-wrapper-env-design.md`, `docs/superpowers/plans/2026-05-09-tssrun-wrapper-env.md`
