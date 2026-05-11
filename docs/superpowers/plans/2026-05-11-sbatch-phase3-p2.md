# Phase 3 P2: `TssrunJobHandle::refresh()` returns Snapshot + adds `wait_terminal()`

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Bring `TssrunJobHandle` to parity with `SbatchJobHandle` on the two async methods that the Phase 3 `JobHandleCommon` trait will require:
1. `refresh(&self) -> Result<TssrunJobSnapshot>` (was `Result<()>`)
2. `wait_terminal(&self, poll_interval: Duration) -> Result<TssrunJobSnapshot>` (new)

Both changes are additive: existing callers that wrote `let _ = handle.refresh().await?;` keep working because Rust does not warn on discarded `Ok(T)`.

**Why now:** P3 will add `impl JobHandleCommon for TssrunJobHandle` whose trait fixes the return type of `refresh` and demands `wait_terminal`. Doing the additive shape change in P2 means P3 is purely the trait wiring.

**Architecture:** `refresh()` already mutates the internal `watch::Sender<TssrunJobSnapshot>` after a query — we just additionally borrow the new value via `*self.snapshot_tx.borrow().clone()` (or by cloning the snapshot before sending) and return it. `wait_terminal()` is a simple `loop { snap = refresh; if finished return; sleep; }` — same structure as `SbatchJobHandle::wait_terminal` but on `&self` (tssrun handle is shareable; sbatch consumes self because it integrates with the `Drop`-warn pattern in `SbatchManager::run`). The `&self` choice keeps tssrun's existing handle ergonomics intact.

**Spec reference:** `docs/superpowers/specs/2026-05-11-sbatch-phase3-design.md` §4.2 (refresh return), §4.5 (wait_terminal addition), §11 (poll_interval default).

**Deviation from spec literal:**
- spec §4.5 wrote `pub async fn wait_terminal(&self, ...)` — confirmed correct here. Sbatch's `consume self` is intentionally **not** mirrored because tssrun already exposes `Clone`-able snapshot watchers and there's no `Drop`-warn pattern to bias the API.

---

## File Structure

| File | Role |
|---|---|
| `src/tssrun/handle.rs` | (1) Change `refresh` return from `Result<()>` to `Result<TssrunJobSnapshot>`. (2) Add `wait_terminal` method. (3) Tests. |
| `src/py_export/tssrun.rs` | Update pyo3 wrapper for `refresh` (already returns `()`/`None` to Python — keep that contract by `.await?;` and ignoring the snapshot internally). Add `wait_terminal` async pyo3 method exposing a snapshot-returning future. |
| `python/slurm_async_runner/_slurm_async_runner_core/tssrun.pyi` | Add `wait_terminal` signature. `refresh` Python signature stays `async def refresh(self) -> None` to avoid Python-side breakage (snapshot fetch is via `snapshot()`). |
| `python/tests/test_tssrun.py` | Add a smoke test for `wait_terminal` against an in-memory mock that flips state after N polls. |
| `CHANGELOG.md` | `[Unreleased]` → `### Phase 3 P2` entry. |

---

## Task 1: Change `TssrunJobHandle::refresh()` return type

**Files:**
- Modify: `src/tssrun/handle.rs`

- [ ] **Step 1: Write the failing test (TDD)**

  Append to `#[cfg(test)] mod tests`:

  ```rust
  #[tokio::test]
  async fn refresh_returns_snapshot() {
      let h = build_attached_handle_for_test();
      let snap: TssrunJobSnapshot = h.refresh().await.unwrap();
      assert_eq!(snap.uuid, h.snapshot().uuid);
  }
  ```

  Run: `cargo test --lib --features pyo3 refresh_returns_snapshot` — fails (current refresh returns `()`).

- [ ] **Step 2: Update the signature + body**

  In `src/tssrun/handle.rs`:

  ```rust
  pub async fn refresh(&self) -> Result<TssrunJobSnapshot> {
      // ... existing query body that ends with self.snapshot_tx.send_replace(new_snap) ...
      Ok(self.snapshot_tx.borrow().clone())
  }
  ```

  If the existing body uses `send_replace(new)` and discards the value, capture it first:
  ```rust
  let new_snap = /* compute */;
  let _ = self.snapshot_tx.send_replace(new_snap.clone());
  Ok(new_snap)
  ```

- [ ] **Step 3: Verify call sites are non-breaking**

  ```bash
  cargo build --all-features 2>&1 | grep -E 'error|warning: unused' | head
  ```

  Existing internal callers like `let _ = handle.refresh().await?;` continue to compile. Any new `let snap = handle.refresh().await?;` patterns now give the snapshot directly.

- [ ] **Step 4: Run test — green**

---

## Task 2: Add `wait_terminal` method

**Files:**
- Modify: `src/tssrun/handle.rs`

- [ ] **Step 1: Write the failing test (TDD)**

  We need a deterministic way to test polling without spawning a real tssrun child. Add a test-only helper that constructs a handle whose `refresh` is mocked, then assert `wait_terminal` returns once `finished` is set.

  ```rust
  #[tokio::test]
  async fn wait_terminal_returns_once_finished_set() {
      use std::time::Duration;

      let h = build_attached_handle_for_test();
      let h2 = h.clone(); // assumes TssrunJobHandle: Clone via internal Arc
      let flipper = tokio::spawn(async move {
          tokio::time::sleep(Duration::from_millis(20)).await;
          h2.test_force_finished(0); // test-only helper; see Step 2
      });

      let snap = h
          .wait_terminal(Duration::from_millis(5))
          .await
          .unwrap();
      flipper.await.unwrap();
      assert!(snap.is_finished());
      assert_eq!(snap.exit_code(), Some(0));
  }
  ```

  If `TssrunJobHandle` is not `Clone`, replace `h2.clone()` with use of the existing `attach_snapshot` constructor or `watch::Receiver` to mutate via the snapshot store side. Adapt to Phase 1/2 actual API.

  Add a `#[cfg(test)] fn test_force_finished(&self, exit: i32)` helper that calls `send_replace` on the snapshot tx with `finished = Some(FinishedInfo { exit_code: Some(exit), finished_at_unix: 0 })`. This must be `#[cfg(test)]` only — no production exposure.

  Run: `cargo test --lib --features pyo3 wait_terminal_returns_once_finished_set` — fails (`wait_terminal` undefined).

- [ ] **Step 2: Implement `wait_terminal`**

  In `src/tssrun/handle.rs`, inside `impl TssrunJobHandle`:

  ```rust
  /// Block (asynchronously) until the snapshot reports `is_finished()`.
  /// Polls via `refresh` every `poll_interval`. The returned snapshot is
  /// the first refreshed snapshot that satisfies `is_finished()`.
  ///
  /// Default poll interval (when called from Python without an argument)
  /// matches `SbatchJobHandle::wait_terminal`: 30 s.
  pub async fn wait_terminal(
      &self,
      poll_interval: std::time::Duration,
  ) -> Result<TssrunJobSnapshot> {
      loop {
          let snap = self.refresh().await?;
          if snap.is_finished() {
              return Ok(snap);
          }
          tokio::time::sleep(poll_interval).await;
      }
  }
  ```

- [ ] **Step 3: Run test — green**

---

## Task 3: pyo3 — keep `refresh` Python signature, add `wait_terminal`

**Files:**
- Modify: `src/py_export/tssrun.rs`, `python/slurm_async_runner/_slurm_async_runner_core/tssrun.pyi`

- [ ] **Step 1: Decide on Python `refresh` semantics**

  The Phase 1/2 Python `refresh` returns `None`. We **keep** that contract — Python users who want the snapshot use `handle.snapshot()`. This avoids Python-side breakage and matches the sbatch pyo3 contract (sbatch Python `refresh` also returns the snapshot — actually verify which). If sbatch Python returns the snapshot, then for symmetry tssrun Python should too:

  ```bash
  rg 'PySbatchJobHandle.*refresh' src/py_export/sbatch.rs
  ```

  Adopt whichever contract sbatch uses. Default decision (if uncertain): **return the snapshot from Python `refresh` too**, because Python users already rely on awaiting it and will appreciate the value. Stub becomes:

  ```python
  async def refresh(self) -> JobHandleSnapshot: ...
  ```

  If this changes the existing stub, document in CHANGELOG as a Python-side enhancement.

- [ ] **Step 2: Add `wait_terminal` pyo3 method**

  In `src/py_export/tssrun.rs`, inside `impl PyJobHandle`:

  ```rust
  /// Block until the job is terminal. `poll_interval_seconds` defaults to 30.
  #[pyo3(signature = (poll_interval_seconds = 30.0))]
  fn wait_terminal<'py>(
      &self,
      py: Python<'py>,
      poll_interval_seconds: f64,
  ) -> PyResult<Bound<'py, PyAny>> {
      let inner = self.inner.clone();
      let dur = std::time::Duration::from_secs_f64(poll_interval_seconds);
      pyo3_async_runtimes::tokio::future_into_py(py, async move {
          let snap = inner
              .wait_terminal(dur)
              .await
              .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
          Python::with_gil(|py| Ok(Py::new(py, PyJobHandleSnapshot::from(snap))?))
      })
  }
  ```

  Adapt to whatever pyo3 future-bridge / async runtime helper this repo uses.

- [ ] **Step 3: Update `tssrun.pyi`**

  ```python
  class JobHandle:
      async def refresh(self) -> JobHandleSnapshot: ...
      async def wait_terminal(self, poll_interval_seconds: float = 30.0) -> JobHandleSnapshot: ...
  ```

- [ ] **Step 4: Verify**

  ```bash
  uv run maturin develop
  uv run pytest python/tests -x
  ```

---

## Task 4: Python smoke test for `wait_terminal`

**Files:**
- Modify: `python/tests/test_tssrun.py`

- [ ] **Step 1: Add a deterministic test**

  This needs a way to trigger a finished snapshot from Python without launching a real tssrun. If a mock dispatcher pyo3 fixture exists, use it. Otherwise mark the test `pytest.skip` with a clear note pointing at the live smoke script.

  ```python
  import pytest

  @pytest.mark.skip(reason="Requires a mock dispatcher; covered by Rust unit test in handle.rs")
  async def test_wait_terminal_returns_snapshot_when_finished():
      ...
  ```

  Real coverage stays in Rust unit tests — the Python skip is a placeholder that documents intent and prevents drift.

---

## Task 5: CHANGELOG entry

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add `### Phase 3 P2` section under `[Unreleased]`**

  ```markdown
  ### Phase 3 P2 — tssrun handle async parity with sbatch

  - `TssrunJobHandle::refresh()` now returns `Result<TssrunJobSnapshot>` instead of `Result<()>`.
    Existing callers using `let _ = handle.refresh().await?;` continue to compile (Rust does not
    warn on discarded `Ok(T)`).
  - `TssrunJobHandle::wait_terminal(poll_interval)` added, mirroring `SbatchJobHandle::wait_terminal`.
    `&self` (not `self`) — tssrun handle ergonomics keep allowing post-wait reuse.
  - Python `PyJobHandle.wait_terminal(poll_interval_seconds=30.0)` exposed; `refresh` now returns
    the snapshot to Python callers as well (matches sbatch).
  ```

---

## Task 6: Final verification

- [ ] `cargo test --lib --features pyo3` — all green
- [ ] `cargo clippy --all-targets --features pyo3 -- -D warnings` — clean
- [ ] `cargo fmt --all --check` — clean
- [ ] `uv run pytest python/tests` — all green
- [ ] CHANGELOG `[Unreleased]` updated
- [ ] Spec checklist (`docs/superpowers/specs/2026-05-11-sbatch-phase3-design.md` §10) ticked off
- [ ] Live smoke (KUDPC if possible): `wait_terminal` returns within expected time on a quick `/bin/true` job
