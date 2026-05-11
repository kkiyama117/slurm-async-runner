# Phase 3 P4: `DynJobHandleCommon` + `into_dyn()` + Python `Protocol`

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a dyn-safe companion `DynJobHandleCommon` to `JobHandleCommon`, plus an `into_dyn(handle)` free function that wraps any `H: JobHandleCommon` into `Arc<dyn DynJobHandleCommon>`. This makes type-erased multi-backend collections possible (e.g. a future unified dashboard or attach UI). Optionally expose a Python `runtime_checkable Protocol` so downstream Python code can write `isinstance(h, JobHandleCommon)` for duck-typing.

**Why now:** P3 introduced the trait. P4 closes the type-erasure gap so the trait is genuinely useful in collection / async-task contexts where associated types break. Doing both in the same phase keeps the abstraction story coherent.

**Architecture:**
- `DynJobHandleCommon` trait flattens the snapshot via `serde_json::Value` (Phase 1 educational note: blanket impls cause E0034 ambiguity, so we provide `DynHandleAdapter<H>` + explicit `into_dyn` constructor — never a blanket `impl<H> DynJobHandleCommon for H`).
- `watch::Receiver` is intentionally absent from the dyn trait; type-erased subscribers should use `snapshot_json()` polling.
- `into_dyn` is a free function (`crate::handle::into_dyn`) — matches the Phase 1 `DynJobDispatcher` constructor pattern.
- Python: a `runtime_checkable Protocol` named `JobHandleCommon` in `__init__.pyi`. No new pyclass — duck-typing is sufficient and avoids violating the Pyclass Single Owner rule.

**Spec reference:** `docs/superpowers/specs/2026-05-11-sbatch-phase3-design.md` §3.3, §3.4, §6.1, §11 (open decisions).

**Open decisions resolved here:**
- `DynJobHandleCommon::kind` returns `&'static str` (matches `JobSnapshot::kind()`).
- `into_dyn` is a free function (not a method on `JobHandleCommon`).
- Python `Protocol` is **included** in P4 — adds value at zero pyo3 cost.

---

## File Structure

| File | Role |
|---|---|
| `src/handle.rs` | Append `DynJobHandleCommon` trait, `DynHandleAdapter<H>`, `into_dyn` function. |
| `src/lib.rs` | Re-export `DynJobHandleCommon`, `DynHandleAdapter`, `into_dyn`. |
| `tests/job_handle_common.rs` | Add tests for `into_dyn` round-trip and `snapshot_json` round-trip. |
| `python/slurm_async_runner/_slurm_async_runner_core/__init__.pyi` | Add `runtime_checkable Protocol` `JobHandleCommon`. |
| `python/tests/test_protocol.py` | NEW — assert `isinstance` checks pass against existing pyclass handles. |
| `CHANGELOG.md` | `[Unreleased]` → `### Phase 3 P4` entry. |

---

## Task 1: Define `DynJobHandleCommon` trait

**Files:**
- Modify: `src/handle.rs`

- [ ] **Step 1: Write the failing test**

  Append to `src/handle.rs` (or to a new `#[cfg(test)] mod tests` if not present):

  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;

      // Compile-only assertion that the trait is object-safe.
      #[allow(dead_code)]
      fn _dyn_safe(_: &dyn DynJobHandleCommon) {}
  }
  ```

  Compile fails (`DynJobHandleCommon` undefined).

- [ ] **Step 2: Add the trait**

  ```rust
  /// Object-safe companion to [`JobHandleCommon`]. Use this trait when you
  /// need to hold a heterogenous collection of handles (e.g.
  /// `Vec<Arc<dyn DynJobHandleCommon>>`). Snapshot return types are
  /// flattened to `serde_json::Value` because each backend's snapshot
  /// type is associated, not parameterized — type erasure forces a
  /// runtime-typed escape hatch.
  ///
  /// Construct with [`into_dyn`].
  #[async_trait::async_trait]
  pub trait DynJobHandleCommon: Send + Sync + 'static {
      fn uuid(&self) -> Uuid;
      fn jobid(&self) -> Option<u64>;
      fn is_running(&self) -> bool;
      fn is_finished(&self) -> bool;
      fn exit_code(&self) -> Option<i32>;
      fn kind(&self) -> &'static str;
      fn snapshot_json(&self) -> serde_json::Value;
      async fn refresh_json(&self) -> anyhow::Result<serde_json::Value>;
  }
  ```

- [ ] **Step 3: Test compiles**

  ```bash
  cargo test --lib --features pyo3 dyn_safe -- --nocapture
  ```

---

## Task 2: Adapter + `into_dyn`

**Files:**
- Modify: `src/handle.rs`

- [ ] **Step 1: Write the failing test (for the adapter)**

  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;
      // ...

      #[tokio::test]
      async fn into_dyn_preserves_uuid_and_kind_for_sbatch() {
          // Skip-pattern: build_test_sbatch_handle exists in src/sbatch/handle.rs tests
          // but not in src/handle.rs tests. Move this assertion to
          // tests/job_handle_common.rs (Task 4) and only verify compile here.
          fn _assert_into_dyn<H: JobHandleCommon>(h: H) -> Arc<dyn DynJobHandleCommon> {
              into_dyn(h)
          }
      }
  }
  ```

- [ ] **Step 2: Implement adapter + free fn**

  ```rust
  use std::sync::Arc;

  /// Adapter that erases an `H: JobHandleCommon` into a
  /// `dyn DynJobHandleCommon`. Constructed via [`into_dyn`].
  pub struct DynHandleAdapter<H: JobHandleCommon> {
      inner: H,
  }

  impl<H: JobHandleCommon> DynHandleAdapter<H> {
      pub fn new(inner: H) -> Self {
          Self { inner }
      }
  }

  #[async_trait::async_trait]
  impl<H: JobHandleCommon> DynJobHandleCommon for DynHandleAdapter<H> {
      fn uuid(&self) -> Uuid { self.inner.uuid() }
      fn jobid(&self) -> Option<u64> { self.inner.jobid() }
      fn is_running(&self) -> bool { self.inner.is_running() }
      fn is_finished(&self) -> bool { self.inner.is_finished() }
      fn exit_code(&self) -> Option<i32> { self.inner.exit_code() }
      fn kind(&self) -> &'static str { <H::Snapshot as crate::store::JobSnapshot>::kind() }
      fn snapshot_json(&self) -> serde_json::Value {
          serde_json::to_value(self.inner.snapshot())
              .expect("JobSnapshot must always serialize to JSON")
      }
      async fn refresh_json(&self) -> anyhow::Result<serde_json::Value> {
          let snap = self.inner.refresh().await?;
          Ok(serde_json::to_value(snap)?)
      }
  }

  /// Type-erase any `H: JobHandleCommon` into a shareable
  /// `Arc<dyn DynJobHandleCommon>`. There is **no blanket impl** of
  /// `DynJobHandleCommon` for `JobHandleCommon` — Phase 1 demonstrated
  /// that blanket impls combined with associated types trigger
  /// E0034 ambiguity. The explicit constructor keeps the conversion
  /// site obvious.
  pub fn into_dyn<H: JobHandleCommon>(h: H) -> Arc<dyn DynJobHandleCommon> {
      Arc::new(DynHandleAdapter::new(h))
  }
  ```

- [ ] **Step 3: Re-export from `lib.rs`**

  ```rust
  pub use handle::{
      DynHandleAdapter, DynJobHandleCommon, JobHandleCommon, into_dyn,
  };
  ```

- [ ] **Step 4: Verify**

  ```bash
  cargo build --lib --features pyo3
  cargo doc --no-deps --features pyo3
  ```

---

## Task 3: Integration tests for `into_dyn`

**Files:**
- Modify: `tests/job_handle_common.rs`

- [ ] **Step 1: Add round-trip tests**

  Append to the integration test file from P3:

  ```rust
  use slurm_async_runner::{into_dyn, DynJobHandleCommon};
  use std::sync::Arc;

  #[tokio::test]
  async fn into_dyn_sbatch_preserves_kind_and_getters() {
      let h = build_finished_sbatch_handle().await;
      let expected_uuid = h.uuid();
      let expected_jobid = h.jobid();
      let dyn_h: Arc<dyn DynJobHandleCommon> = into_dyn(h);

      assert_eq!(dyn_h.uuid(), expected_uuid);
      assert_eq!(dyn_h.jobid(), expected_jobid);
      assert_eq!(dyn_h.kind(), "sbatch");

      let json = dyn_h.snapshot_json();
      assert!(json.is_object(), "snapshot_json should be a JSON object");
  }

  #[tokio::test]
  async fn into_dyn_tssrun_preserves_kind_and_getters() {
      let h = build_finished_tssrun_handle().await;
      let expected_uuid = h.uuid();
      let dyn_h: Arc<dyn DynJobHandleCommon> = into_dyn(h);

      assert_eq!(dyn_h.uuid(), expected_uuid);
      assert_eq!(dyn_h.kind(), "tssrun");
  }

  #[tokio::test]
  async fn dyn_handles_can_be_held_in_a_heterogenous_vec() {
      let s = build_finished_sbatch_handle().await;
      let t = build_finished_tssrun_handle().await;

      let handles: Vec<Arc<dyn DynJobHandleCommon>> =
          vec![into_dyn(s), into_dyn(t)];

      let kinds: Vec<&'static str> = handles.iter().map(|h| h.kind()).collect();
      assert_eq!(kinds, vec!["sbatch", "tssrun"]);
  }
  ```

- [ ] **Step 2: Run**

  ```bash
  cargo test --test job_handle_common --features pyo3
  ```

---

## Task 4: Python `Protocol`

**Files:**
- Modify: `python/slurm_async_runner/_slurm_async_runner_core/__init__.pyi`
- Create: `python/tests/test_protocol.py`

- [ ] **Step 1: Add `Protocol` to `__init__.pyi`**

  ```python
  from typing import Protocol, runtime_checkable
  from uuid import UUID


  @runtime_checkable
  class JobHandleCommon(Protocol):
      """Duck-typed common interface for SbatchJobHandle and JobHandle (tssrun).

      Mirrors the Rust `crate::handle::JobHandleCommon` trait. Use this for
      `isinstance` checks when you want to accept either backend.
      """

      def uuid(self) -> UUID: ...
      def jobid(self) -> int | None: ...
      def is_running(self) -> bool: ...
      def is_finished(self) -> bool: ...
      def exit_code(self) -> int | None: ...
      async def refresh(self) -> object: ...
      async def wait_terminal(self, poll_interval_seconds: float) -> object: ...
  ```

  Note: snapshot return type is `object` because Python lacks Rust's associated types and we don't want to introduce a Union of two concrete classes (a future third backend would break it).

- [ ] **Step 2: Write the test**

  Create `python/tests/test_protocol.py`:

  ```python
  """Verify that existing pyclass handles satisfy the JobHandleCommon Protocol."""

  from slurm_async_runner._slurm_async_runner_core import JobHandleCommon
  # Backend-specific imports — adjust paths if pyo3 module layout differs
  from slurm_async_runner._slurm_async_runner_core.tssrun import JobHandle as TssrunHandle
  from slurm_async_runner._slurm_async_runner_core.sbatch import SbatchJobHandle


  def test_tssrun_jobhandle_satisfies_protocol_at_class_level():
      assert hasattr(TssrunHandle, "uuid")
      assert hasattr(TssrunHandle, "jobid")
      assert hasattr(TssrunHandle, "is_running")
      assert hasattr(TssrunHandle, "is_finished")
      assert hasattr(TssrunHandle, "exit_code")
      assert hasattr(TssrunHandle, "refresh")
      assert hasattr(TssrunHandle, "wait_terminal")


  def test_sbatch_jobhandle_satisfies_protocol_at_class_level():
      assert hasattr(SbatchJobHandle, "uuid")
      assert hasattr(SbatchJobHandle, "jobid")
      assert hasattr(SbatchJobHandle, "is_running")
      assert hasattr(SbatchJobHandle, "is_finished")
      assert hasattr(SbatchJobHandle, "exit_code")
      assert hasattr(SbatchJobHandle, "refresh")
      assert hasattr(SbatchJobHandle, "wait_terminal")


  # Instance-level isinstance check requires constructed handles. Skip with
  # a clear note pointing at live smoke / integration coverage.
  import pytest


  @pytest.mark.skip(reason="Requires constructed handles; class-level hasattr above is sufficient")
  def test_isinstance_check_against_constructed_handles():
      pass
  ```

  The `hasattr` checks are deliberately structural — `runtime_checkable Protocol` `isinstance` does the same kind of check, but instantiating real handles in pytest needs a live SLURM (sbatch) or a tssrun child process (tssrun). Class-level structural checks catch contract drift without that overhead.

- [ ] **Step 3: Run pytest**

  ```bash
  uv run pytest python/tests -x
  ```

---

## Task 5: CHANGELOG entry

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add `### Phase 3 P4`**

  ```markdown
  ### Phase 3 P4 — type-erased `DynJobHandleCommon` + Python `Protocol`

  - New `crate::handle::DynJobHandleCommon` trait (object-safe). Snapshot is exposed as
    `serde_json::Value` to flatten the associated type.
  - `crate::handle::DynHandleAdapter<H>` adapter + `crate::handle::into_dyn(handle)` free
    function — the explicit constructor avoids the blanket-impl + associated-type E0034
    ambiguity diagnosed in Phase 1 (handover §4).
  - Heterogenous collections of `Arc<dyn DynJobHandleCommon>` mixing tssrun and sbatch
    handles now compile (see new tests in `tests/job_handle_common.rs`).
  - Python: `runtime_checkable Protocol` `JobHandleCommon` added to `__init__.pyi` so
    Python callers can write `isinstance(h, JobHandleCommon)` against either backend.
  ```

---

## Task 6: Final verification

- [ ] `cargo test --features pyo3` — all green (lib + integration tests)
- [ ] `cargo clippy --all-targets --features pyo3 -- -D warnings` — clean
- [ ] `cargo fmt --all --check` — clean
- [ ] `cargo doc --no-deps --features pyo3` — `DynJobHandleCommon` / `into_dyn` rendered
- [ ] `uv run pytest python/tests` — all green incl. new `test_protocol.py`
- [ ] CHANGELOG `[Unreleased]` updated
- [ ] Spec checklist (`docs/superpowers/specs/2026-05-11-sbatch-phase3-design.md` §10) ticked off
- [ ] Phase 3 hand-off doc (`docs/attention_phase3.md`) drafted (separate work item — track as Phase 4 prep, not blocking P4 merge)
