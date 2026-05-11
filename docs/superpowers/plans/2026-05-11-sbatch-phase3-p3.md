# Phase 3 P3: Introduce `JobHandleCommon` trait + impl for both backends

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Define `crate::handle::JobHandleCommon` (the unified handle trait) and implement it for both `SbatchJobHandle` and `TssrunJobHandle`. This trait makes the naming convergence enforced in Phase 2 §7.1 mechanically checkable: code that wants "any tssrun-or-sbatch handle" can finally write `H: JobHandleCommon`.

**Why now:** P1 made the Rust struct names symmetric. P2 added the async parity (`refresh -> Result<Snapshot>`, `wait_terminal`). Both prerequisites are now in place; the trait can be expressed without per-backend specialization.

**Architecture:** The trait keeps an `associated type Snapshot: JobSnapshot` so each impl returns its own concrete snapshot type — no boxed snapshots, no JSON serialize on the hot path. The trait is **not** dyn-safe by design; Phase 3 P4 introduces the dyn-safe `DynJobHandleCommon` companion. Both impls forward to existing methods, no duplication.

**Spec reference:** `docs/superpowers/specs/2026-05-11-sbatch-phase3-design.md` §3.1, §3.2, §3.5, §4.4 (`wait_terminal` belongs in trait), §7 (Plan split — P3).

**Deviation from spec literal** — none anticipated; if any impl reveals friction (e.g. `wait_terminal` consume-self vs `&self`), see §7 Open Decisions and document.

---

## File Structure

| File | Role |
|---|---|
| `src/handle.rs` | NEW. Defines `JobHandleCommon` trait. No impls (impls live next to each handle). |
| `src/lib.rs` | `pub mod handle;` + `pub use handle::JobHandleCommon;` re-export. |
| `src/sbatch/handle.rs` | Add `impl JobHandleCommon for SbatchJobHandle` (delegates to existing methods). |
| `src/tssrun/handle.rs` | Add `impl JobHandleCommon for TssrunJobHandle` (delegates). |
| `tests/job_handle_common.rs` | NEW integration test. Generic helper `assert_handle_common_contract<H: JobHandleCommon>(handle)` exercised against both backends. |
| `CHANGELOG.md` | `[Unreleased]` → `### Phase 3 P3` entry. |

---

## Task 1: Create `src/handle.rs` with the trait

**Files:**
- Create: `src/handle.rs`

- [ ] **Step 1: Write the trait + a doctest**

  ```rust
  //! Cross-backend handle abstraction. See
  //! `docs/superpowers/specs/2026-05-11-sbatch-phase3-design.md` §3 for the
  //! design rationale.

  use anyhow::Result;
  use tokio::sync::watch;
  use uuid::Uuid;

  use crate::store::JobSnapshot;

  /// Unified handle abstraction over tssrun and sbatch.
  ///
  /// Implementors expose the **core 5 sync getters** that both backends
  /// already provide ([`uuid`](Self::uuid), [`jobid`](Self::jobid),
  /// [`is_running`](Self::is_running), [`is_finished`](Self::is_finished),
  /// [`exit_code`](Self::exit_code)), plus snapshot accessors and the
  /// async [`refresh`](Self::refresh) / [`wait_terminal`](Self::wait_terminal)
  /// pair.
  ///
  /// This trait is **not** dyn-safe (it has an associated type). For
  /// type-erased usage see [`crate::handle::DynJobHandleCommon`] (Phase 3 P4).
  #[async_trait::async_trait]
  pub trait JobHandleCommon: Send + Sync + 'static {
      /// On-disk snapshot type. Carries the SLURM job state plus backend-specific fields.
      type Snapshot: JobSnapshot;

      // ─── core 5 sync getters (Phase 2 §7.1 naming convergence) ───
      fn uuid(&self) -> Uuid;
      fn jobid(&self) -> Option<u64>;
      fn is_running(&self) -> bool;
      fn is_finished(&self) -> bool;
      fn exit_code(&self) -> Option<i32>;

      // ─── snapshot accessors (lock-free) ───
      fn snapshot(&self) -> Self::Snapshot;
      fn watch(&self) -> watch::Receiver<Self::Snapshot>;

      // ─── async ───
      /// Re-query SLURM (qgroup -l → squeue) and return the new snapshot.
      /// **Must not call `sacct`** (Phase 1 handover §2 invariant).
      async fn refresh(&self) -> Result<Self::Snapshot>;

      /// Block (asynchronously) until the snapshot is terminal, polling
      /// every `poll_interval`. Returns the first terminal snapshot.
      async fn wait_terminal(
          &self,
          poll_interval: std::time::Duration,
      ) -> Result<Self::Snapshot>;
  }
  ```

- [ ] **Step 2: Wire into `src/lib.rs`**

  ```rust
  // existing modules...
  pub mod handle;
  // existing re-exports...
  pub use handle::JobHandleCommon;
  ```

  Place the `pub mod handle;` line near the other top-level module declarations and the `pub use` near the other re-exports.

- [ ] **Step 3: Verify it compiles**

  ```bash
  cargo build --lib --features pyo3
  cargo doc --no-deps --features pyo3
  ```

---

## Task 2: Implement for `SbatchJobHandle`

**Files:**
- Modify: `src/sbatch/handle.rs`

- [ ] **Step 1: Write the failing test**

  Add to `#[cfg(test)] mod tests` in `src/sbatch/handle.rs`:

  ```rust
  #[tokio::test]
  async fn sbatch_handle_implements_jobhandlecommon() {
      use crate::handle::JobHandleCommon;
      let h = build_test_sbatch_handle();
      // Compile-time assertion that the trait is satisfied.
      fn _assert_impl<H: JobHandleCommon>() {}
      _assert_impl::<SbatchJobHandle>();

      // Runtime assertions — getters delegate.
      let _: Uuid = JobHandleCommon::uuid(&h);
      assert_eq!(JobHandleCommon::is_running(&h), h.is_running());
  }
  ```

  Compile fails (no impl yet).

- [ ] **Step 2: Add the impl**

  Below the existing `impl SbatchJobHandle { ... }` block:

  ```rust
  #[async_trait::async_trait]
  impl crate::handle::JobHandleCommon for SbatchJobHandle {
      type Snapshot = SbatchJobSnapshot;

      fn uuid(&self) -> Uuid { Self::uuid(self) }
      fn jobid(&self) -> Option<u64> { Self::jobid(self) }
      fn is_running(&self) -> bool { Self::is_running(self) }
      fn is_finished(&self) -> bool { Self::is_finished(self) }
      fn exit_code(&self) -> Option<i32> { Self::exit_code(self) }

      fn snapshot(&self) -> SbatchJobSnapshot { Self::snapshot(self) }
      fn watch(&self) -> tokio::sync::watch::Receiver<SbatchJobSnapshot> { Self::watch(self) }

      async fn refresh(&self) -> anyhow::Result<SbatchJobSnapshot> {
          Self::refresh(self).await
      }

      async fn wait_terminal(
          &self,
          poll_interval: std::time::Duration,
      ) -> anyhow::Result<SbatchJobSnapshot> {
          // SbatchJobHandle::wait_terminal currently consumes `self`.
          // The trait demands `&self`. Bridge via Arc-clone-and-call:
          // `SbatchJobHandle` is internally `Arc<Inner>` so cloning is cheap.
          self.clone().wait_terminal(poll_interval).await
      }
  }
  ```

  **Important**: if `SbatchJobHandle::wait_terminal` consumes `self`, this trait impl **must clone before calling**. Verify `SbatchJobHandle: Clone` (it should be — Phase 1 design). If it isn't, add a thin `&self` overload `pub async fn wait_terminal_ref(&self, ...)` and call that instead. Note the deviation in the CHANGELOG.

- [ ] **Step 3: Run test — green**

---

## Task 3: Implement for `TssrunJobHandle`

**Files:**
- Modify: `src/tssrun/handle.rs`

- [ ] **Step 1: Write the failing test**

  ```rust
  #[tokio::test]
  async fn tssrun_handle_implements_jobhandlecommon() {
      use crate::handle::JobHandleCommon;
      fn _assert_impl<H: JobHandleCommon>() {}
      _assert_impl::<TssrunJobHandle>();

      let h = build_attached_handle_for_test();
      let _: Uuid = JobHandleCommon::uuid(&h);
      assert_eq!(JobHandleCommon::is_running(&h), h.snapshot().is_running());
  }
  ```

- [ ] **Step 2: Add the impl**

  ```rust
  #[async_trait::async_trait]
  impl crate::handle::JobHandleCommon for TssrunJobHandle {
      type Snapshot = TssrunJobSnapshot;

      fn uuid(&self) -> Uuid { Self::uuid(self) }
      fn jobid(&self) -> Option<u64> { Self::jobid(self) }
      fn is_running(&self) -> bool { Self::is_running(self) }
      fn is_finished(&self) -> bool { Self::is_finished(self) }
      fn exit_code(&self) -> Option<i32> { Self::exit_code(self) }

      fn snapshot(&self) -> TssrunJobSnapshot { Self::snapshot(self) }
      fn watch(&self) -> tokio::sync::watch::Receiver<TssrunJobSnapshot> { Self::watch(self) }

      async fn refresh(&self) -> anyhow::Result<TssrunJobSnapshot> {
          Self::refresh(self).await
      }

      async fn wait_terminal(
          &self,
          poll_interval: std::time::Duration,
      ) -> anyhow::Result<TssrunJobSnapshot> {
          Self::wait_terminal(self, poll_interval).await
      }
  }
  ```

- [ ] **Step 3: Run test — green**

---

## Task 4: Cross-backend integration test (`tests/job_handle_common.rs`)

**Files:**
- Create: `tests/job_handle_common.rs`

- [ ] **Step 1: Write the generic contract helper**

  ```rust
  use slurm_async_runner::JobHandleCommon;
  use std::time::Duration;

  /// Generic contract that every `JobHandleCommon` impl must satisfy.
  /// Run on a handle that has been `attached` to an in-memory store with
  /// at least one snapshot already present.
  async fn assert_handle_common_contract<H: JobHandleCommon>(handle: H) {
      // jobid Option round-trip
      let _: Option<u64> = handle.jobid();

      // snapshot getters agree with sync helpers
      let snap = handle.snapshot();
      assert_eq!(handle.is_running(), !snap.is_finished()
          .then_some(true)
          .unwrap_or(false), "is_running should agree with snapshot.is_finished");

      // refresh returns a snapshot that round-trips through serde
      let _ = handle.refresh().await;

      // wait_terminal terminates promptly when snapshot is already terminal
      if handle.is_finished() {
          let dur = Duration::from_millis(1);
          let snap = tokio::time::timeout(Duration::from_secs(1), handle.wait_terminal(dur))
              .await
              .expect("wait_terminal should return immediately on a finished handle")
              .unwrap();
          assert!(snap.is_finished());
      }
  }

  // Per-backend test harnesses live below. These build a handle from each
  // backend's test fixture (mock dispatcher returning a minimal snapshot)
  // and pass it through assert_handle_common_contract.

  #[tokio::test]
  async fn sbatch_handle_satisfies_common_contract() {
      let h = build_finished_sbatch_handle().await; // helper in this file
      assert_handle_common_contract(h).await;
  }

  #[tokio::test]
  async fn tssrun_handle_satisfies_common_contract() {
      let h = build_finished_tssrun_handle().await; // helper in this file
      assert_handle_common_contract(h).await;
  }

  // Helpers (use existing public attach / mock APIs)
  async fn build_finished_sbatch_handle() -> slurm_async_runner::SbatchJobHandle {
      // Use a `MockDispatcher` returning canned squeue/qgroup output for a
      // Completed jobid. Then call SbatchManager::attach_jobid and prime
      // is_finished by calling refresh once.
      todo!("wire to existing test fixtures")
  }

  async fn build_finished_tssrun_handle() -> slurm_async_runner::TssrunJobHandle {
      // Same idea on tssrun side. Use attach_uuid + a snapshot file
      // with finished = Some(...) pre-saved into an in-memory store.
      todo!("wire to existing test fixtures")
  }
  ```

  Replace the `todo!()` bodies with the actual fixture wiring once the helper crates / mock dispatchers are inspected. If wiring proves difficult, demote one of the two backends to a `#[ignore]`-d test with a TODO note instead of leaving `todo!()` panics in committed code.

- [ ] **Step 2: Verify it compiles + runs**

  ```bash
  cargo test --test job_handle_common --features pyo3
  ```

---

## Task 5: CHANGELOG entry

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add `### Phase 3 P3`**

  ```markdown
  ### Phase 3 P3 — `JobHandleCommon` trait

  - New `crate::handle` module exposing the `JobHandleCommon` trait. The trait surfaces the
    Phase 2 §7.1 naming convergence (5 sync getters + `snapshot` / `watch` + async `refresh` /
    `wait_terminal`) as a mechanically-checkable contract.
  - `SbatchJobHandle` and `TssrunJobHandle` both implement `JobHandleCommon` with their own
    `Snapshot` associated type.
  - The trait is **not** dyn-safe (associated type); see Phase 3 P4 for the type-erased
    `DynJobHandleCommon` companion.
  - New integration test `tests/job_handle_common.rs` runs a single contract on both backends
    via a generic helper.
  ```

---

## Task 6: Final verification

- [ ] `cargo test --features pyo3` — all green (lib + tests/)
- [ ] `cargo clippy --all-targets --features pyo3 -- -D warnings` — clean
- [ ] `cargo fmt --all --check` — clean
- [ ] `cargo doc --no-deps --features pyo3` — `JobHandleCommon` rendered with cross-references
- [ ] `uv run pytest python/tests` — unaffected, all green
- [ ] CHANGELOG `[Unreleased]` updated
- [ ] Spec checklist (`docs/superpowers/specs/2026-05-11-sbatch-phase3-design.md` §10) ticked off
- [ ] No `todo!()` left in committed code
