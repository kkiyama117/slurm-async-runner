# Phase 3 P1: tssrun handle/snapshot rename + deprecated alias

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Rename `tssrun::JobHandle` → `TssrunJobHandle` and `tssrun::JobHandleSnapshot` → `TssrunJobSnapshot` so the tssrun side becomes naming-symmetric with `SbatchJobHandle` / `SbatchJobSnapshot`. Keep both old names as `#[deprecated]` type aliases so no downstream caller breaks. Same treatment in the crate-root re-exports (`src/lib.rs:48`).

**Why now:** Phase 3 P3 introduces a `JobHandleCommon` trait whose docs and impls reference both backends side by side. The asymmetric `JobHandle` / `SbatchJobHandle` naming would be confusing in trait docs and would make `impl JobHandleCommon for JobHandle` read like a stutter. Doing the rename first means P3 can be written cleanly.

**Architecture:** Rename is mechanical: edit the `pub struct` declaration, `impl` blocks, and trait impls in `src/tssrun/handle.rs`; chase all internal references with `cargo check`; expose deprecated aliases at the **same module scope as the renamed type** so external imports keep compiling. Python pyo3 layer keeps its existing `PyJobHandleSnapshot` / `PyJobHandle` pyclass struct names (those are pyo3-internal); only the Rust-side public Rust API gets renamed.

**Spec reference:** `docs/superpowers/specs/2026-05-11-sbatch-phase3-design.md` §4.1 (rename + alias), §9.1 (kind unchanged), §9.2 (additive compat).

**Deviation from spec literal** — none anticipated.

---

## File Structure

| File | Role |
|---|---|
| `src/tssrun/handle.rs` | Rename `JobHandle` → `TssrunJobHandle`, `JobHandleSnapshot` → `TssrunJobSnapshot`. Add `#[deprecated]` `pub type` aliases at the bottom of the file (in the same module). Update doc-comments. |
| `src/tssrun/manager.rs` | Internal references (`JobHandle::from_spawn`, return types) follow the rename. Use the new names directly here — no alias needed inside the crate. |
| `src/tssrun/mod.rs` | If any `pub use ...JobHandle...` re-export exists, update to point at new names + alias. |
| `src/lib.rs` | Update `pub use tssrun::handle::{FinishedInfo, JobHandle, JobHandleSnapshot, LogLocations};` to the new names plus deprecated re-exports for the old. |
| `src/py_export/tssrun.rs` | Internal Rust references chase the rename. The pyo3 wrapper struct names (`PyJobHandle`, `PyJobHandleSnapshot`) stay as-is — those are Python-side identifiers and the tssrun.pyi names. |
| `python/slurm_async_runner/_slurm_async_runner_core/tssrun.pyi` | Python-visible class names stay (`JobHandleSnapshot`, `JobHandle`) — pyo3 binding names are independent of Rust struct names and renaming them is out of scope for P1 (covered by Phase 3 spec §6 if at all). |
| `tests/`, `python/tests/` | Update Rust test references to new names. Python tests untouched. |
| `CHANGELOG.md` | `[Unreleased]` → `### Phase 3 P1` entry (BREAKING with alias). |

---

## Task 1: Rename `JobHandleSnapshot` → `TssrunJobSnapshot`

**Files:**
- Modify: `src/tssrun/handle.rs`

- [ ] **Step 1: Write the failing test (TDD)**

  Append to the existing `#[cfg(test)] mod tests` at the bottom of `src/tssrun/handle.rs`:

  ```rust
  #[test]
  fn tssrun_job_snapshot_alias_for_jobhandlesnapshot_resolves() {
      // After P1 the new name compiles ...
      fn _assert_new_name<T: serde::Serialize>(_: &T) {}
      let snap = TssrunJobSnapshot {
          uuid: Uuid::now_v7(),
          pid: 0,
          argv: vec![],
          sent_env: Default::default(),
          cwd: None,
          started_at_unix: 0,
          log_locations: LogLocations::None,
          jobid: None,
          node: None,
          finished: None,
      };
      _assert_new_name(&snap);

      // ... and so does the deprecated alias.
      #[allow(deprecated)]
      let _: JobHandleSnapshot = snap.clone();
  }
  ```

  Run `cargo test --lib tssrun_job_snapshot_alias_for_jobhandlesnapshot_resolves` — it MUST fail to compile (`TssrunJobSnapshot` undefined).

- [ ] **Step 2: Apply the rename**

  In `src/tssrun/handle.rs`:
  1. Change `pub struct JobHandleSnapshot {` → `pub struct TssrunJobSnapshot {`
  2. Change `impl JobHandleSnapshot {` → `impl TssrunJobSnapshot {`
  3. Update doc-comments referring to `JobHandleSnapshot` (search the file).

- [ ] **Step 3: Add deprecated alias**

  Below the `impl TssrunJobSnapshot { ... }` block, add:

  ```rust
  /// Deprecated alias for [`TssrunJobSnapshot`]. Kept for one minor version
  /// to ease the rename; remove in the next major.
  #[deprecated(
      since = "0.1.0",
      note = "use TssrunJobSnapshot — Phase 3 P1 rename for naming symmetry with SbatchJobSnapshot"
  )]
  pub type JobHandleSnapshot = TssrunJobSnapshot;
  ```

  (Use whatever crate version is current in `Cargo.toml` for `since`.)

- [ ] **Step 4: Chase internal references with cargo check**

  ```bash
  cargo check --lib --features pyo3 2>&1 | grep -E 'error|warning' | head -40
  ```

  Update every internal `JobHandleSnapshot` reference inside `src/tssrun/`, `src/sbatch/`, `src/py_export/` to `TssrunJobSnapshot`. The test from Step 1 should now compile and pass.

- [ ] **Step 5: Verify**

  ```bash
  cargo test --lib --features pyo3 tssrun_job_snapshot_alias_for_jobhandlesnapshot_resolves -- --nocapture
  ```

---

## Task 2: Rename `JobHandle` → `TssrunJobHandle`

**Files:**
- Modify: `src/tssrun/handle.rs`, `src/tssrun/manager.rs`

- [ ] **Step 1: Write the failing test (TDD)**

  Append:

  ```rust
  #[tokio::test]
  async fn tssrun_job_handle_alias_for_jobhandle_resolves() {
      // After P2 the new name compiles for type annotations.
      // We don't construct one here (requires a spawned child) — type-only assertion is enough.
      fn _assert_send_sync<T: Send + Sync>() {}
      _assert_send_sync::<TssrunJobHandle>();

      #[allow(deprecated)]
      fn _alias_assert<T: Send + Sync>() {}
      #[allow(deprecated)]
      _alias_assert::<JobHandle>();
  }
  ```

  Compile fails (`TssrunJobHandle` undefined).

- [ ] **Step 2: Apply the rename**

  In `src/tssrun/handle.rs`:
  1. Change `pub struct JobHandle {` → `pub struct TssrunJobHandle {`
  2. Change every `impl JobHandle {` / `impl SomeTrait for JobHandle` → `TssrunJobHandle`.
  3. Update doc-comments and module-level docs at the top of the file.

- [ ] **Step 3: Add deprecated alias**

  Below the last `impl TssrunJobHandle { ... }` block, add:

  ```rust
  /// Deprecated alias for [`TssrunJobHandle`]. Kept for one minor version.
  #[deprecated(
      since = "0.1.0",
      note = "use TssrunJobHandle — Phase 3 P1 rename for naming symmetry with SbatchJobHandle"
  )]
  pub type JobHandle = TssrunJobHandle;
  ```

- [ ] **Step 4: Update `src/tssrun/manager.rs`**

  Replace internal `JobHandle::from_spawn`, return-type `JobHandle`, etc. with `TssrunJobHandle`. Use grep:

  ```bash
  rg '\bJobHandle\b' src/tssrun/ src/py_export/
  ```

  Each hit either becomes `TssrunJobHandle` (in-crate) or stays `JobHandle` if it is genuinely the deprecated alias being intentionally tested.

- [ ] **Step 5: Verify**

  ```bash
  cargo test --lib --features pyo3 tssrun_job_handle_alias_for_jobhandle_resolves -- --nocapture
  ```

---

## Task 3: Update crate root re-exports (`src/lib.rs`)

**Files:**
- Modify: `src/lib.rs`

- [ ] **Step 1: Replace the existing re-export line**

  Find:
  ```rust
  pub use tssrun::handle::{FinishedInfo, JobHandle, JobHandleSnapshot, LogLocations};
  ```

  Replace with:
  ```rust
  pub use tssrun::handle::{FinishedInfo, LogLocations, TssrunJobHandle, TssrunJobSnapshot};
  // Deprecated re-exports (Phase 3 P1 rename); remove next major.
  #[allow(deprecated)]
  pub use tssrun::handle::{JobHandle, JobHandleSnapshot};
  ```

- [ ] **Step 2: Sanity-check downstream usage**

  ```bash
  rg 'use\s+slurm_async_runner::(JobHandle|JobHandleSnapshot)' tests/
  rg 'use\s+slurm_async_runner::(TssrunJobHandle|TssrunJobSnapshot)' tests/
  ```

  Update integration test imports to the new names so the deprecated re-exports do not silently mask test rot.

- [ ] **Step 3: cargo build / clippy / fmt all pass**

  ```bash
  cargo build --all-features
  cargo clippy --all-targets --features pyo3 -- -D warnings
  cargo fmt --all --check
  ```

  Any `deprecated` warnings inside the crate itself (e.g. someone wrote `JobHandle` instead of `TssrunJobHandle`) need to be fixed at the source — the alias is for **downstream users**, not internal code.

---

## Task 4: Update Python pyo3 layer (Rust-side rename only)

**Files:**
- Modify: `src/py_export/tssrun.rs`

- [ ] **Step 1: Replace internal Rust references**

  ```bash
  rg '\b(JobHandle|JobHandleSnapshot)\b' src/py_export/tssrun.rs
  ```

  Each hit that names the Rust struct (e.g. `let h: JobHandle = ...`, `Py<JobHandle>`) becomes `TssrunJobHandle` / `TssrunJobSnapshot`.

  Each hit that is the **pyo3-side pyclass attribute** (e.g. `#[pyclass(name = "JobHandleSnapshot")]`) stays — Python-visible names are out of scope for P1.

- [ ] **Step 2: Verify pyo3 compile + smoke**

  ```bash
  uv run maturin develop
  uv run pytest python/tests -x
  ```

  Python tests reference `JobHandleSnapshot` (pyclass name) and continue to work because the pyo3 binding name is unchanged.

---

## Task 5: CHANGELOG entry

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add `### Phase 3 P1` section under `[Unreleased]`**

  ```markdown
  ### Phase 3 P1 — tssrun naming symmetry

  **Breaking (with alias)**: `tssrun::JobHandle` and `tssrun::JobHandleSnapshot` are renamed to
  `TssrunJobHandle` and `TssrunJobSnapshot` for naming symmetry with `SbatchJobHandle` /
  `SbatchJobSnapshot`. Deprecated `pub type` aliases preserve compilation; downstream callers
  can migrate at their leisure. Crate-root re-exports (`crate::JobHandle`, `crate::JobHandleSnapshot`)
  remain available via `#[allow(deprecated)]` re-export.

  Python pyo3 binding names (`JobHandleSnapshot`) are unchanged.

  Why: Phase 3 P3 introduces `JobHandleCommon` trait; symmetric Rust struct naming makes the
  trait docs and impls read cleanly.
  ```

---

## Task 6: Final verification

- [ ] `cargo test --lib --features pyo3` — all green
- [ ] `cargo clippy --all-targets --features pyo3 -- -D warnings` — clean
- [ ] `cargo fmt --all --check` — clean
- [ ] `uv run pytest python/tests` — all green
- [ ] `cargo doc --features pyo3 --no-deps` — builds; new types documented
- [ ] No internal usage of the deprecated alias (any in-crate `cargo check` warning about it is a hand-off bug — fix at source)
- [ ] CHANGELOG `[Unreleased]` updated
- [ ] Spec checklist (`docs/superpowers/specs/2026-05-11-sbatch-phase3-design.md` §10) ticked off
