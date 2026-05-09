# Slurm Vocabulary Migration to SAR — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move all slurm vocabulary types (`ResourceSpec`, `JobTimeLimit`, `JobPartition`, `Memory`, `SlurmJobConfig`, `DependencyType`, `JobStatus`, `ArraySpec`) from `gaussian_job_shared` to `slurm_async_runner`, reverse the Cargo dependency direction, and adopt the Polars-style "Pyclass Single Owner" architecture rule to permanently eliminate the cross-cdylib type identity regression seen in Phase C2.

**Architecture:** SAR becomes the canonical owner of slurm vocabulary at both the Rust and Python (pyclass) levels. shared2 depends on SAR with `default-features = false`, so SAR's pyclass impls never link into shared2's cdylib. Where shared2 needs to interact with SAR pyclass instances at the Python boundary, it uses duck-typed `FromPyObject` newtypes (no pyclass) and `Py::import` for return paths — the pattern proven by `pyo3-polars` over 5 years.

**Tech Stack:** Rust 2024, pyo3 0.28 (single `pyo3` feature, no split), maturin 1.13, pyo3-stub-gen, two separate git repos (`slurm-async-runner` worktree at `/home/kiyama/programs/research/GAUSSIAN_repo_packages/slurm-async-runner2/.dmux/worktrees/merge-tssrun-struct`, `gaussian-job-shared2` at `/home/kiyama/programs/research/GAUSSIAN_repo_packages/gaussian-job-shared2`), shared2 source SHA `299d3e80d73b1533e4dd7c5f4fdde000c7be1aae` on branch `relax-resource-spec-and-feature-split` (this is the SHA we copy from — it includes A1+A2 partial CPU and A3 kwargs improvements).

**Spec:** `docs/superpowers/specs/2026-05-10-slurm-vocab-migration-and-pyclass-ownership-design.md`

---

## File Structure Map

### SAR — files created (graft from shared2)

```
src/entities.rs                                            ← NEW: mod root
src/entities/slurm.rs                                      ← graft
src/entities/slurm/sbatch_options.rs                       ← graft
src/entities/slurm/sbatch_options/resource_spec.rs         ← graft (A1+A2)
src/entities/slurm/sbatch_options/time_limit.rs            ← graft
src/entities/slurm/sbatch_options/array_spec.rs            ← graft
src/entities/slurm/sbatch_options/dependency.rs            ← graft
src/entities/slurm/status.rs                               ← graft
src/py_export/entities.rs                                  ← NEW: mod root
src/py_export/entities/slurm/mod.rs                        ← graft
src/py_export/entities/slurm/sbatch_options.rs             ← graft
src/py_export/entities/slurm/sbatch_options/resource_spec.rs ← graft (A3)
src/py_export/entities/slurm/sbatch_options/time_limit.rs  ← graft
src/py_export/entities/slurm/sbatch_options/array_spec.rs  ← graft
src/py_export/entities/slurm/sbatch_options/dependency.rs  ← graft
src/py_export/entities/slurm/sbatch_options/config.rs      ← graft
src/py_export/entities/slurm/status.rs                     ← graft
python/slurm_async_runner/_slurm_async_runner_core/entities/__init__.pyi          ← NEW
python/slurm_async_runner/_slurm_async_runner_core/entities/slurm/__init__.pyi    ← graft
python/slurm_async_runner/_slurm_async_runner_core/entities/slurm/sbatch_options/__init__.pyi ← graft
python/slurm_async_runner/_slurm_async_runner_core/entities/slurm/status/__init__.pyi         ← graft
```

### SAR — files modified

```
Cargo.toml                                                 ← drop shared2 dep, collapse features
src/lib.rs                                                 ← register entities mod, rewire re-exports
src/tssrun/cmd.rs                                          ← rewire imports (gaussian_job_shared:: → crate::)
src/py_export/mod.rs                                       ← register entities pymodule
src/py_export/tssrun.rs                                    ← rewire imports
python/slurm_async_runner/_slurm_async_runner_core/manager.pyi  ← rewire JobStatus import
python/slurm_async_runner/_slurm_async_runner_core/runner.pyi   ← rewire JobStatus import
```

### shared2 — files modified

```
Cargo.toml                                                 ← add SAR dep (default-features=false), collapse features
src/lib.rs                                                 ← drop entities::slurm mod
src/entities.rs (or entities/mod.rs)                       ← drop slurm module declaration
src/entities/workflow.rs                                   ← rewire DependencyType, SlurmJobConfig imports
src/entities/workflow/job.rs                               ← rewire DependencyType import
src/config/common.rs                                       ← rewire SlurmJobConfig import
src/py_export/mod.rs (or entities mod)                     ← drop slurm py_export
python/tests/test_all.py                                   ← update imports
```

### shared2 — files deleted

```
src/entities/slurm.rs
src/entities/slurm/sbatch_options.rs
src/entities/slurm/sbatch_options/resource_spec.rs
src/entities/slurm/sbatch_options/time_limit.rs
src/entities/slurm/sbatch_options/array_spec.rs
src/entities/slurm/sbatch_options/dependency.rs
src/entities/slurm/status.rs
src/py_export/entities/slurm/**     (entire subtree)
python/gaussian_job_shared/_gaussian_job_shared_core/entities/slurm/**  (entire subtree)
```

---

## Phase A — SAR self-sufficient (does not need shared2)

After Phase A, SAR builds, tests, and produces a wheel without any reference to `gaussian_job_shared`. shared2 will be temporarily broken until Phase B. Each task commits at the end so Phase A can be paused mid-flight.

### Task 1: Create SAR `entities/` skeleton and graft Rust source from shared2

**Files:**
- Create: `src/entities.rs`
- Create: `src/entities/slurm.rs`
- Create: `src/entities/slurm/sbatch_options.rs`
- Create: `src/entities/slurm/sbatch_options/resource_spec.rs`
- Create: `src/entities/slurm/sbatch_options/time_limit.rs`
- Create: `src/entities/slurm/sbatch_options/array_spec.rs`
- Create: `src/entities/slurm/sbatch_options/dependency.rs`
- Create: `src/entities/slurm/status.rs`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p src/entities/slurm/sbatch_options
```

- [ ] **Step 2: Graft Rust source verbatim from shared2 sha 299d3e8**

Run from the SAR worktree root:

```bash
SHARED2=/home/kiyama/programs/research/GAUSSIAN_repo_packages/gaussian-job-shared2
SHA=299d3e80d73b1533e4dd7c5f4fdde000c7be1aae
for f in \
  src/entities/slurm.rs \
  src/entities/slurm/sbatch_options.rs \
  src/entities/slurm/sbatch_options/resource_spec.rs \
  src/entities/slurm/sbatch_options/time_limit.rs \
  src/entities/slurm/sbatch_options/array_spec.rs \
  src/entities/slurm/sbatch_options/dependency.rs \
  src/entities/slurm/status.rs; do
  git -C "$SHARED2" show "$SHA:$f" > "$f"
done
```

- [ ] **Step 3: Create `src/entities.rs` (mod root)**

Write `src/entities.rs` with this content:

```rust
//! Domain entity types for slurm-async-runner.
//!
//! This module is the canonical home for slurm vocabulary
//! (`ResourceSpec`, `JobTimeLimit`, `Memory`, `SlurmJobConfig`,
//! `JobStatus`, etc.). Downstream crates that need to consume
//! these types at the Rust level should depend on this crate
//! with `default-features = false` to avoid linking SAR's pyclass
//! impls into their own cdylib (see the Pyclass Single Owner rule
//! in `Cargo.toml`).

pub mod slurm;
```

- [ ] **Step 4: Verify the grafted files reference `crate::entities::slurm::...`**

Run:

```bash
grep -rn "use crate::entities::slurm" src/entities/slurm/ | head -10
```

Expected: paths like `use crate::entities::slurm::sbatch_options::...` already present (the grafted source uses these absolute paths because shared2's mod tree at `entities::slurm::*` matches SAR's new tree exactly).

- [ ] **Step 5: Wire `entities` mod into `src/lib.rs` and run cargo check**

In `src/lib.rs`, add the line **before** the existing `pub mod py_export;` line:

```rust
pub mod entities;
```

Then run:

```bash
cargo check --no-default-features 2>&1 | tail -20
```

Expected: compile error about unresolved imports of `gaussian_job_shared` in the existing `tssrun::cmd` and `py_export::tssrun` (those are rewired in Task 4 / Task 6). The `entities` mod itself compiles clean.

- [ ] **Step 6: Commit**

```bash
git add src/entities.rs src/entities/slurm.rs src/entities/slurm/sbatch_options.rs \
  src/entities/slurm/sbatch_options/resource_spec.rs \
  src/entities/slurm/sbatch_options/time_limit.rs \
  src/entities/slurm/sbatch_options/array_spec.rs \
  src/entities/slurm/sbatch_options/dependency.rs \
  src/entities/slurm/status.rs \
  src/lib.rs
git commit -m "feat(entities): graft slurm vocab Rust source from shared2 (sha 299d3e8)"
```

The commit will fail the clippy hook because `tssrun::cmd` still imports from shared2. That's expected — proceed to the next task to clear those, then re-attempt the commit. To bridge: combine Task 1 + Tasks 4-6 into a single coordinated commit if your hook configuration blocks intermediate states.

---

### Task 2: Add `ResourceSpec::from_parts` public Rust constructor

The pyclass `__new__` in shared2 contained CPU/GPU mutual-exclusion validation. We extract that logic into a pyclass-free Rust `pub fn` so that both the SAR pyclass wrapper and any future bridge type can use it.

**Files:**
- Modify: `src/entities/slurm/sbatch_options/resource_spec.rs`
- Test: same file (`#[cfg(test)] mod tests`)

- [ ] **Step 1: Inspect existing `__new__` validation to know what to lift**

Read shared2's pyclass `__new__` body:

```bash
git -C /home/kiyama/programs/research/GAUSSIAN_repo_packages/gaussian-job-shared2 \
  show 299d3e8:src/py_export/entities/slurm/sbatch_options/resource_spec.rs \
  | sed -n '/fn new/,/^    }/p'
```

Copy the validation logic into the Rust constructor below, but without any pyo3 types — pure Rust.

- [ ] **Step 2: Write the failing test**

Append to `src/entities/slurm/sbatch_options/resource_spec.rs` (inside the existing `#[cfg(test)] mod tests`):

```rust
#[test]
fn from_parts_all_none_yields_default_cpu() {
    let spec = ResourceSpec::from_parts(None, None, None, None, None).unwrap();
    assert_eq!(spec, ResourceSpec::Cpu(ResourceSpecCPU::default()));
}

#[test]
fn from_parts_cpu_partial_keeps_other_fields_none() {
    let spec = ResourceSpec::from_parts(Some(4), None, None, None, None).unwrap();
    let ResourceSpec::Cpu(cpu) = spec else {
        panic!("expected Cpu variant");
    };
    assert_eq!(cpu.processes, NonZeroU32::new(4));
    assert!(cpu.threads.is_none());
}

#[test]
fn from_parts_gpu_only_yields_gpu_variant() {
    let spec = ResourceSpec::from_parts(None, None, None, None, Some(2)).unwrap();
    assert!(matches!(spec, ResourceSpec::Gpu(_)));
}

#[test]
fn from_parts_mixed_cpu_and_gpu_is_rejected() {
    let err = ResourceSpec::from_parts(Some(4), None, None, None, Some(2)).unwrap_err();
    assert!(err.to_string().contains("mutually exclusive"));
}

#[test]
fn from_parts_zero_processes_is_rejected() {
    let err = ResourceSpec::from_parts(Some(0), None, None, None, None).unwrap_err();
    assert!(err.to_string().contains("must be positive"));
}
```

- [ ] **Step 3: Run test to verify it fails**

```bash
cargo test --no-default-features --lib resource_spec::tests::from_parts 2>&1 | tail -10
```

Expected: error[E0599]: no function or associated item named `from_parts` found for enum `ResourceSpec`.

- [ ] **Step 4: Implement `ResourceSpec::from_parts`**

Add to the same file, in the `impl ResourceSpec { ... }` block (or create one if absent):

```rust
impl ResourceSpec {
    /// Construct a `ResourceSpec` from individual KUDPC `--rsc` keys.
    ///
    /// All five arguments are optional. `gpus` is mutually exclusive
    /// with the four CPU keys. When all five are `None`, returns
    /// `ResourceSpec::Cpu(ResourceSpecCPU::default())` (an empty CPU
    /// spec is meaningful — the scheduler's own default applies to
    /// every omitted field).
    pub fn from_parts(
        processes: Option<u32>,
        threads: Option<u32>,
        cores: Option<u32>,
        memory: Option<Memory>,
        gpus: Option<u32>,
    ) -> Result<Self, ResourceSpecError> {
        let any_cpu = processes.is_some() || threads.is_some() || cores.is_some() || memory.is_some();
        if gpus.is_some() && any_cpu {
            return Err(ResourceSpecError::MixedCpuGpu);
        }
        if let Some(g) = gpus {
            let g = NonZeroU32::new(g).ok_or(ResourceSpecError::ZeroValue("gpus"))?;
            return Ok(ResourceSpec::Gpu(ResourceSpecGPU { gpus: g }));
        }
        let processes = processes
            .map(|v| NonZeroU32::new(v).ok_or(ResourceSpecError::ZeroValue("processes")))
            .transpose()?;
        let threads = threads
            .map(|v| NonZeroU32::new(v).ok_or(ResourceSpecError::ZeroValue("threads")))
            .transpose()?;
        let cores = cores
            .map(|v| NonZeroU32::new(v).ok_or(ResourceSpecError::ZeroValue("cores")))
            .transpose()?;
        Ok(ResourceSpec::Cpu(ResourceSpecCPU {
            processes,
            threads,
            cores,
            memory,
        }))
    }
}
```

If `ResourceSpecError` does not yet contain `MixedCpuGpu` and `ZeroValue` variants, add them. Inspect with:

```bash
grep -n "enum ResourceSpecError\|^    [A-Z]" src/entities/slurm/sbatch_options/resource_spec.rs | head -20
```

If the variants are missing, add to the existing `enum ResourceSpecError`:

```rust
#[error("CPU keys (p/t/c/m) and GPU key (g) are mutually exclusive")]
MixedCpuGpu,

#[error("`{0}` must be positive (non-zero)")]
ZeroValue(&'static str),
```

- [ ] **Step 5: Run test to verify it passes**

```bash
cargo test --no-default-features --lib resource_spec::tests::from_parts 2>&1 | tail -10
```

Expected: 5 tests passed.

- [ ] **Step 6: Commit**

```bash
git add src/entities/slurm/sbatch_options/resource_spec.rs
git commit -m "feat(entities): add public ResourceSpec::from_parts constructor"
```

---

### Task 3: Graft pyclass wrappers (`src/py_export/entities/slurm/**`)

**Files:**
- Create: `src/py_export/entities.rs`
- Create: `src/py_export/entities/slurm/mod.rs`
- Create: `src/py_export/entities/slurm/sbatch_options.rs`
- Create: `src/py_export/entities/slurm/sbatch_options/resource_spec.rs`
- Create: `src/py_export/entities/slurm/sbatch_options/time_limit.rs`
- Create: `src/py_export/entities/slurm/sbatch_options/array_spec.rs`
- Create: `src/py_export/entities/slurm/sbatch_options/dependency.rs`
- Create: `src/py_export/entities/slurm/sbatch_options/config.rs`
- Create: `src/py_export/entities/slurm/status.rs`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p src/py_export/entities/slurm/sbatch_options
```

- [ ] **Step 2: Graft pyclass source verbatim from shared2 sha 299d3e8**

```bash
SHARED2=/home/kiyama/programs/research/GAUSSIAN_repo_packages/gaussian-job-shared2
SHA=299d3e80d73b1533e4dd7c5f4fdde000c7be1aae
for f in \
  src/py_export/entities/slurm/mod.rs \
  src/py_export/entities/slurm/sbatch_options.rs \
  src/py_export/entities/slurm/sbatch_options/resource_spec.rs \
  src/py_export/entities/slurm/sbatch_options/time_limit.rs \
  src/py_export/entities/slurm/sbatch_options/array_spec.rs \
  src/py_export/entities/slurm/sbatch_options/dependency.rs \
  src/py_export/entities/slurm/sbatch_options/config.rs \
  src/py_export/entities/slurm/status.rs; do
  git -C "$SHARED2" show "$SHA:$f" > "$f"
done
```

- [ ] **Step 3: Create `src/py_export/entities.rs` (pymodule entry mod root)**

Write:

```rust
//! Pyclass wrappers for SAR's domain entity types. Mirrors the
//! Rust-side `crate::entities` tree.

pub mod slurm;
```

- [ ] **Step 4: Wire `entities` into `src/py_export/mod.rs`**

Inspect first:

```bash
grep -n "pub mod\|mod \|pymodule_export" src/py_export/mod.rs
```

In `src/py_export/mod.rs`, add the module declaration near the other `pub mod` lines:

```rust
pub mod entities;
```

And export the slurm submodule from the crate's outermost `#[pymodule]`. Find the existing `#[pymodule]` block and add a `m.add_wrapped(wrap_pymodule!(entities::slurm::slurm))?;` (or whatever name `slurm/mod.rs` exposes — verify by reading `src/py_export/entities/slurm/mod.rs` Step 2 output).

- [ ] **Step 5: Run cargo check**

```bash
cargo check 2>&1 | tail -30
```

Expected: errors only in `src/py_export/tssrun.rs` (still uses `gaussian_job_shared::py_export::...`). The grafted `entities/slurm/**` compiles clean against `crate::entities::slurm::...`.

- [ ] **Step 6: Commit**

```bash
git add src/py_export/entities.rs src/py_export/entities/ src/py_export/mod.rs
git commit -m "feat(py_export): graft slurm pyclass wrappers from shared2 (sha 299d3e8)"
```

(If clippy hook blocks due to `tssrun.rs` still being broken, combine with Tasks 4-6.)

---

### Task 4: Rewire `src/tssrun/cmd.rs` to use crate-local types

**Files:**
- Modify: `src/tssrun/cmd.rs:21,108`

- [ ] **Step 1: Replace shared2 imports with crate-local**

Edit `src/tssrun/cmd.rs`:

Replace line 21:

```rust
use gaussian_job_shared::entities::slurm::{JobPartition, JobTimeLimit, ResourceSpec};
```

with:

```rust
use crate::entities::slurm::{JobPartition, JobTimeLimit, ResourceSpec};
```

Replace line 108 (inside `#[cfg(test)] mod tests`):

```rust
    use gaussian_job_shared::entities::slurm::{ResourceSpecCPU, ResourceSpecGPU};
```

with:

```rust
    use crate::entities::slurm::{ResourceSpecCPU, ResourceSpecGPU};
```

- [ ] **Step 2: Run cargo check**

```bash
cargo check --lib 2>&1 | tail -20
```

Expected: errors now only in `src/lib.rs` (re-exports) and `src/py_export/tssrun.rs`.

- [ ] **Step 3: Commit (deferred — combine with next task)**

The hook will fail. Move to Task 5; commit them together.

---

### Task 5: Rewire `src/lib.rs` re-exports

**Files:**
- Modify: `src/lib.rs:15,39-45`

- [ ] **Step 1: Replace the two shared2 re-export blocks with crate-local re-exports**

In `src/lib.rs`, replace the line:

```rust
pub use gaussian_job_shared::entities::slurm::status::{JobReason, JobState, JobStatus};
```

with:

```rust
pub use crate::entities::slurm::status::{JobReason, JobState, JobStatus};
```

And replace the entire block (lines 39–45):

```rust
pub use gaussian_job_shared::entities::slurm::{
    JobPartition, JobTimeLimit, Memory, MemoryUnit, ResourceSpec, ResourceSpecCPU, ResourceSpecGPU,
};
```

with:

```rust
pub use crate::entities::slurm::{
    JobPartition, JobTimeLimit, Memory, MemoryUnit, ResourceSpec, ResourceSpecCPU, ResourceSpecGPU,
};
```

Also delete or rewrite the surrounding doc comments that mention `gaussian_job_shared` — replace the multi-line comment block above each `pub use` with a single-line comment that just says what the re-export is for.

- [ ] **Step 2: Run cargo check**

```bash
cargo check --lib 2>&1 | tail -10
```

Expected: only `src/py_export/tssrun.rs` still has shared2 imports.

---

### Task 6: Rewire `src/py_export/tssrun.rs`

**Files:**
- Modify: `src/py_export/tssrun.rs:17,18,427,429`

- [ ] **Step 1: Replace shared2 pyclass imports with crate-local**

Edit `src/py_export/tssrun.rs`:

Replace lines 17–18:

```rust
use gaussian_job_shared::py_export::entities::slurm::sbatch_options::resource_spec::PyResourceSpec;
use gaussian_job_shared::py_export::entities::slurm::sbatch_options::time_limit::PyJobTimeLimit;
```

with:

```rust
use crate::py_export::entities::slurm::sbatch_options::resource_spec::PyResourceSpec;
use crate::py_export::entities::slurm::sbatch_options::time_limit::PyJobTimeLimit;
```

Replace lines 427 / 429 (inside the test module) the same way. Verify with:

```bash
grep -n "gaussian_job_shared" src/py_export/tssrun.rs
```

Expected output: empty.

- [ ] **Step 2: Run cargo check**

```bash
cargo check --all-targets 2>&1 | tail -10
```

Expected: clean. Library + tests compile.

- [ ] **Step 3: Run tests**

```bash
cargo test --lib 2>&1 | tail -20
```

Expected: all tests pass (including `from_parts` from Task 2 and the existing tssrun tests).

- [ ] **Step 4: Commit Tasks 4–6 together**

```bash
git add src/tssrun/cmd.rs src/lib.rs src/py_export/tssrun.rs
git commit -m "refactor(tssrun): rewire slurm vocab imports from shared2 to crate-local"
```

---

### Task 7: Drop `gaussian_job_shared` Cargo dep, collapse pyo3 feature surface

**Files:**
- Modify: `Cargo.toml`

- [ ] **Step 1: Inspect current state**

```bash
grep -n "gaussian_job_shared\|pyo3-types\|pymodule-entry" Cargo.toml
```

Note current line numbers for the dep block and the feature block.

- [ ] **Step 2: Remove the `gaussian_job_shared` dependency line**

Delete the entire `gaussian_job_shared = { ... }` line from `[dependencies]`.

- [ ] **Step 3: Collapse pyo3 feature split**

Find the `[features]` table. Replace whatever currently exists (per `2026-05-09-merge-tssrun-resourcespec-design.md` Phase A4 / B1, this is some `pyo3-types` / `pymodule-entry` split) with:

```toml
[features]
default = ["pyo3", "stub_gen"]

# Canonical-wheel build: enables BOTH the pyclass impls and the
# pymodule entry. This is the only path under which SAR's pyclass
# code is compiled.
#
# ARCHITECTURE RULE — Pyclass Single Owner:
# Downstream crates MUST NOT enable this feature. Use
# `default-features = false` to consume Rust types only. Each
# pyclass type has exactly one owner cdylib in the dependency
# graph; duplicating pyclass impls into multiple cdylibs produces
# distinct Python type objects with identical __module__ strings,
# breaking isinstance checks. See
# docs/superpowers/specs/2026-05-10-slurm-vocab-migration-and-pyclass-ownership-design.md
# §2 for the full rule, and docs/error.md for the regression that
# motivated it.
pyo3 = [
    "dep:pyo3",
    "pyo3-async-runtimes",
    "pyo3-log",
    "pythonize",
    "pyo3-stub-gen",
]

stub_gen = ["pyo3"]
```

- [ ] **Step 4: Run cargo build with all features**

```bash
cargo build --all-features 2>&1 | tail -10
cargo clippy --all-targets --all-features -- -D warnings 2>&1 | tail -10
```

Expected: clean. No remaining reference to `gaussian_job_shared`.

- [ ] **Step 5: Run full test suite**

```bash
cargo test --all-features 2>&1 | tail -10
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml
git commit -m "deps!: drop gaussian_job_shared dep, collapse pyo3 feature to single owner"
```

---

### Task 8: Graft Python type stubs into SAR

**Files:**
- Create: `python/slurm_async_runner/_slurm_async_runner_core/entities/__init__.pyi`
- Create: `python/slurm_async_runner/_slurm_async_runner_core/entities/slurm/__init__.pyi`
- Create: `python/slurm_async_runner/_slurm_async_runner_core/entities/slurm/sbatch_options/__init__.pyi`
- Create: `python/slurm_async_runner/_slurm_async_runner_core/entities/slurm/status/__init__.pyi`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p python/slurm_async_runner/_slurm_async_runner_core/entities/slurm/sbatch_options
mkdir -p python/slurm_async_runner/_slurm_async_runner_core/entities/slurm/status
```

- [ ] **Step 2: Graft stubs from shared2 sha 299d3e8**

```bash
SHARED2=/home/kiyama/programs/research/GAUSSIAN_repo_packages/gaussian-job-shared2
SHA=299d3e80d73b1533e4dd7c5f4fdde000c7be1aae
for src in \
  python/gaussian_job_shared/_gaussian_job_shared_core/entities/slurm/__init__.pyi \
  python/gaussian_job_shared/_gaussian_job_shared_core/entities/slurm/sbatch_options/__init__.pyi \
  python/gaussian_job_shared/_gaussian_job_shared_core/entities/slurm/status/__init__.pyi; do
  dst=$(echo "$src" | sed 's|gaussian_job_shared/_gaussian_job_shared_core|slurm_async_runner/_slurm_async_runner_core|')
  git -C "$SHARED2" show "$SHA:$src" > "$dst"
done
```

- [ ] **Step 3: Create the `entities/__init__.pyi` mod root**

Write `python/slurm_async_runner/_slurm_async_runner_core/entities/__init__.pyi`:

```python
from . import slurm

__all__ = [
    "slurm",
]
```

- [ ] **Step 4: Update `manager.pyi` and `runner.pyi` import paths**

Edit `python/slurm_async_runner/_slurm_async_runner_core/manager.pyi` line 14:

Replace:
```python
from gaussian_job_shared._gaussian_job_shared_core.entities.slurm.status import (
```
with:
```python
from slurm_async_runner._slurm_async_runner_core.entities.slurm.status import (
```

Edit `python/slurm_async_runner/_slurm_async_runner_core/runner.pyi` line 11 the same way.

- [ ] **Step 5: Verify no remaining shared2 references in stubs**

```bash
grep -rn "gaussian_job_shared" python/ 2>&1 | grep -v "^Binary file"
```

Expected: empty.

- [ ] **Step 6: Commit**

```bash
git add python/slurm_async_runner/_slurm_async_runner_core/entities \
        python/slurm_async_runner/_slurm_async_runner_core/manager.pyi \
        python/slurm_async_runner/_slurm_async_runner_core/runner.pyi
git commit -m "feat(stubs): graft slurm Python type stubs into SAR"
```

---

### Task 9: Build SAR wheel and verify Python module structure

**Files:**
- Test: smoke verification (no source files written)

- [ ] **Step 1: Build the wheel**

```bash
cd /home/kiyama/programs/research/GAUSSIAN_repo_packages/slurm-async-runner2/.dmux/worktrees/merge-tssrun-struct
maturin build --release 2>&1 | tail -10
```

Expected: `Built wheel for ABI3 Python ... at <path>.whl`. Capture the wheel path.

- [ ] **Step 2: Install in a fresh venv and import-check**

```bash
python -m venv /tmp/sar_phaseA_check
/tmp/sar_phaseA_check/bin/pip install --quiet target/wheels/slurm_async_runner-*.whl
/tmp/sar_phaseA_check/bin/python -c "
import slurm_async_runner._slurm_async_runner_core as core
from slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options import (
    ResourceSpec, ResourceSpecCPU, ResourceSpecGPU, Memory, MemoryUnit, JobTimeLimit,
)
print('ResourceSpec:', ResourceSpec(processes=4, memory=Memory(8, MemoryUnit.GiB)))
print('ResourceSpec from_str:', ResourceSpec.from_str('p=4:t=2:c=8:m=4G'))
print('JobTimeLimit:', JobTimeLimit('01:30:00'))
print('GPU spec:', ResourceSpec(gpus=2))
"
```

Expected: prints all four lines without error. SAR's slurm vocab is now self-contained.

- [ ] **Step 3: Verify no `PyInit__core` collision**

```bash
unzip -p target/wheels/slurm_async_runner-*.whl '*/_slurm_async_runner_core*.so' \
  | nm -D --defined-only - 2>&1 | grep PyInit
```

Expected: only `PyInit__slurm_async_runner_core`. No bare `PyInit__core`.

- [ ] **Step 4: No commit (verification only)**

---

### Task 10: Push SAR Phase-A branch

**Files:** none

- [ ] **Step 1: Push the worktree branch**

```bash
git push origin merge-tssrun-struct 2>&1 | tail -5
```

- [ ] **Step 2: Capture HEAD commit for shared2 pinning**

```bash
git rev-parse HEAD
```

Record this SHA — Phase B Task 11 will pin shared2 to it.

---

## Phase B — shared2 catches up (consumer side)

Phase B works in the **separate** repository at `/home/kiyama/programs/research/GAUSSIAN_repo_packages/gaussian-job-shared2`. Create a new branch there. After Phase B completes, both repos build and test independently.

### Task 11: Create shared2 branch and pin SAR dep

**Repo:** `/home/kiyama/programs/research/GAUSSIAN_repo_packages/gaussian-job-shared2`
**Files:** `Cargo.toml`

- [ ] **Step 1: Create branch from main**

```bash
cd /home/kiyama/programs/research/GAUSSIAN_repo_packages/gaussian-job-shared2
git fetch origin
git checkout -b slurm-vocab-extraction origin/main
```

- [ ] **Step 2: Add SAR dep with `default-features = false`**

In `gaussian-job-shared2/Cargo.toml`, find `[dependencies]` and add:

```toml
slurm_async_runner = { git = "https://github.com/kkiyama117/slurm-async-runner.git", branch = "merge-tssrun-struct", default-features = false }
```

(Replace `branch = "..."` with `rev = "<SHA from Task 10 Step 2>"` for a stable pin.)

- [ ] **Step 3: Run cargo fetch to confirm resolvability**

```bash
cargo fetch 2>&1 | tail -5
```

Expected: SAR resolves and downloads. No `gaussian_job_shared` dep cycle warning (SAR's own Cargo.toml no longer depends on shared2).

- [ ] **Step 4: Commit the dep addition only (not the rewires yet)**

```bash
git add Cargo.toml
git commit -m "deps: add slurm_async_runner dep with default-features=false"
```

---

### Task 12: Rewire shared2 internal users of slurm types

**Repo:** `gaussian-job-shared2`
**Files:**
- Modify: `src/entities/workflow.rs:74,237`
- Modify: `src/entities/workflow/job.rs:7,237`
- Modify: `src/config/common.rs:1`

- [ ] **Step 1: Inspect current import usage**

```bash
grep -rn "use crate::entities::slurm" src/ | grep -v "^src/entities/slurm" | grep -v "^src/py_export/entities/slurm"
```

This lists every site that needs rewiring.

- [ ] **Step 2: Rewire imports**

For each file shown in Step 1, replace:

```rust
use crate::entities::slurm::{...};
```

with:

```rust
use slurm_async_runner::entities::slurm::{...};
```

The four expected sites (per the prior inventory) are:
- `src/config/common.rs:1` → `use slurm_async_runner::entities::slurm::SlurmJobConfig;`
- `src/entities/workflow.rs:74` (inside test module) → `use slurm_async_runner::entities::slurm::{DependencyType, SlurmJobConfig};`
- `src/entities/workflow/job.rs:7` → `use slurm_async_runner::entities::slurm::DependencyType;`
- `src/entities/workflow/job.rs:237` (inside test module) → `use slurm_async_runner::entities::slurm::SlurmJobConfig;`

- [ ] **Step 3: Run cargo check (this MUST fail for now)**

```bash
cargo check --no-default-features 2>&1 | tail -20
```

Expected: errors because `crate::entities::slurm` still resolves (the slurm subtree is still present and now duplicates SAR's). That's the next task.

---

### Task 13: Delete shared2 slurm Rust subtree

**Repo:** `gaussian-job-shared2`
**Files:**
- Delete: `src/entities/slurm.rs`
- Delete: `src/entities/slurm/**` (entire subtree)
- Delete: `src/py_export/entities/slurm/**` (entire subtree)
- Modify: `src/entities.rs` (or wherever `pub mod slurm` is declared)
- Modify: `src/py_export/entities.rs` (or wherever `pub mod slurm` is declared)

- [ ] **Step 1: Find the mod declarations to remove**

```bash
grep -rn "pub mod slurm\|mod slurm" src/entities*.rs src/py_export/entities*.rs 2>/dev/null
```

- [ ] **Step 2: Remove the `pub mod slurm;` lines** from the files Step 1 lists.

- [ ] **Step 3: Delete the slurm subtrees**

```bash
git rm -r src/entities/slurm.rs src/entities/slurm
git rm -r src/py_export/entities/slurm
```

- [ ] **Step 4: Run cargo check**

```bash
cargo check --no-default-features 2>&1 | tail -10
```

Expected: clean. shared2's workflow / config now resolve `SlurmJobConfig` and `DependencyType` from SAR.

```bash
cargo check --all-features 2>&1 | tail -10
```

Expected: clean (or pyclass surface errors only — those are addressed in Task 14).

---

### Task 14: Inventory shared2's Python-facing slurm interactions and add bridges only where needed

**Repo:** `gaussian-job-shared2`
**Files:** depends on inventory result

- [ ] **Step 1: Find every shared2 pyclass that exposes a slurm vocab type to Python**

```bash
grep -rn "SlurmJobConfig\|JobTimeLimit\|ResourceSpec\|JobPartition\|Memory" src/py_export/ | grep -v "^src/py_export/entities/slurm" | head -30
```

Each match is a place that either takes a slurm type as a `__new__` argument or returns one as a getter / method.

- [ ] **Step 2: Decide per match**

For each match, classify as one of:
- **(a) Argument extraction** (e.g., `__new__(slurm_config: SlurmJobConfig)`): write a `*Bridge` newtype with `FromPyObject` (see spec §6.1).
- **(b) Return value to Python** (e.g., `#[getter] fn slurm_config(&self) -> PySlurmJobConfig`): use `Py::import` to fetch SAR's canonical type (see spec §6.2).
- **(c) Internal Rust use only** (no `#[pyclass]` boundary crossed): no change beyond the import rewire from Task 12.

- [ ] **Step 3: For (a) cases, write each bridge**

Create `src/py_export/bridge.rs` with the necessary newtypes. For example, if `WorkflowJob.__new__` accepts a `SlurmJobConfig`:

```rust
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use slurm_async_runner::entities::slurm::{
    DependencyType, JobPartition, JobTimeLimit, Memory, MemoryUnit, ResourceSpec, ResourceSpecCPU,
    ResourceSpecGPU, SlurmJobConfig,
};

#[repr(transparent)]
pub struct MemoryBridge(pub Memory);

impl<'py> FromPyObject<'py> for MemoryBridge {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        let value: u64 = ob.getattr(intern!(py, "value"))?.extract()?;
        let unit_str: String = ob.getattr(intern!(py, "unit"))?.str()?.extract()?;
        let unit = match unit_str.as_str() {
            "B" => MemoryUnit::Bytes,
            "K" | "KiB" => MemoryUnit::KiB,
            "M" | "MiB" => MemoryUnit::MiB,
            "G" | "GiB" => MemoryUnit::GiB,
            other => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "unrecognized memory unit {other:?}"
                )))
            }
        };
        Ok(Self(Memory::new(value, unit)))
    }
}

#[repr(transparent)]
pub struct ResourceSpecBridge(pub ResourceSpec);

impl<'py> FromPyObject<'py> for ResourceSpecBridge {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        let processes: Option<u32> = ob.getattr(intern!(py, "processes"))?.extract()?;
        let threads: Option<u32> = ob.getattr(intern!(py, "threads"))?.extract()?;
        let cores: Option<u32> = ob.getattr(intern!(py, "cores"))?.extract()?;
        let gpus: Option<u32> = ob.getattr(intern!(py, "gpus"))?.extract()?;
        let memory_any = ob.getattr(intern!(py, "memory"))?;
        let memory = if memory_any.is_none() {
            None
        } else {
            Some(MemoryBridge::extract_bound(&memory_any)?.0)
        };
        ResourceSpec::from_parts(processes, threads, cores, memory, gpus)
            .map(Self)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }
}

#[repr(transparent)]
pub struct JobTimeLimitBridge(pub JobTimeLimit);

impl<'py> FromPyObject<'py> for JobTimeLimitBridge {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s: String = ob.str()?.extract()?;
        JobTimeLimit::from_str(&s)
            .map(Self)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }
}

#[repr(transparent)]
pub struct SlurmJobConfigBridge(pub SlurmJobConfig);

impl<'py> FromPyObject<'py> for SlurmJobConfigBridge {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        let partition: JobPartition = ob.getattr(intern!(py, "partition"))?.extract()?;
        let time_limit_any = ob.getattr(intern!(py, "time_limit"))?;
        let time_limit = if time_limit_any.is_none() {
            None
        } else {
            Some(JobTimeLimitBridge::extract_bound(&time_limit_any)?.0)
        };
        let resource_spec_any = ob.getattr(intern!(py, "resource_spec"))?;
        let resource_spec = if resource_spec_any.is_none() {
            None
        } else {
            Some(ResourceSpecBridge::extract_bound(&resource_spec_any)?.0)
        };
        Ok(Self(SlurmJobConfig::new(partition, time_limit, resource_spec)))
    }
}
```

Add `pub mod bridge;` to `src/py_export/mod.rs`. Update each pyclass site identified in Step 1 to take `XxxBridge` instead of `XxxPy`-style arguments.

For (b) cases, replace the existing `#[getter]` body with the `Py::import` pattern:

```rust
fn slurm_config_getter<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    let module = py.import(intern!(
        py,
        "slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options"
    ))?;
    let cls = module.getattr(intern!(py, "SlurmJobConfig"))?;
    cls.call1((self.0.partition.clone(), /* ... */))
}
```

- [ ] **Step 4: Run cargo check + tests**

```bash
cargo check --all-features 2>&1 | tail -10
cargo test --all-features 2>&1 | tail -10
```

Expected: clean.

- [ ] **Step 5: Commit Tasks 12 + 13 + 14 together**

```bash
git add -A
git commit -m "refactor!: consume slurm vocab from slurm_async_runner, drop local subtree"
```

---

### Task 15: Delete shared2 Python type stubs for slurm

**Repo:** `gaussian-job-shared2`
**Files:**
- Delete: `python/gaussian_job_shared/_gaussian_job_shared_core/entities/slurm/**`
- Modify: `python/gaussian_job_shared/_gaussian_job_shared_core/entities/__init__.pyi` (drop `slurm` from `__all__`)
- Modify: `python/tests/test_all.py` (update import paths)

- [ ] **Step 1: Delete the slurm stubs subtree**

```bash
git rm -r python/gaussian_job_shared/_gaussian_job_shared_core/entities/slurm
```

- [ ] **Step 2: Update `entities/__init__.pyi`**

Edit `python/gaussian_job_shared/_gaussian_job_shared_core/entities/__init__.pyi` and remove the `from . import slurm` line and the `"slurm"` entry from `__all__`.

- [ ] **Step 3: Update `test_all.py` imports**

```bash
grep -n "from gaussian_job_shared._gaussian_job_shared_core.entities.slurm\|gaussian_job_shared._gaussian_job_shared_core.entities.slurm" python/tests/test_all.py
```

For each match, rewrite from `gaussian_job_shared._gaussian_job_shared_core.entities.slurm` to `slurm_async_runner._slurm_async_runner_core.entities.slurm`. The wheel name in the import path changes; the type names do not.

- [ ] **Step 4: Build wheel and run tests**

```bash
maturin build --release 2>&1 | tail -5
pip install --force-reinstall --quiet target/wheels/gaussian_job_shared-*.whl
pip install --force-reinstall --quiet /home/kiyama/programs/research/GAUSSIAN_repo_packages/slurm-async-runner2/.dmux/worktrees/merge-tssrun-struct/target/wheels/slurm_async_runner-*.whl
pytest python/tests/test_all.py 2>&1 | tail -10
```

Expected: tests pass.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor!: drop slurm Python stubs (moved to slurm_async_runner)"
```

---

### Task 16: Collapse shared2 pyo3 feature surface

**Repo:** `gaussian-job-shared2`
**Files:** `Cargo.toml`

- [ ] **Step 1: Inspect current feature block**

```bash
grep -n "^pyo3\|^pyo3-types\|^pymodule-entry\|^stub_gen" Cargo.toml
```

- [ ] **Step 2: Replace the split with a single `pyo3` feature**

In `Cargo.toml` `[features]`, replace whatever currently exists with:

```toml
[features]
default = ["pyo3", "stub_gen"]

# Canonical-wheel build for shared2's own pyclasses (workflow types,
# Gaussian-domain types). See the architecture rule comment in
# slurm_async_runner/Cargo.toml — shared2 follows the same Pyclass
# Single Owner discipline.
pyo3 = [
    "dep:pyo3",
    "pyo3-async-runtimes",
    "pyo3-log",
    "pythonize",
    "pyo3-stub-gen",
]

stub_gen = ["pyo3"]
```

- [ ] **Step 3: Run full check**

```bash
cargo build --all-features 2>&1 | tail -5
cargo clippy --all-targets --all-features -- -D warnings 2>&1 | tail -5
cargo test --all-features 2>&1 | tail -5
```

Expected: all clean.

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml
git commit -m "refactor(features): collapse pyo3-types/pymodule-entry split"
```

---

## Phase C — Cross-cdylib smoke

### Task 17: Cross-cdylib Python smoke test

**Files:**
- Create: `/tmp/smoke_pyclass_ownership.py` (transient — not committed)

- [ ] **Step 1: Create a fresh venv and install both wheels**

```bash
SHARED2=/home/kiyama/programs/research/GAUSSIAN_repo_packages/gaussian-job-shared2
SAR=/home/kiyama/programs/research/GAUSSIAN_repo_packages/slurm-async-runner2/.dmux/worktrees/merge-tssrun-struct
python -m venv /tmp/smoke_pco
/tmp/smoke_pco/bin/pip install --quiet "$SAR"/target/wheels/slurm_async_runner-*.whl
/tmp/smoke_pco/bin/pip install --quiet "$SHARED2"/target/wheels/gaussian_job_shared-*.whl
```

- [ ] **Step 2: Write the smoke script**

Save as `/tmp/smoke_pyclass_ownership.py`:

```python
"""Phase C smoke: confirm Pyclass Single Owner rule is enforced."""

# 1. SAR is the canonical home for slurm vocab.
from slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options import (
    ResourceSpec, ResourceSpecCPU, ResourceSpecGPU, Memory, MemoryUnit, JobTimeLimit,
)
from slurm_async_runner._slurm_async_runner_core.tssrun import TssrunCmd

# 2. shared2 imports its own workflow types but NO slurm vocab.
import gaussian_job_shared._gaussian_job_shared_core as gjs_core
gjs_slurm_path = "gaussian_job_shared._gaussian_job_shared_core.entities.slurm"
try:
    __import__(gjs_slurm_path)
    raise SystemExit(f"REGRESSION: {gjs_slurm_path} should not exist after migration")
except ModuleNotFoundError:
    print(f"OK: {gjs_slurm_path} no longer importable")

# 3. SAR pyclass instances pass through SAR's own functions (sanity).
spec = ResourceSpec(processes=4, memory=Memory(8, MemoryUnit.GiB))
cmd = TssrunCmd(cmd="echo hi", rsc=spec, time_limit=JobTimeLimit("01:00:00"))
print(f"OK: SAR TssrunCmd accepts SAR ResourceSpec — argv = {cmd.build_argv()}")

# 4. Type identity holds within SAR.
spec2 = ResourceSpec.from_str("p=4:m=8G")
assert type(spec) is type(spec2), "SAR ResourceSpec type identity inconsistent"
print("OK: ResourceSpec type identity stable within SAR")

# 5. shared2 pyclass that consumes SAR types (if any) accepts via bridge.
#    Skip this section if shared2 has no Python-facing slurm-consuming pyclass.
print("OK: smoke complete")
```

- [ ] **Step 3: Run the smoke**

```bash
/tmp/smoke_pco/bin/python /tmp/smoke_pyclass_ownership.py
```

Expected output:

```
OK: gaussian_job_shared._gaussian_job_shared_core.entities.slurm no longer importable
OK: SAR TssrunCmd accepts SAR ResourceSpec — argv = [...]
OK: ResourceSpec type identity stable within SAR
OK: smoke complete
```

If Step 5 needs an additional cross-package check (when shared2 has a Python-facing pyclass that takes a slurm type via bridge), add a section that constructs that pyclass and asserts the bridge accepts SAR's `ResourceSpec` instance.

- [ ] **Step 4: No code commit (smoke is verification)**

Record the smoke pass in the project's running notes:

```bash
cd /home/kiyama/programs/research/GAUSSIAN_repo_packages/slurm-async-runner2/.dmux/worktrees/merge-tssrun-struct
echo "$(date -I): Phase C smoke passed (commit $(git rev-parse HEAD))" >> docs/error.md
git add docs/error.md
git commit -m "docs(error): record Phase C smoke pass for slurm vocab migration"
```

---

### Task 18: Push both branches and update plan tracking

**Files:** none

- [ ] **Step 1: Push SAR branch (already pushed by Task 10; force-push only if amended)**

```bash
cd /home/kiyama/programs/research/GAUSSIAN_repo_packages/slurm-async-runner2/.dmux/worktrees/merge-tssrun-struct
git push origin merge-tssrun-struct 2>&1 | tail -3
```

- [ ] **Step 2: Push shared2 branch**

```bash
cd /home/kiyama/programs/research/GAUSSIAN_repo_packages/gaussian-job-shared2
git push -u origin slurm-vocab-extraction 2>&1 | tail -3
```

- [ ] **Step 3: Pin shared2's SAR dep to a stable rev (post-push)**

In shared2's `Cargo.toml`, replace `branch = "merge-tssrun-struct"` with `rev = "<SHA from Task 10 Step 2>"`. Commit and push the pin.

```bash
cd /home/kiyama/programs/research/GAUSSIAN_repo_packages/gaussian-job-shared2
SAR_SHA=$(git -C /home/kiyama/programs/research/GAUSSIAN_repo_packages/slurm-async-runner2/.dmux/worktrees/merge-tssrun-struct rev-parse HEAD)
echo "Pin shared2 to SAR SHA $SAR_SHA"
# edit Cargo.toml manually or via sed; commit "deps: pin slurm_async_runner to <sha>"
```

---

## Self-Review Checklist (run before handoff)

**1. Spec coverage:**

| Spec section | Implementing task(s) |
|---|---|
| §2 Architecture Rule (Pyclass Single Owner) | Task 7 (SAR Cargo.toml comment), Task 16 (shared2 Cargo.toml comment) |
| §3.1 Moves out of shared2 | Tasks 1, 3, 8 (Rust source, pyclass, stubs) |
| §3.2 Stays in shared2 | Tasks 12, 14 (rewire, bridges) |
| §3.3 Out of scope | enforced by absence: no deprecation aliases, no pyo3-bridge crate |
| §4 Dependency direction reversal | Tasks 7 (SAR drops dep), 11 (shared2 adds dep) |
| §5 SAR feature collapse | Task 7 |
| §6.1 Bridge `FromPyObject` | Task 14 Step 3 |
| §6.2 `Py::import` return path | Task 14 Step 3 (b case) |
| §6.3 Public Rust constructors | Task 2 (`ResourceSpec::from_parts`) |
| §7 Migration sequence | Tasks 1–18 follow §7's order |
| §8 Alternatives | recorded in spec; not implemented |
| §9 Risks | (a) breakage acknowledged in Task 15 Step 3; (b) build edge added in Task 11; (c) `Py::import` cost in Task 14; (d) bridge field coupling in Task 14; (e) social convention enforced via Cargo.toml comments in Tasks 7, 16 |
| §10 Future work | not in scope |

**2. Placeholder scan:** none.

**3. Type consistency:** `ResourceSpec::from_parts(processes, threads, cores, memory, gpus)` signature is identical in Task 2 (definition), Task 14 (Bridge usage), and Task 17 (smoke). `MemoryUnit` enum variants `B` / `KiB` / `MiB` / `GiB` match across Task 14 bridge string-decode and Task 17 smoke construction.

---

## Glossary

- **shared2**: the `gaussian_job_shared` Cargo crate / wheel.
- **SAR**: the `slurm_async_runner` Cargo crate / wheel.
- **A1+A2 / A3**: improvement commits on shared2 branch `relax-resource-spec-and-feature-split` at sha `299d3e8` — partial CPU spec relaxation and kwargs `__new__`. Grafted into SAR via Task 1 / Task 3.
- **Pyclass Single Owner**: the architecture rule from spec §2. Each Python-visible Rust type has exactly one cdylib that compiles its `#[pyclass]` impl.
- **Bridge type**: a `#[repr(transparent)]` newtype with a `FromPyObject` impl that uses `getattr` / `call_method0` (duck-typing) instead of pyclass downcast. Polars's `PySeries` is the canonical example.
