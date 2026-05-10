# Slurm Vocabulary Migration and Pyclass Ownership Rule — Design

> **Status:** Draft (2026-05-10) — supersedes the post-Phase-C2 design choices in `2026-05-09-merge-tssrun-resourcespec-design.md`.
> **Triggered by:** Phase C2 finding (`docs/error.md`) — cross-cdylib pyo3 type identity regression.
> **Decision:** P5-β' (Polars-style pyclass single-owner + slurm vocab migration to SAR).

## 1. Background

### 1.1 Concrete failure

Phase C2 of the prior design produced two wheels (`gaussian_job_shared` and `slurm_async_runner`) where each wheel's cdylib carried its own copy of `#[pyclass] PyResourceSpec`, `PyJobTimeLimit`, `PyMemory`. Python loads two distinct type objects with identical `__module__` strings; pyo3 argument extraction does `TypeId` equality against the *own* cdylib's static, rejecting cross-cdylib instances:

```text
TypeError: argument 'rsc': 'ResourceSpec' object is not an instance of 'ResourceSpec'
```

### 1.2 Structural — not incidental

The pyo3 ecosystem has known this for 5 years (issue #1444, open since 2021). Polars solved it not by sharing pyclass identity but by **forbidding pyclass duplication**. A single canonical wheel owns the `#[pyclass]`; downstream cdylibs hold only `#[repr(transparent)]` newtypes (`PySeries(pub Series)`) plus protocol-based `FromPyObject`. The downstream cdylib never registers a Python type.

This design adopts the same architecture rule.

### 1.3 Domain alignment

Slurm vocabulary (`ResourceSpec`, `JobTimeLimit`, `JobPartition`, `Memory`, `SlurmJobConfig`, `DependencyType`, `JobStatus`) belongs to the slurm bounded context, not the Gaussian-job bounded context. The shared2 crate (`gaussian_job_shared`) currently owns it for historical reasons; SAR (`slurm_async_runner`) is the natural home. Migration also gives the canonical pyclass a single, structurally-correct location.

## 2. Architecture Rule (the invariant)

> **Pyclass Single Owner.** For every Python-visible Rust type, exactly one cdylib in the dependency graph compiles its `#[pyclass]` impl. Every other cdylib that needs to interoperate uses (a) duck-typed `FromPyObject` on `Bound<PyAny>`, or (b) `Py::import` to fetch the canonical type object at runtime. **No cargo feature ever causes a pyclass impl to be linked into a non-owner cdylib.**

This rule is enforced by:

- Treating any feature whose purpose is "expose pyclass impls to a downstream crate" as an anti-pattern. Such features must not exist on public, stable surfaces.
- Documenting the contract in each owner crate's README and Cargo.toml comments.

## 3. Scope

### 3.1 Moves out of `gaussian_job_shared`, into `slurm_async_runner`

**Rust source:**

- `src/entities/slurm.rs` (mod root)
- `src/entities/slurm/sbatch_options.rs`
- `src/entities/slurm/sbatch_options/resource_spec.rs`
- `src/entities/slurm/sbatch_options/time_limit.rs`
- `src/entities/slurm/sbatch_options/array_spec.rs`
- `src/entities/slurm/sbatch_options/dependency.rs`
- `src/entities/slurm/status.rs`
- `src/py_export/entities/slurm/**` (the corresponding pyclass wrappers)

**Python type stubs:**

- `python/gaussian_job_shared/_gaussian_job_shared_core/entities/slurm/**`
  → `python/slurm_async_runner/_slurm_async_runner_core/entities/slurm/**`

**Carried-over improvements** from the abandoned Phase A branch (`relax-resource-spec-and-feature-split` @ `299d3e8`):

- A1+A2: `ResourceSpecCPU` partial spec relaxation per KUDPC manual.
- A3: positional/kwargs `__new__` + `from_str` for `PyResourceSpec`.
- A4 (in modified form): SAR's pyo3 feature surface follows the new ownership rule (single `pyo3` feature, no `pyo3-types` split).

### 3.2 Stays in `gaussian_job_shared`

- `src/entities/workflow.rs` (`WorkflowJob` references SAR's `SlurmJobConfig` as a Rust field)
- `src/entities/workflow/job.rs` (uses SAR's `DependencyType`)
- `src/config/common.rs` (uses SAR's `SlurmJobConfig`)
- All Gaussian-domain entities

### 3.3 Out of scope

- Deprecation aliases inside `gaussian_job_shared._gaussian_job_shared_core.entities.slurm.*` (immediate hard removal — the only known consumers are this project's tests and SAR itself, both of which we control). External users get a single `feat!:` migration note.
- New `pyo3-bridge` extraction crate. Not justified for a single consumer (shared2). Revisit when a third Rust+pyo3 consumer arrives.

## 4. Dependency Direction

```
                 Before                                After
  shared2 ◀── SAR (Cargo dep)            shared2 ──▶ SAR (Cargo dep)
  (slurm vocab + workflow)                (workflow + Gaussian domain)
                                           SAR (slurm vocab + runner)
```

- `slurm_async_runner` is loaded with `default-features = false` from `gaussian_job_shared`. shared2 receives Rust types only; **no SAR pyclass impl lands in shared2's cdylib.**
- shared2's existing `pyo3` feature continues to drive shared2's own wheel build for shared2-owned pyclasses (workflow / Gaussian-domain types). It does not propagate to SAR.

Cycle check: SAR currently uses `gaussian_job_shared` only inside `tssrun::cmd` (shared2 `ResourceSpec` etc.). After §3.1 these references resolve to crate-local `crate::entities::slurm::...`. SAR's Cargo.toml drops the shared2 dep entirely, so the new direction is unidirectional.

## 5. SAR Cargo Feature Surface (post-migration)

```toml
[features]
default = ["pyo3", "stub_gen"]

# Canonical-wheel build: enables BOTH the pyclass impls and the
# pymodule entry. This is the only path under which SAR's pyclass
# code is compiled. Downstream crates MUST NOT enable this feature
# (use `default-features = false` to consume Rust types only).
pyo3 = [
  "dep:pyo3",
  "pyo3-async-runtimes",
  "pyo3-log",
  "pythonize",
  "pyo3-stub-gen",
]

# stub_gen binary support — bundled with `pyo3` because the gen_stub
# macros are referenced unconditionally in pyclass code.
stub_gen = ["pyo3"]
```

- The previously-introduced `pyo3-types` / `pymodule-entry` split (Phase A4) is **dropped** in SAR. It was the very feature that made the C2 regression possible.
- Comments in `Cargo.toml` state the architecture rule explicitly so future contributors don't reintroduce a "pyclass-only" feature.

shared2 keeps its own `pyo3` feature for its own canonical pyclasses (workflow types). After this migration shared2 has fewer pyclasses and the feature is simpler again.

## 6. Cross-cdylib Interop Pattern (when shared2 must interact at Python level)

For each direction:

### 6.1 shared2 receives a SAR-owned pyclass instance from Python

If shared2 has a Python-facing `WorkflowJob.__new__(slurm_config: SlurmJobConfig)`, then in shared2:

```rust
// shared2/src/py_export/workflow.rs
use slurm_async_runner::entities::slurm::SlurmJobConfig;  // Rust type, no pyclass

#[repr(transparent)]
pub struct SlurmJobConfigBridge(pub SlurmJobConfig);

impl<'py> FromPyObject<'py> for SlurmJobConfigBridge {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        let partition: String = ob.getattr(intern!(py, "partition"))?.extract()?;
        let time_limit: Option<JobTimeLimitBridge> =
            ob.getattr(intern!(py, "time_limit"))?.extract()?;
        let resource_spec: Option<RscBridge> =
            ob.getattr(intern!(py, "resource_spec"))?.extract()?;
        // Reconstruct from public Rust constructor
        Ok(Self(SlurmJobConfig::new(
            partition,
            time_limit.map(|b| b.0),
            resource_spec.map(|b| b.0),
        )))
    }
}
```

The `Bound<PyAny>` comes from any cdylib — SAR's canonical `SlurmJobConfig`, a stub, a duck. Identity is irrelevant.

### 6.2 shared2 returns a SAR-owned pyclass instance to Python

```rust
fn slurm_config_getter<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    let module = py.import(intern!(
        py,
        "slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options"
    ))?;
    let cls = module.getattr(intern!(py, "SlurmJobConfig"))?;
    cls.call1((self.0.partition.clone(), /* … */))
}
```

This costs one `PyImport_ImportModule` (module is cached after the first call). It returns the SAR-canonical type, so any subsequent `isinstance` / `extract` works.

### 6.3 Required public Rust constructors on the SAR side

To make §6.1 work without re-implementing validation, SAR must expose `pub fn` constructors that mirror its pyclass `__new__` logic:

- `ResourceSpec::from_parts(processes, threads, cores, memory, gpus) -> Result<Self>`
- `Memory::new(value, unit) -> Self`
- `JobTimeLimit::from_str` / `JobTimeLimit::from_seconds` (already public)
- `SlurmJobConfig::new(partition, time_limit, resource_spec)`

Most are trivial; some already exist. The single new requirement of substance is `ResourceSpec::from_parts`, which lifts logic out of `PyResourceSpec::__new__` into a pyclass-free Rust function. Doing so is independently desirable for Rust-side API consistency.

## 7. Migration Plan (broad strokes)

Detailed task breakdown belongs in the implementation plan (next step). High-level sequence:

1. **SAR — graft slurm vocab.** Create `src/entities/slurm/**` with the Phase A1+A2+A3 improvements baked in. Carry `src/py_export/entities/slurm/**` (canonical pyclasses).
2. **SAR — rewire `tssrun::cmd`.** Switch references from `gaussian_job_shared::...` to `crate::entities::slurm::...`. Drop the `gaussian_job_shared` dep from SAR's Cargo.toml.
3. **SAR — feature collapse.** Remove `pyo3-types` / `pymodule-entry`; keep a single `pyo3` feature gating both pyclass impls and the pymodule entry. Add a Cargo.toml comment encoding the architecture rule.
4. **SAR — public Rust constructors.** Add `ResourceSpec::from_parts`, `SlurmJobConfig::new`, etc. (any not yet public).
5. **shared2 — remove slurm subtree.** Delete `src/entities/slurm/**`, `src/py_export/entities/slurm/**`, and the python type stubs.
6. **shared2 — add SAR Cargo dep with `default-features = false`.** Rewrite `workflow.rs`, `workflow/job.rs`, `config/common.rs` to import SAR Rust types.
7. **shared2 — bridge types where needed.** If shared2 has Python-facing workflow APIs that accept/return SlurmJobConfig, write `*_Bridge` newtypes per §6.
8. **shared2 — feature simplification.** Roll back the Phase A4 `pyo3-types` / `pymodule-entry` split since shared2 no longer needs it (its remaining pyclasses are workflow-only and shared2 itself is the canonical owner of those).
9. **Both — tests.** `cargo test`, `cargo clippy --all-targets --all-features -- -D warnings`, `maturin build`, and a Python smoke test that loads both wheels in a fresh venv.
10. **Both — push branches and update plan doc.**

## 8. Alternatives Considered

| # | Name | Why rejected |
|---|------|--------------|
| A | Rust `Bound<PyAny>` arguments only | Loses static typing on Python signatures; weaker than newtype + duck-typing. |
| B | Python facade that converts on call | Added per-call overhead and still leaves duplicated pyclass impls in both cdylibs. |
| C | SAR drops Python re-exports of slurm types | Doesn't solve cross-cdylib structurally — only papered over the immediate case. |
| D | Merge SAR and shared2 into a single crate | Disproportionate refactor; loses the bounded-context separation that we still want. |
| E | Dynamic `Py::import` lookup only (no Rust types in shared2) | Throws away Rust-level type safety in shared2 workflow code. |
| F (original) | Polars-style duck-typing on the **wrong owner** (shared2 keeps slurm vocab) | Solved cdylib identity but kept the misaligned domain ownership. |
| P5-α | Move only `sbatch_options/*`, keep `status` and `SlurmJobConfig` in shared2 | Splits a cohesive bounded context across crates for no architectural reason. |
| P5-γ | Third `slurm-vocab` crate consumed by both | Adds a third cdylib + a third wheel for a two-consumer system; revisit if a third Rust consumer ever arrives. |
| P5-δ | Single-crate consolidation | User retained shared2 as the workflow / Gaussian-domain home. |

## 9. Risks and Tradeoffs

- **Breaking change for any out-of-tree shared2 Python user.** `from gaussian_job_shared._gaussian_job_shared_core.entities.slurm import …` becomes `from slurm_async_runner._slurm_async_runner_core.entities.slurm import …`. Within this project the only consumers are `python/tests/test_all.py` and SAR itself, both of which we update.
- **shared2 → SAR Cargo dep adds a build-time edge.** SAR is a research repo on git; shared2 already pulls a git dep. The mechanical cost is identical to the current SAR → shared2 edge.
- **shared2 Python wrappers around SAR types incur a `Py::import` per construction (§6.2).** Acceptable for the workflow-level granularity we operate at; the cached-module path avoids the cost after first call.
- **§6.1 bridges duplicate the field surface.** If SAR adds a new field to `SlurmJobConfig`, shared2's `SlurmJobConfigBridge::extract_bound` needs an update. This is an explicit, type-checked coupling and surfaces the change rather than hiding it.
- **Architecture rule depends on social convention.** Cargo cannot mechanically enforce "no downstream enables `pyo3` feature." We document the rule prominently in SAR's `Cargo.toml` and `README`. A future reviewer agent (`code-reviewer`) can also check it.

## 10. Out-of-tree Future Work (deliberately deferred)

- A standalone `pyo3-slurm-bridge` crate analogous to `pyo3-polars`. Defer until a second external consumer materializes.
- An automated test that loads two cdylibs from this project in a single Python process and asserts no duplicate `PyInit_*` and no duplicate type registrations. Useful as a regression guard for §C2-class issues but not required for first cut.
- A pyo3-side RFC / PR for a `#[pyclass(canonical_in = "crate_name")]` attribute that emits a compile error when a non-canonical crate enables it. Upstream design problem, not ours to solve in this migration.

## 11. References

- pyo3 Issue #1444 — sharing pyclasses between multiple Rust packages: <https://github.com/PyO3/pyo3/issues/1444>
- pyo3-polars `PySeries::FromPyObject` (the canonical example): <https://github.com/pola-rs/pyo3-polars/blob/main/pyo3-polars/src/types.rs>
- pyo3-arrow PyCapsule Interface: <https://docs.rs/pyo3-arrow/>
- KUDPC `--rsc` syntax: <https://web.kudpc.kyoto-u.ac.jp/manual/ja/run/resource#rscoption>
- This project's prior design doc: `docs/superpowers/specs/2026-05-09-merge-tssrun-resourcespec-design.md`
- C2 incident notes: `docs/error.md`
