# Merge `tssrun::cmd::Resource` with `gaussian_job_shared::ResourceSpec` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Consolidate `tssrun::cmd::Resource` onto the strict-but-relaxable
`gaussian_job_shared::entities::slurm::ResourceSpec` enum (per the KUDPC
manual), bundle three adjacent moves (`time_limit` retype, `queue` →
`partition` rename, pymodule rename), and add a feature-split on shared2
so `slurm-async-runner2` can re-export shared2's pyclass wrappers without
a duplicate-symbol collision.

**Architecture:** Two-phase change across two repositories.
**Phase A** lands changes in `gaussian-job-shared2` (relax `ResourceSpecCPU`
to permit partial CPU specs, split the `pyo3` cargo feature into
`pyo3-types` + `pymodule-entry`, rename the pymodule `_core` →
`_gaussian_job_shared_core`, switch `PyResourceSpec.__new__` to a
positional/kwargs signature with a `from_str` classmethod). **Phase B**
lands the consumer changes in `slurm-async-runner2` (depend on shared2
with `features = ["pyo3-types"]`, delete `Resource`, rewrite
`TssrunCmd`'s field shapes, re-export shared2's `PyResourceSpec` /
`PyJobTimeLimit`, rename pymodule `_core` → `_slurm_async_runner_core`).

**Tech Stack:** Rust 2024 edition, `cargo`, `tokio`, `pyo3` 0.28,
`pyo3-stub-gen`, `maturin` 1.13, `pytest`, `chrono`, `serde`,
`thiserror`/`anyhow`.

**Reference spec:** `docs/superpowers/specs/2026-05-09-merge-tssrun-resourcespec-design.md`

**Working directories (absolute paths):**
- `slurm-async-runner2` worktree: `/home/kiyama/programs/research/GAUSSIAN_repo_packages/slurm-async-runner2/.dmux/worktrees/merge-tssrun-struct/`
- `gaussian-job-shared2` repo: `/home/kiyama/programs/research/GAUSSIAN_repo_packages/gaussian-job-shared2/`

**Branch convention:**
- shared2 work goes on a new branch `relax-resource-spec-and-feature-split`.
- slurm-async-runner work continues on the existing `merge-tssrun-struct`
  worktree branch.
- Phase B's Cargo.toml update will pin shared2 to the head commit of
  `relax-resource-spec-and-feature-split` (push to remote after Phase A).

---

# Phase A: gaussian-job-shared2 changes

## Task A0: Create a feature branch on shared2

**Files:**
- (no edits — git operation only)

- [ ] **Step 1: Create and switch to a new branch**

```bash
cd /home/kiyama/programs/research/GAUSSIAN_repo_packages/gaussian-job-shared2
git fetch origin
git checkout main
git pull --ff-only origin main
git checkout -b relax-resource-spec-and-feature-split
git status
```

Expected: `On branch relax-resource-spec-and-feature-split` with no
uncommitted changes.

---

## Task A1: Relax `ResourceSpecCPU` to allow partial specs

**Files:**
- Modify: `gaussian-job-shared2/src/entities/slurm/sbatch_options/resource_spec.rs`

Per the KUDPC manual at
<https://web.kudpc.kyoto-u.ac.jp/manual/ja/run/resource#rscoption>,
each of `p`, `t`, `c`, `m` is individually optional. The current type
forces all four; relax to `Option<NonZeroU32>` / `Option<Memory>`.

- [ ] **Step 1: Add a failing test for partial CPU parsing**

In `gaussian-job-shared2/src/entities/slurm/sbatch_options/resource_spec.rs`,
add inside the existing `mod tests { ... }` block:

```rust
#[test]
fn parses_kudpc_p60_t1_c1_example() {
    // From the KUDPC manual: an MPI 60-way partial CPU spec.
    let r: ResourceSpec = "p=60:t=1:c=1".parse().unwrap();
    assert_eq!(
        r,
        ResourceSpec::CPU(ResourceSpecCPU {
            p: Some(nz(60)),
            t: Some(nz(1)),
            c: Some(nz(1)),
            m: None,
        })
    );
    assert_eq!(r.to_string(), "p=60:t=1:c=1");
}

#[test]
fn parses_partial_cpu_spec_m_only() {
    let r: ResourceSpec = "m=8G".parse().unwrap();
    if let ResourceSpec::CPU(c) = r {
        assert_eq!(c.p, None);
        assert_eq!(c.t, None);
        assert_eq!(c.c, None);
        assert_eq!(c.m, Some(mem(8, MemoryUnit::Giga)));
    } else {
        panic!("expected CPU variant");
    }
}

#[test]
fn display_round_trips_partial_cpu() {
    let original = ResourceSpec::CPU(ResourceSpecCPU {
        p: Some(nz(60)),
        t: Some(nz(1)),
        c: Some(nz(1)),
        m: None,
    });
    let s = original.to_string();
    let parsed: ResourceSpec = s.parse().unwrap();
    assert_eq!(parsed, original);
}

#[test]
fn cpu_default_is_all_none_and_display_is_empty() {
    let r = ResourceSpec::CPU(ResourceSpecCPU::default());
    assert_eq!(r.to_string(), "");
}
```

- [ ] **Step 2: Run the new tests and confirm they fail to compile**

```bash
cd /home/kiyama/programs/research/GAUSSIAN_repo_packages/gaussian-job-shared2
cargo test --no-default-features --lib resource_spec 2>&1 | head -40
```

Expected: compilation errors (e.g., "expected `NonZeroU32`, found
`Option<NonZeroU32>`" or "no field `default` for `ResourceSpecCPU`").
The new tests assume the relaxed shape.

- [ ] **Step 3: Replace the `ResourceSpecCPU` definition**

In `gaussian-job-shared2/src/entities/slurm/sbatch_options/resource_spec.rs`,
replace:

```rust
/// CPU flavour of [`ResourceSpec`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceSpecCPU {
    /// `p=` — number of MPI processes (>= 1).
    pub p: NonZeroU32,
    /// `t=` — number of threads per process (>= 1).
    pub t: NonZeroU32,
    /// `c=` — number of cores per process (>= 1).
    pub c: NonZeroU32,
    /// `m=` — memory request.
    pub m: Memory,
}
```

with:

```rust
/// CPU flavour of [`ResourceSpec`].
///
/// Per the KUDPC manual
/// (<https://web.kudpc.kyoto-u.ac.jp/manual/ja/run/resource#rscoption>),
/// each of `p`, `t`, `c`, `m` is individually optional — when omitted
/// the system applies its default (1 for the integer fields,
/// system-dependent for memory). All-`None` is permitted and renders
/// to an empty string via [`std::fmt::Display`]; consumers (e.g.
/// `tssrun`'s argv builder) treat that as "skip the `--rsc` flag
/// entirely".
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceSpecCPU {
    /// `p=` — number of MPI processes when set; `None` means "use
    /// the system default" (typically 1). Always `>= 1` when present.
    pub p: Option<NonZeroU32>,
    /// `t=` — threads per process. Same `Option` semantics as `p`.
    pub t: Option<NonZeroU32>,
    /// `c=` — cores per process. Same `Option` semantics as `p`.
    pub c: Option<NonZeroU32>,
    /// `m=` — memory request. Same `Option` semantics as `p`.
    pub m: Option<Memory>,
}
```

- [ ] **Step 4: Replace the `Display` impl**

In the same file, replace the existing `impl std::fmt::Display for ResourceSpec`
block with:

```rust
impl std::fmt::Display for ResourceSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResourceSpec::CPU(c) => {
                let mut parts: Vec<String> = Vec::with_capacity(4);
                if let Some(p) = c.p {
                    parts.push(format!("p={p}"));
                }
                if let Some(t) = c.t {
                    parts.push(format!("t={t}"));
                }
                if let Some(cc) = c.c {
                    parts.push(format!("c={cc}"));
                }
                if let Some(m) = &c.m {
                    parts.push(format!("m={m}"));
                }
                write!(f, "{}", parts.join(":"))
            }
            ResourceSpec::GPU(g) => write!(f, "g={}", g.g),
        }
    }
}
```

- [ ] **Step 5: Replace the `FromStr` impl's terminal `match`**

In the same file, replace the existing `impl std::str::FromStr for ResourceSpec`
body's terminal `match` with the relaxed dispatch:

```rust
        match (g, p, t, c, m) {
            // GPU flavour: exactly `g`, no CPU keys.
            (Some(g), None, None, None, None) => Ok(ResourceSpec::GPU(ResourceSpecGPU { g })),

            // GPU mixed with any CPU key — rejected.
            (Some(_), _, _, _, _) => Err(err()),

            // CPU flavour: any non-empty subset of (p, t, c, m).
            (None, p, t, c, m) if p.is_some() || t.is_some() || c.is_some() || m.is_some() => {
                Ok(ResourceSpec::CPU(ResourceSpecCPU { p, t, c, m }))
            }

            // Empty (no recognised keys) — already caught earlier by
            // the empty-string guard, but keep an explicit fall-through.
            _ => Err(err()),
        }
```

The local variables `p, t, c, m, g` change type from `Option<NonZero...>`
holding "set or not yet seen" to `Option<NonZero...>` holding "set or
absent in input" — same Rust type, different semantics. Keep the
`if p.is_some() { return Err(err()); }` duplicate-key guards intact (they
already use `Option::is_some()`).

- [ ] **Step 6: Update existing tests that construct `ResourceSpecCPU` with un-wrapped fields**

The tests `parses_kudpc_cpu_example`, `parses_cpu_spec_in_arbitrary_order`,
`parses_unitless_memory_in_cpu_spec`, `deserialize_cpu_from_toml_string`,
`serialize_cpu_to_toml_string`, and `toml_roundtrip_preserves_value`
all construct `ResourceSpecCPU { p: nz(N), t: nz(N), c: nz(N), m: mem(...) }`
or assert against `c.m == mem(...)`. These no longer compile because
each field is now `Option<...>`.

Apply two sed rewrites scoped to the `mod tests` region of the file:

```bash
cd /home/kiyama/programs/research/GAUSSIAN_repo_packages/gaussian-job-shared2
# 1. Inside ResourceSpecCPU { ... } literals, wrap each `nz(N)` and
#    `mem(...)` value in Some(...).
#    Tests use `nz(N)` for NonZeroU32 and `mem(N, MemoryUnit::*)` for Memory.
python3 - <<'PY'
import re
path = "src/entities/slurm/sbatch_options/resource_spec.rs"
src = open(path).read()
# Wrap ` nz(N),` → ` Some(nz(N)),`, ` mem(...),` → ` Some(mem(...)),`
# only inside ResourceSpecCPU literal bodies (`{ p: ..., t: ..., c: ..., m: ... }`).
# Easiest: target the four exact field patterns.
def wrap_nz(m):
    return f"{m.group(1)}: Some(nz({m.group(2)}))"
def wrap_mem(m):
    return f"m: Some(mem({m.group(1)}))"
# Match `p: nz(4),`  `t: nz(8),`  `c: nz(8),`
src = re.sub(r"\b([ptc]): nz\(([^)]+)\)", wrap_nz, src)
# Match `m: mem(8, MemoryUnit::Giga),`
src = re.sub(r"\bm: mem\(([^)]+\)?[^),]*)\)", wrap_mem, src)
open(path, "w").write(src)
PY
# 2. Inside the assertion `assert_eq!(c.m, mem(...));` we now have
#    `c.m: Option<Memory>`, so wrap the expected value:
sed -i -e 's/assert_eq!(c\.m, mem(\([^)]*\)));/assert_eq!(c.m, Some(mem(\1)));/g' \
    src/entities/slurm/sbatch_options/resource_spec.rs
```

Verify the rewrite by re-reading any one updated test:

```bash
grep -n -A1 'fn parses_kudpc_cpu_example' src/entities/slurm/sbatch_options/resource_spec.rs
```

Expected: the field-literal lines now read `p: Some(nz(4)),` and
`m: Some(mem(8, MemoryUnit::Giga)),`. If the python+sed combination
missed any (e.g. multi-line `mem(...)` expressions), apply the
remaining wraps by hand and re-run `cargo test --no-default-features
--lib resource_spec`.

- [ ] **Step 7: Delete the now-invalid `rejects_partial_cpu_spec` test**

Find and delete the entire test:

```rust
#[test]
fn rejects_partial_cpu_spec() {
    assert!("p=1:t=56:c=56".parse::<ResourceSpec>().is_err());
    assert!("p=1".parse::<ResourceSpec>().is_err());
}
```

(Both `"p=1:t=56:c=56"` and `"p=1"` are now valid partial CPU specs per
KUDPC.)

- [ ] **Step 8: Run all `resource_spec` tests**

```bash
cd /home/kiyama/programs/research/GAUSSIAN_repo_packages/gaussian-job-shared2
cargo test --no-default-features --lib resource_spec
```

Expected: all tests pass, including the four new ones added in Step 1.
At this point the `py_export` crate is gated off (`--no-default-features`)
so we do not yet need to fix the pyo3 wrappers.

- [ ] **Step 9: Commit**

```bash
git add src/entities/slurm/sbatch_options/resource_spec.rs
git commit -m "feat(slurm)!: allow partial ResourceSpecCPU per KUDPC manual

Relax ResourceSpecCPU's p/t/c/m fields from required NonZeroU32/Memory
to Option<NonZeroU32>/Option<Memory>. The KUDPC manual at
https://web.kudpc.kyoto-u.ac.jp/manual/ja/run/resource#rscoption lists
each as individually optional with documented defaults (1 for counts,
system-dependent for memory).

Display emits only Some keys in canonical order; the all-None CPU value
renders to an empty string. FromStr accepts any non-empty subset of CPU
keys and continues to reject mixed CPU+GPU keys, duplicate keys, zero
counts, and empty input.

BREAKING CHANGE: callers of ResourceSpecCPU that constructed via
positional fields {p, t, c, m: NonZeroU32/Memory} must wrap each value
with Some(...). The pyo3 wrappers are updated in a follow-up commit."
```

---

## Task A2: Update pyo3 wrappers to compile against the relaxed type

**Files:**
- Modify: `gaussian-job-shared2/src/py_export/entities/slurm/sbatch_options/resource_spec.rs`

The pyo3 layer was gated off in Task A1's tests. Re-enable the default
feature set and fix the wrappers.

- [ ] **Step 1: Enable default features and observe the breakage**

```bash
cd /home/kiyama/programs/research/GAUSSIAN_repo_packages/gaussian-job-shared2
cargo build 2>&1 | head -40
```

Expected: errors in
`src/py_export/entities/slurm/sbatch_options/resource_spec.rs` —
specifically in `PyResourceSpecCPU::new` (constructs `ResourceSpecCPU`
with un-wrapped fields) and the four getters (`fn p(&self) -> u32`,
`t`, `c`, `m`) which call `.get()` on what is now `Option<NonZeroU32>`.

- [ ] **Step 2: Update `PyResourceSpecCPU::new` to wrap fields in `Some`**

In `gaussian-job-shared2/src/py_export/entities/slurm/sbatch_options/resource_spec.rs`,
replace the existing `#[new] fn new(...)` body for `PyResourceSpecCPU`:

```rust
    #[new]
    fn new(p: u32, t: u32, c: u32, m: PyMemory) -> PyResult<Self> {
        let p = NonZeroU32::new(p).ok_or_else(|| PyValueError::new_err("p must be > 0"))?;
        let t = NonZeroU32::new(t).ok_or_else(|| PyValueError::new_err("t must be > 0"))?;
        let c = NonZeroU32::new(c).ok_or_else(|| PyValueError::new_err("c must be > 0"))?;
        Ok(Self(inner::ResourceSpecCPU { p, t, c, m: m.0 }))
    }
```

with:

```rust
    /// Construct a fully-specified CPU resource spec — all four of
    /// (`p`, `t`, `c`, `m`) are required positional arguments.
    /// For partial specs (e.g. `p=60:t=1:c=1` per the KUDPC manual),
    /// use [`PyResourceSpec`]'s positional/kwargs constructor instead.
    #[new]
    fn new(p: u32, t: u32, c: u32, m: PyMemory) -> PyResult<Self> {
        let p = NonZeroU32::new(p).ok_or_else(|| PyValueError::new_err("p must be > 0"))?;
        let t = NonZeroU32::new(t).ok_or_else(|| PyValueError::new_err("t must be > 0"))?;
        let c = NonZeroU32::new(c).ok_or_else(|| PyValueError::new_err("c must be > 0"))?;
        Ok(Self(inner::ResourceSpecCPU {
            p: Some(p),
            t: Some(t),
            c: Some(c),
            m: Some(m.0),
        }))
    }
```

- [ ] **Step 3: Update the four getters on `PyResourceSpecCPU`**

In the same `impl PyResourceSpecCPU` block, replace the getters:

```rust
    #[getter]
    fn p(&self) -> u32 {
        self.0.p.get()
    }

    #[getter]
    fn t(&self) -> u32 {
        self.0.t.get()
    }

    #[getter]
    fn c(&self) -> u32 {
        self.0.c.get()
    }

    #[getter]
    fn m(&self) -> PyMemory {
        PyMemory(self.0.m)
    }
```

with the `Option`-aware versions:

```rust
    /// Returns `None` if `p` was not specified.
    #[getter]
    fn p(&self) -> Option<u32> {
        self.0.p.map(NonZeroU32::get)
    }

    /// Returns `None` if `t` was not specified.
    #[getter]
    fn t(&self) -> Option<u32> {
        self.0.t.map(NonZeroU32::get)
    }

    /// Returns `None` if `c` was not specified.
    #[getter]
    fn c(&self) -> Option<u32> {
        self.0.c.map(NonZeroU32::get)
    }

    /// Returns `None` if `m` was not specified.
    #[getter]
    fn m(&self) -> Option<PyMemory> {
        self.0.m.map(PyMemory)
    }
```

- [ ] **Step 4: Update `PyResourceSpecCPU::__repr__`**

In the same `impl PyResourceSpecCPU` block, replace:

```rust
    fn __repr__(&self) -> String {
        format!(
            "ResourceSpecCPU(p={}, t={}, c={}, m={:?})",
            self.0.p.get(),
            self.0.t.get(),
            self.0.c.get(),
            self.0.m.to_string()
        )
    }
```

with:

```rust
    fn __repr__(&self) -> String {
        // Render unset fields as `None` so the repr round-trips
        // visually with the relaxed Option<...> shape.
        let p = self.0.p.map(NonZeroU32::get);
        let t = self.0.t.map(NonZeroU32::get);
        let c = self.0.c.map(NonZeroU32::get);
        let m = self.0.m.map(|m| m.to_string());
        format!("ResourceSpecCPU(p={p:?}, t={t:?}, c={c:?}, m={m:?})")
    }
```

- [ ] **Step 5: Build with default features**

```bash
cargo build 2>&1 | tail -20
```

Expected: clean build (warnings ok, no errors).

- [ ] **Step 6: Run the full test suite**

```bash
cargo test 2>&1 | tail -20
```

Expected: all tests pass (the inner Rust tests from Task A1 + any
existing pyo3 tests).

- [ ] **Step 7: Commit**

```bash
git add src/py_export/entities/slurm/sbatch_options/resource_spec.rs
git commit -m "fix(py): wrap ResourceSpecCPU fields in Some after relaxation

Update PyResourceSpecCPU.__new__ to construct the inner CPU spec with
Some(...) wrappers around each NonZeroU32/Memory, since
ResourceSpecCPU's fields are now Option<...>. Getters return
Option<u32> / Option<PyMemory> instead of unconditionally calling .get().
__repr__ now renders unset fields as None.

PyResourceSpecCPU(p, t, c, m) remains a 'fully-specified' constructor —
partial specs go through the new PyResourceSpec(processes=, threads=, ...)
positional/kwargs API added in a follow-up."
```

---

## Task A3: Add positional/kwargs `PyResourceSpec.__new__` and `from_str`

**Files:**
- Modify: `gaussian-job-shared2/src/py_export/entities/slurm/sbatch_options/resource_spec.rs`
- Create: `gaussian-job-shared2/tests/test_resource_spec_kwargs.py`

`PyResourceSpec` currently has `__new__(s: str)` that delegates to
`FromStr`. Replace it with a positional/kwargs Optional-fields
constructor matching the legacy `tssrun::cmd::Resource` ergonomics, and
add `from_str` as the explicit string-parser classmethod.

- [ ] **Step 1: Add a failing Python integration test for the new API**

Create `gaussian-job-shared2/tests/test_resource_spec_kwargs.py`:

```python
"""Verify the positional/kwargs constructor and from_str classmethod
on gaussian_job_shared._core.entities.slurm.sbatch_options.ResourceSpec.

These tests run inside the maturin develop build, i.e. against the
.so produced by `maturin develop` in this repo.
"""
import pytest

from gaussian_job_shared._core.entities.slurm.sbatch_options import (
    Memory,
    MemoryUnit,
    ResourceSpec,
)


def test_resource_spec_full_cpu_via_positional_args():
    r = ResourceSpec(4, 8, 8, Memory("2G"))
    assert str(r) == "p=4:t=8:c=8:m=2G"


def test_resource_spec_full_cpu_via_kwargs():
    r = ResourceSpec(processes=4, threads=8, cores=8, memory=Memory("2G"))
    assert str(r) == "p=4:t=8:c=8:m=2G"


def test_resource_spec_partial_cpu():
    r = ResourceSpec(processes=60, threads=1, cores=1)
    assert str(r) == "p=60:t=1:c=1"


def test_resource_spec_memory_only():
    r = ResourceSpec(memory=Memory("8G"))
    assert str(r) == "m=8G"


def test_resource_spec_default_constructor_renders_empty_cpu():
    r = ResourceSpec()
    assert str(r) == ""
    assert r.kind == "cpu"


def test_resource_spec_gpu():
    r = ResourceSpec(gpus=1)
    assert str(r) == "g=1"
    assert r.kind == "gpu"


def test_resource_spec_rejects_mixed_cpu_and_gpu():
    with pytest.raises(ValueError, match="mutually exclusive"):
        ResourceSpec(processes=4, gpus=1)


def test_resource_spec_rejects_zero_count():
    with pytest.raises(ValueError, match="must be > 0"):
        ResourceSpec(gpus=0)
    with pytest.raises(ValueError, match="must be > 0"):
        ResourceSpec(processes=0, threads=1, cores=1)


def test_resource_spec_memory_must_be_pymemory():
    # Strict typing — string is not implicitly converted.
    with pytest.raises(TypeError):
        ResourceSpec(processes=4, threads=8, cores=8, memory="2G")


def test_resource_spec_from_str_classmethod():
    r = ResourceSpec.from_str("p=4:t=8:c=8:m=8G")
    assert str(r) == "p=4:t=8:c=8:m=8G"

    r2 = ResourceSpec.from_str("g=1")
    assert str(r2) == "g=1"


def test_resource_spec_from_str_rejects_empty():
    with pytest.raises(ValueError):
        ResourceSpec.from_str("")
```

(The import path is updated again in Task A5 once the pymodule is renamed.)

- [ ] **Step 2: Run the failing tests**

```bash
cd /home/kiyama/programs/research/GAUSSIAN_repo_packages/gaussian-job-shared2
maturin develop 2>&1 | tail -5
pytest tests/test_resource_spec_kwargs.py -v 2>&1 | tail -25
```

Expected: most tests fail. The current `__new__(s: str)` will accept
`ResourceSpec("p=4:t=8:c=8:m=2G")` but reject `ResourceSpec(4, 8, 8, Memory("2G"))`
(int passed where str expected). `ResourceSpec()` with no args is also a
type error currently.

- [ ] **Step 3: Replace `PyResourceSpec.__new__` and add `from_str`**

In `gaussian-job-shared2/src/py_export/entities/slurm/sbatch_options/resource_spec.rs`,
replace:

```rust
    /// Parse a Slurm `--rsc` spec, e.g. `"p=4:t=8:c=8:m=8G"` or `"g=1"`.
    #[new]
    fn new(s: &str) -> PyResult<Self> {
        s.parse::<inner::ResourceSpec>()
            .map(Self)
            .map_err(Into::into)
    }

    #[staticmethod]
    fn parse(s: &str) -> PyResult<Self> {
        Self::new(s)
    }
```

with the kwargs constructor + `from_str` classmethod:

```rust
    /// Build a `ResourceSpec` from individual KUDPC `--rsc` keys.
    ///
    /// All keyword arguments are optional. CPU keys
    /// (`processes`, `threads`, `cores`, `memory`) and the GPU key
    /// (`gpus`) are mutually exclusive — passing any of the former
    /// together with the latter raises `ValueError`. Each integer
    /// key must be `>= 1`. The `memory` parameter must be a
    /// [`PyMemory`] instance — wrap a string with `Memory("2G")` or
    /// `Memory.from_value(2, MemoryUnit.Giga)` first.
    #[new]
    #[pyo3(signature = (
        processes = None, threads = None, cores = None,
        memory = None, gpus = None,
    ))]
    fn new(
        processes: Option<u32>,
        threads: Option<u32>,
        cores: Option<u32>,
        memory: Option<PyMemory>,
        gpus: Option<u32>,
    ) -> PyResult<Self> {
        let to_nz = |v: u32, key: &'static str| {
            NonZeroU32::new(v).ok_or_else(|| {
                PyValueError::new_err(format!("ResourceSpec/{key} must be > 0"))
            })
        };
        let p = processes.map(|v| to_nz(v, "processes")).transpose()?;
        let t = threads.map(|v| to_nz(v, "threads")).transpose()?;
        let c = cores.map(|v| to_nz(v, "cores")).transpose()?;
        let m = memory.map(|pm| pm.0);
        let g = gpus.map(|v| to_nz(v, "gpus")).transpose()?;

        let cpu_keys_present = p.is_some() || t.is_some() || c.is_some() || m.is_some();
        match (cpu_keys_present, g) {
            (true, Some(_)) => Err(PyValueError::new_err(
                "CPU keys (processes/threads/cores/memory) and gpus \
                 are mutually exclusive — pass one group or the other",
            )),
            (false, Some(g)) => Ok(Self(inner::ResourceSpec::GPU(inner::ResourceSpecGPU { g }))),
            (true, None) => Ok(Self(inner::ResourceSpec::CPU(inner::ResourceSpecCPU {
                p, t, c, m,
            }))),
            // No CPU keys, no GPU — the all-None CPU is intentionally valid.
            (false, None) => Ok(Self(inner::ResourceSpec::CPU(
                inner::ResourceSpecCPU::default(),
            ))),
        }
    }

    /// Parse a Slurm `--rsc` spec, e.g. `"p=4:t=8:c=8:m=8G"` or `"g=1"`.
    #[staticmethod]
    fn from_str(s: &str) -> PyResult<Self> {
        s.parse::<inner::ResourceSpec>()
            .map(Self)
            .map_err(Into::into)
    }

    /// Backwards-compatible alias for [`Self::from_str`].
    #[staticmethod]
    fn parse(s: &str) -> PyResult<Self> {
        Self::from_str(s)
    }
```

- [ ] **Step 4: Run the Python tests**

```bash
maturin develop 2>&1 | tail -5
pytest tests/test_resource_spec_kwargs.py -v 2>&1 | tail -25
```

Expected: all 11 tests pass.

- [ ] **Step 5: Run the Rust test suite to confirm no regression**

```bash
cargo test 2>&1 | tail -10
```

Expected: all Rust tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/py_export/entities/slurm/sbatch_options/resource_spec.rs tests/test_resource_spec_kwargs.py
git commit -m "feat(py)!: positional/kwargs ResourceSpec.__new__ + from_str classmethod

Replace ResourceSpec(s: str) with ResourceSpec(processes=, threads=,
cores=, memory=, gpus=) — mirrors the ergonomics of the legacy
tssrun::cmd::Resource and lets callers express partial CPU specs (e.g.
ResourceSpec(processes=60, threads=1, cores=1)) per the KUDPC manual.

The string-parsing path is preserved as ResourceSpec.from_str(s) (also
aliased as ResourceSpec.parse(s) for backwards compatibility). The
memory parameter is strictly typed PyMemory — callers wrap their value
with Memory(\"2G\") or Memory.from_value(2, MemoryUnit.Giga); no
implicit string/tuple/dict conversion.

BREAKING CHANGE: ResourceSpec(\"p=4:...\") no longer parses; use
ResourceSpec.from_str(\"p=4:...\") explicitly."
```

---

## Task A4: Split the `pyo3` cargo feature into `pyo3-types` + `pymodule-entry`

**Files:**
- Modify: `gaussian-job-shared2/Cargo.toml`
- Modify: `gaussian-job-shared2/src/lib.rs`
- Modify: `gaussian-job-shared2/src/py_export/mod.rs`

The goal is to make `slurm-async-runner2` able to depend on shared2 with
just `pyo3-types` (pyclass definitions, no `_core` pymodule entry), so
the linker symbol `PyInit__core` is not pulled in twice.

- [ ] **Step 1: Update `Cargo.toml` to declare the new features**

In `gaussian-job-shared2/Cargo.toml`, replace the `[features]` block:

```toml
[features]
default = ["pyo3", "stub_gen"]

pyo3 = ["dep:pyo3", "pyo3-async-runtimes", "pyo3-log", "pythonize"]
stub_gen = ["pyo3-stub-gen"]
```

with:

```toml
[features]
default = ["pyo3", "stub_gen"]

# Full pyo3 build including the outermost #[pymodule] entry point.
# This is the public surface for the shared2 wheel build and matches
# the pre-split `pyo3` behaviour exactly.
pyo3 = ["pyo3-types", "pymodule-entry"]

# pyclass definitions only — no `_gaussian_job_shared_core` pymodule
# entry. Downstream crates that build their own #[pymodule] should
# enable this (and NOT `pyo3`) to avoid a duplicate `PyInit_*` linker
# symbol.
pyo3-types = ["dep:pyo3", "pyo3-async-runtimes", "pyo3-log", "pythonize"]

# Gates the outermost #[pymodule] block in src/py_export/mod.rs.
# Implied by `pyo3`. Should never be enabled without `pyo3-types`.
pymodule-entry = []

stub_gen = ["pyo3-types", "pyo3-stub-gen"]
```

- [ ] **Step 2: Update `src/lib.rs` cfg gates**

In `gaussian-job-shared2/src/lib.rs`, replace:

```rust
#[cfg(feature = "pyo3")]
pub mod py_export;
#[cfg(feature = "pyo3")]
pub use py_export::stub_info;
```

with:

```rust
#[cfg(feature = "pyo3-types")]
pub mod py_export;

// The `stub_info` symbol is generated by the
// `pyo3_stub_gen::define_stub_info_gatherer!` macro inside
// `py_export::mod` under the `stub_gen` feature; surface it here only
// when stub generation is enabled.
#[cfg(feature = "stub_gen")]
pub use py_export::stub_info;
```

- [ ] **Step 3: Wrap the outermost `#[pymodule]` block under `pymodule-entry`**

In `gaussian-job-shared2/src/py_export/mod.rs`, replace the entire file
contents with:

```rust
#![cfg(feature = "pyo3-types")]

pub mod entities;
pub mod error;

// Stub-info gatherer (collects all #[gen_stub_*] annotations across
// the crate). Only useful when the wheel is being built with stub
// generation enabled.
#[cfg(feature = "stub_gen")]
pyo3_stub_gen::define_stub_info_gatherer!(stub_info);

// The outermost `_core` pymodule entry point. Compiled only when
// `pymodule-entry` is enabled — downstream library consumers
// (e.g. slurm-async-runner2 with `features = ["pyo3-types"]`)
// link the pyclass definitions but NOT `PyInit__core`, so they can
// expose their own pymodule without a duplicate-symbol collision.
#[cfg(feature = "pymodule-entry")]
mod pymodule_entry {
    use pyo3::prelude::*;

    /// A Python module implemented in Rust.
    #[pymodule]
    #[pyo3(name = "_core")]
    mod gaussian_job_shared {
        // TODO: constcat const PYTHON_LIBRARY_NAME: &str = "gaussian_job_shared";
        const PYTHON_MODULE_NAME: &str = "gaussian_job_shared._core";

        #[pymodule_export]
        use crate::py_export::entities::inner_module;

        // ---- legacy demo function ----
        #[pymodule_export]
        use super::sum_as_string;

        #[pymodule_init]
        fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
            let py = m.py();
            py.import("sys")?
                .getattr("modules")?
                .set_item(PYTHON_MODULE_NAME, m)?;
            log::debug!("{} Rust module initialized", PYTHON_MODULE_NAME);
            Ok(())
        }
    }

    /// Formats the sum of two numbers as string.
    #[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "gaussian_job_shared._core")]
    #[pyfunction]
    fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
        Ok((a + b).to_string())
    }
}
```

The pyclass-bearing modules (`entities` and `error`) stay outside the
`pymodule_entry` cfg block, so they compile under `pyo3-types` alone.

- [ ] **Step 4: Verify `--no-default-features` (pure Rust, no pyo3) still builds**

```bash
cd /home/kiyama/programs/research/GAUSSIAN_repo_packages/gaussian-job-shared2
cargo build --no-default-features 2>&1 | tail -10
```

Expected: clean build.

- [ ] **Step 5: Verify `pyo3-types` alone builds**

```bash
cargo build --no-default-features --features pyo3-types 2>&1 | tail -10
```

Expected: clean build. The pyclass defs compile but the outermost
`#[pymodule]` block does not, so no `PyInit__core` symbol is emitted.

- [ ] **Step 6: Verify default features build still works**

```bash
cargo build 2>&1 | tail -10
cargo test 2>&1 | tail -10
```

Expected: clean build, all tests pass.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml src/lib.rs src/py_export/mod.rs
git commit -m "refactor: split pyo3 feature into pyo3-types + pymodule-entry

Allow downstream library consumers to depend on this crate with
pyclass definitions only, avoiding a duplicate PyInit__core symbol
when they link their own #[pymodule].

* pyo3-types — pyclass/pymethods (etc.) only.
* pymodule-entry — the outermost #[pymodule] _core block.
* pyo3 (default) — both, equivalent to the previous behaviour.

The wheel build (default features) is unchanged. stub_gen now implies
pyo3-types since the stub-info gatherer needs the pyclass defs."
```

---

## Task A5: Rename `_core` pymodule to `_gaussian_job_shared_core`

**Files:**
- Modify: `gaussian-job-shared2/pyproject.toml`
- Modify: `gaussian-job-shared2/python/gaussian_job_shared/__init__.py`
- Modify: `gaussian-job-shared2/python/gaussian_job_shared/_core/` → rename to `_gaussian_job_shared_core/`
- Modify: 12 files under `src/py_export/` (PYTHON_MODULE_NAME constants and `module = "..."` attributes)
- Modify: `gaussian-job-shared2/tests/test_resource_spec_kwargs.py`

The linker symbol for shared2's pymodule was previously `PyInit__core`,
which collides with `slurm-async-runner2`'s same-named symbol. Rename
both Python module path and Rust pymodule name to a package-prefixed
unique name.

- [ ] **Step 1: Update the outermost `#[pymodule]` name in `src/py_export/mod.rs`**

In the file rewritten in Task A4 Step 3, change:

```rust
    #[pymodule]
    #[pyo3(name = "_core")]
    mod gaussian_job_shared {
        const PYTHON_MODULE_NAME: &str = "gaussian_job_shared._core";
```

to:

```rust
    #[pymodule]
    #[pyo3(name = "_gaussian_job_shared_core")]
    mod gaussian_job_shared {
        const PYTHON_MODULE_NAME: &str = "gaussian_job_shared._gaussian_job_shared_core";
```

Also update the `gen_stub_pyfunction(module = "gaussian_job_shared._core")`
on `sum_as_string` to
`gen_stub_pyfunction(module = "gaussian_job_shared._gaussian_job_shared_core")`.

- [ ] **Step 2: Update nested `inner_module` PYTHON_MODULE_NAME constants and pyclass `module = ...` attributes**

```bash
cd /home/kiyama/programs/research/GAUSSIAN_repo_packages/gaussian-job-shared2
git ls-files -- 'src/py_export/**/*.rs' | xargs sed -i \
  -e 's/gaussian_job_shared\._core/gaussian_job_shared._gaussian_job_shared_core/g'
```

Verify the rewrite:

```bash
git grep '"_core"\|\._core' -- src/py_export/
```

Expected: only matches inside string fragments where `_core` is part
of the new `_gaussian_job_shared_core` token. There should be no bare
`_core` left except for the `#[pyo3(name = "_core")]` attribute that
was already updated in Step 1 — re-run the same `sed` if any other bare
matches remain (verify by re-reading the file or by running:
`git grep '"_core"' -- src/py_export/`; expected: no matches).

- [ ] **Step 3: Update `pyproject.toml` module-name**

In `gaussian-job-shared2/pyproject.toml`, change:

```toml
module-name = "gaussian_job_shared._core"
```

to:

```toml
module-name = "gaussian_job_shared._gaussian_job_shared_core"
```

- [ ] **Step 4: Rename the Python stub directory**

```bash
git mv python/gaussian_job_shared/_core python/gaussian_job_shared/_gaussian_job_shared_core
```

- [ ] **Step 5: Update `python/gaussian_job_shared/__init__.py`**

Replace:

```python
from gaussian_job_shared import _core

if hasattr(_core, "__doc__"):
    __doc__ = _core.__doc__
if hasattr(_core, "__all__"):
    __all__ = _core.__all__
```

with:

```python
from gaussian_job_shared import _gaussian_job_shared_core as _core

if hasattr(_core, "__doc__"):
    __doc__ = _core.__doc__
if hasattr(_core, "__all__"):
    __all__ = _core.__all__
```

- [ ] **Step 6: Update the kwargs Python test from Task A3**

In `gaussian-job-shared2/tests/test_resource_spec_kwargs.py`, change:

```python
from gaussian_job_shared._core.entities.slurm.sbatch_options import (
```

to:

```python
from gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options import (
```

- [ ] **Step 7: Build, regenerate stubs, and run tests**

```bash
cargo build 2>&1 | tail -5
maturin develop 2>&1 | tail -5
pytest tests/ -v 2>&1 | tail -15
```

Expected: clean cargo build, maturin builds the wheel as
`gaussian_job_shared._gaussian_job_shared_core`, tests pass.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor!: rename pymodule _core to _gaussian_job_shared_core

The linker symbol PyInit__core collides with downstream pyo3 crates
that also expose a top-level _core (e.g. slurm-async-runner2). Rename
both the Rust #[pymodule] name and the Python import path to a
package-prefixed unique name; add a local 'as _core' alias in
python/__init__.py so existing in-module code that references _core
keeps working.

BREAKING CHANGE: Python users must update imports from
gaussian_job_shared._core.* to
gaussian_job_shared._gaussian_job_shared_core.*."
```

---

## Task A6: Push the shared2 branch and capture the head commit

**Files:**
- (no edits — git operation only)

- [ ] **Step 1: Push the branch to the remote**

```bash
cd /home/kiyama/programs/research/GAUSSIAN_repo_packages/gaussian-job-shared2
git push -u origin relax-resource-spec-and-feature-split
```

Expected: branch published.

- [ ] **Step 2: Record the head commit hash for Phase B's Cargo.toml pin**

```bash
git rev-parse HEAD | tee /tmp/shared2_head_commit.txt
```

Expected: a 40-char SHA on stdout. Phase B reads this file to pin the
dependency.

---

# Phase B: slurm-async-runner2 changes

All Phase B tasks run inside the worktree at
`/home/kiyama/programs/research/GAUSSIAN_repo_packages/slurm-async-runner2/.dmux/worktrees/merge-tssrun-struct/`.

## Task B1: Re-pin the `gaussian_job_shared` dependency on `pyo3-types`

**Files:**
- Modify: `slurm-async-runner2/.dmux/worktrees/merge-tssrun-struct/Cargo.toml`

- [ ] **Step 1: Read the recorded shared2 commit hash**

```bash
SHARED2_REV=$(cat /tmp/shared2_head_commit.txt)
echo "shared2 head: $SHARED2_REV"
```

Expected: a 40-char SHA prints.

- [ ] **Step 2: Update the dependency line**

In `Cargo.toml`, replace:

```toml
gaussian_job_shared = { git = "https://github.com/kkiyama117/gaussian_job_shared", branch = "main", default-features = false }
```

with (substituting the actual SHA captured above where `PASTE_SHARED2_HEAD_HERE`
appears):

```toml
gaussian_job_shared = { git = "https://github.com/kkiyama117/gaussian_job_shared", rev = "PASTE_SHARED2_HEAD_HERE", default-features = false, features = ["pyo3-types"] }
```

Pinning by `rev =` is more stable than `branch =` for an in-flight
branch.

- [ ] **Step 3: Update the comment block above the dependency**

Replace the existing comment in `Cargo.toml` (the block discussing
`default-features = false` and "duplicate symbol PyInit__core") with:

```toml
# Shared Gaussian job entities (Job, JobSpec, SlurmJobConfig, JobStatus,
# JobState, JobReason, ResourceSpec, JobTimeLimit). We enable
# `pyo3-types` so the pyclass wrappers (PyResourceSpec, PyJobTimeLimit,
# etc.) are linked into our extension, but NOT the upstream
# `pymodule-entry` feature — that would emit a
# `PyInit__gaussian_job_shared_core` symbol that we don't want in our
# `.so`. The shared2 wheel still ships its own
# `_gaussian_job_shared_core` extension separately for direct Python
# users.
```

- [ ] **Step 4: Refresh the lockfile and verify the build state**

```bash
cd /home/kiyama/programs/research/GAUSSIAN_repo_packages/slurm-async-runner2/.dmux/worktrees/merge-tssrun-struct
cargo update -p gaussian_job_shared 2>&1 | tail -5
cargo build 2>&1 | tail -25
```

Expected: build errors limited to the existing local `Resource` /
`PyResource` types (still in source). The dependency switch alone does
not introduce new errors elsewhere.

- [ ] **Step 5: Commit (intentionally leaving the build broken)**

```bash
git add Cargo.toml Cargo.lock
git commit -m "deps(shared2): pin to relax-resource-spec branch with pyo3-types

Switch the gaussian_job_shared dependency to features = [\"pyo3-types\"]
so pyclass wrappers come along for re-export but the
PyInit__gaussian_job_shared_core entry point does not.

This commit intentionally leaves the crate non-building — Resource and
PyResource still occupy the slots that ResourceSpec/PyResourceSpec are
about to take. Subsequent commits in this branch fix that."
```

---

## Task B2: Replace `Resource` with `ResourceSpec` in `tssrun::cmd`

**Files:**
- Modify: `src/tssrun/cmd.rs`

- [ ] **Step 1: Add the new tests for the relaxed `TssrunCmd`**

In `src/tssrun/cmd.rs`, inside the existing `#[cfg(test)] mod tests { ... }`
block, add:

```rust
    #[test]
    fn cmd_full_flags_cpu_variant() {
        use std::num::NonZeroU32;
        let mut c = TssrunCmd::new("/work/job.sh");
        c.partition = Some("gr19999b".into());
        c.time_limit = Some("1:0:0".parse().unwrap());
        c.rsc = Some(ResourceSpec::CPU(ResourceSpecCPU {
            p: NonZeroU32::new(4),
            t: NonZeroU32::new(8),
            c: NonZeroU32::new(8),
            m: Some("2G".parse().unwrap()),
        }));
        c.x11 = true;
        c.args = vec!["--flag".into(), "value".into()];
        let argv = c.build_argv().unwrap();
        assert_eq!(
            argv,
            vec![
                "tssrun".to_string(),
                "-p".to_string(),
                "gr19999b".to_string(),
                "-t".to_string(),
                "01:00:00".to_string(),
                "--rsc".to_string(),
                "p=4:t=8:c=8:m=2G".to_string(),
                "--x11".to_string(),
                "/work/job.sh".to_string(),
                "--flag".to_string(),
                "value".to_string(),
            ]
        );
    }

    #[test]
    fn cmd_full_flags_gpu_variant() {
        use std::num::NonZeroU32;
        let mut c = TssrunCmd::new("/work/job.sh");
        c.rsc = Some(ResourceSpec::GPU(ResourceSpecGPU {
            g: NonZeroU32::new(1).unwrap(),
        }));
        let argv = c.build_argv().unwrap();
        assert!(argv.contains(&"--rsc".to_string()));
        assert!(argv.contains(&"g=1".to_string()));
    }

    #[test]
    fn cmd_rsc_partial_cpu_emits_only_some_keys() {
        use std::num::NonZeroU32;
        let mut c = TssrunCmd::new("/work/job.sh");
        c.rsc = Some(ResourceSpec::CPU(ResourceSpecCPU {
            p: NonZeroU32::new(4),
            m: Some("2G".parse().unwrap()),
            ..Default::default()
        }));
        let argv = c.build_argv().unwrap();
        assert!(argv.contains(&"--rsc".to_string()));
        assert!(argv.contains(&"p=4:m=2G".to_string()));
    }

    #[test]
    fn cmd_rsc_empty_cpu_omits_flag() {
        // Some(ResourceSpec::CPU(default)) renders to "" → omit --rsc.
        let mut c = TssrunCmd::new("/work/job.sh");
        c.rsc = Some(ResourceSpec::CPU(ResourceSpecCPU::default()));
        let argv = c.build_argv().unwrap();
        assert!(!argv.contains(&"--rsc".to_string()));
    }

    #[test]
    fn cmd_rsc_none_omits_flag() {
        let c = TssrunCmd::new("/work/job.sh");
        let argv = c.build_argv().unwrap();
        assert!(!argv.contains(&"--rsc".to_string()));
    }
```

- [ ] **Step 2: Delete the obsolete `Resource` struct and its tests**

In `src/tssrun/cmd.rs`, delete the entire `Resource` block (definition
+ `impl Resource`) and the now-obsolete tests
`resource_default_renders_none`, `resource_full_renders_in_order`,
`resource_partial_skips_none_keys`, `cmd_full_flags_in_documented_order`,
`cmd_rsc_with_only_some_keys`, `cmd_rsc_all_none_omits_flag_entirely`.

- [ ] **Step 3: Add the new imports at the top of `cmd.rs`**

Insert after the existing
`use std::collections::HashMap;`,
`use std::path::{Path, PathBuf};`
(and before `use anyhow::{Context, Result};`):

```rust
use gaussian_job_shared::entities::slurm::{
    JobPartition, JobTimeLimit, ResourceSpec, ResourceSpecCPU, ResourceSpecGPU,
};
```

- [ ] **Step 4: Replace the `TssrunCmd` struct definition**

Replace:

```rust
#[derive(Debug, Clone)]
pub struct TssrunCmd {
    pub tssrun_bin: String,
    pub queue: Option<String>,
    pub time_limit: Option<String>,
    pub rsc: Option<Resource>,
    pub x11: bool,
    pub program: PathBuf,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub cwd: Option<PathBuf>,
}
```

with:

```rust
#[derive(Debug, Clone)]
pub struct TssrunCmd {
    pub tssrun_bin: String,
    /// Renamed from `queue` to match Slurm's `--partition` vocabulary.
    /// `JobPartition` is a `String` alias from `gaussian_job_shared`.
    pub partition: Option<JobPartition>,
    /// Validated wall-clock limit. Was `Option<String>` previously.
    pub time_limit: Option<JobTimeLimit>,
    /// Validated `--rsc` spec. Was the local `Resource` previously.
    pub rsc: Option<ResourceSpec>,
    pub x11: bool,
    pub program: PathBuf,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub cwd: Option<PathBuf>,
}
```

- [ ] **Step 5: Update `TssrunCmd::new`**

Replace:

```rust
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            tssrun_bin: "tssrun".to_string(),
            queue: None,
            time_limit: None,
            rsc: None,
            x11: false,
            program: program.into(),
            args: Vec::new(),
            env: HashMap::new(),
            cwd: None,
        }
    }
```

with:

```rust
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            tssrun_bin: "tssrun".to_string(),
            partition: None,
            time_limit: None,
            rsc: None,
            x11: false,
            program: program.into(),
            args: Vec::new(),
            env: HashMap::new(),
            cwd: None,
        }
    }
```

- [ ] **Step 6: Replace `build_argv`**

Replace:

```rust
    pub fn build_argv(&self) -> Result<Vec<String>> {
        const MAX_PRELUDE_SLOTS: usize = 8;
        let mut argv: Vec<String> = Vec::with_capacity(MAX_PRELUDE_SLOTS + self.args.len());
        argv.push(self.tssrun_bin.clone());

        if let Some(q) = &self.queue {
            argv.push("-p".to_string());
            argv.push(q.clone());
        }
        if let Some(t) = &self.time_limit {
            argv.push("-t".to_string());
            argv.push(t.clone());
        }
        if let Some(r) = &self.rsc
            && let Some(spec) = r.render()
        {
            argv.push("--rsc".to_string());
            argv.push(spec);
        }
        if self.x11 {
            argv.push("--x11".to_string());
        }

        argv.push(absolutize(&self.program)?);

        for a in &self.args {
            argv.push(a.clone());
        }
        Ok(argv)
    }
```

with:

```rust
    pub fn build_argv(&self) -> Result<Vec<String>> {
        // Maximum prelude slots: bin + (-p PARTITION) + (-t TIME) + (--rsc SPEC)
        // + --x11 + program = 8.
        const MAX_PRELUDE_SLOTS: usize = 8;
        let mut argv: Vec<String> = Vec::with_capacity(MAX_PRELUDE_SLOTS + self.args.len());
        argv.push(self.tssrun_bin.clone());

        if let Some(p) = &self.partition {
            argv.push("-p".to_string());
            argv.push(p.clone());
        }
        if let Some(t) = &self.time_limit {
            argv.push("-t".to_string());
            argv.push(t.to_string());
        }
        if let Some(r) = &self.rsc {
            // Display emits "" for the all-None CPU case; treat that
            // identically to `rsc: None` (omit `--rsc` entirely).
            let spec = r.to_string();
            if !spec.is_empty() {
                argv.push("--rsc".to_string());
                argv.push(spec);
            }
        }
        if self.x11 {
            argv.push("--x11".to_string());
        }

        argv.push(absolutize(&self.program)?);

        for a in &self.args {
            argv.push(a.clone());
        }
        Ok(argv)
    }
```

- [ ] **Step 7: Run the cmd.rs tests**

```bash
cargo test --lib tssrun::cmd 2>&1 | tail -20
```

Expected: all kept tests + the five new tests pass.

- [ ] **Step 8: Commit**

```bash
git add src/tssrun/cmd.rs
git commit -m "refactor(tssrun)!: replace local Resource with shared2 ResourceSpec

Adopt gaussian_job_shared::entities::slurm::ResourceSpec (CPU/GPU enum
with NonZeroU32-backed counts, partial-CPU permitted per the KUDPC
manual). Retype TssrunCmd::time_limit to Option<JobTimeLimit> for
the canonical HH:MM:SS Display form. Rename TssrunCmd::queue to
partition to match Slurm's documented \`-p\`/\`--partition\` flag and
SlurmJobConfig vocabulary.

build_argv now leans on Display::to_string() for both time_limit and
rsc; the empty-Display CPU case is treated identically to None and
omits --rsc.

BREAKING CHANGE: Rust callers must use ResourceSpec/JobTimeLimit and
the partition field name. Python wrapper updated next."
```

---

## Task B3: Update `lib.rs` re-exports

**Files:**
- Modify: `src/lib.rs`

- [ ] **Step 1: Locate the existing re-export of `Resource`**

```bash
grep -n 'Resource\|TssrunCmd' src/lib.rs
```

Expected match:

```rust
pub use tssrun::cmd::{Resource, TssrunCmd};
```

- [ ] **Step 2: Replace the re-export line**

In `src/lib.rs`, replace:

```rust
pub use tssrun::cmd::{Resource, TssrunCmd};
```

with:

```rust
pub use tssrun::cmd::TssrunCmd;
// Re-export shared2's ResourceSpec / JobTimeLimit so downstream Rust
// callers can write `use slurm_async_runner::ResourceSpec;` without a
// direct dependency on `gaussian_job_shared`. The `Resource` struct
// previously lived in `tssrun::cmd`; consult the migration notes for
// the field shape change (CPU/GPU enum, NonZeroU32 counts, Memory).
pub use gaussian_job_shared::entities::slurm::{
    JobPartition, JobTimeLimit, Memory, MemoryUnit, ResourceSpec, ResourceSpecCPU,
    ResourceSpecGPU,
};
```

- [ ] **Step 3: Build and run the lib test suite**

```bash
cargo build 2>&1 | tail -10
cargo test --lib 2>&1 | tail -15
```

Expected: clean build, all unit tests pass. Integration tests under
`tests/` may still fail at this point; addressed in Task B5.

- [ ] **Step 4: Commit**

```bash
git add src/lib.rs
git commit -m "refactor: re-export ResourceSpec from shared2 in lib root

Replace the deleted Resource re-export with the shared2 entities that
consumers most commonly need (ResourceSpec, ResourceSpecCPU/GPU,
Memory, MemoryUnit, JobTimeLimit, JobPartition)."
```

---

## Task B4: Update the Python wrapper (`py_export/tssrun.rs`)

**Files:**
- Modify: `src/py_export/tssrun.rs`

Drop `PyResource`, switch `PyTssrunCmd::__new__` to take shared2's
`PyResourceSpec` / `PyJobTimeLimit`, and re-export shared2's pyclass
wrappers from this crate's `tssrun` pymodule.

- [ ] **Step 1: Update the use-statements at the top of `py_export/tssrun.rs`**

Replace:

```rust
use crate::tssrun::cmd::{Resource, TssrunCmd};
```

with:

```rust
use crate::tssrun::cmd::TssrunCmd;
use gaussian_job_shared::py_export::entities::slurm::sbatch_options::resource_spec::PyResourceSpec;
use gaussian_job_shared::py_export::entities::slurm::sbatch_options::time_limit::PyJobTimeLimit;
```

- [ ] **Step 2: Delete the entire `PyResource` pyclass block**

Delete the section spanning roughly lines 22–72:

```rust
// ---------- Resource ----------

#[pyclass(
    name = "Resource",
    module = "slurm_async_runner._core.tssrun",
    from_py_object,
    frozen
)]
#[derive(Clone)]
pub struct PyResource(pub Resource);

#[pymethods]
impl PyResource {
    #[new]
    #[pyo3(signature = (processes = None, threads = None, cores = None, memory = None, gpus = None))]
    fn new(
        processes: Option<u32>,
        threads: Option<u32>,
        cores: Option<u32>,
        memory: Option<String>,
        gpus: Option<u32>,
    ) -> Self {
        Self(Resource {
            processes,
            threads,
            cores,
            memory,
            gpus,
        })
    }
    #[getter]
    fn processes(&self) -> Option<u32> {
        self.0.processes
    }
    // ... all other PyResource getters ...
}
```

(Delete from `// ---------- Resource ----------` down to and including
the closing `}` of the `impl PyResource {}` block. The next banner —
`// ---------- TssrunCmd ----------` — should immediately follow.)

- [ ] **Step 3: Update the `PyTssrunCmd::__new__` signature**

Replace:

```rust
#[pymethods]
impl PyTssrunCmd {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        program,
        args = Vec::new(),
        queue = None,
        time_limit = None,
        rsc = None,
        x11 = false,
        env = HashMap::new(),
        cwd = None,
        tssrun_bin = "tssrun".to_string(),
    ))]
    fn new(
        program: PathBuf,
        args: Vec<String>,
        queue: Option<String>,
        time_limit: Option<String>,
        rsc: Option<PyResource>,
        x11: bool,
        env: HashMap<String, String>,
        cwd: Option<PathBuf>,
        tssrun_bin: String,
    ) -> Self {
        Self(TssrunCmd {
            tssrun_bin,
            queue,
            time_limit,
            rsc: rsc.map(|r| r.0),
            x11,
            program,
            args,
            env,
            cwd,
        })
    }
```

with:

```rust
#[pymethods]
impl PyTssrunCmd {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        program,
        args = Vec::new(),
        partition = None,
        time_limit = None,
        rsc = None,
        x11 = false,
        env = HashMap::new(),
        cwd = None,
        tssrun_bin = "tssrun".to_string(),
    ))]
    fn new(
        program: PathBuf,
        args: Vec<String>,
        partition: Option<String>,
        time_limit: Option<PyJobTimeLimit>,
        rsc: Option<PyResourceSpec>,
        x11: bool,
        env: HashMap<String, String>,
        cwd: Option<PathBuf>,
        tssrun_bin: String,
    ) -> Self {
        Self(TssrunCmd {
            tssrun_bin,
            partition,
            time_limit: time_limit.map(|t| t.0),
            rsc: rsc.map(|r| r.0),
            x11,
            program,
            args,
            env,
            cwd,
        })
    }
```

- [ ] **Step 4: Update the `inner_module` `pymodule_export` list**

Inside the `#[pymodule] #[pyo3(name = "tssrun")] pub mod inner_module`
block at the bottom of `py_export/tssrun.rs`, replace:

```rust
    #[pymodule_export]
    use super::PyResource;
```

with:

```rust
    // Re-export shared2's ResourceSpec / JobTimeLimit pyclass wrappers
    // so Python callers can construct them via the same import path
    // as TssrunCmd. Both classes' canonical home remains
    // gaussian_job_shared._gaussian_job_shared_core, but exposing them
    // here saves an extra import for the common tssrun use case.
    #[pymodule_export]
    use gaussian_job_shared::py_export::entities::slurm::sbatch_options::resource_spec::PyResourceSpec;
    #[pymodule_export]
    use gaussian_job_shared::py_export::entities::slurm::sbatch_options::time_limit::PyJobTimeLimit;
```

- [ ] **Step 5: Run cargo test for the binary check**

```bash
cargo test --lib 2>&1 | tail -10
```

Expected: clean build, all Rust tests still pass.

- [ ] **Step 6: Commit**

```bash
git add src/py_export/tssrun.rs
git commit -m "refactor(py)!: re-export shared2 ResourceSpec/JobTimeLimit, drop PyResource

Replace the local PyResource (free-string memory, no validation) with
gaussian_job_shared::PyResourceSpec (validated CPU/GPU enum). PyTssrunCmd's
constructor now takes Option<PyJobTimeLimit> for time_limit and
Option<PyResourceSpec> for rsc. The tssrun submodule re-exports both
pyclass wrappers so Python callers can import everything tssrun-related
from one place.

BREAKING CHANGE: Python callers must construct TssrunCmd with
JobTimeLimit(...) and ResourceSpec(...) instead of strings. The 'queue'
kwarg is renamed 'partition'."
```

---

## Task B5: Update integration tests

**Files:**
- Modify (if needed): `tests/tssrun_integration.rs`

- [ ] **Step 1: Inspect existing references to changed fields**

```bash
grep -n 'queue\|time_limit\|rsc\|Resource' tests/tssrun_integration.rs
```

Note any line that references the renamed field, the old `Resource`,
or string-typed `time_limit`.

- [ ] **Step 2: Apply rename + retype edits inline**

For each line found in Step 1, rewrite the call site. The common patterns:

```rust
// Before
let mut cmd = TssrunCmd::new(&script_path);
cmd.queue = Some("gr19999b".into());
cmd.time_limit = Some("1:00:00".into());
cmd.rsc = Some(Resource { processes: Some(4), ..Default::default() });
```

```rust
// After
let mut cmd = TssrunCmd::new(&script_path);
cmd.partition = Some("gr19999b".into());
cmd.time_limit = Some("1:00:00".parse().unwrap());
cmd.rsc = Some(ResourceSpec::CPU(ResourceSpecCPU {
    p: std::num::NonZeroU32::new(4),
    ..Default::default()
}));
```

If `Resource` is not actually used in `tests/tssrun_integration.rs`,
no rewrites beyond the field renames are needed.

- [ ] **Step 3: Run the integration tests**

```bash
cargo test --test tssrun_integration 2>&1 | tail -10
```

Expected: tests pass (or skip on systems without `tssrun` installed —
integration tests historically gate on the binary's presence).

- [ ] **Step 4: Commit if changes were made**

```bash
git status tests/
# If there are unstaged changes:
git add tests/tssrun_integration.rs
git commit -m "test(tssrun): adapt integration test to renamed fields"
# Otherwise skip this step.
```

---

## Task B6: Rename `_core` pymodule to `_slurm_async_runner_core`

**Files:**
- Modify: `pyproject.toml`
- Modify: `python/slurm_async_runner/__init__.py`
- Modify: `python/slurm_async_runner/_core/` → rename to `_slurm_async_runner_core/`
- Modify: `src/py_export/mod.rs`, `src/py_export/manager.rs`, `src/py_export/runner.rs`, `src/py_export/tssrun.rs`

- [ ] **Step 1: Update the outermost `#[pymodule]` name in `src/py_export/mod.rs`**

In `src/py_export/mod.rs`, change:

```rust
#[pymodule]
#[pyo3(name = "_core")]
mod slurm_async_runner {
    use super::*;
    // TODO: constcat const PYTHON_LIBRARY_NAME: &str = "slurm_async_runner";
    const PYTHON_MODULE_NAME: &str = "slurm_async_runner._core";
```

to:

```rust
#[pymodule]
#[pyo3(name = "_slurm_async_runner_core")]
mod slurm_async_runner {
    use super::*;
    // TODO: constcat const PYTHON_LIBRARY_NAME: &str = "slurm_async_runner";
    const PYTHON_MODULE_NAME: &str = "slurm_async_runner._slurm_async_runner_core";
```

Also update the `gen_stub_pyfunction(module = "slurm_async_runner._core")`
attribute on `sum_as_string` to
`gen_stub_pyfunction(module = "slurm_async_runner._slurm_async_runner_core")`.

- [ ] **Step 2: Bulk-update nested `_core` references**

```bash
cd /home/kiyama/programs/research/GAUSSIAN_repo_packages/slurm-async-runner2/.dmux/worktrees/merge-tssrun-struct
git ls-files -- 'src/py_export/**/*.rs' | xargs sed -i \
  -e 's/slurm_async_runner\._core/slurm_async_runner._slurm_async_runner_core/g'
```

Verify:

```bash
git grep '"_core"\|\._core' -- src/
```

Expected: every match is now part of the new
`_slurm_async_runner_core` token. No bare `_core` should remain except
inside `_slurm_async_runner_core`.

- [ ] **Step 3: Update `pyproject.toml`**

Replace:

```toml
module-name = "slurm_async_runner._core"
```

with:

```toml
module-name = "slurm_async_runner._slurm_async_runner_core"
```

- [ ] **Step 4: Rename the Python stub directory**

```bash
git mv python/slurm_async_runner/_core python/slurm_async_runner/_slurm_async_runner_core
```

- [ ] **Step 5: Update `python/slurm_async_runner/__init__.py`**

Replace:

```python
from slurm_async_runner import _core

if hasattr(_core, "__doc__"):
    __doc__ = _core.__doc__
if hasattr(_core, "__all__"):
    __all__ = _core.__all__
```

with:

```python
from slurm_async_runner import _slurm_async_runner_core as _core

if hasattr(_core, "__doc__"):
    __doc__ = _core.__doc__
if hasattr(_core, "__all__"):
    __all__ = _core.__all__
```

- [ ] **Step 6: Build and smoke-test**

```bash
cargo build 2>&1 | tail -10
cargo test 2>&1 | tail -15
maturin develop 2>&1 | tail -5
python -c "from slurm_async_runner._slurm_async_runner_core.tssrun import TssrunCmd; print(TssrunCmd('/bin/true').build_argv())"
```

Expected: clean build, all tests pass, Python smoke command prints
`['tssrun', '/bin/true']`.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "refactor!: rename pymodule _core to _slurm_async_runner_core

Avoid collision with shared2's PyInit__gaussian_job_shared_core (which
itself was renamed from PyInit__core upstream). Both crates now expose
package-prefixed leaf names that cannot collide.

BREAKING CHANGE: Python users must update imports from
slurm_async_runner._core.* to slurm_async_runner._slurm_async_runner_core.*."
```

---

# Phase C: Cross-repo verification

## Task C1: Build wheels in both repos

**Files:**
- (no edits — verification only)

- [ ] **Step 1: Build the shared2 wheel**

```bash
cd /home/kiyama/programs/research/GAUSSIAN_repo_packages/gaussian-job-shared2
maturin build --release 2>&1 | tail -10
ls target/wheels/
```

Expected: a `.whl` is produced under `target/wheels/`.

- [ ] **Step 2: Build the slurm-async-runner2 wheel**

```bash
cd /home/kiyama/programs/research/GAUSSIAN_repo_packages/slurm-async-runner2/.dmux/worktrees/merge-tssrun-struct
maturin build --release 2>&1 | tail -10
ls target/wheels/
```

Expected: a `.whl` is produced. The build must succeed despite both
crates' pyo3 features being on — this verifies the linker symbols
don't collide.

- [ ] **Step 3: Verify no `_core` linker symbol clashes**

Find the produced shared object:

```bash
find target/wheels -name '*.whl' -exec sh -c 'unzip -l "$1" | grep -E "\\.(so|dylib|pyd)$"' _ {} \;
```

Pick the `.so`/`.dylib`/`.pyd` listed and inspect symbols (replace
`PATH_TO_SO` with one of the printed paths after extracting if needed):

```bash
WHEEL=$(ls target/wheels/slurm_async_runner-*.whl | head -1)
unzip -p "$WHEEL" '*.so' > /tmp/sar.so 2>/dev/null || \
  unzip -p "$WHEEL" '*.dylib' > /tmp/sar.so 2>/dev/null
nm /tmp/sar.so 2>/dev/null | grep PyInit_ || echo "(symbols not exposed via nm in this format)"
```

Expected: exactly one `PyInit_` line in this object,
`PyInit__slurm_async_runner_core`. There must be **no**
`PyInit__core`.

---

## Task C2: Python smoke test in a fresh venv

**Files:**
- Create (temporary): `/tmp/smoke_resourcespec.py`

- [ ] **Step 1: Install both wheels into a fresh venv**

```bash
cd /tmp
python -m venv smoke-venv
. smoke-venv/bin/activate
pip install \
  /home/kiyama/programs/research/GAUSSIAN_repo_packages/gaussian-job-shared2/target/wheels/gaussian_job_shared-*.whl \
  /home/kiyama/programs/research/GAUSSIAN_repo_packages/slurm-async-runner2/.dmux/worktrees/merge-tssrun-struct/target/wheels/slurm_async_runner-*.whl
```

- [ ] **Step 2: Write and run the smoke script**

Create `/tmp/smoke_resourcespec.py`:

```python
"""End-to-end check: shared2 types flow through slurm-async-runner."""
from gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options import (
    Memory,
    MemoryUnit,
    ResourceSpec,
    JobTimeLimit,
)
from slurm_async_runner._slurm_async_runner_core.tssrun import TssrunCmd

# ResourceSpec also re-exported via TssrunCmd's pymodule:
from slurm_async_runner._slurm_async_runner_core.tssrun import (
    ResourceSpec as ResourceSpec2,
)
assert ResourceSpec is ResourceSpec2

# Full CPU spec.
cmd = TssrunCmd(
    program="/work/job.sh",
    partition="gr19999b",
    time_limit=JobTimeLimit("1:00:00"),
    rsc=ResourceSpec(4, 8, 8, Memory("2G")),
)
argv = cmd.build_argv()
print(argv)
assert "--rsc" in argv
assert "p=4:t=8:c=8:m=2G" in argv
assert "01:00:00" in argv
assert "gr19999b" in argv

# Partial CPU.
cmd2 = TssrunCmd(
    program="/work/job.sh",
    rsc=ResourceSpec(processes=60, threads=1, cores=1),
)
argv2 = cmd2.build_argv()
assert "p=60:t=1:c=1" in argv2

# GPU.
cmd3 = TssrunCmd(program="/work/job.sh", rsc=ResourceSpec(gpus=1))
argv3 = cmd3.build_argv()
assert "g=1" in argv3

# Empty CPU → no --rsc.
cmd4 = TssrunCmd(program="/work/job.sh", rsc=ResourceSpec())
argv4 = cmd4.build_argv()
assert "--rsc" not in argv4

print("smoke ok")
```

Run:

```bash
python /tmp/smoke_resourcespec.py
```

Expected output: a printed argv list followed by `smoke ok`. No
assertion failures.

- [ ] **Step 3: Deactivate and clean up**

```bash
deactivate
rm -rf /tmp/smoke-venv /tmp/smoke_resourcespec.py
```

---

# Out of scope

The following are intentionally **not** addressed by this plan and remain
for follow-up work, per spec §7:

- A higher-level `TssrunCmd::from_slurm_config` conversion bridging
  `SlurmJobConfig` to a tssrun invocation.
- `SlurmDependency` / `SlurmArraySpec` / `MailType` integration into
  `tssrun` (none of those are consumed by the current frontend).
- An ergonomic builder API for `ResourceSpecCPU`
  (`ResourceSpecCPU::builder().processes(4)?.threads(8)?.build()`).
- Any change to `JobHandleSnapshot` persistence — it does not embed the
  retyped fields, so no migration is needed.
