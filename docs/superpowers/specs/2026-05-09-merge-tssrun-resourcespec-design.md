# Merging `tssrun::cmd::Resource` with `gaussian_job_shared::ResourceSpec`

**Date:** 2026-05-09
**Status:** Draft (under user review)
**Scope:** `slurm-async-runner2` and `gaussian-job-shared2` repositories

## 1. Background

`slurm-async-runner2` ships a thin wrapper around the Kyoto University KUDPC
`tssrun` interactive-batch frontend. The wrapper currently models the
`--rsc` resource list using a local `tssrun::cmd::Resource` struct with all
fields `Option<u32>` (and `memory: Option<String>`). The sibling crate
`gaussian-job-shared2` independently models the same KUDPC primitive as a
strict, fully-validated `ResourceSpec` enum (`CPU(...)` vs `GPU(...)`)
exposed under
`crate::entities::slurm::sbatch_options::resource_spec`.

These two representations duplicate logic and diverge in subtle ways —
`Resource` permits CPU+GPU key mixing (which the KUDPC manual does not
sanction), while `ResourceSpec` enforces all four CPU keys (which the
KUDPC manual lists as individually optional). The goal of this work is
to consolidate on a single representation that **matches the KUDPC
manual exactly**, share the type across both crates, and surface it
ergonomically in Python.

This consolidation is bundled with three adjacent moves:

1. Replace `TssrunCmd::time_limit: Option<String>` with
   `Option<JobTimeLimit>` (the validated `--time` type already in
   `gaussian-job-shared2`).
2. Rename `TssrunCmd::queue: Option<String>` to
   `partition: Option<JobPartition>` so vocabulary matches Slurm and
   `SlurmJobConfig`.
3. Rename the pyo3 entry-point pymodule in **both** crates from `_core`
   to `_<package_name>_core` so the linker symbols
   `PyInit__<package_name>_core` are globally unique. This unblocks
   reusing `gaussian-job-shared2`'s pyclass wrappers from
   `slurm-async-runner2` without a duplicate-symbol collision.

## 2. KUDPC `--rsc` specification (authoritative reference)

Source: <https://web.kudpc.kyoto-u.ac.jp/manual/ja/run/resource#rscoption>

| key | meaning | default | individually optional? |
|----|---------|---------|------------------------|
| `p` | MPI process count | 1 | yes |
| `t` | OpenMP threads per process | 1 | yes |
| `c` | cores per process | 1 | yes |
| `m` | memory per process | system-dependent | yes |
| `g` | GPU count | (no default) | only on GPU partitions |

CPU and GPU forms are alternatives — the manual presents them with "or"
and never combines `g=` with the CPU keys. On GPU partitions, omitting
`g=` and supplying `p/t/c/m` causes the system to auto-allocate one GPU
per 16 cores; on CPU-only specs, `g=` must not appear.

Manual examples:

- `p=1:t=4:c=4:m=8G` (OpenMP, 4 threads)
- `p=60:t=1:c=1` (MPI 60-way, no `m`)
- `p=6:t=4:c=4:m=20G` (hybrid)
- `p=1:t=224:c=112:m=500G` (hyperthreading)
- `g=1` (single GPU)

**Implications for the type system:**

- CPU spec must accept any subset of `{p, t, c, m}`, including the empty
  subset (which means "use all defaults") — see §3.2.
- GPU spec is exactly `{g}`.
- Mixing CPU keys with `g=` is invalid.
- Counts are positive (`NonZeroU32`); memory is positive integer +
  optional unit suffix.

## 3. Design

### 3.1 Repository changes overview

```
gaussian-job-shared2/        (upstream library)
├─ Cargo.toml
│  └─ split feature `pyo3` into `pyo3-types` + `pymodule-entry`
├─ src/lib.rs
│  └─ gate `py_export` on `pyo3-types`, `stub_info` on `stub_gen`
├─ src/py_export/mod.rs
│  ├─ rename pymodule `_core` → `_gaussian_job_shared_core`
│  └─ wrap outermost `#[pymodule]` block in `cfg(feature = "pymodule-entry")`
├─ src/entities/slurm/sbatch_options/resource_spec.rs
│  └─ relax `ResourceSpecCPU` to `Option<NonZeroU32>` / `Option<Memory>`
├─ src/py_export/entities/slurm/sbatch_options/resource_spec.rs
│  └─ make `PyResourceSpec.__new__` positional/kwarg with Optional args,
│     add `from_str` classmethod
└─ pyproject.toml
   └─ update `[tool.maturin] module-name`

slurm-async-runner2/         (this crate)
├─ Cargo.toml
│  └─ depend on shared2 with `default-features = false, features = ["pyo3-types"]`
├─ src/lib.rs
│  └─ stop re-exporting `Resource`; re-export `ResourceSpec` from shared2
├─ src/tssrun/cmd.rs
│  ├─ delete `Resource` struct
│  ├─ rename `TssrunCmd::queue` → `partition` (type `JobPartition`)
│  ├─ retype `TssrunCmd::time_limit` to `Option<JobTimeLimit>`
│  ├─ retype `TssrunCmd::rsc` to `Option<ResourceSpec>`
│  └─ rewrite `build_argv` to use `Display::to_string()`
├─ src/py_export/tssrun.rs
│  ├─ delete `PyResource`
│  ├─ re-export shared2's `PyResourceSpec` / `PyJobTimeLimit`
│  └─ adjust `PyTssrunCmd::__new__` signature to accept the new types
└─ src/py_export/mod.rs (or equivalent top-level pymodule file)
   └─ rename pymodule `_core` → `_slurm_async_runner_core`
```

### 3.2 `ResourceSpec` type relaxation (shared2)

```rust
// gaussian-job-shared2/src/entities/slurm/sbatch_options/resource_spec.rs

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceSpec {
    CPU(ResourceSpecCPU),
    GPU(ResourceSpecGPU),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceSpecCPU {
    pub p: Option<NonZeroU32>,
    pub t: Option<NonZeroU32>,
    pub c: Option<NonZeroU32>,
    pub m: Option<Memory>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceSpecGPU {
    pub g: NonZeroU32,
}
```

Validity matrix:

| `p` | `t` | `c` | `m` | `g` | result |
|----|----|----|----|----|--------|
| any | any | any | any | None | `Ok(CPU{...})` (any subset, incl. all-None) |
| None | None | None | None | Some | `Ok(GPU{g})` |
| ≥1 Some on left | any | Some | error: CPU/GPU mixed |

### 3.3 `Display` and `FromStr`

`Display` emits only `Some` keys, in the canonical order `p, t, c, m`,
joined by `:`. The all-None CPU produces an empty string. GPU emits
`g={g}`.

```rust
impl std::fmt::Display for ResourceSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResourceSpec::CPU(c) => {
                let mut parts = Vec::with_capacity(4);
                if let Some(p) = c.p { parts.push(format!("p={p}")); }
                if let Some(t) = c.t { parts.push(format!("t={t}")); }
                if let Some(cc) = c.c { parts.push(format!("c={cc}")); }
                if let Some(m) = &c.m { parts.push(format!("m={m}")); }
                write!(f, "{}", parts.join(":"))
            }
            ResourceSpec::GPU(g) => write!(f, "g={}", g.g),
        }
    }
}
```

`FromStr` accepts:

- non-empty CPU subsets: `"p=60:t=1:c=1"`, `"m=8G"`, `"p=1:t=4:c=4:m=8G"`
- GPU spec: `"g=1"`

`FromStr` rejects:

- empty string `""`
- duplicate keys (`"p=1:p=2"`)
- mixed CPU + GPU keys (`"p=1:g=1"`)
- unknown keys, malformed tokens, zero counts, zero or invalid memory

The all-None CPU value is constructible programmatically (or via the
Python kwargs constructor) but is intentionally **not** parseable from
the empty string. Display + FromStr round-trip therefore holds for
every CPU value with at least one `Some` field, and for every GPU
value.

### 3.4 Python wrapper API (shared2)

```python
# kwargs (or positional) — individual Optional fields
ResourceSpec(processes=4, threads=8, cores=8, memory="2G")    # full CPU
ResourceSpec(60, 1, 1)                                         # partial CPU (no m)
ResourceSpec(memory="8G")                                      # m only
ResourceSpec()                                                 # CPU all-None (renders to "")
ResourceSpec(gpus=1)                                           # GPU
ResourceSpec(processes=4, gpus=1)                              # ValueError: mixed

# string parsing (KUDPC canonical surface form)
ResourceSpec.from_str("p=60:t=1:c=1")
ResourceSpec.from_str("g=1")

# type-explicit constructors (existing, kept)
ResourceSpec.cpu(ResourceSpecCPU(p=4, t=8, c=8, m=Memory("2G")))
ResourceSpec.gpu(ResourceSpecGPU(g=1))

# Display
str(ResourceSpec(60, 1, 1))                # "p=60:t=1:c=1"
str(ResourceSpec(gpus=1))                  # "g=1"
str(ResourceSpec())                        # ""  (use this if you want --rsc skipped)
```

The Rust wrapper performs `NonZeroU32::new(...)` for each integer
keyword and parses memory via `Memory::from_str`. All errors surface as
`ValueError`.

### 3.5 Feature graph (shared2)

```toml
[features]
default = ["pyo3", "stub_gen"]

# Existing public name. Equivalent behaviour to the current default.
pyo3 = ["pyo3-types", "pymodule-entry"]

# New: pyclass definitions only — no `_gaussian_job_shared_core` pymodule
# entry, so a downstream crate that builds its own pymodule can pull in
# these types without a duplicate-symbol collision.
pyo3-types = ["dep:pyo3", "pyo3-async-runtimes", "pyo3-log", "pythonize"]

# New: gates the outermost `#[pymodule]` block. Implied by `pyo3`.
pymodule-entry = []

stub_gen = ["pyo3-types", "pyo3-stub-gen"]
```

`slurm-async-runner2` consumes `pyo3-types` only.

### 3.6 Pymodule rename details

| crate | old | new | linker symbol |
|-------|-----|-----|---------------|
| gaussian-job-shared2 | `gaussian_job_shared._core` | `gaussian_job_shared._gaussian_job_shared_core` | `PyInit__gaussian_job_shared_core` |
| slurm-async-runner2 | `slurm_async_runner._core` | `slurm_async_runner._slurm_async_runner_core` | `PyInit__slurm_async_runner_core` |

All nested `inner_module` `PYTHON_MODULE_NAME` constants and stub paths
must be updated in lockstep. `pyproject.toml`'s
`[tool.maturin] module-name` and any `__init__.py` shims that re-export
from the C extension must also be updated.

### 3.7 `TssrunCmd` shape after migration

```rust
// slurm-async-runner2/src/tssrun/cmd.rs

pub struct TssrunCmd {
    pub tssrun_bin: String,
    pub partition:  Option<JobPartition>,    // String alias; was `queue`
    pub time_limit: Option<JobTimeLimit>,    // was Option<String>
    pub rsc:        Option<ResourceSpec>,    // was Option<Resource>
    pub x11:        bool,
    pub program:    PathBuf,
    pub args:       Vec<String>,
    pub env:        HashMap<String, String>,
    pub cwd:        Option<PathBuf>,
}

impl TssrunCmd {
    pub fn build_argv(&self) -> Result<Vec<String>> {
        let mut argv: Vec<String> = Vec::with_capacity(MAX_PRELUDE_SLOTS + self.args.len());
        argv.push(self.tssrun_bin.clone());

        if let Some(p) = &self.partition {
            argv.push("-p".into());
            argv.push(p.clone());
        }
        if let Some(t) = &self.time_limit {
            argv.push("-t".into());
            argv.push(t.to_string());
        }
        if let Some(r) = &self.rsc {
            let s = r.to_string();
            if !s.is_empty() {
                argv.push("--rsc".into());
                argv.push(s);
            }
        }
        if self.x11 { argv.push("--x11".into()); }

        argv.push(absolutize(&self.program)?);
        for a in &self.args { argv.push(a.clone()); }
        Ok(argv)
    }
}
```

The empty-Display CPU (`Some(ResourceSpec::CPU(default()))`) is treated
identically to `None` for argv purposes — both omit `--rsc`. This
preserves the previous "all-None means no flag" behaviour for migrating
callers.

### 3.8 Python wrapper for `TssrunCmd` (this crate)

```python
from slurm_async_runner._slurm_async_runner_core.tssrun import TssrunCmd
from gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options.resource_spec import ResourceSpec
from gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options.time_limit import JobTimeLimit

cmd = TssrunCmd(
    program="/work/job.sh",
    partition="gr19999b",
    time_limit=JobTimeLimit("1:00:00"),
    rsc=ResourceSpec(60, 1, 1),     # partial CPU is fine
)
```

`TssrunCmd.__new__`'s `rsc=` parameter accepts `PyResourceSpec`,
`time_limit=` accepts `PyJobTimeLimit`, and `partition=` accepts `str`.

## 4. Error model

| condition | error type | source |
|-----------|------------|--------|
| zero count in any int field | `SchemaParseError::ParseError` (Rust) / `ValueError` (Python) | shared2 |
| invalid memory unit / zero memory | `SchemaParseError::ParseError` / `ValueError` | shared2 |
| empty string in `FromStr` | `SchemaParseError::ParseError` / `ValueError` | shared2 |
| mixed CPU + GPU keys (kwargs or string) | `SchemaParseError::ParseError` / `ValueError` | shared2 |
| duplicate keys in string | `SchemaParseError::ParseError` / `ValueError` | shared2 |
| non-UTF8 program path | `anyhow!` | tssrun (`absolutize`) |

All-None CPU is **not** an error. It produces an empty `Display` and
results in `--rsc` being omitted from the argv.

## 5. Testing strategy

### 5.1 shared2 tests

| status | test |
|--------|------|
| keep | `parses_kudpc_cpu_example`, `parses_cpu_spec_in_arbitrary_order`, `parses_kudpc_gpu_example`, `parses_unitless_memory_in_cpu_spec`, `rejects_empty_string`, `rejects_mixed_cpu_and_gpu_keys`, `rejects_unknown_keys`, `rejects_duplicate_keys`, `rejects_zero_counts`, `rejects_zero_memory`, `rejects_invalid_memory_unit`, `rejects_missing_equals_in_token`, `rejects_empty_value`, `rejects_dangling_separator`, all memory and TOML round-trip tests |
| delete | `rejects_partial_cpu_spec` (KUDPC permits partial) |
| add | `parses_kudpc_p60_t1_c1_example` (partial CPU from manual) |
| add | `parses_partial_cpu_spec_m_only` (`"m=8G"`) |
| add | `display_round_trips_partial_cpu` |
| add | `default_constructor_yields_all_none_cpu` (Python: `ResourceSpec()` → CPU all-None, `str()` is `""`) |
| add | `kwargs_constructor_rejects_mixed` (Python: `ResourceSpec(processes=4, gpus=1)` → ValueError) |
| add | `kwargs_constructor_zero_rejected` (Python: `ResourceSpec(gpus=0)` → ValueError) |
| add | `from_str_classmethod_works` (Python: `ResourceSpec.from_str("p=4:t=8:c=8:m=8G")`) |

### 5.2 slurm-async-runner2 tests

| status | test |
|--------|------|
| keep | `cmd_minimal_argv_is_bin_then_program`, `cmd_relative_program_is_absolutized` |
| delete | `resource_default_renders_none`, `resource_full_renders_in_order`, `resource_partial_skips_none_keys` (move responsibility to shared2 round-trip tests) |
| rewrite | `cmd_full_flags_in_documented_order` → split into `cmd_full_flags_cpu_variant` (CPU 4 keys) and `cmd_full_flags_gpu_variant` (GPU only) |
| rewrite | `cmd_rsc_with_only_some_keys` → use `ResourceSpec::CPU(ResourceSpecCPU { p: Some(...), m: Some(...), ..Default::default() })` |
| rewrite | `cmd_rsc_all_none_omits_flag_entirely` → assert that `Some(ResourceSpec::CPU(default()))` (empty Display) also omits `--rsc` |

### 5.3 Build matrix (CI / local verification)

| crate | feature flags | expected |
|-------|---------------|----------|
| shared2 | `--no-default-features` | builds; pure Rust types only |
| shared2 | `--no-default-features --features pyo3-types` | builds; pyclass available, no `_gaussian_job_shared_core` symbol |
| shared2 | default | builds; full wheel including `_gaussian_job_shared_core` |
| slurm-async-runner2 | default | builds; pulls shared2 with `pyo3-types` and exposes `_slurm_async_runner_core` |

A duplicate-symbol collision is the failure mode being designed against,
so the slurm-async-runner2 default build is the canonical regression
test.

### 5.4 Integration

`tests/tssrun_integration.rs` references `TssrunCmd::new(...)` only; the
field renames are transparent there. Any field accesses (`cmd.queue`)
must be updated to `cmd.partition` and any `cmd.time_limit = "..."`
must wrap the value in `JobTimeLimit::from_str(...)`.

## 6. Migration impact for callers

### 6.1 Rust

```rust
// before
use slurm_async_runner::{Resource, TssrunCmd};
let mut c = TssrunCmd::new("/work/job.sh");
c.queue = Some("gr19999b".into());
c.time_limit = Some("1:00:00".into());
c.rsc = Some(Resource {
    processes: Some(4), threads: Some(8), cores: Some(8),
    memory: Some("2G".into()), ..Default::default()
});

// after
use slurm_async_runner::TssrunCmd;
use gaussian_job_shared::entities::slurm::{
    ResourceSpec, ResourceSpecCPU, JobTimeLimit,
};
use std::num::NonZeroU32;

let mut c = TssrunCmd::new("/work/job.sh");
c.partition = Some("gr19999b".into());
c.time_limit = Some("1:00:00".parse()?);
c.rsc = Some(ResourceSpec::CPU(ResourceSpecCPU {
    p: NonZeroU32::new(4),
    t: NonZeroU32::new(8),
    c: NonZeroU32::new(8),
    m: Some("2G".parse()?),
}));
```

### 6.2 Python

```python
# before
from slurm_async_runner._core.tssrun import TssrunCmd, Resource
cmd = TssrunCmd(
    program="/work/job.sh", queue="gr19999b", time_limit="1:00:00",
    rsc=Resource(processes=4, threads=8, cores=8, memory="2G"),
)

# after
from slurm_async_runner._slurm_async_runner_core.tssrun import TssrunCmd
from gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options.resource_spec import ResourceSpec
from gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options.time_limit import JobTimeLimit

cmd = TssrunCmd(
    program="/work/job.sh", partition="gr19999b",
    time_limit=JobTimeLimit("1:00:00"),
    rsc=ResourceSpec(4, 8, 8, "2G"),
)
```

### 6.3 Persistence and on-disk state

`JobHandleSnapshot` (the only type currently serialised by
`FileSystemStateStore`) does not embed `Resource` or `TssrunCmd`, so no
on-disk migration is required.

## 7. Out of scope

- `SlurmDependency`, `SlurmArraySpec`, `MailType`, and `SlurmJobConfig`
  itself are not consumed by `tssrun` and remain untouched.
- `tssrun` has no concept of an array job, dependency, or stdout/stderr
  redirection through `--out` / `--err`; this design does not add them.
- A future PR may introduce a higher-level `TssrunCmd::from_slurm_config`
  conversion to bridge with `SlurmJobConfig`, but is not part of this
  consolidation.

## 8. Rollout order

1. Land shared2 changes (feature split, `ResourceSpec` relaxation,
   pymodule rename, `PyResourceSpec` API). Cut a new commit on `main`.
2. Bump the `gaussian_job_shared` git dependency in `slurm-async-runner2`
   `Cargo.toml` to that commit and switch to
   `default-features = false, features = ["pyo3-types"]`.
3. Apply `slurm-async-runner2` changes (delete `Resource`, rename
   fields, update Python wrapper, rename pymodule).
4. Update both `pyproject.toml`s and any Python `__init__.py` re-exports.
5. Run full test suites in both crates; verify wheel builds.
