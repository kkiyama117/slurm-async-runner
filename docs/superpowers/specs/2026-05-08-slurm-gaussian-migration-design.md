# slurm-async-runner: pure-Python → Rust+pyo3 + gaussian_job_shared migration

**Date:** 2026-05-08
**Branch:** `slurm-gaussian-migration` (worktree of `miyake-ken/slurm-async-runner`)
**Status:** approved design, awaiting implementation plan

## 1. Goal

Replace the current pure-Python implementation of `slurm-async-runner`
(this repo, `miyake-ken/slurm-async-runner`) with the Rust+pyo3 skeleton
that already lives in `kkiyama117/slurm-async-runner`
(parent dir: `slurm-async-runner2`), and add
`kkiyama117/gaussian_job_shared` (parent dir: `gaussian-job-shared2`)
as a Rust crate dependency.

The migration is performed **step by step** as a sequence of small
commits on the `slurm-gaussian-migration` branch. Each commit leaves
the package in a building, testable state.

## 2. Non-Goals

- Renaming the GitHub repo or changing the package name
  (`slurm_async_runner`).
- Changing the Python-facing async contract: `await
  manager.query_job_states_batch(...)` and `await manager.run_job(...)`
  remain async methods callable under `asyncio.run`.
- Touching `gaussian_job_shared` from this branch. The `SlurmJobState`
  move into `gaussian_job_shared` (S7) is performed in a separate
  Claude session in the `gaussian_job_shared` repository. This branch
  defines `SlurmJobState` locally for S3–S6.
- Introducing `Job` / `JobSpec` / `SlurmJobConfig` as inputs to
  `run_job`. The current contract — "the caller renders a bash file
  elsewhere and hands a `Path` to the runner" — is preserved.

## 3. Inputs & Reference Material

| Source | Role |
|--------|------|
| `src/slurm_async_runner/runner.py` (this repo, current branch) | Reference implementation for all algorithms to port. |
| `tests/test_runner.py`, `tests/test_query_job_states.py` (this repo, current branch) | Reference for behavioral contracts; existing tests will be deleted and rewritten as Rust unit tests + Python smoke tests. |
| `slurm-async-runner2/` (= `kkiyama117/slurm-async-runner`) | Source of the Rust+pyo3 skeleton (Cargo.toml, lib.rs, py_export/, stub_gen, pyproject.toml). |
| `gaussian-job-shared2/` (= `kkiyama117/gaussian_job_shared`) | Source of the new dependency. Reference for module layout (`_core.entities.slurm.*`), pyo3 patterns, and stub_gen wiring. |

## 4. Architecture

### 4.1 Three layers

| Layer | Responsibility | Rust location | Python surface |
|-------|----------------|---------------|----------------|
| Entities | `SlurmJobState` enum (PENDING/RUNNING/.../UNKNOWN, `parse(&str)`) | S3–S7: `src/entities/slurm_job_state.rs`<br>S8 onward: `pub use gaussian_job_shared::entities::slurm::SlurmJobState;` (one-line re-export) | `slurm_async_runner.SlurmJobState` |
| Query | `query_job_states_batch`, `query_job_state` (one `squeue` + at most one `sacct`) | `src/runner/query.rs`; parsers split out as `pub(crate) fn parse_squeue` / `parse_sacct` | `SlurmManager.query_job_states_batch(...)`, `SlurmManager.query_job_state(...)` |
| Dispatch | `run_job`, `SlurmCmd`, `SlurmManager`, `build_shell_str` | `src/runner/dispatch.rs`; `build_shell_str` lives in `src/runner/mod.rs` as a pure `pub(crate) fn` and is also wrapped as a `SlurmManager` method on the pyo3 side | `SlurmManager.run_job(...)`, `SlurmManager.srun_command(...)`, `SlurmManager.build_shell_str(...)`, `SlurmCmd(srun_cmd=...)` |

### 4.2 Why this split

- `gaussian_job_shared/src/entities/slurm/status.rs` already comments
  that `SlurmJobState` belongs in `slurm-async-runner`. We honor that
  through S6, and reverse it in S7+S8 by moving the enum to
  `gaussian_job_shared` and re-exporting from here. The "data types
  live in `gaussian_job_shared`, execution code lives in
  `slurm-async-runner`" thesis stays consistent across both repos.
- Splitting `parse_squeue` / `parse_sacct` / `build_shell_str` out as
  pure functions makes the I/O-free pieces directly testable in
  `cargo test` without spawning subprocesses.
- pyo3-async-runtimes (tokio runtime feature) lets us return a Rust
  `tokio::Future` to Python as an `awaitable`, so the existing async
  Python contract is preserved verbatim.

### 4.3 Dependencies (Cargo.toml after S2)

Inherit the slurm-async-runner2 dep set (anyhow / thiserror / log /
tracing / futures / tokio / chrono / serde / uuid + optional pyo3,
pyo3-async-runtimes, pyo3-log, pythonize, pyo3-stub-gen) and add:

```toml
gaussian_job_shared = { git = "https://github.com/kkiyama117/gaussian_job_shared", branch = "main" }
```

`branch = "main"` chosen at user direction. After S7 lands a tagged
release, S8 may switch to a `rev` or `tag` pin.

## 5. Public Python API (final form, S6 onward)

```python
from slurm_async_runner import (
    SlurmManager,
    SlurmCmd,
    SlurmJobState,
    get_current_shell_str,
)
```

Signatures (preserved from current pure-Python implementation):

```python
class SlurmCmd:
    srun_cmd: str  # default "srun", frozen
    def __new__(cls, srun_cmd: str = "srun") -> SlurmCmd: ...

class SlurmJobState(enum.Enum):
    PENDING; RUNNING; SUSPENDED; COMPLETING; COMPLETED; CANCELLED
    FAILED; TIMEOUT; PREEMPTED; NODE_FAIL; BOOT_FAIL; DEADLINE
    OUT_OF_MEMORY; UNKNOWN
    @staticmethod
    def parse(s: str) -> SlurmJobState: ...

class SlurmManager:
    default_shell: str | None  # mutable for tests (no-shell branch)
    slurm_cmd: SlurmCmd
    def __new__(cls, slurm_cmd: SlurmCmd | None = None) -> SlurmManager: ...
    def srun_command(self, batch_file: pathlib.Path) -> str: ...
    def build_shell_str(self, cmd: typing.Sequence[str]) -> str: ...
    async def run_job(self, batch_file: pathlib.Path, dry_run: bool = False) -> int: ...
    async def query_job_state(self, jobid: int) -> SlurmJobState: ...
    async def query_job_states_batch(self, jobids: list[int]) -> dict[int, SlurmJobState]: ...

def get_current_shell_str() -> str | None: ...
```

The submodule `slurm_async_runner.runner` is removed. Users must
import from the package root.

## 6. Module Layout (S6 final form)

```
slurm-async-runner/
├─ Cargo.toml, Cargo.lock, rust-toolchain.toml
├─ pyproject.toml                 # maturin, module-name = "slurm_async_runner._core"
├─ .gitignore                     # Rust-flavored
├─ README.md, CHANGELOG.md, LICENSE
│
├─ src/
│  ├─ lib.rs                      # pub mod py_export; pub mod entities; pub mod runner; pub mod error;
│  ├─ error.rs                    # thiserror RunnerError + impl From<RunnerError> for PyErr (in py_export)
│  │
│  ├─ entities/
│  │  ├─ mod.rs                   # pub mod slurm_job_state; pub use slurm_job_state::SlurmJobState;
│  │  └─ slurm_job_state.rs       # enum + impl parse(&str)
│  │
│  ├─ runner/
│  │  ├─ mod.rs                   # pub mod query; pub mod dispatch; pub(crate) fn build_shell_str(...)
│  │  ├─ query.rs                 # query_job_states_batch + parse_squeue + parse_sacct
│  │  └─ dispatch.rs              # SlurmCmd, SlurmManager, run_job
│  │
│  ├─ py_export/
│  │  ├─ mod.rs                   # #[pymodule] mod slurm_async_runner; aggregate exports
│  │  ├─ entities/
│  │  │  ├─ mod.rs
│  │  │  └─ slurm_job_state.rs    # #[pyclass] enum wrapper + gen_stub
│  │  └─ runner/
│  │     ├─ mod.rs
│  │     ├─ query.rs              # pyo3-async-runtimes binding for query_*
│  │     └─ dispatch.rs           # SlurmManager / SlurmCmd #[pyclass]
│  │
│  └─ bin/
│     └─ stub_gen.rs              # exact copy of slurm-async-runner2's
│
├─ python/
│  ├─ slurm_async_runner/
│  │  ├─ __init__.py              # from slurm_async_runner import _core; re-export public names
│  │  └─ _core/                   # generated .pyi committed to the repo (matches gaussian_job_shared2)
│  │     ├─ __init__.pyi
│  │     ├─ entities/__init__.pyi
│  │     └─ runner/__init__.pyi
│  └─ tests/
│     ├─ test_smoke.py
│     ├─ test_slurm_job_state.py
│     ├─ test_query_job_states.py
│     └─ test_run_job.py
│
└─ .github/workflows/ci.yml       # maturin develop + cargo test + pytest
```

## 7. Data Flow

### 7.1 `query_job_states_batch([101, 102, 103])`

1. Python `await manager.query_job_states_batch([101, 102, 103])`.
2. pyo3-async-runtimes hands the call to a Tokio runtime.
3. Rust `runner::query::query_job_states_batch`:
   1. de-duplicate input → `"101,102,103"`.
   2. spawn `squeue -h -j 101,102,103 -o "%i %T"` via
      `tokio::process::Command::output().await`.
   3. `parse_squeue(stdout)` → `HashMap<u64, SlurmJobState>` (pure fn).
   4. `missing = inputs filter out keys present in active`.
   5. if `missing` non-empty: spawn `sacct -P -n -j <miss_csv> -o JobID,State`
      and `parse_sacct(stdout)` → `HashMap<u64, SlurmJobState>`.
   6. Build the result dict keyed by **every** input id, defaulting to
      `SlurmJobState::Unknown` when neither backend returned it.
4. Return as `PyDict<i64, SlurmJobState>`.

Empty input short-circuits to `{}` with **zero** subprocess calls.

### 7.2 `run_job(Path("foo.bash"), dry_run=False)`

1. Python `await manager.run_job(Path("foo.bash"))`.
2. Rust `runner::dispatch::run_job`:
   1. `shell_str = build_shell_str(&[srun_cmd, abs_path])`. If
      `default_shell.is_some()`, wrap as `<shell> -c "<inner>"`;
      otherwise join verbatim. Input slice not mutated.
   2. if `dry_run`: `println!("{shell_str}")`; return `0`.
   3. `tokio::process::Command::new("sh").arg("-c").arg(&shell_str).output().await`.
   4. `println!("[<shell_str> exited with {rc}]")` and dump captured
      stdout/stderr (matches current Python `print` calls).
   5. return `rc as i32`, defaulting `None` → `0`.
3. Python receives `int`.

## 8. Error Handling

| Failure | Rust representation | Python surface |
|---------|---------------------|----------------|
| Failed to spawn `squeue`/`sacct`/`sh` | `RunnerError::SubprocessSpawn { program, source: io::Error }` | `RuntimeError` |
| stdout not valid UTF-8 | `RunnerError::SubprocessOutputUtf8 { program, source }` | `ValueError` |
| Unrecognized state token from `squeue`/`sacct` | `→ SlurmJobState::Unknown` (matches current Python) | n/a |
| Subprocess non-zero exit in `run_job` (`dry_run=False`) | not an error — return code propagated as `int` | n/a |

`RunnerError` is `thiserror`-derived in `src/error.rs`; the
`impl From<RunnerError> for PyErr` lives in `src/py_export/mod.rs` to
avoid cross-feature imports. `RunnerError` is **not** exposed to
Python — standard exceptions are sufficient and match the current
zero-custom-exception surface.

## 9. Testing Strategy

### 9.1 Rust unit tests (`cargo test`)

| Target | Cases |
|--------|-------|
| `SlurmJobState::parse` | `"RUNNING"` → `Running`; `"CANCELLED by 1234"` → `Cancelled`; `""` → `Unknown`; `"FOO"` → `Unknown`. |
| `parse_squeue` | empty; one row; multiple rows; row with `< 2` tokens; non-numeric jobid. |
| `parse_sacct` | base+step (`12345.batch`) filtered; `\|` separator; `CANCELLED by N` normalized; empty. |
| `build_shell_str` | `default_shell = None` joins verbatim; set value wraps in `<shell> -c "..."`; input slice unchanged. |
| `run_job` (via `bash`) | `exit 42` returns 42; `exit 0` returns 0; `dry_run = true` returns 0 with no spawn. |

### 9.2 Python smoke tests (`pytest python/tests`)

End-to-end after `maturin develop`. Confirms the Rust extension is
importable and the async surface is awaitable.

| File | Purpose |
|------|---------|
| `test_smoke.py` | `from slurm_async_runner import SlurmManager, SlurmCmd, SlurmJobState, get_current_shell_str` succeeds. |
| `test_slurm_job_state.py` | enum identity (`SlurmJobState.RUNNING is SlurmJobState.parse("RUNNING")`); UNKNOWN fallback. |
| `test_query_job_states.py` | end-to-end with **PATH-stub `squeue` / `sacct` shell scripts** in `tmp_path`. Covers 3-id mix, all-active, empty stdout, both empty (`UNKNOWN`), `CANCELLED by N` normalization, single-id wrapper. |
| `test_run_job.py` | `SlurmCmd(srun_cmd="bash")` + script that `exit 42` → 42; `exit 0` → 0; `dry_run=True` → 0. |

The current `unittest.mock.patch("slurm_async_runner.runner.asyncio.create_subprocess_exec", ...)` approach cannot intercept Rust subprocess calls. PATH-stub is the replacement.

### 9.3 CI

`.github/workflows/ci.yml` (S6):

```yaml
name: CI
on:
  push: { branches: [main] }
  pull_request: { branches: [main] }
permissions: { contents: read }
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@nightly
      - uses: actions/setup-python@v5
        with: { python-version: '3.12' }
      - run: pip install maturin pytest
      - run: maturin develop --release
      - run: cargo test --all-features
      - run: pytest python/tests
```

`ruff format/lint` is dropped — Python source becomes a thin wrapper
and is not worth its own linting pass.

## 10. Step-by-Step Plan

| Step | Commit subject | Acceptance |
|------|----------------|------------|
| S1 | `refactor!: replace pure-Python with Rust+pyo3 skeleton` | `cargo build` ✓; `maturin develop` ✓; `pytest python/tests/test_smoke.py` ✓. Old `src/slurm_async_runner/*.py`, `tests/*.py` removed. |
| S2 | `feat: add gaussian_job_shared as git dependency` | `cargo build` ✓; `Cargo.lock` reflects git-resolved entry; no public symbols introduced yet. |
| S3 | `feat: port SlurmJobState to Rust+pyo3 (TODO: move to gaussian_job_shared)` | `cargo test` ✓ (4 cases for `parse`); `pytest test_slurm_job_state.py` ✓; `from slurm_async_runner import SlurmJobState` works. File-level TODO comment marks the eventual re-export switch. |
| S4 | `feat: port query_job_states_batch to Rust+pyo3 (tokio + squeue/sacct)` | `cargo test` ✓ for parsers; `pytest test_query_job_states.py` ✓ via PATH stubs. |
| S5 | `feat: port run_job/SlurmManager/SlurmCmd to Rust+pyo3 (tokio)` | `cargo test` ✓ for `build_shell_str` + bash-driven `run_job`; `pytest test_run_job.py` ✓. |
| S6 | `ci: switch to maturin/cargo workflow + update README/CHANGELOG` | CI green on a draft push. |
| **S7** *(out of scope: separate Claude session in `gaussian_job_shared` repo)* | `feat: add SlurmJobState enum to gaussian_job_shared` | Handled in `kkiyama117/gaussian_job_shared`. |
| **S8** *(this branch, after S7 lands)* | `refactor: re-export SlurmJobState from gaussian_job_shared` | bump `gaussian_job_shared` rev pin; replace `entities/slurm_job_state.rs` body with one-line `pub use gaussian_job_shared::entities::slurm::SlurmJobState;`; delete the local enum tests (now covered by `gaussian_job_shared`); `cargo test` + `pytest` ✓. |

## 11. Open Risks

- pyo3-async-runtimes + tokio + maturin nightly stack has known
  build-environment fragility (e.g., `auto-initialize` interaction
  with manylinux) — mitigated by mirroring `slurm-async-runner2`'s
  Cargo.toml verbatim, which already has working CI.
- Path-stub testing on Windows runners would not work, but the CI
  matrix is Linux-only (matches current setup).
- `gaussian_job_shared` `branch = "main"` pin is non-deterministic
  across builds; acceptable for development, should be tightened to
  `rev` once S7+S8 lands.

## 12. Out of Scope

- Renaming repos / migrating `miyake-ken/slurm-async-runner` → `kkiyama117/...`.
- PyPI release, wheel publishing automation.
- Introducing `Job` / `JobSpec` as `run_job` arguments. The current
  `Path`-based contract is intentionally preserved for this migration;
  a follow-up branch can layer that on top once `gaussian_job_shared`
  surfaces `Job` to Python (already done in `gaussian-job-shared2`).
