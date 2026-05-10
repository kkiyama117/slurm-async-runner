# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed (BREAKING)

- **PR #5: Slurm vocab migration with single-owner pyclass rule.** The
  `tssrun` Resource type and the broader Slurm vocabulary now live in
  this crate's `entities::slurm` module. Concretely:
  - Rust: `Resource` is **deleted**. Use [`ResourceSpec`](src/entities/slurm)
    (CPU / GPU enum with `Option<NonZeroU32>` slots — partial CPU specs
    like `p=60:t=1:c=1` are accepted per the KUDPC manual).
    `TssrunCmd::queue` is renamed to `partition` (matching Slurm's
    `--partition` flag). `TssrunCmd::time_limit` is retyped from
    `Option<String>` to `Option<JobTimeLimit>` (canonical `HH:MM:SS`
    `Display`). `TssrunCmd::rsc` is retyped from `Option<Resource>` to
    `Option<ResourceSpec>`. New crate-root re-exports: `JobPartition`,
    `JobTimeLimit`, `Memory`, `MemoryUnit`, `ResourceSpec`,
    `ResourceSpecCPU`, `ResourceSpecGPU`.
  - Python: `Resource(...)` is **deleted**. Use
    `ResourceSpec(processes=..., memory=Memory("2G"), ...)` or the GPU
    form `ResourceSpec(gpus=1)`. `TssrunCmd(queue=...)` becomes
    `TssrunCmd(partition=...)`; `time_limit=` requires a
    `JobTimeLimit("1:00:00")` value (no implicit string coercion);
    `rsc=` requires a `ResourceSpec(...)` value. `Memory` requires
    explicit wrapping — pass `Memory("2G")` or
    `Memory.from_value(2, MemoryUnit.Giga)`. `ResourceSpec` and
    `JobTimeLimit` are re-exported from the `tssrun` submodule for
    one-stop import; `Memory` lives in
    `entities.slurm.sbatch_options`.
  - **Pymodule entry rename.** Both crates renamed their pymodule entry
    from `_core` to `_<package>_core` to avoid `PyInit__core`
    duplicate-symbol clashes when both are linked into one process.
    Python imports change accordingly:
    `slurm_async_runner._core.*` → `slurm_async_runner._slurm_async_runner_core.*`,
    `gaussian_job_shared._core.*` → `gaussian_job_shared._gaussian_job_shared_core.*`.
  - **`JobStatus` is now owned by this crate** (single-owner rule for
    pyclass wrappers). The Python type lives at
    `slurm_async_runner._slurm_async_runner_core.entities.slurm.status.JobStatus`.
    The `gaussian_job_shared._gaussian_job_shared_core.entities.slurm.status`
    location is no longer authoritative for in-process pyclass identity.

### Added

- **`slurm_async_runner::tssrun` Rust module and `slurm_async_runner._slurm_async_runner_core.tssrun`
  Python submodule.** Provides `TssrunCmd` (typed `tssrun` argv builder),
  `TssrunManager` (background spawn / attach / state query), `JobHandle`
  with watch-based snapshot, and a pluggable `JobLogSink` trait
  (`Null/Std/InMemory/FileLogSink`).
- **`BackgroundDispatcher` trait + `TokioBackgroundDispatcher`** for
  non-blocking child spawn alongside the existing synchronous
  `JobDispatcher`.
- **Rust port of `slurm-async-runner`.** Initial Rust + pyo3 implementation
  replacing the pure-Python prototype, with a Python-compatible async API
  exposed through `pyo3-async-runtimes` (tokio runtime).
- **`SlurmCmd` / `SlurmManager`** spec types in `src/manager.rs`. Pure data +
  argv builders; no I/O. Both are exported from the crate root.
- **`JobDispatcher` trait** in `src/dispatcher.rs` with two shipped impls:
  `TokioDispatcher` (production, `tokio::process::Command`) and
  `DryRunDispatcher` (echo-only). Custom impls can be plugged in via
  `SlurmManager::run_job_with` / `query_job_states_batch_with`.
- **`runner::query_job_states_batch`** — async `squeue` then `sacct` fallback
  parser. Returns `HashMap<u64, JobStatus>` carrying both state and reason.
  `query_job_states_batch_with<D: JobDispatcher>` exposes the dispatcher
  parameter for tests.
- **Re-exports** of `JobStatus`, `JobState`, `JobReason` from
  [`gaussian_job_shared`](https://github.com/kkiyama117/gaussian_job_shared)
  so downstream Rust callers don't need a direct dependency.
- **Python sub-modules** `slurm_async_runner._slurm_async_runner_core.manager` (`SlurmCmd`,
  `SlurmManager`) and `slurm_async_runner._slurm_async_runner_core.runner`
  (`query_job_states_batch`). Async pyo3 returns native Python coroutines.
- **Hand-written `.pyi` stubs** for the new submodules at
  `python/slurm_async_runner/_slurm_async_runner_core/manager.pyi` and
  `runner.pyi` (the auto-generated `__init__.pyi` only covers sync,
  top-level pyfunctions).
- **CI: cargo + Python pipeline** at `.github/workflows/test.yml`. Runs
  `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test --lib`,
  `maturin develop`, `pytest`, and `ruff` on every push and PR.

### Changed

- **`JobHandle::wait()` returns `Result<Option<i32>>`.** Was `Result<i32>`
  with `unwrap_or(0)` masking signal kills. Now `Ok(None)` distinguishes
  signal-killed children (e.g. SLURM time-limit kill, OOM) from clean
  exits. The `.pyi` stub and Python wrapper surface the same `int | None`.
- **`PyTssrunJobHandle` snapshot getters are lock-free.** Snapshot getters
  (`pid`, `jobid`, `node`, `sent_env`, `is_running`, `exit_code`,
  `live_env`) read from a cloned `watch::Receiver` and never lock the
  `JobHandle` mutex. `wait()` is the only method that takes the mutex,
  so concurrent polls during an in-flight `wait()` no longer block.
- **`TssrunManager` fields are now `pub(crate)`.** Use the
  `with_state_dir` / `with_log_sink` builders instead of mutating fields
  directly — mid-flight mutation isn't retroactive on running handles.
- **No shell wrapping.** The previous Python prototype wrapped `srun` in
  `$SHELL -c "..."`; this port spawns argv directly via
  `tokio::process::Command::args`. Both Python's
  `asyncio.create_subprocess_exec` and Rust's `Command::args` accept argv
  lists, so the shell layer is unnecessary.
- **`SlurmManager` spec / runtime split.** Eliminated `default_shell` /
  `build_shell_str` / `srun_command`. New shape: `SlurmManager` holds a
  `SlurmCmd`, builds argv via `build_argv`, and delegates execution to a
  `JobDispatcher`.

### Notes

- **Note (superseded by PR #5).** Earlier drafts pinned
  `gaussian_job_shared` with `default-features = false` to avoid a
  `PyInit__core` duplicate-symbol clash. PR #5 supersedes that workaround:
  the Slurm vocab (`JobStatus`, `JobState`, `JobReason`, `ResourceSpec`,
  `JobTimeLimit`, `Memory`, ...) is now owned in-tree under
  `entities::slurm`, so this crate no longer depends on
  `gaussian_job_shared` at all. Each pyclass type has exactly one owner
  cdylib in the dependency graph (Pyclass Single Owner rule — see
  `docs/superpowers/specs/2026-05-10-slurm-vocab-migration-and-pyclass-ownership-design.md`).
- All 35 Rust unit tests run without a real SLURM cluster — the test suite
  substitutes the coreutils `true` / `false` / `echo` binaries through
  `SlurmCmd::new(...)`, plus a `MockDispatcher` for argv-plumbing assertions.

[Unreleased]: https://github.com/kkiyama117/slurm-async-runner2/compare/HEAD
