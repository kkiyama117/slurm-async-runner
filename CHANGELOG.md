# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
  `python/slurm_async_runner/_core/manager.pyi` and `runner.pyi` (the
  auto-generated `__init__.pyi` only covers sync, top-level pyfunctions).
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

- `gaussian_job_shared` is pulled in with `default-features = false`. Upstream's
  `pyo3` feature emits its own `PyInit__core`, which would clash with this
  crate's `_core` pymodule at link time. Python users continue to import
  `JobStatus` etc. directly from `gaussian_job_shared._core.entities.slurm.status`.
- All 35 Rust unit tests run without a real SLURM cluster — the test suite
  substitutes the coreutils `true` / `false` / `echo` binaries through
  `SlurmCmd::new(...)`, plus a `MockDispatcher` for argv-plumbing assertions.

[Unreleased]: https://github.com/kkiyama117/slurm-async-runner2/compare/HEAD
