# Design: `--nice` scheduling-priority option (issue #13)

**Date:** 2026-05-26
**Issue:** [#13](https://github.com/kkiyama117/slurm-async-runner/issues/13) — feat: add `--nice` scheduling-priority option to SbatchCmd / SlurmJobConfig
**Status:** Approved (brainstorming)

## Problem

SLURM exposes `--nice[=adjustment]` to adjust a job's scheduling priority, but
the runner has no path to emit it. A downstream consumer (a fire-and-forget
CREST→Gaussian pipeline on the shared KUDPC queue) wants to **deprioritize** its
long batch jobs so higher-priority / interactive work is scheduled ahead of
them. Existing workarounds do not work:

- `args=[...]` is reserved for the batch script's positional arguments.
- In-script `#SBATCH --nice=...` directives are ignored in the runner-driven
  submission path.

A first-class option is the only clean route.

## SLURM `--nice` semantics (reference)

- Positive value **lowers** priority; negative **raises** it (negative requires
  privilege).
- Range: ±2147483645 (fits within `i32`, whose max is 2147483647).
- Bare `--nice` (no value) defaults to +100. We always emit `--nice=<v>` with an
  explicit integer so behaviour is unambiguous. `v=0` is a valid no-op
  adjustment and is still emitted.

## Scope & Decisions

- **Type:** `Option<i32>` (signed; SLURM's range fits inside `i32`).
- **Validation:** none — pass-through. Out-of-range values are left to SLURM to
  reject. This matches how existing simple fields (`comment`, `mail_*`) are
  handled and keeps the change minimal (KISS / YAGNI).
- **Both surfaces, no new conversion:** `nice` is added to **both** `SbatchCmd`
  (where it actually flows into argv) and `SlurmJobConfig` (the `[slurm]` TOML
  config envelope, for parity). There is currently **no** `SlurmJobConfig →
  SbatchCmd` conversion in the codebase, and we do **not** introduce one. Adding
  `nice` to `SlurmJobConfig` is therefore a config-completeness field only — it
  does not by itself reach argv, exactly like the other `SlurmJobConfig` fields
  today. The issue's acceptance criteria only require `SbatchCmd.build_argv()`.
- **argv form:** single token `--nice=<v>` (not `--nice <v>`), so negative
  values like `-5` are not mistaken for a separate flag.
- **argv ordering:** emitted **after `--comment`** and **before** the script
  path. Order does not affect behaviour; this slot avoids disturbing existing
  argv-ordering test assertions.

## Changes by file

### Core (argv) — `src/sbatch/cmd.rs`
- Add `pub nice: Option<i32>` to `SbatchCmd` (next to `comment`).
- Initialise `nice: None` in `SbatchCmd::new()`.
- In `build_argv()`, after the `--comment` block and before pushing the script:
  ```rust
  if let Some(n) = self.nice {
      argv.push(format!("--nice={n}"));
  }
  ```

### Entity (config) — `src/entities/slurm/sbatch_options.rs`
- Add `#[serde(default)] pub nice: Option<i32>` to `SlurmJobConfig`.

### PyO3 — `src/py_export/sbatch.rs` (`PySbatchCmd`)
- Add `nice = None` to the `#[pyo3(signature = (...))]`.
- Add `nice: Option<i32>` parameter to `new(...)`.
- Set `cmd.nice = nice;`.

### PyO3 — `src/py_export/entities/slurm/sbatch_options/config.rs` (`PySlurmJobConfig`)
- Add `nice=None` to the constructor `#[pyo3(signature = (...))]`.
- Add `nice: Option<i32>` parameter and `nice` to the struct literal.
- Add `#[getter] nice` / `#[setter] set_nice`.

### Stubs
- Hand-edit `python/slurm_async_runner/_slurm_async_runner_core/sbatch.pyi`:
  add `nice: builtins.int | None = None` to `SbatchCmd.__init__` (next to
  `comment`). This `.pyi` is handwritten (the `PySbatchCmd` pyclass has no
  `#[gen_stub_*]` macros).
- Regenerate the `SlurmJobConfig` stub
  (`.../entities/slurm/sbatch_options/__init__.pyi`, which is `gen_stub`-driven)
  via `cargo run --bin stub_gen && uv run ruff format python/`. If stub_gen
  fails under the extension-module feature, hand-edit the `SlurmJobConfig`
  getter/setter/`__init__` entries to mirror the Rust change.

### Docs — `CHANGELOG.md`
- Add a one-line `feat` entry under `[unreleased]`.

## Tests

### Rust unit tests — `src/sbatch/cmd.rs`
- `nice_emits_single_token`: `nice = Some(100)` → argv contains `--nice=100`.
- `nice_zero_is_emitted`: `nice = Some(0)` → argv contains `--nice=0`.
- `nice_negative_value`: `nice = Some(-5)` → argv contains `--nice=-5` as a
  single token.
- `nice_omitted_when_none`: default → no argv element starts with `--nice`.

### Rust serde test — `src/entities/slurm/sbatch_options.rs`
- Minimal: a `SlurmJobConfig` with `nice` set round-trips through serde (only if
  it fits the existing test style; otherwise rely on `#[serde(default)]`).

### Python tests — `python/tests/test_sbatch.py`
- `SbatchCmd(script, nice=100).build_argv()` contains `--nice=100`.
- Omitting `nice` produces no `--nice` flag.

## Acceptance criteria

- `SbatchCmd(script, nice=100).build_argv()` contains `--nice=100`.
- Omitting `nice` produces no `--nice` flag.
- `nice=0` produces `--nice=0` (explicit no-op).
- `nice=-5` produces the single token `--nice=-5`.

## Out of scope

- `SlurmJobConfig → SbatchCmd` conversion path.
- Range validation / clamping of `nice`.
- Bare `--nice` (no value) form.
