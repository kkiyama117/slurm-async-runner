# slurm-async-runner

Async SLURM job dispatcher and lifecycle-status query backend, implemented in
Rust and exposed to Python via [pyo3] + [pyo3-async-runtimes].

This is the Rust port of the original pure-Python `slurm-async-runner`. The
public Python API is intentionally compatible: callers continue to `await`
coroutines that wrap the same set of operations.

[pyo3]: https://pyo3.rs/
[pyo3-async-runtimes]: https://github.com/PyO3/pyo3-async-runtimes

## Public API

### Rust

```rust
use slurm_async_runner::{
    SlurmCmd, SlurmManager,
    JobDispatcher, TokioDispatcher, DryRunDispatcher,
    JobStatus, JobState, JobReason,
    // tssrun vocabulary lives in this crate (migrated out of gaussian_job_shared in PR #5):
    JobPartition, JobTimeLimit, Memory, MemoryUnit,
    ResourceSpec, ResourceSpecCPU, ResourceSpecGPU,
    query_job_states_batch, query_job_states_batch_with,
};
use std::path::Path;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let manager = SlurmManager::default(); // launcher = "srun"

    // Dispatch a batch script.
    let exit_code = manager.run_job(Path::new("./job.sh"), false).await?;

    // Query lifecycle status (squeue then sacct fallback).
    let status: JobStatus = manager.query_job_state(12345).await?;
    println!("state={:?} reason={:?}", status.state, status.reason);

    // Bulk query.
    let states = manager.query_job_states_batch(&[12345, 12346]).await?;

    // Plug in a custom dispatcher (e.g. for tests or a remote backend).
    let dry = DryRunDispatcher;
    manager.run_job_with(&dry, Path::new("./job.sh")).await?;
    Ok(())
}
```

### Python

```python
import asyncio
from slurm_async_runner._slurm_async_runner_core.manager import SlurmCmd, SlurmManager
from slurm_async_runner._slurm_async_runner_core.runner import query_job_states_batch
from slurm_async_runner._slurm_async_runner_core.entities.slurm.status import JobStatus

async def main():
    manager = SlurmManager()                      # launcher = "srun"
    # Or override:
    manager = SlurmManager(SlurmCmd(srun_cmd="srun"))

    code: int = await manager.run_job("./job.sh", dry_run=False)
    status: JobStatus = await manager.query_job_state(12345)
    states: dict[int, JobStatus] = await manager.query_job_states_batch([12345, 12346])

    # Module-level helper:
    states = await query_job_states_batch([12345, 12346])

asyncio.run(main())
```

`JobStatus` carries `(state, reason)` and is parsed from SLURM's `squeue`
output (`-o "%i %T %r"`) with an `sacct` fallback for completed jobs. The
state/reason vocabularies (24 official states, ~80 reason codes, with
`Unknown` / `Other(String)` forward-compat fallbacks) live in this crate's
`entities::slurm` module, re-exposed to Python via
`slurm_async_runner._slurm_async_runner_core.entities.slurm.*`. They were
migrated out of [`gaussian_job_shared`](https://github.com/kkiyama117/gaussian_job_shared)
in PR #5 (single-owner rule for pyclass wrappers).

## Architecture

The crate is split along two axes:

| Layer | Type | Concern |
|-------|------|---------|
| Spec | `SlurmCmd`, `SlurmManager` | Pure data + argv builders. No I/O. |
| Runtime | `JobDispatcher` trait, `TokioDispatcher`, `DryRunDispatcher` | Subprocess execution. Swappable. |
| Query | `runner::query_job_states_batch_with` | `squeue` then `sacct` fallback parsing. |

This separation lets tests substitute mock dispatchers without spawning real
processes, and keeps the spec layer language-agnostic so the same argv builder
feeds both the Rust runtime and a Python `asyncio.create_subprocess_exec`
runtime.

No shell wrapping (`$SHELL -c "..."`) is used — both Python's
`asyncio.create_subprocess_exec` and Rust's `tokio::process::Command::args`
accept argv directly.

## tssrun (background mode + env inspection)

For the Kyoto-U ECCS interactive batch frontend `tssrun`, the
`slurm_async_runner.tssrun` submodule offers a non-blocking spawn API
and snapshot-based environment inspection.

```python
import asyncio
from slurm_async_runner._slurm_async_runner_core.tssrun import (
    JobTimeLimit, ResourceSpec, TssrunCmd, TssrunManager,
    file_log_sink, file_system_state_store,
)
from slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options import (
    Memory,
)

async def main():
    cmd = TssrunCmd(
        program="/work/job.sh",
        partition="gr19999b",
        time_limit=JobTimeLimit("1:00:00"),
        rsc=ResourceSpec(processes=4, memory=Memory("2G")),
    )
    sink = await file_log_sink("/tmp/job.out", "/tmp/job.err")
    manager = TssrunManager(
        cmd,
        store=file_system_state_store("/var/lib/slurm-runner"),
        log_sink=sink,
    )

    handle = await manager.spawn()
    print("uuid", await handle.uuid, "pid", await handle.pid, "jobid", await handle.jobid)
    code = await handle.wait()  # int on normal exit, None on signal kill
    print("exit", code)

asyncio.run(main())
```

> **PR #5 migration note.** `Resource(processes=4, memory="2G")` is gone —
> use `ResourceSpec(processes=4, memory=Memory("2G"))` instead. `queue=`
> was renamed to `partition=` (matching Slurm's `--partition` flag), and
> `time_limit=` now expects a `JobTimeLimit(...)` value instead of a raw
> string. `TssrunManager` no longer accepts `state_dir=`; pass
> `store=file_system_state_store(path)` (or omit `store=` for the default
> in-memory backend). `ResourceSpec` and `JobTimeLimit` are re-exported
> from the `tssrun` submodule for one-stop import; `Memory` lives in
> `entities.slurm.sbatch_options`.

### Handle API contract

`TssrunJobHandle` returns awaitables for all reads, but the **snapshot
getters are lock-free against an in-flight `wait()`** — you can poll
liveness while the wait is pending without blocking it:

```python
handle = await manager.spawn()
wait_fut = asyncio.ensure_future(handle.wait())

while not wait_fut.done():
    if await handle.is_running():
        print("jobid", await handle.jobid, "node", await handle.node)
    await asyncio.sleep(1)

code = await wait_fut
```

The full snapshot surface:

| Reader (lock-free) | Returns | Notes |
|---|---|---|
| `await handle.uuid` | `str` | UUID v7 primary key (canonical hyphenated string). Pass straight back to `attach_uuid` |
| `await handle.pid` | `int` | The OS pid of the spawned `tssrun` process |
| `await handle.jobid` | `int \| None` | Parsed from `salloc: Granted job allocation N` |
| `await handle.node` | `str \| None` | Parsed from `salloc: Nodes <spec> are ready for job` |
| `await handle.sent_env` | `dict[str, str]` | Env explicitly passed via `TssrunCmd.env` |
| `await handle.live_env()` | `dict[str, str] \| None` | Reads `/proc/<pid>/environ` on Linux; `None` off-Linux or after exit |
| `await handle.is_running()` | `bool` | `True` until the wait task records `finished` |
| `await handle.exit_code()` | `int \| None` | Available after exit; `None` for signal kill |

| Owner-only | Returns | Notes |
|---|---|---|
| `await handle.wait()` | `int \| None` | `int` = exit code; `None` = killed by signal (SLURM time-limit kill, OOM, etc.). Raises `RuntimeError` on attached / already-waited handles. |

### Cross-process attach

When configured with `store=file_system_state_store(path)`, `TssrunManager`
persists a JSON snapshot to `{path}/{uuid}.json` (atomic rename, UUID v7
primary key) every time `salloc:` parsing or wait completion updates the
state. A separate process can re-attach with read-only semantics:

```python
# Recommended: O(1) lookup by UUID v7 primary key (the canonical reference)
attached = await manager.attach_uuid("01900000-0000-7000-8000-000000000000")

# Best-effort fallbacks (linear scan over the state dir)
attached = await manager.attach_pid(12345)
attached = await manager.attach_jobid(102362)      # only after salloc: parsing
attached = await manager.attach_file("/path/to/01900000-0000-7000-8000-000000000000.json")
```

Attached handles support every snapshot getter (they reflect the JSON's
last-known state) but `wait()` raises — only the original spawner owns
the child. Pids can be recycled by the kernel, so prefer `attach_uuid`
for any long-lived reference.

### Live smoke test on kudpc / ECCS

The unit and integration tests stub `tssrun` with `bash` so they run on any
dev machine. To validate the wrapper against a real SLURM allocation, run
the live smoke script on a host where `tssrun` is actually installed:

```bash
# Standalone — exits 0 on PASS, 0+SKIP message off-cluster, 1 on FAIL.
uv run python scripts/test_tssrun_live.py

# Or via pytest (opt-in, skipped unless RUN_LIVE_TSSRUN=1):
RUN_LIVE_TSSRUN=1 uv run pytest python/tests/test_tssrun_live.py -v -s
```

Configurable via environment variables (all optional):

| Variable | Default | Purpose |
|----------|---------|---------|
| `TSSRUN_LIVE_BIN` | `tssrun` | Path to the tssrun binary |
| `TSSRUN_LIVE_QUEUE` | unset (site default) | Queue / partition (`-p`) |
| `TSSRUN_LIVE_TIME_LIMIT` | `0:01:00` | Wall-clock limit (`-t`) |
| `TSSRUN_LIVE_RSC` | unset | Raw `--rsc` value, e.g. `p=1:c=1:m=512M` |
| `TSSRUN_LIVE_TIMEOUT` | `180` | Hard timeout (s) before the runner gives up |

The script spawns a tiny child, awaits completion, and asserts that
`pid` / `jobid` / `node` / persisted snapshot / `attach_file` round-trip /
captured stdout log all line up — i.e. the same end-to-end shape as the
integration test, but driven through the real tssrun → salloc → srun path.

> **Cluster-side setup matters.** On kudpc / ECCS you need to point
> `TMPDIR` at a shared filesystem (compute nodes can't see the login
> node's `/tmp`) and pick a `TSSRUN_LIVE_QUEUE` your group is permitted
> to use. See [`docs/setup_test.md`](docs/setup_test.md) for the full
> operator checklist, including failure-mode triage.

## Development

```bash
# Rust-side
cargo test --lib                          # 35 unit tests, no SLURM required
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check

# Python-side
uv sync --all-extras
uv run maturin develop                    # builds + installs the extension
uv run pytest python/tests -v
uv run ruff check python/

# Regenerate .pyi stubs (for sync types and #[gen_stub_pyclass] entries — async
# pyfunctions and lock-free getters are hand-written under
# python/slurm_async_runner/_slurm_async_runner_core/{manager,runner,tssrun}.pyi).
# Re-run ruff format afterwards because pyo3-stub-gen output isn't ruff-formatted.
cargo run --bin stub_gen && uv run ruff format python/
```

The CI pipeline runs all of the above on every push and PR; see
[`.github/workflows/test.yml`](.github/workflows/test.yml). Wheel building +
PyPI publishing is in [`.github/workflows/CI.yml`](.github/workflows/CI.yml).

### Pre-commit hooks (local autofix)

[`.pre-commit-config.yaml`](.pre-commit-config.yaml) wires `ruff check --fix`,
`ruff format`, `cargo clippy --fix`, and `rustfmt` (edition 2024, see
[`rustfmt.toml`](rustfmt.toml)) to run before each commit, mirroring CI but
with autofix. One-time setup per clone:

```bash
uv tool install pre-commit          # or: pipx install pre-commit
pre-commit install                  # registers .git/hooks/pre-commit
```

Manual sweep across the repo:

```bash
pre-commit run --all-files
```

When a hook rewrites files, the commit aborts so you can review the diff and
re-stage (`git add -u && git commit`). The first commit on a fresh clone is
slow because `cargo clippy` compiles the workspace; subsequent runs hit the
cargo cache. To bypass hooks in an emergency, use `git commit --no-verify`.

## License

See repository root.
