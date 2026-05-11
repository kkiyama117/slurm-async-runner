# slurm-async-runner

Async SLURM job dispatcher and lifecycle-status query backend, implemented in
Rust and exposed to Python via [pyo3] + [pyo3-async-runtimes].

Two job-submission backends share a common handle contract:

- **[`tssrun`](#tssrun-background-mode--env-inspection)** — background
  `salloc` + `srun` flow for the Kyoto-U ECCS / KUDPC interactive batch
  frontend. Non-blocking spawn, lock-free snapshot getters, cross-process
  attach via UUID v7 primary key, `/proc/<pid>/environ` live inspection.
- **[`sbatch`](#sbatch-queue-managed-batch-jobs)** — queue-managed batch
  jobs (`sbatch --array=<spec>` / `--dependency` / `--mail-*` / `--signal`
  / `--no-requeue` / `--comment` / typed export). Polling-based wait via
  `qgroup -l` → `squeue` → `sacct`, idempotent `cancel(jobid)`,
  `refresh_with_sacct()` for terminal exit-code discovery.

Both expose the same five sync getters (`uuid` / `jobid` / `is_running()` /
`is_finished()` / `exit_code()`) and async `refresh()` / `wait_terminal()`
contract — see [Cross-backend handle abstraction](#cross-backend-handle-abstraction-jobhandlecommon).

[pyo3]: https://pyo3.rs/
[pyo3-async-runtimes]: https://github.com/PyO3/pyo3-async-runtimes

> **Low-level `srun` primitive.** A minimal `SlurmCmd` / `SlurmManager` API
> wrapping `srun` directly (`run_job` / `query_job_state` /
> `query_job_states_batch`) is also exposed for test harnesses and custom
> backend prototypes. See [`docs/architecture.md`](docs/architecture.md)
> §3.1–§3.3 (including §3.3.1 usage example) for the rationale and code.

> **Looking for in-depth docs?** This README covers the public API surface
> only. For the full architecture, code map, runtime flows, development
> workflow, and live-cluster operator checklist, start from the
> documentation index at [`docs/README.md`](docs/README.md).

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
    # uuid / jobid are sync `@property` getters (Phase 3 P5); pid is still async.
    print("uuid", handle.uuid, "pid", await handle.pid, "jobid", handle.jobid)
    code = await handle.wait()  # int on normal exit, None on signal kill
    print("exit", code)

asyncio.run(main())
```

### Handle API contract

`TssrunJobHandle` exposes a mix of **sync snapshot getters** and async
operations. All snapshot reads are lock-free against an in-flight
`wait()` / `refresh()` / `wait_terminal()` — you can poll liveness
while a wait is pending without blocking it:

```python
handle = await manager.spawn()
wait_fut = asyncio.ensure_future(handle.wait())

while not wait_fut.done():
    if handle.is_running():                      # sync (Phase 3 P5)
        print("jobid", handle.jobid, "node", await handle.node)
    await asyncio.sleep(1)

code = await wait_fut
```

The full snapshot surface:

| Reader (lock-free) | Shape | Returns | Notes |
|---|---|---|---|
| `handle.uuid` | sync `@property` | `str` | UUID v7 primary key (canonical hyphenated string). Pass straight back to `attach_uuid` |
| `await handle.pid` | async | `int` | The OS pid of the spawned `tssrun` process |
| `handle.jobid` | sync `@property` | `int \| None` | Parsed from `salloc: Granted job allocation N` |
| `await handle.node` | async | `str \| None` | Parsed from `salloc: Nodes <spec> are ready for job` |
| `await handle.sent_env` | async | `dict[str, str]` | Env explicitly passed via `TssrunCmd.env` |
| `await handle.live_env()` | async | `dict[str, str] \| None` | Reads `/proc/<pid>/environ` on Linux; `None` off-Linux or after exit |
| `handle.is_running()` | sync | `bool` | `True` until the wait task records `finished` |
| `handle.is_finished()` | sync | `bool` | Inverse of `is_running()` |
| `handle.exit_code()` | sync | `int \| None` | Available after exit; `None` for signal kill |

| Owner-only | Returns | Notes |
|---|---|---|
| `await handle.wait()` | `int \| None` | `int` = exit code; `None` = killed by signal (SLURM time-limit kill, OOM, etc.). Raises `RuntimeError` on attached / already-waited handles. |
| `await handle.refresh()` | `None` | Re-reads persisted snapshot and broadcasts it. Read the updated state via the sync getters above. Available on attached handles too. |
| `await handle.wait_terminal(poll_interval_secs)` | `None` | Polls via `refresh()` until `is_finished()` flips. Mirrors `SbatchJobHandle::wait_terminal`. Read the terminal `exit_code` via the sync getter after the await resolves. |

> Rust's `TssrunJobHandle::refresh` / `wait_terminal` return
> `Result<TssrunJobSnapshot>` — the Python pyo3 wrappers intentionally
> drop the snapshot return so Python callers go through the lock-free
> getters / `JobHandleCommon` Protocol contract instead of two parallel
> read paths.

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

For the tssrun spawn / `salloc:` parsing / attach sequence diagrams (the four
`AttachKey` variants and how they resolve through `JobStateStore`), read
[`docs/process-flow.md`](docs/process-flow.md), and see
[`docs/architecture.md`](docs/architecture.md) §3 for the tssrun subsystem
design rationale.

## sbatch (queue-managed batch jobs)

`crate::sbatch` (Rust) and
`slurm_async_runner._slurm_async_runner_core.sbatch` (Python) provide
the same spawn / attach surface for jobs submitted with `sbatch`
instead of `tssrun`. Snapshots persist alongside tssrun's in the same
state directory (the `kind` discriminator field in
`{root}/<uuid>.json` keeps the two backends separated). Typed
flag entities live in `crate::entities::slurm::sbatch_options::*` and
re-export as pyclasses under
`slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options`
(e.g. `SlurmDependency`, `SlurmSignalSpec`, `MailTypeInput`,
`SlurmArraySpec`).

`SbatchManager` mirrors `TssrunManager`:

| Operation | Method | Notes |
|---|---|---|
| Submit single job | `await mgr.spawn(cmd)` | Returns `SbatchJobHandle` |
| Submit `--array=<spec>` | `await mgr.spawn_array(cmd, array_spec)` | Returns `list[SbatchJobHandle]` (one per task) |
| Submit + block until terminal | `await mgr.run(cmd)` | Returns `FinishedInfo`. Rejects array submissions. |
| Cancel | `await mgr.cancel(jobid)` | Idempotent (delegates to `scancel`). |
| Attach by uuid / jobid / file / array | `await mgr.attach_*` | All entry points peek the `kind` discriminator. |

`SbatchJobHandle` exposes the same five sync getters (`uuid`,
`jobid`, `is_running()`, `is_finished()`, `exit_code()`) plus an
opt-in `refresh_with_sacct()` that calls `sacct` for terminal exit-code
discovery. See
[`docs/superpowers/specs/2026-05-10-sbatch-module-design.md`](docs/superpowers/specs/2026-05-10-sbatch-module-design.md)
and the Phase 2 / Phase 3 entries in [`CHANGELOG.md`](CHANGELOG.md)
for the full design rationale.

For the `wait_terminal` → `refresh_with_sacct` → `qgroup -l` → `squeue` →
`sacct` chain (including FINI/FAIL terminal recognition and the sacct gating
heuristic), read [`docs/process-flow.md`](docs/process-flow.md) §5, and see
[`docs/architecture.md`](docs/architecture.md) §3.5 for the sbatch design
judgements (poll-cadence default, watch `send_replace` invariant, sacct as
heavyweight opt-in).

## Cross-backend handle abstraction (`JobHandleCommon`)

Both `SbatchJobHandle` and `TssrunJobHandle` implement the
`JobHandleCommon` trait (Rust) / Protocol (Python), so dashboards and
orchestration code can stay backend-agnostic.

```rust
use slurm_async_runner::{JobHandleCommon, handle::into_dyn};
use std::time::Duration;

async fn watch_until_done<H: JobHandleCommon>(h: H) -> anyhow::Result<()> {
    let snap = h.wait_terminal(Duration::from_secs(5)).await?;
    println!("done, exit_code={:?}", snap.exit_code());
    Ok(())
}

// Heterogenous collection: mix sbatch + tssrun handles in one Vec.
let dyn_handles: Vec<std::sync::Arc<dyn slurm_async_runner::handle::DynJobHandleCommon>> = vec![
    into_dyn(sbatch_handle),
    into_dyn(tssrun_handle),
];
```

```python
from slurm_async_runner import JobHandleCommon

def describe(h: JobHandleCommon) -> None:
    print(h.uuid, h.jobid, h.is_running(), h.exit_code())

# Works against either pyo3 backend without importing the concrete class.
assert isinstance(sbatch_handle, JobHandleCommon)
assert isinstance(tssrun_handle, JobHandleCommon)
```

The Rust trait is **not** dyn-safe by design (it carries an associated
`Snapshot` type); use `crate::handle::into_dyn` to erase into
`Arc<dyn DynJobHandleCommon>` when you need heterogeneous collections.
See
[`docs/superpowers/specs/2026-05-11-sbatch-phase3-design.md`](docs/superpowers/specs/2026-05-11-sbatch-phase3-design.md)
for the full rationale.

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

[`.pre-commit-config.yaml`](.pre-commit-config.yaml) mirrors CI with autofix
(ruff + clippy + rustfmt). One-time setup per clone:

```bash
uv tool install pre-commit          # or: pipx install pre-commit
pre-commit install
```

See [`docs/development.md`](docs/development.md) §3.6 for the manual-sweep,
hook-rewrite reconciliation, and bypass workflow.

For the full local dev workflow (live smoke env vars, gotchas around
`handle.is_finished()` / empty `SubmitFailed.output` / watch `send_replace`
invariants, PR checklist, stub regeneration), read
[`docs/development.md`](docs/development.md).

## License

MIT.
