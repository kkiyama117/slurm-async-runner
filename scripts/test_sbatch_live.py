"""Live smoke test for the sbatch wrapper on real KUDPC / SLURM nodes.

Runs two cases back-to-back against a real `sbatch` binary:

1. **success** — job exits 0; expect ``handle.exit_code() == 0`` and
   ``handle.is_finished()`` after ``refresh_with_sacct``.
2. **failure** — job exits 7; expect ``handle.exit_code() == 7`` and
   ``handle.is_finished()``. This pins the non-zero-exit propagation
   from sacct's ``ExitCode`` column all the way to the Python handle.

Standalone:    uv run python scripts/test_sbatch_live.py
Skipped (exit 0) if `sbatch` binary is not on PATH.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import sys
import tempfile
from pathlib import Path

from slurm_async_runner._slurm_async_runner_core.sbatch import SbatchCmd, SbatchManager


def _env(name: str, default: str | None = None) -> str | None:
    val = os.environ.get(name)
    return val if val is not None else default


def _have_sbatch() -> bool:
    return shutil.which(_env("SBATCH_LIVE_BIN", "sbatch") or "sbatch") is not None


def _write_job(job_dir: Path, script_body: str) -> Path:
    job_path = job_dir / "live_job.sh"
    job_path.write_text(script_body)
    job_path.chmod(0o755)
    return job_path


async def _run_one_case(
    state_dir: Path,
    job_dir: Path,
    label: str,
    script_body: str,
    expected_exit_code: int,
    timeout_s: float,
    poll_interval_s: float,
) -> bool:
    """Submit ``script_body`` as one sbatch job, wait for terminal, and
    assert the post-sacct ``exit_code`` matches ``expected_exit_code``.

    Returns True on pass, False on fail (caller aggregates).
    """
    bin_path = _env("SBATCH_LIVE_BIN", "sbatch") or "sbatch"
    queue = _env("SBATCH_LIVE_QUEUE")
    time_limit = _env("SBATCH_LIVE_TIME_LIMIT", "0:01:00")
    rsc = _env("SBATCH_LIVE_RSC")

    job_path = _write_job(job_dir, script_body)

    cmd = SbatchCmd(
        str(job_path),
        sbatch_bin=bin_path,
        partition=queue,
        time_limit=time_limit,
        rsc=rsc,
        output=str(job_dir / f"stdout-{label}-%j.txt"),
        error=str(job_dir / f"stderr-{label}-%j.txt"),
    )
    mgr = SbatchManager(cmd, state_dir=str(state_dir))
    handle = await mgr.spawn()
    print(f"[live:{label}] submitted: jobid={handle.jobid} uuid={handle.uuid}")

    try:
        await asyncio.wait_for(
            handle.wait_terminal(poll_interval_secs=poll_interval_s),
            timeout=timeout_s,
        )
    except asyncio.TimeoutError:
        print(
            f"[live:{label}] FAIL: timed out after {timeout_s}s waiting for terminal."
        )
        return False

    await handle.refresh_with_sacct()

    finished = handle.is_finished()
    exit_code = handle.exit_code()
    out = handle.output_path
    print(f"[live:{label}] terminal: finished={finished} exit_code={exit_code}")
    print(f"[live:{label}] output_path={out}")
    if out and Path(out).exists():
        print(f"[live:{label}] stdout: {Path(out).read_text()[:200]}")

    if not finished:
        print(
            f"[live:{label}] FAIL: handle.is_finished() returned False after sacct refresh."
        )
        return False
    if exit_code != expected_exit_code:
        print(
            f"[live:{label}] FAIL: exit_code mismatch — "
            f"expected={expected_exit_code}, got={exit_code}"
        )
        return False

    attached = await mgr.attach_uuid(handle.uuid)
    if attached.jobid != handle.jobid:
        print(f"[live:{label}] FAIL: attach_uuid round-trip jobid mismatch.")
        return False
    print(f"[live:{label}] PASS")
    return True


async def _run_live() -> int:
    if not _have_sbatch():
        print("SKIP: sbatch not on PATH; run on a kudpc / SLURM node.")
        return 0

    timeout_s = float(_env("SBATCH_LIVE_TIMEOUT", "180") or "180")
    poll_interval_s = float(_env("SBATCH_LIVE_POLL_INTERVAL", "10") or "10")

    state_dir = Path(tempfile.mkdtemp(prefix="sbatch-live-"))

    success_script = (
        "#!/usr/bin/env bash\n"
        'echo "[live:success] starting"\n'
        "sleep 5\n"
        'echo "[live:success] finished"\n'
    )
    failure_script = (
        "#!/usr/bin/env bash\n"
        'echo "[live:failure] starting; will exit 7"\n'
        "sleep 1\n"
        'echo "[live:failure] exiting 7" 1>&2\n'
        "exit 7\n"
    )

    results: list[tuple[str, bool]] = []
    with tempfile.TemporaryDirectory(prefix="sbatch-live-success-") as job_dir_ok:
        ok = await _run_one_case(
            state_dir=state_dir,
            job_dir=Path(job_dir_ok),
            label="success",
            script_body=success_script,
            expected_exit_code=0,
            timeout_s=timeout_s,
            poll_interval_s=poll_interval_s,
        )
        results.append(("success", ok))

    with tempfile.TemporaryDirectory(prefix="sbatch-live-failure-") as job_dir_fail:
        ok = await _run_one_case(
            state_dir=state_dir,
            job_dir=Path(job_dir_fail),
            label="failure",
            script_body=failure_script,
            expected_exit_code=7,
            timeout_s=timeout_s,
            poll_interval_s=poll_interval_s,
        )
        results.append(("failure", ok))

    failed = [name for name, passed in results if not passed]
    if failed:
        print(f"FAIL: cases failed: {', '.join(failed)}")
        return 1
    print("PASS (all cases)")
    return 0


def main() -> int:
    return asyncio.run(_run_live())


if __name__ == "__main__":
    sys.exit(main())
