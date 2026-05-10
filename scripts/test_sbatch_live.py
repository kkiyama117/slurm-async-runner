"""Live smoke test for the sbatch wrapper on real KUDPC / SLURM nodes.

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


async def _run_live() -> int:
    if not _have_sbatch():
        print("SKIP: sbatch not on PATH; run on a kudpc / SLURM node.")
        return 0

    bin_path = _env("SBATCH_LIVE_BIN", "sbatch") or "sbatch"
    queue = _env("SBATCH_LIVE_QUEUE")
    time_limit = _env("SBATCH_LIVE_TIME_LIMIT", "0:01:00")
    rsc = _env("SBATCH_LIVE_RSC")
    timeout_s = float(_env("SBATCH_LIVE_TIMEOUT", "180") or "180")

    state_dir = Path(tempfile.mkdtemp(prefix="sbatch-live-"))
    with tempfile.TemporaryDirectory(prefix="sbatch-live-job-") as job_dir:
        job_path = Path(job_dir) / "live_job.sh"
        job_path.write_text(
            "#!/usr/bin/env bash\n"
            'echo "[live] starting"\n'
            "sleep 5\n"
            'echo "[live] finished"\n'
        )
        job_path.chmod(0o755)

        cmd = SbatchCmd(
            str(job_path),
            sbatch_bin=bin_path,
            partition=queue,
            time_limit=time_limit,
            rsc=rsc,
            output=str(Path(job_dir) / "stdout-%j.txt"),
            error=str(Path(job_dir) / "stderr-%j.txt"),
        )
        mgr = SbatchManager(cmd, state_dir=str(state_dir))
        handle = await mgr.spawn()
        print(f"[live] submitted: jobid={handle.jobid} uuid={handle.uuid}")

        try:
            await asyncio.wait_for(
                handle.wait_terminal(poll_interval_secs=10.0),
                timeout=timeout_s,
            )
        except asyncio.TimeoutError:
            print(f"FAIL: timed out after {timeout_s}s waiting for terminal.")
            return 1

        await handle.refresh_with_sacct()

        finished = handle.is_finished()
        exit_code = handle.exit_code()
        out = handle.output_path
        print(f"[live] terminal: finished={finished} exit_code={exit_code}")
        print(f"[live] output_path={out}")
        if out and Path(out).exists():
            print(f"[live] stdout: {Path(out).read_text()[:200]}")

        attached = await mgr.attach_uuid(handle.uuid)
        assert attached.jobid == handle.jobid, "attach round-trip failed"
        print("[live] attach_uuid round-trip OK")

    print("PASS")
    return 0


def main() -> int:
    return asyncio.run(_run_live())


if __name__ == "__main__":
    sys.exit(main())
