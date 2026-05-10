import asyncio
import json
import shutil
from pathlib import Path

import pytest

from slurm_async_runner._slurm_async_runner_core.sbatch import SbatchCmd, SbatchManager


def test_sbatch_cmd_minimal_construction():
    cmd = SbatchCmd("/work/job.sh")
    assert cmd is not None
    cmd = SbatchCmd(
        "/work/job.sh",
        partition="gr19999b",
        time_limit="1:00:00",
        rsc="p=4:c=8:m=2G",
        output="slurm-%j.out",
        env={"FOO": "bar"},
    )
    assert cmd is not None


def _have_bash() -> bool:
    return shutil.which("bash") is not None


@pytest.mark.skipif(not _have_bash(), reason="bash required")
def test_spawn_with_bash_emulating_sbatch_yields_jobid(tmp_path: Path):
    fake_sbatch = tmp_path / "fake_sbatch"
    fake_sbatch.write_text('#!/usr/bin/env bash\necho "Submitted batch job 99999"\n')
    fake_sbatch.chmod(0o755)
    job_script = tmp_path / "job.sh"
    job_script.write_text("#!/usr/bin/env bash\necho hello\n")
    job_script.chmod(0o755)

    cmd = SbatchCmd(str(job_script), sbatch_bin=str(fake_sbatch))
    state_dir = tmp_path / "state"
    mgr = SbatchManager(cmd, state_dir=str(state_dir))

    async def go():
        h = await mgr.spawn()
        return h.jobid, h.uuid

    jobid, uuid = asyncio.run(go())
    assert jobid == 99999
    snap_file = state_dir / f"{uuid}.json"
    assert snap_file.exists()
    body = json.loads(snap_file.read_text())
    assert body["kind"] == "sbatch"
    assert body["jobid"] == 99999


@pytest.mark.skipif(not _have_bash(), reason="bash required")
def test_attach_uuid_round_trips(tmp_path: Path):
    fake_sbatch = tmp_path / "fake_sbatch"
    fake_sbatch.write_text("#!/usr/bin/env bash\necho 'Submitted batch job 12345'\n")
    fake_sbatch.chmod(0o755)
    job = tmp_path / "j.sh"
    job.write_text("#!/usr/bin/env bash\n:\n")
    job.chmod(0o755)

    state_dir = tmp_path / "state"
    cmd = SbatchCmd(str(job), sbatch_bin=str(fake_sbatch))
    mgr = SbatchManager(cmd, state_dir=str(state_dir))

    async def go():
        h = await mgr.spawn()
        h2 = await mgr.attach_uuid(h.uuid)
        return h.uuid == h2.uuid and h2.jobid == 12345

    assert asyncio.run(go())


def test_sbatch_cmd_no_requeue_kwarg(tmp_path):
    """no_requeue=True kwarg should produce --no-requeue in argv."""
    job = tmp_path / "job.sh"
    job.write_text("#!/bin/sh\necho hi\n")

    cmd = SbatchCmd(str(job), no_requeue=True)
    argv = cmd.build_argv()
    assert "--no-requeue" in argv


def test_sbatch_cmd_comment_kwarg(tmp_path):
    """comment kwarg should produce --comment <value> in argv."""
    job = tmp_path / "job.sh"
    job.write_text("#!/bin/sh\necho hi\n")

    cmd = SbatchCmd(str(job), comment="phase 2 smoke")
    argv = cmd.build_argv()
    i = argv.index("--comment")
    assert argv[i + 1] == "phase 2 smoke"
