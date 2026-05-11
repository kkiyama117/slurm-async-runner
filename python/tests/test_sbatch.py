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


def test_sbatch_cmd_dependency_kwarg(tmp_path):
    from slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options import (
        SlurmDependency,
    )

    job = tmp_path / "job.sh"
    job.write_text("#!/usr/bin/env bash\necho hi\n")
    cmd = SbatchCmd(str(job), dependency=SlurmDependency.parse("afterok:200"))
    assert cmd is not None


def test_sbatch_cmd_mail_user_kwarg(tmp_path):
    job = tmp_path / "job.sh"
    job.write_text("#!/usr/bin/env bash\necho hi\n")
    cmd = SbatchCmd(str(job), mail_user="alice@example.com")
    assert cmd is not None


def test_sbatch_cmd_mail_types_kwarg(tmp_path):
    from slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options import (
        MailTypeInput,
    )

    job = tmp_path / "job.sh"
    job.write_text("#!/usr/bin/env bash\necho hi\n")
    cmd = SbatchCmd(str(job), mail_types=MailTypeInput.parse("BEGIN,END"))
    assert cmd is not None


def test_sbatch_cmd_kwargs_listed_in_runtime_signature():
    """Smoke: pyo3 must accept all three P2 kwargs as recognized names."""
    cmd = SbatchCmd(
        "/tmp/job.sh",
        dependency=None,
        mail_user=None,
        mail_types=None,
    )
    assert cmd is not None


def test_sbatch_cmd_build_argv_rejects_comma_in_value(tmp_path):
    job = tmp_path / "job.sh"
    job.write_text("#!/usr/bin/env bash\necho hi\n")
    cmd = SbatchCmd(str(job), env={"FOO": "a,b"})
    with pytest.raises(RuntimeError, match="FOO"):
        cmd.build_argv()


def test_sbatch_cmd_signal_kwarg(tmp_path):
    from slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options import (
        SlurmSignalSpec,
    )

    job = tmp_path / "job.sh"
    job.write_text("#!/usr/bin/env bash\necho hi\n")
    cmd = SbatchCmd(str(job), signal=SlurmSignalSpec.parse("USR1@60"))
    argv = cmd.build_argv()
    i = argv.index("--signal")
    assert argv[i + 1] == "USR1@60"


def test_sbatch_cmd_array_spec_kwarg(tmp_path):
    from slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options import (
        SlurmArraySpec,
    )

    job = tmp_path / "job.sh"
    job.write_text("#!/usr/bin/env bash\necho hi\n")
    cmd = SbatchCmd(str(job), array_spec=SlurmArraySpec.parse("0-3"))
    argv = cmd.build_argv()
    i = argv.index("-a")
    assert argv[i + 1] == "0-3"


@pytest.mark.skipif(not _have_bash(), reason="bash required")
def test_spawn_array_with_bash_fake_sbatch(tmp_path: Path):
    fake_sbatch = tmp_path / "fake_sbatch"
    fake_sbatch.write_text('#!/usr/bin/env bash\necho "Submitted batch job 88888"\n')
    fake_sbatch.chmod(0o755)
    job_script = tmp_path / "job.sh"
    job_script.write_text("#!/usr/bin/env bash\necho hello\n")
    job_script.chmod(0o755)

    from slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options import (
        SlurmArraySpec,
    )

    cmd = SbatchCmd(str(job_script), sbatch_bin=str(fake_sbatch))
    state_dir = tmp_path / "state"
    mgr = SbatchManager(cmd, state_dir=str(state_dir))

    async def go():
        handles = await mgr.spawn_array(SlurmArraySpec.parse("0-2"))
        return [(h.jobid, h.array_task_id) for h in handles]

    result = asyncio.run(go())
    assert len(result) == 3
    assert all(jobid == 88888 for jobid, _ in result)
    assert [t for _, t in result] == [0, 1, 2]


@pytest.mark.skipif(not _have_bash(), reason="bash required")
def test_attach_array_jobid_round_trips(tmp_path: Path):
    fake_sbatch = tmp_path / "fake_sbatch"
    fake_sbatch.write_text('#!/usr/bin/env bash\necho "Submitted batch job 99001"\n')
    fake_sbatch.chmod(0o755)
    job_script = tmp_path / "job.sh"
    job_script.write_text("#!/usr/bin/env bash\necho hi\n")
    job_script.chmod(0o755)

    from slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options import (
        SlurmArraySpec,
    )

    cmd = SbatchCmd(str(job_script), sbatch_bin=str(fake_sbatch))
    state_dir = tmp_path / "state"
    mgr = SbatchManager(cmd, state_dir=str(state_dir))

    async def go():
        await mgr.spawn_array(SlurmArraySpec.parse("0-1"))
        return await mgr.attach_array_jobid(99001)

    handles = asyncio.run(go())
    assert len(handles) == 2
    assert handles[0].array_task_id == 0
    assert handles[1].array_task_id == 1
