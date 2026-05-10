"""Integration tests for the PyO3-exported Rust extension.

The tests substitute coreutils binaries (``true`` / ``false`` / ``echo``)
for ``srun`` so they don't require a real SLURM cluster. Async coverage uses
``asyncio.run`` directly because pyo3-async-runtimes returns vanilla coroutines.
"""

import asyncio

import pytest

from slurm_async_runner._slurm_async_runner_core import sum_as_string
from slurm_async_runner._slurm_async_runner_core.manager import SlurmCmd, SlurmManager
from slurm_async_runner._slurm_async_runner_core.runner import query_job_states_batch


# ---------- legacy smoke ----------


def test_sum_as_string() -> None:
    assert sum_as_string(1, 1) == "2"


# ---------- SlurmCmd ----------


def test_slurm_cmd_default_is_srun() -> None:
    assert SlurmCmd().srun_cmd == "srun"


def test_slurm_cmd_custom_launcher() -> None:
    assert SlurmCmd("bash").srun_cmd == "bash"


def test_slurm_cmd_repr() -> None:
    assert "srun" in repr(SlurmCmd())


def test_slurm_cmd_eq_and_hash() -> None:
    a = SlurmCmd("bash")
    b = SlurmCmd("bash")
    c = SlurmCmd("zsh")
    assert a == b
    assert a != c
    assert hash(a) == hash(b)


# ---------- SlurmManager ----------


def test_manager_default_uses_srun() -> None:
    assert SlurmManager().slurm_cmd.srun_cmd == "srun"


def test_manager_explicit_cmd() -> None:
    m = SlurmManager(SlurmCmd("bash"))
    assert m.slurm_cmd.srun_cmd == "bash"


def test_manager_build_argv_absolutizes_path() -> None:
    argv = SlurmManager().build_argv("/tmp/job.sh")
    assert argv == ["srun", "/tmp/job.sh"]


def test_manager_run_job_dry_run() -> None:
    async def _go() -> int:
        return await SlurmManager().run_job("/tmp/dummy.sh", dry_run=True)

    assert asyncio.run(_go()) == 0


def test_manager_run_job_with_true_binary() -> None:
    async def _go() -> int:
        return await SlurmManager(SlurmCmd("true")).run_job(
            "/tmp/dummy.sh", dry_run=False
        )

    assert asyncio.run(_go()) == 0


def test_manager_run_job_with_false_binary() -> None:
    async def _go() -> int:
        return await SlurmManager(SlurmCmd("false")).run_job(
            "/tmp/dummy.sh", dry_run=False
        )

    assert asyncio.run(_go()) == 1


# ---------- runner.query_job_states_batch ----------


def test_query_job_states_batch_empty_input() -> None:
    """Empty list short-circuits without spawning subprocess."""

    async def _go() -> dict[int, object]:
        return await query_job_states_batch([])

    assert asyncio.run(_go()) == {}


def test_query_job_states_batch_returns_dict() -> None:
    """Without a real SLURM cluster, missing ids fall back to default JobStatus.

    `squeue` / `sacct` are expected to either be absent (RuntimeError surfaces)
    or return nothing (every input id maps to default Unknown). We assert the
    contract shape, not the SLURM behavior.
    """

    async def _go() -> dict[int, object]:
        return await query_job_states_batch([99999999])

    try:
        result = asyncio.run(_go())
    except (RuntimeError, ImportError):
        pytest.skip(
            "squeue/sacct unavailable, or upstream gaussian_job_shared "
            "Python wheel not installed in this environment"
        )
    assert set(result.keys()) == {99999999}
