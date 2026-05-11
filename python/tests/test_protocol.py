"""Phase 3 P4: verify the duck-typed ``JobHandleCommon`` Protocol.

The Protocol is a structural type — ``runtime_checkable`` inspects method
*names* on the class, so we exercise both class-level ``hasattr`` checks
and ``isinstance`` against constructed handles to catch contract drift.

Instance-level construction is via ``TssrunManager.spawn`` (does not
require a real SLURM cluster — uses a bash mock for the binary). The
sbatch side is exercised via class-level ``hasattr`` only because
constructing a sbatch handle requires a working ``sbatch`` binary or a
mock dispatcher that pyo3 does not expose to Python.
"""

import asyncio
import tempfile
from pathlib import Path

from slurm_async_runner import JobHandleCommon
from slurm_async_runner._slurm_async_runner_core.sbatch import SbatchJobHandle
from slurm_async_runner._slurm_async_runner_core.tssrun import (
    TssrunCmd,
    TssrunJobHandle,
    TssrunManager,
)


def _required_methods() -> tuple[str, ...]:
    return (
        "uuid",
        "jobid",
        "is_running",
        "is_finished",
        "exit_code",
        "refresh",
        "wait_terminal",
    )


def test_tssrun_jobhandle_has_protocol_methods_at_class_level() -> None:
    """The pyo3-generated class should expose every name the Protocol requires."""
    for name in _required_methods():
        assert hasattr(TssrunJobHandle, name), (
            f"TssrunJobHandle missing required Protocol method: {name}"
        )


def test_sbatch_jobhandle_has_protocol_methods_at_class_level() -> None:
    for name in _required_methods():
        assert hasattr(SbatchJobHandle, name), (
            f"SbatchJobHandle missing required Protocol method: {name}"
        )


def test_tssrun_jobhandle_instance_satisfies_protocol() -> None:
    """``isinstance`` against a real constructed TssrunJobHandle should pass."""

    async def run() -> None:
        with tempfile.TemporaryDirectory() as td:
            script = Path(td) / "ok.sh"
            script.write_text("#!/bin/bash\nexit 0\n")
            script.chmod(0o755)
            cmd = TssrunCmd(program=script, tssrun_bin="bash")
            manager = TssrunManager(cmd)
            handle = await manager.spawn()
            assert isinstance(handle, JobHandleCommon), (
                "TssrunJobHandle must satisfy the runtime_checkable JobHandleCommon Protocol"
            )
            # Drain the spawned child so the wait task can complete.
            await handle.wait()

    asyncio.run(run())


def test_tssrun_jobhandle_call_shape_matches_protocol() -> None:
    """Phase 3 P5 regression: every Protocol-declared sync member must
    actually be sync on a real TssrunJobHandle instance.

    Pre-P5 the Protocol declared sync signatures while the pyo3 binding
    wrapped each getter in ``future_into_py`` — ``isinstance`` passed
    (name-only) but ``h.uuid()`` raised ``TypeError`` at runtime. Exercise
    the real call shape here so the contract drift can't reappear silently.
    """

    async def run() -> None:
        with tempfile.TemporaryDirectory() as td:
            script = Path(td) / "ok.sh"
            script.write_text("#!/bin/bash\nexit 0\n")
            script.chmod(0o755)
            cmd = TssrunCmd(program=script, tssrun_bin="bash")
            manager = TssrunManager(cmd)
            handle = await manager.spawn()
            try:
                # Properties — accessed without parentheses, no await.
                assert isinstance(handle.uuid, str)
                # ``jobid`` may be None pre-banner; type is what matters here.
                assert handle.jobid is None or isinstance(handle.jobid, int)
                # Methods — called with parentheses, no await.
                assert isinstance(handle.is_running(), bool)
                assert isinstance(handle.is_finished(), bool)
                # ``exit_code`` is None until the child exits.
                assert handle.exit_code() is None or isinstance(handle.exit_code(), int)
            finally:
                await handle.wait()

    asyncio.run(run())
