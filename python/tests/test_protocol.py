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
