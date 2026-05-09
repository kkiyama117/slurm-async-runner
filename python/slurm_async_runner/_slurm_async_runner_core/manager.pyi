# Hand-written stubs for slurm_async_runner._slurm_async_runner_core.manager.
#
# pyo3-stub-gen does not derive stubs for:
#   - pyclasses inside #[pymodule] sub-modules wired via #[pymodule_export]
#   - async pyfunctions returned via pyo3_async_runtimes::tokio::future_into_py
# so this file is maintained by hand. Keep it in sync with src/py_export/manager.rs.
# ruff: noqa: E501, F401, F403, F405

import builtins
import os
from collections.abc import Awaitable
from typing import final

from gaussian_job_shared._core.entities.slurm.status import JobStatus

__all__ = [
    "SlurmCmd",
    "SlurmManager",
]

@final
class SlurmCmd:
    """Frozen wrapper around the launcher binary name (default: ``srun``)."""

    srun_cmd: builtins.str

    def __init__(self, srun_cmd: builtins.str = ...) -> None: ...
    def __repr__(self) -> builtins.str: ...
    def __eq__(self, other: builtins.object) -> builtins.bool: ...
    def __hash__(self) -> builtins.int: ...

@final
class SlurmManager:
    """Async SLURM dispatcher. ``run_job`` / ``query_job_state*`` return coroutines."""

    slurm_cmd: SlurmCmd

    def __init__(self, slurm_cmd: SlurmCmd | None = ...) -> None: ...
    def build_argv(
        self, batch_file: builtins.str | os.PathLike[builtins.str]
    ) -> builtins.list[builtins.str]:
        """Build the argv that would dispatch ``batch_file``."""

    def run_job(
        self,
        batch_file: builtins.str | os.PathLike[builtins.str],
        dry_run: builtins.bool = ...,
    ) -> Awaitable[builtins.int]:
        """Dispatch ``batch_file`` via the configured launcher.

        Returns the subprocess exit code (or ``0`` for ``dry_run=True``).
        """

    def query_job_state(self, jobid: builtins.int) -> Awaitable[JobStatus]:
        """Single-jobid form. Returns a ``JobStatus`` with state + reason."""

    def query_job_states_batch(
        self, jobids: builtins.list[builtins.int]
    ) -> Awaitable[builtins.dict[builtins.int, JobStatus]]:
        """Bulk-query ``squeue`` then ``sacct`` for the given jobids."""

    def __repr__(self) -> builtins.str: ...
