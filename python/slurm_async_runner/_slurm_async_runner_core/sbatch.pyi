# Hand-written stubs for slurm_async_runner._slurm_async_runner_core.sbatch.
# pyo3-stub-gen does not derive stubs for pyclasses inside #[pymodule]
# sub-modules wired via #[pymodule_export], so this file is maintained
# by hand. Keep it in sync with src/py_export/sbatch.rs.
# ruff: noqa: E501, F401, F403, F405

import builtins
import os
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, final

if TYPE_CHECKING:
    from slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options import (
        MailTypeInput,
        SlurmArraySpec,
        SlurmDependency,
        SlurmSignalSpec,
    )

__all__ = [
    "SbatchCmd",
    "SbatchManager",
    "SbatchJobHandle",
    "FinishedInfo",
]

@final
class SbatchCmd:
    """Spec for one ``sbatch`` invocation. Pure data + ``build_argv`` (Rust-side)."""

    def __init__(
        self,
        script: builtins.str | os.PathLike[builtins.str],
        *,
        sbatch_bin: builtins.str = "sbatch",
        job_name: builtins.str | None = None,
        partition: builtins.str | None = None,
        time_limit: builtins.str | None = None,
        rsc: builtins.str | None = None,
        output: builtins.str | None = None,
        error: builtins.str | None = None,
        chdir: builtins.str | os.PathLike[builtins.str] | None = None,
        env: builtins.dict[builtins.str, builtins.str] | None = None,
        args: builtins.list[builtins.str] | None = None,
        no_requeue: builtins.bool = False,
        comment: builtins.str | None = None,
        nice: builtins.int | None = None,
        dependency: "SlurmDependency | None" = None,
        mail_user: builtins.str | None = None,
        mail_types: "MailTypeInput | None" = None,
        signal: "SlurmSignalSpec | None" = None,
        array_spec: "SlurmArraySpec | None" = None,
    ) -> None: ...

@final
class SbatchJobHandle:
    """Handle to an in-flight or attached sbatch job. Lock-free reads.

    Note: ``refresh()`` and ``refresh_with_sacct()`` return ``None`` —
    the updated snapshot is observable via the property getters
    (``jobid``, ``is_running``, etc.) afterwards. ``jobid`` is always
    ``Some(int)`` for spawned/attached handles since sbatch always
    returns a jobid; the ``Optional[int]`` return shape mirrors the
    underlying Rust trait but the value is never ``None`` in practice.
    """

    @property
    def uuid(self) -> builtins.str: ...
    @property
    def jobid(self) -> builtins.int | None: ...
    @property
    def array_task_id(self) -> builtins.int | None: ...
    @property
    def partition(self) -> builtins.str | None: ...
    @property
    def job_name(self) -> builtins.str | None: ...
    @property
    def sent_env(self) -> builtins.dict[builtins.str, builtins.str]: ...
    @property
    def output_template(self) -> builtins.str | None: ...
    @property
    def error_template(self) -> builtins.str | None: ...
    @property
    def output_path(self) -> os.PathLike[builtins.str] | None: ...
    @property
    def error_path(self) -> os.PathLike[builtins.str] | None: ...
    def is_running(self) -> builtins.bool: ...
    def is_finished(self) -> builtins.bool: ...
    def exit_code(self) -> builtins.int | None: ...
    def refresh(self) -> Awaitable[None]: ...
    def refresh_with_sacct(self) -> Awaitable[None]: ...
    def wait_terminal(self, poll_interval_secs: builtins.float) -> Awaitable[None]: ...
    def log_lines(
        self, stream: builtins.int, n: builtins.int
    ) -> Awaitable[builtins.list[builtins.str]]:
        """Read the last ``n`` lines of the job's stdout (stream=0) or stderr (stream=1).

        Returns an empty list if the log file does not yet exist.
        Raises ``ValueError`` if ``stream`` is not 0 or 1.
        """
        ...

    def read_log_to_end(self, stream: builtins.int) -> Awaitable[builtins.str]:
        """Read the full contents of the job's stdout (0) or stderr (1) log.

        Returns an empty string if the log file does not yet exist.
        Same error semantics as ``log_lines``.
        """
        ...

@final
class FinishedInfo:
    """Outcome of a finished sbatch job. Returned by ``SbatchManager.run``.

    ``final_state`` is the SLURM state string (e.g. ``"COMPLETED"``, ``"FAILED"``).
    ``final_reason`` is the SLURM reason string (``%r`` column from ``sacct``):
    ``"None"`` for a clean completion, or one of ``"NonZeroExitCode"`` /
    ``"TimeLimit"`` / ``"OutOfMemory"`` / etc. on failure. Unknown reasons
    are returned verbatim as the raw scheduler string.
    ``exit_code`` is the conventional Unix exit code: ``None`` if not resolvable,
    ``128 + signum`` if killed by signal.
    ``finished_at`` is an RFC3339 timestamp string.
    """

    @property
    def final_state(self) -> builtins.str: ...
    @property
    def final_reason(self) -> builtins.str: ...
    @property
    def exit_code(self) -> builtins.int | None: ...
    @property
    def finished_at(self) -> builtins.str: ...

@final
class SbatchManager:
    """Spawn / attach orchestrator. Holds a [`SbatchCmd`] and an optional state dir."""

    def __init__(
        self,
        cmd: SbatchCmd,
        *,
        state_dir: builtins.str | os.PathLike[builtins.str] | None = None,
        scancel_bin: builtins.str | None = None,
    ) -> None: ...
    def spawn(self) -> Awaitable[SbatchJobHandle]: ...
    def attach_uuid(self, uuid: builtins.str) -> Awaitable[SbatchJobHandle]: ...
    def attach_jobid(self, jobid: builtins.int) -> Awaitable[SbatchJobHandle]: ...
    def attach_file(
        self,
        path: builtins.str | os.PathLike[builtins.str],
    ) -> Awaitable[SbatchJobHandle]: ...
    def spawn_array(
        self, array_spec: "SlurmArraySpec"
    ) -> Awaitable[builtins.list[SbatchJobHandle]]: ...
    def attach_array_jobid(
        self, master_jobid: builtins.int
    ) -> Awaitable[builtins.list[SbatchJobHandle]]: ...
    def run(self) -> Awaitable[FinishedInfo]:
        """Submit one job, block until terminal state, return ``FinishedInfo``.

        Raises ``ValueError`` if ``cmd.array_spec`` is set — use ``spawn_array``
        for array submissions. Raises ``RuntimeError`` for spawn / wait / sacct
        failures or non-success terminal states.

        Note: wrapping with ``asyncio.wait_for(mgr.run(), timeout=...)``
        drops the future and strands the jobid. Use
        ``run_with_jobid_callback`` instead if you need to call
        ``cancel(jobid)`` after a timeout.
        """
        ...

    def run_with_jobid_callback(
        self,
        on_spawn: Callable[[builtins.int], object],
    ) -> Awaitable[FinishedInfo]:
        """Same as ``run()`` but invokes ``on_spawn(jobid)`` synchronously
        the moment sbatch returns a parseable jobid.

        Use this when wrapping the resulting awaitable in
        ``asyncio.wait_for(..., timeout=...)`` so you can recover the
        jobid for ``cancel(jobid)`` if the timeout fires.

        Same error contract as ``run()``: ``ValueError`` for array
        submissions (callback is NOT invoked in that case),
        ``RuntimeError`` for spawn / wait / sacct failures.
        Exceptions raised by ``on_spawn`` surface as ``RuntimeError``.
        """
        ...

    def cancel(self, jobid: builtins.int) -> Awaitable[None]:
        """Send ``scancel <jobid>``. Idempotent on the SLURM side.

        Raises ``RuntimeError`` if scancel itself reports a non-zero exit.
        """
        ...
