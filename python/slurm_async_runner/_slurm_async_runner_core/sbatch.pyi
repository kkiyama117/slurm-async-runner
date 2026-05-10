# Hand-written stubs for slurm_async_runner._slurm_async_runner_core.sbatch.
# pyo3-stub-gen does not derive stubs for pyclasses inside #[pymodule]
# sub-modules wired via #[pymodule_export], so this file is maintained
# by hand. Keep it in sync with src/py_export/sbatch.rs.
# ruff: noqa: E501, F401, F403, F405

import builtins
import os
from collections.abc import Awaitable
from typing import final

__all__ = [
    "SbatchCmd",
    "SbatchManager",
    "SbatchJobHandle",
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

    Note (Phase 1 limitation): ``exit_code()`` returns ``None`` even
    after a successful ``refresh_with_sacct()`` call. The sacct
    ``ExitCode`` column is not yet parsed by the underlying Rust
    implementation. A future release will extend the sacct parser to
    capture exit codes.
    """

    @property
    def uuid(self) -> builtins.str: ...
    @property
    def jobid(self) -> builtins.int | None: ...
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

@final
class SbatchManager:
    """Spawn / attach orchestrator. Holds a [`SbatchCmd`] and an optional state dir."""

    def __init__(
        self,
        cmd: SbatchCmd,
        *,
        state_dir: builtins.str | os.PathLike[builtins.str] | None = None,
    ) -> None: ...
    def spawn(self) -> Awaitable[SbatchJobHandle]: ...
    def attach_uuid(self, uuid: builtins.str) -> Awaitable[SbatchJobHandle]: ...
    def attach_jobid(self, jobid: builtins.int) -> Awaitable[SbatchJobHandle]: ...
    def attach_file(
        self,
        path: builtins.str | os.PathLike[builtins.str],
    ) -> Awaitable[SbatchJobHandle]: ...
