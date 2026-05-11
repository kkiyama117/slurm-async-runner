# Hand-written stubs for slurm_async_runner._slurm_async_runner_core.tssrun.
# pyo3-stub-gen does not derive stubs for pyclasses inside #[pymodule]
# sub-modules wired via #[pymodule_export], so this file is maintained
# by hand. Keep it in sync with src/py_export/tssrun.rs.
# ruff: noqa: E501, F401, F403, F405

import builtins
import os
from collections.abc import Awaitable
from typing import final

from slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options import (
    JobTimeLimit,
    ResourceSpec,
)

__all__ = [
    "ResourceSpec",
    "JobTimeLimit",
    "TssrunCmd",
    "LogSink",
    "JobStateStore",
    "TssrunJobHandle",
    "TssrunManager",
    "null_log_sink",
    "std_log_sink",
    "file_log_sink",
    "in_memory_state_store",
    "file_system_state_store",
]

@final
class TssrunCmd:
    """Spec for one ``tssrun`` invocation. Pure data + ``build_argv``."""
    def __init__(
        self,
        program: builtins.str | os.PathLike[builtins.str],
        args: builtins.list[builtins.str] = ...,
        partition: builtins.str | None = ...,
        time_limit: JobTimeLimit | None = ...,
        rsc: ResourceSpec | None = ...,
        x11: builtins.bool = ...,
        env: builtins.dict[builtins.str, builtins.str] = ...,
        cwd: builtins.str | os.PathLike[builtins.str] | None = ...,
        tssrun_bin: builtins.str = ...,
    ) -> None: ...
    def build_argv(self) -> builtins.list[builtins.str]: ...

@final
class LogSink:
    """Opaque handle to a Rust log sink. Construct via the factory helpers."""

def null_log_sink() -> LogSink: ...
def std_log_sink() -> LogSink: ...
def file_log_sink(stdout: builtins.str, stderr: builtins.str) -> Awaitable[LogSink]: ...

@final
class JobStateStore:
    """Opaque handle to a Rust ``JobStateStore`` backend.

    Construct via :func:`in_memory_state_store` or
    :func:`file_system_state_store`. The trait is intentionally not
    subclassable from Python — add new backends in Rust and re-export
    them through the wrapper.
    """

def in_memory_state_store() -> JobStateStore:
    """Process-local in-memory snapshot store. Default for ``TssrunManager``."""
    ...

def file_system_state_store(
    dir: builtins.str | os.PathLike[builtins.str],
) -> JobStateStore:
    """On-disk store rooted at ``dir`` — writes ``{dir}/{uuid}.json`` via atomic rename.

    The directory is created lazily on first save, so a not-yet-existing
    path is fine as long as its parent is writable when persistence fires.
    """
    ...

@final
class TssrunJobHandle:
    """In-process or attached handle to a ``tssrun`` child."""
    @property
    def pid(self) -> Awaitable[builtins.int]: ...
    @property
    def jobid(self) -> Awaitable[builtins.int | None]: ...
    @property
    def node(self) -> Awaitable[builtins.str | None]: ...
    # Canonical hyphenated UUID v7 primary key (e.g.
    # "0190cc1c-7a48-7c0e-a0a0-1234567890ab"). Round-trips through
    # ``TssrunManager.attach_uuid``.
    @property
    def uuid(self) -> Awaitable[builtins.str]: ...
    @property
    def sent_env(self) -> Awaitable[builtins.dict[builtins.str, builtins.str]]: ...
    def live_env(
        self,
    ) -> Awaitable[builtins.dict[builtins.str, builtins.str] | None]: ...
    def is_running(self) -> Awaitable[builtins.bool]: ...
    def exit_code(self) -> Awaitable[builtins.int | None]: ...
    # ``wait`` returns ``None`` when the child was killed by a signal
    # (e.g. SLURM time-limit kill, OOM). An ``int`` is the literal exit code.
    def wait(self) -> Awaitable[builtins.int | None]: ...
    # Re-load the snapshot from the configured store and broadcast it to
    # local subscribers. Raises ``RuntimeError`` if no store is wired.
    def refresh(self) -> Awaitable[None]: ...
    # Block until the snapshot reports ``is_finished``. Polls via
    # ``refresh`` every ``poll_interval_secs`` seconds. Returns ``None`` —
    # read the terminal exit_code via the ``exit_code`` getter once the
    # future resolves. Mirrors ``SbatchJobHandle.wait_terminal``.
    def wait_terminal(self, poll_interval_secs: builtins.float) -> Awaitable[None]: ...

@final
class TssrunManager:
    """Orchestrates spawn / attach / query_state for a ``TssrunCmd``."""
    def __init__(
        self,
        cmd: TssrunCmd,
        store: JobStateStore | None = ...,
        log_sink: LogSink | None = ...,
    ) -> None: ...
    def spawn(self) -> Awaitable[TssrunJobHandle]: ...
    # Attach by canonical hyphenated UUID v7 string. Resolves directly
    # through the configured store with an O(1) primary-key lookup.
    def attach_uuid(self, uuid: builtins.str) -> Awaitable[TssrunJobHandle]: ...
    def attach_pid(self, pid: builtins.int) -> Awaitable[TssrunJobHandle]: ...
    def attach_jobid(self, jobid: builtins.int) -> Awaitable[TssrunJobHandle]: ...
    def attach_file(
        self, path: builtins.str | os.PathLike[builtins.str]
    ) -> Awaitable[TssrunJobHandle]: ...
