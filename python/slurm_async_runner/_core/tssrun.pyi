# Hand-written stubs for slurm_async_runner._core.tssrun.
# pyo3-stub-gen does not derive stubs for pyclasses inside #[pymodule]
# sub-modules wired via #[pymodule_export], so this file is maintained
# by hand. Keep it in sync with src/py_export/tssrun.rs.
# ruff: noqa: E501, F401, F403, F405

import builtins
import os
from collections.abc import Awaitable
from typing import final

__all__ = [
    "Resource",
    "TssrunCmd",
    "LogSink",
    "TssrunJobHandle",
    "TssrunManager",
    "null_log_sink",
    "std_log_sink",
    "file_log_sink",
]

@final
class Resource:
    """Resource spec for ``tssrun --rsc p=:t=:c=:m=:g=``."""

    processes: builtins.int | None
    threads: builtins.int | None
    cores: builtins.int | None
    memory: builtins.str | None
    gpus: builtins.int | None

    def __init__(
        self,
        processes: builtins.int | None = ...,
        threads: builtins.int | None = ...,
        cores: builtins.int | None = ...,
        memory: builtins.str | None = ...,
        gpus: builtins.int | None = ...,
    ) -> None: ...

@final
class TssrunCmd:
    """Spec for one ``tssrun`` invocation. Pure data + ``build_argv``."""
    def __init__(
        self,
        program: builtins.str | os.PathLike[builtins.str],
        args: builtins.list[builtins.str] = ...,
        queue: builtins.str | None = ...,
        time_limit: builtins.str | None = ...,
        rsc: Resource | None = ...,
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
class TssrunJobHandle:
    """In-process or attached handle to a ``tssrun`` child."""
    @property
    def pid(self) -> Awaitable[builtins.int]: ...
    @property
    def jobid(self) -> Awaitable[builtins.int | None]: ...
    @property
    def node(self) -> Awaitable[builtins.str | None]: ...
    @property
    def sent_env(self) -> Awaitable[builtins.dict[builtins.str, builtins.str]]: ...
    def live_env(
        self,
    ) -> Awaitable[builtins.dict[builtins.str, builtins.str] | None]: ...
    def is_running(self) -> Awaitable[builtins.bool]: ...
    def exit_code(self) -> Awaitable[builtins.int | None]: ...
    def wait(self) -> Awaitable[builtins.int]: ...

@final
class TssrunManager:
    """Orchestrates spawn / attach / query_state for a ``TssrunCmd``."""
    def __init__(
        self,
        cmd: TssrunCmd,
        state_dir: builtins.str | os.PathLike[builtins.str] | None = ...,
        log_sink: LogSink | None = ...,
    ) -> None: ...
    def spawn(self) -> Awaitable[TssrunJobHandle]: ...
    def attach_pid(self, pid: builtins.int) -> Awaitable[TssrunJobHandle]: ...
    def attach_jobid(self, jobid: builtins.int) -> Awaitable[TssrunJobHandle]: ...
    def attach_file(
        self, path: builtins.str | os.PathLike[builtins.str]
    ) -> Awaitable[TssrunJobHandle]: ...
