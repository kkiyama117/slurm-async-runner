from typing import Protocol, runtime_checkable
from uuid import UUID

from slurm_async_runner import _slurm_async_runner_core as _core

if hasattr(_core, "__doc__"):
    __doc__ = _core.__doc__
if hasattr(_core, "__all__"):
    __all__ = list(_core.__all__)
else:
    __all__ = []


# ────────────────────────────────────────────────────────────────────
# Phase 3 P4: duck-typed Protocol mirror of the Rust `JobHandleCommon`
# trait. Python lacks Rust's associated types, so the snapshot accessor
# return type is widened to `object`. Use `isinstance(h, JobHandleCommon)`
# (runtime_checkable) to accept either `TssrunJobHandle` or
# `SbatchJobHandle` without importing both concrete pyclass names.
# ────────────────────────────────────────────────────────────────────


@runtime_checkable
class JobHandleCommon(Protocol):
    """Duck-typed common interface for ``TssrunJobHandle`` and ``SbatchJobHandle``.

    Mirrors the Rust ``crate::handle::JobHandleCommon`` trait. Use this
    for ``isinstance(h, JobHandleCommon)`` checks when accepting either
    backend.

    ``runtime_checkable`` only inspects method *names*, not signatures,
    so the structural check passes for both pyo3-backed handles despite
    Python's lack of associated types.
    """

    def uuid(self) -> UUID: ...
    def jobid(self) -> int | None: ...
    def is_running(self) -> bool: ...
    def is_finished(self) -> bool: ...
    def exit_code(self) -> int | None: ...
    async def refresh(self) -> object: ...
    async def wait_terminal(self, poll_interval_secs: float) -> object: ...


__all__.append("JobHandleCommon")
