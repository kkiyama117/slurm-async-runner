from typing import Protocol, runtime_checkable

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
#
# Phase 3 P5: the per-backend call shape now matches the Protocol — both
# `SbatchJobHandle` and `TssrunJobHandle` expose `uuid` / `jobid` as
# sync `@property` getters and `is_running` / `is_finished` /
# `exit_code` as sync methods. The async-shape escape hatches that
# briefly existed during P5 (`uuid_async` etc.) have been removed.
# ────────────────────────────────────────────────────────────────────


@runtime_checkable
class JobHandleCommon(Protocol):
    """Duck-typed common interface for ``TssrunJobHandle`` and ``SbatchJobHandle``.

    Mirrors the Rust ``crate::handle::JobHandleCommon`` trait. Use this
    for ``isinstance(h, JobHandleCommon)`` checks when accepting either
    backend.

    The 5 sync members read lock-free off each backend's local snapshot
    cache and are safe to call concurrently with an in-flight
    ``refresh`` / ``wait_terminal``. ``refresh`` and ``wait_terminal``
    are genuinely async (they re-query the persistent store and/or
    SLURM).

    ``runtime_checkable`` only inspects member *names*, not signatures —
    the structural type check still passes for both pyo3 backends, and
    after Phase 3 P5 the signatures are also accurate at static-type
    check time.
    """

    @property
    def uuid(self) -> str: ...
    @property
    def jobid(self) -> int | None: ...
    def is_running(self) -> bool: ...
    def is_finished(self) -> bool: ...
    def exit_code(self) -> int | None: ...
    async def refresh(self) -> object: ...
    async def wait_terminal(self, poll_interval_secs: float) -> object: ...


__all__.append("JobHandleCommon")
