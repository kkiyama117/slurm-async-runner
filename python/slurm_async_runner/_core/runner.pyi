# Hand-written stubs for slurm_async_runner._core.runner.
#
# pyo3-stub-gen does not derive stubs for async pyfunctions returned via
# pyo3_async_runtimes::tokio::future_into_py, so this file is maintained by
# hand. Keep it in sync with src/py_export/runner.rs.
# ruff: noqa: E501, F401, F403, F405

import builtins
from collections.abc import Awaitable

from gaussian_job_shared._core.entities.slurm.status import JobStatus

__all__ = [
    "query_job_states_batch",
]

def query_job_states_batch(
    jobids: builtins.list[builtins.int],
) -> Awaitable[builtins.dict[builtins.int, JobStatus]]:
    """Bulk-query SLURM for the ``(state, reason)`` of every input jobid.

    Issues at most one ``squeue`` and one ``sacct`` subprocess. Empty input
    short-circuits to an empty dict. Every input id appears in the returned
    dict — ids absent from both backends map to a default ``JobStatus``
    (state=``Unknown``, reason=``None``).
    """
