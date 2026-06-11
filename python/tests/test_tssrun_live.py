"""Pytest wrapper around scripts/test_tssrun_live.py.

This is opt-in: the test is *skipped* unless ``RUN_LIVE_TSSRUN=1`` is set in
the environment AND the ``tssrun`` binary (or the override pointed to by
``TSSRUN_LIVE_BIN``) is on PATH. That keeps the live test out of regular
CI / dev runs while letting kudpc users run

    RUN_LIVE_TSSRUN=1 uv run pytest python/tests/test_tssrun_live.py -v -s

to drive the same smoke test through the standard pytest harness.

The actual logic lives in ``scripts/test_tssrun_live.py``; this file only
delegates so we have one source of truth for the live workflow.
"""

from __future__ import annotations

import importlib.util
import os
import shutil
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_LIVE_SCRIPT = _REPO_ROOT / "scripts" / "test_tssrun_live.py"

_BIN = os.environ.get("TSSRUN_LIVE_BIN", "tssrun")
_OPT_IN = os.environ.get("RUN_LIVE_TSSRUN") == "1"

pytestmark = [
    pytest.mark.skipif(
        not _OPT_IN,
        reason="set RUN_LIVE_TSSRUN=1 to run the live tssrun smoke test",
    ),
    pytest.mark.skipif(
        shutil.which(_BIN) is None,
        reason=f"'{_BIN}' is not on PATH (override via TSSRUN_LIVE_BIN)",
    ),
]


def _load_live_module():
    spec = importlib.util.spec_from_file_location("tssrun_live_runner", _LIVE_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {_LIVE_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    # Register before exec (standard import-system order, so the module can
    # self-reference), but unregister on failure so a broken half-executed
    # module is never reused by a later call.
    sys.modules["tssrun_live_runner"] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop("tssrun_live_runner", None)
        raise
    return module


def test_live_tssrun_smoke() -> None:
    """End-to-end against a real SLURM allocation.

    Delegates to ``scripts/test_tssrun_live.py::main`` and asserts exit
    status ``0``. Use ``pytest -s`` to stream the runner's stdout/stderr.
    """
    module = _load_live_module()
    rc = module.main()
    assert rc == 0, f"live runner returned non-zero exit code {rc}"
