"""Packaging / public-API contract tests.

Regression guards for the 2026-06-12 review findings:
- ``__all__`` advertised names (``tssrun``, ``sbatch``, …) that were not
  actually bound in the ``slurm_async_runner`` namespace, so
  ``from slurm_async_runner import tssrun`` raised ImportError.
- No ``py.typed`` marker (PEP 561), so downstream mypy/pyright silently
  treated the whole package as untyped and ignored every stub.
- Handwritten stub drift: ``SbatchCmd.build_argv`` exists at runtime but
  was missing from ``sbatch.pyi``.
"""

from __future__ import annotations

from importlib.resources import files

import slurm_async_runner


def test_every_name_in_dunder_all_is_importable() -> None:
    # Arrange / Act / Assert: each advertised name must resolve.
    missing = [
        name
        for name in slurm_async_runner.__all__
        if not hasattr(slurm_async_runner, name)
    ]
    assert not missing, f"__all__ advertises names that are not importable: {missing}"


def test_package_reexports_every_core_name() -> None:
    # Drift guard in the other direction: when a new submodule is added
    # to the Rust `_core.__all__`, the explicit re-export list in
    # __init__.py must be extended too.
    from slurm_async_runner import _slurm_async_runner_core as core

    not_reexported = [
        name for name in core.__all__ if name not in slurm_async_runner.__all__
    ]
    assert not not_reexported, (
        f"_core.__all__ names missing from the package top level: {not_reexported} "
        "— extend the re-export list in slurm_async_runner/__init__.py"
    )


def test_submodules_are_importable_from_package_top_level() -> None:
    # The names tools rely on most (`from slurm_async_runner import X`).
    from slurm_async_runner import entities, manager, runner, sbatch, tssrun  # noqa: F401


def test_py_typed_marker_is_shipped() -> None:
    marker = files("slurm_async_runner").joinpath("py.typed")
    assert marker.is_file(), (
        "PEP 561 py.typed marker is missing — stubs are invisible downstream"
    )


def test_sbatch_stub_declares_build_argv() -> None:
    # Drift guard for the handwritten stub: build_argv exists at runtime
    # (and is documented in the SbatchCmd docstring) so the stub must
    # declare it for type checkers.
    from slurm_async_runner._slurm_async_runner_core.sbatch import SbatchCmd

    assert hasattr(SbatchCmd, "build_argv")
    stub = (
        files("slurm_async_runner")
        .joinpath("_slurm_async_runner_core/sbatch.pyi")
        .read_text()
    )
    assert "def build_argv" in stub, "SbatchCmd.build_argv is missing from sbatch.pyi"
