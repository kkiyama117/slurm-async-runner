"""Smoke tests for the PyO3-exported Rust extension."""

from slurm_async_runner2._core import sum_as_string


def test_sum_as_string():
    assert sum_as_string(1, 1) == "2"
