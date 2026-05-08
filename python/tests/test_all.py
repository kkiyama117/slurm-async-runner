import pytest
import slurm_async_runner2


def test_sum_as_string():
    assert slurm_async_runner2.sum_as_string(1, 1) == "2"
