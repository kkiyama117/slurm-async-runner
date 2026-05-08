from slurm_async_runner import _core

if hasattr(_core, "__doc__"):
    __doc__ = _core.__doc__
if hasattr(_core, "__all__"):
    __all__ = _core.__all__
