"""Python-side tests for the tssrun submodule.

Uses bash as a stand-in for tssrun to emit the same ``salloc:`` banner.
"""

import asyncio
import tempfile
from pathlib import Path

from slurm_async_runner._core.tssrun import (
    Resource,
    TssrunCmd,
    TssrunManager,
    file_log_sink,
    null_log_sink,
    std_log_sink,
)

BANNER = (
    'echo "salloc: Granted job allocation 555"\n'
    'echo "salloc: Nodes node-py are ready for job"\n'
    "sleep 0.05\n"
    "echo done\n"
)


def _bash_cmd(workdir: Path) -> TssrunCmd:
    """Build a TssrunCmd that runs the BANNER script via bash.

    The argv built by TssrunCmd is `[tssrun_bin, ...flags, program, ...args]`,
    so passing `program="/bin/true"` and `args=["-c", BANNER]` would produce
    `[bash, /bin/true, -c, BANNER]` — bash sources /bin/true as a script.
    Workaround: write the script to a tempfile and pass it as `program`.
    """
    script = workdir / "mock.sh"
    script.write_text(BANNER)
    return TssrunCmd(program=str(script), tssrun_bin="bash")


def test_resource_render_via_argv() -> None:
    cmd = TssrunCmd(
        program="/bin/true",
        rsc=Resource(processes=4, memory="2G"),
    )
    argv = cmd.build_argv()
    assert "--rsc" in argv
    assert "p=4:m=2G" in argv


def test_null_log_sink_does_not_raise() -> None:
    null_log_sink()


def test_std_log_sink_does_not_raise() -> None:
    std_log_sink()


def test_manager_spawn_then_wait_then_jobid() -> None:
    async def run() -> None:
        with tempfile.TemporaryDirectory() as td:
            manager = TssrunManager(_bash_cmd(Path(td)))
            h = await manager.spawn()
            assert (await h.pid) > 0
            code = await h.wait()
            assert code == 0
            assert (await h.jobid) == 555
            assert (await h.node) == "node-py"

    asyncio.run(run())


def test_manager_with_file_log_sink_persists_logs() -> None:
    async def run() -> None:
        with tempfile.TemporaryDirectory() as td:
            o = Path(td) / "o.log"
            e = Path(td) / "e.log"
            sink = await file_log_sink(str(o), str(e))
            manager = TssrunManager(_bash_cmd(Path(td)), log_sink=sink)
            h = await manager.spawn()
            await h.wait()
            assert "done" in o.read_text()

    asyncio.run(run())


def test_manager_attach_file_round_trip() -> None:
    async def run() -> None:
        with tempfile.TemporaryDirectory() as td:
            manager = TssrunManager(_bash_cmd(Path(td)), state_dir=td)
            h = await manager.spawn()
            pid = await h.pid
            await h.wait()
            path = Path(td) / f"{pid}.json"
            assert path.exists()
            attached = await manager.attach_file(str(path))
            assert (await attached.pid) == pid
            assert (await attached.jobid) == 555

    asyncio.run(run())
