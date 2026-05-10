"""Python-side tests for the tssrun submodule.

Uses bash as a stand-in for tssrun to emit the same ``salloc:`` banner.
"""

import asyncio
import tempfile
from pathlib import Path

from slurm_async_runner._slurm_async_runner_core.tssrun import (
    ResourceSpec,
    TssrunCmd,
    TssrunManager,
    file_log_sink,
    file_system_state_store,
    in_memory_state_store,
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
        rsc=ResourceSpec.from_str("p=4:m=2G"),
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


def test_h1_snapshot_getters_do_not_block_on_inflight_wait() -> None:
    """Regression test for H1 — wait() must not serialize snapshot getters.

    Pre-fix, ``PyTssrunJobHandle`` wrapped a single ``Mutex<JobHandle>``
    that ``wait()`` held for the full job duration, which made parallel
    polls of ``is_running``/``pid``/``jobid`` block until the child
    exited. After splitting the snapshot reader off the waiter, getters
    must return promptly even while wait() is still in flight.
    """

    async def run() -> None:
        with tempfile.TemporaryDirectory() as td:
            script = Path(td) / "slow.sh"
            script.write_text(
                'echo "salloc: Granted job allocation 777"\n'
                'echo "salloc: Nodes node-h1 are ready for job"\n'
                "sleep 0.4\n"
            )
            cmd = TssrunCmd(program=str(script), tssrun_bin="bash")
            manager = TssrunManager(cmd)
            h = await manager.spawn()

            # pyo3-async-runtimes returns awaitables (asyncio Futures), not
            # coroutines. Wrap with `ensure_future` so we can run wait()
            # concurrently with snapshot polls.
            wait_fut = asyncio.ensure_future(h.wait())
            try:
                # Give the salloc: lines time to be parsed but stay well
                # below the 0.4s sleep so wait() is still in flight.
                await asyncio.sleep(0.1)
                assert not wait_fut.done(), "wait() should still be in flight"

                # Each of these must return within a fraction of a second.
                # If the old single-mutex design comes back, they would
                # block until wait_fut completes.
                pid = await asyncio.wait_for(asyncio.ensure_future(h.pid), timeout=0.2)
                running = await asyncio.wait_for(
                    asyncio.ensure_future(h.is_running()), timeout=0.2
                )
                jobid = await asyncio.wait_for(
                    asyncio.ensure_future(h.jobid), timeout=0.2
                )

                assert pid > 0
                assert running is True
                assert jobid == 777
            finally:
                code = await wait_fut
            assert code == 0
            assert (await h.jobid) == 777
            assert (await h.is_running()) is False

    asyncio.run(run())


def test_manager_attach_file_round_trip() -> None:
    async def run() -> None:
        with tempfile.TemporaryDirectory() as td:
            manager = TssrunManager(
                _bash_cmd(Path(td)), store=file_system_state_store(td)
            )
            h = await manager.spawn()
            pid = await h.pid
            uuid = await h.uuid
            await h.wait()
            # State directory layout: {state_dir}/{uuid}.json — pid is no
            # longer the filename key after the UUID v7 migration.
            path = Path(td) / f"{uuid}.json"
            assert path.exists()
            attached = await manager.attach_file(str(path))
            assert (await attached.pid) == pid
            assert (await attached.uuid) == uuid
            assert (await attached.jobid) == 555

    asyncio.run(run())


def test_manager_attach_uuid_round_trip() -> None:
    async def run() -> None:
        with tempfile.TemporaryDirectory() as td:
            manager = TssrunManager(
                _bash_cmd(Path(td)), store=file_system_state_store(td)
            )
            h = await manager.spawn()
            pid = await h.pid
            uuid = await h.uuid
            await h.wait()
            # uuid is the canonical hyphenated string, e.g.
            # "0190cc1c-7a48-7c0e-a0a0-1234567890ab" — 36 chars.
            assert len(uuid) == 36 and uuid.count("-") == 4
            attached = await manager.attach_uuid(uuid)
            assert (await attached.uuid) == uuid
            assert (await attached.pid) == pid
            assert (await attached.jobid) == 555

    asyncio.run(run())


def test_manager_attach_uuid_rejects_invalid_string() -> None:
    async def run() -> None:
        with tempfile.TemporaryDirectory() as td:
            manager = TssrunManager(
                _bash_cmd(Path(td)), store=file_system_state_store(td)
            )
            try:
                await manager.attach_uuid("not-a-uuid")
            except RuntimeError as e:
                assert "invalid uuid" in str(e).lower()
            else:
                raise AssertionError("attach_uuid must reject malformed input")

    asyncio.run(run())


def test_manager_default_store_is_in_memory_and_supports_attach_uuid() -> None:
    """The default ``TssrunManager(cmd)`` uses an in-memory store, so the
    spawn → attach_uuid round-trip works without any writable directory.

    This is the headline use case for the read-only-filesystem scenarios
    that motivated splitting the store out of the manager.
    """

    async def run() -> None:
        with tempfile.TemporaryDirectory() as td:
            # Note: no store= argument — we are exercising the default.
            manager = TssrunManager(_bash_cmd(Path(td)))
            h = await manager.spawn()
            uuid = await h.uuid
            await h.wait()
            attached = await manager.attach_uuid(uuid)
            assert (await attached.uuid) == uuid
            assert (await attached.jobid) == 555

    asyncio.run(run())


def test_manager_explicit_in_memory_store_is_shared_across_managers() -> None:
    """Two managers wired to the *same* in-memory store can attach to
    each other's spawned handles in-process."""

    async def run() -> None:
        with tempfile.TemporaryDirectory() as td:
            store = in_memory_state_store()
            spawner = TssrunManager(_bash_cmd(Path(td)), store=store)
            attacher = TssrunManager(_bash_cmd(Path(td)), store=store)

            h = await spawner.spawn()
            uuid = await h.uuid
            await h.wait()

            attached = await attacher.attach_uuid(uuid)
            assert (await attached.uuid) == uuid
            assert (await attached.jobid) == 555

    asyncio.run(run())


def test_manager_attach_jobid_returns_friendly_error_when_dir_missing() -> None:
    """Regression: pre-store, ``attach_jobid`` surfaced ``ENOENT`` when
    the file system state directory had not been created. With the
    ``FileSystemStateStore``, the missing directory is treated as "no
    entries" and the manager returns the same friendly
    ``no persisted handle matched jobid …`` message it would for an
    empty directory."""

    async def run() -> None:
        with tempfile.TemporaryDirectory() as td:
            missing = Path(td) / "never-created"
            assert not missing.exists()
            manager = TssrunManager(
                _bash_cmd(Path(td)), store=file_system_state_store(missing)
            )
            try:
                await manager.attach_jobid(999)
            except RuntimeError as e:
                assert "no persisted handle matched jobid" in str(e)
            else:
                raise AssertionError(
                    "attach_jobid must fail with a friendly error when the "
                    "state dir does not exist"
                )

    asyncio.run(run())


def test_handle_refresh_picks_up_cross_manager_mutations() -> None:
    """``refresh()`` must re-load the snapshot from the shared store so a
    handle obtained from one manager observes mutations recorded by
    another manager spawning under the same uuid space."""

    async def run() -> None:
        with tempfile.TemporaryDirectory() as td:
            store = in_memory_state_store()
            spawner = TssrunManager(_bash_cmd(Path(td)), store=store)
            attacher = TssrunManager(_bash_cmd(Path(td)), store=store)

            h_spawn = await spawner.spawn()
            uuid = await h_spawn.uuid
            # Attach BEFORE waiting — the attached handle's local snapshot
            # captures the pre-exit state (no ``finished`` yet).
            h_attach = await attacher.attach_uuid(uuid)
            assert await h_attach.is_running() is True

            # Drive the spawner to completion; this updates the store.
            await h_spawn.wait()

            # The attached handle's local snapshot is still stale until we
            # explicitly refresh from the store.
            await h_attach.refresh()
            assert await h_attach.is_running() is False
            assert (await h_attach.exit_code()) == 0

    asyncio.run(run())
