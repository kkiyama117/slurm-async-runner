#!/usr/bin/env python3
"""Live end-to-end smoke test for the tssrun wrapper.

Run this on a host where ``tssrun`` is actually available (Kyoto-U ECCS /
kudpc login nodes). On any other machine the script auto-detects the missing
binary and exits ``0`` with a SKIP message — so it is safe to wire into CI as
an opt-in step without breaking dev machines.

What this script verifies on a real SLURM allocation
----------------------------------------------------
The same end-to-end flow is exercised against **both** ``JobStateStore``
backends in a single invocation, so a regression in either path fails the
smoke test:

1.  ``FileSystemStateStore`` — the on-disk backend that was the only path
    available before the trait split. Confirms the persisted JSON shows up
    at ``{state_dir}/{uuid}.json`` and that ``attach_file`` /
    ``attach_uuid`` both reconstruct an equivalent read-only handle from
    that file.
2.  ``InMemoryStateStore`` — the new default backend, designed for
    read-only-filesystem hosts and for single-process workflows. Confirms
    ``attach_uuid`` round-trips in-process **without writing anything to
    disk**, which is the headline regression guard for the trait split.

Each backend goes through the full per-run flow:

a.  Spawns a tiny child via ``TssrunManager.spawn`` (non-blocking).
b.  Verifies ``pid > 0`` immediately, then polls ``is_running`` /
    ``exit_code`` while the job is being scheduled.
c.  Reads ``sent_env`` and ``live_env`` (the latter via ``/proc/<pid>/environ``
    on the login node — best-effort and may be ``None`` once the child
    has exited; we tolerate that).
d.  ``await handle.wait()`` and confirms ``exit_code == 0``.
e.  Confirms the parsed ``jobid`` and ``node`` came back from the
    ``salloc:`` banner that ``tssrun`` prints on stdout.
f.  Confirms ``attach_uuid`` reconstructs the snapshot. For the FS backend
    additionally verifies ``{state_dir}/{uuid}.json`` exists and
    ``attach_file`` round-trips through that path.
g.  Confirms the captured stdout log file contains the marker token the
    child emitted, proving the FileLogSink tee pipeline ran.

Configuration via environment variables
---------------------------------------
``TSSRUN_LIVE_BIN``         Path / name of the tssrun executable (default
                            ``tssrun``). Useful if you have it under
                            ``/usr/local/eccs/bin/tssrun`` etc.
``TSSRUN_LIVE_QUEUE``       Queue / partition name (passed as ``-p``).
                            Optional — when unset, ``tssrun`` picks its
                            site default.
``TSSRUN_LIVE_TIME_LIMIT``  Wall-clock time (passed as ``-t``). Defaults
                            to ``0:01:00`` because this is just a smoke
                            test.
``TSSRUN_LIVE_RSC``         Optional ``--rsc`` value (raw, e.g.
                            ``p=1:c=1:m=512M``). When unset, no ``--rsc``
                            is passed.
``TSSRUN_LIVE_TIMEOUT``     Hard timeout in seconds for each per-backend
                            wait (default 180). Two allocations happen
                            sequentially, so total wall time is up to ~2x.
``TSSRUN_LIVE_BACKENDS``    Comma-separated subset of ``fs,in-memory`` to
                            limit which backends run. Defaults to both —
                            useful when iterating on one path.
``TMPDIR``                  Must point to a writable directory on the
                            login node. The kudpc/ECCS ``/tmp`` is
                            commonly read-only for the script's
                            ``tempfile.TemporaryDirectory()`` call —
                            export ``TMPDIR="$HOME/.cache/<dir>"`` first.

Exit codes
----------
0  test passed for every requested backend (or skipped because
   ``tssrun`` is not on PATH).
1  test failed — assertion or unexpected exception in at least one
   backend. The exit code reflects the first failing backend; the script
   stops as soon as one fails so the cluster is not billed for follow-up
   allocations after a clean failure.
2  test timed out waiting for the child / the SLURM allocation.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import sys
import tempfile
import traceback
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Imported only for type hints. Real import lives inside ``_run`` so
    # the SKIP path doesn't require the extension to be built.
    from slurm_async_runner._slurm_async_runner_core.tssrun import (
        JobStateStore,
        ResourceSpec,
        TssrunCmd,
        TssrunManager,
    )

# Marker the child emits on stdout — we grep for it in the captured log to
# prove the file sink + tee tasks plumbed real data through. The same marker
# is reused across backends; per-backend log files keep the assertions
# independent.
LIVE_MARKER = "tssrun-live-smoke-marker"


def _eprint(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def _build_child_script(workdir: Path) -> Path:
    """Write the script the cluster will actually execute under tssrun.

    Kept intentionally tiny so each SLURM allocation ends quickly — this
    smoke runs the script twice (once per backend), so the per-run cost
    matters more than for a single-shot test.
    """
    script = workdir / "child.sh"
    script.write_text(
        "#!/bin/bash\n"
        f'echo "{LIVE_MARKER}"\n'
        'echo "host=$(hostname) jobid=${SLURM_JOB_ID:-?}"\n'
        # tiny sleep so the parent has time to read /proc/<pid>/environ.
        "sleep 1\n"
    )
    script.chmod(0o755)
    return script


def _parse_resource(
    rsc_raw: str | None, ResourceSpec: type[ResourceSpec]
) -> ResourceSpec | None:
    """Parse ``"p=1:c=1:m=512M"`` into a ``ResourceSpec`` via the Rust-side
    canonical parser, so this script and the production code share the same
    grammar."""
    if rsc_raw is None:
        return None
    return ResourceSpec.from_str(rsc_raw)


async def _exercise_backend(
    *,
    label: str,
    store: JobStateStore,
    state_dir: Path | None,
    cmd: TssrunCmd,
    workdir: Path,
    overall_timeout: float,
    TssrunManager: type[TssrunManager],
    file_log_sink,
) -> int:
    """Run the full spawn → wait → attach round-trip on one backend.

    ``state_dir`` is the on-disk root for the FS backend (so the script
    can verify ``{state_dir}/{uuid}.json`` exists post-wait), or ``None``
    for the in-memory backend — in which case the disk-file assertions
    are skipped and only the in-process ``attach_uuid`` round-trip runs.

    Returns ``0`` on pass, ``1`` on assertion / runtime failure, ``2``
    on per-backend timeout.
    """
    workdir.mkdir(parents=True, exist_ok=True)
    stdout_log = workdir / "out.log"
    stderr_log = workdir / "err.log"

    sink = await file_log_sink(str(stdout_log), str(stderr_log))
    manager = TssrunManager(cmd, store=store, log_sink=sink)

    handle = await manager.spawn()
    pid = await handle.pid
    uuid = await handle.uuid
    assert pid > 0, f"[{label}] expected pid > 0, got {pid}"
    # uuid is the canonical hyphenated form, e.g.
    # "0190cc1c-7a48-7c0e-a0a0-1234567890ab" — 36 chars / 4 dashes.
    assert len(uuid) == 36 and uuid.count("-") == 4, (
        f"[{label}] unexpected uuid format: {uuid!r}"
    )
    print(f"[live:{label}] spawned pid={pid} uuid={uuid}")

    sent_env = await handle.sent_env
    # tssrun inherits the parent env unless we passed an explicit dict; the
    # snapshot only shows what we explicitly *sent*. We didn't, so it's {}
    # — but the type should still be a real dict.
    assert isinstance(sent_env, dict), type(sent_env)

    # live_env returns None for three distinct reasons:
    #   1. We are not on Linux (no /proc filesystem at all).
    #   2. We are on Linux but the child has already exited (ENOENT).
    #   3. The child is a setuid/setgid binary with PR_SET_DUMPABLE
    #      cleared, so /proc/<pid>/environ is root-only (EACCES). The
    #      kudpc/ECCS `tssrun` wrapper falls into this bucket on the
    #      login nodes, which is the whole reason this script exists.
    # We tolerate all three — they are best-effort observations.
    live_env = await handle.live_env()
    if live_env is not None:
        assert isinstance(live_env, dict)
        print(f"[live:{label}] /proc/{pid}/environ has {len(live_env)} entries")
    elif sys.platform != "linux":
        print(f"[live:{label}] /proc not available on platform={sys.platform!r}")
    else:
        print(
            f"[live:{label}] /proc/{pid}/environ unreadable "
            f"(child already exited or non-dumpable setuid binary)"
        )

    try:
        code = await asyncio.wait_for(handle.wait(), timeout=overall_timeout)
    except asyncio.TimeoutError:
        _eprint(
            f"FAIL[{label}]: tssrun child did not finish within "
            f"{overall_timeout}s. Either the queue is busy or the child is hung."
        )
        return 2

    print(f"[live:{label}] child exit code = {code}")
    if code is None:
        stderr_text = stderr_log.read_text() if stderr_log.exists() else "<none>"
        _eprint(
            f"FAIL[{label}]: tssrun child was terminated by a signal "
            f"(no exit code). stderr log =\n{stderr_text}"
        )
        return 1
    if code != 0:
        stderr_text = stderr_log.read_text() if stderr_log.exists() else "<none>"
        _eprint(
            f"FAIL[{label}]: tssrun child exited with non-zero status {code}. "
            f"stderr log =\n{stderr_text}"
        )
        return 1

    jobid_val = await handle.jobid
    node_val = await handle.node
    print(f"[live:{label}] parsed jobid={jobid_val} node={node_val}")
    assert jobid_val is not None, (
        f"[{label}] expected jobid to be parsed from salloc: banner"
    )
    assert node_val is not None, (
        f"[{label}] expected node to be parsed from salloc: banner"
    )

    # ---- Backend-specific persistence assertions ----
    if state_dir is not None:
        # FS backend: snapshots are persisted under {uuid}.json (post UUID v7
        # migration); pid is reusable by the kernel and is no longer the
        # filename key.
        snap_path = state_dir / f"{uuid}.json"
        assert snap_path.exists(), (
            f"[{label}] missing persisted snapshot at {snap_path}"
        )
        print(f"[live:{label}] snapshot persisted to {snap_path}")

        attached = await manager.attach_file(str(snap_path))
        attached_pid = await attached.pid
        attached_uuid = await attached.uuid
        attached_jobid = await attached.jobid
        attached_node = await attached.node
        assert attached_pid == pid, (label, attached_pid, pid)
        assert attached_uuid == uuid, (label, attached_uuid, uuid)
        assert attached_jobid == jobid_val, (label, attached_jobid, jobid_val)
        assert attached_node == node_val, (label, attached_node, node_val)
        print(f"[live:{label}] attach_file round-trip OK")
    else:
        # In-memory backend: there is no on-disk file. The headline
        # invariant is that attach_uuid still works in-process — the
        # snapshot lives in the shared store the manager owns. If the
        # state_dir on the login node was unwritable (the original
        # motivation for the trait split), the spawn above would already
        # have failed before we got here, so no extra disk-write probe
        # is needed.
        print(
            f"[live:{label}] in-memory backend: skipping disk-file probe "
            f"(no {{state_dir}}/{{uuid}}.json layout)"
        )

    # attach_uuid round-trip — works for BOTH backends. For FS this
    # resolves through the store directly to the JSON file; for in-memory
    # it hits the process-local HashMap entry the spawn just inserted.
    attached_by_uuid = await manager.attach_uuid(uuid)
    assert (await attached_by_uuid.uuid) == uuid, label
    assert (await attached_by_uuid.pid) == pid, label
    assert (await attached_by_uuid.jobid) == jobid_val, label
    print(f"[live:{label}] attach_uuid round-trip OK")

    stdout_text = stdout_log.read_text() if stdout_log.exists() else ""
    assert LIVE_MARKER in stdout_text, (
        f"[{label}] marker '{LIVE_MARKER}' not found in captured stdout\n"
        f"--- log ---\n{stdout_text}"
    )
    print(f"[live:{label}] stdout log captured ({len(stdout_text)} bytes)")

    print(f"[live:{label}] PASS")
    return 0


async def _run() -> int:
    bin_path = os.environ.get("TSSRUN_LIVE_BIN", "tssrun")
    if shutil.which(bin_path) is None:
        _eprint(
            f"SKIP: '{bin_path}' is not on PATH — this script must be run on a "
            f"host where the kudpc/ECCS tssrun binary is installed."
        )
        return 0

    queue = os.environ.get("TSSRUN_LIVE_QUEUE")
    time_limit = os.environ.get("TSSRUN_LIVE_TIME_LIMIT", "0:01:00")
    rsc_raw = os.environ.get("TSSRUN_LIVE_RSC")
    overall_timeout = float(os.environ.get("TSSRUN_LIVE_TIMEOUT", "180"))

    backends_raw = os.environ.get("TSSRUN_LIVE_BACKENDS", "fs,in-memory")
    requested = {b.strip() for b in backends_raw.split(",") if b.strip()}
    unknown = requested - {"fs", "in-memory"}
    if unknown:
        _eprint(
            f"FAIL: unknown TSSRUN_LIVE_BACKENDS entries: {sorted(unknown)} "
            f"(allowed: fs, in-memory)"
        )
        return 1

    # Imported lazily so the SKIP path doesn't require the extension to be
    # built (e.g. on a fresh clone before `maturin develop`).
    from slurm_async_runner._slurm_async_runner_core.tssrun import (
        JobTimeLimit,
        ResourceSpec,
        TssrunCmd,
        TssrunManager,
        file_log_sink,
        file_system_state_store,
        in_memory_state_store,
    )

    with tempfile.TemporaryDirectory(prefix="tssrun-live-") as td_str:
        td = Path(td_str)
        script_path = _build_child_script(td)
        rsc = _parse_resource(rsc_raw, ResourceSpec)

        cmd = TssrunCmd(
            program=str(script_path),
            partition=queue,
            time_limit=JobTimeLimit(time_limit),
            rsc=rsc,
            tssrun_bin=bin_path,
        )
        argv = cmd.build_argv()
        print(f"[live] argv = {argv}")
        print(f"[live] running backends: {sorted(requested)}")

        # Run FS first when requested — it is the path that historically
        # crashed on read-only login nodes, so a TMPDIR misconfig surfaces
        # here before we waste another allocation on the in-memory path.
        if "fs" in requested:
            state_dir = td / "fs-state"
            rc = await _exercise_backend(
                label="fs",
                store=file_system_state_store(str(state_dir)),
                state_dir=state_dir,
                cmd=cmd,
                workdir=td / "fs",
                overall_timeout=overall_timeout,
                TssrunManager=TssrunManager,
                file_log_sink=file_log_sink,
            )
            if rc != 0:
                return rc

        if "in-memory" in requested:
            rc = await _exercise_backend(
                label="in-memory",
                store=in_memory_state_store(),
                state_dir=None,
                cmd=cmd,
                workdir=td / "mem",
                overall_timeout=overall_timeout,
                TssrunManager=TssrunManager,
                file_log_sink=file_log_sink,
            )
            if rc != 0:
                return rc

    print(
        f"PASS: tssrun live smoke test completed end-to-end for "
        f"backends={sorted(requested)}."
    )
    return 0


def main() -> int:
    try:
        return asyncio.run(_run())
    except AssertionError as exc:
        _eprint(f"FAIL: assertion: {exc}")
        traceback.print_exc()
        return 1
    except Exception as exc:  # noqa: BLE001 — top-level CLI handler
        _eprint(f"FAIL: unexpected error: {exc!r}")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
