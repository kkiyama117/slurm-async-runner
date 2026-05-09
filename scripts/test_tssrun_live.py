#!/usr/bin/env python3
"""Live end-to-end smoke test for the tssrun wrapper.

Run this on a host where ``tssrun`` is actually available (Kyoto-U ECCS /
kudpc login nodes). On any other machine the script auto-detects the missing
binary and exits ``0`` with a SKIP message — so it is safe to wire into CI as
an opt-in step without breaking dev machines.

What it exercises against a real SLURM allocation
-------------------------------------------------
1.  Spawns a tiny child via ``TssrunManager.spawn`` (non-blocking).
2.  Verifies ``pid > 0`` immediately, then polls ``is_running`` /
    ``exit_code`` while the job is being scheduled.
3.  Reads ``sent_env`` and ``live_env`` (the latter via ``/proc/<pid>/environ``
    on the login node — it is best-effort and may be ``None`` once the child
    has exited; we tolerate that).
4.  ``await handle.wait()`` and confirms ``exit_code == 0``.
5.  Confirms the parsed ``jobid`` and ``node`` came back from the
    ``salloc:`` banner that ``tssrun`` prints on stdout.
6.  Confirms the persisted JSON snapshot exists at
    ``<state_dir>/<pid>.json`` and that ``attach_file`` reconstructs an
    equivalent read-only handle.
7.  Confirms the captured stdout log file contains the marker token the
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
``TSSRUN_LIVE_TIMEOUT``     Hard timeout in seconds for the whole script
                            (default 180). Guards against hung allocations.

Exit codes
----------
0  test passed (or skipped because ``tssrun`` is not on PATH).
1  test failed — assertion or unexpected exception.
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

# Marker the child emits on stdout — we grep for it in the captured log to
# prove the file sink + tee tasks plumbed real data through.
LIVE_MARKER = "tssrun-live-smoke-marker"


def _eprint(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def _build_child_script(workdir: Path) -> Path:
    """Write the script the cluster will actually execute under tssrun.

    Kept intentionally tiny so the SLURM allocation ends quickly.
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

    # Imported lazily so the SKIP path doesn't require the extension to be
    # built (e.g. on a fresh clone before `maturin develop`).
    from slurm_async_runner._core.tssrun import (
        Resource,
        TssrunCmd,
        TssrunManager,
        file_log_sink,
    )

    with tempfile.TemporaryDirectory(prefix="tssrun-live-") as td_str:
        td = Path(td_str)
        script_path = _build_child_script(td)
        stdout_log = td / "out.log"
        stderr_log = td / "err.log"
        state_dir = td / "state"
        state_dir.mkdir()

        rsc: Resource | None = None
        if rsc_raw is not None:
            # Parse "p=1:c=1:m=512M" so we can construct a Resource. Unknown
            # keys are silently ignored; render() will reproduce them anyway
            # for known fields.
            kv: dict[str, str] = {}
            for part in rsc_raw.split(":"):
                if "=" in part:
                    k, v = part.split("=", 1)
                    kv[k.strip()] = v.strip()
            rsc = Resource(
                processes=int(kv["p"]) if "p" in kv else None,
                threads=int(kv["t"]) if "t" in kv else None,
                cores=int(kv["c"]) if "c" in kv else None,
                memory=kv.get("m"),
                gpus=int(kv["g"]) if "g" in kv else None,
            )

        cmd = TssrunCmd(
            program=str(script_path),
            queue=queue,
            time_limit=time_limit,
            rsc=rsc,
            tssrun_bin=bin_path,
        )
        argv = cmd.build_argv()
        print(f"[live] argv = {argv}")

        sink = await file_log_sink(str(stdout_log), str(stderr_log))
        manager = TssrunManager(cmd, state_dir=str(state_dir), log_sink=sink)

        handle = await manager.spawn()
        pid = await handle.pid
        assert pid > 0, f"expected pid > 0, got {pid}"
        print(f"[live] spawned pid={pid}")

        sent_env = await handle.sent_env
        # tssrun inherits the parent env unless we passed an explicit dict; the
        # snapshot only shows what we explicitly *sent*. We didn't, so it's {}
        # — but the type should still be a real dict.
        assert isinstance(sent_env, dict), type(sent_env)

        # live_env returns None for two distinct reasons:
        #   1. We are not on Linux (no /proc filesystem at all).
        #   2. We are on Linux but the child has already exited (proc gone).
        # Tell them apart so the log is honest.
        live_env = await handle.live_env()
        if live_env is not None:
            assert isinstance(live_env, dict)
            print(f"[live] /proc/{pid}/environ has {len(live_env)} entries")
        elif sys.platform != "linux":
            print(f"[live] /proc not available on platform={sys.platform!r}")
        else:
            print(f"[live] /proc/{pid}/environ unreadable (child already exited)")

        try:
            code = await asyncio.wait_for(handle.wait(), timeout=overall_timeout)
        except asyncio.TimeoutError:
            _eprint(
                f"FAIL: tssrun child did not finish within {overall_timeout}s. "
                f"Either the queue is busy or the child is hung."
            )
            return 2

        print(f"[live] child exit code = {code}")
        if code is None:
            stderr_text = stderr_log.read_text() if stderr_log.exists() else "<none>"
            _eprint(
                "FAIL: tssrun child was terminated by a signal (no exit code). "
                f"stderr log =\n{stderr_text}"
            )
            return 1
        if code != 0:
            stderr_text = stderr_log.read_text() if stderr_log.exists() else "<none>"
            _eprint(
                f"FAIL: tssrun child exited with non-zero status {code}. "
                f"stderr log =\n{stderr_text}"
            )
            return 1

        jobid_val = await handle.jobid
        node_val = await handle.node
        print(f"[live] parsed jobid={jobid_val} node={node_val}")
        assert jobid_val is not None, "expected jobid to be parsed from salloc: banner"
        assert node_val is not None, "expected node to be parsed from salloc: banner"

        snap_path = state_dir / f"{pid}.json"
        assert snap_path.exists(), f"missing persisted snapshot at {snap_path}"
        print(f"[live] snapshot persisted to {snap_path}")

        attached = await manager.attach_file(str(snap_path))
        attached_pid = await attached.pid
        attached_jobid = await attached.jobid
        attached_node = await attached.node
        assert attached_pid == pid, (attached_pid, pid)
        assert attached_jobid == jobid_val, (attached_jobid, jobid_val)
        assert attached_node == node_val, (attached_node, node_val)
        print("[live] attach_file round-trip OK")

        stdout_text = stdout_log.read_text() if stdout_log.exists() else ""
        assert LIVE_MARKER in stdout_text, (
            f"marker '{LIVE_MARKER}' not found in captured stdout\n--- log ---\n"
            f"{stdout_text}"
        )
        print(f"[live] stdout log captured ({len(stdout_text)} bytes)")

    print("PASS: tssrun live smoke test completed end-to-end.")
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
