# Live tssrun smoke test — setup guide

This document is the operator-side checklist for running
[`scripts/test_tssrun_live.py`](../scripts/test_tssrun_live.py) (and its
pytest wrapper [`python/tests/test_tssrun_live.py`](../python/tests/test_tssrun_live.py))
on a host where the kudpc / ECCS `tssrun` wrapper is actually installed.

The unit / integration tests under `cargo test --lib` and
`uv run pytest python/tests` do **not** need any of this — they stub `tssrun`
with `bash`. This document is only for the end-to-end live test against a
real SLURM allocation.

---

## 1. What the test verifies

A single happy-path invocation through the real `tssrun → salloc → srun`
chain, asserting all of the following line up:

1. `TssrunManager.spawn()` returns a positive `pid`.
2. `handle.is_running` / `exit_code` reflect lifecycle correctly while the
   allocation is pending.
3. `sent_env` is a dict; `live_env()` returns either a parsed environ
   (Linux + dumpable child), or `None` (best-effort fallback — see §6.1).
4. `handle.wait()` resolves to `0`.
5. `jobid` and `node` are parsed out of the `salloc:` banner on stdout.
6. The persisted snapshot at `<state_dir>/<pid>.json` exists.
7. `manager.attach_file(...)` round-trips an equivalent read-only handle.
8. The captured stdout log file contains the marker the child emitted —
   proof that the `FileLogSink` tee pipeline saw real data.

Exit codes:

| Code | Meaning |
|------|---------|
| `0` | PASS, or off-cluster SKIP (no `tssrun` on `$PATH`) |
| `1` | FAIL — assertion or unexpected exception |
| `2` | FAIL — overall watchdog timeout (queue too busy, child hung) |

---

## 2. Prerequisites

- A login node with `tssrun` on `$PATH` (Kyoto-U kudpc / ECCS frontends
  satisfy this; any other site needs a path override — see §4.4).
- Membership in at least one SLURM partition / queue that allows
  interactive batch via `tssrun`. Verify with:

  ```bash
  sinfo -o "%P %G"     # partitions and their AllowGroups
  sacctmgr show assoc user=$USER format=Account,Partition
  ```

- Python toolchain bootstrapped (uv / maturin):

  ```bash
  uv sync --all-extras
  uv run maturin develop      # builds & installs the pyo3 extension
  ```

  After any change to Rust code under `src/`, re-run `maturin develop`
  before re-running the live test — the Python wheel needs the rebuilt
  `_core` shared library.

- A working Rust toolchain consistent with `rust-toolchain.toml` (a
  per-user install under `~/.local/share/cargo/bin` is fine; just make
  sure `cargo` resolves to it before any system-wide stale Rust).

---

## 3. The two cluster gotchas (read this first)

These are not bugs — they are facts of life on shared HPC clusters that the
script cannot autodetect. Both bit us during the initial run on kudpc.

### 3.1 `TMPDIR` must point at a shared filesystem

The script writes its child script via `tempfile.TemporaryDirectory(...)`,
which by default lands in `/tmp`. On kudpc / ECCS (and most HPC sites),
`/tmp` is **node-local**, so a script created on the login node is invisible
from the compute node:

```text
slurmstepd: error: execve(): /tmp/tssrun-live-XXXX/child.sh: No such file or directory
srun: error: <node>: task 0: Exited with exit code 2
```

Fix: point `TMPDIR` at any path that is mounted on both the login node and
the compute nodes — typically `$HOME` or a project scratch area:

```bash
mkdir -p "$HOME/.cache/tssrun-live"
export TMPDIR="$HOME/.cache/tssrun-live"
```

If the site provides a high-performance shared scratch (e.g.
`/LARGE0/<group>/`), prefer that over `$HOME` for less I/O contention.

### 3.2 `TSSRUN_LIVE_QUEUE` must be a partition your group can use

If `TSSRUN_LIVE_QUEUE` is unset, `tssrun` picks a site default that may
not be available to your group, causing:

```text
salloc: error: Job submit/allocate failed: User's group not permitted to use this partition
```

Fix: pass an explicit queue. Use `sinfo`/`sacctmgr` from §2 to find one
your account can submit to:

```bash
export TSSRUN_LIVE_QUEUE="<group-or-personal-queue>"   # e.g. gr19999b
```

---

## 4. Environment variables

All variables are optional unless explicitly required by §3.

### 4.1 Required-in-practice on most sites

| Variable | Example | Why |
|----------|---------|-----|
| `TMPDIR` | `$HOME/.cache/tssrun-live` | child.sh must live on a shared FS (§3.1) |
| `TSSRUN_LIVE_QUEUE` | `gr19999b` | site default is often not allowed (§3.2) |

### 4.2 Tunables documented in the script

| Variable | Default | Purpose |
|----------|---------|---------|
| `TSSRUN_LIVE_BIN` | `tssrun` | Path / name of the tssrun executable. Useful when it lives somewhere unusual like `/usr/local/eccs/bin/tssrun`. |
| `TSSRUN_LIVE_TIME_LIMIT` | `0:01:00` | Wall-clock limit (`-t`). Keep small — this is a smoke test. |
| `TSSRUN_LIVE_RSC` | unset | Raw `--rsc` value, e.g. `p=1:c=1:m=512M`. Required on sites where `tssrun` rejects allocations without an explicit resource spec. |
| `TSSRUN_LIVE_TIMEOUT` | `180` | Hard watchdog (s) for the entire script. Bump it on busy queues to avoid the exit-code-`2` timeout path. |

### 4.3 Pytest wrapper gate

The pytest entry point is opt-in:

| Variable | Required value | Purpose |
|----------|----------------|---------|
| `RUN_LIVE_TSSRUN` | `1` | Without it, `python/tests/test_tssrun_live.py` is skipped so plain `uv run pytest` stays green on dev machines. |

### 4.4 Off-cluster behavior

If `tssrun` is not on `$PATH`, the script prints a `SKIP:` message and
exits `0` — safe to wire into CI on developer machines without breaking
them. The pytest wrapper similarly skips. There is nothing to configure
in this case.

---

## 5. Running the test

### 5.1 Standalone

```bash
mkdir -p "$HOME/.cache/tssrun-live"

TMPDIR="$HOME/.cache/tssrun-live" \
TSSRUN_LIVE_QUEUE="<your-allowed-queue>" \
  uv run python scripts/test_tssrun_live.py
```

Optional extras when the queue requires them:

```bash
TMPDIR="$HOME/.cache/tssrun-live" \
TSSRUN_LIVE_QUEUE="<your-allowed-queue>" \
TSSRUN_LIVE_RSC="p=1:c=1:m=512M" \
TSSRUN_LIVE_TIME_LIMIT="0:02:00" \
TSSRUN_LIVE_TIMEOUT="600" \
  uv run python scripts/test_tssrun_live.py
```

### 5.2 Pytest

```bash
TMPDIR="$HOME/.cache/tssrun-live" \
TSSRUN_LIVE_QUEUE="<your-allowed-queue>" \
RUN_LIVE_TSSRUN=1 \
  uv run pytest python/tests/test_tssrun_live.py -v -s
```

`-s` is recommended so the `[live]` log lines stream through to your
terminal instead of being captured.

---

## 6. Reading the output

A successful run looks roughly like this (numbers will differ):

```text
[live] argv = ['tssrun', '-p', 'gr19999b', '-t', '0:01:00', '/path/to/child.sh']
[live] spawned pid=476053
[live] /proc/476053/environ has 42 entries          # or 'unreadable (...)' — see §6.1
[live] child exit code = 0
[live] parsed jobid=7513460 node=xa0073
[live] snapshot persisted to /.../state/476053.json
[live] attach_file round-trip OK
[live] stdout log captured (87 bytes)
PASS: tssrun live smoke test completed end-to-end.
```

### 6.1 The `live_env` line is informational, not a failure

`live_env()` is best-effort and returns `None` for **any** of three
reasons. None of them fail the test:

1. Non-Linux platform (no `/proc`).
2. Linux but the child has already exited (`ENOENT`).
3. Linux and the child is a setuid/setgid binary with `PR_SET_DUMPABLE`
   cleared, so `/proc/<pid>/environ` is `root:root 0400` and only readable
   with `CAP_SYS_PTRACE` (`EACCES`).

The kudpc / ECCS `tssrun` wrapper falls into bucket (3), which is why the
script prints:

```text
[live] /proc/<pid>/environ unreadable (child already exited or non-dumpable setuid binary)
```

This is the expected line on those sites. If you instead see a Python
`RuntimeError: Permission denied (os error 13)`, your `_core` extension is
out of date — re-run `uv run maturin develop` to pick up the
`PermissionDenied → None` mapping in `read_live_env_for_pid`.

---

## 7. Common failure modes

| Symptom (in stderr / log) | Root cause | Fix |
|--------------------------|------------|-----|
| `RuntimeError: Permission denied (os error 13)` from `handle.live_env()` | Old extension build before the EACCES → None fix | Rebuild: `uv run maturin develop` |
| `salloc: error: Job submit/allocate failed: User's group not permitted to use this partition` | `TSSRUN_LIVE_QUEUE` is unset or wrong | Set it to one of the partitions in `sinfo -o "%P %G"` your group is in (§3.2) |
| `slurmstepd: error: execve(): /tmp/.../child.sh: No such file or directory` | `child.sh` was created on node-local `/tmp` | Set `TMPDIR` to a shared-FS path (§3.1) |
| `FAIL: tssrun child did not finish within 180s` (exit code `2`) | Queue is congested or the wall-clock `-t` is too short | Bump `TSSRUN_LIVE_TIMEOUT` and / or `TSSRUN_LIVE_TIME_LIMIT` |
| `assertion: expected jobid to be parsed from salloc: banner` | `tssrun` did not print the `salloc: Granted job allocation N` line on stdout — site-specific wrapper variant | Capture the actual stdout (the script prints the log) and adjust the parser in `src/tssrun/handle.rs::tee_lines` if necessary; report upstream |
| `SKIP: 'tssrun' is not on PATH` | Running on a dev box without the kudpc wrapper | Expected; either run on a login node or set `TSSRUN_LIVE_BIN` to a wrapper path |
| Hangs much longer than `TSSRUN_LIVE_TIMEOUT` | The watchdog cancels the wait but a stuck `salloc` holds resources | Cancel manually: `scancel <jobid>` once you see the `salloc: Granted job allocation N` line |

---

## 8. Cleanup

The script uses `tempfile.TemporaryDirectory(...)` as a context manager,
so the per-run scratch directory under `TMPDIR` is removed automatically
on success. On failure, the directory is also removed — the persisted
JSON snapshot, captured log, and `child.sh` go with it. If you need to
inspect them post-mortem, capture the script output (the `[live]` lines
print absolute paths) before the process exits, or temporarily replace
`TemporaryDirectory` with a hand-managed `Path` while debugging.

`scancel` any stranded `tssrun` job allocations from prior failed runs:

```bash
squeue -u "$USER"
scancel <jobid>
```
