# tssrun wrapper with background execution and environment inspection

- **Status**: Design draft
- **Date**: 2026-05-09
- **Branch**: `tssrun-wrapper-env`
- **Owner**: kkiyama117

## 1. Goal

Provide a Rust-and-Python API to invoke Kyoto University ECCS `tssrun`
(SLURM `salloc`+`srun` interactive-batch frontend) as a **background**
process and to **inspect its environment / lifecycle** at any time after
launch — both from the same process that spawned it and from a later
attached process.

Concretely the caller must be able to:

- Build a typed `tssrun` argv with queue, time limit, resource spec
  (`--rsc p=:t=:c=:m=:g=`), `--x11`, plus program and args.
- Spawn it without blocking on the child, receive a `JobHandle`
  immediately.
- Read at any time:
  - `pid` of the child
  - `sent_env`: the env vars the wrapper handed to the child
  - `live_env`: the child's current `/proc/<pid>/environ` (Linux only)
  - `jobid` / `node`: SLURM allocation info parsed from `salloc:`
    stdout/stderr lines
  - `is_running` / `exit_code` / `wait()`
- Persist the `JobHandle` snapshot to a JSON file on disk and re-attach
  to it from a different process for read-only inspection.
- Plug in an alternative log sink (stdout, file, in-memory, sqlite,
  etc.) without changing the wrapper.

Out of scope:

- Reimplementing `tssrun` itself.
- Cancelling the SLURM job (`scancel`) — left to the existing
  `SlurmManager` patterns or future work.
- Cross-host / SSH attach (only same machine).

## 2. Why this exists

The existing `SlurmManager` only models **synchronous** dispatch via
`SlurmCmd` + `JobDispatcher::run`, which `await`s the child to exit.
For Gaussian-style workflows on the ECCS TSS frontend we need to:

1. fire `tssrun` and let it allocate, then keep working in Python while
   the allocation/run progresses;
2. examine which env vars actually reached the child (the most common
   bug class is "Gaussian started but `g16root` was wrong");
3. recover the SLURM `jobid` so the existing
   `runner::query_job_states_batch` can poll `sacct` later.

## 3. Reference: tssrun behaviour (kudpc manual)

Manual: <https://web.kudpc.kyoto-u.ac.jp/manual/ja/run/interactive>

- Synchronous: `tssrun` waits for the program to finish.
- Argv shape: `tssrun [-p QUEUE] [-t HH:MM:SS] [--rsc p=…:t=…:c=…:m=…:g=…] [--x11] PROGRAM [ARGS…]`.
- On allocation, prints to stdout/stderr:
  - `salloc: Granted job allocation 102362`
  - `salloc: Waiting for resource configuration`
  - `salloc: Nodes cnode3 are ready for job`
- Lifecycle inquiry: `sacct -j JOBID` (≤ 24 h after termination).
- No documented detach / batch flag — backgrounding is achieved by the
  wrapper holding the `tssrun` child itself.

## 4. Architecture

### 4.1 Module layout

```
src/
├── manager.rs           existing SlurmCmd / SlurmManager — UNCHANGED
├── runner.rs            existing sacct/squeue parsing — REUSED
├── dispatcher.rs        existing JobDispatcher / TokioDispatcher / DryRunDispatcher
│                        + NEW: BackgroundDispatcher trait + TokioBackgroundDispatcher
├── tssrun/              NEW
│   ├── mod.rs           re-exports
│   ├── cmd.rs           TssrunCmd, Resource, build_argv
│   ├── parse.rs         pure parsers for `salloc:` lines
│   ├── log.rs           JobLogSink trait + Null/Std/File/InMemory sinks
│   ├── handle.rs        JobHandleSnapshot (Serde) + JobHandle (in-process)
│   └── manager.rs       TssrunManager: spawn / attach / query_state
└── py_export/
    └── tssrun.rs        NEW pyo3 bindings

python/slurm_async_runner/_core/
└── tssrun.pyi           NEW hand-written stubs
```

### 4.2 Reuse vs. new code

- **Reused unchanged**: `SlurmCmd`, `SlurmManager`, `runner::query_job_states_batch*`,
  `JobStatus`/`JobState`/`JobReason` re-exports.
- **Reused via trait**: existing `JobDispatcher` (for the `capture()`
  side of `sacct` polling).
- **Tokio primitives leveraged** (no reinvention): `tokio::process::Command`/`Child`,
  `Child::wait` / `Child::try_wait`, `tokio::sync::watch::channel`,
  `tokio::task::spawn` / `JoinHandle`, `tokio::io::BufReader::lines`,
  `tokio::fs`.
- **New**: `BackgroundDispatcher` trait (`spawn` non-blocking),
  the `tssrun/` module tree, and the corresponding pyo3 bindings.

### 4.3 Key types (Rust)

```rust
// tssrun/cmd.rs
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Resource {
    pub processes: Option<u32>,   // p=
    pub threads:   Option<u32>,   // t=
    pub cores:     Option<u32>,   // c=
    pub memory:    Option<String>,// m=  ("2G" など、自由文字列)
    pub gpus:      Option<u32>,   // g=
}

#[derive(Debug, Clone)]
pub struct TssrunCmd {
    pub tssrun_bin: String,             // default "tssrun"
    pub queue:      Option<String>,     // -p
    pub time_limit: Option<String>,     // -t HH:MM:SS
    pub rsc:        Option<Resource>,   // --rsc
    pub x11:        bool,               // --x11
    pub program:    PathBuf,
    pub args:       Vec<String>,
    pub env:        HashMap<String, String>, // 子に渡す env
    pub cwd:        Option<PathBuf>,
}
impl TssrunCmd {
    pub fn build_argv(&self) -> anyhow::Result<Vec<String>>;
}

// dispatcher.rs (extension)
pub struct SpawnedChild {
    pub pid: u32,
    pub child: tokio::process::Child,   // stdout/stderr piped, stdin null
}
pub trait BackgroundDispatcher: JobDispatcher {
    fn spawn(
        &self,
        argv: &[String],
        env: &HashMap<String, String>,
        cwd: Option<&Path>,
    ) -> impl Future<Output = anyhow::Result<SpawnedChild>> + Send;
}
pub struct TokioBackgroundDispatcher;
// implements both JobDispatcher (delegating to TokioDispatcher) and BackgroundDispatcher.

// tssrun/log.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogStream { Stdout, Stderr }
pub trait JobLogSink: Send + Sync {
    fn append(&self, stream: LogStream, line: &str)
        -> impl Future<Output = anyhow::Result<()>> + Send;
    fn flush(&self) -> impl Future<Output = anyhow::Result<()>> + Send;
}
pub struct NullLogSink;
pub struct StdLogSink;
pub struct FileLogSink { /* tokio::fs::File を Mutex で保持 */ }
pub struct InMemoryLogSink { /* Mutex<Vec<(LogStream, String)>> */ }

// tssrun/handle.rs
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LogLocations {
    None,
    Files { stdout: PathBuf, stderr: PathBuf },
    // future: Sqlite { db_path: PathBuf, run_id: u64 }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FinishedInfo {
    pub exit_code: Option<i32>,
    pub finished_at_unix: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobHandleSnapshot {
    pub pid: u32,
    pub argv: Vec<String>,
    pub sent_env: HashMap<String, String>,
    pub cwd: Option<PathBuf>,
    pub started_at_unix: i64,
    pub log_locations: LogLocations,
    pub jobid: Option<u64>,
    pub node:  Option<String>,
    pub finished: Option<FinishedInfo>,
}

pub struct JobHandle {
    snapshot_rx:   tokio::sync::watch::Receiver<JobHandleSnapshot>,
    snapshot_tx:   tokio::sync::watch::Sender<JobHandleSnapshot>,
    wait_handle:   Option<tokio::task::JoinHandle<anyhow::Result<i32>>>,
    child:         Option<std::sync::Arc<tokio::sync::Mutex<tokio::process::Child>>>,
    persist_path:  Option<PathBuf>,
}

// tssrun/manager.rs
pub enum AttachKey { Pid(u32), JobId(u64), File(PathBuf) }

pub struct TssrunManager {
    pub cmd: TssrunCmd,
    pub state_dir: Option<PathBuf>,
    pub log_sink:  std::sync::Arc<dyn JobLogSink>,   // default StdLogSink
}
impl TssrunManager {
    pub fn new(cmd: TssrunCmd) -> Self;
    pub fn with_state_dir(self, dir: PathBuf) -> Self;
    pub fn with_log_sink(self, sink: std::sync::Arc<dyn JobLogSink>) -> Self;
    pub async fn spawn(&self) -> anyhow::Result<JobHandle>;
    pub async fn spawn_with<D: BackgroundDispatcher>(&self, d: &D) -> anyhow::Result<JobHandle>;
    pub async fn attach(&self, key: AttachKey) -> anyhow::Result<JobHandle>;
    pub async fn query_state(&self, h: &JobHandle) -> anyhow::Result<JobStatus>;
}
```

### 4.4 Python surface

```python
# python/slurm_async_runner/_core/tssrun.pyi
class Resource:
    def __init__(self, *, processes: int|None = None, threads: int|None = None,
                 cores: int|None = None, memory: str|None = None,
                 gpus: int|None = None) -> None: ...

class TssrunCmd:
    def __init__(self, *, program: str, args: list[str] = [],
                 queue: str|None = None, time_limit: str|None = None,
                 rsc: Resource|None = None, x11: bool = False,
                 env: dict[str, str] = {}, cwd: str|None = None,
                 tssrun_bin: str = "tssrun") -> None: ...
    def build_argv(self) -> list[str]: ...

class TssrunJobHandle:
    pid: int
    @property
    def jobid(self) -> int | None: ...
    @property
    def node(self) -> str | None: ...
    @property
    def sent_env(self) -> dict[str, str]: ...
    async def live_env(self) -> dict[str, str] | None: ...
    def is_running(self) -> bool: ...
    def exit_code(self) -> int | None: ...
    async def wait(self) -> int: ...
    async def snapshot_dict(self) -> dict: ...
    async def persist(self) -> None: ...

class TssrunManager:
    def __init__(self, cmd: TssrunCmd, *,
                 state_dir: str|None = None,
                 log_sink: "LogSink|None" = None) -> None: ...
    async def spawn(self) -> TssrunJobHandle: ...
    async def attach_pid(self, pid: int) -> TssrunJobHandle: ...
    async def attach_jobid(self, jobid: int) -> TssrunJobHandle: ...
    async def attach_file(self, path: str) -> TssrunJobHandle: ...
    async def query_state(self, handle: TssrunJobHandle) -> JobStatus: ...

class LogSink:  # opaque pyclass; returned by the factory helpers below.
    ...

def null_log_sink() -> LogSink: ...
def std_log_sink() -> LogSink: ...
def file_log_sink(stdout: str, stderr: str) -> LogSink: ...
```

The `log_sink` parameter on the Python side accepts a `LogSink`
returned by the factory helpers above. Custom Python-side sink
implementations are explicitly out of scope for this iteration; the
Rust trait is sufficient extensibility.

## 5. Data flow

```
TssrunManager.spawn()
  ① cmd.build_argv()                     -> ["tssrun","-p","gr19999b",...,prog,args...]
  ② BackgroundDispatcher.spawn(argv, cmd.env, cmd.cwd)
        tokio::process::Command::new(argv[0]).args(argv[1..])
            .stdin(Stdio::null())
            .stdout(Stdio::piped()).stderr(Stdio::piped())
            .envs(env).current_dir(cwd?)
            .spawn()                     -> SpawnedChild { pid, child }
  ③ initial = JobHandleSnapshot { pid, argv, sent_env=cmd.env, cwd,
                                  started_at_unix=now,
                                  log_locations=…, jobid=None, node=None,
                                  finished=None }
  ④ (tx, rx) = watch::channel(initial)
  ⑤ stdout/stderr are .take()-en off the child
       tokio::spawn(tee_lines_task(stdout, LogStream::Stdout, log_sink, tx, parser))
       tokio::spawn(tee_lines_task(stderr, LogStream::Stderr, log_sink, tx, parser))
  ⑥ wait_handle = tokio::spawn(async move {
        let status = child.lock().await.wait().await?;
        let code   = status.code();
        tx.send_modify(|s| s.finished = Some(FinishedInfo{exit_code: code, finished_at_unix: now()}));
        if let Some(p) = &persist_path { write_atomic_json(p, &*tx.borrow())?; }
        Ok(code.unwrap_or(0))
     });
  ⑦ if state_dir set -> write_atomic_json(persist_path, &initial)
  ⑧ return JobHandle { snapshot_rx: rx, snapshot_tx: tx, wait_handle, child, persist_path }
```

`tee_lines_task` per stream:

```rust
let mut lines = BufReader::new(stream).lines();
while let Some(line) = lines.next_line().await? {
    log_sink.append(stream_kind, &line).await.ok(); // log failures must not abort tee
    if let Some(jid) = parse_salloc_jobid(&line) {
        tx.send_modify(|s| if s.jobid.is_none() { s.jobid = Some(jid) });
        if let Some(p) = &persist_path { let _ = write_atomic_json(p, &*tx.borrow()); }
    }
    if let Some(node) = parse_salloc_node(&line) {
        tx.send_modify(|s| if s.node.is_none() { s.node = Some(node) });
        if let Some(p) = &persist_path { let _ = write_atomic_json(p, &*tx.borrow()); }
    }
}
log_sink.flush().await.ok();
```

Read-side API maps directly to `snapshot_rx.borrow()`:

```rust
pub fn pid(&self)        -> u32  { self.snapshot_rx.borrow().pid }
pub fn is_running(&self) -> bool { self.snapshot_rx.borrow().finished.is_none() }
pub fn exit_code(&self)  -> Option<i32> {
    self.snapshot_rx.borrow().finished.as_ref().and_then(|f| f.exit_code)
}
pub async fn wait(&mut self) -> anyhow::Result<i32> {
    // `&mut self` is required because `JoinHandle::await` consumes the
    // handle. Python bindings call this on `&mut PyTssrunJobHandle`,
    // which pyo3 supports via `#[pymethods] async fn wait(&mut self)`.
    let h = self
        .wait_handle
        .take()
        .ok_or_else(|| anyhow!("not owner of the child / already waited"))?;
    h.await?
}
```

`live_env()` reads `/proc/{pid}/environ` via `tokio::fs::read`, splits
on `\0`, then on the first `=`. On non-Linux returns `Ok(None)`.

## 6. Persistence and attach

- **Path**: `{state_dir}/{pid}.json` written atomically via
  `tempfile::NamedTempFile::persist`.
- **Write triggers**: spawn (initial dump), on `jobid` parsed, on `node`
  parsed, on child exit. All best-effort — failures log a warning via
  `tracing::warn!` and never abort the tee task.
- **Format**: `serde_json::to_string_pretty(&JobHandleSnapshot)`.
  Synthetic example payload:

  ```json
  {
    "pid": 31415,
    "argv": ["tssrun", "-p", "gr19999b", "-t", "1:0:0", "/work/job.sh"],
    "sent_env": { "OMP_NUM_THREADS": "8" },
    "cwd": "/work",
    "started_at_unix": 1746345600,
    "log_locations": {
      "Files": { "stdout": "/var/log/.../31415.stdout",
                 "stderr": "/var/log/.../31415.stderr" }
    },
    "jobid": 102362,
    "node": "cnode3",
    "finished": null
  }
  ```

- **Attach** rebuilds a `JobHandle` with `child = None`,
  `wait_handle = None`. Read APIs work; `wait()` errors with
  `"not owner of the child"`. `refresh_from_disk()` re-reads the file.

## 7. Error handling and constraints

| Case | Behaviour |
|------|-----------|
| `tssrun` binary missing | `BackgroundDispatcher::spawn` returns `Err` from `tokio::process::Command::spawn`. |
| Child exits immediately | `wait_handle` records `exit_code`; `is_running` becomes false. |
| `salloc:` line never appears | `jobid`/`node` stay `None`. The caller must impose a timeout if it needs them. |
| persist write fails | `tracing::warn!`, tee task continues. |
| `attach()` reads malformed JSON | error is propagated. |
| double `wait()` | second call returns `Err("already waited")`. |
| `wait()` on attached handle | `Err("not owner of the child")`. |
| `live_env()` on non-Linux | `Ok(None)` (best effort). |

Known constraints (also called out in rustdoc):

- `tssrun` is `salloc`-based, so jobid extraction depends on the
  literal line `"salloc: Granted job allocation N"`. Site-specific
  modifications break this — parsers stay narrow on purpose.
- `query_state` uses `sacct`; per the kudpc manual jobs older than
  ~24 h return `Unknown`. Mirrors the existing `SlurmManager` semantics.
- `JobHandle::Drop` does **not** kill the child. Callers are
  responsible for either `wait()`-ing or letting persistence carry the
  state forward. This is documented; reaping is left to the caller to
  avoid surprising long-running SLURM allocations being torn down.

## 8. Test plan

### Unit tests (in-file `#[cfg(test)] mod tests`)

- `tssrun/cmd.rs`
  - default argv = `["tssrun", absolute_program]`.
  - all flags populated, ordering: `tssrun` → `-p Q` → `-t T` →
    `--rsc p=N:t=N:c=N:m=S:g=N` → `--x11` → `program` → `args…`.
  - `--rsc` only emits keys whose `Option` is `Some`, separator is `:`.
  - relative `program` becomes absolute via `std::path::absolute`.
- `tssrun/parse.rs`
  - `parse_salloc_jobid("salloc: Granted job allocation 102362")` →
    `Some(102362)`.
  - non-match (whitespace, prefix mismatch, non-numeric) → `None`.
  - `parse_salloc_node("salloc: Nodes cnode3 are ready for job")` →
    `Some("cnode3")`; multi-node form (`cnode[3-4]`) preserved verbatim.
- `tssrun/log.rs`
  - `InMemoryLogSink::append` preserves order.
  - `FileLogSink` writes one line per append, separated by `\n`.
  - `NullLogSink::append` is a no-op (file tree unchanged).
- `tssrun/handle.rs`
  - serde round-trip of `JobHandleSnapshot` for all `LogLocations`
    variants and with/without `finished`.
  - `attach_snapshot(...).wait()` errors with the documented message.
- `dispatcher.rs`
  - `TokioBackgroundDispatcher::spawn(["bash","-c","exit 0"])` returns
    a positive pid, child eventually exits 0.
  - `spawn(["does-not-exist-xyz"])` returns `Err` containing
    `"failed to spawn"`.

### Integration tests (`tests/tssrun_integration.rs`)

Use a bash script as a fake `tssrun_bin`:

```bash
echo "salloc: Granted job allocation 999"
echo "salloc: Nodes node-x are ready for job"
sleep 0.2
echo done
```

Cases:

- `manager.spawn().await` → `handle`. Within 2 s, `handle.snapshot()`
  shows `jobid == Some(999)` and `node == Some("node-x")`.
- `handle.wait().await == 0`.
- `InMemoryLogSink` collects the `done` line.
- `FileLogSink` produces a stdout file containing `done`.
- Persist→attach: spawn with `state_dir=tmp`, observe `{pid}.json`
  appearing, then build a second `TssrunManager` and call
  `attach(File(path))` — read APIs return identical values.

### Python side (`python/tests/test_tssrun.py`)

- `Resource(processes=4, memory="2G")` → `cmd.build_argv()` contains
  `--rsc p=4:m=2G`.
- `await manager.spawn()` returns a handle whose `.pid > 0` and whose
  `.jobid` (after the bash mock runs) equals `999`.
- `await handle.wait() == 0`.
- `null_log_sink()` does not raise; `file_log_sink(s, e)` writes to
  the supplied paths.

### Coverage

- Target ≥ 80 % line coverage in `src/tssrun/` measured via
  `cargo llvm-cov --lib --tests`. Existing CI workflow already runs
  the cargo tests; an additional `cargo llvm-cov` step is optional.

## 9. Migration / compatibility

- **No public API removed**: `SlurmCmd`, `SlurmManager`,
  `JobDispatcher`, `runner::*` are unchanged.
- **New trait `BackgroundDispatcher`** is additive. Existing
  `TokioDispatcher` / `DryRunDispatcher` users are unaffected.
- **Cargo features**: no new features required. Adds `serde_json` and
  `tempfile` to `Cargo.toml` if not already present, and reuses
  `std::time::SystemTime` for unix timestamps (no `chrono` needed).
  Implementation step 0 (before code) runs
  `cargo tree --depth 1 | grep -E '^(serde|serde_json|tempfile|tracing)'`
  to confirm what is already pulled in transitively (`gaussian_job_shared`
  may already provide some of these) and only adds what is missing.
- **Python compat**: new submodule `slurm_async_runner._core.tssrun`.
  No existing imports break.

## 10. Implementation phasing

The plan in `writing-plans` will divide along these natural seams:

1. `tssrun/cmd.rs` + `tssrun/parse.rs` (pure, fully unit-testable).
2. `tssrun/log.rs` (sinks, also pure-ish).
3. `dispatcher.rs` extension (`BackgroundDispatcher` +
   `TokioBackgroundDispatcher`).
4. `tssrun/handle.rs` (snapshot + JobHandle, no manager yet).
5. `tssrun/manager.rs` (orchestration, persist, attach,
   query_state delegation).
6. Integration tests (`tests/tssrun_integration.rs`).
7. `py_export/tssrun.rs` + `python/.../tssrun.pyi` + Python tests.
8. README / CHANGELOG update.

Each step lands behind a passing `cargo test` + `cargo clippy -- -D warnings`
and (for steps 7+) `uv run pytest`.
