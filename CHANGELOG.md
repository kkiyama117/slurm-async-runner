# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v3.1.0] - 2026-06-12

Minor release: array-task status queries join the shared batch caches,
and array sacct finalizers are collapsed onto one master-keyed slurmdbd
query per cache window — both aimed at further reducing SLURM status
command load (sacct above all), continuing the v3.0.0 multiplexing
work. No breaking changes.

### Changed

- **Array-task sacct finalizers are keyed by the master jobid.**
  `refresh_with_sacct()` on an array-task handle now issues
  `sacct -j <master>` (instead of `-j <master>_<idx>`) and extracts the
  task's own `<master>_<idx>` parent row from the expanded listing —
  KUDPC live verification (2026-06-12, jobid 7815414) confirmed sacct
  expands a master-keyed query into per-task rows. All task finalizers
  of one array therefore share a single batch-cache key: one slurmdbd
  query per cache TTL window serves the whole array even when tasks
  finish in different poll cycles, where per-task keys each paid a
  fresh sacct spawn. sacct is the heaviest status command (slurmdbd
  query); this further reduces its invocation count on top of the
  v3.0.0 batch cache. Absent-row semantics are unchanged: a task whose
  row has not reached accounting yet still resolves to "no usable row"
  and is retried later, never another task's outcome.

### Added

- **Array-task squeue/sacct queries join the shared batch caches.**
  Per-task probes (`squeue -j <master>_<idx>`) and finalizers
  (`sacct -j <master>_<idx>`) now share the manager-wide single-flight
  TTL batch caches with plain-job handles, so N array-task handles
  polling concurrently cost one squeue and (on finish bursts) one sacct
  per `poll_interval` instead of N. Live-verified on KUDPC (2026-06-12,
  jobids 7815400–7815414): both commands accept mixed plain/array `-j`
  lists and answer keyed per-task rows. The squeue summary argv gains
  `-r` (plain jobs unaffected; required so PENDING array tasks print one
  keyed row each instead of an aggregate `<master>_[0,2]` row), and the
  array probe switched from positional `-o "%T %r"` parsing to the same
  exact-key `%i` matching the sacct array parser already uses.

## [v3.0.0] - 2026-06-12

Major release: the tssrun backend's Rust API now returns typed errors
(breaking for Rust callers; Python behavior is unchanged), the
cross-backend `JobManager` trait lands, and `refresh_with_sacct()` is
multiplexed across handles via a generic single-flight TTL batch cache
(one slurmdbd query per `poll_interval` instead of one per handle).

### Added

- **Structured errors for the tssrun backend** (issue #16 item 1).
  New `tssrun::error` module with `TssrunSpawnError`, `TssrunAttachError`,
  `TssrunWaitError`, `TssrunRefreshError` (all `#[non_exhaustive]`,
  re-exported from the crate root), mirroring the sbatch error family so
  Rust callers can match on failure modes.
- **`JobManager` trait** (issue #16 item 2) — manager-side companion to
  `JobHandleCommon`: `spawn` / `attach_uuid` / `attach_jobid` with
  associated `Handle`, `SpawnError`, `AttachError` types, implemented by
  both `TssrunManager` and `SbatchManager` (backend-specific entry points
  such as `spawn_array`, `run`, `cancel`, `attach(AttachKey::Pid|File)`
  stay inherent). Cross-backend contract test in
  `tests/job_manager_common.rs`.
- **Shared batched `sacct` exit-code cache (refresh multiplexing,
  part 3).** Handles of the same `SbatchManager` whose jobs finish in a
  burst now share one batched
  `sacct -P -n -j id1,id2,… -o JobID,State,Reason,ExitCode` query per
  `poll_interval` (single-flight TTL cache), instead of one sacct —
  i.e. one slurmdbd hit — per handle per `refresh_with_sacct()` call.
  Accounting-lag retries within the TTL replay the cached listing
  instead of re-querying slurmdbd. The same subset-replay rule as the
  squeue cache applies, so "no row for my id" can never be fabricated.
  Array-task finalizers (`-j <master>_<idx>`) and the legacy 3-column
  listing are never batched or cached. Motivated by the KUDPC manual's
  request not to repeat status commands mechanically; sacct is the
  heaviest such command (accounting DB query).
- **Generic `query_cache` primitive** (issue #16 item 3, internal).
  The single-flight TTL batch cache behind the `qgroup -l`, squeue and
  sacct multiplexing is now one generic implementation
  (`sbatch::query_cache`, parameterized by a `QueryShape`); the three
  shapes share the TTL slot, single-flight locking, subset-replay rule
  and 2 × `poll_interval` key-registry aging. No behavior change for
  the existing qgroup/squeue caches. The array-task sacct parser now
  requires an exact JobID-column match for the queried
  `<master>_<idx>` key (defense in depth; prerequisite for ever
  batching array-task queries).

### Changed

- **Rust API (breaking): tssrun signatures return typed errors.**
  `TssrunManager::spawn`/`spawn_with` → `Result<_, TssrunSpawnError>`,
  `TssrunManager::attach` → `Result<_, TssrunAttachError>`,
  `TssrunJobHandle::wait` → `Result<_, TssrunWaitError>`,
  `TssrunJobHandle::refresh`/`wait_terminal` → `Result<_, TssrunRefreshError>`.
  The `JobHandleCommon` trait impl still exposes `anyhow::Result`, and
  error message strings are unchanged, so Python behavior (exception
  types and messages) is identical.

### CI

- `cargo audit` job (advisory ignores documented in `.cargo/audit.toml`);
  `pyo3-stub-gen` now pinned to the crates.io 0.22.3 release instead of a
  moving `branch = "main"` git pin (issue #16 item 6).
- Stub-drift check: CI regenerates the `.pyi` stubs and fails if the
  committed ones are stale (issue #16 item 7).

## [v2.0.0] - 2026-06-12

Major release: monitoring-correctness fixes change observable behavior
(`refresh()` / `refresh_with_sacct()` can now raise on transient SLURM
failures instead of recording a false vanish), and the refresh path is
multiplexed across handles (shared `qgroup -l` cache + batched squeue
queries). See **Changed** for the breaking items.

### Added

- **Shared `qgroup -l` listing cache (refresh multiplexing).** Handles
  created by the same `SbatchManager` now share a single-flight TTL
  cache (TTL = `poll_interval`) for the `qgroup -l` listing, so N
  concurrently-polling handles spawn one qgroup subprocess per poll
  cycle instead of N (100 handles @ 5 s: 72 000 → 720 spawns/hour).
  Spawn failures (missing qgroup binary on non-KUDPC clusters) are
  cached too, so the squeue fallback no longer re-spawns a failing
  binary per handle per poll. `squeue`/`sacct`/`sbatch`/`scancel` are
  never cached; a manual `refresh()` can observe a listing at most one
  poll cycle old (qgroup data is itself sampled ~30 s at the source).
- **Shared batched `squeue` listing (refresh multiplexing, part 2).**
  Handles of the same `SbatchManager` that fall through to the squeue
  probe now share one batched `squeue -j id1,id2,… -o "%i %T %r"`
  query per poll cycle (single-flight TTL cache, TTL =
  `poll_interval`), instead of one single-id squeue subprocess per
  handle. Built after live verification on KUDPC (2026-06-12) that a
  multi-id `-j` list always exits 0 and returns rows for still-listed
  ids only — even when some or all queried ids are already purged — so
  the existing "absent row = left the queue" semantics carry over
  unchanged. A cached listing is only replayed to requests whose ids
  the batch actually queried (anything else would fabricate a vanish
  signal); a new handle's first poll therefore costs one live re-batch
  and joins the shared listing from the next cycle. Ids idle for
  2 × `poll_interval` age out of the batch. Array-task probes
  (`squeue -j <master>_<idx>`) and `sacct` are never batched or
  cached.

### Changed

- **Structured subprocess capture (`CaptureOutput`).** The internal
  `JobDispatcher::capture` API now returns
  `CaptureOutput { exit_code, stdout, stderr }` instead of merging
  stderr into stdout behind a `[stderr]` marker line. Query parsers read
  `.stdout` only — the marker-misread bug class is structurally
  impossible now — and error diagnostics use
  `CaptureOutput::diagnostic()` (same merged text as before, so
  user-facing error messages are unchanged). `runner::stdout_section`
  is gone.
- **`refresh()` now distinguishes transient SLURM failures from
  vanished jobs.** A `squeue`/`sacct` non-zero exit with a stderr other
  than `Invalid job id specified` (e.g. `Socket timed out` under
  controller overload) propagates as an error instead of being recorded
  as a false "left the queue" observation. `Invalid job id specified`
  still counts as a vanish. **Behavior change**: Python `refresh()` /
  `refresh_with_sacct()` can now raise `RuntimeError` on controller
  hiccups — retry on the next poll.
- **`wait_terminal` tolerates transient refresh failures.** Up to 5
  consecutive refresh errors are logged (`tracing::warn!`) and polling
  continues; the 5th consecutive failure propagates. A multi-hour wait
  no longer dies on a single controller timeout.
- **`log_lines` reads the file tail via backwards chunked seeks.**
  Memory is now `O(n × line length)` instead of `O(file size)` — no
  more whole-file loads (potential OOM) on multi-GB job logs.
  `read_log_to_end` intentionally keeps whole-file semantics.
- **State store hardening:** corrupted snapshot files are skipped with
  a `tracing::warn!` instead of silently vanishing from `list()`;
  `JobStateStore::delete(uuid)` added (idempotent) for garbage
  collection; the on-disk envelope now carries `"schema_version": 1`
  (files without the field load as v1; unsupported versions fail with a
  clear message).

### Fixed

- **`refresh_with_sacct` no longer stamps a non-terminal `FinishedInfo`
  after a false vanish.** A transient `squeue` failure under controller
  overload (stderr `Socket timed out`, empty stdout) is indistinguishable
  from a purge, so `left_active_listing` could flip to `true` while the
  job was still running; the follow-up sacct query then returned a live
  `RUNNING|…|0:0` row that slipped past the no-row sentinel and froze
  `FinishedInfo { final_state: Running, exit_code: Some(0) }` behind the
  idempotency short-circuit. Now only terminal states (or unrecognized
  future tokens carrying an exit code) are stamped; a known non-terminal
  row instead rolls back `left_active_listing`, records the live
  observation, and normal polling resumes.
- **`from slurm_async_runner import tssrun / sbatch / …` works.**
  `__all__` advertised the extension submodules without binding them in
  the package namespace, so every advertised name raised `ImportError`.
  The submodules are now re-exported at the top level.
- **PEP 561 `py.typed` marker added.** Downstream mypy/pyright previously
  treated the whole package as untyped and ignored every `.pyi` stub.
- **Stub fixes:** `SbatchCmd.build_argv` and `FinishedInfo.__repr__` were
  missing from the handwritten `sbatch.pyi`; the PyPy trove classifier
  (impossible for an abi3 CPython extension) and the hyphenated
  `known-first-party` entry in `pyproject.toml` were corrected.

- **sbatch monitoring correctness — stderr misread / sacct-lag freeze /
  qgroup fallback.**
  - **Array-task refresh no longer misreads merged stderr as a job
    state.** `TokioDispatcher::capture` merges the child's stderr after a
    `[stderr]` marker line; once an array master left squeue entirely
    (`slurm_load_jobs error: Invalid job id specified` on stderr),
    `parse_squeue_array_task` read the marker line itself as a state token
    (`Unknown`), so `left_active_listing` never flipped, `wait_terminal`
    polled forever, and `refresh_with_sacct` never resolved the exit code.
    All `runner::query_*` helpers now strip the `[stderr]` section
    (`runner::stdout_section`) before parsing.
  - **`refresh_with_sacct` no longer freezes a fabricated `Unknown`
    outcome when sacct has no row yet** (accounting flush lag, or history
    purge). `lifecycle.finished` is left unset so a later call retries;
    callers can detect the vanished-but-unresolved state via
    `left_active_listing == True and not is_finished()`. **Behavior
    change**: a single `refresh_with_sacct()` call immediately after the
    job vanishes may now report `is_finished() == False` until SLURM
    accounting catches up — poll again instead of assuming one call
    finalizes (affects `scripts/test_sbatch_live.py`-style usage).
  - **`refresh()` falls back to squeue when `qgroup` itself fails** (e.g.
    the binary does not exist on non-KUDPC clusters). The error is logged
    via `tracing::warn!` and treated as a qgroup miss instead of failing
    the whole refresh.

- **Code-review hardening (issue #15).**
  - `PyTssrunJobHandle.wait_terminal` no longer holds the handle mutex
    across the whole polling loop — the lock is taken per `refresh`
    round-trip, so a concurrent `wait()` / `refresh()` on the same handle
    is not serialized for the (potentially minutes-long) wait.
  - `InMemoryStateStore` switched to an internal `std::sync::Mutex`;
    `len()` / `is_empty()` can no longer panic under lock contention
    (previously `try_lock().expect(...)`).
  - `DynJobHandleCommon::snapshot_json` now returns
    `anyhow::Result<serde_json::Value>` instead of panicking on
    serialization failure (**breaking** for `dyn` facade callers).
  - `resolve_log_path` gained an env-free substitution core
    (`resolve_log_path_with`); its tests no longer mutate process env via
    `unsafe { env::set_var }` (multi-threaded test-runner race).
  - `TssrunManager::query_state` doc corrected: `squeue` with `sacct`
    fallback, not sacct-only.
  - Python: `JobHandleCommon.refresh` / `wait_terminal` annotated
    `-> None`; `run_with_jobid_callback(on_spawn)` stub typed
    `Callable[[int], object]`; `scripts/test_sbatch_live.py` cleans up its
    `state_dir` on failure; the H1 snapshot-getter regression test is now
    event-based instead of sleep-based; misc stub / test cleanups.

## [v1.1.0] - 2026-05-28

### Added

- **`nice` option on `SbatchCmd` / `SlurmJobConfig`** (issue #13). Emits
  `--nice=<v>` (single token, so negative values pass through) to adjust SLURM
  scheduling priority — positive lowers priority, negative raises it. Verified
  accepted by the KUDPC sbatch wrapper. `SlurmJobConfig.nice` is a config field
  only (not auto-wired to argv).

### Docs

- **README**: unified `TssrunCmd` and `SbatchCmd` sections into a parallel
  5-subsection layout for easier side-by-side comparison.

## [v1.0.0] - 2026-05-12

### KUDPC live correctness fixes — watch updates, qgroup `FINI`/`FAIL`, sacct gating

- **Fix: `watch::Sender::send` → `send_replace` (6 call sites)**. Both
  `SbatchJobHandle::new` and `TssrunJobHandle::new` construct the watch
  channel and immediately drop the initial `_rx`. Until a caller subscribes
  via `.watch()`, `receiver_count == 0`, and tokio's `send` early-returns
  `Err(SendError)` **without updating the stored value**. `let _ =
  snapshot_tx.send(...)` silently discarded that Err, so every
  `refresh()` / `refresh_with_sacct()` update was a no-op against the
  watch channel. Python-side reads via `borrow()` (e.g.
  `handle.is_finished()` / `exit_code()` / `is_running()`) returned the
  spawn-time defaults forever. Switched all 5 sbatch + 1 tssrun internal
  sends to `send_replace`, which updates the value unconditionally.
  See `docs/architecture.md` §3.5 design judgement #6 and
  `docs/development.md` §5.9 for the diagnosis. Follow-up "internal
  keepalive receiver" / "replace `watch` with `Arc<RwLock<...>>` /
  `ArcSwap`" options are recorded as issue #8 comment for future work.
- **Fix: `SbatchJobHandle::refresh_with_sacct` gating loosened**. The
  previous `!lifecycle.left_active_listing → return early` guard skipped
  sacct whenever the job was still listed in qgroup. After the `FINI`
  token was mapped to `JobState::Completed` (below), a freshly-finished
  job that is still listed in qgroup now provably reaches a terminal
  state via `last_observed_state.is_terminal()`. The new gating fires
  sacct when **either** the active listing has been left **or** the last
  observed state is terminal — `lifecycle.finished.is_some()` keeps the
  idempotency short-circuit. sacct is still treated as heavyweight and
  not called from `refresh()`. Regression test
  `refresh_with_sacct_calls_sacct_when_qgroup_reports_terminal_fini` and
  the idempotency test `refresh_with_sacct_is_idempotent_when_finished_already_set`
  pin the new behaviour.
- **Fix: KUDPC `qgroup -l` parser handles pipe-separated layout and
  `FINI` / `FAIL` STAT tokens.** The current KUDPC layout emits
  `QUEUE USER JOBID | STAT ...` with `|` column dividers; the previous
  `parse_qgroup_l` indexed field 3 as STAT and consumed `"|"` instead,
  silently mapping every observed state to `JobState::Unknown`. The new
  parser drops standalone `|` tokens before indexing and rejects
  `jobid == 0` so per-queue / per-user summary rows aren't injected as
  phantom entries. `JobState::parse` gains `"FINI"` → `Completed` and
  `"FAIL"` → `Failed` as **input-only KUDPC aliases**; `as_token()`
  continues to return the SLURM canonical `"COMPLETED"` / `"FAILED"` so
  persisted snapshots stay in the SLURM vocabulary. Regression tests
  pin both the full 3-section pipe layout and the FINI/FAIL terminal
  round-trip.
- **Refactor: `SbatchManager::new` default `poll_interval` 30 s → 60 s**.
  SLURM's default task-sampling interval is 30 s, so 30 s polling could
  trap two consecutive `qgroup -l` calls inside one sampling window and
  miss state transitions. 60 s guarantees we always cross at least one
  sampling boundary between polls and reduces KUDPC squeue load.
  Override via `SbatchManager::with_poll_interval(Duration)`.
- **Fix: `TokioDispatcher::capture` merges stderr after stdout**.
  Previous implementation returned `(exit_code, stdout)` and silently
  discarded stderr — so `sbatch: error: invalid partition` and friends
  reached `SbatchSpawnError::SubmitFailed::output` as empty strings,
  hiding the actual failure cause from users. `capture` now appends
  stderr after stdout with a `\n[stderr]\n` marker when stderr is
  non-empty, leaving the success path untouched (stderr empty → no
  marker). Line-based parsers (`parse_qgroup_l`, `parse_squeue`,
  `parse_sacct_*`, `parse_submitted_jobid`) ignore unknown prefixes so
  the merge is non-destructive. Regression test
  `tokio_capture_merges_stderr_after_stdout` pins the failure-mode
  diagnostic contract.
- **Test: `scripts/test_sbatch_live.py` adds a failure case**. The
  standalone live smoke script now runs two cases back-to-back: success
  (`exit 0` → expect `exit_code == 0`) and failure (`exit 7` → expect
  `exit_code == 7`). The new `_run_one_case` helper factors the shared
  `wait_terminal → refresh_with_sacct → attach_uuid` round-trip path.
  `SBATCH_LIVE_POLL_INTERVAL` env var added (default `10` s) so live
  smokes can poll faster than the new 60 s `SbatchManager` default.

### Post-merge follow-ups — `run_with` callback, `_async` removal

- **`SbatchManager::run_with<F: FnOnce(u64)>(on_spawn: F)` callback variant added** to close the timeout-with-cancel gap that PR #6 review M2 surfaced. Spec §6.1 instructed callers to wrap `mgr.run()` in `tokio::time::timeout` and call `mgr.cancel(jobid)` after a timeout, but the dropped future stranded the jobid. `run_with` invokes `on_spawn(jobid)` synchronously the moment sbatch returns, giving callers a hook to stash the jobid before the wait_terminal/refresh_with_sacct phase. Existing `run()` now delegates to `run_with(|_| {})`. pyo3 exposes it as `SbatchManager.run_with_jobid_callback(on_spawn)`. Spec §6.1 in `docs/superpowers/specs/2026-05-10-sbatch-phase2-design.md` has a "Post-Phase-2 follow-up" note documenting this resolution. Resolves PR #6 review M2.
- **BREAKING (pyo3): `TssrunJobHandle.*_async` getters / methods removed** (`uuid_async`, `jobid_async`, `is_running_async`, `is_finished_async`, `exit_code_async`). They were added in Phase 3 P5 as temporary escape hatches for callers wired against the pre-P5 await-style contract; with Phase 3 merged and the sync shape now canonical (matching `slurm_async_runner.JobHandleCommon`), the escape hatches are dead weight. Migration: drop the `await` and the `_async` suffix at the call site. Resolves PR #6 review L3.

### PR #6 review follow-ups — typed errors, scancel override, shared test fixture

- **`SbatchManager::spawn_array` no longer `panic!`s** when `expand_array_indices` yields an empty list — direct struct construction that bypasses `SlurmArraySpec::FromStr` now surfaces as `SbatchSpawnError::Other(...)` instead. `FromStr` itself still rejects empty specs (no behavior change for the happy path). Resolves PR #6 review M1.
- **`SbatchManager::with_scancel_bin(bin)` builder added** so cancel paths can be exercised without a real SLURM cluster. The default remains `"scancel"`. pyo3 binding exposes it as `SbatchManager(cmd, scancel_bin=<path>)`. Python smoke tests `test_cancel_uses_scancel_bin_override` and `test_cancel_raises_runtime_error_on_nonzero_exit` replace the previously-skipped placeholder, covering both the success and `SbatchCancelError::Scancel` non-zero-exit branches end-to-end. Resolves PR #6 review L1 + the previously-tracked Phase 3 cancel follow-up.
- **`crate::sbatch::test_util::ArcDispatcher<D>` shared test fixture**: the `MoveDispatcher` / `MoveRecording` newtypes previously duplicated across `handle.rs` and `manager.rs` test modules are now a single generic helper in `src/sbatch/mod.rs` under `#[cfg(test)] pub(crate) mod test_util`. Production binaries are unaffected. Resolves PR #6 review M3.
- **`parse_signal_ident` docstring** clarifies that name matching is lexical-only (`^[A-Z][A-Z0-9_]*$`); semantic signal-table validation is deferred to SLURM at submit time and surfaces via `SbatchSpawnError::SubmitFailed`. Resolves PR #6 review L2.
- **`SbatchJobHandle::refresh_with_sacct` double-lock intent** documented inline — the second `refresh_lock.lock().await` guards the sacct call + finished-info write, distinct from the inner `refresh()` qgroup/squeue serialization. Resolves PR #6 review L4.

### Phase 3 P5 — Python Protocol parity (sync getters for `TssrunJobHandle`)

- **Breaking on the unreleased Phase 3 branch**: `TssrunJobHandle.uuid` / `jobid` / `is_running` / `is_finished` / `exit_code` on the pyo3 wrapper are now **sync** — they read lock-free off the local `watch::Receiver` and never had any tokio runtime work to wait on. The previous `future_into_py` wrappers were bogus overhead and made the new `slurm_async_runner.JobHandleCommon` Protocol structurally incorrect (sbatch was sync, tssrun was async; the Protocol could not honestly span both). Direct call sites change from `await h.uuid` → `h.uuid`, `await h.is_running()` → `h.is_running()`, etc.
- **Backward-compatible escape hatch**: each converted member also gains an `*_async` getter/method (`uuid_async`, `jobid_async`, `is_running_async`, `is_finished_async`, `exit_code_async`) that preserves the pre-P5 await-style contract. Callers that wired against the old shape can migrate at their leisure by either dropping the `await` or renaming to the `_async` member. The `*_async` shapes are intentionally NOT mirrored in the `JobHandleCommon` Protocol — the Protocol is the canonical sync contract.
- **Protocol updated**: `python/slurm_async_runner/__init__.py:JobHandleCommon` now declares `uuid` / `jobid` as `@property` and `is_running` / `is_finished` / `exit_code` as sync methods. After P5 the structural type check `isinstance(h, JobHandleCommon)` AND the static-type signatures both match the actual pyo3 call shape on both backends. Dropped the unused `from uuid import UUID` import (M1 from the PR #7 review).
- **Tests**: `python/tests/test_protocol.py` adds `test_tssrun_jobhandle_call_shape_matches_protocol` exercising the real sync call shape against a constructed handle, so the pre-P5 `runtime_checkable`-name-only false positive cannot reappear silently. `python/tests/test_tssrun.py` and `scripts/test_tssrun_live.py` migrated to the sync shape; one site in `test_tssrun.py` also covers `jobid_async` to keep that contract green.
- Resolves PR #7 review HIGH H1 (Protocol signature mismatch) + MEDIUM M1 (dead `UUID` import).

### Phase 3 P4 — type-erased `DynJobHandleCommon` + Python `Protocol`

- **`crate::handle::DynJobHandleCommon`** — object-safe companion to `JobHandleCommon`. Snapshot is exposed as `serde_json::Value` to flatten the associated type. Carries a static `kind() -> &'static str` discriminator that matches `JobSnapshot::kind()`.
- **`crate::handle::DynHandleAdapter<H>`** + **`crate::handle::into_dyn(handle)`** free function — the explicit constructor avoids the blanket-impl + associated-type E0034 ambiguity diagnosed in Phase 1 (handover §4). Mirrors the `dispatcher::into_dyn` pattern.
- Heterogenous collections of `Arc<dyn DynJobHandleCommon>` mixing tssrun and sbatch handles now compile (see new tests in `tests/job_handle_common.rs`).
- Crate-root re-exports: `pub use handle::{DynHandleAdapter, DynJobHandleCommon, JobHandleCommon};`. `handle::into_dyn` is **not** re-exported at the crate root because `dispatcher::into_dyn` already lives there — use `slurm_async_runner::handle::into_dyn` or aliased `use ... as into_dyn_handle;`.
- **Python**: `runtime_checkable Protocol` `JobHandleCommon` added to `python/slurm_async_runner/__init__.py`. Callers can write `from slurm_async_runner import JobHandleCommon` and `isinstance(h, JobHandleCommon)` to accept either backend.
- **Python (parity)**: `TssrunJobHandle.is_finished()` async method added so the tssrun pyo3 wrapper exposes the same surface as `SbatchJobHandle.is_finished()` and satisfies the Protocol. `.pyi` updated.

### Phase 3 P3 — `JobHandleCommon` cross-backend trait

- New module `crate::handle` exposing the `JobHandleCommon` trait — the Phase 2 §7.1 naming convergence (5 sync getters + `snapshot` / `watch` + async `refresh` / `wait_terminal`) is now a mechanically-checkable contract on the public crate API.
- `SbatchJobHandle` and `TssrunJobHandle` both implement `JobHandleCommon`, each binding its own `Snapshot` associated type — no boxing, no JSON flattening on the hot path.
- The trait is **not** dyn-safe by design (associated type). Phase 3 P4 will introduce the type-erased `DynJobHandleCommon` companion + `into_dyn` constructor for callers that need `dyn` dispatch.
- New integration test `tests/job_handle_common.rs` exercises a single generic contract (`assert_common_contract<H: JobHandleCommon>`) against both backends — adding a future backend means adding one fixture + one line to the contract test.
- Crate-root re-export: `pub use handle::JobHandleCommon;` so downstream callers can write `use slurm_async_runner::JobHandleCommon;`.

### Phase 3 P2 — tssrun handle async parity with sbatch

- **`TssrunJobHandle::refresh()`** now returns `anyhow::Result<TssrunJobSnapshot>` instead of `anyhow::Result<()>`. Existing callers using `let _ = handle.refresh().await?;` continue to compile (Rust does not warn on a discarded `Ok(T)`).
- **`TssrunJobHandle::wait_terminal(poll_interval)`** added, mirroring `SbatchJobHandle::wait_terminal`. Takes `&self` (not `self`) — tssrun handle ergonomics keep allowing post-wait reuse, and there is no `Drop`-warn pattern that would justify consuming `self`.
- **Python**: `PyTssrunJobHandle.wait_terminal(poll_interval_secs)` exposed via pyo3, returning `Awaitable[None]`. The Python `refresh` contract is unchanged (still returns `None` — read the broadcast snapshot via the existing getters).
- Why: Phase 3 P3 will add `impl JobHandleCommon for TssrunJobHandle`; this P2 is the additive method-shape change so P3 is purely the trait wiring.

### Phase 3 P1 — tssrun naming symmetry

- **Breaking (with alias)**: `tssrun::JobHandle` and `tssrun::JobHandleSnapshot` are renamed to `TssrunJobHandle` and `TssrunJobSnapshot` for naming symmetry with `SbatchJobHandle` / `SbatchJobSnapshot`. Deprecated `pub type` aliases preserve compilation; downstream callers can migrate at their leisure.
- Crate-root re-exports (`crate::JobHandle`, `crate::JobHandleSnapshot`) remain available via `#[allow(deprecated)]` re-export.
- Python pyo3 binding names (`TssrunJobHandle`) are unchanged — already named symmetrically.
- Why: Phase 3 P3 introduces a `JobHandleCommon` trait; symmetric Rust struct naming makes the trait docs and impls read cleanly.

### Phase 2 P6

- **`SbatchManager::run()`** — submit a single job and block until terminal state, then return `FinishedInfo`. Rejects array submissions early with `SbatchRunError::ArrayNotSupported` (mapped to Python `ValueError`). Polling cadence defaults to 30 s; override via `with_poll_interval` (tests use 1–10 ms).
- **`SbatchManager::cancel(jobid)`** — send `scancel <jobid>` via the existing `JobDispatcher::capture` seam. Idempotent on the SLURM side. Non-zero exit surfaces as `SbatchCancelError::Scancel { exit_code, stdout }`.
- **`SbatchRunError`** — typed errors for the run pipeline: `Spawn`, `Wait(anyhow::Error)`, `Sacct(anyhow::Error)`, `MissingFinished`, `JobFailed { state, exit_code }`, `ArrayNotSupported`. `#[non_exhaustive]`.
- **`SbatchCancelError`** — `Scancel { exit_code, stdout }` and `Other(anyhow::Error)`. `#[non_exhaustive]`.
- **Python**: `SbatchManager.run` / `SbatchManager.cancel` async methods; new `FinishedInfo` pyclass with `final_state` / `exit_code` / `finished_at` getters.

### Notes (Phase 2 P6)

- `run()` does not use `sbatch --wait`. The poll-based design avoids orphan-on-disconnect risk on KUDPC and preserves Phase 1's snapshot-permanence invariant. See spec §6.0 for the four-bullet justification.
- `cancel()` does not pre-check local `is_finished()` state. SLURM's own `scancel` is idempotent for terminal jobs; pre-checking would race against external state changes.
- Spec §6.5 (Drop auto-cancel + `tracing::warn!`) is explicitly NOT in P6. `SbatchJobHandle::Drop` is left unchanged; the warning is a Phase 3 add-on.
- The scancel-binary-swap Python smoke test is skipped pending a `scancel_bin` override on `SbatchManager`; tracked as Phase 3.
- spec §6.1 sketched `run(&self, cmd: SbatchCmd)`; the implementation follows the established `SbatchManager::spawn(&self)` pattern (`run(&self)`, no `cmd` parameter) for consistency.
- spec §6.2 declared `Wait(std::io::Error)`; implementation uses `Wait(anyhow::Error)` to match the actual `wait_terminal` return type.
- **`JobState` accessor naming**: plan referenced `JobState::as_slurm_str`, but the actual method on `JobState` (in `src/entities/slurm/status.rs`) is `as_token`. Task 6 (`PyFinishedInfo.final_state`) uses `as_token` accordingly. No behavioral change — the token strings are identical.
- **Plan-literal canned-data correction in Tasks 4 + 5**: the plan's `qgroup` canned output `"QUEUE USER JOBID STATUS PROC\ngr u <jobid> CMP 1\n"` does not actually exercise the sacct path because `refresh_with_sacct` early-returns when `qgroup` reports a terminal status (`left_active_listing` stays `false`). Tasks 4 + 5 corrected this to an **empty `qgroup` output** so `left_active_listing=true` flips and the sacct dispatch fires, allowing the canned `sacct` row to populate `FinishedInfo`. End-state assertions are unchanged.

### Added (Phase 2 P5)

- **`SbatchCmd::array_spec: Option<SlurmArraySpec>`** — wires SLURM
  `--array` (`-a`). Reuses `crate::entities::slurm::SlurmArraySpec`. Python:
  `PySbatchCmd(..., array_spec=SlurmArraySpec.parse("0-3"))`.
- **`SbatchJobSnapshot::{array_jobid, array_task_id}`** — two new
  `#[serde(default)] Option<...>` fields persisted in the snapshot JSON.
  `None` for single jobs; `Some(master)` / `Some(idx)` for array tasks.
  Legacy snapshots without these fields decode to `None`.
- **`SbatchManager::spawn_array(array_spec)`** — submits a single
  `sbatch --array=<spec>` invocation, parses the master jobid, and
  persists one snapshot per task. Returns `Vec<SbatchJobHandle>` in
  declaration order.
- **`SbatchManager::attach_array_jobid(master_jobid)`** — returns
  `Vec<SbatchJobHandle>` for every task snapshot of an array submission,
  sorted by `array_task_id` ascending.
- **`SbatchAttachError`** — new typed enum replacing `anyhow::Error` on
  attach paths. Variants: `NotFound { key }`,
  `KindMismatch { expected, got }`, `MultipleMatch { jobid, count }`,
  `Io(#[from] anyhow::Error)`. `attach_jobid` on an array master jobid
  now returns `MultipleMatch` instead of silently resolving to one task.
- **`resolve_log_path` extended tokens** — `%A` (master jobid alias),
  `%a` (array task index), `%u` (`USER` env), `%N` (`HOSTNAME` env,
  spawn-time best-effort). Existing `%j` / `%x` preserved byte-for-byte.
- **`expand_array_indices(&SlurmArraySpec) -> Vec<u32>`** — enumerates
  every task index in a spec, used internally by `spawn_array`.
- **`JobStateStore::find_all_by_jobid(jobid) -> Result<Vec<S>>`** — new
  default-impl trait method.
- **`PySbatchJobHandle.array_jobid` / `array_task_id` getters**.

### Notes

- Array-task-aware `refresh()` (per-task `squeue -j <master>_<idx>`
  filter) is **deferred to Phase 3**. In P5 each
  `SbatchJobHandle.refresh()` on an array-task handle still queries
  by master jobid, so the observed state reflects the master summary
  rather than the specific task. Per-task log read works correctly
  because `resolve_log_path` expands `%a`.

### Added (Phase 2 P4)

- **`crate::entities::slurm::SlurmSignalSpec`** + **`SignalIdent`** — new
  entity modeling SLURM's `--signal=[R:]<sig_num|sig_name>[@<sig_time>]` BNF.
  `FromStr` accepts: `"USR1"`, `"15"`, `"USR1@60"`, `"R:USR1"`,
  `"R:SIGTERM@30"`, `"R:9@5"`. Rejects: empty, lowercase `r:`,
  signal number outside `1..=64`, seconds zero, seconds above `u16::MAX`,
  empty signal, signal names with non-uppercase/non-digit/non-underscore
  characters. `Display` round-trips with `FromStr`. `serde::Serialize` /
  `Deserialize` via the string form.
- **`SbatchCmd::signal: Option<SlurmSignalSpec>`** — emits
  `["--signal", spec.to_string()]` between `--mail-type` and `--no-requeue`.
  Python:
  `PySbatchCmd(..., signal=SlurmSignalSpec.parse("USR1@60"))`.
- **`PySlurmSignalSpec`** pyo3 wrapper at
  `slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options.SlurmSignalSpec`
  exposes `parse(s)` static method, `__str__`, and getters
  `allow_resignal`, `signal` (rendered Display form), `seconds_before_end`.

### Added (Phase 2 P3)

- **`SbatchSpawnError::InvalidExportKey { key }`** and
  **`SbatchSpawnError::InvalidExportValue { key, value }`** —
  `SbatchCmd::build_argv()` now rejects any `env` entry whose key or value
  contains `,` or `=` (SLURM's in-band separators on the `--export` payload).
  Valid pairs round-trip unchanged. Python: `cmd.build_argv()` raises
  `RuntimeError` whose message contains the offending key (and value, for
  value errors).
- **`SbatchCmd::build_argv` return type** is now
  `Result<Vec<String>, SbatchSpawnError>` (was `anyhow::Result<Vec<String>>`).
  Absolutize/I-O errors flow through the existing
  `SbatchSpawnError::Other(#[from] anyhow::Error)` variant, so external
  callers using `?` against `SbatchSpawnError` are unaffected.

### Added (Phase 2 P2)

- **`SbatchCmd::dependency: Option<SlurmDependency>`** — emits `["-d", dep.to_string()]`.
  Reuses `crate::entities::slurm::SlurmDependency` (already implements `FromStr` /
  `Display` for `afterok:200`, `afterok:200,afterany:201`, `afterok:200?afterany:201`,
  `singleton`, etc.). Python:
  `PySbatchCmd(..., dependency=SlurmDependency.parse("afterok:200"))`.
- **`SbatchCmd::mail_user: Option<MailAddress>`** — emits `["--mail-user", addr]`.
  `MailAddress` is the `String` alias from
  `crate::entities::slurm::sbatch_options`. Python:
  `PySbatchCmd(..., mail_user="alice@example.com")`.
- **`SbatchCmd::mail_types: Option<MailTypeInput>`** — emits
  `["--mail-type", types.to_string()]` in canonical comma-separated form
  (`BEGIN,END,FAIL,REQUEUE,ALL`). Python:
  `PySbatchCmd(..., mail_types=MailTypeInput.parse("BEGIN,END"))`.
- **`MailType::as_slurm_str(self) -> &'static str`** plus
  **`impl Display for MailType`** and **`impl Display for MailTypeInput`** in
  `entities::slurm::sbatch_options` — required for round-tripping the
  comma-separated `--mail-type` value.

### Added (Phase 2 P1)

- **sacct `ExitCode` parser.** `SbatchJobHandle::refresh_with_sacct()` now
  populates `FinishedInfo::exit_code` (and the `exit_code()` getter on
  `SbatchLifecycle` / `SbatchJobSnapshot` / `SbatchJobHandle`). Sacct is
  still opt-in. See `parse_sacct_exit_code` in `src/sbatch/parse.rs` and
  the new `query_job_states_with_exit_code_with` in `src/runner.rs`
  (the legacy `query_job_states_batch_with` is unchanged for backward compat).
- **`SbatchCmd::no_requeue: bool`** — emits `--no-requeue` when `true`.
  Python: `PySbatchCmd(..., no_requeue=True)`.
- **`SbatchCmd::comment: Option<String>`** — emits `--comment <value>`
  when `Some`. Python: `PySbatchCmd(..., comment="...")`.
- **`SbatchJobHandle::log_lines` / `read_log_to_end`** — read job
  stdout/stderr via `LogStream { Stdout, Stderr }`. Missing files return
  empty (`Ok(vec![])` / `Ok(String::new())`); template missing returns
  `LogReadError::PathNotResolved`; other I/O via `LogReadError::Io`.
  Python: `PySbatchJobHandle.log_lines(stream: int, n: int)` and
  `read_log_to_end(stream: int)` (`stream`: 0 = stdout, 1 = stderr).

### Refactor (Phase 2 P1)

- **DRY: `absolutize` consolidated to `src/util/path.rs`.** The duplicate
  `fn absolutize` in `src/sbatch/cmd.rs` and `src/tssrun/cmd.rs`, plus
  the inline `std::path::absolute` use in `src/manager.rs`, all now go
  through `crate::util::path::absolutize`. No public API change.

### Docs (Phase 2 P1)

- Removed Phase 1 "this returns None until Phase 2" limitation notes from
  `exit_code` doc-comments on `SbatchLifecycle`, `SbatchJobSnapshot`, and
  `SbatchJobHandle`, and from the corresponding Python `.pyi` docstrings.

### Changed (BREAKING)

- **PR #5: Slurm vocab migration with single-owner pyclass rule.** The
  `tssrun` Resource type and the broader Slurm vocabulary now live in
  this crate's `entities::slurm` module. Concretely:
  - Rust: `Resource` is **deleted**. Use [`ResourceSpec`](src/entities/slurm)
    (CPU / GPU enum with `Option<NonZeroU32>` slots — partial CPU specs
    like `p=60:t=1:c=1` are accepted per the KUDPC manual).
    `TssrunCmd::queue` is renamed to `partition` (matching Slurm's
    `--partition` flag). `TssrunCmd::time_limit` is retyped from
    `Option<String>` to `Option<JobTimeLimit>` (canonical `HH:MM:SS`
    `Display`). `TssrunCmd::rsc` is retyped from `Option<Resource>` to
    `Option<ResourceSpec>`. New crate-root re-exports: `JobPartition`,
    `JobTimeLimit`, `Memory`, `MemoryUnit`, `ResourceSpec`,
    `ResourceSpecCPU`, `ResourceSpecGPU`.
  - Python: `Resource(...)` is **deleted**. Use
    `ResourceSpec(processes=..., memory=Memory("2G"), ...)` or the GPU
    form `ResourceSpec(gpus=1)`. `TssrunCmd(queue=...)` becomes
    `TssrunCmd(partition=...)`; `time_limit=` requires a
    `JobTimeLimit("1:00:00")` value (no implicit string coercion);
    `rsc=` requires a `ResourceSpec(...)` value. `Memory` requires
    explicit wrapping — pass `Memory("2G")` or
    `Memory.from_value(2, MemoryUnit.Giga)`. `ResourceSpec` and
    `JobTimeLimit` are re-exported from the `tssrun` submodule for
    one-stop import; `Memory` lives in
    `entities.slurm.sbatch_options`.
  - **Pymodule entry rename.** Both crates renamed their pymodule entry
    from `_core` to `_<package>_core` to avoid `PyInit__core`
    duplicate-symbol clashes when both are linked into one process.
    Python imports change accordingly:
    `slurm_async_runner._core.*` → `slurm_async_runner._slurm_async_runner_core.*`,
    `gaussian_job_shared._core.*` → `gaussian_job_shared._gaussian_job_shared_core.*`.
  - **`JobStatus` is now owned by this crate** (single-owner rule for
    pyclass wrappers). The Python type lives at
    `slurm_async_runner._slurm_async_runner_core.entities.slurm.status.JobStatus`.
    The `gaussian_job_shared._gaussian_job_shared_core.entities.slurm.status`
    location is no longer authoritative for in-process pyclass identity.

### Added

- **`slurm_async_runner::tssrun` Rust module and `slurm_async_runner._slurm_async_runner_core.tssrun`
  Python submodule.** Provides `TssrunCmd` (typed `tssrun` argv builder),
  `TssrunManager` (background spawn / attach / state query), `JobHandle`
  with watch-based snapshot, and a pluggable `JobLogSink` trait
  (`Null/Std/InMemory/FileLogSink`).
- **`BackgroundDispatcher` trait + `TokioBackgroundDispatcher`** for
  non-blocking child spawn alongside the existing synchronous
  `JobDispatcher`.
- **Rust port of `slurm-async-runner`.** Initial Rust + pyo3 implementation
  replacing the pure-Python prototype, with a Python-compatible async API
  exposed through `pyo3-async-runtimes` (tokio runtime).
- **`SlurmCmd` / `SlurmManager`** spec types in `src/manager.rs`. Pure data +
  argv builders; no I/O. Both are exported from the crate root.
- **`JobDispatcher` trait** in `src/dispatcher.rs` with two shipped impls:
  `TokioDispatcher` (production, `tokio::process::Command`) and
  `DryRunDispatcher` (echo-only). Custom impls can be plugged in via
  `SlurmManager::run_job_with` / `query_job_states_batch_with`.
- **`runner::query_job_states_batch`** — async `squeue` then `sacct` fallback
  parser. Returns `HashMap<u64, JobStatus>` carrying both state and reason.
  `query_job_states_batch_with<D: JobDispatcher>` exposes the dispatcher
  parameter for tests.
- **Re-exports** of `JobStatus`, `JobState`, `JobReason` from
  [`gaussian_job_shared`](https://github.com/kkiyama117/gaussian_job_shared)
  so downstream Rust callers don't need a direct dependency.
- **Python sub-modules** `slurm_async_runner._slurm_async_runner_core.manager` (`SlurmCmd`,
  `SlurmManager`) and `slurm_async_runner._slurm_async_runner_core.runner`
  (`query_job_states_batch`). Async pyo3 returns native Python coroutines.
- **Hand-written `.pyi` stubs** for the new submodules at
  `python/slurm_async_runner/_slurm_async_runner_core/manager.pyi` and
  `runner.pyi` (the auto-generated `__init__.pyi` only covers sync,
  top-level pyfunctions).
- **CI: cargo + Python pipeline** at `.github/workflows/test.yml`. Runs
  `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test --lib`,
  `maturin develop`, `pytest`, and `ruff` on every push and PR.
- New `crate::sbatch` module: SBATCH-based job submission with KUDPC-aware
  polling (`qgroup -l` → `squeue` fallback, opt-in sacct via
  `refresh_with_sacct`). See `docs/superpowers/specs/2026-05-10-sbatch-module-design.md`.
- Generic store layer: `JobSnapshot` trait + `JobStateStore<S>` + `InMemoryStateStore<S>`
  + `FileSystemStateStore<S>`. Both `tssrun` and `sbatch` use it; on-disk JSON
  files now include a top-level `"kind"` discriminator.
- Dyn-compatible dispatcher facade: `DynJobDispatcher` trait + `DynDispatcherAdapter`
  newtype + `DynView` borrow adapter + `into_dyn(d)` helper. Required because the
  base `JobDispatcher` trait uses RPITIT (return-position impl Trait in trait) and is
  not directly dyn-compatible. `into_dyn` is the canonical entry point for callers
  who need `Arc<dyn DynJobDispatcher>` (e.g. `SbatchManager::with_dispatcher`).

### Changed (BREAKING)

- `tssrun::store::JobStateStore` is now `JobStateStore<JobHandleSnapshot>`
  (parametrized). Callers using `Arc<dyn tssrun::store::JobStateStore>` must
  switch to `Arc<dyn JobStateStore<JobHandleSnapshot>>` (re-exported from
  `crate::store`).
- On-disk JSON files written by `FileSystemStateStore` now contain a
  top-level `"kind"` field (`"tssrun"` or `"sbatch"`). Files written by
  older versions are still readable (lenient legacy fallback assumes the
  store's own kind), and will gain the field on next save.
- File path layout is unchanged (`{root}/<uuid>.json`). No manual mv needed.

### Changed

- **`JobHandle::wait()` returns `Result<Option<i32>>`.** Was `Result<i32>`
  with `unwrap_or(0)` masking signal kills. Now `Ok(None)` distinguishes
  signal-killed children (e.g. SLURM time-limit kill, OOM) from clean
  exits. The `.pyi` stub and Python wrapper surface the same `int | None`.
- **`PyTssrunJobHandle` snapshot getters are lock-free.** Snapshot getters
  (`pid`, `jobid`, `node`, `sent_env`, `is_running`, `exit_code`,
  `live_env`) read from a cloned `watch::Receiver` and never lock the
  `JobHandle` mutex. `wait()` is the only method that takes the mutex,
  so concurrent polls during an in-flight `wait()` no longer block.
- **`TssrunManager` fields are now `pub(crate)`.** Use the
  `with_state_dir` / `with_log_sink` builders instead of mutating fields
  directly — mid-flight mutation isn't retroactive on running handles.
- **No shell wrapping.** The previous Python prototype wrapped `srun` in
  `$SHELL -c "..."`; this port spawns argv directly via
  `tokio::process::Command::args`. Both Python's
  `asyncio.create_subprocess_exec` and Rust's `Command::args` accept argv
  lists, so the shell layer is unnecessary.
- **`SlurmManager` spec / runtime split.** Eliminated `default_shell` /
  `build_shell_str` / `srun_command`. New shape: `SlurmManager` holds a
  `SlurmCmd`, builds argv via `build_argv`, and delegates execution to a
  `JobDispatcher`.

### Notes

- **Note (superseded by PR #5).** Earlier drafts pinned
  `gaussian_job_shared` with `default-features = false` to avoid a
  `PyInit__core` duplicate-symbol clash. PR #5 supersedes that workaround:
  the Slurm vocab (`JobStatus`, `JobState`, `JobReason`, `ResourceSpec`,
  `JobTimeLimit`, `Memory`, ...) is now owned in-tree under
  `entities::slurm`, so this crate no longer depends on
  `gaussian_job_shared` at all. Each pyclass type has exactly one owner
  cdylib in the dependency graph (Pyclass Single Owner rule — see
  `docs/superpowers/specs/2026-05-10-slurm-vocab-migration-and-pyclass-ownership-design.md`).
- All 35 Rust unit tests run without a real SLURM cluster — the test suite
  substitutes the coreutils `true` / `false` / `echo` binaries through
  `SlurmCmd::new(...)`, plus a `MockDispatcher` for argv-plumbing assertions.

[Unreleased]: https://github.com/kkiyama117/slurm-async-runner/compare/v3.1.0...HEAD
[v3.1.0]: https://github.com/kkiyama117/slurm-async-runner/compare/v3.0.0...v3.1.0
[v3.0.0]: https://github.com/kkiyama117/slurm-async-runner/compare/v2.0.0...v3.0.0
[v2.0.0]: https://github.com/kkiyama117/slurm-async-runner/compare/v1.1.0...v2.0.0
[v1.1.0]: https://github.com/kkiyama117/slurm-async-runner/compare/v1.0.0...v1.1.0
[v1.0.0]: https://github.com/kkiyama117/slurm-async-runner/releases/tag/v1.0.0
