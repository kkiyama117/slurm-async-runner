# sbatch Phase 2 P5 — `--array` array jobs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Submit a single `sbatch --array=<spec>` invocation, persist N per-task snapshots (one per array index), and let callers attach to all tasks of a master jobid as `Vec<SbatchJobHandle>`. The `kind = "sbatch"` discriminator stays unchanged; array tasks are distinguished by the new `array_task_id: Some(idx)` field on `SbatchJobSnapshot`.

**Architecture:** Three additive changes layered on the Phase 1 store + handle:
1. **Snapshot model** gains two `#[serde(default)] Option<...>` fields (`array_jobid`, `array_task_id`) so existing Phase 1 JSON files load unchanged.
2. **`SbatchManager::spawn_array(mut cmd, spec)`** wraps the existing single-sbatch capture path: one CLI call, one master jobid, then `expand_array_indices(spec)` enumerates per-task indices and we save one snapshot per index with the same master `jobid` but distinct `uuid` + `array_task_id`.
3. **`SbatchAttachError`** typed enum (replacing the prior `anyhow::Result` on attach) carries `MultipleMatch { jobid, count }` so `attach_jobid` on a master id surfaces a deterministic error directing callers to `attach_array_jobid`.

`resolve_log_path` is extended to accept `array_task_id: Option<u32>` so `%A`/`%a` expand on output/error templates; `%u` / `%N` use spawn-time env best-effort. Array-task-aware refresh (per-task squeue filter) is **deferred to Phase 3**; in P5 each `SbatchJobHandle.refresh()` still queries by master jobid and the snapshot stores per-task state opportunistically. This is acknowledged and tested.

**Tech Stack:** Rust 2021, `pyo3` (feature `pyo3`), `pyo3-async-runtimes` (tokio), `thiserror`, `serde`, existing `crate::entities::slurm::SlurmArraySpec`.

---

## File Structure

| File | Responsibility |
|---|---|
| `src/sbatch/handle.rs` | Add `array_jobid: Option<u64>` + `array_task_id: Option<u32>` to `SbatchJobSnapshot`; thread `array_task_id` into `output_path` / `error_path`; add getter on `SbatchJobHandle`. |
| `src/sbatch/cmd.rs` | Add `array_spec: Option<SlurmArraySpec>` field + `-a` argv emission between `chdir` and `--export`. |
| `src/sbatch/parse.rs` | Extend `resolve_log_path` signature with `array_task_id`; expand `%A`/`%a`/`%u`/`%N`. Add `expand_array_indices(spec: &SlurmArraySpec) -> Vec<u32>`. |
| `src/sbatch/error.rs` | Add `SbatchAttachError` typed enum (`NotFound`, `KindMismatch`, `MultipleMatch`, `Io`). |
| `src/store.rs` | Add default `find_all_by_jobid -> Result<Vec<S>>` to `JobStateStore` trait. |
| `src/sbatch/manager.rs` | Change `attach*` methods to return `Result<_, SbatchAttachError>`. Add `spawn_array` and `attach_array_jobid` methods. |
| `src/py_export/sbatch.rs` | Add `array_spec` kwarg to `PySbatchCmd::new`; add `spawn_array(spec)` + `attach_array_jobid(master)` methods on `PySbatchManager`; add `array_jobid` / `array_task_id` getters on `PySbatchJobHandle`. |
| `python/slurm_async_runner/_slurm_async_runner_core/sbatch.pyi` | Mirror the new kwarg and methods. |
| `python/tests/test_sbatch.py` | Smoke tests for `spawn_array` + `attach_array_jobid` + jobid disambiguation. |
| `CHANGELOG.md` | Append `### Added (Phase 2 P5)` block. |

No new files. The plan adds **≈400 LOC source + ≈250 LOC tests**.

---

## Task 1: `SbatchJobSnapshot::{array_jobid, array_task_id}` fields

**Files:**
- Modify: `src/sbatch/handle.rs:24-40` (struct definition + serde default)
- Modify: `src/sbatch/store.rs:28-42` (test fixture `snap` initializer)
- Modify: `src/sbatch/manager.rs:67-83` (snapshot literal in `spawn()`)

**Why:** Establish the persistence shape before the spawn flow uses it. The two `Option<...>` fields with `#[serde(default)]` mean Phase 1 snapshot files (which lack these fields) still load — they decode to `None`, indicating a single (non-array) job. Array tasks distinguish themselves by `array_task_id.is_some()`.

**Constraints:**
- Both fields are `Option<...>` AND have `#[serde(default)]` (spec §2.2 invariant).
- Field placement: immediately after `jobid: u64` (logically grouped — both are jobid-related).
- The `JobSnapshot::jobid()` impl at `src/sbatch/store.rs:12` stays unchanged (still `Some(self.jobid)` — for array tasks this is the master). The new `array_jobid` field is redundant in value (both hold the master) but explicit-redundancy is spec-mandated (§5.2: "`jobid` フィールドは master を保持; `array_jobid: Some(master_jobid)`").

- [ ] **Step 1: Write the failing tests**

Append to the existing `#[cfg(test)] mod tests` block in `src/sbatch/store.rs`:

```rust
    #[tokio::test]
    async fn array_task_fields_roundtrip_via_fs_store() {
        let tmp = tempfile::tempdir().unwrap();
        let store: FileSystemStateStore<SbatchJobSnapshot> = FileSystemStateStore::new(tmp.path());
        let mut s = snap(12345);
        s.array_jobid = Some(12345);
        s.array_task_id = Some(7);
        store.save(&s).await.unwrap();
        let loaded = store.load(s.uuid).await.unwrap().unwrap();
        assert_eq!(loaded.array_jobid, Some(12345));
        assert_eq!(loaded.array_task_id, Some(7));
    }

    #[tokio::test]
    async fn array_task_fields_default_to_none_for_legacy_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        let store: FileSystemStateStore<SbatchJobSnapshot> = FileSystemStateStore::new(tmp.path());
        let s = snap(42);
        store.save(&s).await.unwrap();
        let loaded = store.load(s.uuid).await.unwrap().unwrap();
        assert_eq!(loaded.array_jobid, None);
        assert_eq!(loaded.array_task_id, None);
    }
```

- [ ] **Step 2: Run the failing test**

Run: `cargo test --lib --features pyo3 array_task_fields_roundtrip_via_fs_store 2>&1 | tail -10`

Expected: FAIL with `error[E0609]: no field 'array_jobid' on type 'SbatchJobSnapshot'`.

- [ ] **Step 3: Add the two fields to `SbatchJobSnapshot`**

Edit `src/sbatch/handle.rs`. Insert two new fields immediately after `pub jobid: u64,`:

```rust
    pub uuid: Uuid,
    pub jobid: u64,

    /// Master jobid of the array submission (the `<N>` from `Submitted batch
    /// job <N>` when `--array=...` was passed). `None` for single (non-array)
    /// jobs. For array tasks this is redundant with [`Self::jobid`] (both
    /// hold the master); the explicit field makes attach paths able to
    /// distinguish array tasks from singles without inspecting
    /// `array_task_id`.
    #[serde(default)]
    pub array_jobid: Option<u64>,

    /// Per-task index within the array (e.g. `0`, `1`, `4` for `-a 0-1,4`).
    /// `None` for single (non-array) jobs. Array task identity is
    /// `(array_jobid, array_task_id)` — SLURM also prints this as
    /// `<master>_<idx>` in `squeue -t`.
    #[serde(default)]
    pub array_task_id: Option<u32>,

    pub argv: Vec<String>,
    // ... rest unchanged ...
```

- [ ] **Step 4: Update the `snap()` test helper in `src/sbatch/store.rs`**

The current helper constructs a snapshot literal. Add the two new fields with `None` defaults immediately after `jobid,`:

```rust
    fn snap(jobid: u64) -> SbatchJobSnapshot {
        SbatchJobSnapshot {
            uuid: Uuid::now_v7(),
            jobid,
            array_jobid: None,
            array_task_id: None,
            argv: vec!["sbatch".into()],
            sent_env: HashMap::new(),
            script_path: PathBuf::from("/w/job.sh"),
            chdir: None,
            partition: None,
            job_name: None,
            submitted_at: chrono::Utc::now(),
            log: LogPathSpec::default(),
            lifecycle: SbatchLifecycle::default(),
        }
    }
```

- [ ] **Step 5: Update the `spawn()` snapshot literal in `src/sbatch/manager.rs`**

Locate the `SbatchJobSnapshot { ... }` literal in `spawn()` (around lines 67-83). Add `array_jobid: None, array_task_id: None,` immediately after `jobid,`:

```rust
        let snapshot = SbatchJobSnapshot {
            uuid,
            jobid,
            array_jobid: None,
            array_task_id: None,
            argv,
            sent_env: self.cmd.env.clone(),
            script_path,
            chdir: self.cmd.chdir.clone(),
            partition: self.cmd.partition.clone(),
            job_name: self.cmd.job_name.clone(),
            submitted_at: Utc::now(),
            log: LogPathSpec {
                output_template: self.cmd.output.clone(),
                error_template: self.cmd.error.clone(),
            },
            lifecycle: SbatchLifecycle::default(),
        };
```

If any OTHER `SbatchJobSnapshot { ... }` struct literal exists in the codebase (search `grep -rn 'SbatchJobSnapshot {' src/` excluding the type definition), update it identically. Field order stays — only the two new fields are added.

- [ ] **Step 6: Run tests**

```bash
cargo test --lib --features pyo3 -- array_task_fields 2>&1 | tail -10
cargo test --lib --features pyo3 2>&1 | tail -10
```

Expected: 2 new tests pass; ALL existing tests pass (no compile failures from missing field defaults).

- [ ] **Step 7: Run lints**

```bash
cargo clippy --all-targets --features pyo3 -- -D warnings 2>&1 | tail -10
cargo fmt --all --check
```

Expected: 0 warnings, no diff.

- [ ] **Step 8: Commit**

```bash
git add src/sbatch/handle.rs src/sbatch/store.rs src/sbatch/manager.rs
git commit -m "feat(sbatch): add array_jobid/array_task_id to SbatchJobSnapshot"
```

---

## Task 2: `SbatchCmd::array_spec` field + `-a` argv emission

**Files:**
- Modify: `src/sbatch/cmd.rs`

**Why:** Wire SLURM `--array` / `-a` into argv. `SlurmArraySpec` already lives in `entities::slurm::sbatch_options::array_spec` with `FromStr` + `Display` — we import only.

**Constraints:**
- Field placement: insert `array_spec` **immediately after `chdir`** so spec-shaped fields cluster.
- Argv emission order: place `-a` emission **immediately after `--chdir`** and **before** the `--export` block. SLURM accepts these in any order, but grouping submission-shape flags early reads cleanly.
- `SlurmArraySpec::Display` already produces the canonical form (`0-7`, `0,2,4`, `0-15:2`, etc.). No `expand()` is required here.

- [ ] **Step 1: Write the failing tests**

Append to the existing `#[cfg(test)] mod tests` block in `src/sbatch/cmd.rs` (after the existing `signal_*` tests from P4):

```rust
    #[test]
    fn array_spec_emits_dash_a_with_display_form() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.array_spec = Some("0-3".parse().unwrap());
        let argv = cmd.build_argv().unwrap();
        let i = argv.iter().position(|a| a == "-a").expect("-a present");
        assert_eq!(argv[i + 1], "0-3");
    }

    #[test]
    fn array_spec_with_max_concurrent_renders_percent_form() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.array_spec = Some("0-7%2".parse().unwrap());
        let argv = cmd.build_argv().unwrap();
        let i = argv.iter().position(|a| a == "-a").expect("-a present");
        assert_eq!(argv[i + 1], "0-7%2");
    }

    #[test]
    fn array_spec_with_step_renders_colon_form() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.array_spec = Some("0-15:4".parse().unwrap());
        let argv = cmd.build_argv().unwrap();
        let i = argv.iter().position(|a| a == "-a").expect("-a present");
        assert_eq!(argv[i + 1], "0-15:4");
    }

    #[test]
    fn array_spec_omitted_when_none() {
        let cmd = SbatchCmd::new("/w/job.sh");
        let argv = cmd.build_argv().unwrap();
        assert!(!argv.iter().any(|a| a == "-a"));
    }

    #[test]
    fn array_spec_emits_after_chdir_and_before_export() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.chdir = Some(PathBuf::from("/work"));
        cmd.array_spec = Some("0-3".parse().unwrap());
        cmd.env.insert("FOO".to_string(), "bar".to_string());
        let argv = cmd.build_argv().unwrap();
        let chdir_idx = argv.iter().position(|a| a == "--chdir").unwrap();
        let array_idx = argv.iter().position(|a| a == "-a").unwrap();
        let export_idx = argv.iter().position(|a| a.starts_with("--export=")).unwrap();
        assert!(
            chdir_idx < array_idx && array_idx < export_idx,
            "expected chdir < -a < --export, got argv={argv:?}"
        );
    }
```

- [ ] **Step 2: Run the failing test**

Run: `cargo test --lib --features pyo3 array_spec_emits_dash_a_with_display_form 2>&1 | tail -10`

Expected: FAIL with `error[E0609]: no field 'array_spec' on type 'SbatchCmd'`.

- [ ] **Step 3: Add the `array_spec` field**

Edit `src/sbatch/cmd.rs`. Extend the `use crate::entities::slurm::{...}` import:

```rust
use crate::entities::slurm::{
    JobPartition, JobTimeLimit, MailAddress, MailTypeInput, ResourceSpec, SlurmArraySpec,
    SlurmDependency, SlurmSignalSpec,
};
```

In the `SbatchCmd` struct, find `pub chdir: Option<PathBuf>,`. Insert `array_spec` immediately after it (and before `dependency`):

```rust
    pub chdir: Option<PathBuf>,

    /// `--array` (`-a`) spec. When `Some`, emitted as `["-a", spec.to_string()]`
    /// (e.g. `["-a", "0-7%2"]`). Use [`crate::sbatch::manager::SbatchManager::spawn_array`]
    /// to submit array jobs; direct `spawn()` with `array_spec.is_some()` is
    /// permitted (single sbatch invocation, one snapshot for the master
    /// jobid), but the caller will only receive ONE handle pointing at the
    /// master snapshot rather than per-task handles.
    pub array_spec: Option<SlurmArraySpec>,

    pub dependency: Option<SlurmDependency>,
```

Add the default in `SbatchCmd::new()` immediately after `chdir: None,`:

```rust
            chdir: None,
            array_spec: None,
            dependency: None,
```

- [ ] **Step 4: Add the argv emission**

In `build_argv()`, locate the `--chdir` block:

```rust
        if let Some(c) = &self.chdir {
            argv.push("--chdir".to_string());
            argv.push(absolutize(c)?);
        }
```

Insert the array_spec emission IMMEDIATELY AFTER:

```rust
        if let Some(c) = &self.chdir {
            argv.push("--chdir".to_string());
            argv.push(absolutize(c)?);
        }
        if let Some(a) = &self.array_spec {
            argv.push("-a".to_string());
            argv.push(a.to_string());
        }
```

- [ ] **Step 5: Run tests**

```bash
cargo test --lib --features pyo3 -- array_spec 2>&1 | tail -15
cargo test --lib --features pyo3 sbatch::cmd 2>&1 | tail -5
cargo test --lib --features pyo3 full_flags_cpu_variant_argv_layout -- --exact 2>&1 | tail -5
```

Expected: 5 new array_spec tests pass; all sbatch::cmd tests pass; `full_flags_cpu_variant_argv_layout` byte-identical.

- [ ] **Step 6: Run lints**

```bash
cargo clippy --all-targets --features pyo3 -- -D warnings 2>&1 | tail -10
cargo fmt --all --check
```

Expected: 0 warnings, no diff.

- [ ] **Step 7: Commit**

```bash
git add src/sbatch/cmd.rs
git commit -m "feat(sbatch): wire --array via SlurmArraySpec entity"
```

---

## Task 3: `resolve_log_path` extension for `%A`/`%a`/`%u`/`%N`

**Files:**
- Modify: `src/sbatch/parse.rs:29-42` (function signature + body)
- Modify: `src/sbatch/parse.rs:107-131` (test cases)
- Modify: `src/sbatch/handle.rs:86-99` (`SbatchJobSnapshot::{output_path, error_path}` callers)

**Why:** With array tasks, log templates often contain `%A` (master jobid) and `%a` (task index). Spec §5.4 also requires `%u` (USER) and `%N` (HOSTNAME) expansion. The signature gains one parameter (`array_task_id: Option<u32>`); existing single-job callers pass `None` and the behavior for `%j` / `%x` is preserved byte-for-byte.

**Constraints:**
- `%j` resolves to jobid (kept).
- `%A` resolves to the same jobid (master jobid alias).
- `%a` resolves to `array_task_id` when `Some`; when `None`, the literal `%a` token is preserved.
- `%u` resolves to `USER` env (empty string if unset).
- `%N` resolves to `HOSTNAME` env (empty string if unset). Spawn-time best-effort.
- Width-modified tokens (`%5j`) or unknown tokens (`%t`) stay raw.

- [ ] **Step 1: Write the failing tests**

Append to the existing `#[cfg(test)] mod tests` block in `src/sbatch/parse.rs` (after `resolve_leaves_unsupported_tokens_raw`):

```rust
    #[test]
    fn resolve_substitutes_master_jobid_via_capital_a() {
        let p = resolve_log_path("slurm-%A.out", 12345, None, None);
        assert_eq!(p, PathBuf::from("slurm-12345.out"));
    }

    #[test]
    fn resolve_substitutes_array_task_id_via_lowercase_a() {
        let p = resolve_log_path("slurm-%A_%a.out", 12345, Some(7), None);
        assert_eq!(p, PathBuf::from("slurm-12345_7.out"));
    }

    #[test]
    fn resolve_leaves_array_task_id_token_when_none() {
        let p = resolve_log_path("slurm-%A_%a.out", 12345, None, None);
        assert_eq!(p, PathBuf::from("slurm-12345_%a.out"));
    }

    #[test]
    fn resolve_substitutes_user_env() {
        let prev = std::env::var("USER").ok();
        // SAFETY: single-threaded test, no other threads observing env.
        unsafe { std::env::set_var("USER", "alice"); }
        let p = resolve_log_path("/home/%u/out-%j.log", 999, None, None);
        assert_eq!(p, PathBuf::from("/home/alice/out-999.log"));
        // SAFETY: restore previous USER.
        match prev {
            Some(v) => unsafe { std::env::set_var("USER", v) },
            None => unsafe { std::env::remove_var("USER") },
        }
    }

    #[test]
    fn resolve_substitutes_hostname_env() {
        let prev = std::env::var("HOSTNAME").ok();
        // SAFETY: single-threaded test.
        unsafe { std::env::set_var("HOSTNAME", "loginnode"); }
        let p = resolve_log_path("%N-%j.out", 42, None, None);
        assert_eq!(p, PathBuf::from("loginnode-42.out"));
        match prev {
            Some(v) => unsafe { std::env::set_var("HOSTNAME", v) },
            None => unsafe { std::env::remove_var("HOSTNAME") },
        }
    }
```

Update the existing tests to pass `None` for the new `array_task_id` parameter. Replace these three:

```rust
    #[test]
    fn resolve_substitutes_jobid_only() {
        let p = resolve_log_path("slurm-%j.out", 12345, None, None);
        assert_eq!(p, PathBuf::from("slurm-12345.out"));
    }

    #[test]
    fn resolve_substitutes_jobname_when_some() {
        let p = resolve_log_path("%x-%j.out", 12345, None, Some("g09run"));
        assert_eq!(p, PathBuf::from("g09run-12345.out"));
    }

    #[test]
    fn resolve_leaves_jobname_token_when_none() {
        let p = resolve_log_path("%x-%j.out", 12345, None, None);
        assert_eq!(p, PathBuf::from("%x-12345.out"));
    }
```

And REPLACE the current `resolve_leaves_unsupported_tokens_raw` test with two split tests:

```rust
    #[test]
    fn resolve_leaves_unsupported_array_token_when_none() {
        // %a stays raw when array_task_id is None; %A still expands.
        let p = resolve_log_path("%A_%a-%j.out", 999, None, Some("nm"));
        assert_eq!(p, PathBuf::from("999_%a-999.out"));
    }

    #[test]
    fn resolve_leaves_truly_unsupported_tokens_raw() {
        let p = resolve_log_path("%5j-%t-%j.out", 999, None, None);
        assert_eq!(p, PathBuf::from("%5j-%t-999.out"));
    }
```

- [ ] **Step 2: Run the failing tests**

Run: `cargo test --lib --features pyo3 resolve_substitutes_master_jobid_via_capital_a 2>&1 | tail -10`

Expected: FAIL — either parameter-arity compile error (`resolve_log_path` takes 3 args, test passes 4) or new tokens don't substitute.

- [ ] **Step 3: Change `resolve_log_path` signature + add new substitutions**

Replace the current `resolve_log_path`:

```rust
/// Lenient SLURM `-o`/`-e` template substitution.
///
/// Substitutes the following tokens:
/// - `%j` and `%A` — the jobid (`%A` is SLURM's "master jobid" alias on
///   array submissions; for single jobs the two are identical).
/// - `%x` — `job_name` if `Some`, else preserved raw.
/// - `%a` — `array_task_id` if `Some`, else preserved raw.
/// - `%u` — `USER` env var (empty string if unset).
/// - `%N` — `HOSTNAME` env var (empty string if unset). For pending
///   array tasks SLURM normally fills in the compute node name; the
///   spawn-time `HOSTNAME` is a best-effort placeholder for the login
///   node. We do NOT update `%N` retroactively.
///
/// Tokens NOT in the list above (e.g. `%5j` width modifiers, `%t` task id)
/// are preserved verbatim — caller can detect "still has unresolved
/// variables" by checking for `%` in the returned path.
pub fn resolve_log_path(
    template: &str,
    jobid: u64,
    array_task_id: Option<u32>,
    job_name: Option<&str>,
) -> PathBuf {
    let mut s = template.to_string();
    // Substitute %A first (master jobid alias) so it does not collide with %a.
    let jobid_str = jobid.to_string();
    s = s.replace("%A", &jobid_str);
    s = s.replace("%j", &jobid_str);
    if let Some(idx) = array_task_id {
        s = s.replace("%a", &idx.to_string());
    }
    if let Some(name) = job_name {
        s = s.replace("%x", name);
    }
    let user = std::env::var("USER").unwrap_or_default();
    s = s.replace("%u", &user);
    let hostname = std::env::var("HOSTNAME").unwrap_or_default();
    s = s.replace("%N", &hostname);
    PathBuf::from(s)
}
```

- [ ] **Step 4: Update the two callers in `src/sbatch/handle.rs`**

Replace `SbatchJobSnapshot::output_path` / `error_path` to pass `self.array_task_id`:

```rust
impl SbatchJobSnapshot {
    pub fn output_path(&self) -> Option<PathBuf> {
        self.log
            .output_template
            .as_deref()
            .map(|t| resolve_log_path(t, self.jobid, self.array_task_id, self.job_name.as_deref()))
    }

    pub fn error_path(&self) -> Option<PathBuf> {
        self.log
            .error_template
            .as_deref()
            .map(|t| resolve_log_path(t, self.jobid, self.array_task_id, self.job_name.as_deref()))
    }
    // ... rest unchanged ...
}
```

- [ ] **Step 5: Run tests**

```bash
cargo test --lib --features pyo3 -- resolve_ 2>&1 | tail -20
cargo test --lib --features pyo3 sbatch:: 2>&1 | tail -5
```

Expected: all resolve_ tests pass; all sbatch::* tests pass.

- [ ] **Step 6: Run lints**

```bash
cargo clippy --all-targets --features pyo3 -- -D warnings 2>&1 | tail -10
cargo fmt --all --check
```

Expected: 0 warnings, no diff.

- [ ] **Step 7: Commit**

```bash
git add src/sbatch/parse.rs src/sbatch/handle.rs
git commit -m "feat(sbatch): resolve_log_path expands %A/%a/%u/%N tokens"
```

---

## Task 4: `expand_array_indices` helper

**Files:**
- Modify: `src/sbatch/parse.rs` (append helper + tests)

**Why:** Given a `SlurmArraySpec`, enumerate every task index it covers. Used by `spawn_array` (Task 6).

**Constraints:**
- `pub(crate)` visibility (NOT part of `entities`).
- Result in declaration order (not sorted numerically).
- `max_concurrent` is intentionally ignored.

- [ ] **Step 1: Write the failing tests**

Append to the `#[cfg(test)] mod tests` block in `src/sbatch/parse.rs`:

```rust
    // ---- expand_array_indices ----

    #[test]
    fn expand_single_value() {
        let spec: crate::entities::slurm::SlurmArraySpec = "5".parse().unwrap();
        assert_eq!(expand_array_indices(&spec), vec![5]);
    }

    #[test]
    fn expand_simple_range() {
        let spec: crate::entities::slurm::SlurmArraySpec = "0-3".parse().unwrap();
        assert_eq!(expand_array_indices(&spec), vec![0, 1, 2, 3]);
    }

    #[test]
    fn expand_stepped_range_even() {
        let spec: crate::entities::slurm::SlurmArraySpec = "0-8:2".parse().unwrap();
        assert_eq!(expand_array_indices(&spec), vec![0, 2, 4, 6, 8]);
    }

    #[test]
    fn expand_stepped_range_odd_endpoint() {
        // 0-10:4 -> 0, 4, 8 (10 NOT included since (10-0)%4 != 0)
        let spec: crate::entities::slurm::SlurmArraySpec = "0-10:4".parse().unwrap();
        assert_eq!(expand_array_indices(&spec), vec![0, 4, 8]);
    }

    #[test]
    fn expand_mixed_entries_preserves_order() {
        let spec: crate::entities::slurm::SlurmArraySpec = "0,2,5-7".parse().unwrap();
        assert_eq!(expand_array_indices(&spec), vec![0, 2, 5, 6, 7]);
    }

    #[test]
    fn expand_ignores_max_concurrent() {
        let spec: crate::entities::slurm::SlurmArraySpec = "0-3%2".parse().unwrap();
        assert_eq!(expand_array_indices(&spec), vec![0, 1, 2, 3]);
    }
```

- [ ] **Step 2: Run the failing test**

Run: `cargo test --lib --features pyo3 expand_single_value 2>&1 | tail -10`

Expected: FAIL with `error[E0425]: cannot find function 'expand_array_indices' in this scope`.

- [ ] **Step 3: Add the helper**

Append to `src/sbatch/parse.rs` BEFORE the `#[cfg(test)] mod tests` block:

```rust
/// Enumerate every task index covered by a `SlurmArraySpec`.
///
/// `max_concurrent` (the `%N` suffix) is deliberately ignored — it
/// constrains runtime concurrency at SLURM, not the set of tasks
/// submitted. Indices are returned in declaration order (`Vec` order).
pub(crate) fn expand_array_indices(spec: &crate::entities::slurm::SlurmArraySpec) -> Vec<u32> {
    use crate::entities::slurm::ArrayIndex;
    let mut out = Vec::new();
    for entry in &spec.indices {
        match *entry {
            ArrayIndex::Single(i) => out.push(i),
            ArrayIndex::Range { start, end } => {
                for i in start..=end {
                    out.push(i);
                }
            }
            ArrayIndex::Stepped { start, end, step } => {
                let mut i = start;
                while i <= end {
                    out.push(i);
                    match i.checked_add(step) {
                        Some(next) => i = next,
                        None => break,
                    }
                }
            }
        }
    }
    out
}
```

- [ ] **Step 4: Run tests + lints**

```bash
cargo test --lib --features pyo3 -- expand_ 2>&1 | tail -15
cargo clippy --all-targets --features pyo3 -- -D warnings 2>&1 | tail -10
cargo fmt --all --check
```

Expected: 6 new tests pass; clippy clean.

- [ ] **Step 5: Commit**

```bash
git add src/sbatch/parse.rs
git commit -m "feat(sbatch): add expand_array_indices helper for SlurmArraySpec"
```

---

## Task 5: `SbatchAttachError` typed enum + store `find_all_by_jobid`

**Files:**
- Modify: `src/sbatch/error.rs`
- Modify: `src/store.rs:26-39`
- Modify: `src/sbatch/manager.rs`

**Why:** Spec §5.6 requires `MultipleMatch { jobid, count }` discoverability. Current `attach*` return `anyhow::Result`, which stringifies. Replace with typed enum.

**Constraints:**
- `#[non_exhaustive]` (consistent with `SbatchSpawnError`).
- Python binding still works (`.map_err(py_err)` + `Display` via `thiserror`).
- `find_all_by_jobid` has a default impl filtering `list()`. No override needed for existing stores.

- [ ] **Step 1: Write the failing tests**

Append to the `#[cfg(test)] mod tests` block in `src/sbatch/error.rs`:

```rust
    #[test]
    fn attach_not_found_carries_lookup_key_string() {
        let e = SbatchAttachError::NotFound {
            key: "uuid abc-def".to_string(),
        };
        assert!(e.to_string().contains("abc-def"));
    }

    #[test]
    fn attach_kind_mismatch_carries_both_kinds() {
        let e = SbatchAttachError::KindMismatch {
            expected: "sbatch",
            got: "tssrun".to_string(),
        };
        let msg = e.to_string();
        assert!(msg.contains("kind mismatch"));
        assert!(msg.contains("sbatch"));
        assert!(msg.contains("tssrun"));
    }

    #[test]
    fn attach_multiple_match_carries_jobid_and_count() {
        let e = SbatchAttachError::MultipleMatch {
            jobid: 12345,
            count: 4,
        };
        let msg = e.to_string();
        assert!(msg.contains("12345"));
        assert!(msg.contains("4"));
    }
```

- [ ] **Step 2: Run the failing tests**

Run: `cargo test --lib --features pyo3 attach_not_found_carries_lookup_key_string 2>&1 | tail -10`

Expected: FAIL with `error[E0433]: failed to resolve: use of undeclared type 'SbatchAttachError'`.

- [ ] **Step 3: Add `SbatchAttachError`**

In `src/sbatch/error.rs`, INSERT this enum BEFORE the existing `#[cfg(test)] mod tests` block (after the `SbatchSpawnError` enum):

```rust
/// Errors that can occur while attaching to an existing snapshot.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SbatchAttachError {
    #[error("snapshot not found for key {key:?}")]
    NotFound { key: String },

    #[error("snapshot kind mismatch: expected '{expected}', got '{got}'")]
    KindMismatch {
        expected: &'static str,
        got: String,
    },

    #[error(
        "jobid {jobid} matched {count} snapshots; use attach_array_jobid \
         to retrieve per-task handles or attach_uuid for a specific task"
    )]
    MultipleMatch { jobid: u64, count: usize },

    #[error("io error during attach: {0}")]
    Io(#[from] anyhow::Error),
}
```

- [ ] **Step 4: Add `find_all_by_jobid` to `JobStateStore`**

Edit `src/store.rs:26-39`. Add the new method to the trait inside the `#[async_trait] pub trait JobStateStore<S: JobSnapshot>: Send + Sync { ... }` block:

```rust
    /// Find every snapshot whose `jobid` matches. For single jobs the result
    /// has 0 or 1 entries; for array submissions it has one entry per task.
    async fn find_all_by_jobid(&self, jobid: u64) -> Result<Vec<S>> {
        Ok(self
            .list()
            .await?
            .into_iter()
            .filter(|s| s.jobid() == Some(jobid))
            .collect())
    }
```

(Place it right after the existing `find_by_jobid` default impl, inside the trait.)

- [ ] **Step 5: Migrate `SbatchManager::attach*` to the typed error**

Edit `src/sbatch/manager.rs`. The current `attach`, `attach_uuid`, `attach_jobid`, `attach_file` use `anyhow::Result<SbatchJobHandle>`. Replace the full `attach` method body and the three convenience methods. The new `attach`:

```rust
pub async fn attach(
    &self,
    key: SbatchAttachKey,
) -> Result<SbatchJobHandle, SbatchAttachError> {
    let key_repr = format!("{key:?}");
    let snapshot = match key {
        SbatchAttachKey::Uuid(u) => self
            .store
            .load(u)
            .await
            .map_err(SbatchAttachError::Io)?,
        SbatchAttachKey::JobId(j) => {
            let snaps = self
                .store
                .find_all_by_jobid(j)
                .await
                .map_err(SbatchAttachError::Io)?;
            if snaps.len() > 1 {
                return Err(SbatchAttachError::MultipleMatch {
                    jobid: j,
                    count: snaps.len(),
                });
            }
            snaps.into_iter().next()
        }
        SbatchAttachKey::File(path) => {
            let bytes = tokio::fs::read(&path)
                .await
                .map_err(|e| SbatchAttachError::Io(anyhow::Error::from(e)))?;
            let value: serde_json::Value = serde_json::from_slice(&bytes)
                .map_err(|e| SbatchAttachError::Io(anyhow::Error::from(e)))?;
            if let Some(k) = value.get("kind").and_then(|v| v.as_str())
                && k != <SbatchJobSnapshot as JobSnapshot>::kind()
            {
                return Err(SbatchAttachError::KindMismatch {
                    expected: <SbatchJobSnapshot as JobSnapshot>::kind(),
                    got: k.to_string(),
                });
            }
            Some(
                serde_json::from_value(value)
                    .map_err(|e| SbatchAttachError::Io(anyhow::Error::from(e)))?,
            )
        }
    }
    .ok_or_else(|| SbatchAttachError::NotFound { key: key_repr })?;
    Ok(SbatchJobHandle::new(
        snapshot,
        self.store.clone(),
        self.dispatcher.clone(),
    ))
}

pub async fn attach_uuid(&self, u: Uuid) -> Result<SbatchJobHandle, SbatchAttachError> {
    self.attach(SbatchAttachKey::Uuid(u)).await
}
pub async fn attach_jobid(&self, j: u64) -> Result<SbatchJobHandle, SbatchAttachError> {
    self.attach(SbatchAttachKey::JobId(j)).await
}
pub async fn attach_file(
    &self,
    p: impl Into<PathBuf>,
) -> Result<SbatchJobHandle, SbatchAttachError> {
    self.attach(SbatchAttachKey::File(p.into())).await
}
```

Update the import at the top:

```rust
use crate::sbatch::error::{SbatchAttachError, SbatchSpawnError};
```

If `use anyhow::{Context, Result, anyhow};` becomes partially unused after this change (the `anyhow!` macro is no longer called in the manager body), prune to keep only what's needed. Run `cargo clippy` and let it tell you what's unused.

- [ ] **Step 6: Update the existing manager test**

The existing `attach_file_rejects_wrong_kind_snapshot` test asserts on `e.to_string().contains("kind mismatch")`. This still works via the `Display` impl, but we improve to a typed match. Replace its body:

```rust
    #[tokio::test]
    async fn attach_file_rejects_wrong_kind_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("wrong.json");
        std::fs::write(&path, r#"{"kind":"tssrun"}"#).unwrap();

        let cmd = SbatchCmd::new("/w/job.sh");
        let dispatcher = into_dyn(CannedSbatch::ok(1));
        let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);
        match mgr.attach_file(&path).await {
            Ok(_) => panic!("attach_file should fail on wrong kind"),
            Err(SbatchAttachError::KindMismatch { expected, got }) => {
                assert_eq!(expected, "sbatch");
                assert_eq!(got, "tssrun");
            }
            Err(other) => panic!("expected KindMismatch, got {other:?}"),
        }
    }
```

- [ ] **Step 7: Run tests**

```bash
cargo test --lib --features pyo3 sbatch::error 2>&1 | tail -10
cargo test --lib --features pyo3 sbatch::manager 2>&1 | tail -10
```

Expected: error tests pass (5 P3 + 3 P5 = 8); manager tests pass.

- [ ] **Step 8: Run lints**

```bash
cargo clippy --all-targets --features pyo3 -- -D warnings 2>&1 | tail -10
cargo fmt --all --check
```

Expected: 0 warnings, no diff.

- [ ] **Step 9: Commit**

```bash
git add src/sbatch/error.rs src/store.rs src/sbatch/manager.rs
git commit -m "feat(sbatch): introduce SbatchAttachError typed enum + find_all_by_jobid"
```

---

## Task 6: `SbatchManager::spawn_array`

**Files:**
- Modify: `src/sbatch/manager.rs`

**Why:** Core spawn entry-point for array jobs.

**Constraints:**
- Single sbatch CLI invocation (one `dispatcher.capture` call). NO per-task spawn loop.
- All N snapshots share `jobid` (master) + `argv`, distinct `uuid` + `array_task_id` + `array_jobid` filled with master.
- Result Vec sorted in declaration order (matching `expand_array_indices`).

- [ ] **Step 1: Write the failing tests**

Append to the manager tests block:

```rust
    #[tokio::test]
    async fn spawn_array_creates_one_snapshot_per_task() {
        use crate::entities::slurm::SlurmArraySpec;

        let cmd = SbatchCmd::new("/w/job.sh");
        let dispatcher = into_dyn(CannedSbatch::ok(50000));
        let tmp = tempfile::tempdir().unwrap();
        let mgr = SbatchManager::new(cmd)
            .with_state_dir(tmp.path())
            .with_dispatcher(dispatcher);

        let spec: SlurmArraySpec = "0-3".parse().unwrap();
        let handles = mgr.spawn_array(spec).await.unwrap();

        assert_eq!(handles.len(), 4);
        for (i, h) in handles.iter().enumerate() {
            let snap = h.snapshot();
            assert_eq!(snap.jobid, 50000);
            assert_eq!(snap.array_jobid, Some(50000));
            assert_eq!(snap.array_task_id, Some(i as u32));
            for (j, other) in handles.iter().enumerate() {
                if i != j {
                    assert_ne!(h.uuid(), other.uuid());
                }
            }
        }

        let count = std::fs::read_dir(tmp.path())
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .map(|e| e.path().extension().and_then(|s| s.to_str()) == Some("json"))
                    .unwrap_or(false)
            })
            .count();
        assert_eq!(count, 4);
    }

    #[tokio::test]
    async fn spawn_array_returns_submit_failed_on_nonzero_exit() {
        use crate::entities::slurm::SlurmArraySpec;

        let cmd = SbatchCmd::new("/w/job.sh");
        let dispatcher = into_dyn(CannedSbatch::failed());
        let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);

        let spec: SlurmArraySpec = "0-2".parse().unwrap();
        let Err(err) = mgr.spawn_array(spec).await else {
            panic!("spawn_array should fail");
        };
        assert!(matches!(
            err,
            SbatchSpawnError::SubmitFailed { exit_code: 1, .. }
        ));
    }
```

(The counter-test from the spec discussion is OMITTED for simplicity. The first test implicitly verifies single-invocation behavior because `CannedSbatch` is one-shot and the spawn succeeds with 4 tasks.)

- [ ] **Step 2: Run the failing test**

Run: `cargo test --lib --features pyo3 spawn_array_creates_one_snapshot_per_task 2>&1 | tail -10`

Expected: FAIL with `error[E0599]: no method named 'spawn_array' found for struct 'SbatchManager'`.

- [ ] **Step 3: Implement `spawn_array`**

Add to `impl SbatchManager` after the `spawn` method:

```rust
    /// Submit an array job in a single `sbatch --array=<spec>` invocation,
    /// then persist one snapshot per task and return one handle per task.
    ///
    /// All returned snapshots share the same master `jobid` (from sbatch's
    /// `Submitted batch job <N>` line) and the same `argv`, but each has a
    /// distinct `uuid` and `array_task_id`. The `array_jobid` field also
    /// holds the master jobid for every task.
    ///
    /// `array_spec` overrides any value already on `self.cmd.array_spec`.
    /// Returns the handles in `expand_array_indices` order (declaration
    /// order — not numerical sort if the spec was e.g. `5,0-2`).
    pub async fn spawn_array(
        &self,
        array_spec: crate::entities::slurm::SlurmArraySpec,
    ) -> Result<Vec<SbatchJobHandle>, SbatchSpawnError> {
        use crate::sbatch::parse::expand_array_indices;
        let task_indices = expand_array_indices(&array_spec);
        assert!(
            !task_indices.is_empty(),
            "SlurmArraySpec FromStr guarantees non-empty indices"
        );

        let mut cmd = self.cmd.clone();
        cmd.array_spec = Some(array_spec);
        let argv = cmd.build_argv()?;

        let (exit_code, stdout) = self
            .dispatcher
            .capture(&argv)
            .await
            .map_err(SbatchSpawnError::Other)?;
        if exit_code != 0 {
            return Err(SbatchSpawnError::SubmitFailed { exit_code, stdout });
        }
        let master_jobid = parse_submitted_jobid(&stdout)
            .ok_or_else(|| SbatchSpawnError::JobidParseError {
                stdout: stdout.clone(),
            })?;

        let script_path = std::path::absolute(&cmd.script)
            .with_context(|| format!("absolutize {}", cmd.script.display()))
            .map_err(|e| SbatchSpawnError::Other(e))?;

        let mut handles = Vec::with_capacity(task_indices.len());
        for idx in task_indices {
            let snapshot = SbatchJobSnapshot {
                uuid: Uuid::now_v7(),
                jobid: master_jobid,
                array_jobid: Some(master_jobid),
                array_task_id: Some(idx),
                argv: argv.clone(),
                sent_env: cmd.env.clone(),
                script_path: script_path.clone(),
                chdir: cmd.chdir.clone(),
                partition: cmd.partition.clone(),
                job_name: cmd.job_name.clone(),
                submitted_at: Utc::now(),
                log: LogPathSpec {
                    output_template: cmd.output.clone(),
                    error_template: cmd.error.clone(),
                },
                lifecycle: SbatchLifecycle::default(),
            };
            self.store
                .save(&snapshot)
                .await
                .map_err(|source| SbatchSpawnError::SubmittedButUnpersisted {
                    jobid: master_jobid,
                    source,
                })?;
            handles.push(SbatchJobHandle::new(
                snapshot,
                self.store.clone(),
                self.dispatcher.clone(),
            ));
        }
        Ok(handles)
    }
```

Note: if `Context` was pruned from the imports in Task 5, re-add it for `with_context` here. Otherwise use `.map_err(|e| SbatchSpawnError::Other(anyhow::anyhow!("absolutize: {e}")))` as a fallback.

- [ ] **Step 4: Run tests**

```bash
cargo test --lib --features pyo3 -- spawn_array 2>&1 | tail -15
cargo test --lib --features pyo3 sbatch:: 2>&1 | tail -5
```

Expected: 2 tests pass; all sbatch::* tests pass.

- [ ] **Step 5: Run lints**

```bash
cargo clippy --all-targets --features pyo3 -- -D warnings 2>&1 | tail -10
cargo fmt --all --check
```

Expected: 0 warnings, no diff.

- [ ] **Step 6: Commit**

```bash
git add src/sbatch/manager.rs
git commit -m "feat(sbatch): add SbatchManager::spawn_array for --array submissions"
```

---

## Task 7: `SbatchManager::attach_array_jobid` + handle getters + jobid disambiguation

**Files:**
- Modify: `src/sbatch/manager.rs`
- Modify: `src/sbatch/handle.rs`

**Why:** After `spawn_array`, callers fetch per-task handles via `attach_array_jobid(master)`. Returns `Vec<SbatchJobHandle>` sorted ascending. `attach_jobid` with master id now returns `MultipleMatch` (already enforced via Task 5).

**Constraints:**
- `attach_array_jobid` filters by `array_task_id.is_some()` AND `jobid == master`.
- Sort by `array_task_id` ascending.
- Empty result returns `Ok(vec![])`.

- [ ] **Step 1: Write the failing tests**

Append to the manager tests block:

```rust
    #[tokio::test]
    async fn attach_array_jobid_returns_all_tasks_sorted() {
        use crate::entities::slurm::SlurmArraySpec;

        let cmd = SbatchCmd::new("/w/job.sh");
        let dispatcher = into_dyn(CannedSbatch::ok(60000));
        let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);

        let spec: SlurmArraySpec = "0-2".parse().unwrap();
        let _spawned = mgr.spawn_array(spec).await.unwrap();

        let attached = mgr.attach_array_jobid(60000).await.unwrap();
        assert_eq!(attached.len(), 3);
        assert_eq!(attached[0].snapshot().array_task_id, Some(0));
        assert_eq!(attached[1].snapshot().array_task_id, Some(1));
        assert_eq!(attached[2].snapshot().array_task_id, Some(2));
    }

    #[tokio::test]
    async fn attach_array_jobid_empty_when_no_match() {
        let cmd = SbatchCmd::new("/w/job.sh");
        let dispatcher = into_dyn(CannedSbatch::ok(1));
        let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);
        let attached = mgr.attach_array_jobid(99999).await.unwrap();
        assert!(attached.is_empty());
    }

    #[tokio::test]
    async fn attach_jobid_returns_multiple_match_for_array_master() {
        use crate::entities::slurm::SlurmArraySpec;

        let cmd = SbatchCmd::new("/w/job.sh");
        let dispatcher = into_dyn(CannedSbatch::ok(70000));
        let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);

        let spec: SlurmArraySpec = "0-1".parse().unwrap();
        let _ = mgr.spawn_array(spec).await.unwrap();

        let Err(err) = mgr.attach_jobid(70000).await else {
            panic!("attach_jobid on array master should error")
        };
        match err {
            SbatchAttachError::MultipleMatch { jobid, count } => {
                assert_eq!(jobid, 70000);
                assert_eq!(count, 2);
            }
            other => panic!("expected MultipleMatch, got {other:?}"),
        }
    }
```

- [ ] **Step 2: Run the failing test**

Run: `cargo test --lib --features pyo3 attach_array_jobid_returns_all_tasks_sorted 2>&1 | tail -10`

Expected: FAIL with `error[E0599]: no method named 'attach_array_jobid' found for struct 'SbatchManager'`.

- [ ] **Step 3: Add `attach_array_jobid`**

Add to `impl SbatchManager` after `attach_file`:

```rust
    /// Attach to all per-task snapshots of an array job by its master jobid.
    ///
    /// Returns handles sorted by `array_task_id` ascending. Empty result
    /// (`Ok(vec![])`) means "no array-task snapshots stored under this
    /// master jobid"; single-job snapshots (with `array_task_id == None`)
    /// are filtered out even if they share the master jobid.
    pub async fn attach_array_jobid(
        &self,
        master_jobid: u64,
    ) -> Result<Vec<SbatchJobHandle>, SbatchAttachError> {
        let snaps = self
            .store
            .find_all_by_jobid(master_jobid)
            .await
            .map_err(SbatchAttachError::Io)?;
        let mut filtered: Vec<SbatchJobSnapshot> = snaps
            .into_iter()
            .filter(|s| s.array_task_id.is_some())
            .collect();
        filtered.sort_by_key(|s| s.array_task_id);
        Ok(filtered
            .into_iter()
            .map(|snap| {
                SbatchJobHandle::new(snap, self.store.clone(), self.dispatcher.clone())
            })
            .collect())
    }
```

- [ ] **Step 4: Add getters on `SbatchJobHandle`**

Edit `src/sbatch/handle.rs`. Add two getters AFTER `jobid()`:

```rust
    pub fn array_jobid(&self) -> Option<u64> {
        self.snapshot().array_jobid
    }

    pub fn array_task_id(&self) -> Option<u32> {
        self.snapshot().array_task_id
    }
```

- [ ] **Step 5: Run tests**

```bash
cargo test --lib --features pyo3 -- attach_array_jobid 2>&1 | tail -15
cargo test --lib --features pyo3 -- attach_jobid 2>&1 | tail -15
```

Expected: 3 new tests pass; existing `attach_jobid_finds_via_default_trait_impl` (single job) still passes.

- [ ] **Step 6: Run lints**

```bash
cargo clippy --all-targets --features pyo3 -- -D warnings 2>&1 | tail -10
cargo fmt --all --check
```

Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add src/sbatch/manager.rs src/sbatch/handle.rs
git commit -m "feat(sbatch): add attach_array_jobid + array_jobid/array_task_id getters"
```

---

## Task 8: pyo3 bindings

**Files:**
- Modify: `src/py_export/sbatch.rs`
- Modify: `python/tests/test_sbatch.py`

**Constraints:**
- `array_spec` kwarg on `PySbatchCmd::new` (placed after `signal`).
- New methods on `PySbatchManager`: `spawn_array(spec) -> Vec<PySbatchJobHandle>`, `attach_array_jobid(master) -> Vec<PySbatchJobHandle>` (both async via `future_into_py`).
- New getters on `PySbatchJobHandle`: `array_jobid`, `array_task_id`.

- [ ] **Step 1: Verify `PySlurmArraySpec` path**

Run: `grep -n 'pub struct PySlurmArraySpec' src/py_export/entities/slurm/sbatch_options/array_spec.rs`

Expected: confirms the wrapper exists.

- [ ] **Step 2: Write the failing Python smoke tests**

Append to `python/tests/test_sbatch.py`:

```python
def test_sbatch_cmd_array_spec_kwarg(tmp_path):
    from slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options import (
        SlurmArraySpec,
    )

    job = tmp_path / "job.sh"
    job.write_text("#!/usr/bin/env bash\necho hi\n")
    cmd = SbatchCmd(str(job), array_spec=SlurmArraySpec.parse("0-3"))
    argv = cmd.build_argv()
    i = argv.index("-a")
    assert argv[i + 1] == "0-3"


@pytest.mark.skipif(not _have_bash(), reason="bash required")
def test_spawn_array_with_bash_fake_sbatch(tmp_path: Path):
    fake_sbatch = tmp_path / "fake_sbatch"
    fake_sbatch.write_text('#!/usr/bin/env bash\necho "Submitted batch job 88888"\n')
    fake_sbatch.chmod(0o755)
    job_script = tmp_path / "job.sh"
    job_script.write_text("#!/usr/bin/env bash\necho hello\n")
    job_script.chmod(0o755)

    from slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options import (
        SlurmArraySpec,
    )

    cmd = SbatchCmd(str(job_script), sbatch_bin=str(fake_sbatch))
    state_dir = tmp_path / "state"
    mgr = SbatchManager(cmd, state_dir=str(state_dir))

    async def go():
        handles = await mgr.spawn_array(SlurmArraySpec.parse("0-2"))
        return [(h.jobid, h.array_task_id) for h in handles]

    result = asyncio.run(go())
    assert len(result) == 3
    assert all(jobid == 88888 for jobid, _ in result)
    assert [t for _, t in result] == [0, 1, 2]


@pytest.mark.skipif(not _have_bash(), reason="bash required")
def test_attach_array_jobid_round_trips(tmp_path: Path):
    fake_sbatch = tmp_path / "fake_sbatch"
    fake_sbatch.write_text('#!/usr/bin/env bash\necho "Submitted batch job 99001"\n')
    fake_sbatch.chmod(0o755)
    job_script = tmp_path / "job.sh"
    job_script.write_text("#!/usr/bin/env bash\necho hi\n")
    job_script.chmod(0o755)

    from slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options import (
        SlurmArraySpec,
    )

    cmd = SbatchCmd(str(job_script), sbatch_bin=str(fake_sbatch))
    state_dir = tmp_path / "state"
    mgr = SbatchManager(cmd, state_dir=str(state_dir))

    async def go():
        await mgr.spawn_array(SlurmArraySpec.parse("0-1"))
        return await mgr.attach_array_jobid(99001)

    handles = asyncio.run(go())
    assert len(handles) == 2
    assert handles[0].array_task_id == 0
    assert handles[1].array_task_id == 1
```

- [ ] **Step 3: Run failing test**

```bash
uv run pytest python/tests/test_sbatch.py::test_sbatch_cmd_array_spec_kwarg -v 2>&1 | tail -10
```

Expected: FAIL with `TypeError: SbatchCmd.__init__() got an unexpected keyword argument 'array_spec'`.

- [ ] **Step 4: Extend `PySbatchCmd::new` with `array_spec` kwarg**

Edit `src/py_export/sbatch.rs`. Extend imports:

```rust
use crate::entities::slurm::{
    JobTimeLimit, MailTypeInput, ResourceSpec, SlurmArraySpec, SlurmDependency, SlurmSignalSpec,
};
```

```rust
use crate::py_export::entities::slurm::sbatch_options::array_spec::PySlurmArraySpec;
```

In `PySbatchCmd::new`, add `array_spec` after `signal`:

In `#[pyo3(signature = (...))]`:
```rust
        signal = None,
        array_spec = None,
```

Parameter list:
```rust
        signal: Option<PySlurmSignalSpec>,
        array_spec: Option<PySlurmArraySpec>,
```

Body, after `cmd.signal = ...`:
```rust
        cmd.array_spec = array_spec.map(<PySlurmArraySpec as Into<SlurmArraySpec>>::into);
```

- [ ] **Step 5: Add `spawn_array` and `attach_array_jobid` methods on `PySbatchManager`**

Add to `impl PySbatchManager` (after `spawn` and after `attach_file`):

```rust
    fn spawn_array<'py>(
        &self,
        py: Python<'py>,
        array_spec: PySlurmArraySpec,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mgr = self.0.clone();
        future_into_py(py, async move {
            let handles = mgr
                .spawn_array(array_spec.into())
                .await
                .map_err(|e| match e {
                    SbatchSpawnError::SubmittedButUnpersisted { jobid, source } => {
                        PyRuntimeError::new_err(format!(
                            "submitted but unpersisted: jobid={jobid}, source={source}"
                        ))
                    }
                    other => PyRuntimeError::new_err(other.to_string()),
                })?;
            Ok(handles
                .into_iter()
                .map(PySbatchJobHandle)
                .collect::<Vec<_>>())
        })
    }

    fn attach_array_jobid<'py>(
        &self,
        py: Python<'py>,
        master_jobid: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mgr = self.0.clone();
        future_into_py(py, async move {
            let handles = mgr
                .attach_array_jobid(master_jobid)
                .await
                .map_err(py_err)?;
            Ok(handles
                .into_iter()
                .map(PySbatchJobHandle)
                .collect::<Vec<_>>())
        })
    }
```

- [ ] **Step 6: Add `array_jobid` and `array_task_id` getters on `PySbatchJobHandle`**

After the existing `#[getter] fn jobid(&self) -> Option<u64> { ... }`, add:

```rust
    #[getter]
    fn array_jobid(&self) -> Option<u64> {
        self.0.array_jobid()
    }

    #[getter]
    fn array_task_id(&self) -> Option<u32> {
        self.0.array_task_id()
    }
```

- [ ] **Step 7: Rebuild and run tests**

```bash
uv run maturin develop --features pyo3 2>&1 | tail -3
uv run pytest python/tests/test_sbatch.py -v 2>&1 | tail -25
```

Expected: all P1-P4 + 3 new P5 tests pass.

- [ ] **Step 8: Run lints**

```bash
cargo clippy --all-targets --features pyo3 -- -D warnings 2>&1 | tail -10
cargo fmt --all --check
uv run ruff check python/
```

Expected: clean.

- [ ] **Step 9: Commit**

```bash
git add src/py_export/sbatch.rs python/tests/test_sbatch.py
git commit -m "feat(py): expose array_spec kwarg + spawn_array + attach_array_jobid"
```

---

## Task 9: `.pyi` sync

**Files:**
- Modify: `python/slurm_async_runner/_slurm_async_runner_core/sbatch.pyi`

- [ ] **Step 1: Read the current `.pyi`**

Use Read.

- [ ] **Step 2: Extend `TYPE_CHECKING` and `SbatchCmd.__init__`**

Add `SlurmArraySpec` to `TYPE_CHECKING`:

```python
if TYPE_CHECKING:
    from slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options import (
        MailTypeInput,
        SlurmArraySpec,
        SlurmDependency,
        SlurmSignalSpec,
    )
```

In `SbatchCmd.__init__`, after `signal: "SlurmSignalSpec | None" = None,`:

```python
        signal: "SlurmSignalSpec | None" = None,
        array_spec: "SlurmArraySpec | None" = None,
    ) -> None: ...
```

- [ ] **Step 3: Add new methods on `SbatchManager` and getters on `SbatchJobHandle`**

In `SbatchManager`, after `attach_file`:

```python
    def spawn_array(
        self, array_spec: "SlurmArraySpec"
    ) -> Awaitable[builtins.list[SbatchJobHandle]]: ...
    def attach_array_jobid(
        self, master_jobid: builtins.int
    ) -> Awaitable[builtins.list[SbatchJobHandle]]: ...
```

In `SbatchJobHandle`, after `jobid: builtins.int | None`:

```python
    @property
    def array_jobid(self) -> builtins.int | None: ...
    @property
    def array_task_id(self) -> builtins.int | None: ...
```

- [ ] **Step 4: Smoke-import**

```bash
uv run python -c "
import slurm_async_runner._slurm_async_runner_core.sbatch as m
from slurm_async_runner._slurm_async_runner_core.entities.slurm.sbatch_options import SlurmArraySpec
cmd = m.SbatchCmd('/tmp/job.sh', array_spec=SlurmArraySpec.parse('0-3'))
print('OK', cmd)
"
```

Expected: prints `OK <...>`.

- [ ] **Step 5: Run pytest + ruff**

```bash
uv run pytest python/tests/ -v 2>&1 | tail -20
uv run ruff check python/
```

Expected: all pass; 0 ruff errors.

- [ ] **Step 6: Commit**

```bash
git add python/slurm_async_runner/_slurm_async_runner_core/sbatch.pyi
git commit -m "docs(py): sync .pyi for array_spec kwarg + spawn_array / attach_array_jobid"
```

---

## Task 10: CHANGELOG + final validation

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Insert the P5 section**

Open `CHANGELOG.md`. Insert a new `### Added (Phase 2 P5)` block IMMEDIATELY after `## [Unreleased]` and BEFORE `### Added (Phase 2 P4)`. Use:

```markdown
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

```

- [ ] **Step 2: Run the full validation gate**

```bash
cargo fmt --all --check
cargo clippy --all-targets --features pyo3 -- -D warnings 2>&1 | tail -5
cargo test --lib --features pyo3 2>&1 | tail -10
uv run maturin develop --features pyo3 2>&1 | tail -3
uv run pytest python/tests/ 2>&1 | tail -10
uv run ruff check python/
```

Expected:
- `cargo fmt`: clean
- `cargo clippy`: 0 warnings
- `cargo test --lib --features pyo3`: ≈ 355 passing (P4 baseline 325 + ~30 from P5)
- `maturin develop`: succeeds
- `pytest`: ≈ 39 passing (P4 baseline 36 + 3 new)
- `ruff`: clean

- [ ] **Step 3: Verify no regression**

Run: `cargo test --lib --features pyo3 full_flags_cpu_variant_argv_layout -- --exact`

Expected: PASS (byte-identical).

- [ ] **Step 4: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): record Phase 2 P5 --array spawn_array / attach_array_jobid"
```

- [ ] **Step 5: Sanity-check the commit graph**

Run: `git log --oneline 1af12b9..HEAD`

Expected: ~10 new commits on top of the P4 stub-regen head:
```
<sha> docs(changelog): record Phase 2 P5 ...
<sha> docs(py): sync .pyi for array_spec ...
<sha> feat(py): expose array_spec kwarg + spawn_array + attach_array_jobid
<sha> feat(sbatch): add attach_array_jobid + array_jobid/array_task_id getters
<sha> feat(sbatch): add SbatchManager::spawn_array for --array submissions
<sha> feat(sbatch): introduce SbatchAttachError typed enum + find_all_by_jobid
<sha> feat(sbatch): add expand_array_indices helper for SlurmArraySpec
<sha> feat(sbatch): resolve_log_path expands %A/%a/%u/%N tokens
<sha> feat(sbatch): wire --array via SlurmArraySpec entity
<sha> feat(sbatch): add array_jobid/array_task_id to SbatchJobSnapshot
```

---

## Self-Review Coverage

Spec §5.1 (`SlurmArraySpec` 配線) → Task 2.
Spec §5.2 (snapshot model 拡張) → Task 1.
Spec §5.3 (`spawn_array` フロー) → Task 6.
Spec §5.4 (`resolve_log_path` `%A`/`%a`/`%u`/`%N`) → Task 3.
Spec §5.5 (refresh フロー) → **DEFERRED** to Phase 3 (documented in CHANGELOG Notes).
Spec §5.6 (attach 経路) → Tasks 5 + 7.
Spec §2.1 (vocab single-source) → enforced.
Spec §2.2 invariants: no new `JobDispatcher` method, no new `JobState` variant, no new kind string, no sacct outside `refresh_with_sacct`/`run()`. Array task snapshot fields use `#[serde(default)]`.

## Dependencies

Depends on P1 (`resolve_log_path` already exists from Phase 1) — in `develop` via this branch. Independent of P3 / P4 (concurrent merges OK).
