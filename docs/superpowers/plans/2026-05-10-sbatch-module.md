# sbatch Module Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `crate::sbatch` (Rust) + `slurm_async_runner._core.sbatch` (Python) — a fire-and-forget SLURM batch wrapper with kudpc-aware polling (`qgroup -l` → `squeue` fallback, sacct opt-in), mirroring the existing `tssrun` module's lock-free handle pattern.

**Architecture:** Approach A — passive handle + `tokio::sync::watch` snapshot, no auto-poll task. Spec/Runtime split inherited from `crate::dispatcher::JobDispatcher`. Generic store layer (`JobStateStore<S: JobSnapshot>`) shared with `tssrun`, single `{root}/<uuid>.json` namespace with on-disk `kind` discriminator. Lazy log-path resolution (raw template + variable substitution at read time, not write time).

**Tech Stack:** Rust 1.x edition 2024, `tokio` async runtime, `pyo3` 0.28 + `pyo3-async-runtimes`, `serde`/`serde_json`, `anyhow`/`thiserror`, `tempfile` for atomic-rename, `uuid` v7, `chrono`, kudpc-specific `qgroup -l` / `squeue` / `sacct`.

**Spec:** `docs/superpowers/specs/2026-05-10-sbatch-module-design.md`

---

## File Structure

| Path | Purpose | New / Modify |
|---|---|---|
| `src/store.rs` | Generic `JobSnapshot` trait + `JobStateStore<S>` + `InMemoryStateStore<S>` + `FileSystemStateStore<S>` (with `kind` discriminator) | New |
| `src/lib.rs` | Add `pub mod sbatch; pub mod store;` and re-exports | Modify |
| `src/tssrun/store.rs` | Shrink to `impl JobSnapshot for JobHandleSnapshot` + back-compat type aliases | Modify |
| `src/tssrun/handle.rs` | Move `is_running` / `exit_code` logic from `JobHandle` to `JobHandleSnapshot` helpers; `JobHandle` methods become delegates | Modify |
| `src/tssrun/manager.rs` | Update `Arc<dyn JobStateStore>` → `Arc<dyn JobStateStore<JobHandleSnapshot>>` | Modify |
| `src/runner.rs` | Add `parse_qgroup_l` + `query_job_states_via_qgroup_with` + `query_job_states_squeue_only_with` | Modify |
| `src/sbatch/mod.rs` | Module-level rustdoc + `pub mod` declarations | New |
| `src/sbatch/cmd.rs` | `SbatchCmd` Spec layer + `build_argv` | New |
| `src/sbatch/parse.rs` | `parse_submitted_jobid` + `resolve_log_path` | New |
| `src/sbatch/handle.rs` | `SbatchJobSnapshot` + `SbatchLifecycle` + `FinishedInfo` + `LogPathSpec` + `SbatchJobHandle` + `SbatchAttachKey` | New |
| `src/sbatch/store.rs` | `impl JobSnapshot for SbatchJobSnapshot` + tests | New |
| `src/sbatch/manager.rs` | `SbatchManager` (spawn + attach) | New |
| `src/sbatch/error.rs` | `SbatchSpawnError` thiserror enum | New |
| `src/py_export/mod.rs` | Wire sbatch submodule into pymodule | Modify |
| `src/py_export/sbatch.rs` | pyo3 bindings: `PySbatchCmd`, `PySbatchManager`, `PySbatchJobHandle` | New |
| `python/slurm_async_runner/_core/sbatch.pyi` | Hand-written stubs for async pyfunctions | New |
| `python/slurm_async_runner/_core/__init__.py` | Re-export sbatch submodule | Modify |
| `python/tests/test_sbatch.py` | Python pytest suite | New |
| `scripts/test_sbatch_live.py` | KUDPC live smoke test | New |
| `CHANGELOG.md` | Record breaking change (generic store API + JSON `kind` field) | Modify |

---

## Task 1: Generic Store Layer (trait + InMemoryStateStore)

**Files:**
- Create: `src/store.rs`
- Modify: `src/lib.rs:1-2` (add `pub mod store;`)

- [ ] **Step 1.1: Write the file with implementation + failing tests**

Create `src/store.rs`:

```rust
//! Generic snapshot persistence shared by tssrun and sbatch modules.
//!
//! See `docs/superpowers/specs/2026-05-10-sbatch-module-design.md` §5
//! for the full design rationale.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

pub trait JobSnapshot:
    Serialize + DeserializeOwned + Clone + Send + Sync + 'static
{
    fn uuid(&self) -> Uuid;
    fn jobid(&self) -> Option<u64>;
    /// On-disk JSON `kind` field. Used to silently skip snapshots of
    /// other kinds during dir scans. Must be a stable, ASCII-only token.
    fn kind() -> &'static str;
}

#[async_trait]
pub trait JobStateStore<S: JobSnapshot>: Send + Sync {
    async fn save(&self, snap: &S) -> Result<()>;
    async fn load(&self, uuid: Uuid) -> Result<Option<S>>;
    async fn list(&self) -> Result<Vec<S>>;

    async fn find_by_jobid(&self, jobid: u64) -> Result<Option<S>> {
        Ok(self
            .list()
            .await?
            .into_iter()
            .find(|s| s.jobid() == Some(jobid)))
    }
}

#[derive(Clone)]
pub struct InMemoryStateStore<S: JobSnapshot> {
    inner: Arc<Mutex<HashMap<Uuid, S>>>,
}

impl<S: JobSnapshot> Default for InMemoryStateStore<S> {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<S: JobSnapshot> InMemoryStateStore<S> {
    pub fn new() -> Self { Self::default() }
    pub fn len(&self) -> usize {
        self.inner.lock().expect("InMemoryStateStore poisoned").len()
    }
    pub fn is_empty(&self) -> bool { self.len() == 0 }
}

#[async_trait]
impl<S: JobSnapshot> JobStateStore<S> for InMemoryStateStore<S> {
    async fn save(&self, snap: &S) -> Result<()> {
        let mut g = self.inner.lock()
            .map_err(|_| anyhow!("InMemoryStateStore mutex poisoned"))?;
        g.insert(snap.uuid(), snap.clone());
        Ok(())
    }

    async fn load(&self, uuid: Uuid) -> Result<Option<S>> {
        let g = self.inner.lock()
            .map_err(|_| anyhow!("InMemoryStateStore mutex poisoned"))?;
        Ok(g.get(&uuid).cloned())
    }

    async fn list(&self) -> Result<Vec<S>> {
        let g = self.inner.lock()
            .map_err(|_| anyhow!("InMemoryStateStore mutex poisoned"))?;
        Ok(g.values().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Synthetic {
        uuid: Uuid,
        jobid: Option<u64>,
        payload: String,
    }

    impl JobSnapshot for Synthetic {
        fn uuid(&self) -> Uuid { self.uuid }
        fn jobid(&self) -> Option<u64> { self.jobid }
        fn kind() -> &'static str { "synthetic" }
    }

    fn snap(uuid: Uuid, jobid: Option<u64>) -> Synthetic {
        Synthetic { uuid, jobid, payload: "x".to_string() }
    }

    #[tokio::test]
    async fn in_memory_save_load_round_trip() {
        let store: InMemoryStateStore<Synthetic> = InMemoryStateStore::new();
        let s = snap(Uuid::now_v7(), Some(7));
        store.save(&s).await.unwrap();
        assert_eq!(store.load(s.uuid).await.unwrap(), Some(s));
    }

    #[tokio::test]
    async fn in_memory_list_returns_all() {
        let store: InMemoryStateStore<Synthetic> = InMemoryStateStore::new();
        let a = snap(Uuid::now_v7(), Some(1));
        let b = snap(Uuid::now_v7(), Some(2));
        store.save(&a).await.unwrap();
        store.save(&b).await.unwrap();
        let mut all = store.list().await.unwrap();
        all.sort_by_key(|s| s.jobid);
        assert_eq!(all, vec![a, b]);
    }

    #[tokio::test]
    async fn in_memory_find_by_jobid_uses_default_impl() {
        let store: InMemoryStateStore<Synthetic> = InMemoryStateStore::new();
        let a = snap(Uuid::now_v7(), Some(100));
        store.save(&a).await.unwrap();
        assert_eq!(store.find_by_jobid(100).await.unwrap(), Some(a));
        assert_eq!(store.find_by_jobid(999).await.unwrap(), None);
    }

    #[tokio::test]
    async fn in_memory_save_overwrites_same_uuid() {
        let store: InMemoryStateStore<Synthetic> = InMemoryStateStore::new();
        let uuid = Uuid::now_v7();
        let mut s = snap(uuid, None);
        store.save(&s).await.unwrap();
        s.payload = "y".to_string();
        store.save(&s).await.unwrap();
        assert_eq!(store.load(uuid).await.unwrap().unwrap().payload, "y");
        assert_eq!(store.len(), 1);
    }
}
```

Modify `src/lib.rs` — add `pub mod store;` after the existing `pub mod entities;` (line 1-2):

```rust
pub mod entities;
pub mod error;
pub mod store;
```

- [ ] **Step 1.2: Run tests to verify they pass**

```bash
cd /home/kiyama/programs/research/GAUSSIAN_repo_packages/slurm-async-runner2/.dmux/worktrees/sbatch-module
cargo test --lib store::tests -- --nocapture
```
Expected: 4 tests pass.

- [ ] **Step 1.3: Commit**

```bash
git add src/store.rs src/lib.rs
git commit -m "feat(store): add generic JobSnapshot trait + InMemoryStateStore<S>"
```

---

## Task 2: FileSystemStateStore<S> with kind discriminator

**Files:**
- Modify: `src/store.rs` (append `FileSystemStateStore<S>` and tests)

- [ ] **Step 2.1: Append the impl**

Add to `src/store.rs` after the `InMemoryStateStore` impl block, BEFORE the `#[cfg(test)] mod tests`:

```rust
/// On-disk store: writes `{root}/<uuid>.json` via atomic rename, with a
/// top-level `"kind"` field added on save and verified on load. Files
/// whose `kind` does not match `S::kind()` are silently skipped during
/// scans, so multiple snapshot types may coexist in the same `root`.
///
/// The directory is created lazily on first `save`. A *missing* directory
/// during scan is treated as "no entries" (returns empty vec / Ok(None)).
pub struct FileSystemStateStore<S: JobSnapshot> {
    root: PathBuf,
    _phantom: PhantomData<S>,
}

impl<S: JobSnapshot> FileSystemStateStore<S> {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into(), _phantom: PhantomData }
    }

    pub fn root(&self) -> &Path { &self.root }

    fn path_for(&self, uuid: Uuid) -> PathBuf {
        self.root.join(format!("{uuid}.json"))
    }
}

#[async_trait]
impl<S: JobSnapshot> JobStateStore<S> for FileSystemStateStore<S> {
    async fn save(&self, snap: &S) -> Result<()> {
        let path = self.path_for(snap.uuid());
        let root = self.root.clone();
        let snap = snap.clone();
        tokio::task::spawn_blocking(move || write_atomic_json(&root, &path, &snap))
            .await
            .map_err(|e| anyhow!("save spawn_blocking join failed: {e}"))?
    }

    async fn load(&self, uuid: Uuid) -> Result<Option<S>> {
        let path = self.path_for(uuid);
        let bytes = match tokio::fs::read(&path).await {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
        };
        decode_with_kind_check::<S>(&bytes, &path)
    }

    async fn list(&self) -> Result<Vec<S>> {
        let mut rd = match tokio::fs::read_dir(&self.root).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e).with_context(|| format!("read_dir {}", self.root.display())),
        };
        let mut out = Vec::new();
        while let Some(entry) = rd.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let bytes = match tokio::fs::read(&path).await {
                Ok(b) => b,
                Err(_) => continue,
            };
            if let Ok(Some(snap)) = decode_with_kind_check::<S>(&bytes, &path) {
                out.push(snap);
            }
        }
        Ok(out)
    }
}

/// Decode JSON bytes as `S`, but only if the on-disk `kind` field matches
/// `S::kind()`. Legacy fallback: a missing `kind` is treated as `S::kind()`
/// for back-compat with snapshots written by older code.
fn decode_with_kind_check<S: JobSnapshot>(bytes: &[u8], path: &Path) -> Result<Option<S>> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).with_context(|| format!("decode {}", path.display()))?;
    let on_disk_kind = value.get("kind").and_then(|v| v.as_str());
    let expected = S::kind();
    let kind_ok = match on_disk_kind {
        Some(k) => k == expected,
        None => true,
    };
    if !kind_ok {
        return Ok(None);
    }
    let snap: S = serde_json::from_value(value)
        .with_context(|| format!("decode body of {}", path.display()))?;
    Ok(Some(snap))
}

fn write_atomic_json<S: JobSnapshot>(root: &Path, path: &Path, snap: &S) -> Result<()> {
    std::fs::create_dir_all(root)
        .with_context(|| format!("mkdir -p {}", root.display()))?;
    let mut value =
        serde_json::to_value(snap).with_context(|| "serialize snapshot to json".to_string())?;
    let obj = value
        .as_object_mut()
        .ok_or_else(|| anyhow!("snapshot did not serialize to a JSON object"))?;
    obj.insert("kind".to_string(), serde_json::Value::String(S::kind().to_string()));
    let mut tmp = tempfile::NamedTempFile::new_in(root)
        .with_context(|| format!("tempfile in {}", root.display()))?;
    serde_json::to_writer_pretty(&mut tmp, &value)
        .with_context(|| "write json to tempfile".to_string())?;
    tmp.persist(path)
        .map_err(|e| anyhow!("persist {}: {e}", path.display()))?;
    Ok(())
}
```

- [ ] **Step 2.2: Add FS-specific tests inside the existing `#[cfg(test)] mod tests` block**

```rust
    // ---------- FileSystemStateStore ----------

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct OtherKind {
        uuid: Uuid,
        payload: String,
    }

    impl JobSnapshot for OtherKind {
        fn uuid(&self) -> Uuid { self.uuid }
        fn jobid(&self) -> Option<u64> { None }
        fn kind() -> &'static str { "other" }
    }

    #[tokio::test]
    async fn fs_save_load_round_trip_with_kind_field() {
        let tmp = tempfile::tempdir().unwrap();
        let store: FileSystemStateStore<Synthetic> = FileSystemStateStore::new(tmp.path());
        let s = snap(Uuid::now_v7(), Some(7));
        store.save(&s).await.unwrap();

        let path = tmp.path().join(format!("{}.json", s.uuid));
        let raw = std::fs::read_to_string(&path).unwrap();
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v.get("kind").and_then(|x| x.as_str()), Some("synthetic"));

        assert_eq!(store.load(s.uuid).await.unwrap(), Some(s));
    }

    #[tokio::test]
    async fn fs_load_returns_none_for_wrong_kind() {
        let tmp = tempfile::tempdir().unwrap();
        let synth_store: FileSystemStateStore<Synthetic> = FileSystemStateStore::new(tmp.path());
        let other_store: FileSystemStateStore<OtherKind> = FileSystemStateStore::new(tmp.path());
        let other = OtherKind { uuid: Uuid::now_v7(), payload: "z".to_string() };
        other_store.save(&other).await.unwrap();
        assert_eq!(synth_store.load(other.uuid).await.unwrap(), None);
        assert_eq!(other_store.load(other.uuid).await.unwrap(), Some(other));
    }

    #[tokio::test]
    async fn fs_list_filters_by_kind_in_shared_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let synth_store: FileSystemStateStore<Synthetic> = FileSystemStateStore::new(tmp.path());
        let other_store: FileSystemStateStore<OtherKind> = FileSystemStateStore::new(tmp.path());
        synth_store.save(&snap(Uuid::now_v7(), Some(1))).await.unwrap();
        synth_store.save(&snap(Uuid::now_v7(), Some(2))).await.unwrap();
        other_store
            .save(&OtherKind { uuid: Uuid::now_v7(), payload: "z".to_string() })
            .await
            .unwrap();
        assert_eq!(synth_store.list().await.unwrap().len(), 2);
        assert_eq!(other_store.list().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn fs_save_creates_directory_lazily() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("nested/does/not/exist");
        let store: FileSystemStateStore<Synthetic> = FileSystemStateStore::new(&dir);
        assert!(!dir.exists());
        store.save(&snap(Uuid::now_v7(), None)).await.unwrap();
        assert!(dir.exists());
    }

    #[tokio::test]
    async fn fs_list_returns_empty_when_dir_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("never-created");
        let store: FileSystemStateStore<Synthetic> = FileSystemStateStore::new(&dir);
        assert!(store.list().await.unwrap().is_empty());
        assert!(store.find_by_jobid(123).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn fs_legacy_file_without_kind_field_is_loaded() {
        let tmp = tempfile::tempdir().unwrap();
        let uuid = Uuid::now_v7();
        let s = snap(uuid, Some(50));
        let raw = serde_json::to_string_pretty(&s).unwrap();
        std::fs::write(tmp.path().join(format!("{uuid}.json")), raw).unwrap();
        let store: FileSystemStateStore<Synthetic> = FileSystemStateStore::new(tmp.path());
        assert_eq!(store.load(uuid).await.unwrap(), Some(s));
    }
```

- [ ] **Step 2.3: Run tests**

```bash
cargo test --lib store::tests -- --nocapture
```
Expected: 10 tests pass (4 in-memory + 6 FS).

- [ ] **Step 2.4: Commit**

```bash
git add src/store.rs
git commit -m "feat(store): add FileSystemStateStore<S> with JSON kind discriminator"
```

---

## Task 3: Migrate tssrun::JobHandleSnapshot to use generic store

**Files:**
- Replace: `src/tssrun/store.rs` (shrink to JobSnapshot impl + back-compat aliases)
- Modify: `src/tssrun/handle.rs` (line 52 import + every `Arc<dyn JobStateStore>` → `Arc<dyn JobStateStore<JobHandleSnapshot>>`)
- Modify: `src/tssrun/manager.rs` (same trait-object replacements)

- [ ] **Step 3.1: Replace `src/tssrun/store.rs` with the shrunken version**

```rust
//! `JobHandleSnapshot` participation in the generic [`JobStateStore`] layer.
//!
//! See `docs/superpowers/specs/2026-05-10-sbatch-module-design.md` §5
//! for why this module shrunk from a full trait + two impls to just the
//! `JobSnapshot` trait impl + back-compat type aliases.

use uuid::Uuid;

use crate::store::JobSnapshot;
use crate::tssrun::handle::JobHandleSnapshot;

impl JobSnapshot for JobHandleSnapshot {
    fn uuid(&self) -> Uuid { self.uuid }
    fn jobid(&self) -> Option<u64> { self.jobid }
    fn kind() -> &'static str { "tssrun" }
}

// Back-compat aliases.
pub type JobStateStore = dyn crate::store::JobStateStore<JobHandleSnapshot>;
pub type InMemoryStateStore = crate::store::InMemoryStateStore<JobHandleSnapshot>;
pub type FileSystemStateStore = crate::store::FileSystemStateStore<JobHandleSnapshot>;

/// Tssrun-specific helper: scan the store for the first snapshot whose
/// `pid` equals `pid`. Built on `list()` because `pid` is not a generic
/// `JobSnapshot` concept.
pub async fn find_by_pid(
    store: &(dyn crate::store::JobStateStore<JobHandleSnapshot>),
    pid: u32,
) -> anyhow::Result<Option<JobHandleSnapshot>> {
    Ok(store.list().await?.into_iter().find(|s| s.pid == pid))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::JobStateStore as _;
    use crate::tssrun::handle::LogLocations;

    fn snap(uuid: Uuid, pid: u32, jobid: Option<u64>) -> JobHandleSnapshot {
        JobHandleSnapshot {
            uuid, pid,
            argv: vec![],
            sent_env: Default::default(),
            cwd: None,
            started_at_unix: 0,
            log_locations: LogLocations::None,
            jobid,
            node: None,
            finished: None,
        }
    }

    #[tokio::test]
    async fn tssrun_snapshot_round_trips_via_fs_store() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileSystemStateStore::new(tmp.path());
        let s = snap(Uuid::now_v7(), 42, Some(7));
        store.save(&s).await.unwrap();
        assert_eq!(store.load(s.uuid).await.unwrap(), Some(s.clone()));

        // Confirm the kind discriminator was written.
        let path = tmp.path().join(format!("{}.json", s.uuid));
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(raw.contains("\"kind\""));
        assert!(raw.contains("\"tssrun\""));
    }

    #[tokio::test]
    async fn tssrun_find_by_pid_helper_works() {
        let store = InMemoryStateStore::new();
        let a = snap(Uuid::now_v7(), 100, Some(1));
        let b = snap(Uuid::now_v7(), 200, Some(2));
        store.save(&a).await.unwrap();
        store.save(&b).await.unwrap();
        assert_eq!(find_by_pid(&store, 200).await.unwrap(), Some(b));
        assert!(find_by_pid(&store, 999).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn tssrun_find_by_jobid_uses_default_trait_impl() {
        let store = InMemoryStateStore::new();
        let a = snap(Uuid::now_v7(), 1, Some(50));
        store.save(&a).await.unwrap();
        assert_eq!(store.find_by_jobid(50).await.unwrap(), Some(a));
    }
}
```

- [ ] **Step 3.2: Update `src/tssrun/handle.rs` line 52 and all trait object references**

In `src/tssrun/handle.rs`:
1. Line 52: replace `use crate::tssrun::store::JobStateStore;` with `use crate::store::JobStateStore;`
2. Replace **every** occurrence of `Arc<dyn JobStateStore>` with `Arc<dyn JobStateStore<JobHandleSnapshot>>`. Confirm the locations:

```bash
grep -n "JobStateStore" src/tssrun/handle.rs
```

3. Replace `&dyn JobStateStore` with `&dyn JobStateStore<JobHandleSnapshot>` on the `persist_warn` signature (around line 435).

You can do this with a targeted sed:

```bash
sed -i 's/Arc<dyn JobStateStore>/Arc<dyn JobStateStore<JobHandleSnapshot>>/g; s/&dyn JobStateStore[^<]/\&dyn JobStateStore<JobHandleSnapshot> /g' src/tssrun/handle.rs
```

Then re-verify by running `grep -n "JobStateStore" src/tssrun/handle.rs` and adjusting any malformed substitutions manually.

- [ ] **Step 3.3: Update `src/tssrun/manager.rs`**

```bash
grep -n "JobStateStore" src/tssrun/manager.rs
sed -i 's/Arc<dyn JobStateStore>/Arc<dyn JobStateStore<JobHandleSnapshot>>/g' src/tssrun/manager.rs
```

Confirm the import line at the top of manager.rs is `use crate::store::JobStateStore;` (or via re-export from `crate::tssrun::store::JobStateStore` which is now a type alias). The existing `use crate::tssrun::store::JobStateStore;` will still work because of the back-compat type alias.

If `manager.rs` uses `Arc<dyn JobStateStore + ?Sized>` or other variants, adjust those too.

- [ ] **Step 3.4: Run cargo check**

```bash
cargo check --lib
```
Expected: clean. Fix any "expected `dyn ...<JobHandleSnapshot>` found `dyn ...`" errors by adding the missing `<JobHandleSnapshot>` parameter.

- [ ] **Step 3.5: Run all tssrun tests**

```bash
cargo test --lib tssrun:: -- --nocapture
```
Expected: existing tests pass + 3 new tests in `tssrun::store::tests` pass.

- [ ] **Step 3.6: Commit**

```bash
git add src/tssrun/store.rs src/tssrun/handle.rs src/tssrun/manager.rs
git commit -m "refactor(tssrun): migrate JobHandleSnapshot to generic JobStateStore<S>"
```

---

## Task 4: Refactor tssrun lifecycle helpers (move logic to snapshot)

**Files:**
- Modify: `src/tssrun/handle.rs` (add `impl JobHandleSnapshot { is_running, is_finished, exit_code }`; existing `JobHandle::is_running` etc. become delegates)

- [ ] **Step 4.1: Add the helper impls to `JobHandleSnapshot`**

In `src/tssrun/handle.rs`, after the `JobHandleSnapshot` struct definition (around line 92), add:

```rust
impl JobHandleSnapshot {
    /// True while the child process is still alive (no `finished` recorded).
    pub fn is_running(&self) -> bool {
        self.finished.is_none()
    }

    /// True once the child has exited (regardless of how).
    pub fn is_finished(&self) -> bool {
        self.finished.is_some()
    }

    /// Exit code if the child exited normally; `None` if killed by signal
    /// or if `finished` is not yet recorded.
    pub fn exit_code(&self) -> Option<i32> {
        self.finished.as_ref().and_then(|f| f.exit_code)
    }
}
```

- [ ] **Step 4.2: Replace `JobHandle::is_running` and `JobHandle::exit_code` with delegates**

Locate the existing methods (around lines 233-242) in the `impl JobHandle` block. Replace them with:

```rust
    pub fn is_running(&self) -> bool {
        self.snapshot_rx.borrow().is_running()
    }

    pub fn is_finished(&self) -> bool {
        self.snapshot_rx.borrow().is_finished()
    }

    pub fn exit_code(&self) -> Option<i32> {
        self.snapshot_rx.borrow().exit_code()
    }
```

- [ ] **Step 4.3: Add tests for the snapshot-level helpers**

Add to the `#[cfg(test)] mod tests` block in `src/tssrun/handle.rs`:

```rust
    #[test]
    fn snapshot_is_running_when_finished_is_none() {
        let s = snap_running();
        assert!(s.is_running());
        assert!(!s.is_finished());
        assert_eq!(s.exit_code(), None);
    }

    #[test]
    fn snapshot_is_finished_after_normal_exit() {
        let mut s = snap_running();
        s.finished = Some(FinishedInfo { exit_code: Some(0), finished_at_unix: 1 });
        assert!(!s.is_running());
        assert!(s.is_finished());
        assert_eq!(s.exit_code(), Some(0));
    }

    #[test]
    fn snapshot_signal_killed_has_none_exit_code() {
        let mut s = snap_running();
        s.finished = Some(FinishedInfo { exit_code: None, finished_at_unix: 1 });
        assert!(!s.is_running());
        assert!(s.is_finished());
        assert_eq!(s.exit_code(), None);
    }
```

- [ ] **Step 4.4: Run all tssrun tests**

```bash
cargo test --lib tssrun:: -- --nocapture
```
Expected: existing tests still pass + 3 new pass.

- [ ] **Step 4.5: Commit**

```bash
git add src/tssrun/handle.rs
git commit -m "refactor(tssrun): move is_running/is_finished/exit_code to snapshot helpers"
```

---

## Task 5: parse_qgroup_l + qgroup/squeue-only query fns in runner.rs

**Files:**
- Modify: `src/runner.rs` (append parser + 2 query functions + tests)

> **Implementation note:** the exact column layout of `qgroup -l` requires empirical verification on KUDPC. The plan uses a tolerant whitespace-split parser that takes JOBID at field index 2 and STATUS at field index 3 — see comments. If the actual format differs, the parser is the only thing to update.

- [ ] **Step 5.1: Append parser + query functions to `src/runner.rs`**

Add after the existing parser functions (after `parse_squeue` / `parse_sacct` definitions):

```rust
/// Parse `qgroup -l` output into a `{jobid: JobStatus}` map.
///
/// Expected layout (KUDPC):
/// ```text
/// QUEUE     USER     JOBID          STATUS  PROC  CORE    MEM    ELAPSE(    limit)
/// gr19999b  b59999   12345          RUN        4     1  4570M  00:00:07( 01:00:00)
/// ```
///
/// Behaviour:
/// - Whitespace-split each line; take field index 2 as JOBID and 3 as STATUS.
/// - Lines without at least 4 fields are skipped (header, blanks).
/// - Lines whose JOBID field is not a valid u64 are skipped.
/// - State strings are forwarded to `JobState::parse` (handles "RUN", "QUE",
///   "CMP", and SLURM long forms thanks to forward-compat fallbacks).
/// - Reason is set to `JobReason::None` (qgroup -l does not surface reasons).
pub fn parse_qgroup_l(stdout: &str) -> HashMap<u64, JobStatus> {
    let mut out = HashMap::new();
    for line in stdout.lines() {
        let mut fields = line.split_whitespace();
        let _queue = fields.next();
        let _user = fields.next();
        let jobid_str = match fields.next() {
            Some(s) => s,
            None => continue,
        };
        let state_str = match fields.next() {
            Some(s) => s,
            None => continue,
        };
        let Ok(jobid) = jobid_str.parse::<u64>() else {
            continue;
        };
        out.insert(
            jobid,
            JobStatus { state: JobState::parse(state_str), reason: JobReason::None },
        );
    }
    out
}

/// Bulk-query KUDPC's `qgroup -l` for the given `jobids`. Cheap (KUDPC
/// docs do not flag it as system-intensive). Returns only the jobids that
/// `qgroup -l` reports; missing ids are simply absent from the map (caller
/// decides whether to fall back to squeue / sacct).
pub async fn query_job_states_via_qgroup_with<D: JobDispatcher>(
    dispatcher: &D,
    jobids: &[u64],
) -> Result<HashMap<u64, JobStatus>> {
    if jobids.is_empty() { return Ok(HashMap::new()); }
    let argv = vec!["qgroup".to_string(), "-l".to_string()];
    let (_, stdout) = dispatcher.capture(&argv).await?;
    let all = parse_qgroup_l(&stdout);
    let wanted: HashSet<u64> = jobids.iter().copied().collect();
    Ok(all.into_iter().filter(|(k, _)| wanted.contains(k)).collect())
}

/// Like [`query_job_states_batch_with`] but **squeue only**, no sacct
/// fallback. Returns only the jobids squeue reports; missing ids are
/// absent from the map.
pub async fn query_job_states_squeue_only_with<D: JobDispatcher>(
    dispatcher: &D,
    jobids: &[u64],
) -> Result<HashMap<u64, JobStatus>> {
    if jobids.is_empty() { return Ok(HashMap::new()); }
    let unique = dedupe_preserving_order(jobids);
    let argv = vec![
        "squeue".to_string(),
        "-h".to_string(),
        "-j".to_string(),
        csv_join(&unique),
        "-o".to_string(),
        "%i %T %r".to_string(),
    ];
    let (_, out) = dispatcher.capture(&argv).await?;
    Ok(parse_squeue(&out))
}
```

- [ ] **Step 5.2: Add unit tests**

Append to the existing `#[cfg(test)] mod tests` in `src/runner.rs`:

```rust
    #[test]
    fn parse_qgroup_l_extracts_jobid_and_state() {
        let out = "\
QUEUE     USER     JOBID          STATUS  PROC  CORE    MEM    ELAPSE(    limit)
gr19999b  b59999   12345          RUN        4     1  4570M  00:00:07( 01:00:00)
gr19999b  b59999   12346          QUE        1     1   100M  00:00:00( 00:30:00)
";
        let map = super::parse_qgroup_l(out);
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&12345).unwrap().state, JobState::Running);
        assert_eq!(map.get(&12346).unwrap().state, JobState::Pending);
    }

    #[test]
    fn parse_qgroup_l_skips_blank_and_short_lines() {
        let out = "\
QUEUE USER JOBID STATUS

short
gr19999b u 9999 RUN 1 1 1M 0:0:1(0:1:0)
";
        let map = super::parse_qgroup_l(out);
        assert_eq!(map.len(), 1);
        assert!(map.contains_key(&9999));
    }

    #[test]
    fn parse_qgroup_l_handles_completed_state() {
        let out = "\
QUEUE USER JOBID STATUS PROC
gr19999b u 5555 CMP 1
";
        let map = super::parse_qgroup_l(out);
        assert_eq!(map.get(&5555).map(|s| s.state.clone()), Some(JobState::Completed));
    }
```

- [ ] **Step 5.3: Run tests**

```bash
cargo test --lib runner -- --nocapture
```
Expected: existing tests + 3 new tests pass.

- [ ] **Step 5.4: Commit**

```bash
git add src/runner.rs
git commit -m "feat(runner): add parse_qgroup_l + qgroup/squeue-only query fns"
```

---

## Task 6: SbatchCmd Spec layer

**Files:**
- Create: `src/sbatch/mod.rs`
- Create: `src/sbatch/cmd.rs`
- Create (empty): `src/sbatch/parse.rs`, `src/sbatch/handle.rs`, `src/sbatch/store.rs`, `src/sbatch/manager.rs`, `src/sbatch/error.rs`
- Modify: `src/lib.rs` (add `pub mod sbatch;`)

- [ ] **Step 6.1: Create the sbatch module skeleton**

```bash
mkdir -p src/sbatch
touch src/sbatch/parse.rs src/sbatch/handle.rs src/sbatch/store.rs src/sbatch/manager.rs src/sbatch/error.rs
```

Create `src/sbatch/mod.rs`:

```rust
//! sbatch — fire-and-forget SLURM batch submission with KUDPC-aware polling.
//!
//! See `docs/superpowers/specs/2026-05-10-sbatch-module-design.md` for
//! the full design rationale (Approach A: passive handle + watch).

pub mod cmd;
pub mod error;
pub mod handle;
pub mod manager;
pub mod parse;
pub mod store;
```

Modify `src/lib.rs` — add `pub mod sbatch;` after `pub mod runner;`:

```rust
pub mod dispatcher;
pub mod manager;
pub mod runner;
pub mod sbatch;
pub mod tssrun;
```

- [ ] **Step 6.2: Write `src/sbatch/cmd.rs` with full implementation + tests**

```rust
//! Pure-data spec for one `sbatch` invocation.
//!
//! No I/O — all subprocess work is in [`crate::sbatch::manager::SbatchManager`]
//! / [`crate::dispatcher::JobDispatcher`]. The argv is laid out so that
//! `#SBATCH` directives in the script (which sbatch parses on its own)
//! are still respected; CLI flags only override per the sbatch convention.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::entities::slurm::{JobPartition, JobTimeLimit, ResourceSpec};

#[derive(Debug, Clone)]
pub struct SbatchCmd {
    pub sbatch_bin: String,

    pub job_name: Option<String>,
    pub partition: Option<JobPartition>,

    pub time_limit: Option<JobTimeLimit>,
    pub rsc: Option<ResourceSpec>,

    pub output: Option<String>,
    pub error: Option<String>,
    pub chdir: Option<PathBuf>,

    pub env: HashMap<String, String>,

    pub script: PathBuf,
    pub args: Vec<String>,
}

impl SbatchCmd {
    pub fn new(script: impl Into<PathBuf>) -> Self {
        Self {
            sbatch_bin: "sbatch".to_string(),
            job_name: None,
            partition: None,
            time_limit: None,
            rsc: None,
            output: None,
            error: None,
            chdir: None,
            env: HashMap::new(),
            script: script.into(),
            args: Vec::new(),
        }
    }

    pub fn build_argv(&self) -> Result<Vec<String>> {
        let mut argv = Vec::with_capacity(16 + self.args.len());
        argv.push(self.sbatch_bin.clone());

        if let Some(name) = &self.job_name {
            argv.push("-J".to_string());
            argv.push(name.clone());
        }
        if let Some(p) = &self.partition {
            argv.push("-p".to_string());
            argv.push(p.clone());
        }
        if let Some(t) = &self.time_limit {
            argv.push("-t".to_string());
            argv.push(t.to_string());
        }
        if let Some(r) = &self.rsc {
            let spec = r.to_string();
            if !spec.is_empty() {
                argv.push("--rsc".to_string());
                argv.push(spec);
            }
        }
        if let Some(o) = &self.output {
            argv.push("-o".to_string());
            argv.push(o.clone());
        }
        if let Some(e) = &self.error {
            argv.push("-e".to_string());
            argv.push(e.clone());
        }
        if let Some(c) = &self.chdir {
            argv.push("--chdir".to_string());
            argv.push(absolutize(c)?);
        }
        if !self.env.is_empty() {
            argv.push(format!("--export={}", render_export(&self.env)));
        }
        argv.push(absolutize(&self.script)?);
        argv.extend(self.args.iter().cloned());
        Ok(argv)
    }
}

fn absolutize(p: &Path) -> Result<String> {
    let abs = std::path::absolute(p)
        .with_context(|| format!("failed to absolutize {}", p.display()))?;
    abs.into_os_string()
        .into_string()
        .map_err(|os| anyhow::anyhow!("non-UTF8 path: {os:?}"))
}

/// Render `--export=ALL,K1=V1,K2=V2,...` with deterministic key order
/// so argv is reproducible.
fn render_export(env: &HashMap<String, String>) -> String {
    let mut keys: Vec<&String> = env.keys().collect();
    keys.sort();
    let mut out = String::from("ALL");
    for k in keys {
        out.push(',');
        out.push_str(k);
        out.push('=');
        out.push_str(&env[k]);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entities::slurm::{ResourceSpecCPU, ResourceSpecGPU};
    use std::num::NonZeroU32;

    #[test]
    fn minimal_argv_is_bin_then_script() {
        let cmd = SbatchCmd::new("/work/job.sh");
        let argv = cmd.build_argv().unwrap();
        assert_eq!(argv, vec!["sbatch".to_string(), "/work/job.sh".to_string()]);
    }

    #[test]
    fn relative_script_is_absolutized() {
        let cmd = SbatchCmd::new("job.sh");
        let argv = cmd.build_argv().unwrap();
        let cwd = std::env::current_dir().unwrap();
        assert_eq!(argv[0], "sbatch");
        assert_eq!(argv[1], format!("{}/job.sh", cwd.display()));
    }

    #[test]
    fn full_flags_cpu_variant_argv_layout() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.job_name = Some("g09run".into());
        cmd.partition = Some("gr19999b".into());
        cmd.time_limit = Some("1:0:0".parse().unwrap());
        cmd.rsc = Some(ResourceSpec::CPU(ResourceSpecCPU {
            p: NonZeroU32::new(4),
            t: NonZeroU32::new(8),
            c: NonZeroU32::new(8),
            m: Some("2G".parse().unwrap()),
        }));
        cmd.output = Some("slurm-%j.out".into());
        cmd.error = Some("slurm-%j.err".into());
        cmd.chdir = Some(PathBuf::from("/w"));
        cmd.env.insert("OMP_NUM_THREADS".into(), "8".into());
        cmd.env.insert("FOO".into(), "bar".into());
        cmd.args = vec!["--flag".into(), "v".into()];
        let argv = cmd.build_argv().unwrap();
        assert_eq!(argv, vec![
            "sbatch".to_string(),
            "-J".into(), "g09run".into(),
            "-p".into(), "gr19999b".into(),
            "-t".into(), "01:00:00".into(),
            "--rsc".into(), "p=4:t=8:c=8:m=2G".into(),
            "-o".into(), "slurm-%j.out".into(),
            "-e".into(), "slurm-%j.err".into(),
            "--chdir".into(), "/w".into(),
            "--export=ALL,FOO=bar,OMP_NUM_THREADS=8".into(),
            "/w/job.sh".into(),
            "--flag".into(), "v".into(),
        ]);
    }

    #[test]
    fn empty_env_omits_export_flag() {
        let cmd = SbatchCmd::new("/w/job.sh");
        let argv = cmd.build_argv().unwrap();
        assert!(!argv.iter().any(|a| a.starts_with("--export")));
    }

    #[test]
    fn rsc_empty_cpu_omits_rsc_flag() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.rsc = Some(ResourceSpec::CPU(ResourceSpecCPU::default()));
        let argv = cmd.build_argv().unwrap();
        assert!(!argv.iter().any(|a| a == "--rsc"));
    }

    #[test]
    fn gpu_variant_renders_g_flag() {
        let mut cmd = SbatchCmd::new("/w/job.sh");
        cmd.rsc = Some(ResourceSpec::GPU(ResourceSpecGPU {
            g: NonZeroU32::new(1).unwrap(),
        }));
        let argv = cmd.build_argv().unwrap();
        assert!(argv.contains(&"--rsc".to_string()));
        assert!(argv.contains(&"g=1".to_string()));
    }
}
```

- [ ] **Step 6.3: Run tests**

```bash
cargo test --lib sbatch::cmd -- --nocapture
```
Expected: 6 tests pass.

- [ ] **Step 6.4: Commit**

```bash
git add src/sbatch/ src/lib.rs
git commit -m "feat(sbatch): add SbatchCmd Spec layer with build_argv"
```

---

## Task 7: parse_submitted_jobid + resolve_log_path

**Files:**
- Replace: `src/sbatch/parse.rs` (currently empty)

- [ ] **Step 7.1: Write parse.rs**

```rust
//! Pure parsers / formatters for the sbatch module — no I/O.

use std::path::PathBuf;

/// Parse the jobid from an `sbatch` submission's stdout.
///
/// Typical output: `Submitted batch job 12345`. The line may be embedded
/// among other lines (warnings, multi-cluster output `Submitted batch job
/// 12345 on cluster X`, array form `Submitted batch job 12345_0`).
/// First match wins; trailing non-digit chars are stripped, so for array
/// forms the parent jobid is returned (Phase 1 simplification).
/// Returns `None` if no line matches.
pub fn parse_submitted_jobid(stdout: &str) -> Option<u64> {
    for line in stdout.lines() {
        let line = line.trim();
        let prefix = "Submitted batch job ";
        if let Some(rest) = line.strip_prefix(prefix) {
            let id_str: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
            if !id_str.is_empty() {
                if let Ok(id) = id_str.parse::<u64>() {
                    return Some(id);
                }
            }
        }
    }
    None
}

/// Lenient SLURM `-o`/`-e` template substitution.
///
/// Phase 1 expands `%j` (jobid) and, when `job_name` is `Some`, `%x`.
/// Other tokens (`%A`, `%a`, `%u`, `%N`) are preserved verbatim — caller
/// can detect "still has unresolved variables" by checking for `%` in the
/// returned path.
pub fn resolve_log_path(template: &str, jobid: u64, job_name: Option<&str>) -> PathBuf {
    let mut s = template.to_string();
    s = s.replace("%j", &jobid.to_string());
    if let Some(name) = job_name {
        s = s.replace("%x", name);
    }
    PathBuf::from(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- parse_submitted_jobid ----

    #[test]
    fn parses_clean_single_line() {
        assert_eq!(
            parse_submitted_jobid("Submitted batch job 12345\n"),
            Some(12345)
        );
    }

    #[test]
    fn parses_with_leading_warning() {
        let out = "\
sbatch: warning: ...
Submitted batch job 67890
";
        assert_eq!(parse_submitted_jobid(out), Some(67890));
    }

    #[test]
    fn parses_multi_cluster_form() {
        let out = "Submitted batch job 42 on cluster cluster1\n";
        assert_eq!(parse_submitted_jobid(out), Some(42));
    }

    #[test]
    fn parses_array_form_takes_parent_id() {
        let out = "Submitted batch job 12345_0\n";
        assert_eq!(parse_submitted_jobid(out), Some(12345));
    }

    #[test]
    fn returns_none_when_no_match() {
        assert_eq!(parse_submitted_jobid(""), None);
        assert_eq!(parse_submitted_jobid("error: bad partition\n"), None);
    }

    // ---- resolve_log_path ----

    #[test]
    fn resolve_substitutes_jobid_only() {
        let p = resolve_log_path("slurm-%j.out", 12345, None);
        assert_eq!(p, PathBuf::from("slurm-12345.out"));
    }

    #[test]
    fn resolve_substitutes_jobname_when_some() {
        let p = resolve_log_path("%x-%j.out", 12345, Some("g09run"));
        assert_eq!(p, PathBuf::from("g09run-12345.out"));
    }

    #[test]
    fn resolve_leaves_jobname_token_when_none() {
        let p = resolve_log_path("%x-%j.out", 12345, None);
        assert_eq!(p, PathBuf::from("%x-12345.out"));
    }

    #[test]
    fn resolve_leaves_unsupported_tokens_raw() {
        let p = resolve_log_path("%A_%a-%u-%N-%j.out", 999, Some("nm"));
        assert_eq!(p, PathBuf::from("%A_%a-%u-%N-999.out"));
    }
}
```

- [ ] **Step 7.2: Run tests**

```bash
cargo test --lib sbatch::parse -- --nocapture
```
Expected: 9 tests pass.

- [ ] **Step 7.3: Commit**

```bash
git add src/sbatch/parse.rs
git commit -m "feat(sbatch): add parse_submitted_jobid + resolve_log_path"
```

---

## Task 8: SbatchJobSnapshot + SbatchLifecycle + JobSnapshot impl

**Files:**
- Replace: `src/sbatch/handle.rs`
- Replace: `src/sbatch/store.rs`

- [ ] **Step 8.1: Write handle.rs (snapshot data types only — handle struct is appended in Task 9)**

```rust
//! `SbatchJobSnapshot` and supporting lifecycle types. The runtime
//! handle (`SbatchJobHandle`) is appended in Task 9.
//!
//! See `docs/superpowers/specs/2026-05-10-sbatch-module-design.md` §6
//! and §9 for the full design.

use std::collections::HashMap;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::entities::slurm::JobPartition;
use crate::sbatch::parse::resolve_log_path;
use crate::{JobReason, JobState, JobStatus};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SbatchJobSnapshot {
    pub uuid: Uuid,
    pub jobid: u64,

    pub argv: Vec<String>,
    pub sent_env: HashMap<String, String>,
    pub script_path: PathBuf,
    pub chdir: Option<PathBuf>,
    pub partition: Option<JobPartition>,
    pub job_name: Option<String>,
    pub submitted_at: DateTime<Utc>,

    pub log: LogPathSpec,

    pub lifecycle: SbatchLifecycle,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogPathSpec {
    pub output_template: Option<String>,
    pub error_template: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SbatchLifecycle {
    pub last_observed_state: Option<JobStatus>,
    pub last_observed_at: Option<DateTime<Utc>>,
    pub left_active_listing: bool,
    pub finished: Option<FinishedInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinishedInfo {
    pub final_state: JobState,
    pub final_reason: JobReason,
    pub exit_code: Option<i32>,
    pub finished_at: DateTime<Utc>,
}

impl SbatchLifecycle {
    pub fn is_running(&self) -> bool {
        if self.left_active_listing { return false; }
        self.last_observed_state
            .as_ref()
            .map(|s| s.state.is_running())
            .unwrap_or(false)
    }

    pub fn is_finished(&self) -> bool { self.finished.is_some() }

    pub fn exit_code(&self) -> Option<i32> {
        self.finished.as_ref().and_then(|f| f.exit_code)
    }
}

impl SbatchJobSnapshot {
    pub fn output_path(&self) -> Option<PathBuf> {
        self.log.output_template.as_deref()
            .map(|t| resolve_log_path(t, self.jobid, self.job_name.as_deref()))
    }

    pub fn error_path(&self) -> Option<PathBuf> {
        self.log.error_template.as_deref()
            .map(|t| resolve_log_path(t, self.jobid, self.job_name.as_deref()))
    }

    pub fn is_running(&self) -> bool { self.lifecycle.is_running() }
    pub fn is_finished(&self) -> bool { self.lifecycle.is_finished() }
    pub fn exit_code(&self) -> Option<i32> { self.lifecycle.exit_code() }
}

#[derive(Debug, Clone)]
pub enum SbatchAttachKey {
    Uuid(Uuid),
    JobId(u64),
    File(PathBuf),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(jobid: u64) -> SbatchJobSnapshot {
        SbatchJobSnapshot {
            uuid: Uuid::now_v7(),
            jobid,
            argv: vec!["sbatch".into(), "/w/job.sh".into()],
            sent_env: HashMap::from([("FOO".into(), "bar".into())]),
            script_path: PathBuf::from("/w/job.sh"),
            chdir: Some(PathBuf::from("/w")),
            partition: Some("gr19999b".into()),
            job_name: Some("g09run".into()),
            submitted_at: chrono::Utc::now(),
            log: LogPathSpec {
                output_template: Some("slurm-%j.out".into()),
                error_template: Some("slurm-%j.err".into()),
            },
            lifecycle: SbatchLifecycle::default(),
        }
    }

    #[test]
    fn snapshot_round_trips_through_json() {
        let s = snap(12345);
        let raw = serde_json::to_string(&s).unwrap();
        let back: SbatchJobSnapshot = serde_json::from_str(&raw).unwrap();
        assert_eq!(s, back);
    }

    #[test]
    fn output_path_substitutes_jobid_lazily() {
        let s = snap(12345);
        assert_eq!(s.output_path(), Some(PathBuf::from("slurm-12345.out")));
        assert_eq!(s.error_path(), Some(PathBuf::from("slurm-12345.err")));
    }

    #[test]
    fn lifecycle_running_requires_state_and_no_vanish() {
        let mut s = snap(1);
        assert!(!s.is_running());
        s.lifecycle.last_observed_state = Some(JobStatus {
            state: JobState::Running,
            reason: JobReason::None,
        });
        assert!(s.is_running());
        s.lifecycle.left_active_listing = true;
        assert!(!s.is_running());
    }

    #[test]
    fn lifecycle_finished_records_exit_code() {
        let mut s = snap(1);
        assert!(!s.is_finished());
        assert_eq!(s.exit_code(), None);
        s.lifecycle.finished = Some(FinishedInfo {
            final_state: JobState::Completed,
            final_reason: JobReason::None,
            exit_code: Some(0),
            finished_at: chrono::Utc::now(),
        });
        assert!(s.is_finished());
        assert_eq!(s.exit_code(), Some(0));
    }
}
```

- [ ] **Step 8.2: Write store.rs**

```rust
//! `SbatchJobSnapshot` participation in the generic [`JobStateStore`] layer.

use uuid::Uuid;

use crate::sbatch::handle::SbatchJobSnapshot;
use crate::store::JobSnapshot;

impl JobSnapshot for SbatchJobSnapshot {
    fn uuid(&self) -> Uuid { self.uuid }
    fn jobid(&self) -> Option<u64> { Some(self.jobid) }
    fn kind() -> &'static str { "sbatch" }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sbatch::handle::{LogPathSpec, SbatchLifecycle};
    use crate::store::{FileSystemStateStore, JobStateStore};
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn snap(jobid: u64) -> SbatchJobSnapshot {
        SbatchJobSnapshot {
            uuid: Uuid::now_v7(),
            jobid,
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

    #[tokio::test]
    async fn sbatch_snapshot_round_trips_via_fs_store() {
        let tmp = tempfile::tempdir().unwrap();
        let store: FileSystemStateStore<SbatchJobSnapshot> =
            FileSystemStateStore::new(tmp.path());
        let s = snap(12345);
        store.save(&s).await.unwrap();
        assert_eq!(store.load(s.uuid).await.unwrap(), Some(s.clone()));

        let path = tmp.path().join(format!("{}.json", s.uuid));
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(raw.contains("\"kind\": \"sbatch\""));
    }

    #[tokio::test]
    async fn sbatch_and_tssrun_coexist_in_same_dir() {
        use crate::tssrun::handle::JobHandleSnapshot;
        let tmp = tempfile::tempdir().unwrap();
        let sb_store: FileSystemStateStore<SbatchJobSnapshot> =
            FileSystemStateStore::new(tmp.path());
        let ts_store: FileSystemStateStore<JobHandleSnapshot> =
            FileSystemStateStore::new(tmp.path());
        sb_store.save(&snap(1)).await.unwrap();
        ts_store.save(&JobHandleSnapshot {
            uuid: Uuid::now_v7(),
            pid: 1,
            argv: vec![],
            sent_env: HashMap::new(),
            cwd: None,
            started_at_unix: 0,
            log_locations: crate::tssrun::handle::LogLocations::None,
            jobid: Some(99),
            node: None,
            finished: None,
        }).await.unwrap();
        assert_eq!(sb_store.list().await.unwrap().len(), 1);
        assert_eq!(ts_store.list().await.unwrap().len(), 1);
    }
}
```

- [ ] **Step 8.3: Run tests**

```bash
cargo test --lib sbatch:: -- --nocapture
```
Expected: 4 snapshot tests + 2 store tests pass.

- [ ] **Step 8.4: Commit**

```bash
git add src/sbatch/handle.rs src/sbatch/store.rs
git commit -m "feat(sbatch): add SbatchJobSnapshot + lifecycle helpers + JobSnapshot impl"
```

---

## Task 9: SbatchJobHandle skeleton (Arc-wrapped + lock-free getters)

**Files:**
- Modify: `src/sbatch/handle.rs` (append handle types)

- [ ] **Step 9.1: Append the handle types**

Add to the END of `src/sbatch/handle.rs` (after the existing tests module — and add the new tests inside the existing tests module):

```rust
use std::sync::Arc;

use tokio::sync::{Mutex as TokioMutex, watch};

use crate::dispatcher::JobDispatcher;
use crate::store::JobStateStore;

/// Cheap-to-clone handle to an in-flight or attached sbatch job. All
/// snapshot reads are lock-free; `refresh` / `refresh_with_sacct` /
/// `wait_terminal` serialize through `refresh_lock`.
#[derive(Clone)]
pub struct SbatchJobHandle(pub(crate) Arc<SbatchJobHandleInner>);

pub(crate) struct SbatchJobHandleInner {
    pub(crate) snapshot_tx: watch::Sender<SbatchJobSnapshot>,
    pub(crate) store: Arc<dyn JobStateStore<SbatchJobSnapshot>>,
    pub(crate) dispatcher: Arc<dyn JobDispatcher>,
    pub(crate) refresh_lock: TokioMutex<()>,
}

impl SbatchJobHandle {
    pub(crate) fn new(
        snapshot: SbatchJobSnapshot,
        store: Arc<dyn JobStateStore<SbatchJobSnapshot>>,
        dispatcher: Arc<dyn JobDispatcher>,
    ) -> Self {
        let (tx, _rx) = watch::channel(snapshot);
        Self(Arc::new(SbatchJobHandleInner {
            snapshot_tx: tx,
            store,
            dispatcher,
            refresh_lock: TokioMutex::new(()),
        }))
    }

    // -------- Lock-free snapshot reads --------

    pub fn snapshot(&self) -> SbatchJobSnapshot {
        self.0.snapshot_tx.borrow().clone()
    }
    pub fn watch(&self) -> watch::Receiver<SbatchJobSnapshot> {
        self.0.snapshot_tx.subscribe()
    }

    pub fn uuid(&self) -> Uuid { self.0.snapshot_tx.borrow().uuid }
    pub fn jobid(&self) -> Option<u64> { Some(self.0.snapshot_tx.borrow().jobid) }
    pub fn partition(&self) -> Option<JobPartition> {
        self.0.snapshot_tx.borrow().partition.clone()
    }
    pub fn job_name(&self) -> Option<String> {
        self.0.snapshot_tx.borrow().job_name.clone()
    }
    pub fn sent_env(&self) -> HashMap<String, String> {
        self.0.snapshot_tx.borrow().sent_env.clone()
    }
    pub fn output_template(&self) -> Option<String> {
        self.0.snapshot_tx.borrow().log.output_template.clone()
    }
    pub fn error_template(&self) -> Option<String> {
        self.0.snapshot_tx.borrow().log.error_template.clone()
    }
    pub fn output_path(&self) -> Option<PathBuf> {
        self.0.snapshot_tx.borrow().output_path()
    }
    pub fn error_path(&self) -> Option<PathBuf> {
        self.0.snapshot_tx.borrow().error_path()
    }
    pub fn is_running(&self) -> bool { self.0.snapshot_tx.borrow().is_running() }
    pub fn is_finished(&self) -> bool { self.0.snapshot_tx.borrow().is_finished() }
    pub fn exit_code(&self) -> Option<i32> { self.0.snapshot_tx.borrow().exit_code() }
}
```

- [ ] **Step 9.2: Add handle tests inside the existing tests module**

Inside `mod tests { ... }` in `src/sbatch/handle.rs`, append:

```rust
    #[tokio::test]
    async fn handle_lock_free_getters_return_initial_snapshot() {
        use crate::dispatcher::DryRunDispatcher;
        use crate::store::InMemoryStateStore;

        let s = snap(99);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> =
            Arc::new(InMemoryStateStore::new());
        let dispatcher: Arc<dyn JobDispatcher> = Arc::new(DryRunDispatcher);
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);

        assert_eq!(h.uuid(), s.uuid);
        assert_eq!(h.jobid(), Some(99));
        assert_eq!(h.job_name().as_deref(), Some("g09run"));
        assert_eq!(h.output_path(), Some(PathBuf::from("slurm-99.out")));
        assert!(!h.is_running());
        assert!(!h.is_finished());
    }

    #[tokio::test]
    async fn handle_clone_shares_inner() {
        use crate::dispatcher::DryRunDispatcher;
        use crate::store::InMemoryStateStore;

        let s = snap(1);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> =
            Arc::new(InMemoryStateStore::new());
        let dispatcher: Arc<dyn JobDispatcher> = Arc::new(DryRunDispatcher);
        let h1 = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let h2 = h1.clone();
        assert_eq!(h1.uuid(), h2.uuid());
        // Both subscribe to the same sender.
        let _r1 = h1.watch();
        let _r2 = h2.watch();
    }
```

- [ ] **Step 9.3: Run tests**

```bash
cargo test --lib sbatch::handle -- --nocapture
```
Expected: previous tests + 2 new tests pass.

- [ ] **Step 9.4: Commit**

```bash
git add src/sbatch/handle.rs
git commit -m "feat(sbatch): add SbatchJobHandle skeleton with lock-free getters"
```

---

## Task 10: SbatchJobHandle.refresh (qgroup -l → squeue, no sacct)

**Files:**
- Modify: `src/sbatch/handle.rs`

- [ ] **Step 10.1: Append the refresh impl**

Add to the existing `impl SbatchJobHandle` block:

```rust
    /// Lightweight polling: `qgroup -l` → `squeue` fallback. **Never** calls
    /// sacct. If both lookups miss, sets `lifecycle.left_active_listing = true`.
    pub async fn refresh(&self) -> anyhow::Result<SbatchJobSnapshot> {
        let inner = &*self.0;
        let _guard = inner.refresh_lock.lock().await;

        let mut snap = inner.snapshot_tx.borrow().clone();
        let now = chrono::Utc::now();

        let qgroup = crate::runner::query_job_states_via_qgroup_with(
            &*inner.dispatcher,
            &[snap.jobid],
        ).await?;
        if let Some(status) = qgroup.get(&snap.jobid) {
            snap.lifecycle.last_observed_state = Some(status.clone());
            snap.lifecycle.last_observed_at = Some(now);
            inner.store.save(&snap).await?;
            let _ = inner.snapshot_tx.send(snap.clone());
            return Ok(snap);
        }

        let squeue = crate::runner::query_job_states_squeue_only_with(
            &*inner.dispatcher,
            &[snap.jobid],
        ).await?;
        if let Some(status) = squeue.get(&snap.jobid) {
            snap.lifecycle.last_observed_state = Some(status.clone());
            snap.lifecycle.last_observed_at = Some(now);
            inner.store.save(&snap).await?;
            let _ = inner.snapshot_tx.send(snap.clone());
            return Ok(snap);
        }

        snap.lifecycle.left_active_listing = true;
        snap.lifecycle.last_observed_at = Some(now);
        inner.store.save(&snap).await?;
        let _ = inner.snapshot_tx.send(snap.clone());
        Ok(snap)
    }
```

- [ ] **Step 10.2: Add the CannedDispatcher mock + refresh tests inside the tests module**

```rust
    /// Mock dispatcher: returns canned outputs keyed by argv[0].
    /// argv[0] = "qgroup" → first canned, "squeue" → second, "sacct" → third.
    struct CannedDispatcher {
        qgroup: std::sync::Mutex<String>,
        squeue: std::sync::Mutex<String>,
        sacct: std::sync::Mutex<String>,
        sacct_call_count: std::sync::Mutex<u32>,
    }
    impl CannedDispatcher {
        fn new(qgroup: &str, squeue: &str, sacct: &str) -> Self {
            Self {
                qgroup: std::sync::Mutex::new(qgroup.to_string()),
                squeue: std::sync::Mutex::new(squeue.to_string()),
                sacct: std::sync::Mutex::new(sacct.to_string()),
                sacct_call_count: std::sync::Mutex::new(0),
            }
        }
        fn sacct_calls(&self) -> u32 {
            *self.sacct_call_count.lock().unwrap()
        }
    }
    impl crate::dispatcher::JobDispatcher for CannedDispatcher {
        async fn run(&self, _argv: &[String]) -> anyhow::Result<i32> {
            unimplemented!()
        }
        async fn capture(&self, argv: &[String]) -> anyhow::Result<(i32, String)> {
            let bin = argv[0].as_str();
            let out = match bin {
                "qgroup" => self.qgroup.lock().unwrap().clone(),
                "squeue" => self.squeue.lock().unwrap().clone(),
                "sacct" => {
                    *self.sacct_call_count.lock().unwrap() += 1;
                    self.sacct.lock().unwrap().clone()
                }
                _ => String::new(),
            };
            Ok((0, out))
        }
    }

    #[tokio::test]
    async fn refresh_uses_qgroup_when_jobid_present() {
        use crate::store::InMemoryStateStore;
        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> =
            Arc::new(InMemoryStateStore::new());
        let dispatcher: Arc<dyn JobDispatcher> = Arc::new(CannedDispatcher::new(
            "QUEUE USER JOBID STATUS PROC\ngr u 12345 RUN 1\n",
            "",
            "",
        ));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh().await.unwrap();
        assert_eq!(
            after.lifecycle.last_observed_state.unwrap().state,
            crate::JobState::Running
        );
        assert!(!after.lifecycle.left_active_listing);
    }

    #[tokio::test]
    async fn refresh_falls_back_to_squeue_when_qgroup_misses() {
        use crate::store::InMemoryStateStore;
        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> =
            Arc::new(InMemoryStateStore::new());
        let dispatcher: Arc<dyn JobDispatcher> = Arc::new(CannedDispatcher::new(
            "",
            "12345 RUNNING None\n",
            "",
        ));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh().await.unwrap();
        assert_eq!(
            after.lifecycle.last_observed_state.unwrap().state,
            crate::JobState::Running
        );
    }

    #[tokio::test]
    async fn refresh_marks_left_active_listing_when_both_miss() {
        use crate::store::InMemoryStateStore;
        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> =
            Arc::new(InMemoryStateStore::new());
        let canned = Arc::new(CannedDispatcher::new("", "", ""));
        let dispatcher: Arc<dyn JobDispatcher> = canned.clone();
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh().await.unwrap();
        assert!(after.lifecycle.left_active_listing);
        assert_eq!(canned.sacct_calls(), 0, "refresh must NOT call sacct");
    }
```

- [ ] **Step 10.3: Run tests**

```bash
cargo test --lib sbatch::handle -- --nocapture
```
Expected: 3 new tests pass.

- [ ] **Step 10.4: Commit**

```bash
git add src/sbatch/handle.rs
git commit -m "feat(sbatch): SbatchJobHandle::refresh (qgroup -l → squeue, no sacct)"
```

---

## Task 11: refresh_with_sacct + wait_terminal

**Files:**
- Modify: `src/sbatch/handle.rs`

- [ ] **Step 11.1: Append refresh_with_sacct and wait_terminal**

Add to `impl SbatchJobHandle`:

```rust
    /// Heavyweight finalizer. Calls `refresh()` first; only invokes
    /// sacct if the job has actually left both `qgroup -l` and `squeue`
    /// **and** `lifecycle.finished` is still None. Otherwise behaves
    /// identically to `refresh()`.
    pub async fn refresh_with_sacct(&self) -> anyhow::Result<SbatchJobSnapshot> {
        let mut snap = self.refresh().await?;
        if snap.lifecycle.finished.is_some() { return Ok(snap); }
        if !snap.lifecycle.left_active_listing { return Ok(snap); }

        let inner = &*self.0;
        let _guard = inner.refresh_lock.lock().await;

        let map = crate::runner::query_job_states_batch_with(
            &*inner.dispatcher,
            &[snap.jobid],
        ).await?;
        let final_status = map.get(&snap.jobid).cloned().unwrap_or_default();
        snap.lifecycle.finished = Some(FinishedInfo {
            final_state: final_status.state,
            final_reason: final_status.reason,
            // sacct's ExitCode is not currently parsed by query_job_states_batch_with;
            // surface as None for now. Phase 2 may extend the parser.
            exit_code: None,
            finished_at: chrono::Utc::now(),
        });
        inner.store.save(&snap).await?;
        let _ = inner.snapshot_tx.send(snap.clone());
        Ok(snap)
    }

    /// Lightweight polling loop. Calls `refresh()` (sacct-free) at the
    /// supplied interval until either (a) the observed state is terminal,
    /// or (b) the job leaves both active listings. Caller may follow up
    /// with one `refresh_with_sacct()` if exit_code resolution is needed.
    pub async fn wait_terminal(
        &self,
        poll_interval: std::time::Duration,
    ) -> anyhow::Result<SbatchJobSnapshot> {
        loop {
            let snap = self.refresh().await?;
            if let Some(state) = &snap.lifecycle.last_observed_state {
                if state.state.is_terminal() { return Ok(snap); }
            }
            if snap.lifecycle.left_active_listing { return Ok(snap); }
            tokio::time::sleep(poll_interval).await;
        }
    }
```

- [ ] **Step 11.2: Add tests**

```rust
    #[tokio::test]
    async fn refresh_with_sacct_skips_sacct_when_qgroup_hits() {
        use crate::store::InMemoryStateStore;
        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> =
            Arc::new(InMemoryStateStore::new());
        let canned = Arc::new(CannedDispatcher::new(
            "QUEUE USER JOBID STATUS PROC\ngr u 12345 RUN 1\n",
            "",
            "",
        ));
        let dispatcher: Arc<dyn JobDispatcher> = canned.clone();
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh_with_sacct().await.unwrap();
        assert!(after.lifecycle.finished.is_none());
        assert_eq!(canned.sacct_calls(), 0);
    }

    #[tokio::test]
    async fn refresh_with_sacct_calls_sacct_once_after_vanish() {
        use crate::store::InMemoryStateStore;
        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> =
            Arc::new(InMemoryStateStore::new());
        let canned = Arc::new(CannedDispatcher::new(
            "",
            "",
            "12345|COMPLETED|None\n",
        ));
        let dispatcher: Arc<dyn JobDispatcher> = canned.clone();
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h.refresh_with_sacct().await.unwrap();
        assert!(after.lifecycle.finished.is_some());
        assert_eq!(canned.sacct_calls(), 1);
    }

    #[tokio::test]
    async fn wait_terminal_returns_when_state_is_terminal() {
        use crate::store::InMemoryStateStore;
        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> =
            Arc::new(InMemoryStateStore::new());
        let dispatcher: Arc<dyn JobDispatcher> = Arc::new(CannedDispatcher::new(
            "QUEUE USER JOBID STATUS PROC\ngr u 12345 CMP 1\n",
            "",
            "",
        ));
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h
            .wait_terminal(std::time::Duration::from_millis(10))
            .await
            .unwrap();
        assert_eq!(
            after.lifecycle.last_observed_state.unwrap().state,
            crate::JobState::Completed
        );
    }

    #[tokio::test]
    async fn wait_terminal_returns_on_left_active_listing() {
        use crate::store::InMemoryStateStore;
        let s = snap(12345);
        let store: Arc<dyn JobStateStore<SbatchJobSnapshot>> =
            Arc::new(InMemoryStateStore::new());
        let canned = Arc::new(CannedDispatcher::new("", "", ""));
        let dispatcher: Arc<dyn JobDispatcher> = canned.clone();
        let h = SbatchJobHandle::new(s.clone(), store, dispatcher);
        let after = h
            .wait_terminal(std::time::Duration::from_millis(10))
            .await
            .unwrap();
        assert!(after.lifecycle.left_active_listing);
        assert_eq!(canned.sacct_calls(), 0, "wait_terminal must NEVER call sacct");
    }
```

- [ ] **Step 11.3: Run tests**

```bash
cargo test --lib sbatch::handle -- --nocapture
```
Expected: 4 new tests pass.

- [ ] **Step 11.4: Commit**

```bash
git add src/sbatch/handle.rs
git commit -m "feat(sbatch): refresh_with_sacct (opt-in) + wait_terminal (no sacct)"
```

---

## Task 12: SbatchSpawnError + SbatchManager

**Files:**
- Replace: `src/sbatch/error.rs`
- Replace: `src/sbatch/manager.rs`

- [ ] **Step 12.1: Write error.rs**

```rust
//! Spawn-time errors with structured recovery information.

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SbatchSpawnError {
    #[error("sbatch invocation failed (exit={exit_code}): {stdout}")]
    SubmitFailed { exit_code: i32, stdout: String },

    #[error("sbatch stdout did not contain a parseable jobid: {stdout}")]
    JobidParseError { stdout: String },

    #[error("sbatch submitted jobid={jobid} but snapshot save failed: {source}")]
    SubmittedButUnpersisted {
        jobid: u64,
        #[source]
        source: anyhow::Error,
    },

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
```

- [ ] **Step 12.2: Write manager.rs**

```rust
//! `SbatchManager` — spawn / attach orchestration.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use uuid::Uuid;

use crate::dispatcher::{JobDispatcher, TokioDispatcher};
use crate::sbatch::cmd::SbatchCmd;
use crate::sbatch::error::SbatchSpawnError;
use crate::sbatch::handle::{
    LogPathSpec, SbatchAttachKey, SbatchJobHandle, SbatchJobSnapshot, SbatchLifecycle,
};
use crate::sbatch::parse::parse_submitted_jobid;
use crate::store::{FileSystemStateStore, InMemoryStateStore, JobStateStore};

#[derive(Clone)]
pub struct SbatchManager {
    cmd: SbatchCmd,
    store: Arc<dyn JobStateStore<SbatchJobSnapshot>>,
    dispatcher: Arc<dyn JobDispatcher>,
}

impl SbatchManager {
    pub fn new(cmd: SbatchCmd) -> Self {
        Self {
            cmd,
            store: Arc::new(InMemoryStateStore::<SbatchJobSnapshot>::new()),
            dispatcher: Arc::new(TokioDispatcher),
        }
    }

    pub fn with_state_dir(mut self, root: impl Into<PathBuf>) -> Self {
        self.store = Arc::new(FileSystemStateStore::<SbatchJobSnapshot>::new(root));
        self
    }

    pub fn with_state_store(
        mut self,
        store: Arc<dyn JobStateStore<SbatchJobSnapshot>>,
    ) -> Self {
        self.store = store;
        self
    }

    pub fn with_dispatcher(mut self, dispatcher: Arc<dyn JobDispatcher>) -> Self {
        self.dispatcher = dispatcher;
        self
    }

    pub async fn spawn(&self) -> Result<SbatchJobHandle, SbatchSpawnError> {
        let argv = self.cmd.build_argv()?;
        let (exit_code, stdout) = self
            .dispatcher
            .capture(&argv)
            .await
            .map_err(SbatchSpawnError::Other)?;
        if exit_code != 0 {
            return Err(SbatchSpawnError::SubmitFailed { exit_code, stdout });
        }
        let jobid = parse_submitted_jobid(&stdout)
            .ok_or_else(|| SbatchSpawnError::JobidParseError { stdout: stdout.clone() })?;

        let uuid = Uuid::now_v7();
        let script_path = std::path::absolute(&self.cmd.script)
            .with_context(|| format!("absolutize {}", self.cmd.script.display()))?;
        let snapshot = SbatchJobSnapshot {
            uuid,
            jobid,
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

        self.store
            .save(&snapshot)
            .await
            .map_err(|source| SbatchSpawnError::SubmittedButUnpersisted { jobid, source })?;
        Ok(SbatchJobHandle::new(
            snapshot,
            self.store.clone(),
            self.dispatcher.clone(),
        ))
    }

    pub async fn attach(&self, key: SbatchAttachKey) -> Result<SbatchJobHandle> {
        let snapshot = match key {
            SbatchAttachKey::Uuid(u) => self.store.load(u).await?,
            SbatchAttachKey::JobId(j) => self.store.find_by_jobid(j).await?,
            SbatchAttachKey::File(path) => {
                let bytes = tokio::fs::read(&path).await?;
                Some(serde_json::from_slice(&bytes)?)
            }
        }
        .ok_or_else(|| anyhow!("snapshot not found"))?;
        Ok(SbatchJobHandle::new(
            snapshot,
            self.store.clone(),
            self.dispatcher.clone(),
        ))
    }

    pub async fn attach_uuid(&self, u: Uuid) -> Result<SbatchJobHandle> {
        self.attach(SbatchAttachKey::Uuid(u)).await
    }
    pub async fn attach_jobid(&self, j: u64) -> Result<SbatchJobHandle> {
        self.attach(SbatchAttachKey::JobId(j)).await
    }
    pub async fn attach_file(&self, p: impl Into<PathBuf>) -> Result<SbatchJobHandle> {
        self.attach(SbatchAttachKey::File(p.into())).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatcher::JobDispatcher;
    use std::sync::Mutex;

    struct CannedSbatch {
        stdout: Mutex<String>,
        exit: Mutex<i32>,
    }
    impl CannedSbatch {
        fn ok(jobid: u64) -> Self {
            Self {
                stdout: Mutex::new(format!("Submitted batch job {jobid}\n")),
                exit: Mutex::new(0),
            }
        }
        fn failed() -> Self {
            Self {
                stdout: Mutex::new("error: bad partition\n".into()),
                exit: Mutex::new(1),
            }
        }
        fn ok_no_jobid() -> Self {
            Self {
                stdout: Mutex::new("warning but no parseable id\n".into()),
                exit: Mutex::new(0),
            }
        }
    }
    impl JobDispatcher for CannedSbatch {
        async fn run(&self, _argv: &[String]) -> Result<i32> { unimplemented!() }
        async fn capture(&self, _argv: &[String]) -> Result<(i32, String)> {
            Ok((*self.exit.lock().unwrap(), self.stdout.lock().unwrap().clone()))
        }
    }

    #[tokio::test]
    async fn spawn_happy_path_returns_handle_with_jobid() {
        let cmd = SbatchCmd::new("/w/job.sh");
        let dispatcher: Arc<dyn JobDispatcher> = Arc::new(CannedSbatch::ok(12345));
        let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);
        let h = mgr.spawn().await.unwrap();
        assert_eq!(h.jobid(), Some(12345));
        assert_eq!(h.snapshot().argv[0], "sbatch");
    }

    #[tokio::test]
    async fn spawn_returns_submit_failed_on_nonzero_exit() {
        let cmd = SbatchCmd::new("/w/job.sh");
        let dispatcher: Arc<dyn JobDispatcher> = Arc::new(CannedSbatch::failed());
        let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);
        let err = mgr.spawn().await.unwrap_err();
        assert!(matches!(err, SbatchSpawnError::SubmitFailed { exit_code: 1, .. }));
    }

    #[tokio::test]
    async fn spawn_returns_jobid_parse_error_when_stdout_is_garbage() {
        let cmd = SbatchCmd::new("/w/job.sh");
        let dispatcher: Arc<dyn JobDispatcher> = Arc::new(CannedSbatch::ok_no_jobid());
        let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);
        let err = mgr.spawn().await.unwrap_err();
        assert!(matches!(err, SbatchSpawnError::JobidParseError { .. }));
    }

    #[tokio::test]
    async fn attach_uuid_round_trips_through_in_memory_store() {
        let cmd = SbatchCmd::new("/w/job.sh");
        let dispatcher: Arc<dyn JobDispatcher> = Arc::new(CannedSbatch::ok(99));
        let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);
        let h = mgr.spawn().await.unwrap();
        let attached = mgr.attach_uuid(h.uuid()).await.unwrap();
        assert_eq!(attached.jobid(), Some(99));
    }

    #[tokio::test]
    async fn attach_jobid_finds_via_default_trait_impl() {
        let cmd = SbatchCmd::new("/w/job.sh");
        let dispatcher: Arc<dyn JobDispatcher> = Arc::new(CannedSbatch::ok(77));
        let mgr = SbatchManager::new(cmd).with_dispatcher(dispatcher);
        let _ = mgr.spawn().await.unwrap();
        let attached = mgr.attach_jobid(77).await.unwrap();
        assert_eq!(attached.jobid(), Some(77));
    }

    #[tokio::test]
    async fn attach_file_reads_disk_snapshot_directly() {
        let tmp = tempfile::tempdir().unwrap();
        let cmd = SbatchCmd::new("/w/job.sh");
        let dispatcher: Arc<dyn JobDispatcher> = Arc::new(CannedSbatch::ok(55));
        let mgr = SbatchManager::new(cmd)
            .with_state_dir(tmp.path())
            .with_dispatcher(dispatcher);
        let h = mgr.spawn().await.unwrap();
        let path = tmp.path().join(format!("{}.json", h.uuid()));
        assert!(path.exists());
        let attached = mgr.attach_file(&path).await.unwrap();
        assert_eq!(attached.jobid(), Some(55));
    }
}
```

- [ ] **Step 12.3: Run tests**

```bash
cargo test --lib sbatch::manager -- --nocapture
```
Expected: 6 tests pass.

- [ ] **Step 12.4: Commit**

```bash
git add src/sbatch/manager.rs src/sbatch/error.rs
git commit -m "feat(sbatch): add SbatchManager (spawn + attach) and SbatchSpawnError"
```

---

## Task 13: lib.rs re-exports + CHANGELOG

**Files:**
- Modify: `src/lib.rs`
- Modify: `CHANGELOG.md`

- [ ] **Step 13.1: Add re-exports**

In `src/lib.rs`, after the existing `pub use tssrun::...` block, add:

```rust
// sbatch module re-exports
pub use sbatch::cmd::SbatchCmd;
pub use sbatch::error::SbatchSpawnError;
pub use sbatch::handle::{
    FinishedInfo as SbatchFinishedInfo, LogPathSpec, SbatchAttachKey, SbatchJobHandle,
    SbatchJobSnapshot, SbatchLifecycle,
};
pub use sbatch::manager::SbatchManager;
pub use sbatch::parse::{parse_submitted_jobid, resolve_log_path};

// Generic store re-exports (replaces tssrun-specific ones)
pub use store::{FileSystemStateStore, InMemoryStateStore, JobSnapshot, JobStateStore};
```

> Note: rename to `SbatchFinishedInfo` to avoid clash with `tssrun::handle::FinishedInfo`.

- [ ] **Step 13.2: Append to CHANGELOG.md**

Append:

```markdown
## [Unreleased]

### Added
- New `crate::sbatch` module: SBATCH-based job submission with KUDPC-aware
  polling (`qgroup -l` → `squeue` fallback, opt-in sacct via
  `refresh_with_sacct`). See `docs/superpowers/specs/2026-05-10-sbatch-module-design.md`.
- Generic store layer: `JobSnapshot` trait + `JobStateStore<S>` + `InMemoryStateStore<S>`
  + `FileSystemStateStore<S>`. Both `tssrun` and `sbatch` use it; on-disk JSON
  files now include a top-level `"kind"` discriminator.

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
```

- [ ] **Step 13.3: Build everything**

```bash
cargo build --all-targets && cargo test --lib
```
Expected: full build + all tests pass.

- [ ] **Step 13.4: Commit**

```bash
git add src/lib.rs CHANGELOG.md
git commit -m "feat(sbatch): wire crate::sbatch re-exports + CHANGELOG"
```

---

## Task 14: pyo3 layer for sbatch

**Files:**
- Create: `src/py_export/sbatch.rs`
- Modify: `src/py_export/mod.rs`

- [ ] **Step 14.1: Read existing tssrun pyo3 layer for reference**

```bash
sed -n '1,80p' src/py_export/tssrun.rs
```
Note the patterns for `inner_module`, `pyclass`, `future_into_py`, and `sys.modules` registration.

- [ ] **Step 14.2: Write src/py_export/sbatch.rs**

```rust
//! pyo3 bindings for the sbatch module. Lives at
//! `slurm_async_runner._core.sbatch` in the Python namespace.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;

use crate::dispatcher::JobDispatcher;
use crate::entities::slurm::{JobPartition, JobTimeLimit, ResourceSpec};
use crate::sbatch::cmd::SbatchCmd;
use crate::sbatch::error::SbatchSpawnError;
use crate::sbatch::handle::{SbatchAttachKey, SbatchJobHandle};
use crate::sbatch::manager::SbatchManager;

#[pyclass(name = "SbatchCmd", module = "slurm_async_runner._core.sbatch")]
#[derive(Clone)]
pub struct PySbatchCmd(SbatchCmd);

#[pymethods]
impl PySbatchCmd {
    #[new]
    #[pyo3(signature = (script, *, sbatch_bin = "sbatch".to_string(),
                        job_name = None, partition = None, time_limit = None,
                        rsc = None, output = None, error = None, chdir = None,
                        env = None, args = None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        script: PathBuf,
        sbatch_bin: String,
        job_name: Option<String>,
        partition: Option<String>,
        time_limit: Option<String>,
        rsc: Option<String>,
        output: Option<String>,
        error: Option<String>,
        chdir: Option<PathBuf>,
        env: Option<HashMap<String, String>>,
        args: Option<Vec<String>>,
    ) -> PyResult<Self> {
        let mut cmd = SbatchCmd::new(script);
        cmd.sbatch_bin = sbatch_bin;
        cmd.job_name = job_name;
        cmd.partition = partition.map(JobPartition::from);
        if let Some(s) = time_limit {
            cmd.time_limit = Some(s.parse::<JobTimeLimit>().map_err(py_err)?);
        }
        if let Some(s) = rsc {
            cmd.rsc = Some(s.parse::<ResourceSpec>().map_err(py_err)?);
        }
        cmd.output = output;
        cmd.error = error;
        cmd.chdir = chdir;
        cmd.env = env.unwrap_or_default();
        cmd.args = args.unwrap_or_default();
        Ok(Self(cmd))
    }
}

#[pyclass(name = "SbatchManager", module = "slurm_async_runner._core.sbatch")]
#[derive(Clone)]
pub struct PySbatchManager(SbatchManager);

#[pymethods]
impl PySbatchManager {
    #[new]
    #[pyo3(signature = (cmd, *, state_dir = None))]
    fn new(cmd: PySbatchCmd, state_dir: Option<PathBuf>) -> Self {
        let mut mgr = SbatchManager::new(cmd.0);
        if let Some(d) = state_dir {
            mgr = mgr.with_state_dir(d);
        }
        Self(mgr)
    }

    fn spawn<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mgr = self.0.clone();
        future_into_py(py, async move {
            let h = mgr.spawn().await.map_err(|e| match e {
                SbatchSpawnError::SubmittedButUnpersisted { jobid, source } => {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "submitted but unpersisted: jobid={jobid}, source={source}"
                    ))
                }
                other => pyo3::exceptions::PyRuntimeError::new_err(other.to_string()),
            })?;
            Python::with_gil(|py| Ok(PySbatchJobHandle(h).into_py(py)))
        })
    }

    fn attach_uuid<'py>(&self, py: Python<'py>, uuid: String) -> PyResult<Bound<'py, PyAny>> {
        let mgr = self.0.clone();
        let u = uuid::Uuid::parse_str(&uuid).map_err(py_err)?;
        future_into_py(py, async move {
            let h = mgr.attach_uuid(u).await.map_err(py_err)?;
            Python::with_gil(|py| Ok(PySbatchJobHandle(h).into_py(py)))
        })
    }

    fn attach_jobid<'py>(&self, py: Python<'py>, jobid: u64) -> PyResult<Bound<'py, PyAny>> {
        let mgr = self.0.clone();
        future_into_py(py, async move {
            let h = mgr.attach_jobid(jobid).await.map_err(py_err)?;
            Python::with_gil(|py| Ok(PySbatchJobHandle(h).into_py(py)))
        })
    }

    fn attach_file<'py>(&self, py: Python<'py>, path: PathBuf) -> PyResult<Bound<'py, PyAny>> {
        let mgr = self.0.clone();
        future_into_py(py, async move {
            let h = mgr.attach_file(path).await.map_err(py_err)?;
            Python::with_gil(|py| Ok(PySbatchJobHandle(h).into_py(py)))
        })
    }
}

#[pyclass(name = "SbatchJobHandle", module = "slurm_async_runner._core.sbatch")]
#[derive(Clone)]
pub struct PySbatchJobHandle(SbatchJobHandle);

#[pymethods]
impl PySbatchJobHandle {
    #[getter] fn uuid(&self) -> String { self.0.uuid().to_string() }
    #[getter] fn jobid(&self) -> Option<u64> { self.0.jobid() }
    #[getter] fn partition(&self) -> Option<String> {
        self.0.partition().map(|p| p.to_string())
    }
    #[getter] fn job_name(&self) -> Option<String> { self.0.job_name() }
    #[getter] fn sent_env(&self) -> HashMap<String, String> { self.0.sent_env() }
    #[getter] fn output_template(&self) -> Option<String> { self.0.output_template() }
    #[getter] fn error_template(&self) -> Option<String> { self.0.error_template() }
    #[getter] fn output_path(&self) -> Option<PathBuf> { self.0.output_path() }
    #[getter] fn error_path(&self) -> Option<PathBuf> { self.0.error_path() }

    fn is_running(&self) -> bool { self.0.is_running() }
    fn is_finished(&self) -> bool { self.0.is_finished() }
    fn exit_code(&self) -> Option<i32> { self.0.exit_code() }

    fn refresh<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let h = self.0.clone();
        future_into_py(py, async move {
            h.refresh().await.map_err(py_err)?;
            Python::with_gil(|py| Ok(py.None()))
        })
    }

    fn refresh_with_sacct<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let h = self.0.clone();
        future_into_py(py, async move {
            h.refresh_with_sacct().await.map_err(py_err)?;
            Python::with_gil(|py| Ok(py.None()))
        })
    }

    #[pyo3(signature = (poll_interval_secs))]
    fn wait_terminal<'py>(
        &self,
        py: Python<'py>,
        poll_interval_secs: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let h = self.0.clone();
        future_into_py(py, async move {
            h.wait_terminal(std::time::Duration::from_secs_f64(poll_interval_secs))
                .await
                .map_err(py_err)?;
            Python::with_gil(|py| Ok(py.None()))
        })
    }
}

fn py_err<E: std::fmt::Display>(e: E) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
}

#[pymodule]
#[pyo3(name = "sbatch")]
pub fn inner_module(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySbatchCmd>()?;
    m.add_class::<PySbatchManager>()?;
    m.add_class::<PySbatchJobHandle>()?;
    Ok(())
}
```

- [ ] **Step 14.3: Wire into the parent pymodule**

In `src/py_export/mod.rs`, near the other `pub mod` declarations, add `pub mod sbatch;`. Then in the `#[pymodule]` body, find where `tssrun::inner_module` is registered and add the sbatch one in the same fashion. Example pattern (adapt to existing layout):

```rust
let sbatch_mod = PyModule::new(py, "sbatch")?;
sbatch::inner_module(py, &sbatch_mod)?;
m.add_submodule(&sbatch_mod)?;
let sys_modules = py.import("sys")?.getattr("modules")?;
sys_modules.set_item("slurm_async_runner._core.sbatch", &sbatch_mod)?;
```

- [ ] **Step 14.4: Build the wheel**

```bash
uv sync --all-extras
uv run maturin develop
```
Expected: clean build, no link errors.

- [ ] **Step 14.5: Smoke-test imports**

```bash
uv run python -c "from slurm_async_runner._core.sbatch import SbatchCmd, SbatchManager, SbatchJobHandle; print('OK')"
```
Expected: prints `OK`.

- [ ] **Step 14.6: Commit**

```bash
git add src/py_export/sbatch.rs src/py_export/mod.rs
git commit -m "feat(sbatch): pyo3 bindings (PySbatchCmd/Manager/JobHandle)"
```

---

## Task 15: Python type stubs (.pyi) + pytest suite

**Files:**
- Create: `python/slurm_async_runner/_core/sbatch.pyi`
- Create: `python/tests/test_sbatch.py`
- Modify: `python/slurm_async_runner/_core/__init__.py`

- [ ] **Step 15.1: Write the .pyi stub**

Create `python/slurm_async_runner/_core/sbatch.pyi`:

```python
from pathlib import Path
from typing import Optional

class SbatchCmd:
    def __init__(
        self,
        script: str | Path,
        *,
        sbatch_bin: str = "sbatch",
        job_name: Optional[str] = None,
        partition: Optional[str] = None,
        time_limit: Optional[str] = None,
        rsc: Optional[str] = None,
        output: Optional[str] = None,
        error: Optional[str] = None,
        chdir: Optional[str | Path] = None,
        env: Optional[dict[str, str]] = None,
        args: Optional[list[str]] = None,
    ) -> None: ...

class SbatchJobHandle:
    @property
    def uuid(self) -> str: ...
    @property
    def jobid(self) -> Optional[int]: ...
    @property
    def partition(self) -> Optional[str]: ...
    @property
    def job_name(self) -> Optional[str]: ...
    @property
    def sent_env(self) -> dict[str, str]: ...
    @property
    def output_template(self) -> Optional[str]: ...
    @property
    def error_template(self) -> Optional[str]: ...
    @property
    def output_path(self) -> Optional[Path]: ...
    @property
    def error_path(self) -> Optional[Path]: ...
    def is_running(self) -> bool: ...
    def is_finished(self) -> bool: ...
    def exit_code(self) -> Optional[int]: ...
    async def refresh(self) -> None: ...
    async def refresh_with_sacct(self) -> None: ...
    async def wait_terminal(self, poll_interval_secs: float) -> None: ...

class SbatchManager:
    def __init__(
        self,
        cmd: SbatchCmd,
        *,
        state_dir: Optional[str | Path] = None,
    ) -> None: ...
    async def spawn(self) -> SbatchJobHandle: ...
    async def attach_uuid(self, uuid: str) -> SbatchJobHandle: ...
    async def attach_jobid(self, jobid: int) -> SbatchJobHandle: ...
    async def attach_file(self, path: str | Path) -> SbatchJobHandle: ...
```

- [ ] **Step 15.2: Update __init__.py**

In `python/slurm_async_runner/_core/__init__.py`, append (or edit if a similar pattern already exists for tssrun):

```python
from . import sbatch as sbatch  # noqa: F401
```

- [ ] **Step 15.3: Write Python pytest suite**

Create `python/tests/test_sbatch.py`:

```python
import asyncio
import json
import shutil
from pathlib import Path

import pytest

from slurm_async_runner._core.sbatch import SbatchCmd, SbatchManager


def test_sbatch_cmd_minimal_construction():
    cmd = SbatchCmd("/work/job.sh")
    assert cmd is not None
    cmd = SbatchCmd(
        "/work/job.sh",
        partition="gr19999b",
        time_limit="1:00:00",
        rsc="p=4:c=8:m=2G",
        output="slurm-%j.out",
        env={"FOO": "bar"},
    )
    assert cmd is not None


def _have_bash() -> bool:
    return shutil.which("bash") is not None


@pytest.mark.skipif(not _have_bash(), reason="bash required")
def test_spawn_with_bash_emulating_sbatch_yields_jobid(tmp_path: Path):
    fake_sbatch = tmp_path / "fake_sbatch"
    fake_sbatch.write_text(
        "#!/usr/bin/env bash\n"
        'echo "Submitted batch job 99999"\n'
    )
    fake_sbatch.chmod(0o755)
    job_script = tmp_path / "job.sh"
    job_script.write_text("#!/usr/bin/env bash\necho hello\n")
    job_script.chmod(0o755)

    cmd = SbatchCmd(str(job_script), sbatch_bin=str(fake_sbatch))
    state_dir = tmp_path / "state"
    mgr = SbatchManager(cmd, state_dir=str(state_dir))

    async def go():
        h = await mgr.spawn()
        return h.jobid, h.uuid

    jobid, uuid = asyncio.run(go())
    assert jobid == 99999
    snap_file = state_dir / f"{uuid}.json"
    assert snap_file.exists()
    body = json.loads(snap_file.read_text())
    assert body["kind"] == "sbatch"
    assert body["jobid"] == 99999


@pytest.mark.skipif(not _have_bash(), reason="bash required")
def test_attach_uuid_round_trips(tmp_path: Path):
    fake_sbatch = tmp_path / "fake_sbatch"
    fake_sbatch.write_text(
        "#!/usr/bin/env bash\necho 'Submitted batch job 12345'\n"
    )
    fake_sbatch.chmod(0o755)
    job = tmp_path / "j.sh"
    job.write_text("#!/usr/bin/env bash\n:\n")
    job.chmod(0o755)

    state_dir = tmp_path / "state"
    cmd = SbatchCmd(str(job), sbatch_bin=str(fake_sbatch))
    mgr = SbatchManager(cmd, state_dir=str(state_dir))

    async def go():
        h = await mgr.spawn()
        h2 = await mgr.attach_uuid(h.uuid)
        return h.uuid == h2.uuid and h2.jobid == 12345

    assert asyncio.run(go())
```

- [ ] **Step 15.4: Build wheel + run pytest**

```bash
uv run maturin develop
uv run pytest python/tests/test_sbatch.py -v
```
Expected: all 3 tests pass.

- [ ] **Step 15.5: Commit**

```bash
git add python/slurm_async_runner/_core/sbatch.pyi \
  python/slurm_async_runner/_core/__init__.py \
  python/tests/test_sbatch.py
git commit -m "feat(sbatch): Python type stubs + pytest smoke suite"
```

---

## Task 16: Live smoke test for KUDPC

**Files:**
- Create: `scripts/test_sbatch_live.py`

- [ ] **Step 16.1: Write the live smoke script**

Create `scripts/test_sbatch_live.py`:

```python
"""Live smoke test for the sbatch wrapper on real KUDPC / SLURM nodes.

Standalone:    uv run python scripts/test_sbatch_live.py
Via pytest:    RUN_LIVE_SBATCH=1 uv run pytest python/tests/test_sbatch_live.py -v -s

Skipped (exit 0) if `sbatch` binary is not on PATH.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import sys
import tempfile
from pathlib import Path

from slurm_async_runner._core.sbatch import SbatchCmd, SbatchManager


def _env(name: str, default: str | None = None) -> str | None:
    val = os.environ.get(name)
    return val if val is not None else default


def _have_sbatch() -> bool:
    return shutil.which(_env("SBATCH_LIVE_BIN", "sbatch") or "sbatch") is not None


async def _run_live() -> int:
    if not _have_sbatch():
        print("SKIP: sbatch not on PATH; run on a kudpc / SLURM node.")
        return 0

    bin_path = _env("SBATCH_LIVE_BIN", "sbatch") or "sbatch"
    queue = _env("SBATCH_LIVE_QUEUE")
    time_limit = _env("SBATCH_LIVE_TIME_LIMIT", "0:01:00")
    rsc = _env("SBATCH_LIVE_RSC")
    timeout_s = float(_env("SBATCH_LIVE_TIMEOUT", "180") or "180")

    state_dir = Path(tempfile.mkdtemp(prefix="sbatch-live-"))
    with tempfile.TemporaryDirectory(prefix="sbatch-live-job-") as job_dir:
        job_path = Path(job_dir) / "live_job.sh"
        job_path.write_text(
            "#!/usr/bin/env bash\n"
            'echo "[live] starting"\n'
            "sleep 5\n"
            'echo "[live] finished"\n'
        )
        job_path.chmod(0o755)

        cmd = SbatchCmd(
            str(job_path),
            sbatch_bin=bin_path,
            partition=queue,
            time_limit=time_limit,
            rsc=rsc,
            output=str(Path(job_dir) / "stdout-%j.txt"),
            error=str(Path(job_dir) / "stderr-%j.txt"),
        )
        mgr = SbatchManager(cmd, state_dir=str(state_dir))
        handle = await mgr.spawn()
        print(f"[live] submitted: jobid={handle.jobid} uuid={handle.uuid}")

        try:
            await asyncio.wait_for(
                handle.wait_terminal(poll_interval_secs=10.0),
                timeout=timeout_s,
            )
        except asyncio.TimeoutError:
            print(f"FAIL: timed out after {timeout_s}s waiting for terminal.")
            return 1

        await handle.refresh_with_sacct()

        finished = handle.is_finished()
        exit_code = handle.exit_code()
        out = handle.output_path
        print(f"[live] terminal: finished={finished} exit_code={exit_code}")
        print(f"[live] output_path={out}")
        if out and Path(out).exists():
            print(f"[live] stdout: {Path(out).read_text()[:200]}")

        attached = await mgr.attach_uuid(handle.uuid)
        assert attached.jobid == handle.jobid, "attach round-trip failed"
        print("[live] attach_uuid round-trip OK")

    print("PASS")
    return 0


def main() -> int:
    return asyncio.run(_run_live())


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 16.2: Verify syntax**

```bash
uv run python -c "import ast; ast.parse(open('scripts/test_sbatch_live.py').read()); print('OK')"
```
Expected: prints `OK`.

- [ ] **Step 16.3: Run on local machine (skip path)**

```bash
uv run python scripts/test_sbatch_live.py
```
Expected: prints `SKIP: sbatch not on PATH; run on a kudpc / SLURM node.` and exits 0.

- [ ] **Step 16.4: Commit**

```bash
git add scripts/test_sbatch_live.py
git commit -m "test(sbatch): add live smoke test for KUDPC / SLURM"
```

---

## Task 17: Final verification

**Files:** N/A — verifications only

- [ ] **Step 17.1: Full Rust test suite**

```bash
cargo test --lib --all-targets
```
Expected: all green.

- [ ] **Step 17.2: Clippy**

```bash
cargo clippy --all-targets -- -D warnings
```
Expected: no warnings.

- [ ] **Step 17.3: rustfmt check**

```bash
cargo fmt --all -- --check
```
Expected: no diff.

- [ ] **Step 17.4: Regenerate stubs + ruff format**

```bash
cargo run --bin stub_gen && uv run ruff format python/
```
Expected: stubs regenerated; ruff format produces no remaining issues.

- [ ] **Step 17.5: Final pytest sweep**

```bash
uv run pytest python/tests -v
```
Expected: all green except live tests (which skip).

- [ ] **Step 17.6: Final commit (formatting drift only)**

```bash
git add -A
git status
git commit -m "chore(sbatch): regenerate .pyi stubs + final formatting pass" || \
  echo "no formatting drift; nothing to commit"
```

---

## Out of Scope (Phase 2 — separate plan)

- `--array` (`-a`) configuration + per-task handle / snapshot model
- `--dependency` (`-d`) typed enum
- `--mail-user` / `--mail-type`
- `--no-requeue`, `--signal`, `--comment`
- `sbatch --wait` based synchronous `run()` method
- Log-file `tail` / `read` ergonomics on the handle
- Common `JobHandleCommon` trait covering tssrun + sbatch
- sacct `ExitCode` parsing (Phase 1 leaves `FinishedInfo.exit_code = None` after sacct)
