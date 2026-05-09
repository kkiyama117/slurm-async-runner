//! Pluggable persistence for [`JobHandleSnapshot`].
//!
//! [`TssrunManager`] used to embed a single `state_dir: Option<PathBuf>`
//! and write `{state_dir}/{uuid}.json` directly. That coupling was
//! unworkable in two situations:
//!
//! 1.  **Read-only / non-writable filesystems.** The on-disk writer
//!     swallows save failures with `tracing::warn!`, but the manager
//!     could not opt into a non-FS backend at all.
//! 2.  **Missing `state_dir` directory.** `find_by_jobid` issued a raw
//!     `read_dir` that surfaced `ENOENT` to the caller as "no persisted
//!     handle matched jobid …" via a misleading error chain.
//!
//! Both are fixed by routing all snapshot persistence through the
//! [`JobStateStore`] trait. The crate ships two implementations:
//!
//! - [`InMemoryStateStore`] — process-local `HashMap`. Default for
//!   single-process use; guarantees `save` cannot fail.
//! - [`FileSystemStateStore`] — writes `{dir}/{uuid}.json` via atomic
//!   rename, treats a missing directory as "no matches" rather than as
//!   an I/O error during scans.
//!
//! Other backends (Redis, SQLite, etc.) can be added behind feature
//! flags without changing [`TssrunManager`] or [`JobHandle`].
//!
//! [`TssrunManager`]: crate::tssrun::manager::TssrunManager
//! [`JobHandle`]: crate::tssrun::handle::JobHandle

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use uuid::Uuid;

use crate::tssrun::handle::JobHandleSnapshot;

/// Persistence backend for [`JobHandleSnapshot`]s, keyed by `uuid`.
///
/// Implementations MUST treat "key not found" as `Ok(None)`, not as an
/// error. Only true I/O / backend failures should propagate as `Err`.
/// Implementations are also responsible for any `save` durability
/// guarantees they advertise (atomicity, fsync, etc.).
#[async_trait]
pub trait JobStateStore: Send + Sync + 'static {
    /// Persist `snap` keyed by `snap.uuid`. Overwrites any prior value
    /// at that uuid. Backends should make `save` idempotent so the
    /// repeated calls from the tee task during a job's lifetime are safe.
    async fn save(&self, snap: &JobHandleSnapshot) -> Result<()>;

    /// Load by primary key. Returns `Ok(None)` if `uuid` is unknown.
    async fn load(&self, uuid: Uuid) -> Result<Option<JobHandleSnapshot>>;

    /// Look up by `pid`. Pids may be recycled by the kernel, so the
    /// result is best-effort — prefer [`Self::load`] when a uuid is
    /// available. Returns the *first* match; callers needing all matches
    /// should iterate via a backend-specific API.
    async fn find_by_pid(&self, pid: u32) -> Result<Option<JobHandleSnapshot>>;

    /// Look up by SLURM `jobid`. Useful only after `salloc:` has been
    /// parsed. Returns `Ok(None)` if no snapshot has that jobid yet.
    async fn find_by_jobid(&self, jobid: u64) -> Result<Option<JobHandleSnapshot>>;
}

/// Process-local in-memory store. Default for [`TssrunManager`] when
/// no other backend is configured.
///
/// All operations are non-blocking and infallible. Cloning the store
/// is cheap (it shares the same `Arc<Mutex<...>>`), so multiple
/// managers in the same process can be wired to the same store if
/// desired.
///
/// [`TssrunManager`]: crate::tssrun::manager::TssrunManager
#[derive(Clone, Default)]
pub struct InMemoryStateStore {
    inner: Arc<Mutex<HashMap<Uuid, JobHandleSnapshot>>>,
}

impl InMemoryStateStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of snapshots currently held. Mostly useful for tests.
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("InMemoryStateStore lock poisoned")
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl JobStateStore for InMemoryStateStore {
    async fn save(&self, snap: &JobHandleSnapshot) -> Result<()> {
        let mut g = self
            .inner
            .lock()
            .map_err(|_| anyhow!("InMemoryStateStore mutex poisoned"))?;
        g.insert(snap.uuid, snap.clone());
        Ok(())
    }

    async fn load(&self, uuid: Uuid) -> Result<Option<JobHandleSnapshot>> {
        let g = self
            .inner
            .lock()
            .map_err(|_| anyhow!("InMemoryStateStore mutex poisoned"))?;
        Ok(g.get(&uuid).cloned())
    }

    async fn find_by_pid(&self, pid: u32) -> Result<Option<JobHandleSnapshot>> {
        let g = self
            .inner
            .lock()
            .map_err(|_| anyhow!("InMemoryStateStore mutex poisoned"))?;
        Ok(g.values().find(|s| s.pid == pid).cloned())
    }

    async fn find_by_jobid(&self, jobid: u64) -> Result<Option<JobHandleSnapshot>> {
        let g = self
            .inner
            .lock()
            .map_err(|_| anyhow!("InMemoryStateStore mutex poisoned"))?;
        Ok(g.values().find(|s| s.jobid == Some(jobid)).cloned())
    }
}

/// On-disk store: writes `{dir}/{uuid}.json` via atomic rename.
///
/// The directory is created lazily on first `save` (rather than at
/// construction) so that a manager wired to a not-yet-existing
/// `state_dir` still spawns successfully — and `find_by_*` against that
/// missing directory returns `Ok(None)` rather than surfacing `ENOENT`.
pub struct FileSystemStateStore {
    dir: PathBuf,
}

impl FileSystemStateStore {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn path_for(&self, uuid: Uuid) -> PathBuf {
        self.dir.join(format!("{uuid}.json"))
    }
}

#[async_trait]
impl JobStateStore for FileSystemStateStore {
    async fn save(&self, snap: &JobHandleSnapshot) -> Result<()> {
        let path = self.path_for(snap.uuid);
        let dir = self.dir.clone();
        let snap = snap.clone();
        // Use spawn_blocking so the atomic-rename (which uses sync std::fs
        // via NamedTempFile) does not block the tokio reactor when callers
        // sit on a slow disk.
        tokio::task::spawn_blocking(move || write_atomic_json(&dir, &path, &snap))
            .await
            .map_err(|e| anyhow!("save spawn_blocking join failed: {e}"))?
    }

    async fn load(&self, uuid: Uuid) -> Result<Option<JobHandleSnapshot>> {
        let path = self.path_for(uuid);
        match tokio::fs::read(&path).await {
            Ok(bytes) => {
                let snap: JobHandleSnapshot = serde_json::from_slice(&bytes)
                    .with_context(|| format!("failed to decode {}", path.display()))?;
                Ok(Some(snap))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("failed to read {}", path.display())),
        }
    }

    async fn find_by_pid(&self, pid: u32) -> Result<Option<JobHandleSnapshot>> {
        scan(&self.dir, |s| s.pid == pid).await
    }

    async fn find_by_jobid(&self, jobid: u64) -> Result<Option<JobHandleSnapshot>> {
        scan(&self.dir, |s| s.jobid == Some(jobid)).await
    }
}

/// Linearly scan `dir` for the first `*.json` whose decoded snapshot
/// satisfies `pred`. A *missing* directory is treated as "no entries"
/// and yields `Ok(None)` — that is the bug fix for `find_by_jobid`
/// crashing on a not-yet-created `state_dir`. Decode errors on
/// individual files are skipped silently so a single corrupt entry
/// cannot block the whole lookup.
async fn scan<F>(dir: &Path, pred: F) -> Result<Option<JobHandleSnapshot>>
where
    F: Fn(&JobHandleSnapshot) -> bool,
{
    let mut rd = match tokio::fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(e).with_context(|| format!("failed to read_dir {}", dir.display()));
        }
    };
    while let Some(entry) = rd.next_entry().await? {
        if entry.path().extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        if let Ok(bytes) = tokio::fs::read(entry.path()).await
            && let Ok(snap) = serde_json::from_slice::<JobHandleSnapshot>(&bytes)
            && pred(&snap)
        {
            return Ok(Some(snap));
        }
    }
    Ok(None)
}

fn write_atomic_json(dir: &Path, path: &Path, snap: &JobHandleSnapshot) -> Result<()> {
    std::fs::create_dir_all(dir)
        .with_context(|| format!("failed to mkdir -p {}", dir.display()))?;
    let mut tmp = tempfile::NamedTempFile::new_in(dir)
        .with_context(|| format!("failed to create tempfile in {}", dir.display()))?;
    serde_json::to_writer_pretty(&mut tmp, snap)?;
    tmp.persist(path)
        .map_err(|e| anyhow!("persist rename to {} failed: {e}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tssrun::handle::LogLocations;

    fn snap(uuid: Uuid, pid: u32, jobid: Option<u64>) -> JobHandleSnapshot {
        JobHandleSnapshot {
            uuid,
            pid,
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

    // ---------- InMemoryStateStore ----------

    #[tokio::test]
    async fn in_memory_save_then_load_round_trips() {
        let store = InMemoryStateStore::new();
        let s = snap(Uuid::now_v7(), 100, Some(7));
        store.save(&s).await.unwrap();
        let got = store.load(s.uuid).await.unwrap();
        assert_eq!(got.as_ref(), Some(&s));
    }

    #[tokio::test]
    async fn in_memory_load_unknown_uuid_returns_none() {
        let store = InMemoryStateStore::new();
        assert!(store.load(Uuid::now_v7()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn in_memory_find_by_pid_and_jobid() {
        let store = InMemoryStateStore::new();
        let a = snap(Uuid::now_v7(), 10, Some(100));
        let b = snap(Uuid::now_v7(), 11, Some(101));
        store.save(&a).await.unwrap();
        store.save(&b).await.unwrap();

        assert_eq!(store.find_by_pid(11).await.unwrap().unwrap().pid, 11);
        assert_eq!(
            store.find_by_jobid(100).await.unwrap().unwrap().jobid,
            Some(100)
        );
        assert!(store.find_by_pid(999).await.unwrap().is_none());
        assert!(store.find_by_jobid(999).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn in_memory_save_overwrites_existing_uuid() {
        let store = InMemoryStateStore::new();
        let uuid = Uuid::now_v7();
        let mut s = snap(uuid, 1, None);
        store.save(&s).await.unwrap();
        s.jobid = Some(42);
        store.save(&s).await.unwrap();
        assert_eq!(store.load(uuid).await.unwrap().unwrap().jobid, Some(42));
        assert_eq!(store.len(), 1, "save should overwrite, not duplicate");
    }

    // ---------- FileSystemStateStore ----------

    #[tokio::test]
    async fn fs_save_creates_directory_lazily() {
        // Constructor must not require the directory to exist; save() is
        // responsible for `mkdir -p` so the manager can be configured with
        // a not-yet-created state_dir.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("nested/does/not/exist");
        let store = FileSystemStateStore::new(&dir);
        assert!(!dir.exists());
        let s = snap(Uuid::now_v7(), 1, None);
        store.save(&s).await.unwrap();
        assert!(dir.exists());
        assert!(dir.join(format!("{}.json", s.uuid)).exists());
    }

    #[tokio::test]
    async fn fs_save_then_load_round_trips() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileSystemStateStore::new(tmp.path());
        let s = snap(Uuid::now_v7(), 42, Some(7));
        store.save(&s).await.unwrap();
        assert_eq!(store.load(s.uuid).await.unwrap().as_ref(), Some(&s));
    }

    #[tokio::test]
    async fn fs_load_unknown_uuid_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileSystemStateStore::new(tmp.path());
        assert!(store.load(Uuid::now_v7()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn fs_find_by_jobid_returns_none_when_dir_missing() {
        // Regression: `find_by_jobid` used to surface ENOENT when state_dir
        // didn't yet exist. Should now return Ok(None) — the directory is
        // simply empty as far as the store is concerned.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("never-created");
        let store = FileSystemStateStore::new(&dir);
        assert!(!dir.exists());
        assert!(store.find_by_jobid(123).await.unwrap().is_none());
        assert!(store.find_by_pid(123).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn fs_scan_skips_corrupt_files() {
        // A single malformed *.json must not block lookup of healthy ones.
        let tmp = tempfile::tempdir().unwrap();
        let store = FileSystemStateStore::new(tmp.path());
        let good = snap(Uuid::now_v7(), 5, Some(50));
        store.save(&good).await.unwrap();
        tokio::fs::write(tmp.path().join("garbage.json"), b"{not json")
            .await
            .unwrap();
        assert_eq!(
            store.find_by_jobid(50).await.unwrap().unwrap().pid,
            5,
            "corrupt sibling must not hide the matching record"
        );
    }

    #[tokio::test]
    async fn fs_save_then_find_by_pid() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FileSystemStateStore::new(tmp.path());
        let s = snap(Uuid::now_v7(), 777, None);
        store.save(&s).await.unwrap();
        assert_eq!(store.find_by_pid(777).await.unwrap().unwrap().pid, 777);
    }
}
