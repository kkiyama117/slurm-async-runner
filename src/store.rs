//! Generic snapshot persistence shared by tssrun and sbatch modules.
//!
//! See `docs/superpowers/specs/2026-05-10-sbatch-module-design.md` §5
//! for the full design rationale.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use anyhow::{Context as _, Result, anyhow};
use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

pub trait JobSnapshot: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {
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

    /// Remove the snapshot stored under `uuid`, if any.
    ///
    /// Idempotent: deleting a uuid that does not exist (or was already
    /// deleted) is `Ok(())`, so callers can GC unconditionally without
    /// racing other deleters.
    async fn delete(&self, uuid: Uuid) -> Result<()>;

    async fn find_by_jobid(&self, jobid: u64) -> Result<Option<S>> {
        Ok(self
            .list()
            .await?
            .into_iter()
            .find(|s| s.jobid() == Some(jobid)))
    }

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
    pub fn new() -> Self {
        Self::default()
    }

    /// Lock the map. A `std::sync::Mutex` is deliberate here: every
    /// critical section is a synchronous `HashMap` operation with no
    /// `.await` inside, so the guard can never be held across a yield
    /// point. Poisoning (a panic while holding the lock) is recovered
    /// by taking the inner guard — the map itself is always left in a
    /// consistent state by the operations below.
    fn lock(&self) -> MutexGuard<'_, HashMap<Uuid, S>> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn len(&self) -> usize {
        self.lock().len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl<S: JobSnapshot> JobStateStore<S> for InMemoryStateStore<S> {
    async fn save(&self, snap: &S) -> Result<()> {
        self.lock().insert(snap.uuid(), snap.clone());
        Ok(())
    }

    async fn load(&self, uuid: Uuid) -> Result<Option<S>> {
        Ok(self.lock().get(&uuid).cloned())
    }

    async fn list(&self) -> Result<Vec<S>> {
        Ok(self.lock().values().cloned().collect())
    }

    async fn delete(&self, uuid: Uuid) -> Result<()> {
        // Idempotent: removing an absent key is a no-op.
        self.lock().remove(&uuid);
        Ok(())
    }
}

/// On-disk store: writes `{root}/<uuid>.json` via atomic rename, with
/// top-level `"kind"` and `"schema_version"` fields added on save and
/// verified on load. Files whose `kind` does not match `S::kind()` are
/// silently skipped during scans, so multiple snapshot types may coexist
/// in the same `root`; files with an unsupported `schema_version` are an
/// error on `load` and are skipped with a warning during `list`.
///
/// The directory is created lazily on first `save`. A *missing* directory
/// during scan is treated as "no entries" (returns empty vec / Ok(None)).
pub struct FileSystemStateStore<S: JobSnapshot> {
    root: PathBuf,
    _phantom: PhantomData<S>,
}

impl<S: JobSnapshot> FileSystemStateStore<S> {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            _phantom: PhantomData,
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

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
                Err(e) => {
                    tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "skipping unreadable snapshot file during list()",
                    );
                    continue;
                }
            };
            match decode_with_kind_check::<S>(&bytes, &path) {
                Ok(Some(snap)) => out.push(snap),
                // Different `kind` sharing the dir: silent skip by design.
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "skipping undecodable snapshot file during list()",
                    );
                }
            }
        }
        Ok(out)
    }

    async fn delete(&self, uuid: Uuid) -> Result<()> {
        let path = self.path_for(uuid);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            // Idempotent: already gone (or the root dir was never created).
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).with_context(|| format!("delete {}", path.display())),
        }
    }
}

/// On-disk envelope schema version written by [`write_atomic_json`].
///
/// Bump this only on a breaking change to the envelope/snapshot layout;
/// readers reject files with a *newer* version instead of misparsing them.
const SCHEMA_VERSION: u64 = 1;

/// Decode JSON bytes as `S`, but only if the on-disk `kind` field matches
/// `S::kind()`. Legacy fallback: a missing `kind` is treated as `S::kind()`
/// for back-compat with snapshots written by older code.
///
/// The envelope's `schema_version` is validated first: a missing field is
/// treated as version 1 (every file written before the field existed), and
/// any version other than [`SCHEMA_VERSION`] is an error so that files from
/// a future build are surfaced instead of silently misread.
fn decode_with_kind_check<S: JobSnapshot>(bytes: &[u8], path: &Path) -> Result<Option<S>> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).with_context(|| format!("decode {}", path.display()))?;
    match value.get("schema_version") {
        // Legacy file predating the field: treat as version 1.
        None => {}
        Some(v) => {
            let version = v.as_u64().ok_or_else(|| {
                anyhow!(
                    "unsupported schema_version {v} (this build supports {SCHEMA_VERSION}) in {}",
                    path.display()
                )
            })?;
            if version != SCHEMA_VERSION {
                return Err(anyhow!(
                    "unsupported schema_version {version} (this build supports {SCHEMA_VERSION}) in {}",
                    path.display()
                ));
            }
        }
    }
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
    std::fs::create_dir_all(root).with_context(|| format!("mkdir -p {}", root.display()))?;
    let mut value =
        serde_json::to_value(snap).with_context(|| "serialize snapshot to json".to_string())?;
    let obj = value
        .as_object_mut()
        .ok_or_else(|| anyhow!("snapshot did not serialize to a JSON object"))?;
    obj.insert(
        "kind".to_string(),
        serde_json::Value::String(S::kind().to_string()),
    );
    obj.insert(
        "schema_version".to_string(),
        serde_json::Value::from(SCHEMA_VERSION),
    );
    let mut tmp = tempfile::NamedTempFile::new_in(root)
        .with_context(|| format!("tempfile in {}", root.display()))?;
    serde_json::to_writer_pretty(&mut tmp, &value)
        .with_context(|| "write json to tempfile".to_string())?;
    tmp.persist(path)
        .map_err(|e| anyhow!("persist {}: {e}", path.display()))?;
    Ok(())
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
        fn uuid(&self) -> Uuid {
            self.uuid
        }
        fn jobid(&self) -> Option<u64> {
            self.jobid
        }
        fn kind() -> &'static str {
            "synthetic"
        }
    }

    fn snap(uuid: Uuid, jobid: Option<u64>) -> Synthetic {
        Synthetic {
            uuid,
            jobid,
            payload: "x".to_string(),
        }
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

    // ---------- FileSystemStateStore ----------

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct OtherKind {
        uuid: Uuid,
        payload: String,
    }

    impl JobSnapshot for OtherKind {
        fn uuid(&self) -> Uuid {
            self.uuid
        }
        fn jobid(&self) -> Option<u64> {
            None
        }
        fn kind() -> &'static str {
            "other"
        }
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
        let other = OtherKind {
            uuid: Uuid::now_v7(),
            payload: "z".to_string(),
        };
        other_store.save(&other).await.unwrap();
        assert_eq!(synth_store.load(other.uuid).await.unwrap(), None);
        assert_eq!(other_store.load(other.uuid).await.unwrap(), Some(other));
    }

    #[tokio::test]
    async fn fs_list_filters_by_kind_in_shared_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let synth_store: FileSystemStateStore<Synthetic> = FileSystemStateStore::new(tmp.path());
        let other_store: FileSystemStateStore<OtherKind> = FileSystemStateStore::new(tmp.path());
        synth_store
            .save(&snap(Uuid::now_v7(), Some(1)))
            .await
            .unwrap();
        synth_store
            .save(&snap(Uuid::now_v7(), Some(2)))
            .await
            .unwrap();
        other_store
            .save(&OtherKind {
                uuid: Uuid::now_v7(),
                payload: "z".to_string(),
            })
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

    // ---------- delete ----------

    #[tokio::test]
    async fn in_memory_delete_removes_snapshot_and_unknown_uuid_is_ok() {
        let store: InMemoryStateStore<Synthetic> = InMemoryStateStore::new();
        let s = snap(Uuid::now_v7(), Some(7));
        store.save(&s).await.unwrap();
        assert_eq!(store.load(s.uuid).await.unwrap(), Some(s.clone()));

        store.delete(s.uuid).await.unwrap();
        assert_eq!(store.load(s.uuid).await.unwrap(), None);
        assert!(store.list().await.unwrap().is_empty());

        // Idempotent: deleting again (or a never-saved uuid) is Ok(()).
        store.delete(s.uuid).await.unwrap();
        store.delete(Uuid::now_v7()).await.unwrap();
    }

    #[tokio::test]
    async fn fs_delete_removes_snapshot_and_unknown_uuid_is_ok() {
        let tmp = tempfile::tempdir().unwrap();
        let store: FileSystemStateStore<Synthetic> = FileSystemStateStore::new(tmp.path());
        let a = snap(Uuid::now_v7(), Some(1));
        let b = snap(Uuid::now_v7(), Some(2));
        store.save(&a).await.unwrap();
        store.save(&b).await.unwrap();
        assert_eq!(store.list().await.unwrap().len(), 2);

        store.delete(a.uuid).await.unwrap();
        assert_eq!(store.load(a.uuid).await.unwrap(), None);
        assert_eq!(store.list().await.unwrap(), vec![b]);
        assert!(!tmp.path().join(format!("{}.json", a.uuid)).exists());

        // Idempotent: file already gone, and dir-missing case too.
        store.delete(a.uuid).await.unwrap();
        let missing_dir: FileSystemStateStore<Synthetic> =
            FileSystemStateStore::new(tmp.path().join("never-created"));
        missing_dir.delete(Uuid::now_v7()).await.unwrap();
    }

    // ---------- corruption handling ----------

    #[tokio::test]
    async fn fs_list_skips_corrupt_file_but_returns_valid_ones() {
        let tmp = tempfile::tempdir().unwrap();
        let store: FileSystemStateStore<Synthetic> = FileSystemStateStore::new(tmp.path());
        let s = snap(Uuid::now_v7(), Some(7));
        store.save(&s).await.unwrap();
        std::fs::write(
            tmp.path().join(format!("{}.json", Uuid::now_v7())),
            b"{ this is not json",
        )
        .unwrap();

        // The corrupt file is skipped (with a warn! we cannot easily assert),
        // valid snapshots are still returned, and list() does not error.
        assert_eq!(store.list().await.unwrap(), vec![s]);
    }

    // ---------- schema_version ----------

    #[tokio::test]
    async fn fs_save_writes_schema_version_1_and_round_trips() {
        let tmp = tempfile::tempdir().unwrap();
        let store: FileSystemStateStore<Synthetic> = FileSystemStateStore::new(tmp.path());
        let s = snap(Uuid::now_v7(), Some(7));
        store.save(&s).await.unwrap();

        let raw = std::fs::read_to_string(tmp.path().join(format!("{}.json", s.uuid))).unwrap();
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v.get("schema_version").and_then(|x| x.as_u64()), Some(1));

        assert_eq!(store.load(s.uuid).await.unwrap(), Some(s));
    }

    #[tokio::test]
    async fn fs_legacy_file_without_schema_version_is_loaded() {
        // Mimic the pre-schema_version on-disk format: snapshot fields plus
        // a top-level "kind", but no "schema_version".
        let tmp = tempfile::tempdir().unwrap();
        let s = snap(Uuid::now_v7(), Some(50));
        let mut value = serde_json::to_value(&s).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("kind".to_string(), serde_json::json!("synthetic"));
        std::fs::write(
            tmp.path().join(format!("{}.json", s.uuid)),
            serde_json::to_string_pretty(&value).unwrap(),
        )
        .unwrap();

        let store: FileSystemStateStore<Synthetic> = FileSystemStateStore::new(tmp.path());
        assert_eq!(store.load(s.uuid).await.unwrap(), Some(s));
    }

    #[tokio::test]
    async fn fs_unsupported_schema_version_fails_load_and_is_skipped_by_list() {
        let tmp = tempfile::tempdir().unwrap();
        let store: FileSystemStateStore<Synthetic> = FileSystemStateStore::new(tmp.path());

        let future = snap(Uuid::now_v7(), Some(99));
        let mut value = serde_json::to_value(&future).unwrap();
        let obj = value.as_object_mut().unwrap();
        obj.insert("kind".to_string(), serde_json::json!("synthetic"));
        obj.insert("schema_version".to_string(), serde_json::json!(99));
        std::fs::write(
            tmp.path().join(format!("{}.json", future.uuid)),
            serde_json::to_string_pretty(&value).unwrap(),
        )
        .unwrap();

        let err = store.load(future.uuid).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("unsupported schema_version 99 (this build supports 1)"),
            "unexpected error message: {msg}"
        );

        // list() skips the unreadable-future file instead of aborting.
        let ok = snap(Uuid::now_v7(), Some(1));
        store.save(&ok).await.unwrap();
        assert_eq!(store.list().await.unwrap(), vec![ok]);
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
}
