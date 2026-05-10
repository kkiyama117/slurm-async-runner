//! Generic snapshot persistence shared by tssrun and sbatch modules.
//!
//! See `docs/superpowers/specs/2026-05-10-sbatch-module-design.md` §5
//! for the full design rationale.

#[allow(unused_imports)]
use std::collections::HashMap;
#[allow(unused_imports)]
use std::marker::PhantomData;
#[allow(unused_imports)]
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[allow(unused_imports)]
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
    pub fn new() -> Self {
        Self::default()
    }
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("InMemoryStateStore poisoned")
            .len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl<S: JobSnapshot> JobStateStore<S> for InMemoryStateStore<S> {
    async fn save(&self, snap: &S) -> Result<()> {
        let mut g = self
            .inner
            .lock()
            .map_err(|_| anyhow!("InMemoryStateStore mutex poisoned"))?;
        g.insert(snap.uuid(), snap.clone());
        Ok(())
    }

    async fn load(&self, uuid: Uuid) -> Result<Option<S>> {
        let g = self
            .inner
            .lock()
            .map_err(|_| anyhow!("InMemoryStateStore mutex poisoned"))?;
        Ok(g.get(&uuid).cloned())
    }

    async fn list(&self) -> Result<Vec<S>> {
        let g = self
            .inner
            .lock()
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
}
