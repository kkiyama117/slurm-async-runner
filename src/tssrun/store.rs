//! `TssrunJobSnapshot` participation in the generic [`JobStateStore`] layer.
//!
//! See `docs/superpowers/specs/2026-05-10-sbatch-module-design.md` §5
//! for why this module shrunk from a full trait + two impls to just the
//! `JobSnapshot` trait impl + back-compat type aliases.

use uuid::Uuid;

use crate::store::JobSnapshot;
use crate::tssrun::handle::TssrunJobSnapshot;

impl JobSnapshot for TssrunJobSnapshot {
    fn uuid(&self) -> Uuid {
        self.uuid
    }
    fn jobid(&self) -> Option<u64> {
        self.jobid
    }
    fn kind() -> &'static str {
        "tssrun"
    }
}

// Back-compat aliases.
pub type JobStateStore = dyn crate::store::JobStateStore<TssrunJobSnapshot>;
pub type InMemoryStateStore = crate::store::InMemoryStateStore<TssrunJobSnapshot>;
pub type FileSystemStateStore = crate::store::FileSystemStateStore<TssrunJobSnapshot>;

/// Tssrun-specific helper: scan the store for the first snapshot whose
/// `pid` equals `pid`. Built on `list()` because `pid` is not a generic
/// `JobSnapshot` concept.
pub async fn find_by_pid(
    store: &dyn crate::store::JobStateStore<TssrunJobSnapshot>,
    pid: u32,
) -> anyhow::Result<Option<TssrunJobSnapshot>> {
    Ok(store.list().await?.into_iter().find(|s| s.pid == pid))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::JobStateStore as _;
    use crate::tssrun::handle::LogLocations;

    fn snap(uuid: Uuid, pid: u32, jobid: Option<u64>) -> TssrunJobSnapshot {
        TssrunJobSnapshot {
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
