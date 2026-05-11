//! `SbatchJobSnapshot` participation in the generic [`JobStateStore`] layer.

use uuid::Uuid;

use crate::sbatch::handle::SbatchJobSnapshot;
use crate::store::JobSnapshot;

impl JobSnapshot for SbatchJobSnapshot {
    fn uuid(&self) -> Uuid {
        self.uuid
    }
    fn jobid(&self) -> Option<u64> {
        Some(self.jobid)
    }
    fn kind() -> &'static str {
        "sbatch"
    }
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

    #[tokio::test]
    async fn sbatch_snapshot_round_trips_via_fs_store() {
        let tmp = tempfile::tempdir().unwrap();
        let store: FileSystemStateStore<SbatchJobSnapshot> = FileSystemStateStore::new(tmp.path());
        let s = snap(12345);
        store.save(&s).await.unwrap();
        assert_eq!(store.load(s.uuid).await.unwrap(), Some(s.clone()));

        let path = tmp.path().join(format!("{}.json", s.uuid));
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(raw.contains("\"kind\": \"sbatch\""));
    }

    #[tokio::test]
    async fn array_task_id_roundtrips_via_fs_store() {
        let tmp = tempfile::tempdir().unwrap();
        let store: FileSystemStateStore<SbatchJobSnapshot> = FileSystemStateStore::new(tmp.path());
        let mut s = snap(12345);
        s.array_task_id = Some(7);
        store.save(&s).await.unwrap();
        let loaded = store.load(s.uuid).await.unwrap().unwrap();
        assert_eq!(loaded.array_task_id, Some(7));
    }

    #[tokio::test]
    async fn array_task_id_defaults_to_none_for_legacy_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        let store: FileSystemStateStore<SbatchJobSnapshot> = FileSystemStateStore::new(tmp.path());
        let s = snap(42);
        store.save(&s).await.unwrap();
        let loaded = store.load(s.uuid).await.unwrap().unwrap();
        assert_eq!(loaded.array_task_id, None);
    }

    /// Forward-compat check: Phase 2 snapshots on disk contain
    /// `array_jobid: <N>` from when the field was still mirrored. Phase
    /// 3 dropped the field. Because `SbatchJobSnapshot` does NOT set
    /// `#[serde(deny_unknown_fields)]`, the surplus key must be
    /// silently ignored.
    #[tokio::test]
    async fn legacy_array_jobid_field_is_ignored_on_load() {
        let tmp = tempfile::tempdir().unwrap();
        let uuid = Uuid::now_v7();
        let path = tmp.path().join(format!("{uuid}.json"));
        let raw = serde_json::json!({
            "kind": "sbatch",
            "uuid": uuid.to_string(),
            "jobid": 77777u64,
            "array_jobid": 77777u64,
            "array_task_id": 3,
            "argv": ["sbatch"],
            "sent_env": {},
            "script_path": "/w/job.sh",
            "submitted_at": "2026-05-11T12:00:00Z",
            "log": { "output_template": null, "error_template": null },
            "lifecycle": {
                "last_observed_state": null,
                "last_observed_at": null,
                "left_active_listing": false,
                "finished": null
            }
        });
        std::fs::write(&path, serde_json::to_vec_pretty(&raw).unwrap()).unwrap();

        let store: FileSystemStateStore<SbatchJobSnapshot> = FileSystemStateStore::new(tmp.path());
        let loaded = store.load(uuid).await.unwrap().unwrap();
        assert_eq!(loaded.jobid, 77777);
        assert_eq!(loaded.array_task_id, Some(3));
    }

    #[tokio::test]
    async fn sbatch_and_tssrun_coexist_in_same_dir() {
        use crate::tssrun::handle::TssrunJobSnapshot;
        let tmp = tempfile::tempdir().unwrap();
        let sb_store: FileSystemStateStore<SbatchJobSnapshot> =
            FileSystemStateStore::new(tmp.path());
        let ts_store: FileSystemStateStore<TssrunJobSnapshot> =
            FileSystemStateStore::new(tmp.path());
        sb_store.save(&snap(1)).await.unwrap();
        ts_store
            .save(&TssrunJobSnapshot {
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
            })
            .await
            .unwrap();
        assert_eq!(sb_store.list().await.unwrap().len(), 1);
        assert_eq!(ts_store.list().await.unwrap().len(), 1);
    }
}
