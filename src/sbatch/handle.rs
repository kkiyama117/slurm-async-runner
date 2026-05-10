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
        if self.left_active_listing {
            return false;
        }
        self.last_observed_state
            .as_ref()
            .map(|s| s.state.is_running())
            .unwrap_or(false)
    }

    pub fn is_finished(&self) -> bool {
        self.finished.is_some()
    }

    pub fn exit_code(&self) -> Option<i32> {
        self.finished.as_ref().and_then(|f| f.exit_code)
    }
}

impl SbatchJobSnapshot {
    pub fn output_path(&self) -> Option<PathBuf> {
        self.log
            .output_template
            .as_deref()
            .map(|t| resolve_log_path(t, self.jobid, self.job_name.as_deref()))
    }

    pub fn error_path(&self) -> Option<PathBuf> {
        self.log
            .error_template
            .as_deref()
            .map(|t| resolve_log_path(t, self.jobid, self.job_name.as_deref()))
    }

    pub fn is_running(&self) -> bool {
        self.lifecycle.is_running()
    }
    pub fn is_finished(&self) -> bool {
        self.lifecycle.is_finished()
    }
    pub fn exit_code(&self) -> Option<i32> {
        self.lifecycle.exit_code()
    }
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
