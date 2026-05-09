//! `JobHandleSnapshot` (Serde) and `JobHandle` (in-process state).

use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Where the tee task is writing the child's logs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum LogLocations {
    /// Either no sink is attached or the sink is non-file-backed.
    None,
    /// Two append-only files on the local filesystem.
    Files { stdout: PathBuf, stderr: PathBuf },
    // Future: Sqlite { db_path: PathBuf, run_id: u64 }
}

/// Recorded once the child exits.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FinishedInfo {
    pub exit_code: Option<i32>,
    pub finished_at_unix: i64,
}

/// Persistable snapshot of a tssrun job. Updated by the tee task as the
/// `salloc:` lines arrive and by the wait task on child exit.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobHandleSnapshot {
    pub pid: u32,
    pub argv: Vec<String>,
    pub sent_env: HashMap<String, String>,
    pub cwd: Option<PathBuf>,
    pub started_at_unix: i64,
    pub log_locations: LogLocations,
    pub jobid: Option<u64>,
    pub node: Option<String>,
    pub finished: Option<FinishedInfo>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn snap_running() -> JobHandleSnapshot {
        JobHandleSnapshot {
            pid: 31415,
            argv: vec!["tssrun".into(), "/work/job.sh".into()],
            sent_env: HashMap::from([("OMP_NUM_THREADS".into(), "8".into())]),
            cwd: Some(PathBuf::from("/work")),
            started_at_unix: 1746345600,
            log_locations: LogLocations::Files {
                stdout: PathBuf::from("/var/log/x/o"),
                stderr: PathBuf::from("/var/log/x/e"),
            },
            jobid: Some(102362),
            node: Some("cnode3".into()),
            finished: None,
        }
    }

    #[test]
    fn snapshot_round_trip_running() {
        let s = snap_running();
        let json = serde_json::to_string(&s).unwrap();
        let back: JobHandleSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }

    #[test]
    fn snapshot_round_trip_finished_none_loglocations() {
        let mut s = snap_running();
        s.log_locations = LogLocations::None;
        s.finished = Some(FinishedInfo {
            exit_code: Some(0),
            finished_at_unix: 1746349200,
        });
        let json = serde_json::to_string(&s).unwrap();
        let back: JobHandleSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }
}
