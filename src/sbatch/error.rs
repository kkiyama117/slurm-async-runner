//! Spawn-time errors with structured recovery information.

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SbatchSpawnError {
    #[error("sbatch invocation failed (exit={exit_code}): {stdout}")]
    SubmitFailed { exit_code: i32, stdout: String },

    #[error("sbatch stdout did not contain a parseable jobid: {stdout}")]
    JobidParseError { stdout: String },

    #[error("--export key contains forbidden char (`,` or `=`): {key:?}")]
    InvalidExportKey { key: String },

    #[error("--export value for key {key:?} contains forbidden char (`,` or `=`): {value:?}")]
    InvalidExportValue { key: String, value: String },

    #[error("sbatch submitted jobid={jobid} but snapshot save failed: {source}")]
    SubmittedButUnpersisted {
        jobid: u64,
        #[source]
        source: anyhow::Error,
    },

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Errors that can occur while attaching to an existing snapshot.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SbatchAttachError {
    #[error("snapshot not found for key {key:?}")]
    NotFound { key: String },

    #[error("snapshot kind mismatch: expected '{expected}', got '{got}'")]
    KindMismatch { expected: &'static str, got: String },

    #[error(
        "jobid {jobid} matched {count} snapshots; use attach_array_jobid \
         to retrieve per-task handles or attach_uuid for a specific task"
    )]
    MultipleMatch { jobid: u64, count: usize },

    #[error("io error during attach: {0}")]
    Io(#[from] anyhow::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn invalid_export_key_carries_offending_string() {
        let e = SbatchSpawnError::InvalidExportKey {
            key: "BAD,KEY".to_string(),
        };
        let msg = e.to_string();
        assert!(
            msg.contains("BAD,KEY"),
            "expected key in message, got: {msg}"
        );
    }

    #[test]
    fn invalid_export_value_carries_offending_strings() {
        let e = SbatchSpawnError::InvalidExportValue {
            key: "FOO".to_string(),
            value: "a=b".to_string(),
        };
        let msg = e.to_string();
        assert!(msg.contains("FOO"), "expected key in message, got: {msg}");
        assert!(msg.contains("a=b"), "expected value in message, got: {msg}");
    }
}
