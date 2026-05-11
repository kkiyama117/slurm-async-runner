//! Spawn-time errors with structured recovery information.

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SbatchSpawnError {
    /// `output` is the captured combined stdout/stderr from the
    /// `sbatch` invocation. The dispatcher `capture` API merges both
    /// streams, so this field can contain stderr lines as well.
    #[error("sbatch invocation failed (exit={exit_code}): {output}")]
    SubmitFailed { exit_code: i32, output: String },

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

/// Errors that can occur during a blocking submit-and-wait `run()` call.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SbatchRunError {
    #[error("spawn failed: {0}")]
    Spawn(#[from] SbatchSpawnError),

    #[error("wait_terminal io error: {0}")]
    Wait(anyhow::Error),

    #[error("sacct refresh failed: {0}")]
    Sacct(anyhow::Error),

    #[error("sacct returned but finished info was not populated for jobid={jobid}")]
    MissingFinished { jobid: u64 },

    #[error("job ended in non-success terminal state: {state}, exit_code={exit_code:?}")]
    JobFailed {
        state: crate::JobState,
        exit_code: Option<i32>,
    },

    #[error(
        "array submission is not supported by run(); use spawn_array() instead \
         and await tasks individually"
    )]
    ArrayNotSupported,
}

/// Errors that can occur while sending `scancel <jobid>`.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SbatchCancelError {
    /// `output` is the captured combined stdout/stderr from the
    /// `scancel` invocation. The dispatcher `capture` API merges both
    /// streams, so this field can contain stderr lines as well.
    #[error("scancel failed (exit={exit_code}): {output}")]
    Scancel { exit_code: i32, output: String },

    #[error(transparent)]
    Other(#[from] anyhow::Error),
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

    #[test]
    fn run_error_array_not_supported_displays_helpful_message() {
        let e = SbatchRunError::ArrayNotSupported;
        let msg = e.to_string();
        assert!(
            msg.contains("array"),
            "expected 'array' in message, got: {msg}"
        );
        assert!(
            msg.contains("spawn_array"),
            "should point user at spawn_array, got: {msg}"
        );
    }

    #[test]
    fn run_error_job_failed_carries_state_and_exit_code() {
        let e = SbatchRunError::JobFailed {
            state: crate::JobState::Failed,
            exit_code: Some(2),
        };
        let msg = e.to_string();
        // SLURM canonical token via `JobState::Display` (matches `sacct` /
        // `squeue` output).
        assert!(msg.contains("FAILED"), "state should appear, got: {msg}");
        assert!(msg.contains('2'), "exit code should appear, got: {msg}");
    }

    #[test]
    fn run_error_from_spawn_error_preserves_variant() {
        let inner = SbatchSpawnError::SubmitFailed {
            exit_code: 1,
            output: "boom".into(),
        };
        let e: SbatchRunError = inner.into();
        assert!(matches!(e, SbatchRunError::Spawn(_)));
    }

    #[test]
    fn cancel_error_scancel_carries_exit_and_output() {
        let e = SbatchCancelError::Scancel {
            exit_code: 1,
            output: "scancel: error: invalid job id specified".into(),
        };
        let msg = e.to_string();
        assert!(msg.contains("scancel"));
        assert!(msg.contains("invalid job id specified"));
    }
}
