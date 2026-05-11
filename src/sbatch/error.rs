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

#[cfg(test)]
mod tests {
    use super::*;

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
