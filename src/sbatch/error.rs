//! Spawn-time errors with structured recovery information.

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SbatchSpawnError {
    #[error("sbatch invocation failed (exit={exit_code}): {stdout}")]
    SubmitFailed { exit_code: i32, stdout: String },

    #[error("sbatch stdout did not contain a parseable jobid: {stdout}")]
    JobidParseError { stdout: String },

    #[error("sbatch submitted jobid={jobid} but snapshot save failed: {source}")]
    SubmittedButUnpersisted {
        jobid: u64,
        #[source]
        source: anyhow::Error,
    },

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
