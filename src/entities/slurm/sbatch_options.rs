//! sbatch directive primitives plus the [`SlurmJobConfig`] envelope
//! that aggregates them. Sibling module
//! [`crate::entities::slurm::status`] holds the runtime job status
//! snapshot (`JobStatus` / `JobState` / `JobReason`); the workflow
//! node (`Job` / `JobSpec` / `JobEdge` / `JobId` / `Program`) lives
//! under [`crate::entities::workflow`].
//!
//! For detail, see [Kyoto Univ doc](https://web.kudpc.kyoto-u.ac.jp/manual/ja/run/batch#slurm) and [Official SLURM page](https://slurm.schedmd.com/sbatch.html)

pub mod array_spec;

pub mod dependency;

pub mod resource_spec;

pub mod time_limit;

use std::path::PathBuf;

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::error::SchemaParseError;

// `SlurmArraySpec` and `ArrayIndex` live in their own file so the parsing
// and serde plumbing can be reasoned about in isolation. They are
// re-exported here so existing call sites referencing
// `crate::entities::slurm::SlurmArraySpec` keep working.
pub use array_spec::{ArrayIndex, SlurmArraySpec};

// `SlurmDependency` and friends live in their own file (see
// [`crate::entities::dependency`]) so the `--dependency` parsing and serde
// plumbing can be reasoned about in isolation. Re-exported here so existing
// references such as `crate::entities::slurm::SlurmDependency` keep working.
//
//   #SBATCH -d afterok:200
//
// https://slurm.schedmd.com/sbatch.html
// https://web.kudpc.kyoto-u.ac.jp/manual/ja/run/tips#dependency
pub use dependency::{
    DependencyClause, DependencyJobRef, DependencyJoin, DependencyType, SlurmDependency,
};

// `ResourceSpec` and friends live in their own file (see
// [`crate::entities::resource_spec`]) so the colon-separated `--rsc`
// parsing and serde plumbing can be reasoned about in isolation.
// Re-exported here so existing references such as
// `crate::entities::slurm::ResourceSpec` keep working.
//
//   #SBATCH --rsc p=1:t=56:c=56:m=56G   (CPU)
//   #SBATCH --rsc g=2                    (GPU)
//
// https://web.kudpc.kyoto-u.ac.jp/manual/ja/run/batch#slurm
// https://slurm.schedmd.com/sbatch.html
pub use resource_spec::{Memory, MemoryUnit, ResourceSpec, ResourceSpecCPU, ResourceSpecGPU};

// `JobTimeLimit` lives in its own file (see [`crate::entities::slurm::time_limit`])
// so the Slurm `--time` parsing and serde plumbing can be reasoned about in
// isolation. Re-exported here so existing references such as
// `crate::entities::slurm::JobTimeLimit` keep working.
//
//   #SBATCH --time 01:00:00      (HH:MM:SS)
//   #SBATCH --time 3-12:00:00    (D-H:M:S)
//
// https://web.kudpc.kyoto-u.ac.jp/manual/ja/run/batch#slurm
// https://slurm.schedmd.com/sbatch.html
pub use time_limit::JobTimeLimit;

// Each field of Slurm job entity.
pub type JobPartition = String;

/// TODO: implement Custom struct
pub type JobRSC = String;

pub type MailAddress = String;

#[derive(Clone, Debug, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub enum MailType {
    BEGIN,
    END,
    FAIL,
    REQUEUE,
    ALL,
}

impl TryFrom<&str> for MailType {
    type Error = SchemaParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "BEGIN" => Ok(MailType::BEGIN),
            "END" => Ok(MailType::END),
            "FAIL" => Ok(MailType::FAIL),
            "REQUEUE" => Ok(MailType::REQUEUE),
            "ALL" => Ok(MailType::ALL),
            _ => Err(Self::Error::ParseError {
                key: "mail_types".to_string(),
                value: value.to_string(),
            }),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct MailTypeInput(Vec<MailType>);

impl TryFrom<String> for MailTypeInput {
    type Error = SchemaParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(MailTypeInput(
            value.split(',').map(MailType::try_from).try_collect()?,
        ))
    }
}

/// `[slurm]` table — Replesent config  of SLURM submission.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlurmJobConfig {
    /// queue of job. It is required thing
    pub partition: JobPartition,

    /// Wall-clock limit (`--time`). Accepts any of Slurm's six surface forms
    /// (`M`, `M:S`, `H:M:S`, `D-H`, `D-H:M`, `D-H:M:S`); always re-emitted as
    /// canonical `HH:MM:SS`. See [`JobTimeLimit`] for parsing and the
    /// [`TimeDelta`](chrono::TimeDelta) interop.
    #[serde(default)]
    pub time_limit: Option<JobTimeLimit>,

    /// path of stdout
    #[serde(default)]
    pub log_stdout: Option<PathBuf>,

    /// path of stderr
    #[serde(default)]
    pub log_stderr: Option<PathBuf>,

    /// comment
    pub comment: Option<String>,

    /// job_name
    #[serde(default)]
    pub job_name: Option<String>,

    /// spec of Array job
    #[serde(default)]
    pub array_spec: Option<SlurmArraySpec>,

    // / Optional per-field overrides for the post-processing batch.
    // / When omitted, the post batch inherits the surrounding `[slurm]`
    // / values verbatim (per-field fallback per the α design).
    pub dependency: Option<SlurmDependency>,

    #[serde(default)]
    pub mail_user: Option<MailAddress>,

    #[serde(default)]
    pub mail_types: Option<MailTypeInput>,

    /// p=PROCS:t=THREADSc=CORES:m=MEMORY (or g=GPU)
    #[serde(default)]
    pub resource_spec: Option<ResourceSpec>,
}
