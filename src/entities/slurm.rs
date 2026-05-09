//! SLURM tier — sbatch directive primitives ([`sbatch_options`]) plus
//! the runtime job status snapshot ([`status`]).
//!
//! - [`sbatch_options`] — `SlurmJobConfig` envelope and the per-field
//!   primitives (`SlurmArraySpec`, `SlurmDependency`, `ResourceSpec`,
//!   `JobTimeLimit`, `MailType`, …) that build up an `sbatch` invocation.
//! - [`status`] — `JobStatus` `(state, reason)` pair mirroring what
//!   `squeue %T %r` / `sacct %State %Reason` reports.
//!
//! Top-level items from both submodules are re-exported here so existing
//! call sites such as `crate::entities::slurm::SlurmJobConfig` and
//! `crate::entities::slurm::JobStatus` keep working without a path
//! change.
//!
//! For detail, see [Kyoto Univ doc](https://web.kudpc.kyoto-u.ac.jp/manual/ja/run/batch#slurm) and [Official SLURM page](https://slurm.schedmd.com/sbatch.html).

pub mod sbatch_options;
pub mod status;

pub use sbatch_options::{
    ArrayIndex, DependencyClause, DependencyJobRef, DependencyJoin, DependencyType, JobPartition,
    JobRSC, JobTimeLimit, MailAddress, MailType, MailTypeInput, Memory, MemoryUnit, ResourceSpec,
    ResourceSpecCPU, ResourceSpecGPU, SlurmArraySpec, SlurmDependency, SlurmJobConfig,
};
pub use status::{JobReason, JobState, JobStatus};
