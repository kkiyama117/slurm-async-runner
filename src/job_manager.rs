//! Cross-backend manager abstraction (issue #16 item 2).
//!
//! [`JobManager`] is the manager-side companion to
//! [`JobHandleCommon`](crate::handle::JobHandleCommon): where the handle
//! trait unifies *observing* a job across tssrun and sbatch, this trait
//! unifies *obtaining* one — `spawn` plus the two attach lookups both
//! backends support (`uuid` primary key, `jobid` best-effort).
//!
//! Backend-specific entry points stay inherent on purpose:
//! `SbatchManager::spawn_array` / `attach_array_jobid` / `run` / `cancel`
//! have no tssrun counterpart, and `TssrunManager::attach(AttachKey::Pid |
//! File)` has no sbatch counterpart. Generic code that needs those should
//! take the concrete manager.
//!
//! Like `JobHandleCommon`, the trait is **not** dyn-safe — it carries
//! associated `Handle` and error types so callers keep the concrete handle
//! and the typed error enums ([`TssrunSpawnError`](crate::TssrunSpawnError),
//! [`SbatchSpawnError`](crate::SbatchSpawnError), …) without boxing. No
//! `DynJobManager` companion exists yet; add one only when a heterogenous
//! manager collection is actually needed.

use uuid::Uuid;

use crate::handle::JobHandleCommon;

/// Unified manager abstraction over tssrun and sbatch.
///
/// Implementors hand out their backend's [`JobHandleCommon`] handle via
/// `spawn` (submit / launch the configured command) and the attach pair
/// (reconstruct a handle from persisted state).
///
/// # Invariants
///
/// - `attach_uuid` is the primary-key lookup: O(1) against the configured
///   store, never a scan.
/// - `attach_jobid` is best-effort: it may scan, and it fails (typed
///   `AttachError`) when the jobid is unknown — or, for sbatch array
///   submissions, ambiguous.
#[async_trait::async_trait]
pub trait JobManager: Send + Sync {
    /// Handle type this manager hands out.
    type Handle: JobHandleCommon;
    /// Typed spawn-failure enum (e.g. [`crate::TssrunSpawnError`]).
    type SpawnError: std::error::Error + Send + Sync + 'static;
    /// Typed attach-failure enum (e.g. [`crate::TssrunAttachError`]).
    type AttachError: std::error::Error + Send + Sync + 'static;

    /// Launch / submit the configured command and return a live handle.
    async fn spawn(&self) -> Result<Self::Handle, Self::SpawnError>;

    /// Reconstruct a handle from the persisted snapshot keyed by `uuid`.
    async fn attach_uuid(&self, uuid: Uuid) -> Result<Self::Handle, Self::AttachError>;

    /// Reconstruct a handle by SLURM jobid (best-effort lookup).
    async fn attach_jobid(&self, jobid: u64) -> Result<Self::Handle, Self::AttachError>;
}
