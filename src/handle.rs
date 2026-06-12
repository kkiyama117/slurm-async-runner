//! Cross-backend handle abstraction.
//!
//! [`JobHandleCommon`] is the trait that makes the Phase 2 §7.1 naming
//! convergence mechanically checkable: any code that wants a tssrun-or-sbatch
//! handle can now write `H: JobHandleCommon`.
//!
//! Phase 1 / 2 / 3 P1+P2 set this up:
//! - Phase 2 §7.1 normalized the inherent method names on both backends.
//! - Phase 3 P1 renamed the tssrun structs to `TssrunJobHandle` /
//!   `TssrunJobSnapshot`, making the impl signatures symmetric.
//! - Phase 3 P2 brought `TssrunJobHandle::refresh` to `Result<Snapshot>` and
//!   added `wait_terminal`, completing the method-shape parity with
//!   `SbatchJobHandle`.
//!
//! See `docs/superpowers/specs/2026-05-11-sbatch-phase3-design.md` §3 for the
//! full design rationale.
//!
//! The trait is **not** dyn-safe by design — it carries an associated type
//! `Snapshot`. Phase 3 P4 introduces the type-erased `DynJobHandleCommon`
//! companion for callers that need `dyn` dispatch.

use std::sync::Arc;

use anyhow::Result;
use tokio::sync::watch;
use uuid::Uuid;

use crate::store::JobSnapshot;

/// Unified handle abstraction over tssrun and sbatch.
///
/// Implementors expose the **core 5 sync getters** that both backends already
/// provide ([`uuid`](Self::uuid), [`jobid`](Self::jobid),
/// [`is_running`](Self::is_running), [`is_finished`](Self::is_finished),
/// [`exit_code`](Self::exit_code)), plus snapshot accessors and the async
/// [`refresh`](Self::refresh) / [`wait_terminal`](Self::wait_terminal) pair.
///
/// All sync methods read from a lock-free [`watch::Receiver`] snapshot — they
/// are non-blocking and safe to call concurrently with an in-flight `refresh`
/// / `wait_terminal` (Phase 2 §2.4 invariant).
///
/// # Invariants
///
/// - `refresh()` MUST NOT call `sacct` (Phase 1 handover §2). Sacct is the
///   heavy-weight finalizer reserved for `SbatchJobHandle::refresh_with_sacct`
///   and `SbatchManager::run`.
/// - `wait_terminal(d)` polls via `refresh()` every `d` and returns the first
///   snapshot that satisfies the backend's "terminal" predicate (sbatch:
///   `last_observed_state.is_terminal() || left_active_listing`; tssrun:
///   `is_finished()`).
///
/// # Why not dyn-safe?
///
/// The associated `Snapshot` type lets each backend return its own concrete
/// snapshot struct without boxing or JSON flattening on the hot path. For
/// `dyn`-erased usage see `DynJobHandleCommon` (Phase 3 P4) which trades the
/// concrete type for object safety.
#[async_trait::async_trait]
pub trait JobHandleCommon: Send + Sync + 'static {
    /// On-disk snapshot type carrying the SLURM job state plus
    /// backend-specific fields.
    type Snapshot: JobSnapshot;

    // ─── core 5 sync getters (Phase 2 §7.1 naming convergence) ───

    /// Stable UUID v7 primary key for this job — survives `pid` recycling
    /// and pre-`jobid` lookups.
    fn uuid(&self) -> Uuid;

    /// SLURM job id once observed. `None` for a tssrun job that has not yet
    /// emitted its `salloc:` banner; always `Some(...)` for sbatch.
    fn jobid(&self) -> Option<u64>;

    /// True while the job has not yet reached a terminal state. Inverse of
    /// [`is_finished`](Self::is_finished).
    fn is_running(&self) -> bool;

    /// True once the job has reached a terminal state.
    fn is_finished(&self) -> bool;

    /// Exit code if the job exited normally; `None` if killed by signal or
    /// if the finished record has not yet been populated.
    fn exit_code(&self) -> Option<i32>;

    // ─── snapshot accessors (lock-free) ───

    /// Clone of the current snapshot. Reads from the underlying
    /// `watch::Receiver` — no `await`, no lock contention.
    fn snapshot(&self) -> Self::Snapshot;

    /// Fresh `watch::Receiver` subscriber. Each clone is independently
    /// readable; uses standard `watch` semantics (no version skipping).
    fn watch(&self) -> watch::Receiver<Self::Snapshot>;

    // ─── async ───

    /// Re-query SLURM / the persistent store and broadcast the new snapshot.
    ///
    /// **MUST NOT** call `sacct` — that is the
    /// `SbatchJobHandle::refresh_with_sacct` heavy-weight finalizer's job.
    async fn refresh(&self) -> Result<Self::Snapshot>;

    /// Block (asynchronously) until the snapshot reaches a terminal state.
    /// Polls via [`refresh`](Self::refresh) every `poll_interval`. Returns
    /// the first refreshed snapshot that the backend considers terminal.
    async fn wait_terminal(&self, poll_interval: std::time::Duration) -> Result<Self::Snapshot>;
}

/// Object-safe companion to [`JobHandleCommon`].
///
/// Use this trait when you need a heterogenous collection of handles —
/// e.g. `Vec<Arc<dyn DynJobHandleCommon>>` mixing tssrun and sbatch jobs in
/// a single dashboard or attach UI. The associated `Snapshot` of
/// [`JobHandleCommon`] is flattened to [`serde_json::Value`] so dynamic
/// dispatch is possible.
///
/// Construct with [`into_dyn`] — there is intentionally **no blanket
/// `impl<H: JobHandleCommon> DynJobHandleCommon for H`**. Phase 1's
/// handover §4 documented that blanket impls combined with an associated
/// type trigger `E0034: multiple applicable items in scope`; the explicit
/// `into_dyn` constructor keeps the conversion site obvious and the trait
/// resolution unambiguous.
///
/// [`watch::Receiver`] is intentionally absent — type-erased subscribers
/// should poll via [`Self::snapshot_json`] / [`Self::refresh_json`].
#[async_trait::async_trait]
pub trait DynJobHandleCommon: Send + Sync + 'static {
    /// See [`JobHandleCommon::uuid`].
    fn uuid(&self) -> Uuid;
    /// See [`JobHandleCommon::jobid`].
    fn jobid(&self) -> Option<u64>;
    /// See [`JobHandleCommon::is_running`].
    fn is_running(&self) -> bool;
    /// See [`JobHandleCommon::is_finished`].
    fn is_finished(&self) -> bool;
    /// See [`JobHandleCommon::exit_code`].
    fn exit_code(&self) -> Option<i32>;

    /// Static discriminator string for the underlying snapshot type.
    /// Matches [`crate::store::JobSnapshot::kind`] — `"sbatch"` or `"tssrun"`.
    fn kind(&self) -> &'static str;

    /// Current snapshot, flattened to JSON. The structure matches
    /// `serde_json::to_value(handle.snapshot())` of the concrete
    /// `JobHandleCommon` impl.
    ///
    /// Returns `Err` if the snapshot fails to serialize — possible for
    /// third-party `JobHandleCommon` impls whose snapshot type has a
    /// fallible `Serialize` (the two in-tree snapshot types always
    /// serialize cleanly).
    fn snapshot_json(&self) -> Result<serde_json::Value>;

    /// JSON-flattened equivalent of [`JobHandleCommon::refresh`]. Same
    /// invariants apply (MUST NOT call `sacct`).
    async fn refresh_json(&self) -> Result<serde_json::Value>;
}

/// Adapter that erases an `H: JobHandleCommon` into a
/// `dyn DynJobHandleCommon`. Constructed via [`into_dyn`].
pub struct DynHandleAdapter<H: JobHandleCommon> {
    inner: H,
}

impl<H: JobHandleCommon> DynHandleAdapter<H> {
    /// Wrap `inner` in an adapter. Prefer the free function [`into_dyn`]
    /// when you want an `Arc<dyn DynJobHandleCommon>` directly.
    pub fn new(inner: H) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl<H: JobHandleCommon> DynJobHandleCommon for DynHandleAdapter<H> {
    fn uuid(&self) -> Uuid {
        self.inner.uuid()
    }
    fn jobid(&self) -> Option<u64> {
        self.inner.jobid()
    }
    fn is_running(&self) -> bool {
        self.inner.is_running()
    }
    fn is_finished(&self) -> bool {
        self.inner.is_finished()
    }
    fn exit_code(&self) -> Option<i32> {
        self.inner.exit_code()
    }
    fn kind(&self) -> &'static str {
        <H::Snapshot as JobSnapshot>::kind()
    }
    fn snapshot_json(&self) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(self.inner.snapshot())?)
    }
    async fn refresh_json(&self) -> Result<serde_json::Value> {
        let snap = self.inner.refresh().await?;
        Ok(serde_json::to_value(snap)?)
    }
}

/// Type-erase any `H: JobHandleCommon` into a shareable
/// `Arc<dyn DynJobHandleCommon>`.
///
/// Mirrors the Phase 1 `dispatcher::into_dyn` constructor pattern.
/// There is intentionally **no blanket impl** of `DynJobHandleCommon`
/// for `JobHandleCommon`; see the [`DynJobHandleCommon`] doc-comment.
pub fn into_dyn<H: JobHandleCommon>(h: H) -> Arc<dyn DynJobHandleCommon> {
    Arc::new(DynHandleAdapter::new(h))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compile-only assertion that `DynJobHandleCommon` is object-safe.
    /// A non-object-safe trait would fail to coerce `Box<T>` to
    /// `Box<dyn Trait>`, which this function exercises in argument position.
    #[allow(dead_code)]
    fn _dyn_safe(_h: &dyn DynJobHandleCommon) {}

    /// Compile-only assertion that `into_dyn` accepts any
    /// `JobHandleCommon` impl and yields an `Arc<dyn DynJobHandleCommon>`.
    #[allow(dead_code)]
    fn _into_dyn_signature<H: JobHandleCommon>(h: H) -> Arc<dyn DynJobHandleCommon> {
        into_dyn(h)
    }
}
