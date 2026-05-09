//! Non-blocking wrapper around the Kyoto-U ECCS `tssrun` interactive-batch
//! frontend (a `salloc`+`srun` combo).
//!
//! # What this module exposes
//!
//! - [`cmd::TssrunCmd`] — pure-data spec for one `tssrun` invocation
//!   plus an `argv` builder. No I/O. The `--rsc` spec is the shared
//!   `gaussian_job_shared::entities::slurm::ResourceSpec` (CPU / GPU
//!   enum); the wall-clock limit is the shared `JobTimeLimit`.
//! - [`manager::TssrunManager`] — orchestrates `spawn` / `attach` / state
//!   queries with a shared log sink and (optional) on-disk state directory.
//! - [`handle::JobHandle`] — owns the spawned child plus background tee
//!   tasks that parse `salloc:` lines into a [`watch::Receiver`]-backed
//!   snapshot. The snapshot is also persisted as JSON via atomic rename.
//! - [`log::JobLogSink`] — pluggable trait for tee'd stdout/stderr lines.
//!   Built-in implementations: [`log::NullLogSink`], [`log::StdLogSink`],
//!   [`log::InMemoryLogSink`], [`log::FileLogSink`].
//! - [`parse::parse_salloc_jobid`] / [`parse::parse_salloc_node`] — pure
//!   line parsers exposed for downstream reuse and direct testing.
//!
//! # Concurrency model
//!
//! - [`handle::JobHandle::watch`] returns a clonable
//!   [`watch::Receiver`] over the snapshot. Snapshot reads
//!   are lock-free against [`handle::JobHandle::wait`], which is the only
//!   `&mut self` method (it `.take()`s the join handles exactly once).
//! - The pyo3 wrapper exploits this so Python users can poll
//!   `is_running` / `pid` / `jobid` while `await handle.wait()` is in
//!   flight without serializing on a single mutex.
//!
//! # Exit-status semantics
//!
//! [`handle::JobHandle::wait`] returns `Result<Option<i32>>`:
//!
//! - `Ok(Some(n))` — the child exited normally with code `n`.
//! - `Ok(None)` — the child was terminated by a signal (e.g. SLURM
//!   time-limit kill, OOM). The persisted snapshot's `finished.exit_code`
//!   is also `None`.
//! - `Err(_)` — `wait` itself failed, or the handle is attached / already
//!   consumed.
//!
//! [`watch::Receiver`]: tokio::sync::watch::Receiver
//!
//! See `docs/superpowers/specs/2026-05-09-tssrun-wrapper-env-design.md`
//! for the full design rationale.

pub mod cmd;
pub mod handle;
pub mod log;
pub mod manager;
pub mod parse;
pub mod store;
