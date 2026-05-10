//! sbatch — fire-and-forget SLURM batch submission with KUDPC-aware polling.
//!
//! See `docs/superpowers/specs/2026-05-10-sbatch-module-design.md` for
//! the full design rationale (Approach A: passive handle + watch).

pub mod cmd;
pub mod error;
pub mod handle;
pub mod manager;
pub mod parse;
pub mod store;
