//! Domain entity types for slurm-async-runner.
//!
//! This module is the canonical home for slurm vocabulary
//! (`ResourceSpec`, `JobTimeLimit`, `Memory`, `SlurmJobConfig`,
//! `JobStatus`, etc.). Downstream crates that need to consume
//! these types at the Rust level should depend on this crate
//! with `default-features = false` to avoid linking SAR's pyclass
//! impls into their own cdylib (see the Pyclass Single Owner rule
//! in `Cargo.toml`).

pub mod slurm;
