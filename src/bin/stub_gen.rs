//! pyo3-stub-gen entry point. Re-exposes `slurm_async_runner2::stub_info`
//! (defined in `src/py_export.rs` via `define_stub_info_gatherer!`).
//! Only built when the `stub_gen` feature is on.

fn main() -> pyo3_stub_gen::Result<()> {
    let stub = slurm_async_runner2::stub_info()?;
    stub.generate()?;
    Ok(())
}
