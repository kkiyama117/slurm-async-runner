#![cfg(feature = "pyo3")]

use pyo3::prelude::*;

pub mod runner;

pyo3_stub_gen::define_stub_info_gatherer!(stub_info);

/// A Python module implemented in Rust.
#[pymodule]
#[pyo3(name = "_core")]
mod slurm_async_runner {
    use super::*;
    // TODO: constcat const PYTHON_LIBRARY_NAME: &str = "slurm_async_runner";
    const PYTHON_MODULE_NAME: &str = "slurm_async_runner._core";

    // ---- legacy demo function ----
    #[pymodule_export]
    use crate::py_export::sum_as_string;

    // ---- async batch-query sub-module: slurm_async_runner._core.runner ----
    #[pymodule_export]
    use super::runner::inner_module as runner_module;

    #[pymodule_init]
    fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
        let py = m.py();
        py.import("sys")?
            .getattr("modules")?
            .set_item(PYTHON_MODULE_NAME, m)?;
        log::debug!("{} Rust module initialized", PYTHON_MODULE_NAME);
        Ok(())
    }
}

/// Formats the sum of two numbers as string.
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "slurm_async_runner._core")]
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}
