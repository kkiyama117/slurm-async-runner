#![cfg(feature="pyo3")]

use pyo3::prelude::*;

pyo3_stub_gen::define_stub_info_gatherer!(stub_info);


/// A Python module implemented in Rust.
#[pymodule]
#[pyo3(name="_core")]
mod slurm_async_runner2 {
    use super::*;
    // TODO: constcat const PYTHON_LIBRARY_NAME: &str = "slurm_async_runner2";
    const PYTHON_MODULE_NAME: &str = "slurm_async_runner2._core";
        #[pymodule_init]
        fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
            let py = m.py();
            py.import("sys")?
                .getattr("modules")?
                .set_item(PYTHON_MODULE_NAME, m)?;
            log::debug!("{} Rust module initialized", PYTHON_MODULE_NAME);
            Ok(())
        }

    /// Formats the sum of two numbers as string.
    #[pyfunction]
    fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
        Ok((a + b).to_string())
    }
}
