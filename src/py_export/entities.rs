//! Pyclass wrappers for SAR's domain entity types. Mirrors the
//! Rust-side `crate::entities` tree.

pub mod slurm;

use pyo3::prelude::*;

#[pymodule(name = "entities")]
pub(crate) mod inner_module {
    use super::*;

    const PYTHON_MODULE_NAME: &str = "slurm_async_runner._slurm_async_runner_core.entities";

    #[pymodule_export]
    use super::slurm::inner_module as slurm_module;

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
