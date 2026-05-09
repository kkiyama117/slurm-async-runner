//! Python-facing wrappers for `crate::entities::slurm::sbatch_options::*`
//! (sbatch directive primitives plus the [`PySlurmJobConfig`] envelope).
//! Status types (`PyJobStatus`, etc.) live under the sibling
//! [`crate::py_export::entities::slurm::status`] module.

pub mod array_spec;
pub mod config;
pub mod dependency;
pub mod resource_spec;
pub mod time_limit;

use pyo3::prelude::*;

#[pymodule(name = "sbatch_options")]
pub(crate) mod inner_module {
    use super::*;

    const PYTHON_MODULE_NAME: &str =
        "gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options";

    #[pymodule_export]
    use super::dependency::{
        PyDependencyClause, PyDependencyJobRef, PyDependencyJoin, PyDependencyType,
        PySlurmDependency,
    };

    #[pymodule_export]
    use super::array_spec::{PyArrayIndex, PySlurmArraySpec};

    #[pymodule_export]
    use super::resource_spec::{
        PyMemory, PyMemoryUnit, PyResourceSpec, PyResourceSpecCPU, PyResourceSpecGPU,
    };

    #[pymodule_export]
    use super::time_limit::PyJobTimeLimit;

    #[pymodule_export]
    use super::config::{PyMailType, PyMailTypeInput, PySlurmJobConfig};

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
