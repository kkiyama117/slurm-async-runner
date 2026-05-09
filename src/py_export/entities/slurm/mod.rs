//! Python-facing wrappers for `crate::entities::slurm::*`.
//!
//! Two sub-modules are exposed at `gaussian_job_shared._gaussian_job_shared_core.entities.slurm`:
//!
//! - [`sbatch_options`] — sbatch directive primitives and the
//!   [`sbatch_options::config::PySlurmJobConfig`] envelope. Available
//!   at `gaussian_job_shared._gaussian_job_shared_core.entities.slurm.sbatch_options`.
//! - [`status`] — runtime job status (`PyJobStatus`, `PyJobState`,
//!   `PyJobReason`). Available at
//!   `gaussian_job_shared._gaussian_job_shared_core.entities.slurm.status`.

pub mod sbatch_options;
pub mod status;

use pyo3::prelude::*;

#[pymodule(name = "slurm")]
pub(crate) mod inner_module {
    use super::*;

    const PYTHON_MODULE_NAME: &str = "gaussian_job_shared._gaussian_job_shared_core.entities.slurm";

    #[pymodule_export]
    use super::sbatch_options::inner_module as sbatch_options_module;

    #[pymodule_export]
    use super::status::inner_module as status_module;

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
