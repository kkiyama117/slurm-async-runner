use pyo3::{
    PyErr,
    exceptions::{PyRuntimeError, PyValueError},
};

use crate::error::{SLURMJOBError, SchemaParseError};

impl From<SchemaParseError> for PyErr {
    fn from(value: SchemaParseError) -> Self {
        PyValueError::new_err(value.to_string())
    }
}

impl From<SLURMJOBError> for PyErr {
    fn from(value: SLURMJOBError) -> Self {
        PyRuntimeError::new_err(value.to_string())
    }
}
