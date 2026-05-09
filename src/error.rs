/// Strict schema validation utilities and exception family.
use thiserror::Error;

/// Toml loaders raise [`SchemaParseError`] on any
/// schema violation. Unknown keys, missing keys, type mismatches, and unknown
/// discriminator values all surface here.
#[derive(Debug, Clone, Error)]
pub enum SchemaParseError {
    /// A TOML mapping contained a key not in the allowed set.
    #[error("Unknown key(s): {0}")]
    UnknownKey(String),

    /// A TOML mapping was missing a required key.
    #[error("Missing requred key(s): {0}")]
    MissianRequiredKey(String),

    /// Parse error occurred
    #[error("{key} parse error around '{value}'")]
    ParseError { key: String, value: String },
}

#[derive(Debug, Clone, Error)]
pub enum SLURMJOBError {
    #[error("JobFailed")]
    JobFailed(String),
}
