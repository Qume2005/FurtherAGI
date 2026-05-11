//! Configuration build errors.

use crate::workflow::error::WorkflowError;
use thiserror::Error;

/// Errors that can occur while building a workflow DAG from configuration.
#[derive(Error, Debug)]
pub enum ConfigBuildError {
    /// TOML syntax or schema error.
    #[error("TOML parse error: {0}")]
    ParseError(#[from] toml::de::Error),

    /// File I/O error.
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// A type name referenced in config was never registered in the [`TypeRegistry`](super::TypeRegistry).
    #[error("unknown type '{name}' referenced in node '{node}'")]
    UnknownType {
        node: String,
        name: String,
    },

    /// A workflow implementation name was never registered in the [`WorkflowFactoryRegistry`](super::WorkflowFactoryRegistry).
    #[error("unknown workflow '{name}' referenced in node '{node}'")]
    UnknownWorkflow {
        node: String,
        name: String,
    },

    /// A node name referenced in edges, loop body, or error handler was not found.
    #[error("unknown node '{0}'")]
    UnknownNode(String),

    /// An error from the underlying DAG builder (type mismatch, cycle, etc.).
    #[error("DAG error: {0}")]
    DagError(#[from] WorkflowError),
}
