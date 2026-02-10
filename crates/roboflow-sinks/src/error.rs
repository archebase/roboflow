// Error types for sinks

use std::path::PathBuf;
use thiserror::Error;

/// Result type for sink operations.
pub type SinkResult<T> = Result<T, SinkError>;

/// Errors that can occur when working with sinks.
#[derive(Error, Debug)]
pub enum SinkError {
    /// I/O error occurred
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The sink format is not supported
    #[error("Unsupported sink format: {0}")]
    UnsupportedFormat(String),

    /// Failed to create the sink
    #[error("Failed to create sink: {path}: {error}")]
    CreateFailed {
        /// Path that failed to create
        path: PathBuf,
        /// Underlying error
        #[source]
        error: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Failed to write to the sink
    #[error("Failed to write: {0}")]
    WriteFailed(String),

    /// Failed to encode data
    #[error("Failed to encode data: {0}")]
    EncodeFailed(String),

    /// The sink does not support checkpointing
    #[error("Checkpoint operation not supported for this sink")]
    CheckpointNotSupported,

    /// The sink does not support restore
    #[error("Restore operation not supported for this sink")]
    RestoreNotSupported,

    /// The sink does not support cloning
    #[error("Clone operation not supported for this sink")]
    CloneNotSupported,

    /// Invalid configuration
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Storage error
    #[error("Storage error: {0}")]
    Storage(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = SinkError::WriteFailed("test error".to_string());
        assert!(err.to_string().contains("test error"));

        let err = SinkError::CheckpointNotSupported;
        assert!(err.to_string().contains("not supported"));
    }
}
