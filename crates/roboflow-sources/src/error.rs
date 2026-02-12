// Error types for sources

use std::path::PathBuf;
use thiserror::Error;

/// Result type for source operations.
pub type SourceResult<T> = Result<T, SourceError>;

/// Errors that can occur when working with sources.
#[derive(Error, Debug)]
pub enum SourceError {
    /// I/O error occurred
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The source format is not supported
    #[error("Unsupported source format: {0}")]
    UnsupportedFormat(String),

    /// Failed to open the source
    #[error("Failed to open source: {path}")]
    OpenFailed {
        /// Path that failed to open
        path: PathBuf,
        /// Underlying error
        #[source]
        error: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Failed to read from the source
    #[error("Failed to read from source: {0}")]
    ReadFailed(String),

    /// Failed to decode a message
    #[error("Failed to decode message: {0}")]
    DecodeFailed(String),

    /// The source does not support seeking
    #[error("Seek operation not supported for this source")]
    SeekNotSupported,

    /// The source does not support cloning
    #[error("Clone operation not supported for this source")]
    CloneNotSupported,

    /// Invalid configuration
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Required topic not found in source
    #[error("Required topic '{0}' not found in source")]
    TopicNotFound(String),

    /// End of stream reached
    #[error("End of stream reached")]
    EndOfStream,

    /// Storage error
    #[error("Storage error: {0}")]
    Storage(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = SourceError::ReadFailed("test error".to_string());
        assert!(err.to_string().contains("test error"));

        let err = SourceError::SeekNotSupported;
        assert!(err.to_string().contains("not supported"));
    }
}
