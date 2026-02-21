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

    #[test]
    fn test_unsupported_format_error() {
        let err = SourceError::UnsupportedFormat("xyz".to_string());
        assert!(err.to_string().contains("Unsupported source format"));
        assert!(err.to_string().contains("xyz"));
    }

    #[test]
    fn test_invalid_config_error() {
        let err = SourceError::InvalidConfig("missing path".to_string());
        assert!(err.to_string().contains("Invalid configuration"));
        assert!(err.to_string().contains("missing path"));
    }

    #[test]
    fn test_topic_not_found_error() {
        let err = SourceError::TopicNotFound("/camera/left".to_string());
        assert!(err.to_string().contains("/camera/left"));
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_decode_failed_error() {
        let err = SourceError::DecodeFailed("invalid CDR".to_string());
        assert!(err.to_string().contains("Failed to decode"));
        assert!(err.to_string().contains("invalid CDR"));
    }

    #[test]
    fn test_storage_error() {
        let err = SourceError::Storage("S3 access denied".to_string());
        assert!(err.to_string().contains("Storage error"));
        assert!(err.to_string().contains("S3 access denied"));
    }

    #[test]
    fn test_end_of_stream_error() {
        let err = SourceError::EndOfStream;
        assert!(err.to_string().contains("End of stream"));
    }

    #[test]
    fn test_clone_not_supported_error() {
        let err = SourceError::CloneNotSupported;
        assert!(err.to_string().contains("Clone operation not supported"));
    }
}
