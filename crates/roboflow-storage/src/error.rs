// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Storage error types.
//!
//! This module provides unified error handling for all storage backends.

/// Unified error type for all storage operations.
///
/// This error type encompasses failures across all storage backends,
/// providing consistent error handling regardless of the underlying storage.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// The requested object does not exist.
    #[error("object not found: {0}")]
    NotFound(String),

    /// Permission denied for the requested operation.
    #[error("permission denied: {0}")]
    PermissionDenied(String),

    /// An object with the same name already exists (for create-exclusive operations).
    #[error("object already exists: {0}")]
    AlreadyExists(String),

    /// The provided path or URL is invalid.
    #[error("invalid path: {0}")]
    InvalidPath(String),

    /// A network error occurred during a cloud storage operation.
    #[error("network error: {0}")]
    NetworkError(String),

    /// The operation timed out.
    #[error("operation timed out: {0}")]
    Timeout(String),

    /// An underlying I/O error occurred.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// An error occurred during cloud storage operations.
    #[error("cloud storage error: {0}")]
    Cloud(String),

    /// A generic error with a message.
    #[error("{0}")]
    Other(String),
}

impl StorageError {
    /// Create a not found error for the given path.
    pub fn not_found(path: impl Into<String>) -> Self {
        Self::NotFound(path.into())
    }

    /// Create a permission denied error for the given path.
    pub fn permission_denied(path: impl Into<String>) -> Self {
        Self::PermissionDenied(path.into())
    }

    /// Create an invalid path error.
    pub fn invalid_path(path: impl Into<String>) -> Self {
        Self::InvalidPath(path.into())
    }

    /// Create a timeout error.
    pub fn timeout(operation: impl Into<String>) -> Self {
        Self::Timeout(operation.into())
    }

    /// Create a network error.
    pub fn network(msg: impl Into<String>) -> Self {
        Self::NetworkError(msg.into())
    }

    /// Create an other error with a message.
    pub fn other(msg: impl Into<String>) -> Self {
        Self::Other(msg.into())
    }

    /// Check if this error is retryable.
    ///
    /// Retryable errors include timeouts, network errors, and some cloud errors.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::NetworkError(_) | Self::Timeout(_) | Self::Cloud(_)
        )
    }
}

/// Result type for storage operations.
pub type StorageResult<T> = std::result::Result<T, StorageError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_error_is_retryable() {
        assert!(StorageError::network("test").is_retryable());
        assert!(StorageError::timeout("test").is_retryable());
        assert!(StorageError::Cloud("test".to_string()).is_retryable());
        assert!(!StorageError::NotFound("test".to_string()).is_retryable());
        assert!(!StorageError::PermissionDenied("test".to_string()).is_retryable());
        assert!(!StorageError::InvalidPath("test".to_string()).is_retryable());
    }
}
