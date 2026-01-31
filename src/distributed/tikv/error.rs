// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! TiKV-specific errors.

/// TiKV-specific error type.
#[derive(Debug, thiserror::Error)]
pub enum TikvError {
    /// Connection failed to TiKV cluster.
    #[error("TiKV connection failed: {0}")]
    ConnectionFailed(String),

    /// Transaction aborted.
    #[error("Transaction aborted: {0}")]
    TransactionAborted(String),

    /// Key not found.
    #[error("Key not found: {0}")]
    KeyNotFound(String),

    /// Serialization error.
    #[error("Serialization failed: {0}")]
    Serialization(String),

    /// Deserialization error.
    #[error("Deserialization failed: {0}")]
    Deserialization(String),

    /// CAS (Compare-And-Swap) operation failed.
    #[error("CAS operation failed: expected version {expected}, got {got}")]
    CasFailed { expected: u64, got: u64 },

    /// Lock acquisition failed.
    #[error("Failed to acquire lock: {0}")]
    LockAcquisitionFailed(String),

    /// Invalid configuration.
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Timeout.
    #[error("Operation timed out: {0}")]
    Timeout(String),

    /// Wrapped TiKV client error.
    #[error("TiKV client error: {0}")]
    ClientError(String),

    /// Retryable error with context.
    #[error("Retryable error (attempt {attempt}/{max}): {message}")]
    Retryable {
        attempt: u32,
        max: u32,
        message: String,
    },

    /// Generic error with context.
    #[error("{0}")]
    Other(String),
}

impl TikvError {
    /// Check if this error is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Retryable { .. } | Self::Timeout(_) | Self::ConnectionFailed(_)
        )
    }

    /// Create a retryable error.
    pub fn retryable(attempt: u32, max: u32, message: impl Into<String>) -> Self {
        Self::Retryable {
            attempt,
            max,
            message: message.into(),
        }
    }
}

/// Result type for TiKV operations.
pub type Result<T> = std::result::Result<T, TikvError>;

/// Convert TikvError to RoboflowError.
impl From<TikvError> for crate::RoboflowError {
    fn from(err: TikvError) -> Self {
        crate::RoboflowError::other(format!("TiKV error: {}", err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_is_retryable() {
        assert!(TikvError::Timeout("test".to_string()).is_retryable());
        assert!(TikvError::ConnectionFailed("test".to_string()).is_retryable());
        assert!(!TikvError::KeyNotFound("test".to_string()).is_retryable());
        assert!(
            !TikvError::CasFailed {
                expected: 1,
                got: 2
            }
            .is_retryable()
        );
    }
}
