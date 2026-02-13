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
    CasFailed {
        /// Expected version number.
        expected: u64,
        /// Actual version number found.
        got: u64,
    },

    /// Lock acquisition failed.
    #[error("Failed to acquire lock: {0}")]
    LockAcquisitionFailed(String),

    /// Invalid configuration.
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Timeout.
    #[error("Operation timed out: {0}")]
    Timeout(String),

    /// Write conflict detected (should be retried).
    #[error("Write conflict detected - operation should be retried")]
    WriteConflict,

    /// Wrapped TiKV client error.
    #[error("TiKV client error: {0}")]
    ClientError(String),

    /// Retryable error with context.
    #[error("Retryable error (attempt {attempt}/{max}): {message}")]
    Retryable {
        /// Current retry attempt number.
        attempt: u32,
        /// Maximum retry attempts allowed.
        max: u32,
        /// Error message describing the failure.
        message: String,
    },

    /// Generic error with context.
    #[error("{0}")]
    Other(String),

    /// Circuit breaker is open - requests are failing fast.
    #[error("Circuit breaker is open (failures: {failures}) - rejecting call")]
    CircuitOpen {
        /// Number of consecutive failures that triggered the circuit.
        failures: u32,
    },
}

impl TikvError {
    /// Check if this error is retryable.
    ///
    /// Write conflicts, timeouts, connection failures, and transaction aborts
    /// are all considered retryable. Lock acquisition failures can be retried
    /// with backoff as the lock may become available.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::WriteConflict => true,
            Self::Retryable { .. } => true,
            Self::Timeout(_) => true,
            Self::ConnectionFailed(_) => true,
            Self::TransactionAborted(msg) => {
                // Check if it's a write conflict (retryable)
                msg.contains("WriteConflict")
                    || msg.contains("Write Conflict")
                    || msg.contains("key_version")
            }
            Self::ClientError(msg) => {
                // Check if it's a write conflict or pessimistic lock conflict (retryable)
                msg.contains("WriteConflict")
                    || msg.contains("Write Conflict")
                    || msg.contains("key_version")
                    || msg.contains("PessimisticLock")
                    || msg.contains("PessimisticRetry")
            }
            Self::LockAcquisitionFailed(_) => true,
            _ => false,
        }
    }

    /// Check if this error is a write conflict.
    pub fn is_write_conflict(&self) -> bool {
        if matches!(self, Self::WriteConflict) {
            return true;
        }
        if let Self::ClientError(msg) | Self::TransactionAborted(msg) = self {
            msg.contains("WriteConflict") || msg.contains("key_version")
        } else {
            false
        }
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
impl From<TikvError> for roboflow_core::RoboflowError {
    fn from(err: TikvError) -> Self {
        roboflow_core::RoboflowError::other(format!("TiKV error: {}", err))
    }
}

/// Implement IsRetryableRef for TikvError so it can be used with retry_with_backoff.
impl roboflow_core::retry::IsRetryableRef for TikvError {
    fn is_retryable_ref(&self) -> bool {
        self.is_retryable()
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
        // New tests
        assert!(TikvError::WriteConflict.is_retryable());
        assert!(TikvError::LockAcquisitionFailed("test".to_string()).is_retryable());
        assert!(TikvError::ClientError("WriteConflict".to_string()).is_retryable());
        assert!(TikvError::ClientError("key_version error".to_string()).is_retryable());
        assert!(TikvError::TransactionAborted("WriteConflict".to_string()).is_retryable());
        assert!(!TikvError::ClientError("other error".to_string()).is_retryable());
    }

    #[test]
    fn test_error_is_write_conflict() {
        assert!(TikvError::WriteConflict.is_write_conflict());
        assert!(TikvError::ClientError("WriteConflict detected".to_string()).is_write_conflict());
        assert!(TikvError::ClientError("key_version mismatch".to_string()).is_write_conflict());
        assert!(TikvError::TransactionAborted("WriteConflict".to_string()).is_write_conflict());
        assert!(!TikvError::Timeout("test".to_string()).is_write_conflict());
        assert!(!TikvError::ConnectionFailed("test".to_string()).is_write_conflict());
    }

    #[test]
    fn test_retryable_constructor() {
        let err = TikvError::retryable(1, 3, "test message");
        assert!(matches!(
            err,
            TikvError::Retryable {
                attempt: 1,
                max: 3,
                ..
            }
        ));
        assert!(err.is_retryable());
    }

    #[test]
    fn test_error_display() {
        let err = TikvError::ConnectionFailed("localhost:2379".to_string());
        assert!(err.to_string().contains("connection failed"));

        let err = TikvError::CasFailed {
            expected: 1,
            got: 2,
        };
        assert!(err.to_string().contains("CAS"));

        let err = TikvError::CircuitOpen { failures: 5 };
        assert!(err.to_string().contains("Circuit breaker"));
    }

    #[test]
    fn test_other_error() {
        let err = TikvError::Other("custom error".to_string());
        assert!(!err.is_retryable());
        assert!(!err.is_write_conflict());
    }

    #[test]
    fn test_serialization_errors() {
        let err = TikvError::Serialization("failed".to_string());
        assert!(!err.is_retryable());

        let err = TikvError::Deserialization("failed".to_string());
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_invalid_config_error() {
        let err = TikvError::InvalidConfig("missing pd address".to_string());
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_pessimistic_lock_retryable() {
        assert!(TikvError::ClientError("PessimisticLock".to_string()).is_retryable());
        assert!(TikvError::ClientError("PessimisticRetry needed".to_string()).is_retryable());
    }
}
