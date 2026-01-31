// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Core error types for roboflow.
//!
//! Provides a unified error type that all codec crates can use.
//!
//! # Error Categories
//!
//! Errors are categorized by their source:
//! - **Parse**: Schema or data parsing failures (error codes: 1xxx)
//! - **Schema**: Invalid schema definitions (error codes: 2xxx)
//! - **Runtime**: Buffer and decoding issues (error codes: 3xxx)
//! - **Codec**: Encoding/codec-specific errors (error codes: 4xxx)
//! - **Transform**: Message transformation errors (error codes: 5xxx)

use std::fmt;

/// Error category for grouping related errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    /// Schema or data parsing failures
    Parse,
    /// Invalid schema definitions
    Schema,
    /// Runtime buffer/decoding issues
    Runtime,
    /// Encoding/codec-specific errors
    Codec,
    /// Message transformation errors
    Transform,
}

impl ErrorCategory {
    /// Get the numeric prefix for this category's error codes.
    pub const fn code_prefix(&self) -> u16 {
        match self {
            ErrorCategory::Parse => 1000,
            ErrorCategory::Schema => 2000,
            ErrorCategory::Runtime => 3000,
            ErrorCategory::Codec => 4000,
            ErrorCategory::Transform => 5000,
        }
    }

    /// Get the category name for logging.
    pub const fn as_str(&self) -> &'static str {
        match self {
            ErrorCategory::Parse => "PARSE",
            ErrorCategory::Schema => "SCHEMA",
            ErrorCategory::Runtime => "RUNTIME",
            ErrorCategory::Codec => "CODEC",
            ErrorCategory::Transform => "TRANSFORM",
        }
    }
}

/// Errors that can occur during roboflow operations.
#[derive(Debug)]
pub enum RoboflowError {
    /// Parse error in schema or data
    ParseError {
        /// What was being parsed
        context: String,
        /// Error message
        message: String,
    },

    /// Invalid schema format
    InvalidSchema {
        /// Schema name or identifier
        schema_name: String,
        /// Validation error message
        reason: String,
    },

    /// Type not found in registry
    TypeNotFound {
        /// Type name that was not found
        type_name: String,
    },

    /// Buffer too short for requested read
    BufferTooShort {
        /// Requested bytes
        requested: usize,
        /// Available bytes
        available: usize,
        /// Cursor position when error occurred
        cursor_pos: u64,
    },

    /// Invalid alignment
    AlignmentError {
        /// Expected alignment
        expected: u64,
        /// Actual position
        actual: u64,
    },

    /// Array or sequence length exceeded data bounds
    LengthExceeded {
        /// Length that was read
        length: usize,
        /// Position in buffer
        position: usize,
        /// Buffer length
        buffer_len: usize,
    },

    /// Field decode error with context
    FieldDecodeError {
        /// Field name
        field_name: String,
        /// Field type
        field_type: String,
        /// Cursor position when error occurred
        cursor_pos: u64,
        /// Underlying error
        cause: String,
    },

    /// Unsupported type or feature
    Unsupported {
        /// What is not supported
        feature: String,
    },

    /// Encoding error
    EncodeError {
        /// Codec context (e.g., "CDR", "Protobuf")
        codec: String,
        /// Error message
        message: String,
    },

    /// Transformation error (topic/type renaming, schema rewrite)
    TransformError {
        /// Transformation type
        transform_type: String,
        /// Error message
        message: String,
    },

    /// Format I/O error (wrapped from robocodec)
    CodecError {
        /// Error message from robocodec
        message: String,
    },

    /// Invariant violation (for unsafe block validation failures)
    InvariantViolation {
        /// Description of the invariant that was violated
        invariant: String,
    },

    /// Other error
    Other(String),

    /// Timeout error
    Timeout(String),

    /// Storage error (wrapped from storage layer)
    #[cfg(feature = "cloud-storage")]
    Storage(crate::storage::StorageError),
}

impl RoboflowError {
    /// Create a parse error.
    pub fn parse(context: impl Into<String>, message: impl Into<String>) -> Self {
        RoboflowError::ParseError {
            context: context.into(),
            message: message.into(),
        }
    }

    /// Create an invalid schema error.
    pub fn invalid_schema(schema_name: impl Into<String>, reason: impl Into<String>) -> Self {
        RoboflowError::InvalidSchema {
            schema_name: schema_name.into(),
            reason: reason.into(),
        }
    }

    /// Create a type not found error.
    pub fn type_not_found(type_name: impl Into<String>) -> Self {
        RoboflowError::TypeNotFound {
            type_name: type_name.into(),
        }
    }

    /// Create a buffer too short error.
    pub fn buffer_too_short(requested: usize, available: usize, cursor_pos: u64) -> Self {
        RoboflowError::BufferTooShort {
            requested,
            available,
            cursor_pos,
        }
    }

    /// Create an unsupported error.
    pub fn unsupported(feature: impl Into<String>) -> Self {
        RoboflowError::Unsupported {
            feature: feature.into(),
        }
    }

    /// Create an encode error.
    pub fn encode(codec: impl Into<String>, message: impl Into<String>) -> Self {
        RoboflowError::EncodeError {
            codec: codec.into(),
            message: message.into(),
        }
    }

    /// Create a transform error.
    pub fn transform(transform_type: impl Into<String>, message: impl Into<String>) -> Self {
        RoboflowError::TransformError {
            transform_type: transform_type.into(),
            message: message.into(),
        }
    }

    /// Create an invariant violation error (for unsafe block validation).
    pub fn invariant_violation(invariant: impl Into<String>) -> Self {
        RoboflowError::InvariantViolation {
            invariant: invariant.into(),
        }
    }

    /// Create a timeout error.
    pub fn timeout(message: impl Into<String>) -> Self {
        RoboflowError::Timeout(message.into())
    }

    /// Create an I/O error.
    pub fn io(message: impl Into<String>) -> Self {
        RoboflowError::Other(message.into())
    }

    /// Create a storage error.
    #[cfg(feature = "cloud-storage")]
    pub fn storage(err: crate::storage::StorageError) -> Self {
        RoboflowError::Storage(err)
    }

    /// Create an other error.
    pub fn other(message: impl Into<String>) -> Self {
        RoboflowError::Other(message.into())
    }

    /// Check if this error is retryable.
    ///
    /// Retryable errors include timeouts, network errors, and transient cloud storage errors.
    pub fn is_retryable(&self) -> bool {
        match self {
            RoboflowError::Timeout(_) => true,
            #[cfg(feature = "cloud-storage")]
            RoboflowError::Storage(e) => e.is_retryable(),
            _ => false,
        }
    }

    /// Get the error category for this error.
    pub fn category(&self) -> ErrorCategory {
        match self {
            RoboflowError::ParseError { .. } => ErrorCategory::Parse,
            RoboflowError::InvalidSchema { .. } => ErrorCategory::Schema,
            RoboflowError::TypeNotFound { .. } => ErrorCategory::Schema,
            RoboflowError::BufferTooShort { .. } => ErrorCategory::Runtime,
            RoboflowError::AlignmentError { .. } => ErrorCategory::Runtime,
            RoboflowError::LengthExceeded { .. } => ErrorCategory::Runtime,
            RoboflowError::FieldDecodeError { .. } => ErrorCategory::Runtime,
            RoboflowError::Unsupported { .. } => ErrorCategory::Codec,
            RoboflowError::EncodeError { .. } => ErrorCategory::Codec,
            RoboflowError::TransformError { .. } => ErrorCategory::Transform,
            RoboflowError::CodecError { .. } => ErrorCategory::Runtime,
            RoboflowError::InvariantViolation { .. } => ErrorCategory::Runtime,
            RoboflowError::Other(_) => ErrorCategory::Runtime,
            RoboflowError::Timeout(_) => ErrorCategory::Runtime,
            #[cfg(feature = "cloud-storage")]
            RoboflowError::Storage(_) => ErrorCategory::Runtime,
        }
    }

    /// Get the error code for this error.
    pub fn code(&self) -> u16 {
        let base = self.category().code_prefix();
        match self {
            RoboflowError::ParseError { .. } => base + 1,
            RoboflowError::InvalidSchema { .. } => base + 1,
            RoboflowError::TypeNotFound { .. } => base + 2,
            RoboflowError::BufferTooShort { .. } => base + 1,
            RoboflowError::AlignmentError { .. } => base + 2,
            RoboflowError::LengthExceeded { .. } => base + 3,
            RoboflowError::FieldDecodeError { .. } => base + 4,
            RoboflowError::Unsupported { .. } => base + 1,
            RoboflowError::EncodeError { .. } => base + 2,
            RoboflowError::TransformError { .. } => base + 1,
            RoboflowError::CodecError { .. } => base + 6,
            RoboflowError::InvariantViolation { .. } => base + 5,
            RoboflowError::Other(_) => base + 99,
            RoboflowError::Timeout(_) => base + 98,
            #[cfg(feature = "cloud-storage")]
            RoboflowError::Storage(_) => base + 97,
        }
    }

    /// Get structured fields for logging.
    pub fn log_fields(&self) -> Vec<(&'static str, String)> {
        match self {
            RoboflowError::ParseError { context, message } => {
                vec![("context", context.clone()), ("message", message.clone())]
            }
            RoboflowError::InvalidSchema {
                schema_name,
                reason,
            } => vec![("schema", schema_name.clone()), ("reason", reason.clone())],
            RoboflowError::TypeNotFound { type_name } => vec![("type", type_name.clone())],
            RoboflowError::BufferTooShort {
                requested,
                available,
                cursor_pos,
            } => vec![
                ("requested", requested.to_string()),
                ("available", available.to_string()),
                ("cursor", cursor_pos.to_string()),
            ],
            RoboflowError::AlignmentError { expected, actual } => vec![
                ("expected", expected.to_string()),
                ("actual", actual.to_string()),
            ],
            RoboflowError::LengthExceeded {
                length,
                position,
                buffer_len,
            } => vec![
                ("length", length.to_string()),
                ("position", position.to_string()),
                ("buffer_len", buffer_len.to_string()),
            ],
            RoboflowError::FieldDecodeError {
                field_name,
                field_type,
                cursor_pos,
                cause,
            } => vec![
                ("field", field_name.clone()),
                ("type", field_type.clone()),
                ("cursor", cursor_pos.to_string()),
                ("cause", cause.clone()),
            ],
            RoboflowError::Unsupported { feature } => vec![("feature", feature.clone())],
            RoboflowError::EncodeError { codec, message } => {
                vec![("codec", codec.clone()), ("message", message.clone())]
            }
            RoboflowError::TransformError {
                transform_type,
                message,
            } => vec![
                ("transform", transform_type.clone()),
                ("message", message.clone()),
            ],
            RoboflowError::CodecError { message } => vec![("error", message.clone())],
            RoboflowError::InvariantViolation { invariant } => {
                vec![("invariant", invariant.clone())]
            }
            RoboflowError::Other(msg) => vec![("message", msg.clone())],
            RoboflowError::Timeout(msg) => vec![("timeout", msg.clone())],
            #[cfg(feature = "cloud-storage")]
            RoboflowError::Storage(err) => vec![("storage", err.to_string())],
        }
    }
}

impl fmt::Display for RoboflowError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let category = self.category();
        let code = self.code();
        write!(f, "[{}-{:04}] ", category.as_str(), code)?;
        match self {
            RoboflowError::ParseError { context, message } => {
                write!(f, "Parse error in '{context}': {message}")
            }
            RoboflowError::InvalidSchema {
                schema_name,
                reason,
            } => {
                write!(f, "Invalid schema '{schema_name}': {reason}")
            }
            RoboflowError::TypeNotFound { type_name } => {
                write!(f, "Type not found: '{type_name}'")
            }
            RoboflowError::BufferTooShort {
                requested,
                available,
                cursor_pos,
            } => write!(
                f,
                "Buffer too short: requested {requested} bytes at position {cursor_pos}, but only {available} bytes available"
            ),
            RoboflowError::AlignmentError { expected, actual } => write!(
                f,
                "Alignment error: expected alignment of {expected}, but position is {actual}"
            ),
            RoboflowError::LengthExceeded {
                length,
                position,
                buffer_len,
            } => write!(
                f,
                "Length {length} exceeds buffer at position {position} (buffer length: {buffer_len})"
            ),
            RoboflowError::FieldDecodeError {
                field_name,
                field_type,
                cursor_pos,
                cause,
            } => write!(
                f,
                "Failed to decode field '{field_name}' (type: '{field_type}', cursor_pos: {cursor_pos}): {cause}"
            ),
            RoboflowError::Unsupported { feature } => {
                write!(f, "Unsupported feature: '{feature}'")
            }
            RoboflowError::EncodeError { codec, message } => {
                write!(f, "{codec} encode error: {message}")
            }
            RoboflowError::TransformError {
                transform_type,
                message,
            } => {
                write!(f, "Transform error ({transform_type}): {message}")
            }
            RoboflowError::CodecError { message } => {
                write!(f, "Format I/O error: {message}")
            }
            RoboflowError::InvariantViolation { invariant } => {
                write!(f, "Invariant violation: {invariant}")
            }
            RoboflowError::Other(msg) => write!(f, "{msg}"),
            RoboflowError::Timeout(msg) => write!(f, "Timeout: {msg}"),
            #[cfg(feature = "cloud-storage")]
            RoboflowError::Storage(err) => write!(f, "Storage error: {}", err),
        }
    }
}

impl std::error::Error for RoboflowError {}

impl Clone for RoboflowError {
    fn clone(&self) -> Self {
        match self {
            RoboflowError::ParseError { context, message } => RoboflowError::ParseError {
                context: context.clone(),
                message: message.clone(),
            },
            RoboflowError::InvalidSchema {
                schema_name,
                reason,
            } => RoboflowError::InvalidSchema {
                schema_name: schema_name.clone(),
                reason: reason.clone(),
            },
            RoboflowError::TypeNotFound { type_name } => RoboflowError::TypeNotFound {
                type_name: type_name.clone(),
            },
            RoboflowError::BufferTooShort {
                requested,
                available,
                cursor_pos,
            } => RoboflowError::BufferTooShort {
                requested: *requested,
                available: *available,
                cursor_pos: *cursor_pos,
            },
            RoboflowError::AlignmentError { expected, actual } => RoboflowError::AlignmentError {
                expected: *expected,
                actual: *actual,
            },
            RoboflowError::LengthExceeded {
                length,
                position,
                buffer_len,
            } => RoboflowError::LengthExceeded {
                length: *length,
                position: *position,
                buffer_len: *buffer_len,
            },
            RoboflowError::FieldDecodeError {
                field_name,
                field_type,
                cursor_pos,
                cause,
            } => RoboflowError::FieldDecodeError {
                field_name: field_name.clone(),
                field_type: field_type.clone(),
                cursor_pos: *cursor_pos,
                cause: cause.clone(),
            },
            RoboflowError::Unsupported { feature } => RoboflowError::Unsupported {
                feature: feature.clone(),
            },
            RoboflowError::EncodeError { codec, message } => RoboflowError::EncodeError {
                codec: codec.clone(),
                message: message.clone(),
            },
            RoboflowError::TransformError {
                transform_type,
                message,
            } => RoboflowError::TransformError {
                transform_type: transform_type.clone(),
                message: message.clone(),
            },
            RoboflowError::CodecError { message } => RoboflowError::CodecError {
                message: message.clone(),
            },
            RoboflowError::InvariantViolation { invariant } => RoboflowError::InvariantViolation {
                invariant: invariant.clone(),
            },
            RoboflowError::Other(msg) => RoboflowError::Other(msg.clone()),
            RoboflowError::Timeout(msg) => RoboflowError::Timeout(msg.clone()),
            #[cfg(feature = "cloud-storage")]
            RoboflowError::Storage(err) => {
                // StorageError is not Clone, convert to string representation
                RoboflowError::Other(err.to_string())
            }
        }
    }
}

impl From<std::io::Error> for RoboflowError {
    fn from(err: std::io::Error) -> Self {
        RoboflowError::EncodeError {
            codec: "IO".to_string(),
            message: err.to_string(),
        }
    }
}

impl From<robocodec::CodecError> for RoboflowError {
    fn from(err: robocodec::CodecError) -> Self {
        RoboflowError::CodecError {
            message: err.to_string(),
        }
    }
}

// Forward KPS writer errors to codec errors
#[cfg(feature = "dataset-hdf5")]
impl From<crate::dataset::kps::writers::KpsWriterError> for RoboflowError {
    fn from(err: crate::dataset::kps::writers::KpsWriterError) -> Self {
        RoboflowError::EncodeError {
            codec: "KpsWriter".to_string(),
            message: err.to_string(),
        }
    }
}

#[cfg(all(feature = "dataset-parquet", not(feature = "dataset-hdf5")))]
impl From<crate::dataset::kps::writers::KpsWriterError> for RoboflowError {
    fn from(err: crate::dataset::kps::writers::KpsWriterError) -> Self {
        RoboflowError::EncodeError {
            codec: "KpsWriter".to_string(),
            message: err.to_string(),
        }
    }
}

#[cfg(feature = "cloud-storage")]
impl From<crate::storage::StorageError> for RoboflowError {
    fn from(err: crate::storage::StorageError) -> Self {
        RoboflowError::Storage(err)
    }
}

/// Result type for codec operations.
pub type Result<T> = std::result::Result<T, RoboflowError>;

/// Helper macro for creating structured error logs.
///
/// This macro integrates with tracing to provide structured logging
/// with error codes and fields.
#[macro_export]
macro_rules! log_error {
    ($error:expr) => {
        tracing::error!(
            error_code = $error.code(),
            error_category = $error.category().as_str(),
            {:tracing::field::debug($error.log_fields())},
            "{}", $error
        )
    };
}

/// Helper macro for creating structured warning logs.
#[macro_export]
macro_rules! log_warning {
    ($context:expr, $message:expr) => {
        tracing::warn!(context = $context, message = $message,)
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = RoboflowError::parse("schema", "unexpected token");
        assert!(format!("{err}").contains("[PARSE-1001]"));
        assert!(format!("{err}").contains("Parse error in 'schema': unexpected token"));

        let err = RoboflowError::type_not_found("MyType");
        assert!(format!("{err}").contains("[SCHEMA-2002]"));
        assert!(format!("{err}").contains("Type not found: 'MyType'"));
    }

    #[test]
    fn test_error_category() {
        let err = RoboflowError::parse("test", "test");
        assert_eq!(err.category(), ErrorCategory::Parse);
        assert_eq!(err.code(), 1001);

        let err = RoboflowError::invalid_schema("test", "test");
        assert_eq!(err.category(), ErrorCategory::Schema);
        assert_eq!(err.code(), 2001);

        let err = RoboflowError::buffer_too_short(10, 5, 0);
        assert_eq!(err.category(), ErrorCategory::Runtime);
        assert_eq!(err.code(), 3001);
    }

    #[test]
    fn test_log_fields() {
        let err = RoboflowError::type_not_found("MyType");
        let fields = err.log_fields();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].0, "type");
        assert_eq!(fields[0].1, "MyType");
    }

    #[test]
    fn test_transform_error() {
        let err = RoboflowError::transform("TopicRename", "collision detected");
        assert_eq!(err.category(), ErrorCategory::Transform);
        assert_eq!(err.code(), 5001);
        assert!(format!("{err}").contains("Transform error (TopicRename)"));
    }

    #[test]
    fn test_invariant_violation() {
        let err = RoboflowError::invariant_violation("mmap data dropped while stream active");
        assert_eq!(err.category(), ErrorCategory::Runtime);
        assert_eq!(err.code(), 3005);
        assert!(format!("{err}").contains("Invariant violation"));
    }
}
