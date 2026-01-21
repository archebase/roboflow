//! Core error types for robocodec.
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

/// Errors that can occur during codec operations.
#[derive(Debug, Clone)]
pub enum CodecError {
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

    /// Invariant violation (for unsafe block validation failures)
    InvariantViolation {
        /// Description of the invariant that was violated
        invariant: String,
    },

    /// Other error
    Other(String),
}

impl CodecError {
    /// Create a parse error.
    pub fn parse(context: impl Into<String>, message: impl Into<String>) -> Self {
        CodecError::ParseError {
            context: context.into(),
            message: message.into(),
        }
    }

    /// Create an invalid schema error.
    pub fn invalid_schema(schema_name: impl Into<String>, reason: impl Into<String>) -> Self {
        CodecError::InvalidSchema {
            schema_name: schema_name.into(),
            reason: reason.into(),
        }
    }

    /// Create a type not found error.
    pub fn type_not_found(type_name: impl Into<String>) -> Self {
        CodecError::TypeNotFound {
            type_name: type_name.into(),
        }
    }

    /// Create a buffer too short error.
    pub fn buffer_too_short(requested: usize, available: usize, cursor_pos: u64) -> Self {
        CodecError::BufferTooShort {
            requested,
            available,
            cursor_pos,
        }
    }

    /// Create an unsupported error.
    pub fn unsupported(feature: impl Into<String>) -> Self {
        CodecError::Unsupported {
            feature: feature.into(),
        }
    }

    /// Create an encode error.
    pub fn encode(codec: impl Into<String>, message: impl Into<String>) -> Self {
        CodecError::EncodeError {
            codec: codec.into(),
            message: message.into(),
        }
    }

    /// Create a transform error.
    pub fn transform(transform_type: impl Into<String>, message: impl Into<String>) -> Self {
        CodecError::TransformError {
            transform_type: transform_type.into(),
            message: message.into(),
        }
    }

    /// Create an invariant violation error (for unsafe block validation).
    pub fn invariant_violation(invariant: impl Into<String>) -> Self {
        CodecError::InvariantViolation {
            invariant: invariant.into(),
        }
    }

    /// Get the error category for this error.
    pub fn category(&self) -> ErrorCategory {
        match self {
            CodecError::ParseError { .. } => ErrorCategory::Parse,
            CodecError::InvalidSchema { .. } => ErrorCategory::Schema,
            CodecError::TypeNotFound { .. } => ErrorCategory::Schema,
            CodecError::BufferTooShort { .. } => ErrorCategory::Runtime,
            CodecError::AlignmentError { .. } => ErrorCategory::Runtime,
            CodecError::LengthExceeded { .. } => ErrorCategory::Runtime,
            CodecError::FieldDecodeError { .. } => ErrorCategory::Runtime,
            CodecError::Unsupported { .. } => ErrorCategory::Codec,
            CodecError::EncodeError { .. } => ErrorCategory::Codec,
            CodecError::TransformError { .. } => ErrorCategory::Transform,
            CodecError::InvariantViolation { .. } => ErrorCategory::Runtime,
            CodecError::Other(_) => ErrorCategory::Runtime,
        }
    }

    /// Get the error code for this error.
    pub fn code(&self) -> u16 {
        let base = self.category().code_prefix();
        match self {
            CodecError::ParseError { .. } => base + 1,
            CodecError::InvalidSchema { .. } => base + 1,
            CodecError::TypeNotFound { .. } => base + 2,
            CodecError::BufferTooShort { .. } => base + 1,
            CodecError::AlignmentError { .. } => base + 2,
            CodecError::LengthExceeded { .. } => base + 3,
            CodecError::FieldDecodeError { .. } => base + 4,
            CodecError::Unsupported { .. } => base + 1,
            CodecError::EncodeError { .. } => base + 2,
            CodecError::TransformError { .. } => base + 1,
            CodecError::InvariantViolation { .. } => base + 5,
            CodecError::Other(_) => base + 99,
        }
    }

    /// Get structured fields for logging.
    pub fn log_fields(&self) -> Vec<(&'static str, String)> {
        match self {
            CodecError::ParseError { context, message } => {
                vec![("context", context.clone()), ("message", message.clone())]
            }
            CodecError::InvalidSchema {
                schema_name,
                reason,
            } => vec![("schema", schema_name.clone()), ("reason", reason.clone())],
            CodecError::TypeNotFound { type_name } => vec![("type", type_name.clone())],
            CodecError::BufferTooShort {
                requested,
                available,
                cursor_pos,
            } => vec![
                ("requested", requested.to_string()),
                ("available", available.to_string()),
                ("cursor", cursor_pos.to_string()),
            ],
            CodecError::AlignmentError { expected, actual } => vec![
                ("expected", expected.to_string()),
                ("actual", actual.to_string()),
            ],
            CodecError::LengthExceeded {
                length,
                position,
                buffer_len,
            } => vec![
                ("length", length.to_string()),
                ("position", position.to_string()),
                ("buffer_len", buffer_len.to_string()),
            ],
            CodecError::FieldDecodeError {
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
            CodecError::Unsupported { feature } => vec![("feature", feature.clone())],
            CodecError::EncodeError { codec, message } => {
                vec![("codec", codec.clone()), ("message", message.clone())]
            }
            CodecError::TransformError {
                transform_type,
                message,
            } => vec![
                ("transform", transform_type.clone()),
                ("message", message.clone()),
            ],
            CodecError::InvariantViolation { invariant } => {
                vec![("invariant", invariant.clone())]
            }
            CodecError::Other(msg) => vec![("message", msg.clone())],
        }
    }
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let category = self.category();
        let code = self.code();
        write!(f, "[{}-{:04}] ", category.as_str(), code)?;
        match self {
            CodecError::ParseError { context, message } => {
                write!(f, "Parse error in '{context}': {message}")
            }
            CodecError::InvalidSchema {
                schema_name,
                reason,
            } => {
                write!(f, "Invalid schema '{schema_name}': {reason}")
            }
            CodecError::TypeNotFound { type_name } => {
                write!(f, "Type not found: '{type_name}'")
            }
            CodecError::BufferTooShort {
                requested,
                available,
                cursor_pos,
            } => write!(
                f,
                "Buffer too short: requested {requested} bytes at position {cursor_pos}, but only {available} bytes available"
            ),
            CodecError::AlignmentError { expected, actual } => write!(
                f,
                "Alignment error: expected alignment of {expected}, but position is {actual}"
            ),
            CodecError::LengthExceeded {
                length,
                position,
                buffer_len,
            } => write!(
                f,
                "Length {length} exceeds buffer at position {position} (buffer length: {buffer_len})"
            ),
            CodecError::FieldDecodeError {
                field_name,
                field_type,
                cursor_pos,
                cause,
            } => write!(
                f,
                "Failed to decode field '{field_name}' (type: '{field_type}', cursor_pos: {cursor_pos}): {cause}"
            ),
            CodecError::Unsupported { feature } => {
                write!(f, "Unsupported feature: '{feature}'")
            }
            CodecError::EncodeError { codec, message } => {
                write!(f, "{codec} encode error: {message}")
            }
            CodecError::TransformError {
                transform_type,
                message,
            } => {
                write!(f, "Transform error ({transform_type}): {message}")
            }
            CodecError::InvariantViolation { invariant } => {
                write!(f, "Invariant violation: {invariant}")
            }
            CodecError::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for CodecError {}

impl From<std::io::Error> for CodecError {
    fn from(err: std::io::Error) -> Self {
        CodecError::EncodeError {
            codec: "IO".to_string(),
            message: err.to_string(),
        }
    }
}

/// Result type for codec operations.
pub type Result<T> = std::result::Result<T, CodecError>;

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
        let err = CodecError::parse("schema", "unexpected token");
        assert!(format!("{err}").contains("[PARSE-1001]"));
        assert!(format!("{err}").contains("Parse error in 'schema': unexpected token"));

        let err = CodecError::type_not_found("MyType");
        assert!(format!("{err}").contains("[SCHEMA-2002]"));
        assert!(format!("{err}").contains("Type not found: 'MyType'"));
    }

    #[test]
    fn test_error_category() {
        let err = CodecError::parse("test", "test");
        assert_eq!(err.category(), ErrorCategory::Parse);
        assert_eq!(err.code(), 1001);

        let err = CodecError::invalid_schema("test", "test");
        assert_eq!(err.category(), ErrorCategory::Schema);
        assert_eq!(err.code(), 2001);

        let err = CodecError::buffer_too_short(10, 5, 0);
        assert_eq!(err.category(), ErrorCategory::Runtime);
        assert_eq!(err.code(), 3001);
    }

    #[test]
    fn test_log_fields() {
        let err = CodecError::type_not_found("MyType");
        let fields = err.log_fields();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].0, "type");
        assert_eq!(fields[0].1, "MyType");
    }

    #[test]
    fn test_transform_error() {
        let err = CodecError::transform("TopicRename", "collision detected");
        assert_eq!(err.category(), ErrorCategory::Transform);
        assert_eq!(err.code(), 5001);
        assert!(format!("{err}").contains("Transform error (TopicRename)"));
    }

    #[test]
    fn test_invariant_violation() {
        let err = CodecError::invariant_violation("mmap data dropped while stream active");
        assert_eq!(err.category(), ErrorCategory::Runtime);
        assert_eq!(err.code(), 3005);
        assert!(format!("{err}").contains("Invariant violation"));
    }
}
