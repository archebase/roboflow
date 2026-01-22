// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Core error types for robocodec.
//!
//! Provides error types for format I/O operations:
//! - Schema and data parsing
//! - Buffer and decoding
//! - Encoding operations
//! - File format operations

use std::fmt;

/// Errors that can occur during format I/O operations.
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

    /// Encoding/decoding error
    EncodeError {
        /// Codec context (e.g., "CDR", "Protobuf", "JSON")
        codec: String,
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

    /// Create a "type not found" error.
    pub fn type_not_found(type_name: impl Into<String>) -> Self {
        CodecError::TypeNotFound {
            type_name: type_name.into(),
        }
    }

    /// Create an encode/decode error.
    pub fn encode(codec: impl Into<String>, message: impl Into<String>) -> Self {
        CodecError::EncodeError {
            codec: codec.into(),
            message: message.into(),
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

    /// Create an alignment error.
    pub fn alignment_error(expected: u64, actual: u64) -> Self {
        CodecError::AlignmentError { expected, actual }
    }

    /// Create a length exceeded error.
    pub fn length_exceeded(length: usize, position: usize, buffer_len: usize) -> Self {
        CodecError::LengthExceeded {
            length,
            position,
            buffer_len,
        }
    }

    /// Create an unsupported feature error.
    pub fn unsupported(feature: impl Into<String>) -> Self {
        CodecError::Unsupported {
            feature: feature.into(),
        }
    }

    /// Create an invariant violation error (for unsafe block validation).
    pub fn invariant_violation(invariant: impl Into<String>) -> Self {
        CodecError::InvariantViolation {
            invariant: invariant.into(),
        }
    }

    /// Create an "unknown codec" error.
    pub fn unknown_codec(encoding: impl Into<String>) -> Self {
        CodecError::Unsupported {
            feature: format!("unknown codec: {}", encoding.into()),
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
            CodecError::InvariantViolation { invariant } => {
                vec![("invariant", invariant.clone())]
            }
            CodecError::Other(msg) => vec![("message", msg.clone())],
        }
    }
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodecError::ParseError { context, message } => {
                write!(f, "Parse error in {context}: {message}")
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
            CodecError::InvariantViolation { invariant } => {
                write!(f, "Invariant violation: {invariant}")
            }
            CodecError::Other(msg) => write!(f, "Other error: {msg}"),
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

/// Result type for robocodec operations.
pub type Result<T> = std::result::Result<T, CodecError>;
