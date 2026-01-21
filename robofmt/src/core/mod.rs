//! Core types used throughout robofmt.
//!
//! This module provides the foundational types for the library:
//! - [`Error`] - Comprehensive error handling
//! - [`CodecValue`] - Unified value representation
//! - [`TypeRegistry`] - Schema type registry
//! - [`Encoding`] - Message encoding format identifier

pub mod error;
pub mod registry;
pub mod value;

pub use error::{CodecError, Result};
pub use registry::{SchemaProvider, TypeAccessor, TypeRegistry};
pub use value::{CodecValue, DecodedMessage, PrimitiveType};

/// Encoding format identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Encoding {
    /// CDR (Common Data Representation) encoding
    Cdr,
    /// Protobuf encoding
    Protobuf,
    /// JSON encoding
    Json,
}

impl Encoding {
    /// Check if this encoding is CDR.
    pub fn is_cdr(&self) -> bool {
        matches!(self, Encoding::Cdr)
    }

    /// Check if this encoding is Protobuf.
    pub fn is_protobuf(&self) -> bool {
        matches!(self, Encoding::Protobuf)
    }

    /// Check if this encoding is JSON.
    pub fn is_json(&self) -> bool {
        matches!(self, Encoding::Json)
    }

    /// Convert to string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            Encoding::Cdr => "cdr",
            Encoding::Protobuf => "protobuf",
            Encoding::Json => "json",
        }
    }

    /// Parse from string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "cdr" => Some(Encoding::Cdr),
            "protobuf" => Some(Encoding::Protobuf),
            "json" => Some(Encoding::Json),
            _ => None,
        }
    }
}
