// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Core types used throughout robocodec.
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

/// Error returned when parsing an `Encoding` from string fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseEncodingError {
    _private: (),
}

impl std::fmt::Display for ParseEncodingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "invalid encoding name, expected 'cdr', 'protobuf', or 'json'"
        )
    }
}

impl std::error::Error for ParseEncodingError {}

impl std::str::FromStr for Encoding {
    type Err = ParseEncodingError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "cdr" => Ok(Encoding::Cdr),
            "protobuf" => Ok(Encoding::Protobuf),
            "json" => Ok(Encoding::Json),
            _ => Err(ParseEncodingError { _private: () }),
        }
    }
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
}
