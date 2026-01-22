// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Schema parser implementations.

pub mod idl_parser;
pub mod msg_parser;
pub mod ros2_idl_parser;

// Include the unified parser (has format detection and ROS2 IDL handling)
pub mod unified;

pub use msg_parser::{parse_with_encoding, parse_with_version, RosVersion};
pub use ros2_idl_parser::{normalize_ros2_idl, parse as parse_ros2_idl};

// Main parser interface
use crate::core::Result;
use crate::schema::{MessageSchema, SchemaFormat};

/// Parse a schema from a string.
///
/// # Arguments
///
/// * `name` - Message name
/// * `definition` - Schema definition string
///
/// # Returns
///
/// Parsed `MessageSchema`
pub fn parse_schema(name: &str, definition: &str) -> Result<MessageSchema> {
    parse_schema_with_encoding(name, definition, SchemaFormat::Msg)
}

/// Parse a schema with explicit format specification.
///
/// # Arguments
///
/// * `name` - Message name
/// * `definition` - Schema definition string
/// * `format` - Schema format (Msg, Idl, etc.)
///
/// # Returns
///
/// Parsed `MessageSchema`
pub fn parse_schema_with_encoding(
    name: &str,
    definition: &str,
    format: SchemaFormat,
) -> Result<MessageSchema> {
    match format {
        SchemaFormat::Msg => msg_parser::parse(name, definition)
            .map_err(|e| crate::core::CodecError::parse("schema", e.to_string())),
        SchemaFormat::Idl => idl_parser::parse(name, definition)
            .map_err(|e| crate::core::CodecError::parse("schema", e.to_string())),
    }
}

/// Parse a schema with string-based encoding specification.
///
/// # Arguments
///
/// * `name` - Message name
/// * `definition` - Schema definition string
/// * `encoding` - Schema encoding string (e.g., "ros1msg", "ros2msg", "ros2idl")
///
/// # Returns
///
/// Parsed `MessageSchema`
pub fn parse_schema_with_encoding_str(
    name: &str,
    definition: &str,
    encoding: &str,
) -> Result<MessageSchema> {
    let encoding_lower = encoding.to_lowercase();

    // ROS2 IDL format needs special handling (strips separator headers)
    if encoding_lower.contains("ros2idl") {
        return ros2_idl_parser::parse(name, definition)
            .map_err(|e| crate::core::CodecError::parse("schema", e.to_string()));
    }

    // For other encodings, use the format-based parser from unified.rs
    // which handles format detection and ROS2 IDL header stripping
    unified::parse_schema_with_encoding(name, definition, &encoding_lower)
        .map_err(|e| crate::core::CodecError::parse("schema", e.to_string()))
}
