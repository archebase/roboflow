// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified parser for IDL and ROS .msg schema files.
//!
//! This module provides two separate Pest grammars:
//! - `grammar/msg.pest` for ROS .msg format
//! - `grammar/omg_idl.pest` for OMG IDL format

use crate::core::{CodecError, Result as CoreResult};
use crate::schema::ast::MessageSchema;
use crate::schema::builtin_types;

// Import separate parsers (each has its own Rule enum in its own module)
use super::idl_parser;
use super::msg_parser;

// =============================================================================
// Schema Format Detection
// =============================================================================

/// Schema format detected during parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaFormat {
    /// Classic ROS .msg format (simple field list)
    ClassicMsg,
    /// Pure OMG IDL format (module/struct declarations)
    OmgIdl,
    /// ROS 2 adapter IDL format (with separator lines and IDL headers)
    Ros2Idl,
}

/// Parse a schema definition (supports IDL, ROS .msg, and ROS 2 adapter formats).
///
/// This function automatically detects the format:
/// - **Classic MSG**: Simple list of field definitions
/// - **IDL format**: Contains `module`, `struct` keywords
/// - **ROS 2 adapter**: Contains separator lines with `IDL:` headers
///
/// # Arguments
///
/// * `name` - The name of the message type (e.g., "std_msgs/Header")
/// * `definition` - The schema file contents
///
/// # Examples
///
/// ```no_run
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use robocodec::schema::parse_schema;
///
/// // Parse simple MSG format
/// let schema = parse_schema("std_msgs/Header", "uint32 seq\ntime stamp\nstring frame_id")?;
///
/// // Parse IDL format
/// let idl_schema = parse_schema("geometry_msgs/Pose",
///     "struct Pose {\n    Point position;\n    Quaternion orientation;\n};")?;
/// # Ok(())
/// # }
/// ```
pub fn parse_schema(name: impl Into<String>, definition: &str) -> CoreResult<MessageSchema> {
    let name = name.into();
    let format = detect_format(definition);

    let mut schema = match format {
        SchemaFormat::ClassicMsg => msg_parser::parse(&name, definition),
        SchemaFormat::OmgIdl => idl_parser::parse(&name, definition),
        SchemaFormat::Ros2Idl => parse_ros2_idl(&name, definition),
    }?;

    // Populate with predefined builtin types
    populate_builtin_types(&mut schema);

    Ok(schema)
}

/// Parse a schema definition with explicit encoding info.
///
/// Use this when you know the message encoding (e.g., from MCAP channel metadata).
/// The encoding helps determine ROS version for proper Header field handling.
///
/// # Arguments
///
/// * `name` - The name of the message type (e.g., "std_msgs/Header")
/// * `definition` - The schema file contents
/// * `encoding` - The message encoding (e.g., "cdr", "ros1msg")
pub fn parse_schema_with_encoding(
    name: impl Into<String>,
    definition: &str,
    encoding: &str,
) -> CoreResult<MessageSchema> {
    let name = name.into();
    let format = detect_format(definition);

    let mut schema = match format {
        SchemaFormat::ClassicMsg => msg_parser::parse_with_encoding(&name, definition, encoding),
        SchemaFormat::OmgIdl => idl_parser::parse_with_encoding(&name, definition, encoding),
        SchemaFormat::Ros2Idl => parse_ros2_idl_with_encoding(&name, definition, encoding),
    }?;

    // Populate with predefined builtin types
    populate_builtin_types(&mut schema);

    Ok(schema)
}

/// Populate a schema with predefined builtin types.
///
/// This adds standard ROS2 builtin_interfaces types like Time and Duration
/// to the schema, ensuring they're available when decoding messages that
/// reference them.
fn populate_builtin_types(schema: &mut MessageSchema) {
    for builtin_type in builtin_types::get_all() {
        // Only add if not already present (user schemas can override)
        if !schema.types.contains_key(&builtin_type.name) {
            schema.add_type(builtin_type);
        }
    }
}

/// Detect the schema format from the definition.
fn detect_format(definition: &str) -> SchemaFormat {
    // Check for ROS 2 adapter format - schemas starting with "IDL:" header
    // This can be either:
    // 1. Separator lines with "IDL:" header: =========\nIDL: xyz
    // 2. Direct "IDL:" at the start of a line
    if definition.contains("IDL:") {
        // Check for separator pattern or direct IDL: lines
        for line in definition.lines() {
            if line.starts_with("===") && line.len() >= 3 {
                // Separator line found, this is ROS2 IDL format
                return SchemaFormat::Ros2Idl;
            }
            if line.starts_with("IDL:") {
                // Direct IDL: header found, this is ROS2 IDL format
                return SchemaFormat::Ros2Idl;
            }
        }
    }

    // Check for pure IDL format (module keyword or struct keyword)
    let trimmed = definition.trim_start();
    if trimmed.starts_with("module ") || trimmed.starts_with("struct ") {
        return SchemaFormat::OmgIdl;
    }

    // Default to classic MSG
    SchemaFormat::ClassicMsg
}

/// Parse ROS 2 adapter IDL format (with separator lines).
///
/// This format has separator lines like:
///   ================================================================================================
///   IDL: std_msgs/msg/Header
///
/// We strip these headers and parse the entire content as pure OMG IDL.
fn parse_ros2_idl(name: &str, definition: &str) -> CoreResult<MessageSchema> {
    // Default to ROS2 for ROS2 IDL format
    parse_ros2_idl_with_encoding(name, definition, "cdr")
}

/// Parse ROS 2 adapter IDL format with explicit encoding info.
fn parse_ros2_idl_with_encoding(
    name: &str,
    definition: &str,
    encoding: &str,
) -> CoreResult<MessageSchema> {
    // Strip ROS2 IDL header separator lines (lines starting with 80+ '=' chars)
    // Also strip "IDL: type/name" header lines
    let cleaned: String = definition
        .lines()
        .filter(|line| {
            // Skip separator lines (80+ '=' chars) and "IDL:" header lines
            !(line.starts_with("IDL:") || (line.starts_with('=') && line.len() >= 80))
        })
        .collect::<Vec<&str>>()
        .join("\n");

    // Check if there's any actual schema content left
    let trimmed = cleaned.trim();
    if trimmed.is_empty() || !trimmed.contains("struct") {
        // No actual schema content - this is likely a placeholder or binary-only schema
        return Err(CodecError::parse(
            "IDL schema",
            "No valid schema content found (possibly binary/protobuf schema)",
        ));
    }

    // After stripping headers, parse as pure OMG IDL with encoding
    idl_parser::parse_with_encoding(name, &cleaned, encoding)
}

// Backward compatibility: SchemaParser that wraps parse_schema
pub struct SchemaParser;

impl SchemaParser {
    /// Parse using the appropriate parser based on format detection
    pub fn parse_auto(name: &str, input: &str) -> CoreResult<MessageSchema> {
        parse_schema(name, input)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ast::{FieldType, PrimitiveType};

    #[test]
    fn test_parse_msg_format() {
        let schema = parse_schema("TestMsg", "int32 value\nstring name").unwrap();
        let msg_type = schema.get_type("TestMsg").unwrap();

        assert_eq!(msg_type.fields.len(), 2);
        assert_eq!(msg_type.fields[0].name, "value");
        assert_eq!(msg_type.fields[1].name, "name");
    }

    #[test]
    fn test_parse_msg_arrays() {
        let schema = parse_schema("TestMsg", "int32[] dynamic\nint32[5] fixed").unwrap();
        let msg_type = schema.get_type("TestMsg").unwrap();

        assert_eq!(msg_type.fields.len(), 2);

        if let FieldType::Array { size, .. } = &msg_type.fields[0].type_name {
            assert!(size.is_none());
        } else {
            panic!("Expected array");
        }

        if let FieldType::Array { size, .. } = &msg_type.fields[1].type_name {
            assert_eq!(*size, Some(5));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_parse_idl_struct() {
        let idl = "struct Point { double x; double y; };";

        let schema = parse_schema("Point", idl).unwrap();
        let msg_type = schema.get_type("Point").unwrap();

        assert_eq!(msg_type.fields.len(), 2);
        assert_eq!(msg_type.fields[0].name, "x");
        assert_eq!(msg_type.fields[1].name, "y");
    }

    #[test]
    fn test_parse_idl_struct_with_multiple_types() {
        let idl = "struct Point { double x; double y; };
                   struct Vector3 { double x; double y; double z; };";

        let schema = parse_schema("Test", idl).unwrap();
        let point = schema.get_type("Point").unwrap();
        let vector = schema.get_type("Vector3").unwrap();

        assert_eq!(point.fields.len(), 2);
        assert_eq!(vector.fields.len(), 3);
    }

    #[test]
    fn test_parse_idl_struct_with_integer_types() {
        let idl = "struct Numbers {
            long a;
            unsigned long b;
            long long c;
            unsigned long long d;
            short e;
            unsigned short f;
        };";

        let schema = parse_schema("Numbers", idl).unwrap();
        let msg_type = schema.get_type("Numbers").unwrap();

        assert_eq!(msg_type.fields.len(), 6);
        assert_eq!(msg_type.fields[0].name, "a");
        assert_eq!(msg_type.fields[1].name, "b");
        assert_eq!(msg_type.fields[2].name, "c");
        assert_eq!(msg_type.fields[3].name, "d");
        assert_eq!(msg_type.fields[4].name, "e");
        assert_eq!(msg_type.fields[5].name, "f");
    }

    #[test]
    fn test_parse_idl_struct_with_sequence() {
        let idl = "struct ArrayData { sequence<long> values; };";

        let schema = parse_schema("ArrayData", idl).unwrap();
        let msg_type = schema.get_type("ArrayData").unwrap();

        assert_eq!(msg_type.fields.len(), 1);
        assert_eq!(msg_type.fields[0].name, "values");
        // Verify it's an array type
        match &msg_type.fields[0].type_name {
            FieldType::Array { base_type, size } => {
                assert!(size.is_none()); // sequence is dynamic
                assert!(matches!(
                    base_type.as_ref(),
                    FieldType::Primitive(PrimitiveType::Int32)
                ));
            }
            _ => panic!("Expected Array type"),
        }
    }

    #[test]
    fn test_parse_idl_struct_with_string() {
        let idl = "struct StringData { string name; };";

        let schema = parse_schema("StringData", idl).unwrap();
        let msg_type = schema.get_type("StringData").unwrap();

        assert_eq!(msg_type.fields.len(), 1);
        assert_eq!(msg_type.fields[0].name, "name");
        assert!(matches!(
            msg_type.fields[0].type_name,
            FieldType::Primitive(PrimitiveType::String)
        ));
    }

    #[test]
    fn test_parse_nested_types() {
        let schema =
            parse_schema("TestMsg", "std_msgs/Header header\ngeometry_msgs/Pose pose").unwrap();
        let msg_type = schema.get_type("TestMsg").unwrap();

        assert_eq!(msg_type.fields.len(), 2);

        assert!(matches!(msg_type.fields[0].type_name, FieldType::Nested(_)));
        assert!(matches!(msg_type.fields[1].type_name, FieldType::Nested(_)));
    }

    #[test]
    fn test_parse_with_comments() {
        let schema = parse_schema(
            "TestMsg",
            "# Header comment\nint32 value  # inline comment\nstring name",
        )
        .unwrap();
        let msg_type = schema.get_type("TestMsg").unwrap();

        assert_eq!(msg_type.fields.len(), 2);
    }

    #[test]
    fn test_format_detection() {
        assert_eq!(
            detect_format("int32 value\nstring name"),
            SchemaFormat::ClassicMsg
        );
        assert_eq!(
            detect_format("struct Foo {\n    int32 x;\n};"),
            SchemaFormat::OmgIdl
        );
    }

    #[test]
    fn test_builtin_types_included() {
        // Verify that builtin types are automatically included in parsed schemas
        let schema = parse_schema("TestMsg", "int32 value").unwrap();

        // Check that both naming variants for Time are present
        assert!(schema.get_type("builtin_interfaces/Time").is_some());
        assert!(schema.get_type("builtin_interfaces/msg/Time").is_some());

        // Check that both naming variants for Duration are present
        assert!(schema.get_type("builtin_interfaces/Duration").is_some());
        assert!(schema.get_type("builtin_interfaces/msg/Duration").is_some());
    }

    #[test]
    fn test_builtin_time_field_structure() {
        let schema = parse_schema("TestMsg", "int32 value").unwrap();

        // Verify Time type has correct field structure
        let time = schema.get_type("builtin_interfaces/Time").unwrap();
        assert_eq!(time.fields.len(), 2);
        assert_eq!(time.fields[0].name, "sec");
        assert_eq!(time.fields[1].name, "nanosec");

        // Verify Duration type has correct field structure
        let duration = schema.get_type("builtin_interfaces/Duration").unwrap();
        assert_eq!(duration.fields.len(), 2);
        assert_eq!(duration.fields[0].name, "sec");
        assert_eq!(duration.fields[1].name, "nanosec");
    }

    #[test]
    fn test_schema_with_builtin_time_reference() {
        // Test that a schema can reference builtin_interfaces/Time
        let schema = parse_schema("TestMsg", "builtin_interfaces/Time stamp").unwrap();

        let msg_type = schema.get_type("TestMsg").unwrap();
        assert_eq!(msg_type.fields.len(), 1);
        assert_eq!(msg_type.fields[0].name, "stamp");

        // Verify it's a nested type reference
        match &msg_type.fields[0].type_name {
            FieldType::Nested(name) => {
                assert_eq!(name, "builtin_interfaces/Time");
            }
            _ => panic!("Expected Nested type"),
        }

        // Verify the referenced type exists
        assert!(schema
            .get_type_variants("builtin_interfaces/Time")
            .is_some());
    }
}
