// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! MSG format parser using Pest.
//!
//! This module handles parsing of ROS .msg format files.
//!
//! The format supports:
//! - Simple field lists (root message)
//! - Dependency blocks with "MSG: TypeName" headers
//! - Array types: T[] (dynamic) or T[n] (fixed)
//! - Nested types: package/MessageName
//! - Comments (# style)

use crate::core::CodecError;
use crate::core::Result as CoreResult;
use crate::schema::ast::MessageSchema;
use crate::schema::ast::{Field, FieldType, MessageType, PrimitiveType};
use pest::Parser;
use pest_derive::Parser;

/// ROS version detected from encoding and schema format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RosVersion {
    /// ROS1 - uses ros1msg encoding, Header has seq field
    Ros1,
    /// ROS2 - uses CDR encoding, Header has no seq field
    Ros2,
    /// Unknown version - don't modify schema
    Unknown,
}

impl RosVersion {
    /// Detect ROS version from message encoding string.
    ///
    /// # Arguments
    /// * `encoding` - The message encoding (e.g., "ros1msg", "cdr")
    ///
    /// # Returns
    /// * `Ros1` if encoding is "ros1msg"
    /// * `Ros2` if encoding is "cdr"
    /// * `Unknown` otherwise
    pub fn from_encoding(encoding: &str) -> Self {
        let encoding_lower = encoding.to_lowercase();
        if encoding_lower.contains("ros1") {
            RosVersion::Ros1
        } else if encoding_lower == "cdr" {
            RosVersion::Ros2
        } else {
            RosVersion::Unknown
        }
    }

    /// Detect ROS version from message type name.
    ///
    /// ROS2 types use `/msg/` in their path (e.g., `std_msgs/msg/Header`).
    /// ROS1 types use just `/` (e.g., `std_msgs/Header`).
    pub fn from_type_name(type_name: &str) -> Self {
        if type_name.contains("/msg/") {
            RosVersion::Ros2
        } else if type_name.contains('/') && !type_name.contains("/msg/") {
            RosVersion::Ros1
        } else {
            RosVersion::Unknown
        }
    }
}

/// Pest parser for ROS .msg schema files.
#[derive(Parser)]
#[grammar = "schema/parser/msg_parser/msg.pest"] // Path relative to src/ directory
pub struct MsgParser;

/// Parse classic ROS .msg format (auto-detects ROS version from type name).
pub fn parse(name: &str, definition: &str) -> CoreResult<MessageSchema> {
    // Auto-detect ROS version from type name
    let ros_version = RosVersion::from_type_name(name);
    parse_with_version(name, definition, ros_version)
}

/// Parse classic ROS .msg format with explicit encoding.
///
/// Use this when you know the message encoding from the container format (e.g., MCAP).
pub fn parse_with_encoding(
    name: &str,
    definition: &str,
    encoding: &str,
) -> CoreResult<MessageSchema> {
    let ros_version = RosVersion::from_encoding(encoding);
    parse_with_version(name, definition, ros_version)
}

/// Preprocess schema to convert indented inline type definitions to standard MSG format.
///
/// Converts:
/// ```text
/// geometry_msgs/Vector3 linear
///   float64 x
///   float64 y
///   float64 z
/// ```
///
/// To:
/// ```text
/// geometry_msgs/Vector3 linear
/// ===
/// MSG: geometry_msgs/Vector3
/// float64 x
/// float64 y
/// float64 z
/// ```
fn preprocess_indented_schema(definition: &str) -> String {
    let lines: Vec<&str> = definition.lines().collect();
    let mut root_lines: Vec<String> = Vec::new();
    let mut nested_types: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    let mut current_nested_type: Option<String> = None;

    for line in lines {
        let trimmed = line.trim();

        // Skip empty lines and comments
        if trimmed.is_empty() || trimmed.starts_with('#') {
            if current_nested_type.is_none() {
                root_lines.push(line.to_string());
            }
            continue;
        }

        // Check if line starts with whitespace (indented)
        let is_indented = line.starts_with(' ') || line.starts_with('\t');

        if is_indented {
            // This is a field of the current nested type
            if let Some(ref type_name) = current_nested_type {
                nested_types
                    .entry(type_name.clone())
                    .or_default()
                    .push(trimmed.to_string());
            }
        } else {
            // Non-indented line - this is a root field
            current_nested_type = None;
            root_lines.push(line.to_string());

            // Check if this field references a nested type (not a primitive)
            // Format: "package/Type fieldname" or "Type fieldname"
            if let Some(nested_type) = extract_nested_type(trimmed) {
                current_nested_type = Some(nested_type);
            }
        }
    }

    // Build the final schema with dependency blocks
    let mut result = root_lines.join("\n");

    for (type_name, fields) in nested_types {
        if !fields.is_empty() {
            result.push_str("\n===\nMSG: ");
            result.push_str(&type_name);
            result.push('\n');
            result.push_str(&fields.join("\n"));
            result.push('\n');
        }
    }

    result
}

/// Extract nested type name from a field declaration, if any.
/// Returns None for primitive types.
fn extract_nested_type(line: &str) -> Option<String> {
    // Skip constants (contain '=')
    if line.contains('=') {
        return None;
    }

    // Split on whitespace to get the type part
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.is_empty() {
        return None;
    }

    let type_part = parts[0];

    // Remove array suffix if present
    let base_type = type_part.split('[').next().unwrap_or(type_part);

    // Check if it's a nested type (contains '/' or is not a primitive)
    let primitives = [
        "bool", "boolean", "byte", "char", "int8", "int16", "int32", "int64", "uint8", "uint16",
        "uint32", "uint64", "float32", "float64", "float", "double", "string", "wstring", "time",
        "duration",
    ];

    if primitives.contains(&base_type) {
        None
    } else {
        Some(base_type.to_string())
    }
}

/// Parse classic ROS .msg format with explicit ROS version.
pub fn parse_with_version(
    name: &str,
    definition: &str,
    ros_version: RosVersion,
) -> CoreResult<MessageSchema> {
    // Preprocess to convert indented format to standard MSG format
    let definition = preprocess_indented_schema(definition);

    let pairs = MsgParser::parse(Rule::schema, &definition)
        .map_err(|e| CodecError::parse("msg schema", format!("{e}")))?;

    let mut schema = MessageSchema::new(name.to_string());

    for pair in pairs {
        // schema = SOI ~ root_msg ~ (separator ~ dependency_msg)* ~ EOI
        for item in pair.into_inner() {
            match item.as_rule() {
                Rule::EOI => {}
                Rule::root_msg => {
                    // Parse root message fields
                    let mut msg_type = MessageType::new(name.to_string());
                    for field_item in item.into_inner() {
                        if let Some(field) = parse_msg_line(field_item) {
                            msg_type.add_field(field);
                        }
                    }
                    schema.add_type(msg_type);
                }
                Rule::dependency_msg => {
                    // Parse dependency block: dependency_header ~ (msg_line | comment | empty_line)*
                    let mut inner = item.into_inner();

                    // First item is dependency_header: "MSG: std_msgs/Header"
                    if let Some(header) = inner.next() {
                        // Try to extract the type name from the dependency header
                        // The format is "MSG: package/type" but we use the full string as type name
                        let type_name = header.as_str().to_string();

                        // Remove "MSG:" prefix if present
                        let type_name = type_name.strip_prefix("MSG:").unwrap_or(&type_name).trim();

                        if !type_name.is_empty() {
                            let mut msg_type = MessageType::new(type_name.to_string());

                            // Remaining items are msg_line, comment, or empty_line
                            for field_item in inner {
                                if let Some(field) = parse_msg_line(field_item) {
                                    msg_type.add_field(field);
                                }
                            }

                            schema.add_type(msg_type);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    // Post-processing: Add seq field to Header types for ROS1 data
    // ROS1 Header has: seq, stamp, frame_id
    // ROS2 Header has: stamp, frame_id (no seq)
    if ros_version == RosVersion::Ros1 {
        add_seq_field_to_header_types(&mut schema);
        // Remove Header fields from top-level messages for ROS1 bags
        // because the Header data is already in the ROS1 record header
        remove_header_fields_from_ros1_messages(&mut schema);
    }

    Ok(schema)
}

/// Parse a single msg_line into a Field, if possible.
fn parse_msg_line(pair: pest::iterators::Pair<Rule>) -> Option<Field> {
    if pair.as_rule() != Rule::msg_line {
        return None;
    }

    // msg_line structure: base_type ~ array_suffix? ~ WHITESPACE+ ~ field_name ~ ...
    // Since msg_line is atomic, we extract from the string directly
    let content = pair.as_str();

    // Skip constant declarations (lines containing "=" before the field name)
    // Constants look like: "byte DEBUG=10" while fields look like: "byte level"
    // We need to check if there's an "=" before any whitespace that would separate field name
    if content.contains('=') {
        // This is a constant declaration, skip it
        return None;
    }

    // Find the first space (after base_type and optional array_spec)
    let space_pos = content.find(|c: char| c.is_whitespace())?;
    let type_part = &content[..space_pos];

    // Extract base_type and array_spec from type_part
    let (base_type_str, is_array, array_size) = if let Some(bracket_pos) = type_part.find('[') {
        let base = &type_part[..bracket_pos];
        let array_part = &type_part[bracket_pos..];
        let digits: String = array_part.chars().filter(|c| c.is_ascii_digit()).collect();
        let size = if !digits.is_empty() {
            digits.parse().ok()
        } else {
            None
        };
        (base.to_string(), true, size)
    } else {
        (type_part.to_string(), false, None)
    };

    // Find field name (after the space)
    let after_type = content[space_pos..].trim_start();
    let field_end = after_type
        .find(|c: char| c.is_whitespace())
        .unwrap_or(after_type.len());
    let field_name = after_type[..field_end].to_string();

    let field_type = build_field_type(&base_type_str, is_array, array_size);
    Some(Field {
        name: field_name,
        type_name: field_type,
    })
}

/// Build a FieldType from a base type string and array info.
fn build_field_type(base_type_str: &str, is_array: bool, array_size: Option<usize>) -> FieldType {
    let base_type_str = base_type_str.trim();
    let base = if let Some(prim) = PrimitiveType::try_from_str(base_type_str) {
        FieldType::Primitive(prim)
    } else {
        // Nested type (e.g., "std_msgs/Header")
        FieldType::Nested(base_type_str.to_string())
    };

    if is_array {
        FieldType::Array {
            base_type: Box::new(base),
            size: array_size,
        }
    } else {
        base
    }
}

/// Add seq field to all std_msgs/Header variants for ROS1 compatibility.
///
/// ROS1 Header has: uint32 seq, time stamp, string frame_id
/// ROS2 Header has: builtin_interfaces/Time stamp, string frame_id
///
/// This function adds the seq field to Header types when parsing ROS1 data.
fn add_seq_field_to_header_types(schema: &mut MessageSchema) {
    // Find all Header type variants in the schema
    let header_variants: Vec<String> = schema
        .types
        .keys()
        .filter(|k| {
            // Match Header types (various naming conventions)
            k.contains("Header") && (k.contains("std_msgs") || k.ends_with("/Header"))
        })
        .cloned()
        .collect();

    for variant_name in &header_variants {
        if let Some(header_type) = schema.types.get_mut(variant_name) {
            let has_seq = header_type.fields.iter().any(|f| f.name == "seq");
            if !has_seq {
                // Insert seq field after stamp field (at index 1)
                // ROS1 Header order: seq, stamp, frame_id
                // But we insert at index 1 because stamp is at index 0
                let seq_field = Field {
                    name: "seq".to_string(),
                    type_name: FieldType::Primitive(PrimitiveType::UInt32),
                };

                // Find stamp field index, insert seq after it
                let stamp_idx = header_type
                    .fields
                    .iter()
                    .position(|f| f.name == "stamp")
                    .unwrap_or(0);

                header_type.fields.insert(stamp_idx + 1, seq_field);
                header_type.max_alignment = header_type.max_alignment.max(4);
            }
        }
    }
}

/// Remove Header fields from top-level messages for ROS1 bags.
///
/// In ROS1 bags, the Header field is often not serialized because the
/// timestamp is already in the record header. This function removes
/// the Header field ONLY from the top-level message type (schema.name),
/// not from nested dependency types.
///
/// For example, if decoding `tf2_msgs/TFMessage` which contains an array
/// of `geometry_msgs/TransformStamped`, only TFMessage's header field is
/// removed (if it has one). The TransformStamped nested type keeps its
/// header field because that data IS present in the message bytes.
fn remove_header_fields_from_ros1_messages(schema: &mut MessageSchema) {
    // Only modify the top-level message type (the one that matches schema.name)
    // Do NOT modify nested dependency types like geometry_msgs/TransformStamped
    let top_level_name = &schema.name;

    // Skip if the top-level type doesn't exist or is a Header type itself
    if top_level_name.ends_with("/Header") || top_level_name == "Header" {
        return;
    }

    // Check if the top-level type has a header field as its first field
    if let Some(msg_type) = schema.types.get_mut(top_level_name) {
        // Skip if this looks like a Header type (has frame_id but not seq)
        if msg_type.fields.iter().any(|f| f.name == "frame_id")
            && !msg_type.fields.iter().any(|f| f.name == "seq")
        {
            return;
        }

        // Remove the first field if it's named "header" and contains "Header" in type
        if !msg_type.fields.is_empty() && msg_type.fields[0].name == "header" {
            if let FieldType::Nested(nested_type) = &msg_type.fields[0].type_name {
                if nested_type.contains("Header") {
                    msg_type.fields.remove(0);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_field() {
        let schema = parse("TestMsg", "int32 value").unwrap();
        let msg_type = schema.get_type("TestMsg").unwrap();

        assert_eq!(msg_type.fields.len(), 1);
        assert_eq!(msg_type.fields[0].name, "value");
    }

    #[test]
    fn test_parse_multiple_fields() {
        let schema = parse("TestMsg", "int32 x\nint32 y").unwrap();
        let msg_type = schema.get_type("TestMsg").unwrap();

        assert_eq!(msg_type.fields.len(), 2);
        assert_eq!(msg_type.fields[0].name, "x");
        assert_eq!(msg_type.fields[1].name, "y");
    }

    #[test]
    fn test_parse_dynamic_array() {
        let schema = parse("TestMsg", "int32[] values").unwrap();
        let msg_type = schema.get_type("TestMsg").unwrap();

        assert_eq!(msg_type.fields.len(), 1);
        assert_eq!(msg_type.fields[0].name, "values");
        match &msg_type.fields[0].type_name {
            FieldType::Array { size, .. } => {
                assert!(size.is_none(), "Expected dynamic array");
            }
            _ => panic!("Expected Array type"),
        }
    }

    #[test]
    fn test_parse_fixed_array() {
        let schema = parse("TestMsg", "float32[3] position").unwrap();
        let msg_type = schema.get_type("TestMsg").unwrap();

        assert_eq!(msg_type.fields.len(), 1);
        assert_eq!(msg_type.fields[0].name, "position");
        match &msg_type.fields[0].type_name {
            FieldType::Array { size, .. } => {
                assert_eq!(*size, Some(3));
            }
            _ => panic!("Expected Array type"),
        }
    }

    #[test]
    fn test_parse_nested_type() {
        let schema = parse("TestMsg", "std_msgs/Header header").unwrap();
        let msg_type = schema.get_type("TestMsg").unwrap();

        assert_eq!(msg_type.fields.len(), 1);
        assert_eq!(msg_type.fields[0].name, "header");
        match &msg_type.fields[0].type_name {
            FieldType::Nested(name) => {
                assert_eq!(name, "std_msgs/Header");
            }
            _ => panic!("Expected Nested type"),
        }
    }

    #[test]
    fn test_parse_with_comments() {
        let schema = parse("TestMsg", "# This is a comment\nint32 value").unwrap();
        let msg_type = schema.get_type("TestMsg").unwrap();

        assert_eq!(msg_type.fields.len(), 1);
        assert_eq!(msg_type.fields[0].name, "value");
    }

    #[test]
    fn test_parse_ros2_joint_state() {
        let msg = r#"
std_msgs/Header header

string[] name
float64[] position
float64[] velocity
float64[] effort
"#;
        let schema = parse("JointState", msg).unwrap();
        let msg_type = schema.get_type("JointState").unwrap();

        assert_eq!(msg_type.fields.len(), 5);
        assert_eq!(msg_type.fields[0].name, "header");
        assert_eq!(msg_type.fields[1].name, "name");
        assert_eq!(msg_type.fields[2].name, "position");
        assert_eq!(msg_type.fields[3].name, "velocity");
        assert_eq!(msg_type.fields[4].name, "effort");
    }

    #[test]
    fn test_ros_version_detection() {
        // ROS2 types have /msg/ in the path
        assert_eq!(
            RosVersion::from_type_name("sensor_msgs/msg/Image"),
            RosVersion::Ros2
        );
        assert_eq!(
            RosVersion::from_type_name("std_msgs/msg/Header"),
            RosVersion::Ros2
        );

        // ROS1 types don't have /msg/
        assert_eq!(
            RosVersion::from_type_name("sensor_msgs/Image"),
            RosVersion::Ros1
        );
        assert_eq!(
            RosVersion::from_type_name("std_msgs/Header"),
            RosVersion::Ros1
        );

        // Encoding detection
        assert_eq!(RosVersion::from_encoding("ros1msg"), RosVersion::Ros1);
        assert_eq!(RosVersion::from_encoding("cdr"), RosVersion::Ros2);
        assert_eq!(RosVersion::from_encoding("CDR"), RosVersion::Ros2);
    }

    #[test]
    fn test_ros1_header_has_seq() {
        let msg = r#"
std_msgs/Header header
"#;
        // Parse as ROS1 - should add seq field
        let schema = parse_with_encoding("test/Msg", msg, "ros1msg").unwrap();

        // Check if Header type exists and has seq field
        if let Some(header_type) = schema.get_type("std_msgs/Header") {
            let has_seq = header_type.fields.iter().any(|f| f.name == "seq");
            assert!(has_seq, "ROS1 Header should have seq field");
        }
    }

    #[test]
    fn test_ros2_header_no_seq() {
        let msg = r#"
std_msgs/Header header
===
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
"#;
        // Parse as ROS2 - should NOT add seq field
        let schema = parse_with_encoding("sensor_msgs/msg/Image", msg, "cdr").unwrap();

        if let Some(header_type) = schema.get_type("std_msgs/Header") {
            let has_seq = header_type.fields.iter().any(|f| f.name == "seq");
            assert!(!has_seq, "ROS2 Header should NOT have seq field");
        }
    }
}
