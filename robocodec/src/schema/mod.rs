// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Schema parsing for ROS and IDL formats.
//!
//! This module provides parsing for:
//! - ROS1 `.msg` files
//! - ROS2 IDL formats
//! - OMG IDL formats

pub mod ast;
pub mod builtin_types;
pub mod descriptor;
pub mod parser;

pub use ast::{Field, FieldType, MessageSchema, MessageType, PrimitiveType};
pub use descriptor::{FieldInfo, SchemaDescriptor};
pub use parser::{parse_schema, parse_schema_with_encoding};

// Re-export parser-specific types
pub use parser::{msg_parser, ros2_idl_parser};

// Legacy re-exports for compatibility
pub use msg_parser::{parse_with_encoding, parse_with_version, RosVersion};
pub use ros2_idl_parser::{normalize_ros2_idl, parse as parse_ros2_idl};

/// Schema format type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaFormat {
    /// ROS .msg format
    Msg,
    /// OMG IDL format
    Idl,
}

impl SchemaFormat {
    /// Parse from string.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "msg" => Some(SchemaFormat::Msg),
            "idl" => Some(SchemaFormat::Idl),
            _ => None,
        }
    }

    /// Get string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            SchemaFormat::Msg => "msg",
            SchemaFormat::Idl => "idl",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_format_from_str_msg() {
        assert_eq!(SchemaFormat::parse("msg"), Some(SchemaFormat::Msg));
        assert_eq!(SchemaFormat::parse("MSG"), Some(SchemaFormat::Msg));
        assert_eq!(SchemaFormat::parse("Msg"), Some(SchemaFormat::Msg));
    }

    #[test]
    fn test_schema_format_from_str_idl() {
        assert_eq!(SchemaFormat::parse("idl"), Some(SchemaFormat::Idl));
        assert_eq!(SchemaFormat::parse("IDL"), Some(SchemaFormat::Idl));
        assert_eq!(SchemaFormat::parse("Idl"), Some(SchemaFormat::Idl));
    }

    #[test]
    fn test_schema_format_from_str_unknown() {
        assert_eq!(SchemaFormat::parse("unknown"), None);
        assert_eq!(SchemaFormat::parse(""), None);
        assert_eq!(SchemaFormat::parse("xml"), None);
    }

    #[test]
    fn test_schema_format_as_str() {
        assert_eq!(SchemaFormat::Msg.as_str(), "msg");
        assert_eq!(SchemaFormat::Idl.as_str(), "idl");
    }

    #[test]
    fn test_schema_format_equality() {
        assert_eq!(SchemaFormat::Msg, SchemaFormat::Msg);
        assert_eq!(SchemaFormat::Idl, SchemaFormat::Idl);
        assert_ne!(SchemaFormat::Msg, SchemaFormat::Idl);
    }

    #[test]
    fn test_parse_schema_reexport() {
        // Verify that parse_schema is accessible
        let schema = parse_schema("test/Type", "int32 value");
        assert!(schema.is_ok());
    }

    #[test]
    fn test_parse_schema_with_encoding_reexport() {
        // Verify that parse_schema_with_encoding is accessible
        let schema = parse_schema_with_encoding("test/Type", "int32 value", SchemaFormat::Msg);
        assert!(schema.is_ok());
    }

    #[test]
    fn test_message_schema_reexport() {
        // Verify that MessageSchema is accessible
        let schema = MessageSchema::new("test/Type".to_string());
        assert_eq!(schema.name, "test/Type");
    }

    #[test]
    fn test_parse_with_encoding_reexport() {
        // Verify parse_with_encoding re-export
        let schema = parse_with_encoding("test/Type", "int32 value", "ros1msg");
        assert!(schema.is_ok());
    }

    #[test]
    fn test_parse_ros2_idl_reexport() {
        // Verify parse_ros2_idl re-export
        let idl = "struct Test { int32 value; };";
        let result = parse_ros2_idl("test/Type", idl);
        assert!(result.is_ok());
    }

    #[test]
    fn test_normalize_ros2_idl_reexport() {
        // Verify normalize_ros2_idl re-export
        let idl = "#include 'test.idl'\nstruct Test { int32 value; };";
        let normalized = normalize_ros2_idl(idl);
        assert!(normalized.contains("struct Test"));
    }
}
