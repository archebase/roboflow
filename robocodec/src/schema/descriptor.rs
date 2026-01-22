// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Schema descriptor trait for loose coupling.
//!
//! This trait provides an abstraction over schema types, allowing
//! codecs and other components to work with schemas without depending
//! on concrete types.

use crate::schema::ast::FieldType;

/// Information about a field in a schema.
#[derive(Debug, Clone)]
pub struct FieldInfo {
    /// Field name
    pub name: String,
    /// Field type
    pub type_name: FieldType,
    /// Field index (for ordered access)
    pub index: usize,
}

/// Abstract schema descriptor.
///
/// This trait allows encoding/decoding code to work with schemas
/// without depending on concrete MessageSchema types, enabling:
/// - Better testability (can use mock schemas)
/// - Loose coupling between modules
/// - Future support for dynamic/runtime schemas
pub trait SchemaDescriptor {
    /// Get all fields in this schema.
    fn fields(&self) -> Vec<FieldInfo>;

    /// Get a field by name.
    fn get_field(&self, name: &str) -> Option<FieldInfo> {
        self.fields().into_iter().find(|f| f.name == name)
    }

    /// Get the number of fields.
    fn field_count(&self) -> usize {
        self.fields().len()
    }

    /// Get the type name for this schema.
    fn type_name(&self) -> &str;

    /// Check if this schema has a nested field (dot notation).
    ///
    /// # Example
    ///
    /// ```
    /// # use robocodec::schema::descriptor::SchemaDescriptor;
    /// // schema.has_nested_field("header.stamp") -> bool
    /// ```
    fn has_nested_field(&self, path: &[&str]) -> bool;
}

/// Implement SchemaDescriptor for MessageSchema.
impl SchemaDescriptor for crate::schema::MessageSchema {
    fn fields(&self) -> Vec<FieldInfo> {
        // Get the main type from the types HashMap
        match self.types.get(&self.name) {
            Some(msg_type) => msg_type
                .fields
                .iter()
                .enumerate()
                .map(|(index, field)| FieldInfo {
                    name: field.name.clone(),
                    type_name: field.type_name.clone(),
                    index,
                })
                .collect(),
            None => Vec::new(),
        }
    }

    fn type_name(&self) -> &str {
        &self.name
    }

    fn has_nested_field(&self, path: &[&str]) -> bool {
        if path.is_empty() {
            return false;
        }

        // For now, this is a simplified implementation.
        // A full implementation would check if the field type is a message type
        // (which has sub-fields) vs a primitive type (which doesn't).
        // This would require recursive type checking through the schema.
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::parse_schema;

    #[test]
    fn test_descriptor_basic() {
        let schema = parse_schema("test/Msg", "int32 value\nstring name\n").unwrap();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field_count(), 2);
        assert_eq!(schema.type_name(), "test/Msg");
    }

    #[test]
    fn test_descriptor_get_field() {
        let schema = parse_schema("test/Msg", "int32 value\nstring name\n").unwrap();

        let field = schema.get_field("value");
        assert!(field.is_some());
        assert_eq!(field.unwrap().name, "value");
    }

    #[test]
    fn test_descriptor_get_field_not_found() {
        let schema = parse_schema("test/Msg", "int32 value\n").unwrap();

        let field = schema.get_field("nonexistent");
        assert!(field.is_none());
    }

    #[test]
    fn test_has_nested_field_simple() {
        let schema = parse_schema("test/Msg", "int32 value\n").unwrap();

        assert!(!schema.has_nested_field(&["value"]));
        assert!(!schema.has_nested_field(&[]));
    }
}
