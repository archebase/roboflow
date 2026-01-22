// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! AST types for parsed ROS .msg schemas.

use std::collections::HashMap;

/// A parsed ROS message schema.
#[derive(Debug, Clone, PartialEq)]
pub struct MessageSchema {
    /// Schema name (e.g., "std_msgs/msg/Header" or just "Header")
    pub name: String,
    /// Package name (e.g., "std_msgs")
    pub package: Option<String>,
    /// All types defined in this schema (main type + nested types)
    pub types: HashMap<String, MessageType>,
}

/// A message type definition with its fields.
#[derive(Debug, Clone, PartialEq)]
pub struct MessageType {
    /// Type name including package if available
    pub name: String,
    /// Ordered list of fields
    pub fields: Vec<Field>,
    /// Maximum alignment required for this type
    pub max_alignment: u64,
}

/// A field in a message type.
#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    /// Field name
    pub name: String,
    /// Field type
    pub type_name: FieldType,
}

/// Field type - can be primitive, array, or nested message.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldType {
    /// Primitive type
    Primitive(PrimitiveType),
    /// Array type
    Array {
        /// Base type (element type)
        base_type: Box<FieldType>,
        /// Array size (None = dynamic, Some(N) = fixed)
        size: Option<usize>,
    },
    /// Nested message type
    Nested(String),
}

/// Primitive ROS types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PrimitiveType {
    /// Boolean
    Bool,
    /// 8-bit signed integer
    Int8,
    /// 16-bit signed integer
    Int16,
    /// 32-bit signed integer
    Int32,
    /// 64-bit signed integer
    Int64,
    /// 8-bit unsigned integer
    UInt8,
    /// 16-bit unsigned integer
    UInt16,
    /// 32-bit unsigned integer
    UInt32,
    /// 64-bit unsigned integer
    UInt64,
    /// 32-bit float
    Float32,
    /// 64-bit float
    Float64,
    /// String
    String,
    /// Wide string (UTF-16)
    WString,
    /// Byte (alias for UInt8)
    Byte,
    /// Char (alias for Int8)
    Char,
    /// Time (ROS timestamp: sec:int32, nsec:uint32)
    Time,
    /// Duration (ROS duration: sec:int32, nsec:uint32)
    Duration,
}

impl PrimitiveType {
    /// Get the alignment requirement for this primitive type.
    pub fn alignment(self) -> u64 {
        match self {
            PrimitiveType::Bool
            | PrimitiveType::Int8
            | PrimitiveType::UInt8
            | PrimitiveType::Byte
            | PrimitiveType::Char => 1,
            PrimitiveType::Int16 | PrimitiveType::UInt16 => 2,
            PrimitiveType::Int32 | PrimitiveType::UInt32 | PrimitiveType::Float32 => 4,
            PrimitiveType::Int64 | PrimitiveType::UInt64 | PrimitiveType::Float64 => 8,
            PrimitiveType::String | PrimitiveType::WString => 4, // Length prefix is 4-byte aligned
            PrimitiveType::Time | PrimitiveType::Duration => 4,  // 8 bytes total, 4-byte alignment
        }
    }

    /// Get the size in bytes for this primitive type, if fixed.
    pub fn size(self) -> Option<usize> {
        match self {
            PrimitiveType::Bool => Some(1),
            PrimitiveType::Int8
            | PrimitiveType::UInt8
            | PrimitiveType::Byte
            | PrimitiveType::Char => Some(1),
            PrimitiveType::Int16 | PrimitiveType::UInt16 => Some(2),
            PrimitiveType::Int32 | PrimitiveType::UInt32 | PrimitiveType::Float32 => Some(4),
            PrimitiveType::Int64 | PrimitiveType::UInt64 | PrimitiveType::Float64 => Some(8),
            PrimitiveType::String | PrimitiveType::WString => None, // Variable length
            PrimitiveType::Time | PrimitiveType::Duration => Some(8), // sec:int32 + nsec:uint32
        }
    }

    /// Parse a primitive type from a string.
    pub fn try_from_str(s: &str) -> Option<Self> {
        match s {
            "bool" | "boolean" => Some(PrimitiveType::Bool),
            "int8" => Some(PrimitiveType::Int8),
            "int16" => Some(PrimitiveType::Int16),
            "int32" => Some(PrimitiveType::Int32),
            "int64" => Some(PrimitiveType::Int64),
            "uint8" => Some(PrimitiveType::UInt8),
            "uint16" => Some(PrimitiveType::UInt16),
            "uint32" => Some(PrimitiveType::UInt32),
            "uint64" => Some(PrimitiveType::UInt64),
            "float32" | "float" => Some(PrimitiveType::Float32),
            "float64" | "double" => Some(PrimitiveType::Float64),
            "string" => Some(PrimitiveType::String),
            "wstring" => Some(PrimitiveType::WString),
            "byte" => Some(PrimitiveType::Byte),
            "char" => Some(PrimitiveType::Char),
            "time" => Some(PrimitiveType::Time),
            "duration" => Some(PrimitiveType::Duration),
            _ => None,
        }
    }

    /// Convert to the core PrimitiveType.
    pub fn to_core(self) -> crate::PrimitiveType {
        match self {
            PrimitiveType::Bool => crate::PrimitiveType::Bool,
            PrimitiveType::Int8 => crate::PrimitiveType::Int8,
            PrimitiveType::Int16 => crate::PrimitiveType::Int16,
            PrimitiveType::Int32 => crate::PrimitiveType::Int32,
            PrimitiveType::Int64 => crate::PrimitiveType::Int64,
            PrimitiveType::UInt8 => crate::PrimitiveType::UInt8,
            PrimitiveType::UInt16 => crate::PrimitiveType::UInt16,
            PrimitiveType::UInt32 => crate::PrimitiveType::UInt32,
            PrimitiveType::UInt64 => crate::PrimitiveType::UInt64,
            PrimitiveType::Float32 => crate::PrimitiveType::Float32,
            PrimitiveType::Float64 => crate::PrimitiveType::Float64,
            PrimitiveType::String | PrimitiveType::WString => crate::PrimitiveType::String,
            PrimitiveType::Byte | PrimitiveType::Char => crate::PrimitiveType::Byte,
            PrimitiveType::Time | PrimitiveType::Duration => crate::PrimitiveType::Int64, // Fallback
        }
    }
}

impl FieldType {
    /// Get the alignment requirement for this field type.
    pub fn alignment(&self) -> u64 {
        match self {
            FieldType::Primitive(p) => p.alignment(),
            FieldType::Array { base_type, .. } => base_type.alignment(),
            FieldType::Nested(_) => 4, // Nested structs have 4-byte alignment in CDR
        }
    }

    /// Check if this is a complex type (requires per-element alignment in arrays).
    pub fn is_complex(&self) -> bool {
        !matches!(
            self,
            FieldType::Primitive(
                PrimitiveType::Bool
                    | PrimitiveType::Int8
                    | PrimitiveType::UInt8
                    | PrimitiveType::Byte
                    | PrimitiveType::Char
                    | PrimitiveType::Int16
                    | PrimitiveType::UInt16
                    | PrimitiveType::Int32
                    | PrimitiveType::UInt32
                    | PrimitiveType::Float32
                    | PrimitiveType::Int64
                    | PrimitiveType::UInt64
                    | PrimitiveType::Float64
            )
        )
    }
}

impl MessageSchema {
    /// Create an empty schema.
    pub fn new(name: String) -> Self {
        Self {
            package: extract_package(&name),
            name,
            types: HashMap::new(),
        }
    }

    /// Register a type in this schema.
    pub fn add_type(&mut self, msg_type: MessageType) {
        self.types.insert(msg_type.name.clone(), msg_type);
    }

    /// Look up a type by name.
    pub fn get_type(&self, name: &str) -> Option<&MessageType> {
        self.types.get(name)
    }

    /// Look up a type by name with variant resolution.
    pub fn get_type_variants(&self, name: &str) -> Option<&MessageType> {
        // Try exact match first
        if let Some(t) = self.types.get(name) {
            return Some(t);
        }

        // Convert :: to / (IDL uses :: but we store with /)
        let normalized_name = name.replace("::", "/");

        // Try with normalized name
        if let Some(t) = self.types.get(&normalized_name) {
            return Some(t);
        }

        // Try with /msg/ suffix
        if !normalized_name.contains("/msg/") {
            let with_msg = normalized_name.replace('/', "/msg/");
            if let Some(t) = self.types.get(&with_msg) {
                return Some(t);
            }
        }

        // Try without /msg/ suffix
        if normalized_name.contains("/msg/") {
            let without_msg = normalized_name.replace("/msg/", "/");
            if let Some(t) = self.types.get(&without_msg) {
                return Some(t);
            }
        }

        // Try short name match
        if !normalized_name.contains('/') {
            for (full_name, msg_type) in &self.types {
                if full_name.ends_with(&format!("/{normalized_name}"))
                    || full_name.ends_with(&format!("/msg/{normalized_name}"))
                    || full_name.as_str() == normalized_name
                {
                    return Some(msg_type);
                }
            }
        }

        None
    }

    /// Rename all types in the schema by applying a package name transformation.
    ///
    /// This updates:
    /// - The schema's own name and package
    /// - All type names in the types HashMap
    /// - All nested type references in field types
    ///
    /// # Arguments
    ///
    /// * `old_package` - The old package name (e.g., "genie_msgs")
    /// * `new_package` - The new package name (e.g., "archebase")
    pub fn rename_package(&mut self, old_package: &str, new_package: &str) {
        // Update schema name
        self.name = self
            .name
            .replace(&format!("{old_package}/"), &format!("{new_package}/"));
        self.name = self
            .name
            .replace(&format!("{old_package}::"), &format!("{new_package}::"));

        // Update package field
        if self.package.as_deref() == Some(old_package) {
            self.package = Some(new_package.to_string());
        }

        // Build new types HashMap with updated keys and values
        let mut new_types = HashMap::new();
        for (old_key, mut msg_type) in self.types.drain() {
            // Update the type's name
            let new_key = old_key.replace(&format!("{old_package}/"), &format!("{new_package}/"));
            let new_key = new_key.replace(&format!("{old_package}::"), &format!("{new_package}::"));

            msg_type.name = msg_type
                .name
                .replace(&format!("{old_package}/"), &format!("{new_package}/"));
            msg_type.name = msg_type
                .name
                .replace(&format!("{old_package}::"), &format!("{new_package}::"));

            // Update field type references
            for field in &mut msg_type.fields {
                Self::rename_field_type(&mut field.type_name, old_package, new_package);
            }

            new_types.insert(new_key, msg_type);
        }
        self.types = new_types;
    }

    /// Rename package in a field type recursively.
    fn rename_field_type(field_type: &mut FieldType, old_package: &str, new_package: &str) {
        match field_type {
            FieldType::Nested(type_name) => {
                *type_name =
                    type_name.replace(&format!("{old_package}/"), &format!("{new_package}/"));
                *type_name =
                    type_name.replace(&format!("{old_package}::"), &format!("{new_package}::"));
            }
            FieldType::Array { base_type, .. } => {
                Self::rename_field_type(base_type, old_package, new_package);
            }
            FieldType::Primitive(_) => {}
        }
    }
}

impl MessageType {
    /// Create a new message type.
    pub fn new(name: String) -> Self {
        Self {
            name,
            fields: Vec::new(),
            max_alignment: 1,
        }
    }

    /// Add a field to this message type.
    pub fn add_field(&mut self, field: Field) {
        // Update max alignment
        let field_alignment = field.type_name.alignment();
        self.max_alignment = self.max_alignment.max(field_alignment);
        self.fields.push(field);
    }
}

/// Extract package name from a fully-qualified type name.
fn extract_package(name: &str) -> Option<String> {
    if name.contains('/') {
        let parts: Vec<&str> = name.split('/').collect();
        if parts.len() >= 2 {
            Some(parts[0].to_string())
        } else {
            None
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primitive_type_from_str() {
        assert_eq!(
            PrimitiveType::try_from_str("int32"),
            Some(PrimitiveType::Int32)
        );
        assert_eq!(
            PrimitiveType::try_from_str("float64"),
            Some(PrimitiveType::Float64)
        );
        assert_eq!(PrimitiveType::try_from_str("unknown"), None);
    }

    #[test]
    fn test_primitive_type_alignment() {
        assert_eq!(PrimitiveType::Bool.alignment(), 1);
        assert_eq!(PrimitiveType::Int16.alignment(), 2);
        assert_eq!(PrimitiveType::Int32.alignment(), 4);
        assert_eq!(PrimitiveType::Int64.alignment(), 8);
        assert_eq!(PrimitiveType::String.alignment(), 4);
    }

    #[test]
    fn test_field_type_is_complex() {
        assert!(!FieldType::Primitive(PrimitiveType::Int32).is_complex());
        assert!(FieldType::Primitive(PrimitiveType::String).is_complex());
        assert!(FieldType::Array {
            base_type: Box::new(FieldType::Primitive(PrimitiveType::Int32)),
            size: None,
        }
        .is_complex());
    }
}
