// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Schema transformation traits and types.
//!
//! This module provides the [`SchemaTransformer`] trait for encoding-agnostic
//! schema transformations, used by the rewriter to apply topic and type renames.

use std::collections::HashMap;

use crate::core::{CodecError, Encoding, Result};

// =============================================================================
// Schema Metadata
// =============================================================================

/// Unified schema metadata that works across all encoding formats.
///
/// This type wraps format-specific schema data and provides a common
/// interface for the codec system.
#[derive(Clone, Debug)]
pub enum SchemaMetadata {
    /// CDR/ROS2 text schema
    Cdr {
        /// Type name (e.g., "sensor_msgs/msg/JointState")
        type_name: String,
        /// Schema text (IDL/MSG format)
        schema_text: String,
    },
    /// Protobuf FileDescriptorSet
    Protobuf {
        /// Message type name (e.g., "nmx.msg.Lowdim")
        type_name: String,
        /// FileDescriptorSet binary data
        file_descriptor_set: Vec<u8>,
        /// Original schema text (for debugging/validation)
        schema_text: Option<String>,
    },
    /// JSON schema
    Json {
        /// Type name
        type_name: String,
        /// JSON schema
        schema_text: String,
    },
}

impl SchemaMetadata {
    /// Get the type name for this schema.
    pub fn type_name(&self) -> &str {
        match self {
            SchemaMetadata::Cdr { type_name, .. } => type_name,
            SchemaMetadata::Protobuf { type_name, .. } => type_name,
            SchemaMetadata::Json { type_name, .. } => type_name,
        }
    }

    /// Get the encoding for this schema.
    pub fn encoding(&self) -> Encoding {
        match self {
            SchemaMetadata::Cdr { .. } => Encoding::Cdr,
            SchemaMetadata::Protobuf { .. } => Encoding::Protobuf,
            SchemaMetadata::Json { .. } => Encoding::Json,
        }
    }

    /// Create CDR schema metadata.
    pub fn cdr(type_name: String, schema_text: String) -> Self {
        SchemaMetadata::Cdr {
            type_name,
            schema_text,
        }
    }

    /// Create Protobuf schema metadata.
    pub fn protobuf(type_name: String, file_descriptor_set: Vec<u8>) -> Self {
        SchemaMetadata::Protobuf {
            type_name,
            file_descriptor_set,
            schema_text: None,
        }
    }

    /// Create Protobuf schema metadata with optional schema text.
    pub fn protobuf_with_text(
        type_name: String,
        file_descriptor_set: Vec<u8>,
        schema_text: Option<String>,
    ) -> Self {
        SchemaMetadata::Protobuf {
            type_name,
            file_descriptor_set,
            schema_text,
        }
    }

    /// Create JSON schema metadata.
    pub fn json(type_name: String, schema_text: String) -> Self {
        SchemaMetadata::Json {
            type_name,
            schema_text,
        }
    }
}

// =============================================================================
// Schema Transformer Trait
// =============================================================================

/// Trait for transforming schemas between different formats or with renames.
///
/// This trait abstracts schema transformation logic, allowing the rewriter
/// to handle both text-based (ROS IDL) and binary (Protobuf FileDescriptorSet)
/// schemas through a common interface.
pub trait SchemaTransformer: Send + Sync {
    /// Transform a schema by applying package/type renames.
    ///
    /// # Arguments
    ///
    /// * `schema` - Input schema metadata
    /// * `type_mappings` - Map of old type names to new type names
    ///
    /// # Returns
    ///
    /// Transformed schema metadata
    fn transform(
        &self,
        schema: &SchemaMetadata,
        type_mappings: &HashMap<String, String>,
    ) -> Result<SchemaMetadata>;

    /// Get the encoding type this transformer handles.
    fn encoding(&self) -> Encoding;

    /// Check if this transformer can handle the given schema.
    fn can_handle(&self, schema: &SchemaMetadata) -> bool {
        schema.encoding() == self.encoding()
    }
}

// =============================================================================
// Transform Result
// =============================================================================

/// Result of a schema transformation operation.
#[derive(Debug, Clone)]
pub struct TransformResult {
    /// Transformed schema metadata
    pub schema: SchemaMetadata,
    /// Whether the schema was modified
    pub modified: bool,
    /// Types that were renamed
    pub renamed_types: Vec<(String, String)>,
}

impl TransformResult {
    /// Create a new transform result.
    pub fn new(schema: SchemaMetadata) -> Self {
        Self {
            schema,
            modified: false,
            renamed_types: Vec::new(),
        }
    }

    /// Create a modified transform result.
    pub fn modified(schema: SchemaMetadata, renamed_types: Vec<(String, String)>) -> Self {
        Self {
            schema,
            modified: true,
            renamed_types,
        }
    }

    /// Create an unmodified transform result.
    pub fn unmodified(schema: SchemaMetadata) -> Self {
        Self {
            schema,
            modified: false,
            renamed_types: Vec::new(),
        }
    }
}

// =============================================================================
// CDR Schema Transformer
// =============================================================================

/// Transformer for CDR/ROS2 text-based schemas.
///
/// Handles package renaming in ROS IDL/MSG format schemas.
pub struct CdrSchemaTransformer;

impl CdrSchemaTransformer {
    /// Create a new CDR schema transformer.
    pub fn new() -> Self {
        Self
    }

    /// Rewrite a CDR schema with type renames.
    ///
    /// # Arguments
    ///
    /// * `schema_text` - Original schema text
    /// * `old_type_name` - Old type name (e.g., "genie_msgs/msg/ArmState")
    /// * `new_type_name` - New type name (e.g., "archebase/msgs/ArmState")
    ///
    /// # Returns
    ///
    /// Rewritten schema text
    pub fn rewrite_schema(
        &self,
        schema_text: &str,
        old_type_name: &str,
        new_type_name: &str,
    ) -> String {
        if old_type_name.is_empty() || new_type_name.is_empty() || old_type_name == new_type_name {
            return schema_text.to_string();
        }

        // Extract the prefixes (everything except the message name)
        let old_prefix = Self::extract_type_prefix(old_type_name);
        let new_prefix = Self::extract_type_prefix(new_type_name);

        if old_prefix.is_empty() || new_prefix.is_empty() || old_prefix == new_prefix {
            return schema_text.to_string();
        }

        let mut result = schema_text.to_string();

        // Replace "old_pkg/msg/" patterns (ROS2 style in IDL: headers)
        // e.g., "genie_msgs/msg/" → "archebase/msgs/"
        result = result.replace(&old_prefix, &new_prefix);

        // Replace "old_pkg::msg::" patterns (IDL style)
        // Convert / to :: for IDL replacement
        let old_idl = old_prefix.replace('/', "::");
        let new_idl = new_prefix.replace('/', "::");
        result = result.replace(&old_idl, &new_idl);

        result
    }

    /// Extract the prefix from a type name (everything except the message name).
    ///
    /// For "sensor_msgs/msg/JointState" → "sensor_msgs/msg/"
    /// For "archebase/msgs/ArmState" → "archebase/msgs/"
    /// For "MessageType" → ""
    fn extract_type_prefix(type_name: &str) -> String {
        if let Some(last_slash) = type_name.rfind('/') {
            format!("{}/", &type_name[..last_slash])
        } else {
            String::new()
        }
    }
}

impl Default for CdrSchemaTransformer {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaTransformer for CdrSchemaTransformer {
    fn transform(
        &self,
        schema: &SchemaMetadata,
        type_mappings: &HashMap<String, String>,
    ) -> Result<SchemaMetadata> {
        match schema {
            SchemaMetadata::Cdr {
                type_name,
                schema_text,
            } => {
                // Check if this type needs transformation
                let new_type_name = type_mappings
                    .get(type_name)
                    .cloned()
                    .unwrap_or_else(|| type_name.clone());

                // Extract type prefixes for rewriting
                let old_prefix = Self::extract_type_prefix(type_name);
                let new_prefix = Self::extract_type_prefix(&new_type_name);

                // Rewrite schema if prefix changed
                let new_schema_text =
                    if !old_prefix.is_empty() && !new_prefix.is_empty() && old_prefix != new_prefix
                    {
                        self.rewrite_schema(
                            schema_text,
                            old_prefix.trim_end_matches('/'),
                            new_prefix.trim_end_matches('/'),
                        )
                    } else {
                        schema_text.clone()
                    };

                Ok(SchemaMetadata::Cdr {
                    type_name: new_type_name,
                    schema_text: new_schema_text,
                })
            }
            _ => Err(CodecError::invalid_schema(
                schema.type_name(),
                "Schema is not a CDR schema",
            )),
        }
    }

    fn encoding(&self) -> Encoding {
        Encoding::Cdr
    }
}

// =============================================================================
// Protobuf Schema Transformer
// =============================================================================

/// Transformer for Protobuf FileDescriptorSet schemas.
///
/// Handles package renaming in binary protobuf FileDescriptorSet data.
pub struct ProtobufSchemaTransformer;

impl ProtobufSchemaTransformer {
    /// Create a new Protobuf schema transformer.
    pub fn new() -> Self {
        Self
    }

    /// Transform a FileDescriptorSet by renaming packages.
    ///
    /// # Arguments
    ///
    /// * `fds_bytes` - FileDescriptorSet binary data
    /// * `old_package` - Old package name to replace
    /// * `new_package` - New package name
    ///
    /// # Returns
    ///
    /// Transformed FileDescriptorSet binary data
    pub fn transform_file_descriptor_set(
        &self,
        fds_bytes: &[u8],
        old_package: &str,
        new_package: &str,
    ) -> Result<Vec<u8>> {
        use prost::Message;
        use prost_types::FileDescriptorSet;

        if old_package.is_empty() || new_package.is_empty() || old_package == new_package {
            return Ok(fds_bytes.to_vec());
        }

        // Decode FileDescriptorSet
        let mut fds = FileDescriptorSet::decode(fds_bytes).map_err(|e| {
            CodecError::parse(
                "protobuf",
                format!("Failed to decode FileDescriptorSet: {e}"),
            )
        })?;

        // Transform each file
        for file in &mut fds.file {
            // Update package declaration
            if file.package.as_deref() == Some(old_package) {
                file.package = Some(new_package.to_string());
            }

            // Update type references in messages
            for message_type in &mut file.message_type {
                self.update_message_type_references(message_type, old_package, new_package);
            }

            // Update enum references
            for enum_type in &mut file.enum_type {
                self.update_enum_type_references(enum_type, old_package, new_package);
            }

            // Update service references
            for service in &mut file.service {
                self.update_service_references(service, old_package, new_package);
            }
        }

        // Re-encode FileDescriptorSet
        let mut buffer = Vec::new();
        fds.encode(&mut buffer).map_err(|e| {
            CodecError::encode(
                "protobuf",
                format!("Failed to encode FileDescriptorSet: {e}"),
            )
        })?;

        Ok(buffer)
    }

    /// Update type references in a message descriptor.
    #[allow(clippy::only_used_in_recursion)]
    fn update_message_type_references(
        &self,
        message_type: &mut prost_types::DescriptorProto,
        old_package: &str,
        new_package: &str,
    ) {
        // Update nested message types
        for nested_type in &mut message_type.nested_type {
            self.update_message_type_references(nested_type, old_package, new_package);
        }

        // Update field type references
        for field in &mut message_type.field {
            if let Some(type_name) = &field.type_name {
                if type_name.starts_with(".") {
                    // Fully qualified type name (e.g., ".old_pkg.Message")
                    let new_type_name = type_name.replacen(
                        &format!(".{old_package}"),
                        &format!(".{new_package}"),
                        1,
                    );
                    field.type_name = Some(new_type_name);
                }
            }
        }
    }

    /// Update type references in an enum descriptor.
    fn update_enum_type_references(
        &self,
        _enum_type: &mut prost_types::EnumDescriptorProto,
        _old_package: &str,
        _new_package: &str,
    ) {
        // Enums typically don't have cross-references
        // EnumDescriptorProto doesn't have nested types
    }

    /// Update type references in a service descriptor.
    fn update_service_references(
        &self,
        service: &mut prost_types::ServiceDescriptorProto,
        old_package: &str,
        new_package: &str,
    ) {
        // Update method input/output types
        for method in &mut service.method {
            if let Some(input_type) = &method.input_type {
                let new_type =
                    input_type.replacen(&format!(".{old_package}"), &format!(".{new_package}"), 1);
                method.input_type = Some(new_type);
            }
            if let Some(output_type) = &method.output_type {
                let new_type =
                    output_type.replacen(&format!(".{old_package}"), &format!(".{new_package}"), 1);
                method.output_type = Some(new_type);
            }
        }
    }

    /// Rename a message type within a FileDescriptorSet.
    ///
    /// This renames the message type definition and updates all references to it
    /// throughout the FileDescriptorSet.
    ///
    /// # Arguments
    ///
    /// * `fds_bytes` - FileDescriptorSet binary data
    /// * `old_message_name` - Old message name (e.g., "LowdimData")
    /// * `new_message_name` - New message name (e.g., "JointStates")
    /// * `package` - Package name for context (e.g., "nmx.msg")
    ///
    /// # Returns
    ///
    /// Transformed FileDescriptorSet binary data with the message renamed
    pub fn rename_message_type_in_fds(
        &self,
        fds_bytes: &[u8],
        old_message_name: &str,
        new_message_name: &str,
        package: &str,
    ) -> Result<Vec<u8>> {
        use prost::Message;
        use prost_types::FileDescriptorSet;

        if old_message_name.is_empty()
            || new_message_name.is_empty()
            || old_message_name == new_message_name
        {
            return Ok(fds_bytes.to_vec());
        }

        // Decode FileDescriptorSet
        let mut fds = FileDescriptorSet::decode(fds_bytes).map_err(|e| {
            CodecError::parse(
                "protobuf",
                format!("Failed to decode FileDescriptorSet: {e}"),
            )
        })?;

        // Build fully qualified type names
        let old_fully_qualified = if package.is_empty() {
            format!(".{old_message_name}")
        } else {
            format!(".{package}.{old_message_name}")
        };
        let new_fully_qualified = if package.is_empty() {
            format!(".{new_message_name}")
        } else {
            format!(".{package}.{new_message_name}")
        };

        // Transform each file
        for file in &mut fds.file {
            // Rename the message type definition
            for message_type in &mut file.message_type {
                if message_type.name.as_deref() == Some(old_message_name) {
                    message_type.name = Some(new_message_name.to_string());
                }
                // Update nested message type names (handles nested messages)
                self.rename_nested_message_types(message_type, old_message_name, new_message_name);
            }

            // Update references to the renamed message type in all message fields
            for message_type in &mut file.message_type {
                self.update_message_type_name_references(
                    message_type,
                    &old_fully_qualified,
                    &new_fully_qualified,
                );
            }

            // Update enum references (unlikely but possible for enum values referencing messages)
            for enum_type in &mut file.enum_type {
                // Enum values might reference the renamed message in options
                self.update_enum_name_references(
                    enum_type,
                    &old_fully_qualified,
                    &new_fully_qualified,
                );
            }

            // Update service references
            for service in &mut file.service {
                self.update_service_name_references(
                    service,
                    &old_fully_qualified,
                    &new_fully_qualified,
                );
            }
        }

        // Re-encode FileDescriptorSet
        let mut buffer = Vec::new();
        fds.encode(&mut buffer).map_err(|e| {
            CodecError::encode(
                "protobuf",
                format!("Failed to encode FileDescriptorSet: {e}"),
            )
        })?;

        Ok(buffer)
    }

    /// Recursively rename nested message types.
    #[allow(clippy::only_used_in_recursion)]
    fn rename_nested_message_types(
        &self,
        message_type: &mut prost_types::DescriptorProto,
        old_name: &str,
        new_name: &str,
    ) {
        for nested_type in &mut message_type.nested_type {
            if nested_type.name.as_deref() == Some(old_name) {
                nested_type.name = Some(new_name.to_string());
            }
            // Recursively handle deeper nesting
            self.rename_nested_message_types(nested_type, old_name, new_name);
        }
    }

    /// Update message type name references (for message type renaming).
    #[allow(clippy::only_used_in_recursion)]
    fn update_message_type_name_references(
        &self,
        message_type: &mut prost_types::DescriptorProto,
        old_fully_qualified: &str,
        new_fully_qualified: &str,
    ) {
        // Update field type references
        for field in &mut message_type.field {
            if let Some(type_name) = &field.type_name {
                if type_name == old_fully_qualified {
                    field.type_name = Some(new_fully_qualified.to_string());
                }
            }
        }

        // Recursively update nested messages
        for nested_type in &mut message_type.nested_type {
            self.update_message_type_name_references(
                nested_type,
                old_fully_qualified,
                new_fully_qualified,
            );
        }
    }

    /// Update enum name references (for message type renaming).
    fn update_enum_name_references(
        &self,
        _enum_type: &mut prost_types::EnumDescriptorProto,
        _old_fully_qualified: &str,
        _new_fully_qualified: &str,
    ) {
        // Enum descriptors typically don't reference message types directly
        // This is a placeholder for potential edge cases with custom options
    }

    /// Update service name references (for message type renaming).
    fn update_service_name_references(
        &self,
        service: &mut prost_types::ServiceDescriptorProto,
        old_fully_qualified: &str,
        new_fully_qualified: &str,
    ) {
        for method in &mut service.method {
            if let Some(input_type) = &method.input_type {
                if input_type == old_fully_qualified {
                    method.input_type = Some(new_fully_qualified.to_string());
                }
            }
            if let Some(output_type) = &method.output_type {
                if output_type == old_fully_qualified {
                    method.output_type = Some(new_fully_qualified.to_string());
                }
            }
        }
    }

    /// Extract package name from a protobuf type name.
    ///
    /// # Arguments
    ///
    /// * `type_name` - Full type name (e.g., "nmx.msg.Lowdim" or ".nmx.msg.Lowdim")
    ///
    /// # Returns
    ///
    /// Package name (e.g., "nmx.msg")
    pub fn extract_package(type_name: &str) -> Option<String> {
        // Remove leading dot if present
        let name = type_name.strip_prefix('.').unwrap_or(type_name);

        // For "pkg.msg.Type" or "pkg.Type", extract "pkg.msg" or "pkg"
        let parts: Vec<&str> = name.split('.').collect();
        if parts.len() >= 2 {
            // Return everything except the last part (the type name)
            Some(parts[..parts.len() - 1].join("."))
        } else {
            None
        }
    }
}

impl Default for ProtobufSchemaTransformer {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaTransformer for ProtobufSchemaTransformer {
    fn transform(
        &self,
        schema: &SchemaMetadata,
        type_mappings: &HashMap<String, String>,
    ) -> Result<SchemaMetadata> {
        match schema {
            SchemaMetadata::Protobuf {
                type_name,
                file_descriptor_set,
                schema_text,
            } => {
                // Check if this type needs transformation
                let new_type_name = type_mappings
                    .get(type_name)
                    .cloned()
                    .unwrap_or_else(|| type_name.clone());

                // Extract package names
                let old_package = Self::extract_package(type_name).unwrap_or_default();
                let new_package = Self::extract_package(&new_type_name).unwrap_or_default();

                // Transform FileDescriptorSet if package changed
                let new_fds = if !old_package.is_empty()
                    && !new_package.is_empty()
                    && old_package != new_package
                {
                    self.transform_file_descriptor_set(
                        file_descriptor_set,
                        &old_package,
                        &new_package,
                    )?
                } else {
                    file_descriptor_set.clone()
                };

                Ok(SchemaMetadata::Protobuf {
                    type_name: new_type_name,
                    file_descriptor_set: new_fds,
                    schema_text: schema_text.clone(),
                })
            }
            _ => Err(CodecError::invalid_schema(
                schema.type_name(),
                "Schema is not a Protobuf schema",
            )),
        }
    }

    fn encoding(&self) -> Encoding {
        Encoding::Protobuf
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // ========================================================================
    // SchemaMetadata Tests
    // ========================================================================

    #[test]
    fn test_schema_metadata_cdr_type_name() {
        let schema = SchemaMetadata::cdr("std_msgs/String".to_string(), "string data".to_string());
        assert_eq!(schema.type_name(), "std_msgs/String");
    }

    #[test]
    fn test_schema_metadata_cdr_encoding() {
        let schema = SchemaMetadata::cdr("std_msgs/String".to_string(), "string data".to_string());
        assert_eq!(schema.encoding(), Encoding::Cdr);
    }

    #[test]
    fn test_schema_metadata_protobuf_type_name() {
        let schema = SchemaMetadata::protobuf("nmx.msg.Lowdim".to_string(), vec![1, 2, 3]);
        assert_eq!(schema.type_name(), "nmx.msg.Lowdim");
    }

    #[test]
    fn test_schema_metadata_protobuf_encoding() {
        let schema = SchemaMetadata::protobuf("nmx.msg.Lowdim".to_string(), vec![1, 2, 3]);
        assert_eq!(schema.encoding(), Encoding::Protobuf);
    }

    #[test]
    fn test_schema_metadata_json_type_name() {
        let schema = SchemaMetadata::json("MyType".to_string(), "{}".to_string());
        assert_eq!(schema.type_name(), "MyType");
    }

    #[test]
    fn test_schema_metadata_json_encoding() {
        let schema = SchemaMetadata::json("MyType".to_string(), "{}".to_string());
        assert_eq!(schema.encoding(), Encoding::Json);
    }

    #[test]
    fn test_schema_metadata_cdr_constructor() {
        let schema = SchemaMetadata::cdr("foo/Msg".to_string(), "int32 value".to_string());
        match schema {
            SchemaMetadata::Cdr {
                type_name,
                schema_text,
            } => {
                assert_eq!(type_name, "foo/Msg");
                assert_eq!(schema_text, "int32 value");
            }
            _ => panic!("Expected CDR variant"),
        }
    }

    #[test]
    fn test_schema_metadata_protobuf_constructor() {
        let fds = vec![0x08, 0x01];
        let schema = SchemaMetadata::protobuf("bar/Msg".to_string(), fds.clone());
        match schema {
            SchemaMetadata::Protobuf {
                type_name,
                file_descriptor_set,
                schema_text,
            } => {
                assert_eq!(type_name, "bar/Msg");
                assert_eq!(file_descriptor_set, fds);
                assert!(schema_text.is_none());
            }
            _ => panic!("Expected Protobuf variant"),
        }
    }

    #[test]
    fn test_schema_metadata_protobuf_with_text_constructor() {
        let fds = vec![0x08, 0x01];
        let text = Some("message Msg {}".to_string());
        let schema =
            SchemaMetadata::protobuf_with_text("baz/Msg".to_string(), fds.clone(), text.clone());
        match schema {
            SchemaMetadata::Protobuf {
                type_name,
                file_descriptor_set,
                schema_text,
            } => {
                assert_eq!(type_name, "baz/Msg");
                assert_eq!(file_descriptor_set, fds);
                assert_eq!(schema_text, text);
            }
            _ => panic!("Expected Protobuf variant"),
        }
    }

    #[test]
    fn test_schema_metadata_json_constructor() {
        let schema =
            SchemaMetadata::json("pux/Type".to_string(), "{\"type\": \"object\"}".to_string());
        match schema {
            SchemaMetadata::Json {
                type_name,
                schema_text,
            } => {
                assert_eq!(type_name, "pux/Type");
                assert_eq!(schema_text, "{\"type\": \"object\"}");
            }
            _ => panic!("Expected Json variant"),
        }
    }

    // ========================================================================
    // TransformResult Tests
    // ========================================================================

    #[test]
    fn test_transform_result_new() {
        let schema = SchemaMetadata::cdr("test/Msg".to_string(), "int32 value".to_string());
        let result = TransformResult::new(schema.clone());
        assert!(!result.modified);
        assert!(result.renamed_types.is_empty());
    }

    #[test]
    fn test_transform_result_modified() {
        let schema = SchemaMetadata::cdr("new/Msg".to_string(), "int32 value".to_string());
        let renamed = vec![("old/Msg".to_string(), "new/Msg".to_string())];
        let result = TransformResult::modified(schema.clone(), renamed.clone());
        assert!(result.modified);
        assert_eq!(result.renamed_types, renamed);
    }

    #[test]
    fn test_transform_result_unmodified() {
        let schema = SchemaMetadata::cdr("test/Msg".to_string(), "int32 value".to_string());
        let result = TransformResult::unmodified(schema.clone());
        assert!(!result.modified);
        assert!(result.renamed_types.is_empty());
    }

    // ========================================================================
    // CdrSchemaTransformer Tests
    // ========================================================================

    #[test]
    fn test_cdr_transformer_new() {
        let transformer = CdrSchemaTransformer::new();
        assert_eq!(transformer.encoding(), Encoding::Cdr);
    }

    #[test]
    fn test_cdr_transformer_default() {
        let transformer = CdrSchemaTransformer;
        assert_eq!(transformer.encoding(), Encoding::Cdr);
    }

    #[test]
    fn test_cdr_transformer_extract_type_prefix() {
        assert_eq!(
            CdrSchemaTransformer::extract_type_prefix("sensor_msgs/msg/JointState"),
            "sensor_msgs/msg/"
        );
        assert_eq!(
            CdrSchemaTransformer::extract_type_prefix("std_msgs/Header"),
            "std_msgs/"
        );
        assert_eq!(CdrSchemaTransformer::extract_type_prefix("MessageType"), "");
        assert_eq!(
            CdrSchemaTransformer::extract_type_prefix("foo/bar/baz/Type"),
            "foo/bar/baz/"
        );
    }

    #[test]
    fn test_cdr_transformer_rewrite_schema_no_change() {
        let transformer = CdrSchemaTransformer::new();
        let schema = "int32 value\nfloat64 data";

        // Same type name - no change
        let rewritten = transformer.rewrite_schema(schema, "std_msgs/String", "std_msgs/String");
        assert_eq!(rewritten, schema);
    }

    #[test]
    fn test_cdr_transformer_rewrite_schema_empty_names() {
        let transformer = CdrSchemaTransformer::new();
        let schema = "int32 value\nfloat64 data";

        // Empty names - no change
        let rewritten = transformer.rewrite_schema(schema, "", "");
        assert_eq!(rewritten, schema);
    }

    #[test]
    fn test_cdr_transformer_rewrite_schema_ros2_style() {
        let transformer = CdrSchemaTransformer::new();
        let schema = "sensor_msgs/msg/Header header\nsensor_msgs/msg/String string\n";

        // Use full type names with message suffix to get proper prefix extraction
        let rewritten =
            transformer.rewrite_schema(schema, "sensor_msgs/msg/Header", "my_msgs/msg/Header");
        assert!(rewritten.contains("my_msgs/msg/Header header"));
        assert!(rewritten.contains("my_msgs/msg/String string"));
        assert!(!rewritten.contains("sensor_msgs/msg/"));
    }

    #[test]
    fn test_cdr_transformer_rewrite_schema_idl_style() {
        let transformer = CdrSchemaTransformer::new();
        let schema = "sequence<sensor_msgs::msg::JointState> joints";

        let rewritten = transformer.rewrite_schema(schema, "sensor_msgs/msg", "my_msgs/msg");
        assert!(rewritten.contains("sequence<my_msgs::msg::JointState>"));
    }

    #[test]
    fn test_cdr_transformer_can_handle() {
        let transformer = CdrSchemaTransformer::new();
        let cdr_schema = SchemaMetadata::cdr("test/Msg".to_string(), "int32 value".to_string());
        let protobuf_schema = SchemaMetadata::protobuf("test/Msg".to_string(), vec![]);

        assert!(transformer.can_handle(&cdr_schema));
        assert!(!transformer.can_handle(&protobuf_schema));
    }

    #[test]
    fn test_cdr_transformer_transform_no_mapping() {
        let transformer = CdrSchemaTransformer::new();
        let schema = SchemaMetadata::cdr("std_msgs/String".to_string(), "string data".to_string());
        let mappings = HashMap::new();

        let result = transformer.transform(&schema, &mappings);
        assert!(result.is_ok());
        let transformed = result.unwrap();
        assert_eq!(transformed.type_name(), "std_msgs/String");
    }

    #[test]
    fn test_cdr_transformer_transform_with_mapping() {
        let transformer = CdrSchemaTransformer::new();
        let schema = SchemaMetadata::cdr("old_pkg/String".to_string(), "string data".to_string());
        let mut mappings = HashMap::new();
        mappings.insert("old_pkg/String".to_string(), "new_pkg/String".to_string());

        let result = transformer.transform(&schema, &mappings);
        assert!(result.is_ok());
        let transformed = result.unwrap();
        assert_eq!(transformed.type_name(), "new_pkg/String");
    }

    #[test]
    fn test_cdr_transformer_transform_non_cdr_schema() {
        let transformer = CdrSchemaTransformer::new();
        let schema = SchemaMetadata::protobuf("test/Msg".to_string(), vec![]);
        let mappings = HashMap::new();

        let result = transformer.transform(&schema, &mappings);
        assert!(result.is_err());
    }

    // ========================================================================
    // ProtobufSchemaTransformer Tests
    // ========================================================================

    #[test]
    fn test_protobuf_transformer_new() {
        let transformer = ProtobufSchemaTransformer::new();
        assert_eq!(transformer.encoding(), Encoding::Protobuf);
    }

    #[test]
    fn test_protobuf_transformer_default() {
        let transformer = ProtobufSchemaTransformer;
        assert_eq!(transformer.encoding(), Encoding::Protobuf);
    }

    #[test]
    fn test_protobuf_transformer_extract_package() {
        assert_eq!(
            ProtobufSchemaTransformer::extract_package("nmx.msg.Lowdim"),
            Some("nmx.msg".to_string())
        );
        assert_eq!(
            ProtobufSchemaTransformer::extract_package(".nmx.msg.Lowdim"),
            Some("nmx.msg".to_string())
        );
        assert_eq!(
            ProtobufSchemaTransformer::extract_package("MessageType"),
            None
        );
        assert_eq!(
            ProtobufSchemaTransformer::extract_package("pkg.subpkg.Type"),
            Some("pkg.subpkg".to_string())
        );
    }

    #[test]
    fn test_protobuf_transformer_can_handle() {
        let transformer = ProtobufSchemaTransformer::new();
        let protobuf_schema = SchemaMetadata::protobuf("test/Msg".to_string(), vec![]);
        let cdr_schema = SchemaMetadata::cdr("test/Msg".to_string(), "int32 value".to_string());

        assert!(transformer.can_handle(&protobuf_schema));
        assert!(!transformer.can_handle(&cdr_schema));
    }

    #[test]
    fn test_protobuf_transformer_no_change_needed() {
        let transformer = ProtobufSchemaTransformer::new();

        // Same package - should return unchanged
        let fds = vec![0x08, 0x01, 0x10, 0x02];
        let result = transformer
            .transform_file_descriptor_set(&fds, "same", "same")
            .unwrap();
        assert_eq!(result, fds);

        // Empty packages - should return unchanged
        let result = transformer
            .transform_file_descriptor_set(&fds, "", "")
            .unwrap();
        assert_eq!(result, fds);
    }

    #[test]
    fn test_protobuf_transformer_transform_no_mapping() {
        let transformer = ProtobufSchemaTransformer::new();
        let schema = SchemaMetadata::protobuf("nmx.msg.Lowdim".to_string(), vec![1, 2, 3]);
        let mappings = HashMap::new();

        let result = transformer.transform(&schema, &mappings);
        assert!(result.is_ok());
        let transformed = result.unwrap();
        assert_eq!(transformed.type_name(), "nmx.msg.Lowdim");
    }

    #[test]
    fn test_protobuf_transformer_transform_non_protobuf_schema() {
        let transformer = ProtobufSchemaTransformer::new();
        let schema = SchemaMetadata::cdr("test/Msg".to_string(), "int32 value".to_string());
        let mappings = HashMap::new();

        let result = transformer.transform(&schema, &mappings);
        assert!(result.is_err());
    }

    #[test]
    fn test_protobuf_transformer_rename_message_no_change() {
        let transformer = ProtobufSchemaTransformer::new();
        let fds = vec![0x08, 0x01, 0x10, 0x02];

        // Same names - no change
        let result = transformer
            .rename_message_type_in_fds(&fds, "OldMsg", "OldMsg", "pkg")
            .unwrap();
        assert_eq!(result, fds);

        // Empty names - no change
        let result = transformer
            .rename_message_type_in_fds(&fds, "", "NewMsg", "pkg")
            .unwrap();
        assert_eq!(result, fds);
    }

    // ========================================================================
    // Encoding Enum Tests
    // ========================================================================

    #[test]
    fn test_encoding_cdr_is_cdr() {
        assert!(Encoding::Cdr.is_cdr());
        assert!(!Encoding::Cdr.is_protobuf());
        assert!(!Encoding::Cdr.is_json());
    }

    #[test]
    fn test_encoding_protobuf_is_protobuf() {
        assert!(!Encoding::Protobuf.is_cdr());
        assert!(Encoding::Protobuf.is_protobuf());
        assert!(!Encoding::Protobuf.is_json());
    }

    #[test]
    fn test_encoding_json_is_json() {
        assert!(!Encoding::Json.is_cdr());
        assert!(!Encoding::Json.is_protobuf());
        assert!(Encoding::Json.is_json());
    }

    #[test]
    fn test_encoding_as_str() {
        assert_eq!(Encoding::Cdr.as_str(), "cdr");
        assert_eq!(Encoding::Protobuf.as_str(), "protobuf");
        assert_eq!(Encoding::Json.as_str(), "json");
    }
}
