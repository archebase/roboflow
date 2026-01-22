// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! MCAP rewrite engine orchestrator.
//!
//! This module provides the [`McapRewriteEngine`] which coordinates
//! message decoding, schema transformation, and re-encoding for the
//! MCAP rewriter.

use std::collections::HashMap;

use crate::core::Result;
use crate::encoding::{CodecFactory, ProtobufSchemaTransformer, SchemaMetadata};
use crate::io::formats::mcap::reader::{ChannelInfo, McapReader, RawMessage};
use crate::transform::MultiTransform;

/// Extract package name from a protobuf type name.
///
/// # Arguments
///
/// * `type_name` - Full type name (e.g., "pkg.foo.Bar" or ".pkg.foo.Bar")
///
/// # Returns
///
/// Package name (e.g., "pkg.foo") or empty string if not found
fn extract_protobuf_package(type_name: &str) -> String {
    // Remove leading dot if present
    let name = type_name.strip_prefix('.').unwrap_or(type_name);

    // For "pkg.foo.Bar" or "pkg.Bar", extract "pkg.foo" or "pkg"
    let parts: Vec<&str> = name.split('.').collect();
    if parts.len() >= 2 {
        // Return everything except the last part (the message name)
        parts[..parts.len() - 1].join(".")
    } else {
        String::new()
    }
}

/// Extract message name from a protobuf type name.
///
/// # Arguments
///
/// * `type_name` - Full type name (e.g., "pkg.foo.Bar" or ".pkg.foo.Bar")
///
/// # Returns
///
/// Message name (e.g., "Bar") or empty string if not found
fn extract_protobuf_message_name(type_name: &str) -> String {
    // Remove leading dot if present
    let name = type_name.strip_prefix('.').unwrap_or(type_name);

    // For "pkg.foo.Bar", extract "Bar"
    let parts: Vec<&str> = name.split('.').collect();
    if !parts.is_empty() {
        parts.last().unwrap_or(&"").to_string()
    } else {
        String::new()
    }
}

/// Validate that a protobuf message name is valid.
///
/// Protobuf message names must be valid identifiers:
/// - Start with a letter or underscore
/// - Contain only letters, digits, and underscores
///
/// # Arguments
///
/// * `message_name` - The message name to validate
/// * `full_type_name` - The full type name (for error reporting)
///
/// # Returns
///
/// Ok(()) if valid, Err with details if invalid
fn validate_protobuf_message_name(message_name: &str, full_type_name: &str) -> Result<()> {
    if message_name.is_empty() {
        return Err(crate::core::CodecError::invalid_schema(
            full_type_name,
            "Message name cannot be empty",
        ));
    }

    // Check first character is letter or underscore
    let first_char = message_name.chars().next().unwrap();
    if !first_char.is_alphabetic() && first_char != '_' {
        return Err(crate::core::CodecError::invalid_schema(
            full_type_name,
            format!(
                "Invalid protobuf message name '{full_type_name}': must start with a letter or underscore, found '{first_char}'"
            ),
        ));
    }

    // Check all characters are valid (letters, digits, underscore only)
    for (idx, ch) in message_name.chars().enumerate() {
        if !ch.is_alphanumeric() && ch != '_' {
            return Err(crate::core::CodecError::invalid_schema(
                full_type_name,
                format!(
                    "Invalid protobuf message name '{full_type_name}': character '{ch}' at position {idx} is not allowed (only letters, digits, and underscore '_' are allowed in protobuf message names)"
                ),
            ));
        }
    }

    Ok(())
}

/// Statistics from MCAP message rewriting operations.
#[derive(Debug, Clone, Default)]
pub struct McapRewriteStats {
    /// Total messages processed
    pub message_count: u64,
    /// Messages successfully re-encoded
    pub reencoded_count: u64,
    /// Messages passed through without re-encoding
    pub passthrough_count: u64,
    /// Messages that failed to decode
    pub decode_failures: u64,
    /// Messages that failed to encode
    pub encode_failures: u64,
    /// Number of topics renamed
    pub topics_renamed: u64,
    /// Number of types renamed
    pub types_renamed: u64,
}

/// Message rewrite engine that orchestrates decode-transform-encode flow.
///
/// This engine:
/// 1. Detects message encoding from channel metadata
/// 2. Selects appropriate codec (CDR, Protobuf, JSON)
/// 3. Applies schema transformations from the pipeline
/// 4. Decodes and re-encodes messages
/// 5. Writes output to the MCAP writer
pub struct McapRewriteEngine {
    /// Codec factory for creating codec instances
    codec_factory: CodecFactory,
    /// Statistics
    stats: McapRewriteStats,
    /// Cached schemas indexed by transformed type name
    schemas: HashMap<String, SchemaMetadata>,
    /// Original schema data indexed by channel ID
    channel_schemas: HashMap<u16, SchemaMetadata>,
    /// Transformed channel topics
    channel_topics: HashMap<u16, String>,
}

impl McapRewriteEngine {
    /// Create a new message rewrite engine.
    pub fn new() -> Self {
        Self {
            codec_factory: CodecFactory::new(),
            stats: McapRewriteStats::default(),
            schemas: HashMap::new(),
            channel_schemas: HashMap::new(),
            channel_topics: HashMap::new(),
        }
    }

    /// Get the current statistics.
    pub fn stats(&self) -> &McapRewriteStats {
        &self.stats
    }

    /// Take the statistics, resetting the internal counter.
    pub fn take_stats(&mut self) -> McapRewriteStats {
        std::mem::take(&mut self.stats)
    }

    /// Get the number of schemas prepared.
    pub fn schema_count(&self) -> usize {
        self.schemas.len()
    }

    /// Prepare schemas for rewriting.
    ///
    /// This phase:
    /// 1. Parses all schemas from the input MCAP
    /// 2. Applies transformations from the pipeline (including topic-specific type transforms)
    /// 3. Caches the transformed schemas for use during message processing
    ///
    /// # Arguments
    ///
    /// * `reader` - The MCAP reader
    /// * `pipeline` - Optional transform pipeline to apply
    pub fn prepare_schemas(
        &mut self,
        reader: &McapReader,
        pipeline: Option<&MultiTransform>,
    ) -> Result<()> {
        for (channel_id, channel) in reader.channels() {
            // Detect encoding
            let encoding = self
                .codec_factory
                .detect_encoding(&channel.encoding, channel.schema_encoding.as_deref());

            // Create schema metadata from channel info
            let schema = self.create_schema_metadata(channel, &encoding)?;

            // Store original schema for this channel (before transformation)
            self.channel_schemas.insert(*channel_id, schema.clone());

            // Apply transformations with topic context if pipeline provided
            let transformed_schema = if let Some(p) = pipeline {
                self.apply_transformations_with_topic(schema, &channel.topic, p)?
            } else {
                schema
            };

            // Cache by channel_id to support topic-specific type transforms
            // (same source type can map to different target types based on topic)
            self.schemas
                .insert(channel_id.to_string(), transformed_schema);

            // Store transformed topic
            let transformed_topic = if let Some(p) = pipeline {
                p.transform_topic(&channel.topic)
                    .unwrap_or_else(|| channel.topic.clone())
            } else {
                channel.topic.clone()
            };
            self.channel_topics.insert(*channel_id, transformed_topic);

            // Track transformation statistics using topic context
            if let Some(p) = pipeline {
                let original_topic = &channel.topic;
                let original_type = &channel.message_type;

                if p.transform_topic(original_topic).as_deref() != Some(original_topic) {
                    self.stats.topics_renamed += 1;
                }

                // Use topic-aware type transformation for accurate statistics
                let (transformed_type, _) =
                    p.transform_type_with_topic(original_topic, original_type, None);
                if transformed_type != *original_type {
                    self.stats.types_renamed += 1;
                }
            }
        }

        Ok(())
    }

    /// Create schema metadata from channel information.
    fn create_schema_metadata(
        &self,
        channel: &ChannelInfo,
        encoding: &crate::Encoding,
    ) -> Result<SchemaMetadata> {
        match encoding {
            crate::Encoding::Cdr => {
                let schema_text = channel
                    .schema
                    .as_ref()
                    .ok_or_else(|| {
                        crate::core::CodecError::invalid_schema(
                            &channel.message_type,
                            "No schema data for CDR channel",
                        )
                    })?
                    .clone();
                Ok(SchemaMetadata::cdr(
                    channel.message_type.clone(),
                    schema_text,
                ))
            }
            crate::Encoding::Protobuf => {
                let schema_data = channel
                    .schema_data
                    .as_ref()
                    .ok_or_else(|| {
                        crate::core::CodecError::invalid_schema(
                            &channel.message_type,
                            "No schema data for protobuf channel",
                        )
                    })?
                    .clone();
                Ok(SchemaMetadata::protobuf(
                    channel.message_type.clone(),
                    schema_data,
                ))
            }
            crate::Encoding::Json => {
                let schema_text = channel
                    .schema
                    .as_ref()
                    .ok_or_else(|| {
                        crate::core::CodecError::invalid_schema(
                            &channel.message_type,
                            "No schema data for JSON channel",
                        )
                    })?
                    .clone();
                Ok(SchemaMetadata::json(
                    channel.message_type.clone(),
                    schema_text,
                ))
            }
        }
    }

    /// Apply transformations to a schema with topic context.
    ///
    /// This applies type transformations that may be topic-specific,
    /// allowing the same source type to map to different target types
    /// based on the channel topic.
    fn apply_transformations_with_topic(
        &self,
        schema: SchemaMetadata,
        topic: &str,
        pipeline: &MultiTransform,
    ) -> Result<SchemaMetadata> {
        // Get the original schema text based on the variant type
        let original_schema_text = match &schema {
            SchemaMetadata::Cdr { schema_text, .. } => Some(schema_text.as_str()),
            SchemaMetadata::Protobuf { schema_text, .. } => schema_text.as_deref(),
            SchemaMetadata::Json { schema_text, .. } => Some(schema_text.as_str()),
        };

        // Apply topic-aware type transformation
        let (new_type_name, new_schema_text) =
            pipeline.transform_type_with_topic(topic, schema.type_name(), original_schema_text);

        // If type didn't change, return original schema
        let original_type_name = schema.type_name().to_string();
        if new_type_name == original_type_name && new_schema_text.is_none() {
            return Ok(schema);
        }

        // Create a new schema with the transformed type name and schema text
        match schema {
            SchemaMetadata::Cdr { .. } => {
                let text = new_schema_text
                    .or_else(|| original_schema_text.map(|s| s.to_string()))
                    .unwrap_or_default();
                Ok(SchemaMetadata::cdr(new_type_name, text))
            }
            SchemaMetadata::Protobuf {
                type_name: _,
                file_descriptor_set,
                ref schema_text,
            } => {
                // For protobuf, we need to transform the FileDescriptorSet
                // to update package names and/or message type names
                let transformer = ProtobufSchemaTransformer::new();

                // Extract package and message names from type names
                // Format: "pkg.foo.Bar" -> package: "pkg.foo", message: "Bar"
                let old_package = extract_protobuf_package(&original_type_name);
                let new_package = extract_protobuf_package(&new_type_name);
                let old_message_name = extract_protobuf_message_name(&original_type_name);
                let new_message_name = extract_protobuf_message_name(&new_type_name);

                // Detect ambiguous type transformations
                // If target has more dot-separated elements than source, it's ambiguous
                // Example: "pkg.foo.Bar" -> "pkg.foo.baz.qux"
                // The intent could be:
                //   - package: "pkg.foo", message: "baz.qux" (contains . - invalid)
                //   - package: "pkg.foo.baz", message: "qux" (changes package structure)
                // Both are problematic, so we require the user to clarify
                let old_element_count = original_type_name.split('.').count();
                let new_element_count = new_type_name.split('.').count();
                if new_element_count > old_element_count && !old_package.is_empty() {
                    return Err(crate::core::CodecError::invalid_schema(
                        &new_type_name,
                        format!(
                            "Ambiguous protobuf type transformation: '{}' has more dot-separated elements than '{}'. \
                            Please clarify: use '_' instead of '.' in message names (e.g., 'pkg.foo.baz_qux'), \
                            or use a format that doesn't change the package structure.",
                            &new_type_name, &original_type_name
                        ),
                    ));
                }

                // Validate the new message name is valid for protobuf
                // (must only contain letters, digits, and underscore)
                if !new_message_name.is_empty() && new_message_name != old_message_name {
                    validate_protobuf_message_name(&new_message_name, &new_type_name)?;
                }

                // Use the common package for renaming operations
                let common_package = if !old_package.is_empty() {
                    &old_package
                } else if !new_package.is_empty() {
                    &new_package
                } else {
                    &String::new()
                };

                // Step 1: Transform package if changed
                let transformed_fds = if !old_package.is_empty()
                    && !new_package.is_empty()
                    && old_package != new_package
                {
                    transformer.transform_file_descriptor_set(
                        &file_descriptor_set,
                        &old_package,
                        &new_package,
                    )?
                } else {
                    file_descriptor_set
                };

                // Step 2: Rename message type if changed
                let transformed_fds = if !old_message_name.is_empty()
                    && !new_message_name.is_empty()
                    && old_message_name != new_message_name
                {
                    transformer.rename_message_type_in_fds(
                        &transformed_fds,
                        &old_message_name,
                        &new_message_name,
                        common_package,
                    )?
                } else {
                    transformed_fds
                };

                Ok(SchemaMetadata::Protobuf {
                    type_name: new_type_name,
                    file_descriptor_set: transformed_fds,
                    schema_text: schema_text.clone(),
                })
            }
            SchemaMetadata::Json { .. } => {
                let text = new_schema_text
                    .or_else(|| original_schema_text.map(|s| s.to_string()))
                    .unwrap_or_default();
                Ok(SchemaMetadata::json(new_type_name, text))
            }
        }
    }

    /// Get the transformed topic for a channel.
    pub fn get_transformed_topic(&self, channel_id: u16) -> Option<&str> {
        self.channel_topics.get(&channel_id).map(|s| s.as_str())
    }

    /// Get the transformed schema for a channel.
    pub fn get_transformed_schema(&self, channel_id: u16) -> Option<&SchemaMetadata> {
        // Schemas are keyed by channel_id to support topic-specific type transforms
        self.schemas.get(&channel_id.to_string())
    }

    /// Rewrite a single message.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to rewrite
    /// * `channel_info` - Channel information
    /// * `skip_decode_failures` - Whether to skip messages that fail to decode
    /// * `encode_callback` - Callback to write the encoded data
    ///
    /// # Returns
    ///
    /// True if the message was processed (encoded or passed through),
    /// false if it was skipped
    pub fn rewrite_message<F>(
        &mut self,
        msg: &RawMessage,
        channel_info: &ChannelInfo,
        skip_decode_failures: bool,
        mut encode_callback: F,
    ) -> Result<bool>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        self.stats.message_count += 1;

        // Detect encoding
        let encoding = self.codec_factory.detect_encoding(
            &channel_info.encoding,
            channel_info.schema_encoding.as_deref(),
        );

        // Get the schema for this channel
        let schema = match self.get_transformed_schema(msg.channel_id) {
            Some(s) => s.clone(),
            None => {
                // No schema available, pass through
                encode_callback(&msg.data)?;
                self.stats.passthrough_count += 1;
                return Ok(true);
            }
        };

        // Get the codec for this encoding (mutable for encode)
        let codec = self.codec_factory.get_codec_mut(encoding)?;

        // Decode the message
        let decoded = match codec.decode_dynamic(&msg.data, &schema) {
            Ok(d) => d,
            Err(e) => {
                self.stats.decode_failures += 1;
                if skip_decode_failures {
                    // Skip decode failure: pass through original data
                    encode_callback(&msg.data)?;
                    self.stats.passthrough_count += 1;
                    return Ok(true);
                }
                // Fail on decode error: return error to caller
                return Err(e);
            }
        };

        // Encode the message
        let encoded_data = match codec.encode_dynamic(&decoded, &schema) {
            Ok(data) => data,
            Err(e) => {
                self.stats.encode_failures += 1;
                return Err(e);
            }
        };

        // Write the encoded data
        encode_callback(&encoded_data)?;
        self.stats.reencoded_count += 1;

        Ok(true)
    }

    /// Reset the engine state for a new rewrite operation.
    pub fn reset(&mut self) {
        self.stats = McapRewriteStats::default();
        self.schemas.clear();
        self.channel_schemas.clear();
        self.channel_topics.clear();
    }
}

impl Default for McapRewriteEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// Get the fixtures directory path
    fn fixtures_dir() -> PathBuf {
        // Use CARGO_MANIFEST_DIR to get the robocodec crate root,
        // then go up to workspace root to access shared fixtures
        let manifest_dir =
            std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| String::from("."));
        PathBuf::from(manifest_dir)
            .parent()
            .expect("manifest dir should have parent")
            .join("tests")
            .join("fixtures")
    }

    fn fixture_path(name: &str) -> PathBuf {
        fixtures_dir().join(name)
    }

    #[test]
    fn test_engine_creation() {
        let engine = McapRewriteEngine::new();
        assert_eq!(engine.stats.message_count, 0);
    }

    #[test]
    fn test_engine_default() {
        let engine = McapRewriteEngine::default();
        assert_eq!(engine.stats.message_count, 0);
    }

    #[test]
    fn test_engine_reset() {
        let mut engine = McapRewriteEngine::new();
        engine.stats.message_count = 100;
        engine.stats.reencoded_count = 50;

        engine.reset();

        assert_eq!(engine.stats.message_count, 0);
        assert_eq!(engine.stats.reencoded_count, 0);
    }

    #[test]
    fn test_stats_default() {
        let stats = McapRewriteStats::default();
        assert_eq!(stats.message_count, 0);
        assert_eq!(stats.reencoded_count, 0);
        assert_eq!(stats.passthrough_count, 0);
    }

    #[test]
    fn test_prepare_schemas_with_reader() {
        let reader = crate::McapReader::open(&fixture_path("robocodec_test_5.mcap")).unwrap();
        let mut engine = McapRewriteEngine::new();

        // Should successfully prepare schemas
        let result = engine.prepare_schemas(&reader, None);
        assert!(result.is_ok(), "prepare_schemas failed: {:?}", result.err());

        // Should have cached schemas
        assert!(!engine.schemas.is_empty());
    }

    #[test]
    fn test_prepare_schemas_with_transforms() {
        let reader = crate::McapReader::open(&fixture_path("robocodec_test_5.mcap")).unwrap();
        let mut engine = McapRewriteEngine::new();

        // Create a transform pipeline using the builder
        let pipeline = crate::transform::TransformBuilder::new()
            .with_type_rename("std_msgs", "my_msgs")
            .build();

        // Should successfully prepare schemas with transforms
        let result = engine.prepare_schemas(&reader, Some(&pipeline));
        assert!(
            result.is_ok(),
            "prepare_schemas with transforms failed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_protobuf_rewriting() {
        let fixture_path_str = fixture_path("robocodec_test_3.mcap");
        let reader = crate::McapReader::open(&fixture_path_str).unwrap_or_else(|e| {
            panic!("Failed to open {:?}: {e}", fixture_path_str);
        });

        println!("=== Channels in fixture ===");
        for (id, channel) in reader.channels() {
            println!("Channel {}: {}", id, channel.topic);
            println!("  Message type: {}", channel.message_type);
            println!("  Encoding: {}", channel.encoding);
            println!("  Schema encoding: {:?}", channel.schema_encoding);
        }

        let mut engine = McapRewriteEngine::new();

        // Create type rename mappings (example using actual fixture types):
        // /lowdim/joint: pkg.foo.Bar -> pkg.foo.Baz
        // /lowdim/tcp: pkg.foo.Bar -> pkg.foo.Qux
        // /lowdim/ee_state: pkg.foo.Bar -> pkg.foo.Quux
        // /lowdim/airexo_joint: pkg.foo.Bar -> pkg.foo.Corge
        // /camera/intrinsic/camid_1: pkg.foo.Bar -> pkg.foo.baz_qux

        let pipeline = crate::transform::TransformBuilder::new()
            .with_type_rename("nmx.msg.LowdimData", "nmx.msg.JointStates")
            .build();

        // Prepare schemas with transforms
        engine
            .prepare_schemas(&reader, Some(&pipeline))
            .expect("Failed to prepare schemas");

        // Verify schemas were transformed
        assert!(!engine.schemas.is_empty(), "No schemas were cached");

        println!("=== Prepared schemas ===");
        println!("Prepared {} schemas", engine.schemas.len());
        for type_name in engine.schemas.keys() {
            println!("  - {type_name}");
        }

        // For now, let's just check that we have some schemas
        // The actual transformation will be implemented in the apply_transformations method
        assert!(!engine.schemas.is_empty(), "Expected schemas to be cached");

        // Verify statistics
        let stats = engine.stats();
        println!("=== Stats ===");
        println!("  Topics renamed: {}", stats.topics_renamed);
        println!("  Types renamed: {}", stats.types_renamed);
    }

    #[test]
    fn test_extract_protobuf_package_with_leading_dot() {
        // Test with leading dot: ".pkg.foo.Bar" -> "pkg.foo"
        let result = extract_protobuf_package(".pkg.foo.Bar");
        assert_eq!(result, "pkg.foo");
    }

    #[test]
    fn test_extract_protobuf_package_without_leading_dot() {
        // Test without leading dot: "pkg.foo.Bar" -> "pkg.foo"
        let result = extract_protobuf_package("pkg.foo.Bar");
        assert_eq!(result, "pkg.foo");
    }

    #[test]
    fn test_extract_protobuf_package_single_part() {
        // Test with single part: "MessageType" -> ""
        let result = extract_protobuf_package("MessageType");
        assert_eq!(result, "");
    }

    #[test]
    fn test_extract_protobuf_package_two_parts() {
        // Test with two parts: "pkg.Message" -> "pkg"
        let result = extract_protobuf_package("pkg.Message");
        assert_eq!(result, "pkg");
    }

    #[test]
    fn test_extract_protobuf_package_nested_packages() {
        // Test with nested packages: "foo.bar.baz.Type" -> "foo.bar.baz"
        let result = extract_protobuf_package("foo.bar.baz.Type");
        assert_eq!(result, "foo.bar.baz");
    }

    #[test]
    fn test_extract_protobuf_package_empty_string() {
        // Test with empty string: "" -> ""
        let result = extract_protobuf_package("");
        assert_eq!(result, "");
    }

    #[test]
    fn test_extract_protobuf_package_only_dots() {
        // Test with only dots: "..." -> "." (join of empty strings from split)
        let result = extract_protobuf_package("...");
        assert_eq!(result, ".");
    }
}
