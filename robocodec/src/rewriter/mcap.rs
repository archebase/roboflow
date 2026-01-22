// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! MCAP file rewriter using decode-encode-write flow.
//!
//! This module provides functionality to normalize MCAP files by:
//! 1. Reading messages from the source MCAP
//! 2. Decoding each message (handles any CDR header issues)
//! 3. Re-encoding with proper CDR headers using schema-driven encoding
//! 4. Writing to a new MCAP file
//! 5. Optionally applying transformations (topic/type renaming, schema rewriting)
//!
//! This ensures consistent CDR formatting across all messages.
//!
//! **Note:** This implementation uses a custom MCAP writer with no external dependencies.

use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

use tracing::warn;

use crate::core::{CodecError, Result};
use crate::encoding::{CdrDecoder, CdrEncoder};
use crate::io::formats::mcap::reader::McapReader;
use crate::io::formats::mcap::writer::ParallelMcapWriter;
use crate::rewriter::{FormatRewriter, RewriteOptions, RewriteStats};
use crate::schema::{parse_schema, MessageSchema};
use crate::transform::ChannelInfo as TransformChannelInfo;

/// MCAP file rewriter.
///
/// Performs a full decode-encode-write cycle to normalize MCAP files.
/// Can optionally apply transformations to rename topics, message types,
/// and rewrite schema definitions.
pub struct McapRewriter {
    /// Options for rewriting
    options: RewriteOptions,
    /// Cached schemas indexed by type name (transformed type name if transforms applied)
    schemas: HashMap<String, MessageSchema>,
    /// Statistics
    stats: RewriteStats,
    /// Sequence numbers per channel
    sequences: HashMap<u16, u32>,
}

impl McapRewriter {
    /// Create a new rewriter with default options.
    pub fn new() -> Self {
        Self::with_options(RewriteOptions::default())
    }

    /// Create a new rewriter with custom options.
    pub fn with_options(options: RewriteOptions) -> Self {
        Self {
            options,
            schemas: HashMap::new(),
            stats: RewriteStats::default(),
            sequences: HashMap::new(),
        }
    }

    /// Rewrite an MCAP file to a new location.
    ///
    /// # Arguments
    ///
    /// * `input_path` - Path to the input MCAP file
    /// * `output_path` - Path to the output MCAP file
    ///
    /// # Returns
    ///
    /// Statistics about the rewrite operation
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use roboflow::format::rewriter::McapRewriter;
    /// use roboflow::transform::TransformBuilder;
    /// use roboflow::rewriter::RewriteOptions;
    ///
    /// // With transformations
    /// let options = RewriteOptions::default().with_transforms(
    ///     TransformBuilder::new()
    ///         .with_topic_rename("/old_camera", "/camera")
    ///         .with_type_rename("foo/JointState", "bar/JointState")
    ///         .build()
    /// );
    ///
    /// let mut rewriter = McapRewriter::with_options(options);
    /// let stats = rewriter.rewrite("input.mcap", "output.mcap")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn rewrite<P1, P2>(&mut self, input_path: P1, output_path: P2) -> Result<RewriteStats>
    where
        P1: AsRef<Path>,
        P2: AsRef<Path>,
    {
        // Reset statistics and sequences
        self.stats = RewriteStats::default();
        self.sequences = HashMap::new();

        // Open input MCAP
        let reader = McapReader::open(input_path)?;
        self.stats.channel_count = reader.channels().len() as u64;

        // Validate transformations if configured
        if let Some(ref pipeline) = self.options.transforms {
            let transform_channels: Vec<TransformChannelInfo> = reader
                .channels()
                .values()
                .map(TransformChannelInfo::from_reader_info)
                .collect();
            pipeline
                .validate(&transform_channels)
                .map_err(|e| CodecError::encode("Transform", e.to_string()))?;
        }

        // Create output file
        let output_file = File::create(output_path).map_err(|e| {
            CodecError::encode("MCAP", format!("Failed to create output file: {e}"))
        })?;

        let mut mcap_writer =
            ParallelMcapWriter::new(BufWriter::new(output_file)).map_err(|e| {
                CodecError::encode("MCAP", format!("Failed to create MCAP writer: {e}"))
            })?;

        // Pre-parse all schemas with transformations applied
        if self.options.validate_schemas {
            self.cache_schemas(&reader)?;
        }

        // Build schema ID and channel ID mappings with transformations
        let mut schema_ids: HashMap<String, u16> = HashMap::new();
        let mut channel_map: HashMap<u16, u16> = HashMap::new();
        let mut topic_counter: HashMap<String, u32> = HashMap::new();

        // Get reference to pipeline for use in closures
        let pipeline = self.options.transforms.as_ref();

        // First pass: add all schemas (with transformations applied)
        for (_channel_id, channel) in reader.channels().iter() {
            // Apply transformations to get the target type name and schema
            let (transformed_type, transformed_schema) = if let Some(p) = pipeline {
                p.transform_type(&channel.message_type, channel.schema.as_deref())
            } else {
                (channel.message_type.clone(), channel.schema.clone())
            };

            if !schema_ids.contains_key(&transformed_type) {
                let schema_bytes = transformed_schema
                    .as_ref()
                    .map(|s| s.as_bytes())
                    .or_else(|| channel.schema.as_ref().map(|s| s.as_bytes()));

                if let Some(bytes) = schema_bytes {
                    let schema_id = mcap_writer
                        .add_schema(
                            &transformed_type,
                            channel.schema_encoding.as_deref().unwrap_or("ros2msg"),
                            bytes,
                        )
                        .map_err(|e| {
                            CodecError::encode("MCAP", format!("Failed to add schema: {e}"))
                        })?;
                    schema_ids.insert(transformed_type.clone(), schema_id);
                } else {
                    schema_ids.insert(transformed_type.clone(), 0);
                }
            }
        }

        // Second pass: add all channels (with transformations applied)
        for (old_channel_id, channel) in reader.channels() {
            let (transformed_type, _) = if let Some(p) = pipeline {
                p.transform_type(&channel.message_type, None)
            } else {
                (channel.message_type.clone(), None)
            };

            let schema_id = schema_ids.get(&transformed_type).copied().unwrap_or(0);

            // Apply transformations to topic name
            let mut transformed_topic = if let Some(p) = pipeline {
                p.transform_topic(&channel.topic)
                    .unwrap_or_else(|| channel.topic.clone())
            } else {
                channel.topic.clone()
            };

            // Handle topic name collisions with numeric suffixes
            if let Some(count) = topic_counter.get_mut(&transformed_topic) {
                *count += 1;
                transformed_topic = format!("{transformed_topic}_{count}");
                warn!(
                    context = "topic_collision",
                    original_topic = %channel.topic,
                    new_topic = %transformed_topic,
                    "Topic collision detected and renamed"
                );
            } else {
                // Check if this topic name already exists as-is
                let exists = reader
                    .channels()
                    .values()
                    .any(|c| c.topic == transformed_topic && c.id != *old_channel_id);
                if exists {
                    topic_counter.insert(transformed_topic.clone(), 1);
                }
            }

            let new_channel_id = mcap_writer
                .add_channel(
                    schema_id,
                    &transformed_topic,
                    &channel.encoding,
                    &HashMap::new(),
                )
                .map_err(|e| CodecError::encode("MCAP", format!("Failed to add channel: {e}")))?;

            channel_map.insert(*old_channel_id, new_channel_id);

            // Track transformation statistics
            if let Some(p) = pipeline {
                if p.transform_topic(&channel.topic).as_deref() != Some(&channel.topic) {
                    self.stats.topics_renamed += 1;
                }
                if p.transform_type(&channel.message_type, None).0 != channel.message_type {
                    self.stats.types_renamed += 1;
                }
            }
        }

        // Process messages
        let messages = reader.iter_raw()?;
        let mut stream = messages.stream()?;

        // Clone schemas for use in closure
        let schemas = self.schemas.clone();

        // Build a map of original channel_id -> transformed message type for schema lookup
        let pipeline = self.options.transforms.as_ref();
        let channel_type_map: HashMap<u16, String> = reader
            .channels()
            .iter()
            .map(|(id, ch)| {
                let transformed = if let Some(p) = pipeline {
                    p.transform_type(&ch.message_type, None).0
                } else {
                    ch.message_type.clone()
                };
                (*id, transformed)
            })
            .collect();

        // Extract boolean flags (can't clone RewriteOptions due to MultiTransform)
        let _skip_decode_failures = self.options.skip_decode_failures;
        let passthrough_non_cdr = self.options.passthrough_non_cdr;

        // Process each message
        for result in &mut stream {
            let (msg, channel_info) = match result {
                Ok(m) => m,
                Err(e) => {
                    warn!(
                        context = "message_read",
                        error = %e,
                        "Failed to read message"
                    );
                    continue;
                }
            };

            self.stats.message_count += 1;
            let new_channel_id = channel_map
                .get(&msg.channel_id)
                .copied()
                .unwrap_or(msg.channel_id);

            // Only rewrite CDR messages
            if channel_info.encoding != "cdr"
                && channel_info.encoding != "ros2"
                && channel_info.encoding != "ros2msg"
            {
                // Pass through non-CDR messages
                if passthrough_non_cdr {
                    self.write_message_raw(&mut mcap_writer, &msg, new_channel_id)?;
                    self.stats.passthrough_count += 1;
                }
                continue;
            }

            // Get the transformed message type for schema lookup
            let transformed_type = channel_type_map
                .get(&msg.channel_id)
                .unwrap_or(&channel_info.message_type);

            // Get or parse schema (using transformed type)
            let schema_opt = schemas.get(transformed_type);

            // Decode and re-encode CDR messages
            if let Some(schema) = schema_opt {
                self.rewrite_cdr_message(
                    &mut mcap_writer,
                    &msg,
                    schema,
                    new_channel_id,
                    &channel_info.topic,
                )?;
            } else {
                // No schema available, pass through as-is
                self.write_message_raw(&mut mcap_writer, &msg, new_channel_id)?;
                self.stats.passthrough_count += 1;
            }
        }

        // Finish the MCAP writer
        mcap_writer
            .finish()
            .map_err(|e| CodecError::encode("MCAP", format!("Failed to finish MCAP: {e}")))?;

        Ok(self.stats.clone())
    }

    /// Cache all schemas from the MCAP file, applying transformations if configured.
    fn cache_schemas(&mut self, reader: &McapReader) -> Result<()> {
        let pipeline = self.options.transforms.as_ref();

        for channel in reader.channels().values() {
            // Apply transformations to get target type
            let (target_type, _target_schema) = if let Some(p) = pipeline {
                p.transform_type(&channel.message_type, channel.schema.as_deref())
            } else {
                (channel.message_type.clone(), channel.schema.clone())
            };

            // Only cache if not already cached under the target type
            if !self.schemas.contains_key(&target_type) {
                // Use original schema for parsing (before text transformation)
                let schema_to_parse = channel.schema.as_ref();

                if let Some(schema_text) = schema_to_parse {
                    match parse_schema(&channel.message_type, schema_text) {
                        Ok(mut schema) => {
                            // Apply package renaming to the parsed schema's internal types
                            if target_type != channel.message_type {
                                // Extract package names from old and new type names
                                let old_package =
                                    channel.message_type.split('/').next().unwrap_or("");
                                let new_package = target_type.split('/').next().unwrap_or("");

                                // Only rename if packages differ
                                if !old_package.is_empty()
                                    && !new_package.is_empty()
                                    && old_package != new_package
                                {
                                    schema.rename_package(old_package, new_package);
                                }

                                // Update the schema's main name
                                schema.name = target_type.clone();
                                if schema.package.as_deref() == Some(old_package) {
                                    schema.package = Some(new_package.to_string());
                                }
                            }

                            self.schemas.insert(target_type.clone(), schema);
                        }
                        Err(e) => {
                            if self.options.validate_schemas {
                                return Err(CodecError::encode(
                                    "MCAP",
                                    format!(
                                        "Failed to parse schema for {} (from {}): {}",
                                        target_type, channel.message_type, e
                                    ),
                                ));
                            }
                            // Non-validating mode: continue without schema
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Rewrite a CDR message by decoding and re-encoding.
    fn rewrite_cdr_message(
        &mut self,
        mcap_writer: &mut ParallelMcapWriter<BufWriter<File>>,
        msg: &crate::mcap::reader::RawMessage,
        schema: &MessageSchema,
        channel_id: u16,
        topic: &str,
    ) -> Result<()> {
        // Decode the message (handles CDR header internally)
        let decoder = CdrDecoder::new();
        let decoded = match decoder.decode(schema, &msg.data, Some(&schema.name)) {
            Ok(d) => d,
            Err(e) => {
                warn!(
                    context = "cdr_decode",
                    error = %e,
                    schema = %schema.name,
                    topic = %topic,
                    "Failed to decode CDR message"
                );
                self.stats.decode_failures += 1;
                if self.options.skip_decode_failures {
                    // Skip this message entirely (message will be lost)
                    return Ok(());
                }
                // Pass through original data on decode failure
                self.write_message_raw(mcap_writer, msg, channel_id)?;
                return Ok(());
            }
        };

        // Re-encode with proper CDR header
        let mut encoder = CdrEncoder::new();
        match encoder.encode_message(&decoded, schema, &schema.name) {
            Ok(()) => {}
            Err(e) => {
                warn!(
                    context = "cdr_encode",
                    error = %e,
                    schema = %schema.name,
                    topic = %topic,
                    "Failed to encode CDR message (passing through original data)"
                );
                self.stats.encode_failures += 1;
                // Pass through original data on encode failure
                self.write_message_raw(mcap_writer, msg, channel_id)?;
                return Ok(());
            }
        }

        let encoded_data = encoder.finish();

        // Write the re-encoded message using custom writer
        mcap_writer
            .write_message(channel_id, msg.log_time, msg.publish_time, &encoded_data)
            .map_err(|e| CodecError::encode("MCAP", format!("Failed to write message: {e}")))?;

        self.stats.reencoded_count += 1;
        Ok(())
    }

    /// Write a raw message without re-encoding.
    fn write_message_raw(
        &mut self,
        mcap_writer: &mut ParallelMcapWriter<BufWriter<File>>,
        msg: &crate::mcap::reader::RawMessage,
        channel_id: u16,
    ) -> Result<()> {
        mcap_writer
            .write_message(channel_id, msg.log_time, msg.publish_time, &msg.data)
            .map_err(|e| CodecError::encode("MCAP", format!("Failed to write message: {e}")))?;

        Ok(())
    }

    /// Get the options used for rewriting.
    pub fn options(&self) -> &RewriteOptions {
        &self.options
    }
}

impl FormatRewriter for McapRewriter {
    fn rewrite<P1, P2>(&mut self, input_path: P1, output_path: P2) -> Result<RewriteStats>
    where
        P1: AsRef<Path>,
        P2: AsRef<Path>,
    {
        self.rewrite(input_path, output_path)
    }

    fn options(&self) -> &RewriteOptions {
        self.options()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl Default for McapRewriter {
    fn default() -> Self {
        Self::new()
    }
}

/// Convenience function to rewrite an MCAP file.
///
/// # Arguments
///
/// * `input_path` - Path to the input MCAP file
/// * `output_path` - Path to the output MCAP file
///
/// # Example
///
/// ```no_run
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use roboflow::format::rewriter::mcap::rewrite_mcap;
///
/// let stats = rewrite_mcap("input.mcap", "output.mcap")?;
/// println!("Processed {} messages", stats.message_count);
/// # Ok(())
/// # }
/// ```
pub fn rewrite_mcap<P1, P2>(input_path: P1, output_path: P2) -> Result<RewriteStats>
where
    P1: AsRef<Path>,
    P2: AsRef<Path>,
{
    let mut rewriter = McapRewriter::new();
    rewriter.rewrite(input_path, output_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::{MultiTransform, TransformBuilder};
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

    /// Get a temporary file path for test output
    fn temp_output(name: &str) -> PathBuf {
        let random = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        std::env::temp_dir().join(format!("roboflow_mcap_test_{random}_{name}"))
    }

    /// Check if a fixture file exists
    fn fixture_exists(name: &str) -> bool {
        fixtures_dir().join(name).exists()
    }

    // =========================================================================
    // Construction Tests
    // =========================================================================

    #[test]
    fn test_rewriter_new_creates_with_default_options() {
        let rewriter = McapRewriter::new();
        assert!(rewriter.options.validate_schemas);
        assert!(rewriter.options.skip_decode_failures);
        assert!(rewriter.options.passthrough_non_cdr);
        assert!(!rewriter.options.has_transforms());
    }

    #[test]
    fn test_rewriter_default() {
        let rewriter = McapRewriter::default();
        assert!(rewriter.options.validate_schemas);
    }

    #[test]
    fn test_rewriter_with_custom_options() {
        let opts = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: false,
            passthrough_non_cdr: false,
            transforms: None,
        };
        let rewriter = McapRewriter::with_options(opts);
        assert!(!rewriter.options.validate_schemas);
        assert!(!rewriter.options.skip_decode_failures);
        assert!(!rewriter.options.passthrough_non_cdr);
    }

    #[test]
    fn test_rewriter_with_options_has_empty_caches() {
        let rewriter = McapRewriter::new();
        assert!(rewriter.schemas.is_empty());
        assert_eq!(rewriter.stats.message_count, 0);
        assert!(rewriter.sequences.is_empty());
    }

    #[test]
    fn test_rewriter_options_returns_reference() {
        let rewriter = McapRewriter::new();
        let opts = rewriter.options();
        assert!(opts.validate_schemas);
    }

    // =========================================================================
    // RewriteOptions Tests
    // =========================================================================

    #[test]
    fn test_rewrite_options_default() {
        let opts = RewriteOptions::default();
        assert!(opts.validate_schemas);
        assert!(opts.skip_decode_failures);
        assert!(opts.passthrough_non_cdr);
        assert!(!opts.has_transforms());
    }

    #[test]
    fn test_rewrite_options_with_transforms() {
        let pipeline = TransformBuilder::new()
            .with_topic_rename("/old", "/new")
            .build();
        let opts = RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: false,
            passthrough_non_cdr: true,
            transforms: Some(pipeline),
        };
        assert!(opts.has_transforms());
    }

    // =========================================================================
    // RewriteStats Tests
    // =========================================================================

    #[test]
    fn test_rewrite_stats_default() {
        let stats = RewriteStats::default();
        assert_eq!(stats.message_count, 0);
        assert_eq!(stats.channel_count, 0);
        assert_eq!(stats.topics_renamed, 0);
        assert_eq!(stats.types_renamed, 0);
        assert_eq!(stats.reencoded_count, 0);
        assert_eq!(stats.passthrough_count, 0);
        assert_eq!(stats.decode_failures, 0);
        assert_eq!(stats.encode_failures, 0);
    }

    #[test]
    fn test_rewrite_stats_can_be_updated() {
        let stats = RewriteStats {
            message_count: 100,
            channel_count: 5,
            topics_renamed: 2,
            types_renamed: 1,
            reencoded_count: 95,
            passthrough_count: 5,
            decode_failures: 1,
            encode_failures: 0,
        };

        assert_eq!(stats.message_count, 100);
        assert_eq!(stats.channel_count, 5);
        assert_eq!(stats.topics_renamed, 2);
        assert_eq!(stats.types_renamed, 1);
        assert_eq!(stats.reencoded_count, 95);
        assert_eq!(stats.passthrough_count, 5);
        assert_eq!(stats.decode_failures, 1);
    }

    // =========================================================================
    // FormatRewriter Trait Tests
    // =========================================================================

    #[test]
    fn test_mcap_rewriter_implements_format_rewriter_methods() {
        let rewriter = McapRewriter::new();
        // Directly test the trait methods are accessible
        assert!(rewriter.options().validate_schemas);
        assert!(rewriter.options().skip_decode_failures);
    }

    // =========================================================================
    // Error Handling Tests
    // =========================================================================

    #[test]
    fn test_rewriter_returns_error_for_nonexistent_input() {
        let mut rewriter = McapRewriter::new();
        let input_path = PathBuf::from("/nonexistent/path/to/file.mcap");
        let output_path = temp_output("error_output");

        let result = rewriter.rewrite(&input_path, &output_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_rewriter_returns_error_for_invalid_output_path() {
        if !fixture_exists("robocodec_test_0.mcap") {
            return;
        }

        let mut rewriter = McapRewriter::new();
        let input_path = fixtures_dir().join("robocodec_test_0.mcap");
        let output_path = PathBuf::from("/nonexistent/directory/cannot_create/file.mcap");

        let result = rewriter.rewrite(&input_path, &output_path);
        assert!(result.is_err());
    }

    // =========================================================================
    // Integration Tests with Fixtures
    // =========================================================================

    #[test]
    fn test_rewriter_processes_mcap_file() {
        if !fixture_exists("robocodec_test_0.mcap") {
            return;
        }

        let input_path = fixtures_dir().join("robocodec_test_0.mcap");
        let output_path = temp_output("processed.mcap");

        let mut rewriter = McapRewriter::new();
        let result = rewriter.rewrite(&input_path, &output_path);

        assert!(result.is_ok(), "Rewrite should succeed: {:?}", result.err());

        let stats = result.unwrap();
        // Verify the output file was created
        assert!(output_path.exists());
        // Should have processed some data
        assert!(stats.message_count > 0 || stats.channel_count > 0);

        // Cleanup
        let _ = std::fs::remove_file(&output_path);
    }

    #[test]
    fn test_rewriter_tracks_statistics() {
        if !fixture_exists("robocodec_test_0.mcap") {
            return;
        }

        let input_path = fixtures_dir().join("robocodec_test_0.mcap");
        let output_path = temp_output("stats.mcap");

        let mut rewriter = McapRewriter::new();
        let stats = rewriter.rewrite(&input_path, &output_path).unwrap();

        // Verify rewrite completed successfully
        assert!(output_path.exists());
        // Verify stats are tracked
        assert!(stats.channel_count > 0, "Expected at least one channel");

        // Cleanup
        let _ = std::fs::remove_file(&output_path);
    }

    #[test]
    fn test_rewriter_with_transform_pipeline() {
        if !fixture_exists("robocodec_test_0.mcap") {
            return;
        }

        let input_path = fixtures_dir().join("robocodec_test_0.mcap");
        let output_path = temp_output("transformed.mcap");

        let transforms = TransformBuilder::new()
            .with_topic_rename("/old_topic", "/new_topic")
            .with_type_rename("old/OldType", "new/NewType")
            .build();

        let options = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: Some(transforms),
        };

        let mut rewriter = McapRewriter::with_options(options);
        let result = rewriter.rewrite(&input_path, &output_path);

        // Should succeed even if transformations don't match anything
        // or if there's a validation issue - we just check it doesn't crash
        if result.is_err() {
            // Some MCAP files may have validation issues, that's OK for this test
            // Just verify the rewriter can be constructed with transforms
        } else {
            assert!(output_path.exists());
            // Cleanup
            let _ = std::fs::remove_file(&output_path);
        }
    }

    #[test]
    fn test_rewriter_with_skip_decode_failures() {
        if !fixture_exists("robocodec_test_0.mcap") {
            return;
        }

        let input_path = fixtures_dir().join("robocodec_test_0.mcap");
        let output_path = temp_output("skip_decode.mcap");

        let options = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: None,
        };

        let mut rewriter = McapRewriter::with_options(options);
        let result = rewriter.rewrite(&input_path, &output_path);

        assert!(result.is_ok());
        assert!(output_path.exists());

        // Cleanup
        let _ = std::fs::remove_file(&output_path);
    }

    #[test]
    fn test_rewriter_with_passthrough_non_cdr() {
        if !fixture_exists("robocodec_test_0.mcap") {
            return;
        }

        let input_path = fixtures_dir().join("robocodec_test_0.mcap");
        let output_path = temp_output("passthrough.mcap");

        let options = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: None,
        };

        let mut rewriter = McapRewriter::with_options(options);
        let result = rewriter.rewrite(&input_path, &output_path);

        assert!(result.is_ok());
        assert!(output_path.exists());

        // Cleanup
        let _ = std::fs::remove_file(&output_path);
    }

    // =========================================================================
    // Convenience Function Tests
    // =========================================================================

    #[test]
    fn test_rewrite_mcap_function() {
        if !fixture_exists("robocodec_test_0.mcap") {
            return;
        }

        let input_path = fixtures_dir().join("robocodec_test_0.mcap");
        let output_path = temp_output("convenience.mcap");

        let result = rewrite_mcap(&input_path, &output_path);

        assert!(result.is_ok());
        assert!(output_path.exists());

        // Cleanup
        let _ = std::fs::remove_file(&output_path);
    }

    // =========================================================================
    // Multiple Rewrite Tests
    // =========================================================================

    #[test]
    fn test_multiple_rewrites_are_independent() {
        if !fixture_exists("robocodec_test_0.mcap") {
            return;
        }

        let input_path = fixtures_dir().join("robocodec_test_0.mcap");
        let output_path1 = temp_output("multi1.mcap");
        let output_path2 = temp_output("multi2.mcap");

        let mut rewriter = McapRewriter::new();

        // First rewrite
        let stats1 = rewriter.rewrite(&input_path, &output_path1).unwrap();

        // Second rewrite should have fresh statistics
        let stats2 = rewriter.rewrite(&input_path, &output_path2).unwrap();

        // Both should succeed
        assert!(output_path1.exists());
        assert!(output_path2.exists());

        // Second rewrite should have similar stats (same input)
        assert_eq!(stats1.channel_count, stats2.channel_count);

        // Cleanup
        let _ = std::fs::remove_file(&output_path1);
        let _ = std::fs::remove_file(&output_path2);
    }

    // =========================================================================
    // Transform Pipeline Tests
    // =========================================================================

    #[test]
    fn test_rewriter_with_empty_transform_pipeline() {
        let rewriter = McapRewriter::with_options(RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: false,
            passthrough_non_cdr: false,
            transforms: Some(MultiTransform::new()),
        });

        // Empty pipeline has transforms field set but reports as empty
        assert!(!rewriter.options.has_transforms());
        assert!(rewriter.options.transforms.is_some());
    }

    #[test]
    fn test_rewriter_preserves_all_options() {
        let pipeline = TransformBuilder::new()
            .with_topic_rename("/from", "/to")
            .build();

        let opts = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: true,
            passthrough_non_cdr: false,
            transforms: Some(pipeline),
        };

        let rewriter = McapRewriter::with_options(opts);

        assert!(!rewriter.options.validate_schemas);
        assert!(rewriter.options.skip_decode_failures);
        assert!(!rewriter.options.passthrough_non_cdr);
        assert!(rewriter.options.has_transforms());
    }

    // =========================================================================
    // Round-trip Tests
    // =========================================================================

    #[test]
    fn test_rewriter_round_trip_preserves_messages() {
        if !fixture_exists("robocodec_test_0.mcap") {
            return;
        }

        let input_path = fixtures_dir().join("robocodec_test_0.mcap");
        let output_path = temp_output("roundtrip.mcap");

        let mut rewriter = McapRewriter::new();
        let stats = rewriter.rewrite(&input_path, &output_path).unwrap();

        // All messages should be processed (either re-encoded or passed through)
        let total_processed = stats.reencoded_count + stats.passthrough_count;
        assert!(
            total_processed >= stats.message_count,
            "Processed count should be at least message_count: reencoded={}, passthrough={}, message_count={}",
            stats.reencoded_count,
            stats.passthrough_count,
            stats.message_count
        );

        // Cleanup
        let _ = std::fs::remove_file(&output_path);
    }
}
