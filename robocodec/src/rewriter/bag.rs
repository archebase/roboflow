// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! ROS1 bag file rewriter using decode-encode-write flow.
//!
//! This module provides functionality to rewrite ROS1 bag files with
//! optional transformations (topic/type renaming).

use std::collections::HashMap;
use std::path::Path;

use tracing::warn;

use crate::core::{CodecError, Result};
use crate::encoding::{CdrDecoder, CdrEncoder};
use crate::io::formats::bag::writer::BagWriter;
use crate::io::formats::bag::BagFormat;
use crate::io::traits::FormatReader;
use crate::rewriter::{FormatRewriter, RewriteOptions, RewriteStats};
use crate::schema::{parse_schema, MessageSchema};

/// ROS1 bag file rewriter.
///
/// Performs a full decode-encode-write cycle to normalize ROS1 bag files.
/// Can optionally apply transformations to rename topics and message types.
pub struct BagRewriter {
    /// Options for rewriting
    options: RewriteOptions,
    /// Cached schemas indexed by type name
    schemas: HashMap<String, MessageSchema>,
    /// Statistics
    stats: RewriteStats,
}

impl BagRewriter {
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
        }
    }

    /// Rewrite a ROS1 bag file to a new location.
    ///
    /// # Arguments
    ///
    /// * `input_path` - Path to the input bag file
    /// * `output_path` - Path to the output bag file
    ///
    /// # Returns
    ///
    /// Statistics about the rewrite operation.
    pub fn rewrite<P1, P2>(&mut self, input_path: P1, output_path: P2) -> Result<RewriteStats>
    where
        P1: AsRef<Path>,
        P2: AsRef<Path>,
    {
        // Reset statistics
        self.stats = RewriteStats::default();

        // Open input bag to get channel information
        let reader = BagFormat::open(input_path.as_ref())?;
        let channels = FormatReader::channels(&reader).clone();
        self.stats.channel_count = channels.len() as u64;

        // Validate transformations if configured
        if let Some(ref pipeline) = self.options.transforms {
            let transform_channels: Vec<crate::transform::ChannelInfo> = channels
                .values()
                .map(|ch| crate::transform::ChannelInfo {
                    id: ch.id,
                    topic: ch.topic.clone(),
                    message_type: ch.message_type.clone(),
                    encoding: ch.encoding.clone(),
                    schema: ch.schema.clone(),
                    schema_encoding: ch.schema_encoding.clone(),
                })
                .collect();
            pipeline
                .validate(&transform_channels)
                .map_err(|e| CodecError::encode("Transform", e.to_string()))?;
        }

        // Create output bag writer
        let mut writer = BagWriter::create(output_path)?;

        // Pre-parse all schemas with transformations applied
        if self.options.validate_schemas {
            self.cache_schemas(&reader)?;
        }

        // Build connection ID mapping for transformed topics
        // Maps: original channel_id -> new sequential connection ID
        let mut conn_mapping: HashMap<u16, u16> = HashMap::new();
        // Use composite key (topic, callerid) to preserve connections from different publishers
        let mut topic_callerid_to_new_conn: HashMap<(String, Option<String>), u16> = HashMap::new();
        let mut next_new_conn_id: u16 = 0;

        let pipeline = self.options.transforms.as_ref();

        // First pass: add all connections (with transformations applied)
        for (orig_channel_id, channel) in channels.iter() {
            // Apply transformations to get the target type and topic
            let (transformed_type, transformed_schema) = if let Some(p) = pipeline {
                p.transform_type(&channel.message_type, channel.schema.as_deref())
            } else {
                (channel.message_type.clone(), channel.schema.clone())
            };

            let transformed_topic = if let Some(p) = pipeline {
                p.transform_topic(&channel.topic)
                    .unwrap_or_else(|| channel.topic.clone())
            } else {
                channel.topic.clone()
            };

            // Preserve callerid from the original channel (ROS1-specific metadata)
            let callerid = channel.callerid.clone();

            // Check if we already have a connection for this (topic, callerid) combination
            // This ensures we don't merge connections from different publishers
            let conn_key = (transformed_topic.clone(), callerid.clone());
            let new_conn_id = if let Some(&existing_id) = topic_callerid_to_new_conn.get(&conn_key)
            {
                existing_id
            } else {
                let new_id = next_new_conn_id;
                next_new_conn_id = next_new_conn_id.wrapping_add(1);

                // Add connection to writer with callerid preserved
                let callerid_str = callerid.as_deref().unwrap_or("");
                writer.add_connection_with_callerid(
                    new_id,
                    &transformed_topic,
                    &transformed_type,
                    transformed_schema.as_deref().unwrap_or(""),
                    callerid_str,
                )?;

                topic_callerid_to_new_conn.insert(conn_key, new_id);
                new_id
            };

            conn_mapping.insert(*orig_channel_id, new_conn_id);

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
        let reader = BagFormat::open(input_path.as_ref())?;
        let iter = reader.iter_raw()?;
        let stream = iter;

        // Build a map of channel_id -> transformed type for schema lookup
        let channel_type_map: HashMap<u16, String> = channels
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

        let cdr_decoder = CdrDecoder::new();
        let schemas = self.schemas.clone();

        // Process each message
        for result in stream {
            let (raw_msg, _channel_info) = match result {
                Ok(msg) => msg,
                Err(e) => {
                    warn!(
                        context = "bag_message_read",
                        error = %e,
                        "Failed to read message"
                    );
                    continue;
                }
            };

            self.stats.message_count += 1;

            // Get the new connection ID for this message
            let new_conn_id = conn_mapping.get(&raw_msg.channel_id).copied();

            // Skip if we don't have a mapping (shouldn't happen)
            let new_conn_id = match new_conn_id {
                Some(id) => id,
                None => {
                    warn!(
                        context = "bag_rewrite",
                        channel_id = raw_msg.channel_id,
                        "No connection mapping for channel, skipping message"
                    );
                    continue;
                }
            };

            // Get the transformed message type for schema lookup
            let transformed_type = channel_type_map
                .get(&raw_msg.channel_id)
                .map(|s| s.as_str());

            // Try to decode and re-encode CDR messages
            if let Some(type_str) = transformed_type {
                if let Some(schema) = schemas.get(type_str) {
                    // Use the io::RawMessage directly
                    match self.rewrite_cdr_message(&cdr_decoder, &raw_msg, schema) {
                        Ok(data) => {
                            // Write re-encoded message
                            writer.write_message(&crate::bag::writer::BagMessage::from_raw(
                                new_conn_id,
                                raw_msg.log_time,
                                data,
                            ))?;
                            self.stats.reencoded_count += 1;
                        }
                        Err(e) => {
                            warn!(
                                context = "bag_decode",
                                error = %e,
                                "Failed to decode message"
                            );
                            self.stats.decode_failures += 1;
                            if self.options.skip_decode_failures {
                                continue;
                            }
                            // Pass through original data
                            writer.write_message(&crate::bag::writer::BagMessage::from_raw(
                                new_conn_id,
                                raw_msg.log_time,
                                raw_msg.data.clone(),
                            ))?;
                        }
                    }
                } else {
                    // No schema, pass through
                    writer.write_message(&crate::bag::writer::BagMessage::from_raw(
                        new_conn_id,
                        raw_msg.log_time,
                        raw_msg.data.clone(),
                    ))?;
                    self.stats.passthrough_count += 1;
                }
            } else {
                // Pass through original data
                writer.write_message(&crate::bag::writer::BagMessage::from_raw(
                    new_conn_id,
                    raw_msg.log_time,
                    raw_msg.data.clone(),
                ))?;
                self.stats.passthrough_count += 1;
            }
        }

        // Finish the bag writer
        writer.finish()?;

        Ok(self.stats.clone())
    }

    /// Cache all schemas from the bag file, applying transformations if configured.
    fn cache_schemas(&mut self, reader: &crate::bag::ParallelBagReader) -> Result<()> {
        let pipeline = self.options.transforms.as_ref();
        let channels = FormatReader::channels(reader);

        for channel in channels.values() {
            // Apply transformations to get target type
            let (target_type, _target_schema) = if let Some(p) = pipeline {
                p.transform_type(&channel.message_type, channel.schema.as_deref())
            } else {
                (channel.message_type.clone(), channel.schema.clone())
            };

            // Only cache if not already cached under the target type
            if !self.schemas.contains_key(&target_type) {
                let schema_to_parse = channel.schema.as_ref();

                if let Some(schema_text) = schema_to_parse {
                    match parse_schema(&channel.message_type, schema_text) {
                        Ok(mut schema) => {
                            // Apply package renaming if types differ
                            if target_type != channel.message_type {
                                let old_package =
                                    channel.message_type.split('/').next().unwrap_or("");
                                let new_package = target_type.split('/').next().unwrap_or("");

                                if !old_package.is_empty()
                                    && !new_package.is_empty()
                                    && old_package != new_package
                                {
                                    schema.rename_package(old_package, new_package);
                                }
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
                                    "BagRewriter",
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
        decoder: &CdrDecoder,
        msg: &crate::io::metadata::RawMessage,
        schema: &MessageSchema,
    ) -> Result<Vec<u8>> {
        // Decode the message (handles CDR header internally)
        let decoded = decoder.decode(schema, &msg.data, Some(&schema.name))?;

        // Re-encode with proper CDR header
        let mut encoder = CdrEncoder::new();
        encoder.encode_message(&decoded, schema, &schema.name)?;

        let encoded_data = encoder.finish();
        Ok(encoded_data)
    }

    /// Get the options used for rewriting.
    pub fn options(&self) -> &RewriteOptions {
        &self.options
    }
}

impl FormatRewriter for BagRewriter {
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

impl Default for BagRewriter {
    fn default() -> Self {
        Self::new()
    }
}
