// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Transform stage - applies schema and topic transformations.
//!
//! The transform stage is responsible for:
//! - Receiving chunks from the reader stage
//! - Applying transform pipeline (topic rename, type rename, schema rewrite)
//! - Remapping channel IDs to match transformed channels
//! - Sending transformed chunks to the compression stage
//!
//! # Pipeline Position
//!
//! ```text
//! Reader Stage → Transform Stage → Compression Stage → Writer Stage
//! ```
//!
//! # Transform Flow
//!
//! 1. **Metadata Transformation**: Apply transform pipeline to channel metadata
//! 2. **Channel ID Remapping**: Create mapping from original to transformed channel IDs
//! 3. **Chunk Transformation**: Remap channel IDs in each message
//! 4. **Schema Storage**: Store transformed schemas for writer to use

use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use crossbeam_channel::{Receiver, Sender};
use tracing::{debug, info, instrument};

use crate::core::{Result, RoboflowError};
use robocodec::io::traits::MessageChunkData;
use robocodec::transform::{ChannelInfo, MultiTransform, TransformedChannel};

/// Configuration for the transform stage.
#[derive(Debug, Clone)]
pub struct TransformStageConfig {
    /// Enable transform stage (if false, chunks pass through unchanged)
    pub enabled: bool,
    /// Enable verbose logging
    pub verbose: bool,
}

impl Default for TransformStageConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            verbose: false,
        }
    }
}

/// Transform stage - applies transformations to chunks and metadata.
///
/// This stage sits between the reader and compression stages, applying
/// topic renames, type renames, and schema transformations.
pub struct TransformStage {
    /// Transform configuration
    config: TransformStageConfig,
    /// Transform pipeline to apply
    transform_pipeline: Option<Arc<MultiTransform>>,
    /// Original channel information (from reader)
    channels: HashMap<u16, ChannelInfo>,
    /// Channel for receiving chunks from reader
    chunks_receiver: Receiver<MessageChunkData>,
    /// Channel for sending transformed chunks to compression
    chunks_sender: Sender<MessageChunkData>,
}

impl TransformStage {
    /// Create a new transform stage.
    ///
    /// # Arguments
    ///
    /// * `config` - Transform stage configuration
    /// * `transform_pipeline` - Optional transform pipeline (if None, chunks pass through)
    /// * `channels` - Original channel information from the input file
    /// * `chunks_receiver` - Channel for receiving chunks from reader
    /// * `chunks_sender` - Channel for sending chunks to compression
    pub fn new(
        config: TransformStageConfig,
        transform_pipeline: Option<MultiTransform>,
        channels: HashMap<u16, ChannelInfo>,
        chunks_receiver: Receiver<MessageChunkData>,
        chunks_sender: Sender<MessageChunkData>,
    ) -> Self {
        Self {
            config,
            transform_pipeline: transform_pipeline.map(Arc::new),
            channels,
            chunks_receiver,
            chunks_sender,
        }
    }

    /// Spawn the transform stage in a new thread.
    pub fn spawn(self) -> Result<thread::JoinHandle<Result<TransformStageOutput>>> {
        let handle = thread::spawn(move || self.run());
        Ok(handle)
    }

    /// Run the transform stage.
    ///
    /// Returns the transformed channel information for the writer to use.
    #[instrument(skip_all, fields(
        enabled = self.config.enabled,
        has_transform_pipeline = self.transform_pipeline.is_some(),
        num_channels = self.channels.len(),
    ))]
    fn run(self) -> Result<TransformStageOutput> {
        let start = Instant::now();

        if self.config.enabled {
            info!("Starting transform stage");
        } else {
            debug!("Transform stage disabled, passing chunks through");
        }

        // Build transformed channel metadata
        let transformed_channels = self.build_transformed_channels()?;

        // Build channel ID remapping (original -> transformed)
        let channel_id_map = self.build_channel_id_map(&transformed_channels);
        let channel_id_map_clone = channel_id_map.clone();

        // Process chunks and remap channel IDs
        let chunks_received = self.process_chunks(channel_id_map)?;

        let duration = start.elapsed();

        info!(
            chunks_received,
            channels_transformed = transformed_channels.len(),
            duration_sec = duration.as_secs_f64(),
            "Transform stage complete"
        );

        Ok(TransformStageOutput {
            transformed_channels,
            channel_id_map: channel_id_map_clone,
            chunks_received,
        })
    }

    /// Build transformed channel metadata.
    fn build_transformed_channels(&self) -> Result<HashMap<u16, TransformedChannel>> {
        let Some(ref pipeline) = self.transform_pipeline else {
            // No transform pipeline - use original channels
            return Ok(self
                .channels
                .iter()
                .map(|(id, ch)| {
                    (
                        *id,
                        TransformedChannel {
                            original_id: *id,
                            topic: ch.topic.clone(),
                            message_type: ch.message_type.clone(),
                            schema: ch.schema.clone(),
                            encoding: ch.encoding.clone(),
                            schema_encoding: ch.schema_encoding.clone(),
                        },
                    )
                })
                .collect());
        };

        // Validate transforms against channels
        let channel_list: Vec<ChannelInfo> = self.channels.values().cloned().collect();
        pipeline
            .validate(&channel_list)
            .map_err(|e| RoboflowError::encode("TransformStage", e.to_string()))?;

        // Transform each channel
        let mut transformed = HashMap::new();
        for channel in self.channels.values() {
            let transformed_channel = pipeline.transform_channel(channel);
            // Use sequential IDs starting from 0 for transformed channels
            let new_id = transformed.len() as u16;
            transformed.insert(new_id, transformed_channel);
        }

        Ok(transformed)
    }

    /// Build mapping from original channel ID to transformed channel ID.
    fn build_channel_id_map(
        &self,
        transformed_channels: &HashMap<u16, TransformedChannel>,
    ) -> HashMap<u16, u16> {
        let mut map = HashMap::new();

        // Build reverse lookup: original_id -> index in transformed_channels
        let mut original_to_index: HashMap<u16, usize> = HashMap::new();
        for (idx, (_, transformed)) in transformed_channels.iter().enumerate() {
            original_to_index.insert(transformed.original_id, idx);
        }

        // Map original channel ID to transformed channel ID
        for original_id in self.channels.keys() {
            if let Some(&idx) = original_to_index.get(original_id) {
                map.insert(*original_id, idx as u16);
            }
        }

        map
    }

    /// Process all chunks from reader and remap channel IDs.
    fn process_chunks(self, channel_id_map: HashMap<u16, u16>) -> Result<u64> {
        let mut chunks_received = 0u64;
        let chunks_sender = self.chunks_sender;

        for chunk in self.chunks_receiver {
            chunks_received += 1;

            // Remap channel IDs in chunk messages
            let transformed_chunk = transform_chunk(chunk, &channel_id_map)?;

            // Send to compression stage
            chunks_sender.send(transformed_chunk).map_err(|_| {
                RoboflowError::encode(
                    "TransformStage",
                    "Failed to send chunk to compression stage",
                )
            })?;
        }

        Ok(chunks_received)
    }
}

/// Transform a single chunk by remapping channel IDs.
///
/// This is a standalone function to avoid borrowing issues with `self`.
fn transform_chunk(
    chunk: MessageChunkData,
    channel_id_map: &HashMap<u16, u16>,
) -> Result<MessageChunkData> {
    use robocodec::io::metadata::RawMessage;

    // If no transforms, pass through unchanged
    if channel_id_map.is_empty() {
        return Ok(chunk);
    }

    // Create new chunk with remapped channel IDs
    let mut transformed = MessageChunkData::new(chunk.sequence);
    transformed.message_start_time = chunk.message_start_time;
    transformed.message_end_time = chunk.message_end_time;

    for msg in &chunk.messages {
        let new_channel_id = channel_id_map
            .get(&msg.channel_id)
            .copied()
            .unwrap_or(msg.channel_id);

        // Add message with remapped channel ID
        let transformed_msg = RawMessage {
            channel_id: new_channel_id,
            log_time: msg.log_time,
            publish_time: msg.publish_time,
            data: msg.data.clone(),
            sequence: msg.sequence,
        };
        transformed.add_message(transformed_msg);
    }

    Ok(transformed)
}

/// Output from the transform stage, containing transformed metadata.
#[derive(Debug, Clone)]
pub struct TransformStageOutput {
    /// Transformed channel information (new channel ID -> transformed channel)
    pub transformed_channels: HashMap<u16, TransformedChannel>,
    /// Mapping from original channel ID to transformed channel ID
    pub channel_id_map: HashMap<u16, u16>,
    /// Number of chunks processed
    pub chunks_received: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transform_config_default() {
        let config = TransformStageConfig::default();
        assert!(config.enabled);
        assert!(!config.verbose);
    }

    #[test]
    fn test_channel_id_map_empty() {
        let map: HashMap<u16, u16> = HashMap::new();
        assert!(map.is_empty());
    }
}
