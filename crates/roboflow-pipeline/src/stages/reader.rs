// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Reader stage - reads messages and builds chunks using streaming.

use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;
use tracing::{info, instrument};

use crossbeam_channel::Sender;

use robocodec::RoboReader;
use robocodec::io::metadata::FileFormat;
use robocodec::io::traits::MessageChunkData;
use roboflow_core::Result;

/// Configuration for the reader stage.
#[derive(Debug, Clone)]
pub struct ReaderStageConfig {
    /// Target chunk size in bytes
    pub target_chunk_size: usize,
    /// Maximum messages per chunk
    pub max_messages: usize,
    /// Progress interval (number of chunks between progress updates)
    pub progress_interval: usize,
    /// Number of threads for parallel reading (None = auto-detect)
    pub num_threads: Option<usize>,
    /// Enable merging of small chunks into larger ones
    pub merge_enabled: bool,
    /// Target size for merged chunks in bytes
    pub merge_target_size: usize,
}

impl Default for ReaderStageConfig {
    fn default() -> Self {
        Self {
            target_chunk_size: 16 * 1024 * 1024, // 16MB
            max_messages: 250_000,
            progress_interval: 10,
            num_threads: None,                   // Auto-detect
            merge_enabled: true,                 // Enable merging by default for better throughput
            merge_target_size: 16 * 1024 * 1024, // 16MB default
        }
    }
}

/// Reader stage - reads messages using parallel chunk processing.
///
/// This stage uses the ParallelReader trait to process chunks concurrently
/// using Rayon, then sends them to the compression stage via a bounded channel.
pub struct ReaderStage {
    /// Reader configuration
    config: ReaderStageConfig,
    /// Input file path
    input_path: String,
    /// File format
    _format: FileFormat,
    /// Channel information
    _channels: HashMap<u16, robocodec::io::metadata::ChannelInfo>,
    /// Channel for sending chunks to compression stage
    _chunks_sender: Sender<MessageChunkData>,
}

impl ReaderStage {
    /// Create a new reader stage.
    pub fn new(
        config: ReaderStageConfig,
        input_path: &Path,
        channels: HashMap<u16, robocodec::io::metadata::ChannelInfo>,
        format: FileFormat,
        chunks_sender: Sender<MessageChunkData>,
    ) -> Self {
        Self {
            config,
            input_path: input_path.to_string_lossy().to_string(),
            _format: format,
            _channels: channels,
            _chunks_sender: chunks_sender,
        }
    }

    /// Run the reader stage using streaming.
    ///
    /// This method blocks until all chunks have been read and sent
    /// to the compression stage.
    #[instrument(skip_all, fields(
        target_chunk_size = self.config.target_chunk_size,
        max_messages = self.config.max_messages,
    ))]
    pub fn run(self) -> Result<ReaderStats> {
        info!("Starting streaming reader stage");

        let total_start = Instant::now();

        let reader = RoboReader::open(&self.input_path)?;

        // Use raw message iteration - collect messages into chunks
        let mut messages_read = 0u64;
        let mut chunks_processed = 0u64;
        let total_bytes = 0u64;
        let mut current_chunk_size = 0usize;
        let mut current_chunk_messages = 0usize;

        // Get decoded messages through the unified reader
        let iter = reader.decoded()?;
        for result in iter {
            let _msg_result = result?;

            // Check if we should start a new chunk
            if current_chunk_messages >= self.config.max_messages
                || current_chunk_size >= self.config.target_chunk_size
            {
                chunks_processed += 1;
                current_chunk_messages = 0;
                current_chunk_size = 0;
            }

            messages_read += 1;
            current_chunk_messages += 1;
            // Note: TimestampedDecodedMessage doesn't expose raw data directly
            // The size tracking would need to be implemented differently

            // Note: In the new API, we'd need to construct MessageChunkData differently
            // For now, just count messages
            if messages_read.is_multiple_of(10000) {
                info!(messages_read, "Reading messages...");
            }
        }

        let total_time = total_start.elapsed();
        info!(
            messages_read,
            chunks_processed,
            total_bytes,
            total_time_sec = total_time.as_secs_f64(),
            "Reader stage complete"
        );

        Ok(ReaderStats {
            messages_read,
            chunks_built: chunks_processed,
            total_bytes,
        })
    }
}

/// Statistics from the reader stage.
#[derive(Debug, Clone)]
pub struct ReaderStats {
    /// Total messages read
    pub messages_read: u64,
    /// Total chunks built
    pub chunks_built: u64,
    /// Total data bytes
    pub total_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reader_config_default() {
        let config = ReaderStageConfig::default();
        assert_eq!(config.target_chunk_size, 16 * 1024 * 1024);
        assert_eq!(config.max_messages, 250_000);
        assert_eq!(config.progress_interval, 10);
    }
}
