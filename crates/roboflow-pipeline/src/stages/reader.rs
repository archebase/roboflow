// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Reader stage - reads messages using parallel chunk processing.

use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;
use tracing::{info, instrument};

use crossbeam_channel::Sender;

use robocodec::io::formats::bag::ParallelBagReader;
use robocodec::io::formats::mcap::parallel::ParallelMcapReader;
use robocodec::io::metadata::{ChannelInfo, FileFormat};
use robocodec::io::traits::{MessageChunkData, ParallelReader, ParallelReaderConfig};
use roboflow_core::{Result, RoboflowError};

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
///
/// Supports both BAG and MCAP input formats.
pub struct ReaderStage {
    /// Reader configuration
    config: ReaderStageConfig,
    /// Input file path
    input_path: String,
    /// File format
    _format: FileFormat,
    /// Channel information
    _channels: HashMap<u16, ChannelInfo>,
    /// Channel for sending chunks to compression stage
    chunks_sender: Sender<MessageChunkData>,
}

impl ReaderStage {
    /// Create a new reader stage.
    pub fn new(
        config: ReaderStageConfig,
        input_path: &Path,
        channels: HashMap<u16, ChannelInfo>,
        format: FileFormat,
        chunks_sender: Sender<MessageChunkData>,
    ) -> Self {
        Self {
            config,
            input_path: input_path.to_string_lossy().to_string(),
            _format: format,
            _channels: channels,
            chunks_sender,
        }
    }

    /// Run the reader stage using parallel processing.
    ///
    /// This method blocks until all chunks have been read and sent
    /// to the compression stage.
    #[instrument(skip_all, fields(
        target_chunk_size = self.config.target_chunk_size,
        max_messages = self.config.max_messages,
    ))]
    pub fn run(self) -> Result<ReaderStats> {
        info!("Starting parallel reader stage");

        let total_start = Instant::now();

        // Build parallel reader config
        let config = ParallelReaderConfig {
            num_threads: self.config.num_threads,
            topic_filter: None,
            channel_capacity: None,
            progress_interval: self.config.progress_interval,
            merge_enabled: self.config.merge_enabled,
            merge_target_size: self.config.merge_target_size,
        };

        // Open and run the appropriate reader based on format
        let stats = match self._format {
            FileFormat::Mcap => self.run_mcap_parallel(config)?,
            FileFormat::Bag => self.run_bag_parallel(config)?,
            _ => {
                return Err(RoboflowError::parse(
                    "ReaderStage",
                    format!(
                        "Unsupported file format: {:?}. Only MCAP and BAG are supported.",
                        self._format
                    ),
                ));
            }
        };

        let total_time = total_start.elapsed();
        info!(
            messages_read = stats.messages_read,
            chunks_built = stats.chunks_built,
            total_bytes = stats.total_bytes,
            total_time_sec = total_time.as_secs_f64(),
            "Reader stage complete"
        );

        Ok(stats)
    }

    /// Run MCAP file using parallel reader.
    fn run_mcap_parallel(&self, config: ParallelReaderConfig) -> Result<ReaderStats> {
        info!("Opening MCAP file with parallel reader");

        let reader = ParallelMcapReader::open(&self.input_path).map_err(|e| {
            RoboflowError::parse("ReaderStage", format!("Failed to open MCAP file: {}", e))
        })?;

        // Run parallel reading - this sends chunks to our channel
        let parallel_stats = reader
            .read_parallel(config, self.chunks_sender.clone())
            .map_err(|e| {
                RoboflowError::parse("ReaderStage", format!("Parallel reading failed: {}", e))
            })?;

        Ok(ReaderStats {
            messages_read: parallel_stats.messages_read,
            chunks_built: parallel_stats.chunks_processed as u64,
            total_bytes: parallel_stats.total_bytes,
        })
    }

    /// Run BAG file using parallel reader.
    fn run_bag_parallel(&self, config: ParallelReaderConfig) -> Result<ReaderStats> {
        info!("Opening BAG file with parallel reader");

        let reader = ParallelBagReader::open(&self.input_path).map_err(|e| {
            RoboflowError::parse("ReaderStage", format!("Failed to open BAG file: {}", e))
        })?;

        // Run parallel reading
        let parallel_stats = reader
            .read_parallel(config, self.chunks_sender.clone())
            .map_err(|e| {
                RoboflowError::parse("ReaderStage", format!("Parallel reading failed: {}", e))
            })?;

        Ok(ReaderStats {
            messages_read: parallel_stats.messages_read,
            chunks_built: parallel_stats.chunks_processed as u64,
            total_bytes: parallel_stats.total_bytes,
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
