//! Reader stage - reads messages and builds chunks using parallel reading.
//!
//! The reader stage is responsible for:
//! - Reading messages from the input file in parallel
//! - Sending chunks to the compression stage
//! - Managing backpressure when the compression channel is full

use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;
use tracing::{info, instrument};

use crossbeam_channel::Sender;

use crate::core::{Result, RoboflowError};
use robocodec::io::metadata::FileFormat;
use robocodec::io::traits::{MessageChunkData, ParallelReader, ParallelReaderConfig};

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
    format: FileFormat,
    /// Channel information
    #[allow(dead_code)]
    channels: HashMap<u16, robocodec::io::metadata::ChannelInfo>,
    /// Channel for sending chunks to compression stage
    chunks_sender: Sender<MessageChunkData>,
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
            format,
            channels,
            chunks_sender,
        }
    }

    /// Run the reader stage using parallel reading.
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

        // Create parallel reader config
        let parallel_config = ParallelReaderConfig {
            num_threads: self.config.num_threads,
            topic_filter: None,     // No filtering for pipeline
            channel_capacity: None, // No backpressure limit
            progress_interval: self.config.progress_interval,
            merge_enabled: self.config.merge_enabled,
            merge_target_size: self.config.merge_target_size,
        };

        // Create the appropriate reader based on format and read in parallel
        let stats = match self.format {
            FileFormat::Mcap => {
                use robocodec::mcap::McapFormat;
                let reader = McapFormat::open(&self.input_path)?;
                reader.read_parallel(parallel_config, self.chunks_sender)?
            }
            FileFormat::Bag => {
                use robocodec::bag::BagFormat;
                let reader = BagFormat::open(&self.input_path)?;
                reader.read_parallel(parallel_config, self.chunks_sender)?
            }
            FileFormat::Unknown => {
                return Err(RoboflowError::encode(
                    "ReaderStage",
                    "Unknown file format".to_string(),
                ));
            }
        };

        let total_time = total_start.elapsed();
        info!(
            messages_read = stats.messages_read,
            chunks_processed = stats.chunks_processed,
            total_bytes = stats.total_bytes,
            total_time_sec = total_time.as_secs_f64(),
            "Reader stage complete"
        );

        Ok(ReaderStats {
            messages_read: stats.messages_read,
            chunks_built: stats.chunks_processed as u64,
            total_bytes: stats.total_bytes,
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
