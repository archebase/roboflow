// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! HyperPipeline orchestrator - coordinates all stages.
//!
//! The orchestrator is responsible for:
//! - Creating channels for inter-stage communication
//! - Spawning all stage threads
//! - Coordinating graceful shutdown
//! - Collecting and reporting metrics
//!
//! Architecture:
//! ```text
//! ReaderStage → CompressionStage → CrcPacketizerStage → Writer
//! ```
//!
//! The ReaderStage uses the existing ParallelReader implementation which
//! supports both BAG and MCAP input formats.

use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::bounded;
use tracing::{debug, info, instrument};

use crate::hyper::config::HyperPipelineConfig;
use crate::hyper::stages::crc_packetizer::{CrcPacketizerConfig, CrcPacketizerStage};
use crate::hyper::types::PacketizedChunk;
use crate::stages::compression::{CompressionStage, CompressionStageConfig};
use crate::stages::reader::{ReaderStage, ReaderStageConfig};
use robocodec::io::detection::detect_format;
use robocodec::io::metadata::{ChannelInfo, FileFormat};
use robocodec::io::traits::FormatReader;
use robocodec::mcap::ParallelMcapWriter;
use roboflow_core::{Result, RoboflowError};

/// Hyper-Pipeline for maximum throughput file conversion.
///
/// This pipeline uses a staged architecture for optimal performance:
///
/// 1. **Reader** - Parallel reading using ParallelReader (supports BAG and MCAP)
/// 2. **Compressor** - Parallel ZSTD compression with multiple workers
/// 3. **CRC/Packetizer** - CRC32 checksums for data integrity
/// 4. **Writer** - Sequential output with ordering guarantees
///
/// # Supported Formats
///
/// - Input: ROS BAG files, MCAP files
/// - Output: MCAP files
///
/// # Example
///
/// ```no_run
/// use roboflow::pipeline::hyper::{HyperPipeline, HyperPipelineConfig};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let config = HyperPipelineConfig::new("input.bag", "output.mcap");
/// let pipeline = HyperPipeline::new(config)?;
/// let report = pipeline.run()?;
/// println!("Throughput: {:.2} MB/s", report.throughput_mb_s);
/// # Ok(())
/// # }
/// ```
pub struct HyperPipeline {
    config: HyperPipelineConfig,
}

impl HyperPipeline {
    /// Create a new hyper-pipeline.
    pub fn new(config: HyperPipelineConfig) -> Result<Self> {
        // Validate input file exists
        if !config.input_path.exists() {
            return Err(RoboflowError::parse(
                "HyperPipeline",
                format!("Input file not found: {}", config.input_path.display()),
            ));
        }

        Ok(Self { config })
    }

    /// Create a pipeline from builder.
    pub fn builder() -> crate::hyper::config::HyperPipelineBuilder {
        crate::hyper::config::HyperPipelineBuilder::new()
    }

    /// Run the pipeline to completion.
    #[instrument(skip_all, fields(
        input = %self.config.input_path.display(),
        output = %self.config.output_path.display(),
    ))]
    pub fn run(self) -> Result<HyperPipelineReport> {
        let start = Instant::now();

        info!(
            input = %self.config.input_path.display(),
            output = %self.config.output_path.display(),
            compression_level = self.config.compression.compression_level,
            compression_threads = self.config.compression.num_threads,
            enable_crc = self.config.packetizer.enable_crc,
            "Starting HyperPipeline"
        );

        // Get input file size
        let input_size = std::fs::metadata(&self.config.input_path)
            .map(|m| m.len())
            .unwrap_or(0);

        // Detect format and get channel info
        let format = detect_format(&self.config.input_path)?;
        let channels = self.get_channel_info(&format)?;
        let channel_count = channels.len();

        info!(
            format = ?format,
            channels = channel_count,
            input_size_mb = input_size as f64 / (1024.0 * 1024.0),
            "Input file analyzed"
        );

        // Create bounded channels for inter-stage communication
        let capacity = self.config.channel_capacity;

        // Channel 1: Reader → Compression
        let (reader_tx, reader_rx) = bounded(capacity);

        // Channel 2: Compression → CRC/Packetizer
        let (compress_tx, compress_rx) = bounded(capacity);

        // Channel 3: Packetizer → Writer
        let (packet_tx, packet_rx) = bounded(capacity);

        debug!(capacity, "Created 3 inter-stage channels");

        // Spawn stages in reverse order (downstream first)

        // Stage 4: Writer
        let writer_handle = self.spawn_writer_stage(packet_rx, &channels)?;

        // Stage 3: CRC/Packetizer
        let packetizer_config = CrcPacketizerConfig {
            enable_crc: self.config.packetizer.enable_crc,
            num_threads: self.config.packetizer.num_threads,
        };
        let packetizer_stage = CrcPacketizerStage::new(packetizer_config, compress_rx, packet_tx);
        let packetizer_handle = packetizer_stage.spawn()?;

        // Stage 2: Compression
        let compression_config = CompressionStageConfig {
            num_threads: self.config.compression.num_threads,
            compression_level: self.config.compression.compression_level,
            window_log: self.config.compression.window_log,
            target_chunk_size: self.config.batcher.target_size,
            buffer_pool: self.config.compression.buffer_pool.clone(),
            ..Default::default()
        };
        let compression_stage = CompressionStage::new(compression_config, reader_rx, compress_tx);
        let compression_handle = compression_stage.spawn()?;

        // Stage 1: Reader (using ParallelReader for BAG and MCAP support)
        let reader_config = ReaderStageConfig {
            target_chunk_size: self.config.batcher.target_size,
            max_messages: self.config.batcher.max_messages,
            num_threads: Some(self.config.parser.num_threads),
            merge_enabled: true,
            merge_target_size: self.config.batcher.target_size,
            ..Default::default()
        };
        let reader_stage = ReaderStage::new(
            reader_config,
            &self.config.input_path,
            channels.clone(),
            format,
            reader_tx,
        );

        // Spawn reader in separate thread
        let reader_handle = thread::spawn(move || reader_stage.run());

        // Wait for all stages to complete

        // Wait for reader
        let reader_result = reader_handle
            .join()
            .map_err(|_| RoboflowError::encode("HyperPipeline", "Reader thread panicked"))?;
        let reader_stats = reader_result?;
        debug!(
            messages = reader_stats.messages_read,
            chunks = reader_stats.chunks_built,
            bytes_mb = reader_stats.total_bytes as f64 / (1024.0 * 1024.0),
            "Reader complete"
        );

        // Wait for compression
        let compression_result = compression_handle
            .join()
            .map_err(|_| RoboflowError::encode("HyperPipeline", "Compression thread panicked"))?;
        compression_result?;
        debug!("Compression complete");

        // Wait for packetizer
        let packetizer_result = packetizer_handle
            .join()
            .map_err(|_| RoboflowError::encode("HyperPipeline", "Packetizer thread panicked"))?;
        let packetizer_stats = packetizer_result?;
        debug!(
            chunks = packetizer_stats.chunks_processed,
            crc_time_sec = packetizer_stats.crc_time_sec,
            "Packetizer complete"
        );

        // Wait for writer
        let writer_result = writer_handle
            .join()
            .map_err(|_| RoboflowError::encode("HyperPipeline", "Writer thread panicked"))?;
        let writer_stats = writer_result?;
        debug!(
            chunks = writer_stats.chunks_written,
            bytes_mb = writer_stats.total_compressed_bytes as f64 / (1024.0 * 1024.0),
            "Writer complete"
        );

        let duration = start.elapsed();

        // Get output file size
        let output_size = std::fs::metadata(&self.config.output_path)
            .map(|m| m.len())
            .unwrap_or(0);

        let compression_ratio = if input_size > 0 {
            output_size as f64 / input_size as f64
        } else {
            1.0
        };

        let throughput_mb_s = if duration.as_secs_f64() > 0.0 {
            (input_size as f64 / (1024.0 * 1024.0)) / duration.as_secs_f64()
        } else {
            0.0
        };

        info!(
            duration_sec = duration.as_secs_f64(),
            throughput_mb_s = throughput_mb_s,
            compression_ratio = compression_ratio,
            output_size_mb = output_size as f64 / (1024.0 * 1024.0),
            "HyperPipeline complete"
        );

        Ok(HyperPipelineReport {
            input_file: self.config.input_path.display().to_string(),
            output_file: self.config.output_path.display().to_string(),
            input_size_bytes: input_size,
            output_size_bytes: output_size,
            duration,
            throughput_mb_s,
            compression_ratio,
            message_count: reader_stats.messages_read,
            chunks_written: writer_stats.chunks_written,
            crc_enabled: self.config.packetizer.enable_crc,
        })
    }

    /// Get channel info from input file.
    fn get_channel_info(&self, format: &FileFormat) -> Result<HashMap<u16, ChannelInfo>> {
        match format {
            FileFormat::Mcap => {
                use robocodec::mcap::McapFormat;
                let reader = McapFormat::open(&self.config.input_path)?;
                Ok(reader.channels().clone())
            }
            FileFormat::Bag => {
                use robocodec::bag::BagFormat;
                let reader = BagFormat::open(&self.config.input_path)?;
                Ok(reader.channels().clone())
            }
            FileFormat::Unknown => Err(RoboflowError::parse(
                "HyperPipeline",
                format!("Unknown file format: {}", self.config.input_path.display()),
            )),
        }
    }

    /// Spawn the writer stage.
    fn spawn_writer_stage(
        &self,
        receiver: crossbeam_channel::Receiver<PacketizedChunk>,
        channels: &HashMap<u16, ChannelInfo>,
    ) -> Result<std::thread::JoinHandle<Result<WriterStats>>> {
        let output_path = self.config.output_path.clone();
        let buffer_size = self.config.writer.buffer_size;
        let flush_interval = self.config.writer.flush_interval;
        let channels = channels.clone();

        let handle = std::thread::spawn(move || {
            Self::writer_thread(output_path, buffer_size, flush_interval, receiver, channels)
        });

        Ok(handle)
    }

    /// Writer thread function.
    fn writer_thread(
        output_path: std::path::PathBuf,
        buffer_size: usize,
        flush_interval: u64,
        receiver: crossbeam_channel::Receiver<PacketizedChunk>,
        channels: HashMap<u16, ChannelInfo>,
    ) -> Result<WriterStats> {
        info!("Starting writer stage");

        // Create output file
        let file = File::create(&output_path).map_err(|e| {
            RoboflowError::encode("Writer", format!("Failed to create output file: {e}"))
        })?;

        let buffered_writer = BufWriter::with_capacity(buffer_size, file);
        let mut writer = ParallelMcapWriter::new(buffered_writer)?;

        // Write schemas and channels
        let mut schema_ids: HashMap<String, u16> = HashMap::new();

        for (&original_id, channel) in &channels {
            let schema_id = if let Some(schema) = &channel.schema {
                let encoding = channel.schema_encoding.as_deref().unwrap_or("ros1msg");
                if let Some(&existing_id) = schema_ids.get(&channel.message_type) {
                    existing_id
                } else {
                    let id = writer
                        .add_schema(&channel.message_type, encoding, schema.as_bytes())
                        .map_err(|e| {
                            RoboflowError::encode(
                                "Writer",
                                format!("Failed to add schema for {}: {}", channel.message_type, e),
                            )
                        })?;
                    schema_ids.insert(channel.message_type.clone(), id);
                    id
                }
            } else {
                0
            };

            writer
                .add_channel_with_id(
                    original_id,
                    schema_id,
                    &channel.topic,
                    &channel.encoding,
                    &HashMap::new(),
                )
                .map_err(|e| {
                    RoboflowError::encode(
                        "Writer",
                        format!("Failed to add channel {}: {}", channel.topic, e),
                    )
                })?;
        }

        info!(
            schemas = schema_ids.len(),
            channels = channels.len(),
            "Writer registered schemas and channels"
        );

        // Write chunks with ordering
        let mut chunk_buffer: HashMap<u64, PacketizedChunk> = HashMap::new();
        let mut next_sequence = 0u64;
        let mut chunks_written = 0u64;
        let mut chunks_since_flush = 0u64;
        let mut total_compressed_bytes = 0u64;

        const MAX_BUFFER_SIZE: usize = 1024;

        while let Ok(packet) = receiver.recv() {
            if packet.sequence == next_sequence {
                // Write immediately
                total_compressed_bytes += packet.compressed_data.len() as u64;
                let compressed_chunk = packet.into_compressed_chunk();
                writer.write_compressed_chunk(compressed_chunk)?;
                chunks_written += 1;
                chunks_since_flush += 1;
                next_sequence += 1;

                // Periodic flush
                if flush_interval > 0 && chunks_since_flush >= flush_interval {
                    writer.flush()?;
                    chunks_since_flush = 0;
                }

                // Drain buffer
                while let Some(buffered) = chunk_buffer.remove(&next_sequence) {
                    total_compressed_bytes += buffered.compressed_data.len() as u64;
                    let compressed_chunk = buffered.into_compressed_chunk();
                    writer.write_compressed_chunk(compressed_chunk)?;
                    chunks_written += 1;
                    chunks_since_flush += 1;
                    next_sequence += 1;

                    if flush_interval > 0 && chunks_since_flush >= flush_interval {
                        writer.flush()?;
                        chunks_since_flush = 0;
                    }
                }
            } else {
                // Buffer out-of-order chunk
                if chunk_buffer.len() >= MAX_BUFFER_SIZE {
                    return Err(RoboflowError::encode(
                        "Writer",
                        format!(
                            "Chunk buffer overflow: waiting for {}, got {}",
                            next_sequence, packet.sequence
                        ),
                    ));
                }
                chunk_buffer.insert(packet.sequence, packet);
            }
        }

        // Final flush and finish
        writer.flush()?;
        writer.finish()?;

        info!(
            chunks = chunks_written,
            bytes_mb = total_compressed_bytes as f64 / (1024.0 * 1024.0),
            "Writer complete"
        );

        Ok(WriterStats {
            chunks_written,
            total_compressed_bytes,
        })
    }
}

/// Statistics from the writer stage.
#[derive(Debug, Clone)]
struct WriterStats {
    chunks_written: u64,
    total_compressed_bytes: u64,
}

/// Report from a hyper-pipeline run.
#[derive(Debug, Clone)]
pub struct HyperPipelineReport {
    /// Input file path
    pub input_file: String,
    /// Output file path
    pub output_file: String,
    /// Input file size in bytes
    pub input_size_bytes: u64,
    /// Output file size in bytes
    pub output_size_bytes: u64,
    /// Total duration
    pub duration: Duration,
    /// Throughput in MB/s
    pub throughput_mb_s: f64,
    /// Compression ratio (output / input)
    pub compression_ratio: f64,
    /// Number of messages processed
    pub message_count: u64,
    /// Number of chunks written
    pub chunks_written: u64,
    /// Whether CRC was enabled
    pub crc_enabled: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hyper_pipeline_builder() {
        let result = HyperPipeline::builder()
            .input_path("/nonexistent/input.bag")
            .output_path("/tmp/output.mcap")
            .compression_level(3)
            .enable_crc(true)
            .build();

        // Should fail because input doesn't exist
        // But builder should work
        assert!(result.is_ok());
    }
}
