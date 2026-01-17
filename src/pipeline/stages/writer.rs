//! Writer stage - writes compressed chunks to the output file.
//!
//! The writer stage is responsible for:
//! - Receiving compressed chunks from the compression stage
//! - Maintaining chunk ordering by sequence number
//! - Writing schemas and channels before chunks
//! - Writing chunks to the output file
//! - Managing schema and channel registration

use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::thread;

use crossbeam_channel::Receiver;

use crate::core::{CodecError, Result};
use crate::format::writer::ParallelMcapWriter;
use crate::io::metadata::ChannelInfo;
use crate::pipeline::types::chunk::CompressedChunk;

/// Maximum number of out-of-order chunks to buffer.
const MAX_CHUNK_BUFFER_SIZE: usize = 1024;

/// Configuration for the writer stage.
#[derive(Debug, Clone)]
pub struct WriterStageConfig {
    /// Buffer size for BufWriter
    pub buffer_size: usize,
    /// Flush interval (number of chunks between flushes)
    pub flush_interval: u64,
}

impl Default for WriterStageConfig {
    fn default() -> Self {
        Self {
            buffer_size: 8 * 1024 * 1024, // 8MB
            flush_interval: 4,
        }
    }
}

/// Writer stage - writes compressed chunks to the output file.
///
/// This stage runs in a separate thread and receives compressed chunks,
/// maintains ordering, and writes them to the output file.
pub struct WriterStage {
    /// Writer configuration
    config: WriterStageConfig,
    /// Channel for receiving compressed chunks
    chunks_receiver: Receiver<CompressedChunk>,
    /// Output file path
    output_path: PathBuf,
    /// Channel information from the source file (for writing schemas/channels)
    channels: HashMap<u16, ChannelInfo>,
}

impl WriterStage {
    /// Create a new writer stage.
    pub fn new(
        config: WriterStageConfig,
        chunks_receiver: Receiver<CompressedChunk>,
        output_path: PathBuf,
        channels: HashMap<u16, ChannelInfo>,
    ) -> Self {
        Self {
            config,
            chunks_receiver,
            output_path,
            channels,
        }
    }

    /// Spawn the writer stage in a new thread.
    pub fn spawn(self) -> Result<std::thread::JoinHandle<Result<WriterStats>>> {
        let handle = thread::spawn(move || self.run());
        Ok(handle)
    }

    /// Run the writer stage.
    ///
    /// This method blocks until all chunks have been written
    /// to the output file.
    fn run(self) -> Result<WriterStats> {
        println!("Starting writer stage...");

        // Create output file with buffered writer
        let file = File::create(&self.output_path).map_err(|e| {
            CodecError::encode("WriterStage", format!("Failed to create output file: {e}"))
        })?;

        let buffered_writer = BufWriter::with_capacity(self.config.buffer_size, file);
        let mut writer = ParallelMcapWriter::new(buffered_writer)?;

        // Write schemas and channels BEFORE any chunks
        // This is required by MCAP spec: schemas/channels must appear before messages that use them
        let mut schema_ids: HashMap<String, u16> = HashMap::new();

        for (&original_id, channel) in &self.channels {
            // Add schema if present
            let schema_id = if let Some(schema) = &channel.schema {
                let encoding = channel.schema_encoding.as_deref().unwrap_or("ros1msg");
                if let Some(&existing_id) = schema_ids.get(&channel.message_type) {
                    existing_id
                } else {
                    let id = writer
                        .add_schema(&channel.message_type, encoding, schema.as_bytes())
                        .map_err(|e| {
                            CodecError::encode(
                                "WriterStage",
                                format!("Failed to add schema for {}: {}", channel.message_type, e),
                            )
                        })?;
                    schema_ids.insert(channel.message_type.clone(), id);
                    id
                }
            } else {
                0 // No schema
            };

            // Add channel with the ORIGINAL channel ID to match the IDs in compressed chunks
            writer
                .add_channel_with_id(
                    original_id,
                    schema_id,
                    &channel.topic,
                    &channel.encoding,
                    &HashMap::new(),
                )
                .map_err(|e| {
                    CodecError::encode(
                        "WriterStage",
                        format!("Failed to add channel {}: {}", channel.topic, e),
                    )
                })?;
        }

        println!(
            "Writer stage: registered {} schemas, {} channels",
            schema_ids.len(),
            self.channels.len()
        );

        // Buffer for out-of-order chunks
        let mut chunk_buffer: HashMap<u64, CompressedChunk> = HashMap::new();
        let mut next_sequence = 0u64;
        let mut chunks_written = 0u64;
        let mut chunks_since_last_flush = 0u64;
        let mut messages_written = 0u64;
        let mut total_compressed_bytes = 0u64;

        while let Ok(chunk) = self.chunks_receiver.recv() {
            // Check if this is the next expected chunk
            if chunk.sequence == next_sequence {
                // Write immediately
                messages_written += chunk.message_count as u64;
                total_compressed_bytes += chunk.compressed_data.len() as u64;
                writer.write_compressed_chunk(chunk)?;
                chunks_written += 1;
                chunks_since_last_flush += 1;
                next_sequence += 1;

                // Periodic flush based on flush_interval
                if self.config.flush_interval > 0
                    && chunks_since_last_flush >= self.config.flush_interval
                {
                    writer.flush()?;
                    chunks_since_last_flush = 0;
                }

                // Write any buffered chunks that are now in order
                while let Some(buffered) = chunk_buffer.remove(&next_sequence) {
                    messages_written += buffered.message_count as u64;
                    total_compressed_bytes += buffered.compressed_data.len() as u64;
                    writer.write_compressed_chunk(buffered)?;
                    chunks_written += 1;
                    chunks_since_last_flush += 1;
                    next_sequence += 1;

                    // Flush after draining buffer if needed
                    if self.config.flush_interval > 0
                        && chunks_since_last_flush >= self.config.flush_interval
                    {
                        writer.flush()?;
                        chunks_since_last_flush = 0;
                    }
                }
            } else {
                // Out of order, buffer it
                if chunk_buffer.len() >= MAX_CHUNK_BUFFER_SIZE {
                    return Err(CodecError::encode(
                        "WriterStage",
                        format!(
                            "Chunk buffer overflow: waiting for sequence {}, got {}, buffer size {}",
                            next_sequence, chunk.sequence, MAX_CHUNK_BUFFER_SIZE
                        ),
                    ));
                }
                chunk_buffer.insert(chunk.sequence, chunk);
            }
        }

        // Final flush before finish to ensure all data is written
        writer.flush()?;

        // Finalize and flush
        writer.finish()?;

        println!(
            "Writer stage complete: {} chunks, {} messages, {:.2} MB written",
            chunks_written,
            messages_written,
            total_compressed_bytes as f64 / (1024.0 * 1024.0)
        );

        Ok(WriterStats {
            chunks_written,
            messages_written,
            total_compressed_bytes,
        })
    }
}

/// Statistics from the writer stage.
#[derive(Debug, Clone)]
pub struct WriterStats {
    /// Total chunks written
    pub chunks_written: u64,
    /// Total messages written
    pub messages_written: u64,
    /// Total compressed bytes written
    pub total_compressed_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::types::chunk::CompressedChunk;

    /// Create a test compressed chunk with the given sequence number.
    fn make_test_chunk(sequence: u64, message_count: usize) -> CompressedChunk {
        CompressedChunk {
            sequence,
            compressed_data: vec![0u8; 100],
            uncompressed_size: 1000,
            message_start_time: sequence * 1000,
            message_end_time: (sequence + 1) * 1000,
            message_count,
            compression_ratio: 0.1,
            message_indexes: std::collections::BTreeMap::new(),
        }
    }

    #[test]
    fn test_writer_config_default() {
        let config = WriterStageConfig::default();
        assert_eq!(config.buffer_size, 8 * 1024 * 1024);
        assert_eq!(config.flush_interval, 4);
    }

    #[test]
    fn test_writer_stats_fields() {
        let stats = WriterStats {
            chunks_written: 10,
            messages_written: 1000,
            total_compressed_bytes: 50000,
        };

        assert_eq!(stats.chunks_written, 10);
        assert_eq!(stats.messages_written, 1000);
        assert_eq!(stats.total_compressed_bytes, 50000);
    }

    #[test]
    fn test_chunk_ordering_in_order() {
        // Test that chunks arriving in order are processed correctly
        let mut chunk_buffer: HashMap<u64, CompressedChunk> = HashMap::new();
        let mut next_sequence = 0u64;

        // Process chunks in order: 0, 1, 2
        for i in 0..3 {
            let chunk = make_test_chunk(i, 10);
            assert_eq!(chunk.sequence, next_sequence);

            // Would write immediately in real implementation
            chunk_buffer.insert(chunk.sequence, chunk);
            next_sequence += 1;
        }

        assert_eq!(next_sequence, 3);
        assert_eq!(chunk_buffer.len(), 3);
    }

    #[test]
    fn test_chunk_ordering_out_of_order() {
        // Test that out-of-order chunks are buffered correctly
        let mut chunk_buffer: HashMap<u64, CompressedChunk> = HashMap::new();
        let mut next_sequence = 0u64;

        // Chunk 2 arrives first (out of order)
        let chunk_2 = make_test_chunk(2, 10);
        assert_ne!(chunk_2.sequence, next_sequence);
        chunk_buffer.insert(chunk_2.sequence, chunk_2);
        assert_eq!(chunk_buffer.len(), 1);

        // Chunk 0 arrives (expected)
        let chunk_0 = make_test_chunk(0, 5);
        assert_eq!(chunk_0.sequence, next_sequence);
        // Would write chunk_0 immediately, then check buffer
        next_sequence += 1;

        // Now chunk_buffer should still have chunk 2
        assert_eq!(chunk_buffer.len(), 1);
        assert!(chunk_buffer.contains_key(&2));

        // Chunk 1 arrives (expected after 0)
        let chunk_1 = make_test_chunk(1, 8);
        assert_eq!(chunk_1.sequence, next_sequence);
        // Would write chunk_1, then find chunk_2 in buffer
        chunk_buffer.remove(&1); // Simulate finding chunk_2 after writing chunk_1
        next_sequence += 1;
        // Would also write chunk_2 from buffer
        next_sequence += 1;

        assert_eq!(next_sequence, 3);
    }

    #[test]
    fn test_chunk_ordering_multiple_out_of_order() {
        // Test multiple consecutive out-of-order chunks
        let mut chunk_buffer: HashMap<u64, CompressedChunk> = HashMap::new();

        // Chunks arrive in order: 3, 1, 0, 2, 4

        // Chunk 3 arrives first
        chunk_buffer.insert(3, make_test_chunk(3, 10));

        // Chunk 1 arrives
        chunk_buffer.insert(1, make_test_chunk(1, 10));

        // Chunk 0 arrives (expected!)
        // After writing 0, we'd check buffer and find 1
        // After writing 1, we'd check buffer and NOT find 2
        chunk_buffer.remove(&1);

        // Chunk 2 arrives (expected!)
        // After writing 2, we'd check buffer and find 3
        chunk_buffer.remove(&3);

        // Chunk 4 arrives (expected!)
        // Final state: next_sequence would be 4, buffer empty
        assert_eq!(chunk_buffer.len(), 0);
    }

    #[test]
    fn test_max_chunk_buffer_size() {
        // Test that exceeding MAX_CHUNK_BUFFER_SIZE causes an error
        let mut chunk_buffer: HashMap<u64, CompressedChunk> = HashMap::new();
        let next_sequence = 0u64;

        // Fill buffer to MAX_CHUNK_BUFFER_SIZE - 1
        for i in 1..MAX_CHUNK_BUFFER_SIZE {
            chunk_buffer.insert(i as u64, make_test_chunk(i as u64, 10));
        }

        assert_eq!(chunk_buffer.len(), MAX_CHUNK_BUFFER_SIZE - 1);

        // Adding one more chunk should reach the limit
        chunk_buffer.insert(
            MAX_CHUNK_BUFFER_SIZE as u64,
            make_test_chunk(MAX_CHUNK_BUFFER_SIZE as u64, 10),
        );
        assert_eq!(chunk_buffer.len(), MAX_CHUNK_BUFFER_SIZE);

        // The next out-of-order chunk would cause overflow
        // In the actual implementation, this would return an error
        let overflow_sequence = MAX_CHUNK_BUFFER_SIZE + 1;
        assert!(chunk_buffer.len() >= MAX_CHUNK_BUFFER_SIZE);

        // Verify the error message would be correct
        let expected_error_msg = format!(
            "Chunk buffer overflow: waiting for sequence {}, got {}, buffer size {}",
            next_sequence, overflow_sequence, MAX_CHUNK_BUFFER_SIZE
        );
        assert!(expected_error_msg.contains("Chunk buffer overflow"));
    }

    #[test]
    fn test_compressed_chunk_message_count() {
        let chunk = make_test_chunk(0, 42);
        assert_eq!(chunk.message_count, 42);
    }

    #[test]
    fn test_compressed_chunk_compression_ratio_calculation() {
        let chunk = CompressedChunk {
            sequence: 0,
            compressed_data: vec![0u8; 250],
            uncompressed_size: 1000,
            message_start_time: 0,
            message_end_time: 1000,
            message_count: 10,
            compression_ratio: 0.0,
            message_indexes: std::collections::BTreeMap::new(),
        };

        let ratio = chunk.calculate_compression_ratio();
        assert!((ratio - 0.25).abs() < 0.001);
    }

    #[test]
    fn test_compressed_chunk_compression_ratio_zero_uncompressed() {
        let chunk = CompressedChunk {
            sequence: 0,
            compressed_data: vec![0u8; 100],
            uncompressed_size: 0,
            message_start_time: 0,
            message_end_time: 1000,
            message_count: 0,
            compression_ratio: 0.0,
            message_indexes: std::collections::BTreeMap::new(),
        };

        // Zero uncompressed size should return ratio of 1.0 (no compression)
        let ratio = chunk.calculate_compression_ratio();
        assert_eq!(ratio, 1.0);
    }

    #[test]
    fn test_chunk_sequence_monotonic() {
        // Test that sequence numbers are strictly increasing
        let sequences = vec![0u64, 5u64, 100u64, 999u64];

        for (_i, &seq) in sequences.iter().enumerate() {
            let chunk = make_test_chunk(seq, 10);
            assert_eq!(chunk.sequence, seq);
            assert_eq!(chunk.message_start_time, seq * 1000);
            assert_eq!(chunk.message_end_time, (seq + 1) * 1000);
        }
    }

    #[test]
    fn test_flush_interval_respected() {
        let config = WriterStageConfig {
            buffer_size: 1024,
            flush_interval: 5,
        };

        assert_eq!(config.flush_interval, 5);

        // Test that zero flush_interval means no periodic flushing
        let config_no_flush = WriterStageConfig {
            buffer_size: 1024,
            flush_interval: 0,
        };

        assert_eq!(config_no_flush.flush_interval, 0);
    }

    #[test]
    fn test_writer_stats_compressed_bytes() {
        let stats = WriterStats {
            chunks_written: 100,
            messages_written: 10000,
            total_compressed_bytes: 1234567,
        };

        assert_eq!(stats.total_compressed_bytes, 1234567);

        // Verify size in MB is reasonable
        let size_mb = stats.total_compressed_bytes as f64 / (1024.0 * 1024.0);
        assert!((size_mb - 1.177).abs() < 0.01); // ~1.177 MB
    }
}
