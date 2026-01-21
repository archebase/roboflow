// Copyright (c) 2026 ArcheBase
// Roboflow is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//     http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

//! Stage 6: CRC/Packetizer - Add CRC32 checksums for data integrity.
//!
//! This stage computes CRC32 checksums over compressed data and
//! wraps chunks in the final packet format for the writer.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use crossbeam_channel::{Receiver, Sender};
use tracing::{debug, info, instrument};

use crate::core::{Result, RoboflowError};
use crate::pipeline::hyper::types::{MessageIndexEntry, PacketizedChunk, PacketizerStats};
use robocodec::types::chunk::CompressedChunk;

/// Configuration for the CRC/packetizer stage.
#[derive(Debug, Clone)]
pub struct CrcPacketizerConfig {
    /// Enable CRC32 computation
    pub enable_crc: bool,
    /// Number of packetizer threads
    pub num_threads: usize,
}

impl Default for CrcPacketizerConfig {
    fn default() -> Self {
        Self {
            enable_crc: true,
            num_threads: 2,
        }
    }
}

/// Stage 6: CRC/Packetizer
///
/// Computes CRC32 checksums and prepares final packet format.
pub struct CrcPacketizerStage {
    config: CrcPacketizerConfig,
    receiver: Receiver<CompressedChunk>,
    sender: Sender<PacketizedChunk>,
    stats: Arc<CrcPacketizerStats>,
}

#[derive(Debug, Default)]
struct CrcPacketizerStats {
    chunks_processed: AtomicU64,
    bytes_checksummed: AtomicU64,
    crc_time_ns: AtomicU64,
}

impl CrcPacketizerStage {
    /// Create a new CRC/packetizer stage.
    pub fn new(
        config: CrcPacketizerConfig,
        receiver: Receiver<CompressedChunk>,
        sender: Sender<PacketizedChunk>,
    ) -> Self {
        Self {
            config,
            receiver,
            sender,
            stats: Arc::new(CrcPacketizerStats::default()),
        }
    }

    /// Spawn the stage in a new thread.
    pub fn spawn(self) -> Result<thread::JoinHandle<Result<PacketizerStats>>> {
        let handle = thread::spawn(move || self.run());
        Ok(handle)
    }

    /// Run the CRC/packetizer stage.
    #[instrument(skip_all, fields(enable_crc = self.config.enable_crc))]
    fn run(self) -> Result<PacketizerStats> {
        info!(
            enable_crc = self.config.enable_crc,
            threads = self.config.num_threads,
            "Starting CRC/packetizer stage"
        );

        let start = Instant::now();

        // Spawn worker threads
        let mut worker_handles = Vec::new();

        for worker_id in 0..self.config.num_threads {
            let receiver = self.receiver.clone();
            let sender = self.sender.clone();
            let stats = Arc::clone(&self.stats);
            let enable_crc = self.config.enable_crc;

            let handle =
                thread::spawn(move || Self::worker(worker_id, receiver, sender, stats, enable_crc));

            worker_handles.push(handle);
        }

        // Drop our references
        drop(self.receiver);
        drop(self.sender);

        // Wait for workers
        let mut worker_errors = Vec::new();
        for handle in worker_handles {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => worker_errors.push(e.to_string()),
                Err(_) => worker_errors.push("Packetizer worker panicked".to_string()),
            }
        }

        if !worker_errors.is_empty() {
            return Err(RoboflowError::encode(
                "CrcPacketizer",
                format!("Worker errors: {}", worker_errors.join(", ")),
            ));
        }

        let duration = start.elapsed();
        let stats = PacketizerStats {
            chunks_processed: self.stats.chunks_processed.load(Ordering::Relaxed),
            bytes_checksummed: self.stats.bytes_checksummed.load(Ordering::Relaxed),
            crc_time_sec: self.stats.crc_time_ns.load(Ordering::Relaxed) as f64 / 1e9,
        };

        info!(
            chunks = stats.chunks_processed,
            bytes_mb = stats.bytes_checksummed as f64 / (1024.0 * 1024.0),
            crc_time_sec = stats.crc_time_sec,
            duration_sec = duration.as_secs_f64(),
            "CRC/packetizer stage complete"
        );

        Ok(stats)
    }

    /// Worker function.
    fn worker(
        worker_id: usize,
        receiver: Receiver<CompressedChunk>,
        sender: Sender<PacketizedChunk>,
        stats: Arc<CrcPacketizerStats>,
        enable_crc: bool,
    ) -> Result<()> {
        debug!(worker_id, "Packetizer worker started");

        while let Ok(chunk) = receiver.recv() {
            let data_len = chunk.compressed_data.len();

            // Compute CRC32
            let (crc32, crc_time) = if enable_crc {
                let crc_start = Instant::now();
                let crc = Self::compute_crc32(&chunk.compressed_data);
                (crc, crc_start.elapsed().as_nanos() as u64)
            } else {
                (0, 0)
            };

            // Convert message indexes
            let message_indexes = chunk
                .message_indexes
                .into_iter()
                .map(|(channel_id, entries)| {
                    let converted: Vec<MessageIndexEntry> = entries
                        .into_iter()
                        .map(|e| MessageIndexEntry {
                            log_time: e.log_time,
                            offset: e.offset,
                        })
                        .collect();
                    (channel_id, converted)
                })
                .collect();

            // Create packetized chunk
            let packetized = PacketizedChunk {
                sequence: chunk.sequence,
                compressed_data: chunk.compressed_data,
                crc32,
                uncompressed_size: chunk.uncompressed_size,
                message_start_time: chunk.message_start_time,
                message_end_time: chunk.message_end_time,
                message_count: chunk.message_count,
                compression_ratio: chunk.compression_ratio,
                message_indexes,
            };

            // Send to writer
            sender
                .send(packetized)
                .map_err(|_| RoboflowError::encode("CrcPacketizer", "Channel closed"))?;

            // Update stats
            stats.chunks_processed.fetch_add(1, Ordering::Relaxed);
            stats
                .bytes_checksummed
                .fetch_add(data_len as u64, Ordering::Relaxed);
            stats.crc_time_ns.fetch_add(crc_time, Ordering::Relaxed);
        }

        debug!(worker_id, "Packetizer worker finished");
        Ok(())
    }

    /// Compute CRC32 checksum using crc32fast (hardware-accelerated).
    #[inline]
    fn compute_crc32(data: &[u8]) -> u32 {
        crc32fast::hash(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_packetizer_config_default() {
        let config = CrcPacketizerConfig::default();
        assert!(config.enable_crc);
        assert_eq!(config.num_threads, 2);
    }

    #[test]
    fn test_crc32_computation() {
        let data = b"hello world";
        let crc = CrcPacketizerStage::compute_crc32(data);
        // Known CRC32 value for "hello world"
        assert_eq!(crc, 0x0D4A1185);
    }

    #[test]
    fn test_crc32_empty() {
        let data = b"";
        let crc = CrcPacketizerStage::compute_crc32(data);
        assert_eq!(crc, 0);
    }
}
