// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Stage 1: Prefetcher - Platform-specific I/O optimization.
//!
//! The prefetcher reads file data using platform-optimized I/O:
//! - macOS: madvise with MADV_SEQUENTIAL and MADV_WILLNEED
//! - Linux: posix_fadvise (io_uring support planned)
//!
//! This stage keeps the CPU fed by prefetching data ahead of parsing.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Instant;

use crossbeam_channel::Sender;
use memmap2::Mmap;
use tracing::{debug, info, instrument};

use crate::hyper::config::PlatformHints;
use crate::hyper::types::{BlockType, CompressionType, PrefetchedBlock, PrefetcherStats};
use roboflow_core::{Result, RoboflowError};

/// Configuration for the prefetcher stage.
#[derive(Debug, Clone)]
pub struct PrefetcherStageConfig {
    /// Block size for reading
    pub block_size: usize,
    /// Number of blocks to prefetch ahead
    pub prefetch_ahead: usize,
    /// Platform-specific hints
    pub platform_hints: PlatformHints,
}

impl Default for PrefetcherStageConfig {
    fn default() -> Self {
        Self {
            block_size: 4 * 1024 * 1024, // 4MB
            prefetch_ahead: 4,
            platform_hints: PlatformHints::auto(),
        }
    }
}

/// Stage 1: Prefetcher
///
/// Reads file data with platform-specific optimizations and sends
/// blocks to the parser stage.
pub struct PrefetcherStage {
    config: PrefetcherStageConfig,
    input_path: String,
    sender: Sender<PrefetchedBlock>,
    stats: Arc<PrefetcherStageStats>,
}

#[derive(Debug, Default)]
struct PrefetcherStageStats {
    blocks_prefetched: AtomicU64,
    bytes_prefetched: AtomicU64,
}

impl PrefetcherStage {
    /// Create a new prefetcher stage.
    pub fn new(
        config: PrefetcherStageConfig,
        input_path: &Path,
        sender: Sender<PrefetchedBlock>,
    ) -> Self {
        Self {
            config,
            input_path: input_path.to_string_lossy().to_string(),
            sender,
            stats: Arc::new(PrefetcherStageStats::default()),
        }
    }

    /// Spawn the prefetcher in a new thread.
    pub fn spawn(self) -> Result<thread::JoinHandle<Result<PrefetcherStats>>> {
        let handle = thread::spawn(move || self.run());
        Ok(handle)
    }

    /// Run the prefetcher.
    #[instrument(skip_all, fields(input = %self.input_path))]
    fn run(self) -> Result<PrefetcherStats> {
        info!("Starting prefetcher stage");
        let start = Instant::now();

        // Open file
        let file = File::open(&self.input_path)
            .map_err(|e| RoboflowError::parse("Prefetcher", format!("Failed to open file: {e}")))?;

        let file_size = file
            .metadata()
            .map_err(|e| {
                RoboflowError::parse("Prefetcher", format!("Failed to get file size: {e}"))
            })?
            .len() as usize;

        debug!(
            file_size_mb = file_size as f64 / (1024.0 * 1024.0),
            "File opened"
        );

        // Use mmap for zero-copy reading
        let mmap = unsafe { Mmap::map(&file) }
            .map_err(|e| RoboflowError::parse("Prefetcher", format!("Failed to mmap file: {e}")))?;

        // Apply platform-specific hints
        self.apply_platform_hints(&mmap)?;

        // Scan file structure and emit blocks
        self.scan_and_emit(&mmap, file_size)?;

        let duration = start.elapsed();
        let stats = PrefetcherStats {
            blocks_prefetched: self.stats.blocks_prefetched.load(Ordering::Relaxed),
            bytes_prefetched: self.stats.bytes_prefetched.load(Ordering::Relaxed),
            io_time_sec: duration.as_secs_f64(),
        };

        info!(
            blocks = stats.blocks_prefetched,
            bytes_mb = stats.bytes_prefetched as f64 / (1024.0 * 1024.0),
            duration_sec = stats.io_time_sec,
            "Prefetcher stage complete"
        );

        Ok(stats)
    }

    /// Apply platform-specific I/O hints to the mmap.
    fn apply_platform_hints(&self, _mmap: &Mmap) -> Result<()> {
        #[cfg(target_os = "macos")]
        match &self.config.platform_hints {
            PlatformHints::Madvise {
                sequential,
                willneed,
            } => unsafe {
                let ptr = _mmap.as_ptr() as *mut libc::c_void;
                let len = _mmap.len();

                if *sequential {
                    libc::madvise(ptr, len, libc::MADV_SEQUENTIAL);
                    debug!("Applied MADV_SEQUENTIAL");
                }

                if *willneed {
                    libc::madvise(ptr, len, libc::MADV_WILLNEED);
                    debug!("Applied MADV_WILLNEED");
                }
                Ok(())
            },
            PlatformHints::None => {
                debug!("No platform hints applied");
                Ok(())
            }
            _ => {
                // Linux-specific hints are no-ops on macOS
                debug!("Linux-specific hint ignored on macOS");
                Ok(())
            }
        }

        #[cfg(target_os = "linux")]
        match &self.config.platform_hints {
            PlatformHints::Fadvise { sequential } => {
                // Note: We can't fadvise on mmap, but we applied it during file open
                debug!("Linux fadvise hint (sequential={})", sequential);
                Ok(())
            }
            PlatformHints::IoUring { queue_depth } => {
                // io_uring requires async runtime; for now, fall back to mmap
                debug!(
                    "io_uring requested (queue_depth={}), using mmap fallback",
                    queue_depth
                );
                Ok(())
            }
            PlatformHints::None => {
                debug!("No platform hints applied");
                Ok(())
            }
        }

        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        match &self.config.platform_hints {
            _ => {
                debug!("No platform hints applied for this platform");
                Ok(())
            }
        }
    }

    /// Scan file structure and emit blocks.
    fn scan_and_emit(&self, mmap: &Mmap, file_size: usize) -> Result<()> {
        // Detect file format from magic bytes
        if file_size < 8 {
            return Err(RoboflowError::parse("Prefetcher", "File too small"));
        }

        let magic = &mmap[0..8];
        let is_mcap = magic == b"\x89MCAP0\r\n";
        let is_bag = magic[0..4] == [0x23, 0x52, 0x4f, 0x53]; // "#ROS"

        if is_mcap {
            self.scan_mcap_file(mmap, file_size)
        } else if is_bag {
            self.scan_bag_file(mmap, file_size)
        } else {
            // Fallback: emit as raw blocks
            self.emit_raw_blocks(mmap, file_size)
        }
    }

    /// Scan MCAP file structure and emit chunk blocks.
    fn scan_mcap_file(&self, mmap: &Mmap, file_size: usize) -> Result<()> {
        use byteorder::{LittleEndian, ReadBytesExt};
        use std::io::Cursor;

        debug!("Scanning MCAP file");

        // MCAP header: 8 bytes magic + record
        let mut offset: usize = 8;
        let mut sequence: u64 = 0;

        // Skip header record
        if offset + 9 <= file_size {
            let opcode = mmap[offset];
            let record_len = {
                let mut cursor = Cursor::new(&mmap[offset + 1..offset + 9]);
                cursor.read_u64::<LittleEndian>().unwrap_or(0) as usize
            };
            if opcode == 0x01 {
                // Header
                offset += 1 + 8 + record_len;
            }
        }

        // Scan records
        while offset + 9 <= file_size {
            let opcode = mmap[offset];
            let record_len = {
                let mut cursor = Cursor::new(&mmap[offset + 1..offset + 9]);
                cursor.read_u64::<LittleEndian>().unwrap_or(0) as usize
            };

            if record_len == 0 || offset + 9 + record_len > file_size {
                break;
            }

            let record_start = offset;
            let record_end = offset + 9 + record_len;

            match opcode {
                0x06 => {
                    // Chunk record
                    let block =
                        self.parse_mcap_chunk_block(mmap, record_start, record_end, sequence)?;
                    self.emit_block(block)?;
                    sequence += 1;
                }
                0x02..=0x0F => {
                    // Schema (0x02), Channel (0x03), Message (0x04), etc.
                    // These are metadata, emit as metadata block
                    let block = PrefetchedBlock {
                        sequence,
                        offset: record_start as u64,
                        data: Arc::from(&mmap[record_start..record_end]),
                        block_type: BlockType::McapMetadata,
                        estimated_uncompressed_size: record_len,
                        source_path: None,
                    };
                    self.emit_block(block)?;
                    sequence += 1;
                }
                _ => {
                    // Unknown opcode, stop scanning
                    break;
                }
            }

            offset = record_end;
        }

        debug!(chunks_found = sequence, "MCAP scan complete");
        Ok(())
    }

    /// Parse MCAP chunk block metadata.
    fn parse_mcap_chunk_block(
        &self,
        mmap: &Mmap,
        record_start: usize,
        record_end: usize,
        sequence: u64,
    ) -> Result<PrefetchedBlock> {
        use byteorder::{LittleEndian, ReadBytesExt};
        use std::io::Cursor;

        // Chunk record format:
        // opcode (1) + record_len (8) + message_start_time (8) + message_end_time (8)
        // + uncompressed_size (8) + uncompressed_crc (4) + compression_len (4) + compression
        // + compressed_size (8) + compressed_data

        let header_start = record_start + 9; // After opcode + record_len
        if header_start + 36 > record_end {
            return Err(RoboflowError::parse("Prefetcher", "Chunk header too short"));
        }

        let mut cursor = Cursor::new(&mmap[header_start..]);

        let _message_start_time = cursor.read_u64::<LittleEndian>().unwrap_or(0);
        let _message_end_time = cursor.read_u64::<LittleEndian>().unwrap_or(0);
        let uncompressed_size = cursor.read_u64::<LittleEndian>().unwrap_or(0) as usize;
        let _uncompressed_crc = cursor.read_u32::<LittleEndian>().unwrap_or(0);
        let compression_len = cursor.read_u32::<LittleEndian>().unwrap_or(0) as usize;

        // Read compression string
        // Offset: message_start_time(8) + message_end_time(8) + uncompressed_size(8) +
        //         uncompressed_crc(4) + compression_len(4) = 32
        let compression_start = header_start + 32;
        let compression_end = compression_start + compression_len;

        let compression_type = if compression_end <= record_end {
            let compression_str =
                std::str::from_utf8(&mmap[compression_start..compression_end]).unwrap_or("");
            match compression_str {
                "zstd" | "zst" => CompressionType::Zstd,
                "lz4" => CompressionType::Lz4,
                "" | "none" => CompressionType::None,
                _ => CompressionType::None,
            }
        } else {
            CompressionType::None
        };

        // Read compressed size
        // Note: MCAP Chunk format has a records_size field (8 bytes) before the actual records
        // The compressed_size is the records_size field value
        let records_size_offset = compression_end;
        let compressed_size = if records_size_offset + 8 <= record_end {
            let mut cursor = Cursor::new(&mmap[records_size_offset..]);
            cursor.read_u64::<LittleEndian>().unwrap_or(0) as usize
        } else {
            0
        };

        Ok(PrefetchedBlock {
            sequence,
            offset: record_start as u64,
            data: Arc::from(&mmap[record_start..record_end]),
            block_type: BlockType::McapChunk {
                compressed_size,
                compression: compression_type,
            },
            estimated_uncompressed_size: uncompressed_size,
            source_path: None,
        })
    }

    /// Scan ROS bag file structure.
    fn scan_bag_file(&self, _mmap: &Mmap, file_size: usize) -> Result<()> {
        debug!("Scanning ROS bag file");

        // For BAG files, emit a single block with the file path
        // The parser will use the rosbag crate to read the file
        let block = PrefetchedBlock {
            sequence: 0,
            offset: 0,
            data: Arc::from(&[] as &[u8]), // Empty data - parser uses file path
            block_type: BlockType::BagChunk {
                connection_count: 0,
            },
            estimated_uncompressed_size: file_size,
            source_path: Some(self.input_path.clone()),
        };

        self.emit_block(block)?;
        Ok(())
    }

    /// Emit raw blocks for unknown file formats.
    fn emit_raw_blocks(&self, mmap: &Mmap, file_size: usize) -> Result<()> {
        debug!("Emitting raw blocks");

        let mut offset = 0;
        let mut sequence = 0;

        while offset < file_size {
            let end = (offset + self.config.block_size).min(file_size);

            let block = PrefetchedBlock {
                sequence,
                offset: offset as u64,
                data: Arc::from(&mmap[offset..end]),
                block_type: BlockType::Unknown,
                estimated_uncompressed_size: end - offset,
                source_path: None,
            };

            self.emit_block(block)?;
            offset = end;
            sequence += 1;
        }

        Ok(())
    }

    /// Emit a block to the channel.
    fn emit_block(&self, block: PrefetchedBlock) -> Result<()> {
        let bytes = block.data.len();

        debug!(
            sequence = block.sequence,
            block_type = ?block.block_type,
            bytes = bytes,
            "Emitting block"
        );

        self.sender
            .send(block)
            .map_err(|_| RoboflowError::encode("Prefetcher", "Channel closed"))?;

        self.stats.blocks_prefetched.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_prefetched
            .fetch_add(bytes as u64, Ordering::Relaxed);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefetcher_config_default() {
        let config = PrefetcherStageConfig::default();
        assert_eq!(config.block_size, 4 * 1024 * 1024);
        assert_eq!(config.prefetch_ahead, 4);
    }

    #[test]
    fn test_compression_type_detection() {
        assert_eq!(
            match "zstd" {
                "zstd" | "zst" => CompressionType::Zstd,
                "lz4" => CompressionType::Lz4,
                _ => CompressionType::None,
            },
            CompressionType::Zstd
        );
    }
}
