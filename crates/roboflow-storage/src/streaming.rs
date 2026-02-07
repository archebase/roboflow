// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Streaming readers with range request support.
//!
//! This module provides streaming implementations for cloud and local storage
//! that use HTTP range requests to enable reading large files without buffering
//! entire objects into memory.
//!
//! # Note
//!
//! Background prefetch is not yet implemented. The `prefetch_count` configuration
//! option in `StreamingConfig` is reserved for future use.

use bytes::Bytes;
use std::io::{Read, Result as IoResult};
use std::sync::Arc;

use crate::{StorageError, StorageResult, StreamingRead};

// =============================================================================
// Streaming OSS Reader
// =============================================================================

/// Streaming reader for OSS/S3 with range requests.
///
/// Fetches data in configurable chunks using HTTP range requests.
///
/// # Note
///
/// Prefetch is not yet implemented - the `prefetch_count` in `StreamingConfig`
/// is currently unused. This is a future enhancement that would involve
/// background fetching of subsequent chunks while processing the current one.
pub struct StreamingOssReader {
    /// Object store client
    store: Arc<dyn object_store::ObjectStore>,
    /// Tokio runtime handle for async operations
    runtime: tokio::runtime::Handle,
    /// Path to the object
    path: object_store::path::Path,
    /// Total size of the object
    object_size: u64,
    /// Current read position
    position: u64,
    /// Chunk size for range requests
    chunk_size: usize,
    /// Current buffer being read
    current_buffer: Option<Bytes>,
    /// Buffer offset within the object
    buffer_offset: u64,
    // Prefetch fields for future optimization:
    // prefetch_count: usize,
    // chunk_receiver: Receiver<Option<Bytes>>,
    // _shutdown_sender: Option<Sender<()>>,
}

impl StreamingOssReader {
    /// Create a new streaming OSS reader.
    pub fn new(
        store: Arc<dyn object_store::ObjectStore>,
        runtime: tokio::runtime::Handle,
        path: object_store::path::Path,
        object_size: u64,
        config: &crate::StreamingConfig,
    ) -> StorageResult<Self> {
        Ok(Self {
            store,
            runtime,
            path,
            object_size,
            position: 0,
            chunk_size: config.chunk_size,
            current_buffer: None,
            buffer_offset: 0,
        })
    }

    /// Ensure we have data loaded at the current position.
    fn ensure_buffer(&mut self) -> IoResult<()> {
        // Check if we need to load a new buffer
        if self.current_buffer.is_none()
            || (self.buffer_offset + self.current_buffer.as_ref().unwrap().len() as u64)
                <= self.position
        {
            // Check if we're at EOF
            if self.position >= self.object_size {
                return Ok(());
            }

            // Fetch a new chunk
            let chunk = self.fetch_chunk_at(self.position)?;
            self.buffer_offset = self.position;
            self.current_buffer = Some(chunk);
        }
        Ok(())
    }

    /// Fetch a chunk starting at the given position.
    fn fetch_chunk_at(&mut self, start: u64) -> IoResult<Bytes> {
        let end = std::cmp::min(start + self.chunk_size as u64, self.object_size);
        // Convert to usize for get_range API (with overflow check)
        let start_usize = usize::try_from(start)
            .map_err(|_| std::io::Error::other(format!("start offset {} too large", start)))?;
        let end_usize = usize::try_from(end)
            .map_err(|_| std::io::Error::other(format!("end offset {} too large", end)))?;

        self.runtime
            .block_on(async {
                self.store
                    .get_range(&self.path, start_usize..end_usize)
                    .await
            })
            .map_err(|e| std::io::Error::other(format!("Failed to fetch range: {}", e)))
    }

    /// Get the offset within the current buffer.
    fn buffer_position(&self) -> usize {
        if self.position < self.buffer_offset {
            return 0;
        }
        (self.position - self.buffer_offset) as usize
    }
}

impl Read for StreamingOssReader {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        // Check if we're at EOF
        if self.position >= self.object_size {
            return Ok(0);
        }

        // Ensure we have data loaded
        self.ensure_buffer()?;

        if let Some(ref buffer) = self.current_buffer {
            let buf_pos = self.buffer_position();
            let remaining = buffer.len() - buf_pos;
            let to_read = std::cmp::min(remaining, buf.len());

            if to_read > 0 {
                buf[..to_read].copy_from_slice(&buffer[buf_pos..buf_pos + to_read]);
                self.position += to_read as u64;
                Ok(to_read)
            } else {
                // EOF reached
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }
}

impl StreamingRead for StreamingOssReader {
    fn position(&self) -> u64 {
        self.position
    }

    fn seek_to(&mut self, offset: u64) -> StorageResult<()> {
        if offset > self.object_size {
            return Err(StorageError::invalid_path(format!(
                "Seek offset {} exceeds object size {}",
                offset, self.object_size
            )));
        }

        self.position = offset;
        // Invalidate current buffer - will be fetched on next read
        self.current_buffer = None;
        self.buffer_offset = 0;

        Ok(())
    }
}

// =============================================================================
// Streaming Local Reader
// =============================================================================

/// Streaming reader wrapper for local files.
///
/// Wraps a seekable file and tracks position to implement StreamingRead.
pub struct StreamingLocalReader {
    /// The underlying file
    inner: std::io::BufReader<std::fs::File>,
    /// Current position (tracked separately for efficiency)
    position: u64,
}

impl StreamingLocalReader {
    /// Create a new streaming local reader.
    pub fn new(file: std::fs::File) -> StorageResult<Self> {
        Ok(Self {
            inner: std::io::BufReader::new(file),
            position: 0,
        })
    }
}

impl Read for StreamingLocalReader {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let n = self.inner.read(buf)?;
        self.position += n as u64;
        Ok(n)
    }
}

impl StreamingRead for StreamingLocalReader {
    fn position(&self) -> u64 {
        self.position
    }

    fn seek_to(&mut self, offset: u64) -> StorageResult<()> {
        use std::io::Seek;
        self.inner
            .seek(std::io::SeekFrom::Start(offset))
            .map_err(StorageError::Io)?;
        self.position = offset;
        Ok(())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_streaming_local_reader_position() {
        // Create a temporary file with known content
        let temp_dir = std::env::temp_dir();
        let file_path = temp_dir.join("test_streaming_local.bin");
        let content = b"Hello, world! This is a test file for streaming.";

        std::fs::write(&file_path, content).unwrap();

        let file = std::fs::File::open(&file_path).unwrap();
        let mut reader = StreamingLocalReader::new(file).unwrap();

        // Initial position should be 0
        assert_eq!(reader.position(), 0);

        // Read some data
        let mut buf = [0u8; 5];
        let n = reader.read(&mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"Hello");
        assert_eq!(reader.position(), 5);

        // Seek to a new position
        reader.seek_to(7).unwrap();
        assert_eq!(reader.position(), 7);

        let mut buf = [0u8; 5];
        let n = reader.read(&mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"world");
        assert_eq!(reader.position(), 12);

        // Cleanup
        std::fs::remove_file(&file_path).ok();
    }
}
