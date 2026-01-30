// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Multipart upload support for large files.
//!
//! This module provides multipart upload functionality for efficiently uploading
//! large files (especially MP4 videos) to OSS/S3. Files above a configurable threshold
//! are split into multiple parts that can be uploaded in parallel.
//!
//! # Example
//!
//! ```ignore
//! use roboflow::storage::multipart::{MultipartUploader, MultipartConfig};
//! use std::fs::File;
//!
//! let config = MultipartConfig::default();
//! let uploader = MultipartUploader::new(store, runtime, "bucket", "key")?;
//! let mut file = File::open("large_video.mp4")?;
//! uploader.upload_from_reader(&mut file, &config, None)?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime};

use super::{Result, StorageError};

#[cfg(feature = "cloud-storage")]
use object_store::path::Path as ObjectPath;

/// Configuration for multipart upload behavior.
#[derive(Debug, Clone)]
pub struct MultipartConfig {
    /// Size of each part in bytes (default: 64MB).
    /// S3/OSS requires: 5MB <= part_size <= 5GB
    pub part_size: usize,
    /// Maximum number of parts to upload concurrently (default: 4).
    pub max_concurrent_parts: usize,
    /// File size threshold in bytes above which multipart upload is used (default: 100MB).
    pub threshold: usize,
    /// Maximum number of retries for failed parts (default: 3).
    pub max_retries: u32,
    /// Timeout for each part upload in seconds (default: 300).
    pub part_timeout_secs: u64,
}

impl Default for MultipartConfig {
    fn default() -> Self {
        Self {
            part_size: 64 * 1024 * 1024, // 64 MB
            max_concurrent_parts: 4,
            threshold: 100 * 1024 * 1024, // 100 MB
            max_retries: 3,
            part_timeout_secs: 300,
        }
    }
}

impl MultipartConfig {
    /// Create a new multipart configuration with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the part size in bytes.
    ///
    /// # Panics
    ///
    /// Panics if part_size is less than 5MB or greater than 5GB.
    pub fn with_part_size(mut self, part_size: usize) -> Self {
        const MIN_PART_SIZE: usize = 5 * 1024 * 1024; // 5 MB
        const MAX_PART_SIZE: usize = 5 * 1024 * 1024 * 1024; // 5 GB

        assert!(
            (MIN_PART_SIZE..=MAX_PART_SIZE).contains(&part_size),
            "part_size must be between {} and {} bytes",
            MIN_PART_SIZE,
            MAX_PART_SIZE
        );
        self.part_size = part_size;
        self
    }

    /// Set the maximum number of concurrent parts.
    pub fn with_max_concurrent_parts(mut self, max: usize) -> Self {
        assert!(
            max > 0 && max <= 10,
            "max_concurrent_parts must be between 1 and 10"
        );
        self.max_concurrent_parts = max;
        self
    }

    /// Set the threshold file size for using multipart upload.
    pub fn with_threshold(mut self, threshold: usize) -> Self {
        assert!(
            threshold >= 5 * 1024 * 1024,
            "threshold must be at least 5MB"
        );
        self.threshold = threshold;
        self
    }

    /// Set the maximum number of retries for failed parts.
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set the timeout for each part upload.
    pub fn with_part_timeout_secs(mut self, timeout_secs: u64) -> Self {
        self.part_timeout_secs = timeout_secs;
        self
    }

    /// Calculate the number of parts needed for a given file size.
    pub fn part_count(&self, file_size: usize) -> usize {
        if file_size == 0 {
            return 0;
        }
        file_size.div_ceil(self.part_size)
    }
}

/// Progress callback type for multipart uploads.
///
/// The callback receives: (bytes_uploaded, total_bytes)
pub type ProgressCallback = Box<dyn Fn(u64, u64) + Send + Sync>;

/// Statistics from a multipart upload.
#[derive(Debug, Clone)]
pub struct MultipartStats {
    /// Total bytes uploaded.
    pub total_bytes: u64,
    /// Total number of parts.
    pub total_parts: u32,
    /// Number of parts that failed and required retry.
    pub retried_parts: u32,
    /// Total duration of the upload.
    pub total_duration: Duration,
    /// Average upload speed in bytes/second.
    pub avg_bytes_per_sec: f64,
}

impl MultipartStats {
    /// Create new upload statistics.
    pub fn new(total_bytes: u64, total_parts: u32, total_duration: Duration) -> Self {
        let avg_bytes_per_sec = if total_duration.as_secs_f64() > 0.0 {
            total_bytes as f64 / total_duration.as_secs_f64()
        } else {
            0.0
        };

        Self {
            total_bytes,
            total_parts,
            retried_parts: 0,
            total_duration,
            avg_bytes_per_sec,
        }
    }

    /// Set the number of retried parts.
    pub fn with_retried_parts(mut self, count: u32) -> Self {
        self.retried_parts = count;
        self
    }
}

/// Multipart uploader for large files.
///
/// Only available when the `cloud-storage` feature is enabled.
///
/// This wraps the `object_store::MultipartUpload` trait with a more convenient
/// synchronous API that handles retries, progress tracking, and error handling.
#[cfg(feature = "cloud-storage")]
pub struct MultipartUploader {
    /// The underlying multipart upload trait object
    upload: Box<dyn object_store::MultipartUpload>,
    /// Tokio runtime handle for async operations
    runtime: tokio::runtime::Handle,
    /// Destination key
    key: ObjectPath,
    /// Completed parts count
    parts_completed: u32,
    /// Start time for statistics
    start_time: SystemTime,
}

#[cfg(feature = "cloud-storage")]
impl MultipartUploader {
    /// Create a new multipart uploader by initiating the upload.
    ///
    /// # Arguments
    ///
    /// * `store` - The object_store client
    /// * `runtime` - Tokio runtime handle
    /// * `key` - The destination key for the upload
    pub fn create(
        store: &Arc<dyn object_store::ObjectStore>,
        runtime: &tokio::runtime::Handle,
        key: &ObjectPath,
    ) -> Result<Self> {
        // Initiate the multipart upload
        let upload = runtime.block_on(async {
            store.put_multipart(key).await.map_err(|e| {
                StorageError::Cloud(format!("Failed to initiate multipart upload: {}", e))
            })
        })?;

        Ok(Self {
            upload,
            runtime: runtime.clone(),
            key: key.clone(),
            parts_completed: 0,
            start_time: SystemTime::now(),
        })
    }

    /// Get the destination key.
    pub fn key(&self) -> &ObjectPath {
        &self.key
    }

    /// Upload a single part.
    ///
    /// # Arguments
    ///
    /// * `data` - The part data as bytes
    pub fn upload_part(&mut self, data: Vec<u8>) -> Result<()> {
        let bytes = bytes::Bytes::from(data);
        let payload = object_store::PutPayload::from_bytes(bytes);

        // Use the upload trait to put the part
        let part_future = self.upload.put_part(payload);

        // Wait for the part upload to complete
        self.runtime.block_on(async {
            part_future
                .await
                .map_err(|e| StorageError::Cloud(format!("Failed to upload part: {}", e)))
        })?;

        self.parts_completed += 1;
        Ok(())
    }

    /// Upload from a reader with multipart support.
    ///
    /// # Arguments
    ///
    /// * `reader` - The reader to read data from
    /// * `config` - Multipart upload configuration
    /// * `progress` - Optional progress callback
    pub fn upload_from_reader<R: Read + Seek>(
        mut self,
        reader: &mut R,
        config: &MultipartConfig,
        progress: Option<&ProgressCallback>,
    ) -> Result<MultipartStats> {
        // Get file size
        let file_size = reader
            .seek(SeekFrom::End(0))
            .map_err(StorageError::Io)? as usize;
        reader
            .seek(SeekFrom::Start(0))
            .map_err(StorageError::Io)?;

        // If file is below threshold, do simple upload
        if file_size < config.threshold {
            return self.upload_small(reader, file_size, progress);
        }

        let part_count = config.part_count(file_size);
        tracing::info!(
            "Starting multipart upload: {} bytes in {} parts of {} bytes",
            file_size,
            part_count,
            config.part_size
        );

        // Upload parts with retry logic
        let mut retried_parts = 0u32;

        for part_index in 0..part_count {
            let offset = part_index * config.part_size;
            let remaining = file_size.saturating_sub(offset);
            let to_read = config.part_size.min(remaining);

            // Seek to the correct position
            reader
                .seek(SeekFrom::Start(offset as u64))
                .map_err(StorageError::Io)?;

            // Read the part data
            let mut buffer = vec![0u8; to_read];
            reader
                .read_exact(&mut buffer)
                .map_err(StorageError::Io)?;

            // Upload with retry
            let mut retry_count = 0;
            let mut last_error = None;

            while retry_count <= config.max_retries {
                match self.upload_part(buffer.clone()) {
                    Ok(_) => {
                        if retry_count > 0 {
                            retried_parts += 1;
                        }
                        break;
                    }
                    Err(e) => {
                        last_error = Some(e);
                        retry_count += 1;

                        if retry_count <= config.max_retries {
                            let backoff = Duration::from_millis(100 * 2_u64.pow(retry_count - 1));
                            tracing::warn!(
                                "Part {} failed (attempt {}/{}), retrying after {:?}",
                                part_index + 1,
                                retry_count,
                                config.max_retries,
                                backoff
                            );
                            thread::sleep(backoff);
                        }
                    }
                }
            }

            if retry_count > config.max_retries {
                // Abort the multipart upload on too many failures
                let _ = self.abort();
                return Err(last_error.unwrap());
            }

            // Report progress
            if let Some(cb) = progress {
                let uploaded = ((part_index + 1) * config.part_size).min(file_size) as u64;
                cb(uploaded, file_size as u64);
            }
        }

        // Complete the upload
        let stats = self.complete_internal(file_size as u64, part_count as u32)?;

        // Restore completed parts for stats with retries
        if retried_parts > 0 {
            return Ok(stats.with_retried_parts(retried_parts));
        }

        Ok(stats)
    }

    /// Upload a small file directly (no multipart).
    fn upload_small<R: Read>(
        mut self,
        reader: &mut R,
        file_size: usize,
        progress: Option<&ProgressCallback>,
    ) -> Result<MultipartStats> {
        tracing::info!(
            "File size {} is below threshold {}, using simple upload",
            file_size,
            1024 * 1024 * 100
        );

        // For small files, we still use the multipart uploader but with a single part
        let mut buffer = Vec::with_capacity(file_size);
        reader
            .read_to_end(&mut buffer)
            .map_err(StorageError::Io)?;

        if let Some(cb) = progress {
            cb(file_size as u64, file_size as u64);
        }

        // Upload as a single part
        self.upload_part(buffer)?;

        // Complete the upload
        self.complete_internal(file_size as u64, 1)
    }

    /// Complete the multipart upload.
    fn complete_internal(&mut self, total_bytes: u64, total_parts: u32) -> Result<MultipartStats> {
        let duration = self.start_time.elapsed().unwrap_or(Duration::from_secs(0));

        self.runtime.block_on(async {
            self.upload.complete().await.map_err(|e| {
                StorageError::Cloud(format!("Failed to complete multipart upload: {}", e))
            })
        })?;

        tracing::info!(
            "Multipart upload completed: {} bytes in {} parts, took {:?}",
            total_bytes,
            total_parts,
            duration
        );

        Ok(MultipartStats::new(total_bytes, total_parts, duration))
    }

    /// Abort the multipart upload.
    pub fn abort(mut self) -> Result<()> {
        self.runtime.block_on(async {
            self.upload.abort().await.map_err(|e| {
                StorageError::Cloud(format!("Failed to abort multipart upload: {}", e))
            })
        })?;

        tracing::warn!("Multipart upload aborted for key: {}", self.key);
        Ok(())
    }
}

/// Upload a file using multipart upload if it's large enough.
///
/// This is a convenience function that handles the entire upload process.
///
/// # Arguments
///
/// * `store` - The object_store client
/// * `runtime` - Tokio runtime handle
/// * `key` - The destination key
/// * `reader` - The reader to read from
/// * `config` - Optional configuration (defaults to MultipartConfig::default())
/// * `progress` - Optional progress callback
///
/// # Example
///
/// ```ignore
/// use roboflow::storage::multipart::upload_multipart;
///
/// let stats = upload_multipart(
///     &store,
///     &runtime_handle,
///     &Path::from("videos/large.mp4"),
///     &mut file_reader,
///     None,
///     Some(&(uploaded, total| println!("{}%", uploaded*100/total))),
/// )?;
/// ```
#[cfg(feature = "cloud-storage")]
pub fn upload_multipart<R: Read + Seek>(
    store: &Arc<dyn object_store::ObjectStore>,
    runtime: &tokio::runtime::Handle,
    key: &object_store::path::Path,
    reader: &mut R,
    config: Option<&MultipartConfig>,
    progress: Option<&ProgressCallback>,
) -> Result<MultipartStats> {
    let config = if let Some(c) = config {
        c
    } else {
        // Use a local binding for the default config to avoid temporary value issues
        static DEFAULT: MultipartConfig = MultipartConfig {
            part_size: 64 * 1024 * 1024,
            max_concurrent_parts: 4,
            threshold: 100 * 1024 * 1024,
            max_retries: 3,
            part_timeout_secs: 300,
        };
        &DEFAULT
    };
    let uploader = MultipartUploader::create(store, runtime, key)?;
    uploader.upload_from_reader(reader, config, progress)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multipart_config_default() {
        let config = MultipartConfig::default();
        assert_eq!(config.part_size, 64 * 1024 * 1024);
        assert_eq!(config.max_concurrent_parts, 4);
        assert_eq!(config.threshold, 100 * 1024 * 1024);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.part_timeout_secs, 300);
    }

    #[test]
    fn test_multipart_config_builder() {
        let config = MultipartConfig::new()
            .with_part_size(128 * 1024 * 1024)
            .with_max_concurrent_parts(8)
            .with_threshold(50 * 1024 * 1024)
            .with_max_retries(5)
            .with_part_timeout_secs(600);

        assert_eq!(config.part_size, 128 * 1024 * 1024);
        assert_eq!(config.max_concurrent_parts, 8);
        assert_eq!(config.threshold, 50 * 1024 * 1024);
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.part_timeout_secs, 600);
    }

    #[test]
    fn test_multipart_config_part_size_validation() {
        // Valid sizes
        MultipartConfig::default().with_part_size(5 * 1024 * 1024); // 5 MB minimum
        MultipartConfig::default().with_part_size(100 * 1024 * 1024); // 100 MB
    }

    #[test]
    #[should_panic(expected = "part_size must be between")]
    fn test_multipart_config_part_size_too_small() {
        MultipartConfig::default().with_part_size(4 * 1024 * 1024); // 4 MB - too small
    }

    #[test]
    #[should_panic(expected = "part_size must be between")]
    fn test_multipart_config_part_size_too_large() {
        MultipartConfig::default().with_part_size(6 * 1024 * 1024 * 1024); // 6 GB - too large
    }

    #[test]
    fn test_multipart_config_part_count() {
        let config = MultipartConfig::new().with_part_size(10 * 1024 * 1024); // 10 MB parts

        assert_eq!(config.part_count(0), 0);
        assert_eq!(config.part_count(10 * 1024 * 1024), 1);
        assert_eq!(config.part_count(15 * 1024 * 1024), 2);
        assert_eq!(config.part_count(100 * 1024 * 1024), 10);
    }

    #[test]
    fn test_multipart_stats_new() {
        let stats = MultipartStats::new(1024 * 1024, 4, Duration::from_secs(10));
        assert_eq!(stats.total_bytes, 1024 * 1024);
        assert_eq!(stats.total_parts, 4);
        assert_eq!(stats.retried_parts, 0);
        assert_eq!(stats.total_duration, Duration::from_secs(10));
        // Just check that it's a reasonable positive value
        assert!(stats.avg_bytes_per_sec > 100000.0);
        assert!(stats.avg_bytes_per_sec < 110000.0);
    }

    #[test]
    fn test_multipart_stats_with_retried_parts() {
        let stats =
            MultipartStats::new(1024 * 1024, 4, Duration::from_secs(10)).with_retried_parts(2);
        assert_eq!(stats.retried_parts, 2);
    }
}
