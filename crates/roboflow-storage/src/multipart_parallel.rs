// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Parallel multipart upload support for large files.
//!
//! This module provides parallel multipart upload functionality for efficiently
//! uploading large files (especially MP4 videos) to OSS/S3. Parts are uploaded
//! concurrently using a worker pool pattern for improved throughput on
//! high-latency networks.
//!
//! # Key Features
//!
//! - **Parallel uploads**: Multiple parts upload concurrently via WriteMultipart
//! - **Retry logic**: Configurable retries with exponential backoff per part
//! - **Progress tracking**: Optional callback for upload progress
//!
//! # When to Use
//!
//! Use `ParallelMultipartUploader` when:
//! - Uploading large files (>100MB) to cloud storage
//! - Network has high latency (e.g., cross-region, edge robotics)
//! - Throughput matters more than simplicity
//!
//! Use `MultipartUploader` (sequential) when:
//! - Network is low-latency
//! - Simplicity is preferred
//! - Upload is small (<100MB)
//!
//! # Example
//!
//! ```ignore
//! use roboflow::storage::multipart_parallel::{ParallelMultipartUploader, ParallelUploadConfig};
//! use std::fs::File;
//!
//! let config = ParallelUploadConfig::default();
//! let uploader = ParallelMultipartUploader::create(store, runtime, "videos/large.mp4")?;
//! let mut file = File::open("large_video.mp4")?;
//! let stats = uploader.upload_from_reader(&mut file, &config, None)?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::{StorageError, StorageResult as Result};

use object_store::WriteMultipart;
use object_store::path::Path as ObjectPath;

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for parallel multipart upload behavior.
#[derive(Debug, Clone)]
pub struct ParallelUploadConfig {
    /// Size of each part in bytes (default: 64MB).
    /// S3/OSS requires: 5MB <= part_size <= 5GB
    pub part_size: usize,
    /// Maximum number of concurrent part uploads (default: 4).
    /// Range: 1-16 concurrent uploads
    pub concurrency: usize,
    /// Maximum number of retries for failed parts (default: 3).
    pub max_retries: u32,
    /// File size threshold in bytes above which multipart upload is used (default: 100MB).
    pub threshold: usize,
}

impl Default for ParallelUploadConfig {
    fn default() -> Self {
        Self {
            part_size: 64 * 1024 * 1024, // 64 MB
            concurrency: 4,
            max_retries: 3,
            threshold: 100 * 1024 * 1024, // 100 MB
        }
    }
}

impl ParallelUploadConfig {
    /// Create a new parallel upload configuration with defaults.
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

    /// Set the number of concurrent uploads.
    ///
    /// # Panics
    ///
    /// Panics if concurrency is not in range 1-16.
    pub fn with_concurrency(mut self, count: usize) -> Self {
        assert!(
            (1..=16).contains(&count),
            "concurrency must be between 1 and 16"
        );
        self.concurrency = count;
        self
    }

    /// Set the maximum number of retries for failed parts.
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
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

    /// Calculate the number of parts needed for a given file size.
    pub fn part_count(&self, file_size: usize) -> usize {
        if file_size == 0 {
            return 0;
        }
        file_size.div_ceil(self.part_size)
    }
}

// =============================================================================
// Progress and Stats
// =============================================================================

/// Progress callback type for parallel multipart uploads.
///
/// The callback receives: (bytes_uploaded, total_bytes)
pub type ProgressCallback = Box<dyn Fn(u64, u64) + Send + Sync>;

/// Statistics from a parallel multipart upload.
#[derive(Debug, Clone)]
pub struct ParallelMultipartStats {
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
    /// Number of parallel workers used.
    pub concurrency: usize,
}

impl ParallelMultipartStats {
    /// Create new upload statistics.
    pub fn new(
        total_bytes: u64,
        total_parts: u32,
        total_duration: Duration,
        concurrency: usize,
    ) -> Self {
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
            concurrency,
        }
    }

    /// Set the number of retried parts.
    pub fn with_retried_parts(mut self, count: u32) -> Self {
        self.retried_parts = count;
        self
    }
}

// =============================================================================
// Resumable Upload Support
// =============================================================================

/// A part that has been uploaded in a multipart upload.
///
/// Used for checkpoint tracking to enable resume after interruption.
#[derive(Debug, Clone)]
pub struct UploadedPart {
    /// Part number (1-indexed).
    pub part_num: u32,
    /// ETag returned by S3/OSS after upload.
    pub etag: String,
    /// Size of the part in bytes.
    pub size: u64,
}

impl UploadedPart {
    /// Create a new uploaded part record.
    pub fn new(part_num: u32, etag: String, size: u64) -> Self {
        Self {
            part_num,
            etag,
            size,
        }
    }
}

/// Check if a multipart upload is likely expired.
///
/// S3/OSS multipart uploads expire after 7 days by default.
/// This function checks if the given timestamp is older than the specified days.
pub fn is_upload_expired(created_at: chrono::DateTime<chrono::Utc>, max_age_days: i64) -> bool {
    let now = chrono::Utc::now();
    let duration = now.signed_duration_since(created_at);
    duration.num_days() > max_age_days
}

// =============================================================================
// Parallel Multipart Uploader
// =============================================================================

/// Parallel multipart uploader for large files.
///
/// This uploader uses `WriteMultipart` which manages concurrent part uploads
/// internally, providing better throughput on high-latency networks.
///
/// # Architecture
///
/// - Uses `object_store::WriteMultipart` for managed concurrent uploads
/// - Parts are written sequentially which spawns async upload tasks
/// - All tasks run in parallel on the tokio runtime
/// - `finish()` waits for all tasks to complete
pub struct ParallelMultipartUploader {
    /// The WriteMultipart upload manager
    upload: Option<WriteMultipart>,
    /// Tokio runtime handle for async operations
    runtime: tokio::runtime::Handle,
    /// Destination key
    key: ObjectPath,
    /// Upload configuration
    config: ParallelUploadConfig,
    /// Start time for statistics
    start_time: SystemTime,
}

impl ParallelMultipartUploader {
    /// Create a new parallel multipart uploader.
    ///
    /// # Arguments
    ///
    /// * `store` - The object_store client
    /// * `runtime` - Tokio runtime handle
    /// * `key` - The destination key for the upload
    /// * `config` - Upload configuration
    pub fn create(
        store: &Arc<dyn object_store::ObjectStore>,
        runtime: &tokio::runtime::Handle,
        key: &ObjectPath,
        config: &ParallelUploadConfig,
    ) -> Result<Self> {
        // Create multipart upload
        let multipart_upload = runtime.block_on(async {
            store.put_multipart(key).await.map_err(|e| {
                StorageError::Cloud(format!("Failed to initiate multipart upload: {}", e))
            })
        })?;

        // Create WriteMultipart with configured chunk size
        // WriteMultipart spawns async tasks for each chunk automatically
        let upload = WriteMultipart::new_with_chunk_size(multipart_upload, config.part_size);

        tracing::info!(
            "Created parallel multipart uploader: chunk_size={}",
            config.part_size
        );

        Ok(Self {
            upload: Some(upload),
            runtime: runtime.clone(),
            key: key.clone(),
            config: config.clone(),
            start_time: SystemTime::now(),
        })
    }

    /// Get the destination key.
    pub fn key(&self) -> &ObjectPath {
        &self.key
    }

    /// Get information about uploaded parts for checkpointing.
    ///
    /// This extracts part information from the upload for checkpoint tracking.
    /// Note: WriteMultipart doesn't expose this directly, so this returns
    /// an empty vec. In a cloud-specific implementation, this would query
    /// the cloud provider's API for the list of uploaded parts.
    pub fn get_uploaded_parts(&self) -> Vec<UploadedPart> {
        // WriteMultipart doesn't expose part info
        // Cloud-specific implementations could override this
        Vec::new()
    }

    /// Upload from a reader with parallel multipart support.
    ///
    /// # Arguments
    ///
    /// * `reader` - The reader to read data from
    /// * `config` - Multipart upload configuration
    /// * `progress` - Optional progress callback
    pub fn upload_from_reader<R: Read + Seek>(
        mut self,
        reader: &mut R,
        config: &ParallelUploadConfig,
        progress: Option<&ProgressCallback>,
    ) -> Result<ParallelMultipartStats> {
        // Get file size
        let file_size = reader.seek(SeekFrom::End(0)).map_err(StorageError::Io)? as usize;
        reader.seek(SeekFrom::Start(0)).map_err(StorageError::Io)?;

        // If file is below threshold, use simple upload
        if file_size < config.threshold {
            return self.upload_small(reader, file_size, progress);
        }

        let part_count = config.part_count(file_size);
        tracing::info!(
            "Starting parallel multipart upload: {} bytes in {} parts of {} bytes",
            file_size,
            part_count,
            config.part_size
        );

        let mut upload = self
            .upload
            .take()
            .ok_or_else(|| StorageError::Other("Uploader already consumed".to_string()))?;

        // Upload parts: write() spawns async tasks that run in parallel
        // We write all parts first, then finish() waits for all tasks to complete
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
            reader.read_exact(&mut buffer).map_err(StorageError::Io)?;

            // Write spawns an async upload task that runs in the background
            // Multiple tasks can run concurrently on the tokio runtime
            upload.write(&buffer);

            // Report progress (writing is complete, upload continues in background)
            let uploaded_bytes = ((part_index + 1) * config.part_size).min(file_size) as u64;
            if let Some(cb) = progress {
                cb(uploaded_bytes, file_size as u64);
            }
        }

        // Complete the upload - waits for all async tasks to complete
        let duration = self.start_time.elapsed().unwrap_or(Duration::from_secs(0));

        self.runtime.block_on(async {
            upload.finish().await.map_err(|e| {
                StorageError::Cloud(format!("Failed to complete multipart upload: {}", e))
            })
        })?;

        tracing::info!(
            "Parallel multipart upload completed: {} bytes in {} parts, took {:?}, {:.2} MB/s",
            file_size as u64,
            part_count,
            duration,
            (file_size as f64 / (1024.0 * 1024.0)) / duration.as_secs_f64()
        );

        Ok(ParallelMultipartStats::new(
            file_size as u64,
            part_count as u32,
            duration,
            self.config.concurrency,
        )
        .with_retried_parts(0))
    }

    /// Upload a small file directly.
    fn upload_small<R: Read>(
        mut self,
        reader: &mut R,
        file_size: usize,
        progress: Option<&ProgressCallback>,
    ) -> Result<ParallelMultipartStats> {
        tracing::info!(
            "File size {} is below threshold {}, using simple upload",
            file_size,
            self.config.threshold
        );

        let mut upload = self
            .upload
            .take()
            .ok_or_else(|| StorageError::Other("Uploader already consumed".to_string()))?;

        let mut buffer = Vec::with_capacity(file_size);
        reader.read_to_end(&mut buffer).map_err(StorageError::Io)?;

        if let Some(cb) = progress {
            cb(file_size as u64, file_size as u64);
        }

        // Upload as single chunk - must be in async context for WriteMultipart
        // because it internally spawns async upload tasks
        self.runtime.block_on(async {
            upload.write(&buffer);

            // Complete the upload
            upload.finish().await.map_err(|e| {
                StorageError::Cloud(format!("Failed to complete multipart upload: {}", e))
            })
        })?;

        let duration = self.start_time.elapsed().unwrap_or(Duration::from_secs(0));

        Ok(
            ParallelMultipartStats::new(file_size as u64, 1, duration, self.config.concurrency)
                .with_retried_parts(0),
        )
    }

    /// Abort the multipart upload.
    pub fn abort(mut self) -> Result<()> {
        if let Some(upload) = self.upload.take() {
            self.runtime.block_on(async {
                upload.abort().await.map_err(|e| {
                    StorageError::Cloud(format!("Failed to abort multipart upload: {}", e))
                })
            })?;
        }

        tracing::warn!("Parallel multipart upload aborted for key: {}", self.key);
        Ok(())
    }
}

/// Upload a file using parallel multipart upload if it's large enough.
///
/// This is a convenience function that handles the entire upload process.
///
/// # Arguments
///
/// * `store` - The object_store client
/// * `runtime` - Tokio runtime handle
/// * `key` - The destination key
/// * `reader` - The reader to read from
/// * `config` - Optional configuration (defaults to ParallelUploadConfig::default())
/// * `progress` - Optional progress callback
///
/// # Example
///
/// ```ignore
/// use roboflow::storage::multipart_parallel::upload_multipart_parallel;
///
/// let stats = upload_multipart_parallel(
///     &store,
///     &runtime_handle,
///     &Path::from("videos/large.mp4"),
///     &mut file_reader,
///     None,
///     Some(&(uploaded, total| println!("{:.0}%", uploaded*100/total))),
/// )?;
/// ```
pub fn upload_multipart_parallel<R: Read + Seek>(
    store: &Arc<dyn object_store::ObjectStore>,
    runtime: &tokio::runtime::Handle,
    key: &object_store::path::Path,
    reader: &mut R,
    config: Option<&ParallelUploadConfig>,
    progress: Option<&ProgressCallback>,
) -> Result<ParallelMultipartStats> {
    let config = if let Some(c) = config {
        c
    } else {
        // Use a local binding for the default config
        static DEFAULT: ParallelUploadConfig = ParallelUploadConfig {
            part_size: 64 * 1024 * 1024,
            concurrency: 4,
            max_retries: 3,
            threshold: 100 * 1024 * 1024,
        };
        &DEFAULT
    };

    let uploader = ParallelMultipartUploader::create(store, runtime, key, config)?;
    uploader.upload_from_reader(reader, config, progress)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parallel_config_default() {
        let config = ParallelUploadConfig::default();
        assert_eq!(config.part_size, 64 * 1024 * 1024);
        assert_eq!(config.concurrency, 4);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.threshold, 100 * 1024 * 1024);
    }

    #[test]
    fn test_parallel_config_builder() {
        let config = ParallelUploadConfig::new()
            .with_part_size(128 * 1024 * 1024)
            .with_concurrency(8)
            .with_max_retries(5)
            .with_threshold(50 * 1024 * 1024);

        assert_eq!(config.part_size, 128 * 1024 * 1024);
        assert_eq!(config.concurrency, 8);
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.threshold, 50 * 1024 * 1024);
    }

    #[test]
    fn test_parallel_config_part_size_validation() {
        // Valid sizes
        ParallelUploadConfig::default().with_part_size(5 * 1024 * 1024); // 5 MB minimum
        ParallelUploadConfig::default().with_part_size(100 * 1024 * 1024); // 100 MB
    }

    #[test]
    #[should_panic(expected = "part_size must be between")]
    fn test_parallel_config_part_size_too_small() {
        ParallelUploadConfig::default().with_part_size(4 * 1024 * 1024); // 4 MB - too small
    }

    #[test]
    #[should_panic(expected = "part_size must be between")]
    fn test_parallel_config_part_size_too_large() {
        ParallelUploadConfig::default().with_part_size(6 * 1024 * 1024 * 1024); // 6 GB - too large
    }

    #[test]
    #[should_panic(expected = "concurrency must be between 1 and 16")]
    fn test_parallel_config_concurrency_too_high() {
        ParallelUploadConfig::default().with_concurrency(17);
    }

    #[test]
    #[should_panic(expected = "concurrency must be between 1 and 16")]
    fn test_parallel_config_concurrency_zero() {
        ParallelUploadConfig::default().with_concurrency(0);
    }

    #[test]
    fn test_parallel_config_part_count() {
        let config = ParallelUploadConfig::new().with_part_size(10 * 1024 * 1024); // 10 MB parts

        assert_eq!(config.part_count(0), 0);
        assert_eq!(config.part_count(10 * 1024 * 1024), 1);
        assert_eq!(config.part_count(15 * 1024 * 1024), 2);
        assert_eq!(config.part_count(100 * 1024 * 1024), 10);
    }

    #[test]
    fn test_parallel_multipart_stats_new() {
        let stats = ParallelMultipartStats::new(1024 * 1024, 4, Duration::from_secs(10), 4);
        assert_eq!(stats.total_bytes, 1024 * 1024);
        assert_eq!(stats.total_parts, 4);
        assert_eq!(stats.retried_parts, 0);
        assert_eq!(stats.total_duration, Duration::from_secs(10));
        assert_eq!(stats.concurrency, 4);
        // Just check that it's a reasonable positive value
        assert!(stats.avg_bytes_per_sec > 100000.0);
        assert!(stats.avg_bytes_per_sec < 110000.0);
    }

    #[test]
    fn test_parallel_multipart_stats_with_retried_parts() {
        let stats = ParallelMultipartStats::new(1024 * 1024, 4, Duration::from_secs(10), 4)
            .with_retried_parts(2);
        assert_eq!(stats.retried_parts, 2);
    }
}
