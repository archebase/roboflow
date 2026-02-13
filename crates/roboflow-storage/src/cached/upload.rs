// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Upload worker logic for cached storage.
//!
//! Provides background upload workers that transfer files from local cache
//! to remote storage, with configurable concurrency and graceful shutdown.

use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crossbeam_channel::Receiver;

use crate::{Storage, StorageError, StorageResult as Result};

/// Statistics about cache performance (re-exported from parent module).
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// Number of cache hits.
    pub cache_hits: u64,
    /// Number of cache misses.
    pub cache_misses: u64,
    /// Total bytes currently cached.
    pub total_cached_bytes: u64,
    /// Number of files currently cached.
    pub cached_file_count: u64,
    /// Number of pending uploads.
    pub pending_uploads: u64,
    /// Total uploads completed.
    pub uploads_completed: u64,
    /// Total uploads failed.
    pub uploads_failed: u64,
    /// Total bytes uploaded.
    pub bytes_uploaded: u64,
}

impl CacheStats {
    /// Calculate cache hit rate as a percentage.
    pub fn hit_rate(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            return 0.0;
        }
        (self.cache_hits as f64 / total as f64) * 100.0
    }
}

/// Cache entry metadata for tracking and eviction.
#[derive(Debug)]
pub struct CacheEntry {
    /// Relative path within cache.
    pub _path: PathBuf,
    /// File size in bytes.
    pub size: u64,
    /// Last access time (for LRU).
    pub last_accessed: std::time::SystemTime,
    /// Creation time (for FIFO).
    pub created_at: std::time::SystemTime,
    /// Access count (for LFU).
    pub access_count: std::sync::atomic::AtomicU64,
    /// Whether a file has a pending upload.
    pub pending_upload: bool,
}

impl CacheEntry {
    /// Create a new cache entry.
    pub fn new(path: PathBuf, size: u64) -> Self {
        let now = std::time::SystemTime::now();
        Self {
            _path: path,
            size,
            last_accessed: now,
            created_at: now,
            access_count: std::sync::atomic::AtomicU64::new(1),
            pending_upload: false,
        }
    }

    /// Record an access to this cache entry.
    pub fn record_access(&self) {
        self.access_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Upload task for the background worker queue.
#[derive(Debug)]
pub struct UploadTask {
    /// Local cache file path.
    pub local_path: PathBuf,
    /// Remote destination path.
    pub remote_path: PathBuf,
    /// File size for tracking.
    pub size: u64,
}

/// Configuration for upload workers.
pub struct UploadWorkerConfig {
    /// Worker identifier for logging.
    pub worker_id: usize,
    /// Whether to delete local file after successful upload.
    pub delete_after_upload: bool,
    /// Local cache directory path (for future use).
    pub _cache_dir: PathBuf,
}

/// Run an upload worker that processes tasks from the receiver.
///
/// The worker will:
/// - Process upload tasks from the receiver channel
/// - Update statistics after each upload
/// - Respect shutdown signals
/// - Exit gracefully when channel closes or shutdown is requested
#[allow(clippy::too_many_arguments)]
pub fn run_upload_worker(
    config: UploadWorkerConfig,
    receiver: Receiver<UploadTask>,
    remote: Arc<dyn Storage>,
    stats: Arc<Mutex<CacheStats>>,
    shutdown: Arc<AtomicUsize>,
    entries: Arc<Mutex<std::collections::HashMap<PathBuf, CacheEntry>>>,
) {
    let worker_id = config.worker_id;
    tracing::info!("Upload worker {} started", worker_id);

    loop {
        // Check for shutdown signal with timeout
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(task) => {
                // Double-check shutdown before processing
                if shutdown.load(Ordering::Acquire) != 0 {
                    // Shutdown requested, don't process this task
                    // Task will remain in cache for upload on next restart
                    break;
                }

                tracing::debug!(
                    "Worker {} uploading {} ({} bytes)",
                    worker_id,
                    task.local_path.display(),
                    task.size
                );

                let result = upload_single_file(&remote, &task, config.delete_after_upload);

                // Clear pending_upload flag after upload attempt (success or failure)
                // This allows eviction to proceed even if upload failed
                if let Ok(mut entries) = entries.lock()
                    && let Some(entry) = entries.get_mut(&task.remote_path)
                {
                    entry.pending_upload = false;
                }

                // Update stats
                if let Ok(mut s) = stats.lock() {
                    if result.is_ok() {
                        s.uploads_completed += 1;
                        s.bytes_uploaded += task.size;
                        s.pending_uploads = s.pending_uploads.saturating_sub(1);
                    } else {
                        s.uploads_failed += 1;
                        s.pending_uploads = s.pending_uploads.saturating_sub(1);
                    }
                }

                if let Err(e) = result {
                    tracing::error!(
                        "Worker {} failed to upload {}: {}",
                        worker_id,
                        task.local_path.display(),
                        e
                    );
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                // Check shutdown signal on timeout
                if shutdown.load(Ordering::Acquire) != 0 {
                    break;
                }
                // Continue loop to check for shutdown again
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                // Channel closed, exit
                break;
            }
        }
    }

    tracing::info!("Upload worker {} stopped", worker_id);
}

/// Upload a single file from cache to remote storage.
///
/// Uses streaming copy to avoid loading entire file into memory.
pub fn upload_single_file(
    remote: &Arc<dyn Storage>,
    task: &UploadTask,
    delete_after_upload: bool,
) -> Result<()> {
    // Open local file
    let local_path = &task.local_path;
    let remote_path = &task.remote_path;

    // Stream upload using fixed buffer to avoid OOM on large files
    const BUFFER_SIZE: usize = 64 * 1024; // 64KB buffer
    let mut file = File::open(local_path)
        .map_err(|e| StorageError::Other(format!("Failed to open cached file: {}", e)))?;

    let mut remote_writer = remote.writer(remote_path)?;
    let mut buffer = vec![0u8; BUFFER_SIZE];

    loop {
        let n_read = file
            .read(&mut buffer)
            .map_err(|e| StorageError::Other(format!("Failed to read cached file: {}", e)))?;
        if n_read == 0 {
            break;
        }
        remote_writer.write_all(&buffer[..n_read])?;
    }

    remote_writer.flush()?;

    tracing::debug!(
        "Uploaded {} to remote {}",
        local_path.display(),
        remote_path.display()
    );

    // Delete local file if configured
    if delete_after_upload {
        let _ = fs::remove_file(local_path);
    }

    Ok(())
}

/// Spawn upload worker threads.
///
/// Creates the specified number of worker threads that process upload tasks.
#[allow(clippy::too_many_arguments)]
pub fn spawn_upload_workers(
    count: usize,
    receiver: Receiver<UploadTask>,
    remote: Arc<dyn Storage>,
    stats: Arc<Mutex<CacheStats>>,
    shutdown: Arc<AtomicUsize>,
    entries: Arc<Mutex<std::collections::HashMap<PathBuf, CacheEntry>>>,
    delete_after_upload: bool,
    cache_dir: PathBuf,
) -> Result<Vec<thread::JoinHandle<()>>> {
    let mut workers = Vec::with_capacity(count);

    for worker_id in 0..count {
        let receiver = receiver.clone();
        let remote = Arc::clone(&remote);
        let stats = Arc::clone(&stats);
        let shutdown = Arc::clone(&shutdown);
        let entries = Arc::clone(&entries);
        let cache_dir = cache_dir.clone();

        let handle = thread::Builder::new()
            .name(format!("cached-upload-{}", worker_id))
            .spawn(move || {
                run_upload_worker(
                    UploadWorkerConfig {
                        worker_id,
                        delete_after_upload,
                        _cache_dir: cache_dir,
                    },
                    receiver,
                    remote,
                    stats,
                    shutdown,
                    entries,
                )
            })
            .map_err(|e| StorageError::Other(format!("Failed to spawn upload worker: {}", e)))?;

        workers.push(handle);
    }

    Ok(workers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upload_task_debug() {
        let task = UploadTask {
            local_path: PathBuf::from("/tmp/cache/file.dat"),
            remote_path: PathBuf::from("remote/file.dat"),
            size: 1024,
        };
        let debug_str = format!("{:?}", task);
        assert!(debug_str.contains("UploadTask"));
        assert!(debug_str.contains("file.dat"));
    }

    #[test]
    fn test_upload_worker_config() {
        let config = UploadWorkerConfig {
            worker_id: 5,
            delete_after_upload: true,
            _cache_dir: PathBuf::from("/tmp/cache"),
        };
        assert_eq!(config.worker_id, 5);
        assert!(config.delete_after_upload);
    }
}
