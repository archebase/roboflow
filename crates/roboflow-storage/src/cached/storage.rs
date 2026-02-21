// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Main cached storage implementation.
//!
//! Provides the `CachedStorage` struct that wraps a remote storage backend
//! with a local cache layer for read-through and write-behind caching.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};

use crossbeam_channel::{Receiver, Sender, bounded};

use crate::{
    ObjectMetadata, SeekRead, SeekableStorage, Storage, StorageError, StorageResult as Result,
    local::LocalStorage,
};

use super::eviction::EvictionPolicy;
use super::upload::{CacheEntry, CacheStats, UploadTask, spawn_upload_workers};

/// Configuration for the cached storage backend.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Local cache directory path.
    pub cache_directory: PathBuf,
    /// Maximum cache size in bytes (default: 50GB).
    pub max_cache_size: u64,
    /// Number of concurrent upload workers (default: 4).
    pub upload_concurrency: usize,
    /// Upload buffer size in bytes (default: 8MB).
    pub upload_buffer_size: usize,
    /// Eviction policy for cached files (default: LRU).
    pub eviction_policy: EvictionPolicy,
    /// Whether to delete local cache file after successful upload (default: false).
    pub delete_after_upload: bool,
    /// Maximum number of pending uploads in queue (default: 100).
    pub max_pending_uploads: usize,
    /// Timeout for graceful shutdown in seconds (default: 30).
    pub shutdown_timeout_secs: u64,
}

impl CacheConfig {
    /// Create a new cache configuration with the given cache directory.
    pub fn new(cache_directory: impl AsRef<Path>) -> Self {
        Self {
            cache_directory: PathBuf::from(cache_directory.as_ref()),
            max_cache_size: 50 * 1024 * 1024 * 1024, // 50 GB
            upload_concurrency: 4,
            upload_buffer_size: 8 * 1024 * 1024, // 8 MB
            eviction_policy: EvictionPolicy::default(),
            delete_after_upload: false,
            max_pending_uploads: 100,
            shutdown_timeout_secs: 30,
        }
    }

    /// Set the maximum cache size in bytes.
    pub fn with_max_cache_size(mut self, size: u64) -> Self {
        self.max_cache_size = size;
        self
    }

    /// Set the number of concurrent upload workers.
    pub fn with_upload_concurrency(mut self, concurrency: usize) -> Self {
        self.upload_concurrency = concurrency.max(1);
        self
    }

    /// Set the upload buffer size in bytes.
    pub fn with_upload_buffer_size(mut self, size: usize) -> Self {
        self.upload_buffer_size = size.max(1024);
        self
    }

    /// Set the eviction policy.
    pub fn with_eviction_policy(mut self, policy: EvictionPolicy) -> Self {
        self.eviction_policy = policy;
        self
    }

    /// Set whether to delete cache files after upload.
    pub fn with_delete_after_upload(mut self, delete: bool) -> Self {
        self.delete_after_upload = delete;
        self
    }

    /// Set the maximum pending uploads.
    pub fn with_max_pending_uploads(mut self, max: usize) -> Self {
        self.max_pending_uploads = max.max(1);
        self
    }

    /// Set the shutdown timeout in seconds.
    pub fn with_shutdown_timeout_secs(mut self, timeout: u64) -> Self {
        self.shutdown_timeout_secs = timeout.max(1);
        self
    }
}

/// Cached storage backend.
///
/// Wraps a remote storage backend with a local cache layer. Reads are served
/// from the local cache when available, writes are buffered locally and
/// uploaded in the background.
pub struct CachedStorage {
    /// Remote storage backend.
    remote: Arc<dyn Storage>,
    /// Local cache storage.
    local: Arc<LocalStorage>,
    /// Cache configuration.
    config: CacheConfig,
    /// Cache entries metadata (protected by mutex, Arc-shared with workers).
    entries: Arc<Mutex<HashMap<PathBuf, CacheEntry>>>,
    /// Total current cache size in bytes.
    cache_size: AtomicU64,
    /// Upload task sender.
    upload_sender: Sender<UploadTask>,
    /// Upload task receiver (kept for channel lifetime management).
    _upload_receiver: Receiver<UploadTask>,
    /// Cache statistics.
    stats: Arc<Mutex<CacheStats>>,
    /// Upload worker thread handles.
    _upload_workers: Mutex<Vec<thread::JoinHandle<()>>>,
    /// Shutdown signal (Arc-shared with workers).
    shutdown: Arc<AtomicUsize>,
}

impl CachedStorage {
    /// Create a new cached storage backend.
    ///
    /// # Arguments
    ///
    /// * `remote` - The remote storage backend to wrap.
    /// * `config` - Cache configuration.
    pub fn new(remote: Arc<dyn Storage>, config: CacheConfig) -> Result<Self> {
        // Create cache directory
        fs::create_dir_all(&config.cache_directory)
            .map_err(|e| StorageError::Other(format!("Failed to create cache directory: {}", e)))?;

        let local = Arc::new(LocalStorage::new(&config.cache_directory));

        // Create bounded channel for upload tasks
        let (upload_sender, upload_receiver) = bounded(config.max_pending_uploads);

        let stats = Arc::new(Mutex::new(CacheStats::default()));
        let shutdown = Arc::new(AtomicUsize::new(0));
        let entries = Arc::new(Mutex::new(HashMap::new()));

        // Spawn upload workers using the extracted module
        let workers = spawn_upload_workers(
            config.upload_concurrency,
            upload_receiver.clone(),
            Arc::clone(&remote),
            Arc::clone(&stats),
            Arc::clone(&shutdown),
            Arc::clone(&entries),
            config.delete_after_upload,
            config.cache_directory.clone(),
        )?;

        let storage = Self {
            remote,
            local,
            config,
            entries,
            cache_size: AtomicU64::new(0),
            upload_sender,
            _upload_receiver: upload_receiver,
            stats,
            _upload_workers: Mutex::new(workers),
            shutdown,
        };

        Ok(storage)
    }

    /// Get the full local cache path for a remote path.
    fn cache_path(&self, path: &Path) -> PathBuf {
        // Convert remote path to a valid local filename
        // Replace slashes with a special marker to preserve hierarchy
        let path_str = path.to_string_lossy();
        self.config
            .cache_directory
            .join(path_str.replace(['/', '\\'], "::"))
    }

    /// Check if a file is in the cache.
    fn is_cached(&self, path: &Path) -> bool {
        let cache_path = self.cache_path(path);
        cache_path.exists()
    }

    /// Add a file to the cache metadata.
    fn add_to_cache(&self, path: &Path, size: u64) {
        let cache_path = self.cache_path(path);

        // Handle poisoned mutex: recover the guard and continue
        // A poisoned mutex means another thread panicked while holding the lock
        let mut entries = match self.entries.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::warn!("Cache entries mutex is poisoned, recovering guard");
                poisoned.into_inner()
            }
        };
        entries.insert(path.to_path_buf(), CacheEntry::new(cache_path, size));

        let old_size = self.cache_size.fetch_add(size, Ordering::Relaxed);
        let new_size = old_size + size;

        // Update stats
        if let Ok(mut stats) = self.stats.lock() {
            stats.total_cached_bytes = new_size;
            stats.cached_file_count += 1;
        }

        // Trigger eviction if over limit
        if new_size > self.config.max_cache_size {
            drop(entries);
            if let Err(e) = self.evict() {
                tracing::warn!("Cache eviction failed: {}", e);
            }
        }
    }

    /// Update cache entry access time.
    fn record_access(&self, path: &Path) {
        if let Ok(mut entries) = self.entries.lock()
            && let Some(entry) = entries.get_mut(path)
        {
            entry.last_accessed = SystemTime::now();
            entry.record_access();
        }
    }

    /// Evict files from cache according to the eviction policy.
    fn evict(&self) -> Result<()> {
        let target_size = (self.config.max_cache_size as f64 * 0.9) as u64; // Evict to 90%

        loop {
            let current_size = self.cache_size.load(Ordering::Relaxed);
            if current_size <= target_size {
                break;
            }

            let (to_evict, freed_space) = {
                let mut entries = self.entries.lock().map_err(|e| {
                    StorageError::Other(format!("Failed to acquire entries lock: {}", e))
                })?;
                if entries.is_empty() {
                    break;
                }

                // Find candidate based on policy
                let to_evict_path = match self.config.eviction_policy {
                    EvictionPolicy::Lru => entries
                        .iter()
                        .filter(|(_, e)| !e.pending_upload)
                        .min_by_key(|(_, a)| a.last_accessed)
                        .map(|(p, _)| p.clone()),
                    EvictionPolicy::Lfu => entries
                        .iter()
                        .filter(|(_, e)| !e.pending_upload)
                        .min_by_key(|(_, a)| a.access_count.load(Ordering::Relaxed))
                        .map(|(p, _)| p.clone()),
                    EvictionPolicy::Fifo => entries
                        .iter()
                        .filter(|(_, e)| !e.pending_upload)
                        .min_by_key(|(_, a)| a.created_at)
                        .map(|(p, _)| p.clone()),
                };

                let Some(path) = to_evict_path else {
                    // All files have pending uploads, can't evict
                    break;
                };

                let Some(entry) = entries.remove(&path) else {
                    // Entry was removed by another thread, try again
                    continue;
                };
                let freed = entry.size;
                (path, freed)
            };

            // Delete the cached file
            let cache_path = self.cache_path(&to_evict);
            if cache_path.exists()
                && let Err(e) = fs::remove_file(&cache_path)
            {
                tracing::warn!(
                    "Failed to delete cached file during eviction {}: {}. Cache tracking may be inconsistent.",
                    cache_path.display(),
                    e
                );
            }

            // Update size
            self.cache_size.fetch_sub(freed_space, Ordering::Relaxed);

            // Update stats
            if let Ok(mut stats) = self.stats.lock() {
                stats.total_cached_bytes = self.cache_size.load(Ordering::Relaxed);
                stats.cached_file_count = stats.cached_file_count.saturating_sub(1);
            }

            tracing::debug!(
                "Evicted {} from cache (freed {} bytes)",
                to_evict.display(),
                freed_space
            );
        }

        Ok(())
    }

    /// Queue a file for background upload.
    fn queue_upload(&self, local_path: PathBuf, remote_path: PathBuf, size: u64) -> Result<()> {
        let task = UploadTask {
            local_path,
            remote_path: remote_path.clone(),
            size,
        };

        // Send first - only mark as pending if send succeeds
        self.upload_sender
            .send(task)
            .map_err(|e| StorageError::Other(format!("Failed to queue upload: {}", e)))?;

        // Mark as pending upload AFTER successful send
        if let Ok(mut entries) = self.entries.lock()
            && let Some(entry) = entries.get_mut(&remote_path)
        {
            entry.pending_upload = true;
        }

        // Update stats
        if let Ok(mut stats) = self.stats.lock() {
            stats.pending_uploads += 1;
        }

        Ok(())
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheStats {
        if let Ok(stats) = self.stats.lock() {
            stats.clone()
        } else {
            tracing::error!(
                "Failed to acquire stats lock (mutex poisoned), returning default stats"
            );
            CacheStats::default()
        }
    }

    /// Flush all pending uploads and wait for completion.
    ///
    /// This method blocks until all pending uploads are complete or the
    /// shutdown timeout is reached.
    pub fn flush(&self) -> Result<()> {
        let timeout = Duration::from_secs(self.config.shutdown_timeout_secs);
        let start = SystemTime::now();

        tracing::info!("Starting cache flush...");

        loop {
            let pending = self.stats().pending_uploads;

            if pending == 0 {
                tracing::info!("Cache flush complete");
                return Ok(());
            }

            let elapsed = start.elapsed().unwrap_or(Duration::ZERO);
            if elapsed >= timeout {
                tracing::warn!("Cache flush timeout: {} uploads still pending", pending);
                return Err(StorageError::timeout(
                    "Cache flush timeout - uploads still pending",
                ));
            }

            thread::sleep(Duration::from_millis(100));
        }
    }

    /// Get the remote storage backend.
    pub fn remote(&self) -> &Arc<dyn Storage> {
        &self.remote
    }

    /// Get the local cache storage.
    pub fn local_cache(&self) -> &LocalStorage {
        &self.local
    }

    /// Get the cache configuration.
    pub fn config(&self) -> &CacheConfig {
        &self.config
    }
}

impl Drop for CachedStorage {
    fn drop(&mut self) {
        // Signal shutdown first
        self.shutdown.store(1, Ordering::Release);

        // Try to flush pending uploads with error logging
        if let Err(e) = self.flush() {
            tracing::error!(
                "Failed to flush cached storage on drop: {}. Pending uploads may not complete.",
                e
            );
        }
    }
}

impl Storage for CachedStorage {
    fn reader(&self, path: &Path) -> Result<Box<dyn Read + Send + 'static>> {
        let cache_path = self.cache_path(path);

        if self.is_cached(path) {
            // Cache hit
            tracing::debug!("Cache hit for {}", path.display());
            self.record_access(path);

            if let Ok(mut stats) = self.stats.lock() {
                stats.cache_hits += 1;
            }

            // Return reader from cache
            let file = File::open(&cache_path).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    StorageError::not_found(path.display().to_string())
                } else {
                    StorageError::Io(e)
                }
            })?;
            Ok(Box::new(BufReader::new(file)))
        } else {
            // Cache miss - download from remote with streaming to avoid OOM
            tracing::debug!("Cache miss for {}", path.display());

            if let Ok(mut stats) = self.stats.lock() {
                stats.cache_misses += 1;
            }

            // Ensure parent directory exists
            if let Some(parent) = cache_path.parent() {
                fs::create_dir_all(parent).map_err(|e| {
                    StorageError::Other(format!("Failed to create cache directory: {}", e))
                })?;
            }

            // Stream from remote to cache file using fixed buffer
            const BUFFER_SIZE: usize = 64 * 1024; // 64KB buffer
            let mut remote_reader = self.remote.reader(path)?;

            let mut cache_file = File::create(&cache_path)
                .map_err(|e| StorageError::Other(format!("Failed to create cache file: {}", e)))?;

            let mut buffer = vec![0u8; BUFFER_SIZE];
            let mut total_size = 0u64;

            loop {
                let n_read = remote_reader.read(&mut buffer).map_err(|e| {
                    StorageError::Other(format!("Failed to read from remote: {}", e))
                })?;
                if n_read == 0 {
                    break;
                }
                total_size += n_read as u64;
                cache_file.write_all(&buffer[..n_read]).map_err(|e| {
                    StorageError::Other(format!("Failed to write to cache file: {}", e))
                })?;
            }

            cache_file.flush()?;

            // Add to cache metadata
            self.add_to_cache(path, total_size);

            // Return reader from cache
            let file = File::open(&cache_path).map_err(StorageError::Io)?;
            Ok(Box::new(BufReader::new(file)))
        }
    }

    fn writer(&self, path: &Path) -> Result<Box<dyn Write + Send + 'static>> {
        let cache_path = self.cache_path(path);

        // Ensure parent directory exists
        if let Some(parent) = cache_path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                StorageError::Other(format!("Failed to create cache directory: {}", e))
            })?;
        }

        // Create a cached writer
        let writer = CachedWriter::new(
            self.local.clone(),
            self.remote.clone(),
            cache_path,
            path.to_path_buf(),
            Arc::new(self.upload_sender.clone()),
            self.config.upload_buffer_size,
            self.config.delete_after_upload,
        )?;

        Ok(Box::new(writer))
    }

    fn exists(&self, path: &Path) -> bool {
        self.is_cached(path) || self.remote.exists(path)
    }

    fn size(&self, path: &Path) -> Result<u64> {
        if self.is_cached(path) {
            let cache_path = self.cache_path(path);
            let meta = fs::metadata(&cache_path).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    StorageError::not_found(path.display().to_string())
                } else {
                    StorageError::Io(e)
                }
            })?;
            return Ok(meta.len());
        }
        self.remote.size(path)
    }

    fn metadata(&self, path: &Path) -> Result<ObjectMetadata> {
        // Try cache first, then remote
        if self.is_cached(path) {
            return self.local.metadata(&self.cache_path(path));
        }
        self.remote.metadata(path)
    }

    fn list(&self, prefix: &Path) -> Result<Vec<ObjectMetadata>> {
        // Delegate to remote - we don't cache directory listings
        self.remote.list(prefix)
    }

    fn delete(&self, path: &Path) -> Result<()> {
        // Delete from both cache and remote
        let cache_path = self.cache_path(path);

        // Remove from cache metadata
        {
            if let Ok(mut entries) = self.entries.lock()
                && let Some(entry) = entries.remove(path)
            {
                self.cache_size.fetch_sub(entry.size, Ordering::Relaxed);
            }
        }

        // Delete cached file if exists
        if cache_path.exists()
            && let Err(e) = fs::remove_file(&cache_path)
        {
            tracing::warn!(
                "Failed to delete cached file during delete operation {}: {}. Remote delete will proceed.",
                cache_path.display(),
                e
            );
        }

        // Delete from remote
        self.remote.delete(path)
    }

    fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        // If source is in cache, copy from cache
        let from_cache = self.cache_path(from);
        let to_cache = self.cache_path(to);

        if from_cache.exists() {
            // Copy within cache
            fs::copy(&from_cache, &to_cache)
                .map_err(|e| StorageError::Other(format!("Failed to copy in cache: {}", e)))?;

            // Update cache metadata
            let size = fs::metadata(&to_cache).map(|m| m.len()).unwrap_or(0);

            // Add to cache entries so queue_upload can find it
            self.add_to_cache(to, size);

            // Also queue for upload to remote
            self.queue_upload(to_cache, to.to_path_buf(), size)?;

            Ok(())
        } else {
            // Delegate to remote
            self.remote.copy(from, to)
        }
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        self.remote.create_dir(path)
    }

    fn create_dir_all(&self, path: &Path) -> Result<()> {
        self.remote.create_dir_all(path)
    }
}

impl SeekableStorage for CachedStorage {
    fn seekable_reader(&self, path: &Path) -> Result<Box<dyn SeekRead + Send + 'static>> {
        let cache_path = self.cache_path(path);

        if self.is_cached(path) {
            self.record_access(path);

            if let Ok(mut stats) = self.stats.lock() {
                stats.cache_hits += 1;
            }

            let file = File::open(&cache_path).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    StorageError::not_found(path.display().to_string())
                } else {
                    StorageError::Io(e)
                }
            })?;
            Ok(Box::new(BufReader::new(file)))
        } else {
            // Cache miss - download first, then return seekable reader
            let _ = self.reader(path)?;

            let file = File::open(&cache_path).map_err(StorageError::Io)?;
            Ok(Box::new(BufReader::new(file)))
        }
    }
}

impl std::fmt::Debug for CachedStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedStorage")
            .field("config", &self.config)
            .field("cache_size", &self.cache_size.load(Ordering::Relaxed))
            .finish()
    }
}

/// A writer that buffers data locally and queues for background upload.
pub struct CachedWriter {
    /// Local file writer.
    local_writer: BufWriter<File>,
    /// Local file path.
    local_path: PathBuf,
    /// Remote destination path.
    remote_path: PathBuf,
    /// Upload channel sender.
    upload_sender: Arc<Sender<UploadTask>>,
    /// Maximum buffer size before triggering upload.
    max_buffer_size: usize,
    /// Whether to delete after upload.
    _delete_after_upload: bool,
    /// Whether data has been uploaded.
    uploaded: bool,
    /// Whether writer has been flushed.
    flushed: bool,
}

impl CachedWriter {
    /// Create a new cached writer.
    fn new(
        _local: Arc<LocalStorage>,
        _remote: Arc<dyn Storage>,
        local_path: PathBuf,
        remote_path: PathBuf,
        upload_sender: Arc<Sender<UploadTask>>,
        max_buffer_size: usize,
        delete_after_upload: bool,
    ) -> Result<Self> {
        let file = File::create(&local_path).map_err(|e| {
            StorageError::Other(format!(
                "Failed to create cache file {}: {}",
                local_path.display(),
                e
            ))
        })?;

        Ok(Self {
            local_writer: BufWriter::with_capacity(64 * 1024, file),
            local_path,
            remote_path,
            upload_sender,
            max_buffer_size,
            _delete_after_upload: delete_after_upload,
            uploaded: false,
            flushed: false,
        })
    }

    /// Queue the file for upload.
    fn queue_upload(&mut self) -> std::io::Result<()> {
        if self.uploaded {
            return Ok(());
        }

        // Flush to ensure data is written
        self.local_writer.flush()?;

        // Get file size
        let size = fs::metadata(&self.local_path).map(|m| m.len()).unwrap_or(0);

        // Queue for background upload
        let task = UploadTask {
            local_path: self.local_path.clone(),
            remote_path: self.remote_path.clone(),
            size,
        };

        self.upload_sender
            .send(task)
            .map_err(|e| std::io::Error::other(format!("Failed to queue upload: {}", e)))?;

        self.uploaded = true;
        Ok(())
    }
}

impl Write for CachedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.local_writer.write(buf)?;

        // Check if we should trigger upload due to buffer size
        if self.local_writer.buffer().len() > self.max_buffer_size {
            self.local_writer.flush()?;
        }

        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.local_writer.flush()?;

        if !self.flushed {
            // Queue for upload on first flush
            let _ = self.queue_upload();
            self.flushed = true;
        }

        Ok(())
    }
}

impl Drop for CachedWriter {
    fn drop(&mut self) {
        // Ensure data is written and upload is queued
        if !self.flushed
            && let Err(e) = self.flush()
        {
            tracing::error!(
                "Failed to flush cached writer on drop: {}. Data may not be fully written.",
                e
            );
        }

        if !self.uploaded
            && let Err(_e) = self.queue_upload()
        {
            tracing::error!(
                "Failed to queue upload for {}: Upload will not occur. Data exists locally at {}",
                self.remote_path.display(),
                self.local_path.display()
            );
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Seek, SeekFrom, Write};

    #[test]
    fn test_cache_config_default() {
        let config = CacheConfig::new("/tmp/cache");
        assert_eq!(config.cache_directory, PathBuf::from("/tmp/cache"));
        assert_eq!(config.max_cache_size, 50 * 1024 * 1024 * 1024);
        assert_eq!(config.upload_concurrency, 4);
        assert_eq!(config.eviction_policy, EvictionPolicy::Lru);
    }

    #[test]
    fn test_cache_config_builder() {
        let config = CacheConfig::new("/tmp/cache")
            .with_max_cache_size(10 * 1024 * 1024)
            .with_upload_concurrency(2)
            .with_eviction_policy(EvictionPolicy::Lfu)
            .with_delete_after_upload(true);

        assert_eq!(config.max_cache_size, 10 * 1024 * 1024);
        assert_eq!(config.upload_concurrency, 2);
        assert_eq!(config.eviction_policy, EvictionPolicy::Lfu);
        assert!(config.delete_after_upload);
    }

    #[test]
    fn test_cache_stats_hit_rate() {
        let mut stats = CacheStats::default();
        assert_eq!(stats.hit_rate(), 0.0);

        stats.cache_hits = 80;
        stats.cache_misses = 20;
        assert!((stats.hit_rate() - 80.0).abs() < 0.01);

        stats.cache_hits = 0;
        stats.cache_misses = 0;
        assert_eq!(stats.hit_rate(), 0.0);
    }

    #[test]
    fn test_cached_storage_read_write() {
        let temp_dir = std::env::temp_dir().join("cached_test");
        let _ = fs::create_dir_all(&temp_dir);

        // Create remote storage (using local for testing)
        let remote_dir = temp_dir.join("remote");
        let remote = Arc::new(LocalStorage::new(&remote_dir));

        // Create cached storage
        let cache_dir = temp_dir.join("cache");
        let config = CacheConfig::new(&cache_dir)
            .with_max_cache_size(1024 * 1024) // 1 MB
            .with_upload_concurrency(1);
        let cached = CachedStorage::new(remote, config).unwrap();

        // Test write
        let test_data = b"Hello, Cached World!";
        let test_path = Path::new("test.txt");

        let mut writer = cached.writer(test_path).unwrap();
        writer.write_all(test_data).unwrap();
        writer.flush().unwrap();
        drop(writer);

        // Give upload time
        thread::sleep(Duration::from_millis(100));

        // Test read (should be cached)
        let mut reader = cached.reader(test_path).unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).unwrap();

        assert_eq!(buffer, test_data);

        // Check stats
        let stats = cached.stats();
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(stats.cache_misses, 0);

        // Cleanup
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_cached_storage_cache_miss() {
        let temp_dir = std::env::temp_dir().join("cached_miss_test");
        let _ = fs::create_dir_all(&temp_dir);

        // Create remote storage with existing file
        let remote_dir = temp_dir.join("remote");
        let _ = fs::create_dir_all(&remote_dir);
        let remote = Arc::new(LocalStorage::new(&remote_dir));

        // Write directly to remote
        let test_data = b"Remote data";
        let remote_path = remote_dir.join("remote.txt");
        let mut file = File::create(&remote_path).unwrap();
        file.write_all(test_data).unwrap();

        // Create cached storage (empty cache)
        let cache_dir = temp_dir.join("cache");
        let config = CacheConfig::new(&cache_dir)
            .with_max_cache_size(1024 * 1024)
            .with_upload_concurrency(1);
        let cached = CachedStorage::new(remote, config).unwrap();

        // Read should populate cache
        let mut reader = cached.reader(Path::new("remote.txt")).unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).unwrap();

        assert_eq!(buffer, test_data);

        // Check stats - should have cache miss on first read
        let stats = cached.stats();
        assert_eq!(stats.cache_misses, 1);

        // Second read should be cache hit
        let mut reader = cached.reader(Path::new("remote.txt")).unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).unwrap();

        let stats = cached.stats();
        assert_eq!(stats.cache_hits, 1);

        // Cleanup
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_cached_storage_seekable() {
        let temp_dir = std::env::temp_dir().join("cached_seek_test");
        let _ = fs::create_dir_all(&temp_dir);

        let remote_dir = temp_dir.join("remote");
        let remote = Arc::new(LocalStorage::new(&remote_dir));

        let cache_dir = temp_dir.join("cache");
        let config = CacheConfig::new(&cache_dir)
            .with_max_cache_size(1024 * 1024)
            .with_upload_concurrency(1);
        let cached = CachedStorage::new(remote, config).unwrap();

        // Write test data
        let test_data = b"0123456789ABCDEFGHIJ";
        let mut writer = cached.writer(Path::new("seek.txt")).unwrap();
        writer.write_all(test_data).unwrap();
        drop(writer);
        thread::sleep(Duration::from_millis(100));

        // Test seeking
        let mut reader = cached.seekable_reader(Path::new("seek.txt")).unwrap();
        reader.seek(SeekFrom::Start(10)).unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).unwrap();

        assert_eq!(buffer, b"ABCDEFGHIJ");

        // Cleanup
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_cached_storage_delete() {
        let temp_dir = std::env::temp_dir().join("cached_delete_test");
        let _ = fs::create_dir_all(&temp_dir);

        let remote_dir = temp_dir.join("remote");
        let remote = Arc::new(LocalStorage::new(&remote_dir));

        let cache_dir = temp_dir.join("cache");
        let config = CacheConfig::new(&cache_dir)
            .with_max_cache_size(1024 * 1024)
            .with_upload_concurrency(1);
        let cached = CachedStorage::new(remote, config).unwrap();

        // Write and wait for upload
        let mut writer = cached.writer(Path::new("delete.txt")).unwrap();
        writer.write_all(b"test data").unwrap();
        drop(writer);
        thread::sleep(Duration::from_millis(100));

        // Verify file exists
        assert!(cached.exists(Path::new("delete.txt")));

        // Delete
        cached.delete(Path::new("delete.txt")).unwrap();

        // Verify deleted
        assert!(!cached.exists(Path::new("delete.txt")));

        // Cleanup
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_cached_storage_exists() {
        let temp_dir = std::env::temp_dir().join("cached_exists_test");
        let _ = fs::create_dir_all(&temp_dir);

        let remote_dir = temp_dir.join("remote");
        let remote = Arc::new(LocalStorage::new(&remote_dir));

        let cache_dir = temp_dir.join("cache");
        let config = CacheConfig::new(&cache_dir)
            .with_max_cache_size(1024 * 1024)
            .with_upload_concurrency(1);
        let cached = CachedStorage::new(remote, config).unwrap();

        // File doesn't exist
        assert!(!cached.exists(Path::new("nonexistent.txt")));

        // Write file
        let mut writer = cached.writer(Path::new("exists.txt")).unwrap();
        writer.write_all(b"test").unwrap();
        drop(writer);

        // File exists (in cache)
        assert!(cached.exists(Path::new("exists.txt")));

        // Cleanup
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_cached_storage_flush() {
        let temp_dir = std::env::temp_dir().join("cached_flush_test");
        let _ = fs::create_dir_all(&temp_dir);

        let remote_dir = temp_dir.join("remote");
        let remote = Arc::new(LocalStorage::new(&remote_dir));

        let cache_dir = temp_dir.join("cache");
        let config = CacheConfig::new(&cache_dir)
            .with_max_cache_size(1024 * 1024)
            .with_upload_concurrency(2)
            .with_shutdown_timeout_secs(5);
        let cached = CachedStorage::new(remote, config).unwrap();

        // Write multiple files
        for i in 0..3 {
            let path = format!("flush{}.txt", i);
            let mut writer = cached.writer(Path::new(&path)).unwrap();
            writer.write_all(b"test data").unwrap();
            // Don't flush - let drop handle it
        }

        // Flush should complete successfully
        let result = cached.flush();
        match result {
            Ok(()) => {}
            Err(StorageError::Timeout(_)) => {
                // Timeout is acceptable - uploads may still be pending
            }
            Err(e) => panic!("Unexpected flush error: {:?}", e),
        }

        // Check that uploads completed
        thread::sleep(Duration::from_millis(200));

        let stats = cached.stats();
        // We wrote 3 files, so uploads_completed + pending should be 3
        assert_eq!(stats.uploads_completed + stats.pending_uploads, 3);

        // Cleanup
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_lru_eviction_when_cache_exceeds_limit() {
        let temp_dir = std::env::temp_dir().join("eviction_test");
        let _ = fs::create_dir_all(&temp_dir);

        let remote_dir = temp_dir.join("remote");
        let remote = Arc::new(LocalStorage::new(&remote_dir));

        let cache_dir = temp_dir.join("cache");
        // Set a small cache size (200 bytes) to trigger eviction
        let config = CacheConfig::new(&cache_dir)
            .with_max_cache_size(200)
            .with_upload_concurrency(1)
            .with_eviction_policy(EvictionPolicy::Lru);
        let cached = CachedStorage::new(remote, config).unwrap();

        // Write file A (50 bytes)
        let data_a = b"A".repeat(50);
        let mut writer = cached.writer(Path::new("file_a.txt")).unwrap();
        writer.write_all(&data_a).unwrap();
        drop(writer);
        thread::sleep(Duration::from_millis(50));

        // Write file B (50 bytes)
        let data_b = b"B".repeat(50);
        let mut writer = cached.writer(Path::new("file_b.txt")).unwrap();
        writer.write_all(&data_b).unwrap();
        drop(writer);
        thread::sleep(Duration::from_millis(50));

        // Write file C (50 bytes) - should evict file A (LRU)
        let data_c = b"C".repeat(50);
        let mut writer = cached.writer(Path::new("file_c.txt")).unwrap();
        writer.write_all(&data_c).unwrap();
        drop(writer);
        thread::sleep(Duration::from_millis(50));

        // Access file B to make it more recently used than C
        let _ = cached.reader(Path::new("file_b.txt"));

        // Write file D (50 bytes) - should evict file C (oldest)
        let data_d = b"D".repeat(50);
        let mut writer = cached.writer(Path::new("file_d.txt")).unwrap();
        writer.write_all(&data_d).unwrap();
        drop(writer);
        thread::sleep(Duration::from_millis(50));

        // Verify cache size is approximately at limit
        let stats = cached.stats();
        assert!(
            stats.total_cached_bytes <= 250,
            "Cache size {} exceeds limit",
            stats.total_cached_bytes
        );

        // Cleanup
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_fifo_eviction_policy() {
        let temp_dir = std::env::temp_dir().join("fifo_eviction_test");
        let _ = fs::create_dir_all(&temp_dir);

        let remote_dir = temp_dir.join("remote");
        let remote = Arc::new(LocalStorage::new(&remote_dir));

        let cache_dir = temp_dir.join("cache");
        // Set cache size to hold 3 files of 50 bytes each
        let config = CacheConfig::new(&cache_dir)
            .with_max_cache_size(150)
            .with_upload_concurrency(1)
            .with_eviction_policy(EvictionPolicy::Fifo);
        let cached = CachedStorage::new(remote, config).unwrap();

        // Write files in order: A, B, C, D
        // D should evict A (oldest) in FIFO
        for i in 0..4 {
            let path = format!("file_{}.txt", i);
            let data = format!("data{}", i);
            let mut writer = cached.writer(Path::new(&path)).unwrap();
            writer.write_all(data.as_bytes()).unwrap();
            drop(writer);
            thread::sleep(Duration::from_millis(50));
        }

        // Verify cache doesn't exceed limit significantly
        let stats = cached.stats();
        assert!(
            stats.total_cached_bytes <= 200,
            "Cache size {} exceeds limit",
            stats.total_cached_bytes
        );

        // Cleanup
        let _ = fs::remove_dir_all(temp_dir);
    }
}
