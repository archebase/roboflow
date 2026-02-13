// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Parallel episode upload with progress tracking.
//!
//! This module provides coordinated parallel upload of episode files (Parquet + videos)
//! with progress tracking and statistics collection.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use std::collections::HashMap;

use std::fs::File;

use std::io::{BufReader, Read};

use std::sync::Mutex;

use std::thread;

use std::time::Instant;

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};

use std::sync::MutexGuard;

use std::thread::JoinHandle;

use crossbeam_channel::{Receiver, Sender, bounded};

use roboflow_storage::Storage;

use roboflow_core::Result;

// Import the unified upload coordinator trait
use crate::common::upload_coordinator::{UploadCoordinator, UploadProgress as UnifiedProgress};

/// Progress callback type for upload progress tracking.
///
/// Called with (file_name, bytes_uploaded, total_bytes).
pub type UploadProgress = Arc<dyn Fn(&str, u64, u64) + Send + Sync>;

// =============================================================================
// Upload Configuration
// =============================================================================

/// Upload configuration for episode files.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadConfig {
    /// Maximum number of concurrent upload workers.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,

    /// Whether to show progress updates.
    #[serde(default = "default_show_progress")]
    pub show_progress: bool,

    /// Whether to delete local files immediately after successful upload.
    /// If false, files are deleted during `shutdown_and_cleanup()`.
    #[serde(default = "default_delete_after_upload")]
    pub delete_after_upload: bool,

    /// Maximum number of pending uploads in the queue.
    #[serde(default = "default_max_pending")]
    pub max_pending: usize,

    /// Maximum retry attempts for failed uploads.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Initial backoff duration in milliseconds.
    #[serde(default = "default_initial_backoff_ms")]
    pub initial_backoff_ms: u64,
}

fn default_concurrency() -> usize {
    4
}

fn default_show_progress() -> bool {
    true
}

fn default_delete_after_upload() -> bool {
    false
}

fn default_max_pending() -> usize {
    100
}

fn default_max_retries() -> u32 {
    3
}

fn default_initial_backoff_ms() -> u64 {
    100
}

impl Default for UploadConfig {
    fn default() -> Self {
        Self {
            concurrency: default_concurrency(),
            show_progress: default_show_progress(),
            delete_after_upload: default_delete_after_upload(),
            max_pending: default_max_pending(),
            max_retries: default_max_retries(),
            initial_backoff_ms: default_initial_backoff_ms(),
        }
    }
}

// =============================================================================
// Episode Files
// =============================================================================

/// Type alias for completed uploads tracking per episode.
/// Maps episode_index -> (completed_video_cameras, parquet_completed)
pub type CompletedUploadsMap = HashMap<u64, (Vec<String>, bool)>;

// =============================================================================

/// Collection of files for a single episode.
#[derive(Debug, Clone)]
pub struct EpisodeFiles {
    /// Path to the Parquet file.
    pub parquet_path: PathBuf,

    /// Video file paths with camera names.
    pub video_paths: Vec<(String, PathBuf)>,

    /// Remote prefix for upload (e.g., "bucket/path").
    pub remote_prefix: String,

    /// Episode index.
    pub episode_index: u64,
}

impl EpisodeFiles {
    /// Create a new EpisodeFiles instance.
    pub fn new(
        parquet_path: PathBuf,
        video_paths: Vec<(String, PathBuf)>,
        remote_prefix: String,
        episode_index: u64,
    ) -> Self {
        Self {
            parquet_path,
            video_paths,
            remote_prefix,
            episode_index,
        }
    }

    /// Get all file paths to upload.
    pub fn all_paths(&self) -> Vec<PathBuf> {
        let mut paths = vec![self.parquet_path.clone()];
        for (_, path) in &self.video_paths {
            paths.push(path.clone());
        }
        paths
    }

    /// Calculate total size of all files.
    pub fn total_size(&self) -> Result<u64> {
        let mut total = 0;
        for path in self.all_paths() {
            let metadata = std::fs::metadata(&path).map_err(|e| {
                roboflow_core::RoboflowError::io(format!("Failed to get file size: {}", e))
            })?;
            total += metadata.len();
        }
        Ok(total)
    }

    /// Get the number of files.
    pub fn file_count(&self) -> usize {
        1 + self.video_paths.len()
    }
}

// =============================================================================
// Upload Statistics
// =============================================================================

/// Statistics collected during episode uploads.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UploadStats {
    /// Total bytes uploaded.
    pub total_bytes: u64,

    /// Total number of files uploaded.
    pub total_files: u32,

    /// Total duration of all uploads.
    #[serde(default)]
    pub total_duration: Duration,

    /// Number of files that failed to upload.
    pub failed_count: u32,

    /// List of files that failed to upload.
    pub failed_files: Vec<String>,

    /// Number of files currently pending upload.
    pub pending_count: usize,

    /// Number of uploads in progress.
    pub in_progress_count: usize,
}

impl UploadStats {
    /// Create a new empty UploadStats.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the success rate as a percentage.
    pub fn success_rate(&self) -> f64 {
        let total = self.total_files + self.failed_count;
        if total == 0 {
            return 100.0;
        }
        (self.total_files as f64 / total as f64) * 100.0
    }

    /// Get the average throughput in MB/s.
    pub fn throughput_mbps(&self) -> f64 {
        let secs = self.total_duration.as_secs_f64();
        if secs == 0.0 {
            return 0.0;
        }
        (self.total_bytes as f64 / (1024.0 * 1024.0)) / secs
    }
}

// =============================================================================
// Upload Task (Internal)
// =============================================================================

/// File type for upload tracking.
#[derive(Debug, Clone, PartialEq)]
enum UploadFileType {
    /// Parquet dataset file.
    Parquet,
    /// Video file with camera name.
    Video(String),
}

/// Internal task for the upload worker queue.
#[derive(Debug)]
struct UploadTask {
    /// Local file path to upload.
    local_path: PathBuf,

    /// Remote destination path.
    remote_path: PathBuf,

    /// File size for progress tracking.
    file_size: u64,

    /// Episode index for tracking (0 if not applicable).
    episode_index: Option<u64>,

    /// File type identifier for tracking.
    file_type: UploadFileType,
}

// =============================================================================
// Episode Upload Coordinator
// =============================================================================

/// Coordinator for parallel episode file uploads.
///
/// This coordinator manages a pool of worker threads that upload files
/// concurrently, with progress tracking and statistics collection.
pub struct EpisodeUploadCoordinator {
    /// Storage backend for uploads.
    storage: Arc<dyn Storage>,

    /// Upload configuration.
    config: UploadConfig,

    /// Optional progress callback.
    progress: Option<UploadProgress>,

    /// Upload task sender.
    sender: Sender<UploadTask>,

    /// Worker thread handles.
    workers: Mutex<Vec<JoinHandle<()>>>,

    /// Upload statistics (Arc-shared with workers).
    stats: Arc<Mutex<UploadStats>>,

    /// Pending files per episode (for cleanup).
    pending_files: Arc<Mutex<HashMap<u64, Vec<PathBuf>>>>,

    /// Completed uploads per episode for checkpoint tracking.
    completed_uploads: Arc<Mutex<CompletedUploadsMap>>,

    /// Atomic counters for thread-safe stats.
    bytes_uploaded: Arc<AtomicU64>,
    files_uploaded: Arc<AtomicU32>,
    files_failed: Arc<AtomicU32>,
    files_pending: Arc<AtomicUsize>,
    files_in_progress: Arc<AtomicUsize>,

    /// Shutdown signal (0 = running, 1 = shutting down).
    shutdown: Arc<AtomicUsize>,

    /// Start time for duration tracking.
    start_time: Instant,
}

impl EpisodeUploadCoordinator {
    /// Create a new upload coordinator.
    ///
    /// # Arguments
    ///
    /// * `storage` - Storage backend for uploads
    /// * `config` - Upload configuration
    /// * `progress` - Optional progress callback
    pub fn new(
        storage: Arc<dyn Storage>,
        config: UploadConfig,
        progress: Option<UploadProgress>,
    ) -> Result<Self> {
        let (sender, receiver) = bounded(config.max_pending);

        let stats = Arc::new(Mutex::new(UploadStats::new()));
        let pending_files = Arc::new(Mutex::new(HashMap::new()));
        let completed_uploads: Arc<Mutex<CompletedUploadsMap>> =
            Arc::new(Mutex::new(HashMap::new()));
        let bytes_uploaded = Arc::new(AtomicU64::new(0));
        let files_uploaded = Arc::new(AtomicU32::new(0));
        let files_failed = Arc::new(AtomicU32::new(0));
        let files_pending = Arc::new(AtomicUsize::new(0));
        let files_in_progress = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicUsize::new(0));

        let coordinator = Self {
            storage,
            config,
            progress,
            sender,
            workers: Mutex::new(Vec::new()),
            stats,
            pending_files,
            completed_uploads,
            bytes_uploaded,
            files_uploaded,
            files_failed,
            files_pending,
            files_in_progress,
            shutdown,
            start_time: Instant::now(),
        };

        coordinator.spawn_workers(receiver)?;
        Ok(coordinator)
    }

    /// Spawn background upload worker threads.
    fn spawn_workers(&self, receiver: Receiver<UploadTask>) -> Result<()> {
        let mut workers = self.workers.lock().map_err(|e| {
            roboflow_core::RoboflowError::other(format!("Failed to acquire workers lock: {}", e))
        })?;

        for worker_id in 0..self.config.concurrency {
            let receiver = receiver.clone();
            let storage = Arc::clone(&self.storage);
            let progress = self.progress.clone();
            let stats = Arc::clone(&self.stats);
            let completed_uploads = Arc::clone(&self.completed_uploads);
            let bytes_uploaded = Arc::clone(&self.bytes_uploaded);
            let files_uploaded = Arc::clone(&self.files_uploaded);
            let files_failed = Arc::clone(&self.files_failed);
            let files_pending = Arc::clone(&self.files_pending);
            let files_in_progress = Arc::clone(&self.files_in_progress);
            let shutdown = Arc::clone(&self.shutdown);
            let max_retries = self.config.max_retries;
            let initial_backoff_ms = self.config.initial_backoff_ms;
            let delete_after_upload = self.config.delete_after_upload;

            let handle = thread::Builder::new()
                .name(format!("episode-upload-{}", worker_id))
                .spawn(move || {
                    Self::upload_worker(
                        worker_id,
                        receiver,
                        storage,
                        progress,
                        stats,
                        completed_uploads,
                        bytes_uploaded,
                        files_uploaded,
                        files_failed,
                        files_pending,
                        files_in_progress,
                        shutdown,
                        max_retries,
                        initial_backoff_ms,
                        delete_after_upload,
                    )
                })
                .map_err(|e| {
                    roboflow_core::RoboflowError::other(format!(
                        "Failed to spawn upload worker: {}",
                        e
                    ))
                })?;

            workers.push(handle);
        }

        Ok(())
    }

    /// Background upload worker function.
    #[allow(clippy::too_many_arguments)]
    fn upload_worker(
        worker_id: usize,
        receiver: Receiver<UploadTask>,
        storage: Arc<dyn Storage>,
        progress: Option<UploadProgress>,
        stats: Arc<Mutex<UploadStats>>,
        completed_uploads: Arc<Mutex<CompletedUploadsMap>>,
        bytes_uploaded: Arc<AtomicU64>,
        files_uploaded: Arc<AtomicU32>,
        files_failed: Arc<AtomicU32>,
        files_pending: Arc<AtomicUsize>,
        files_in_progress: Arc<AtomicUsize>,
        shutdown: Arc<AtomicUsize>,
        max_retries: u32,
        initial_backoff_ms: u64,
        delete_after_upload: bool,
    ) {
        tracing::debug!("Upload worker {} started", worker_id);

        loop {
            // Check for shutdown signal
            if shutdown.load(Ordering::Acquire) != 0 {
                tracing::debug!("Upload worker {} shutting down", worker_id);
                break;
            }

            // Receive task with timeout
            match receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(task) => {
                    // Update counters BEFORE checking shutdown to ensure consistency
                    files_in_progress.fetch_add(1, Ordering::Relaxed);
                    files_pending.fetch_sub(1, Ordering::Relaxed);

                    // Check shutdown AFTER updating counters - if set, finish this task then exit
                    if shutdown.load(Ordering::Acquire) != 0 {
                        tracing::debug!(
                            "Worker {} shutting down, completing current task",
                            worker_id
                        );
                    }

                    let result = Self::upload_with_retry(
                        worker_id,
                        &task,
                        &storage,
                        &progress,
                        max_retries,
                        initial_backoff_ms,
                    );

                    match result {
                        Ok(bytes) => {
                            bytes_uploaded.fetch_add(bytes, Ordering::Relaxed);
                            files_uploaded.fetch_add(1, Ordering::Relaxed);

                            tracing::info!(
                                worker = worker_id,
                                file = %task.local_path.display(),
                                bytes = bytes,
                                remote = %task.remote_path.display(),
                                "Upload completed successfully"
                            );

                            // Track completed upload for checkpointing
                            if let Some(episode_idx) = task.episode_index {
                                let mut completed =
                                    completed_uploads.lock().unwrap_or_else(|e| e.into_inner());
                                let entry = completed
                                    .entry(episode_idx)
                                    .or_insert_with(|| (Vec::new(), false));
                                if task.file_type == UploadFileType::Parquet {
                                    entry.1 = true; // Mark parquet as completed
                                } else if let UploadFileType::Video(camera) = task.file_type {
                                    entry.0.push(camera);
                                }
                            }

                            // Delete local file if configured
                            if delete_after_upload {
                                if let Err(e) = std::fs::remove_file(&task.local_path) {
                                    tracing::error!(
                                        "Worker {} failed to delete file {}: {}",
                                        worker_id,
                                        task.local_path.display(),
                                        e
                                    );
                                } else {
                                    tracing::trace!(
                                        "Worker {} deleted local file: {}",
                                        worker_id,
                                        task.local_path.display()
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                "Worker {} failed to upload {}: {}",
                                worker_id,
                                task.local_path.display(),
                                e
                            );
                            files_failed.fetch_add(1, Ordering::Relaxed);

                            // Update failed files list - recover from poisoned state
                            let mut stats_guard = stats.lock().unwrap_or_else(
                                |e: std::sync::PoisonError<MutexGuard<UploadStats>>| e.into_inner(),
                            );
                            stats_guard
                                .failed_files
                                .push(task.local_path.display().to_string());
                        }
                    }

                    files_in_progress.fetch_sub(1, Ordering::Relaxed);

                    // Check shutdown after completing task and exit if set
                    if shutdown.load(Ordering::Acquire) != 0 {
                        tracing::debug!(
                            "Worker {} exiting after completing current task",
                            worker_id
                        );
                        break;
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    // Timeout, continue loop to check shutdown
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    tracing::debug!("Upload worker {} channel disconnected", worker_id);
                    break;
                }
            }
        }

        tracing::debug!("Upload worker {} stopped", worker_id);
    }

    /// Upload a file with retry logic.
    fn upload_with_retry(
        worker_id: usize,
        task: &UploadTask,
        storage: &Arc<dyn Storage>,
        progress: &Option<UploadProgress>,
        max_retries: u32,
        initial_backoff_ms: u64,
    ) -> Result<u64> {
        let mut attempt = 0;
        let mut backoff_ms = initial_backoff_ms;

        loop {
            attempt += 1;

            match Self::upload_file(worker_id, task, storage, progress) {
                Ok(bytes) => {
                    if attempt > 1 {
                        tracing::info!(
                            "Worker {} upload succeeded on attempt {}: {}",
                            worker_id,
                            attempt,
                            task.local_path.display()
                        );
                    }
                    return Ok(bytes);
                }
                Err(e) => {
                    // Use the is_retryable method for proper error classification
                    let is_retryable = e.is_retryable();

                    if attempt >= max_retries || !is_retryable {
                        tracing::error!(
                            "Worker {} upload failed after {} attempts: {} - {}",
                            worker_id,
                            attempt,
                            task.local_path.display(),
                            e
                        );
                        return Err(e);
                    }

                    tracing::warn!(
                        "Worker {} upload attempt {} failed, retrying in {}ms: {} - {}",
                        worker_id,
                        attempt,
                        backoff_ms,
                        task.local_path.display(),
                        e
                    );

                    thread::sleep(Duration::from_millis(backoff_ms));
                    backoff_ms = (backoff_ms * 2).min(5000); // Exponential backoff, max 5s
                }
            }
        }
    }

    /// Upload a single file with chunked streaming.
    ///
    /// This method streams the file in chunks (256KB) to avoid loading
    /// the entire file into memory, which is important for large video files.
    fn upload_file(
        worker_id: usize,
        task: &UploadTask,
        storage: &Arc<dyn Storage>,
        progress: &Option<UploadProgress>,
    ) -> Result<u64> {
        const CHUNK_SIZE: usize = 256 * 1024; // 256KB chunks

        let file = File::open(&task.local_path).map_err(|e| {
            roboflow_core::RoboflowError::io(format!(
                "Failed to open file {}: {}",
                task.local_path.display(),
                e
            ))
        })?;

        let mut reader = BufReader::with_capacity(CHUNK_SIZE, file);

        // Upload to storage
        let mut writer = storage.writer(&task.remote_path).map_err(|e| {
            roboflow_core::RoboflowError::storage(
                "storage",
                format!(
                    "Failed to create writer for {}: {}",
                    task.remote_path.display(),
                    e
                ),
                false,
            )
        })?;

        use std::io::Write;
        let mut buffer = vec![0u8; CHUNK_SIZE];
        let mut total_bytes = 0u64;

        loop {
            let n = reader.read(&mut buffer).map_err(|e| {
                roboflow_core::RoboflowError::io(format!(
                    "Failed to read file {}: {}",
                    task.local_path.display(),
                    e
                ))
            })?;

            if n == 0 {
                break; // EOF
            }

            writer.write_all(&buffer[..n]).map_err(|e| {
                roboflow_core::RoboflowError::io(format!("Failed to write data: {}", e))
            })?;

            total_bytes += n as u64;
        }

        writer.flush().map_err(|e| {
            roboflow_core::RoboflowError::io(format!("Failed to flush data: {}", e))
        })?;

        // Call progress callback
        if let Some(cb) = progress {
            let file_name = match &task.file_type {
                UploadFileType::Parquet => "parquet",
                UploadFileType::Video(camera) => camera,
            };
            cb(file_name, total_bytes, task.file_size);
        }

        tracing::trace!(
            "Worker {} uploaded {} ({} bytes) -> {}",
            worker_id,
            task.local_path.display(),
            total_bytes,
            task.remote_path.display()
        );

        Ok(total_bytes)
    }

    /// Queue an episode for upload.
    ///
    /// This queues all files (Parquet + videos) for parallel upload.
    pub fn queue_episode_upload(&self, episode: EpisodeFiles) -> Result<()> {
        // Build remote path prefix - avoid leading slash when prefix is empty
        let prefix = if episode.remote_prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", episode.remote_prefix.trim_end_matches('/'))
        };

        let mut files = vec![(
            episode.parquet_path.clone(),
            format!(
                "{}data/chunk-000/episode_{:06}.parquet",
                prefix, episode.episode_index
            ),
            UploadFileType::Parquet,
        )];

        for (camera, path) in &episode.video_paths {
            let filename = path
                .file_name()
                .ok_or_else(|| {
                    roboflow_core::RoboflowError::other(format!(
                        "Invalid video path (no filename): {}",
                        path.display()
                    ))
                })?
                .to_string_lossy();
            files.push((
                path.clone(),
                format!("{}videos/chunk-000/{}/{}", prefix, camera, filename),
                UploadFileType::Video(camera.clone()),
            ));
        }

        // Get file sizes and update stats
        for (local_path, remote_path, file_type) in &files {
            // Check if local file exists before queuing
            if !local_path.exists() {
                tracing::error!(
                    local = %local_path.display(),
                    remote = %remote_path,
                    "Cannot queue upload - local file does not exist"
                );
                return Err(roboflow_core::RoboflowError::io(format!(
                    "Cannot queue upload - local file does not exist: {}",
                    local_path.display()
                )));
            }
            let metadata = std::fs::metadata(local_path).map_err(|e| {
                roboflow_core::RoboflowError::io(format!("Failed to get file size: {}", e))
            })?;

            let task = UploadTask {
                local_path: local_path.clone(),
                remote_path: PathBuf::from(remote_path),
                file_size: metadata.len(),
                episode_index: Some(episode.episode_index),
                file_type: file_type.clone(),
            };

            self.sender.send(task).map_err(|e| {
                roboflow_core::RoboflowError::other(format!("Failed to queue upload task: {}", e))
            })?;

            self.files_pending.fetch_add(1, Ordering::Relaxed);

            // Track pending files for cleanup
            let mut pending = self.pending_files.lock().map_err(|e| {
                roboflow_core::RoboflowError::other(format!(
                    "Failed to acquire pending lock: {}",
                    e
                ))
            })?;
            pending
                .entry(episode.episode_index)
                .or_default()
                .push(local_path.clone());
        }

        tracing::debug!(
            "Queued {} files for upload (episode {})",
            files.len(),
            episode.episode_index
        );

        Ok(())
    }

    /// Get current upload statistics.
    pub fn stats(&self) -> UploadStats {
        let mut stats = self.stats.lock().unwrap_or_else(
            |e: std::sync::PoisonError<MutexGuard<UploadStats>>| {
                tracing::warn!(
                    "Stats mutex was poisoned, recovering. This indicates a previous panic."
                );
                e.into_inner()
            },
        );
        stats.total_bytes = self.bytes_uploaded.load(Ordering::Relaxed);
        stats.total_files = self.files_uploaded.load(Ordering::Relaxed);
        stats.failed_count = self.files_failed.load(Ordering::Relaxed);
        stats.pending_count = self.files_pending.load(Ordering::Relaxed);
        stats.in_progress_count = self.files_in_progress.load(Ordering::Relaxed);
        stats.total_duration = self.start_time.elapsed();
        stats.clone()
    }

    /// Get completed upload state for checkpointing.
    ///
    /// Returns a map of episode_index -> (completed_video_cameras, parquet_completed).
    /// This can be used to track upload progress for fault tolerance.
    pub fn completed_uploads(&self) -> CompletedUploadsMap {
        self.completed_uploads
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Wait for all pending uploads to complete.
    pub fn flush(&self) -> Result<()> {
        let timeout = Duration::from_secs(300); // 5 minute timeout
        let start = Instant::now();

        let initial_pending = self.files_pending.load(Ordering::Relaxed);
        let initial_in_progress = self.files_in_progress.load(Ordering::Relaxed);

        tracing::debug!(
            pending = initial_pending,
            in_progress = initial_in_progress,
            "Upload flush: starting wait"
        );

        while self.files_pending.load(Ordering::Relaxed) > 0
            || self.files_in_progress.load(Ordering::Relaxed) > 0
        {
            if start.elapsed() > timeout {
                let pending = self.files_pending.load(Ordering::Relaxed);
                let in_progress = self.files_in_progress.load(Ordering::Relaxed);
                return Err(roboflow_core::RoboflowError::timeout(format!(
                    "Flush timed out waiting for uploads to complete. Pending: {}, In progress: {}",
                    pending, in_progress
                )));
            }
            thread::sleep(Duration::from_millis(100));
        }

        tracing::debug!(
            elapsed_ms = start.elapsed().as_millis(),
            "Upload flush: all uploads complete"
        );

        Ok(())
    }

    /// Shutdown and cleanup.
    ///
    /// This waits for all pending uploads to complete and then cleans up
    /// local files if configured.
    pub fn shutdown_and_cleanup(self) -> Result<UploadStats> {
        tracing::info!("Shutting down upload coordinator...");

        // Flush pending uploads FIRST before signaling shutdown
        // This ensures workers process all queued tasks before exiting
        self.flush()?;

        // Signal shutdown AFTER flush completes
        self.shutdown.store(1, Ordering::Release);

        // Join workers
        let mut workers = self.workers.lock().map_err(|e| {
            roboflow_core::RoboflowError::other(format!("Failed to acquire workers lock: {}", e))
        })?;

        for worker in workers.drain(..) {
            let worker: JoinHandle<()> = worker;
            if let Err(e) = worker.join() {
                tracing::error!("Worker thread panicked: {:?}", e);
            }
        }

        // Clean up pending files if not already deleted
        if !self.config.delete_after_upload {
            let pending = self.pending_files.lock().unwrap_or_else(
                |e: std::sync::PoisonError<std::sync::MutexGuard<HashMap<u64, Vec<PathBuf>>>>| {
                    tracing::warn!("Pending files mutex was poisoned during cleanup");
                    e.into_inner()
                },
            );
            for (_episode, files) in pending.iter() {
                for path in files {
                    if let Err(e) = std::fs::remove_file(path) {
                        tracing::warn!("Failed to delete file {}: {}", path.display(), e);
                    }
                }
            }
        }

        // Get final stats
        let stats = self.stats();

        tracing::info!(
            "Upload coordinator shut down: {} files uploaded ({} failed, {:.2} MB, {:.2}s)",
            stats.total_files,
            stats.failed_count,
            stats.total_bytes as f64 / (1024.0 * 1024.0),
            stats.total_duration.as_secs_f64()
        );

        Ok(stats)
    }
}

impl Drop for EpisodeUploadCoordinator {
    fn drop(&mut self) {
        // Signal shutdown
        self.shutdown.store(1, Ordering::Release);

        // Try to flush with logging
        match self.flush() {
            Ok(_) => {
                tracing::debug!("Upload coordinator flushed successfully before drop");
            }
            Err(e) => tracing::error!(
                "Upload coordinator drop failed to flush pending uploads: {}. Pending: {}, In progress: {}",
                e,
                self.files_pending.load(Ordering::Relaxed),
                self.files_in_progress.load(Ordering::Relaxed)
            ),
        }
    }
}

// =============================================================================
// UploadCoordinator Trait Implementation
// =============================================================================

impl UploadCoordinator for EpisodeUploadCoordinator {
    fn upload(&self, local_path: &Path, remote_path: &Path) -> Result<()> {
        // Check if local file exists
        if !local_path.exists() {
            return Err(roboflow_core::RoboflowError::io(format!(
                "Local file does not exist: {}",
                local_path.display()
            )));
        }

        // Get file size
        let metadata = std::fs::metadata(local_path).map_err(|e| {
            roboflow_core::RoboflowError::io(format!("Failed to get file size: {}", e))
        })?;

        // Create and queue the upload task
        let task = UploadTask {
            local_path: local_path.to_path_buf(),
            remote_path: remote_path.to_path_buf(),
            file_size: metadata.len(),
            episode_index: None,
            file_type: UploadFileType::Parquet, // Default to Parquet type
        };

        self.sender.send(task).map_err(|e| {
            roboflow_core::RoboflowError::other(format!("Failed to queue upload task: {}", e))
        })?;

        self.files_pending.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    fn upload_parallel(&self, items: &[(PathBuf, PathBuf)]) -> Result<()> {
        for (local_path, remote_path) in items {
            self.upload(local_path, remote_path)?;
        }
        Ok(())
    }

    fn progress(&self) -> UnifiedProgress {
        UnifiedProgress {
            files_uploaded: self.files_uploaded.load(Ordering::Relaxed) as u64,
            files_failed: self.files_failed.load(Ordering::Relaxed) as u64,
            bytes_uploaded: self.bytes_uploaded.load(Ordering::Relaxed),
            files_pending: self.files_pending.load(Ordering::Relaxed) as u64,
            files_in_progress: self.files_in_progress.load(Ordering::Relaxed) as u64,
        }
    }

    fn flush(&self) -> Result<()> {
        // Call the existing flush implementation from EpisodeUploadCoordinator
        EpisodeUploadCoordinator::flush(self)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upload_config_default() {
        let config = UploadConfig::default();
        assert_eq!(config.concurrency, 4);
        assert!(config.show_progress);
        assert!(!config.delete_after_upload);
        assert_eq!(config.max_pending, 100);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_backoff_ms, 100);
    }

    #[test]
    fn test_upload_stats_success_rate() {
        let mut stats = UploadStats::new();
        assert_eq!(stats.success_rate(), 100.0);

        stats.total_files = 8;
        stats.failed_count = 2;
        assert_eq!(stats.success_rate(), 80.0);
    }

    #[test]
    fn test_upload_stats_throughput() {
        let mut stats = UploadStats::new();
        stats.total_bytes = 10 * 1024 * 1024; // 10 MB
        stats.total_duration = Duration::from_secs(2);
        assert_eq!(stats.throughput_mbps(), 5.0);
    }
}
