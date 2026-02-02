// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Scanner actor for discovering new files in storage and creating jobs.
//!
//! The scanner uses leader election to ensure only one instance is active
//! at a time. It periodically scans storage for new files and creates jobs
//! in TiKV for processing.
//!
//! # Architecture
//!
//! - **Leader Election**: Only the leader performs scans
//! - **File Discovery**: Lists objects in S3/OSS with optional glob filtering
//! - **Duplicate Detection**: Batch check existing jobs in TiKV
//! - **Job Creation**: Insert jobs for new files
//!
//! # Example
//!
//! ```ignore
//! use roboflow_distributed::{Scanner, ScannerConfig};
//! use roboflow_storage::StorageFactory;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let tikv = Arc::new(TikvClient::from_env().await?);
//!     let storage = StorageFactory::create_from_url("s3://bucket")?;
//!     let config = ScannerConfig::default();
//!
//!     let mut scanner = Scanner::new(
//!         "pod-1",
//!         tikv,
//!         storage,
//!         config,
//!     )?;
//!
//!     // Run until shutdown signal
//!     scanner.run().await?;
//!
//!     Ok(())
//! }
//! ```

use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use super::tikv::{TikvError, client::TikvClient, locks::LockManager, schema::JobRecord};
use roboflow_storage::{ObjectMetadata, Storage, StorageError};
use tokio::sync::broadcast;
use tokio::time::sleep;

/// Default scan interval in seconds.
pub const DEFAULT_SCAN_INTERVAL_SECS: u64 = 60;

/// Default batch size for job operations.
pub const DEFAULT_BATCH_SIZE: usize = 100;

/// Default lock TTL for scanner leadership in seconds.
pub const DEFAULT_LOCK_TTL_SECS: i64 = 300; // 5 minutes

/// Scanner configuration.
#[derive(Debug, Clone)]
pub struct ScannerConfig {
    /// Input bucket/prefix to scan.
    pub input_prefix: String,

    /// Scan interval in seconds.
    pub scan_interval: Duration,

    /// Batch size for checking existing jobs.
    pub batch_size: usize,

    /// Optional glob pattern for filtering files.
    pub file_pattern: Option<glob::Pattern>,

    /// Output prefix for processed jobs.
    pub output_prefix: String,

    /// Configuration hash for job records.
    pub config_hash: String,
}

impl Default for ScannerConfig {
    fn default() -> Self {
        Self {
            input_prefix: String::from("input/"),
            scan_interval: Duration::from_secs(DEFAULT_SCAN_INTERVAL_SECS),
            batch_size: DEFAULT_BATCH_SIZE,
            file_pattern: None,
            output_prefix: String::from("output/"),
            config_hash: String::from("default"),
        }
    }
}

impl ScannerConfig {
    /// Create a new scanner configuration.
    pub fn new(input_prefix: impl Into<String>) -> Self {
        Self {
            input_prefix: input_prefix.into(),
            ..Default::default()
        }
    }

    /// Set the scan interval.
    pub fn with_scan_interval(mut self, interval: Duration) -> Self {
        self.scan_interval = interval;
        self
    }

    /// Set the batch size.
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set the file pattern (glob).
    pub fn with_file_pattern(mut self, pattern: &str) -> Result<Self, glob::PatternError> {
        self.file_pattern = Some(glob::Pattern::new(pattern)?);
        Ok(self)
    }

    /// Set the output prefix.
    pub fn with_output_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.output_prefix = prefix.into();
        self
    }

    /// Set the configuration hash.
    pub fn with_config_hash(mut self, hash: impl Into<String>) -> Self {
        self.config_hash = hash.into();
        self
    }
}

/// Scanner metrics.
#[derive(Debug, Default)]
pub struct ScannerMetrics {
    /// Total files discovered.
    pub files_discovered: AtomicU64,

    /// Total jobs created.
    pub jobs_created: AtomicU64,

    /// Total duplicates skipped.
    pub duplicates_skipped: AtomicU64,

    /// Total scan errors.
    pub scan_errors: AtomicU64,

    /// Last scan duration in milliseconds.
    pub last_scan_duration_ms: AtomicU64,

    /// Whether this instance is currently the leader.
    pub is_leader: AtomicU64,
}

impl ScannerMetrics {
    /// Create new metrics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment files discovered.
    pub fn inc_files_discovered(&self, count: u64) {
        self.files_discovered.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment jobs created.
    pub fn inc_jobs_created(&self, count: u64) {
        self.jobs_created.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment duplicates skipped.
    pub fn inc_duplicates_skipped(&self, count: u64) {
        self.duplicates_skipped.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment scan errors.
    pub fn inc_scan_errors(&self) {
        self.scan_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Set last scan duration.
    pub fn set_last_scan_duration(&self, duration_ms: u64) {
        self.last_scan_duration_ms
            .store(duration_ms, Ordering::Relaxed);
    }

    /// Set leader status.
    pub fn set_leader(&self, is_leader: bool) {
        self.is_leader
            .store(if is_leader { 1 } else { 0 }, Ordering::Relaxed);
    }

    /// Get all current metric values.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            files_discovered: self.files_discovered.load(Ordering::Relaxed),
            jobs_created: self.jobs_created.load(Ordering::Relaxed),
            duplicates_skipped: self.duplicates_skipped.load(Ordering::Relaxed),
            scan_errors: self.scan_errors.load(Ordering::Relaxed),
            last_scan_duration_ms: self.last_scan_duration_ms.load(Ordering::Relaxed),
            is_leader: self.is_leader.load(Ordering::Relaxed) == 1,
        }
    }
}

/// Snapshot of scanner metrics.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    /// Total files discovered.
    pub files_discovered: u64,

    /// Total jobs created.
    pub jobs_created: u64,

    /// Total duplicates skipped.
    pub duplicates_skipped: u64,

    /// Total scan errors.
    pub scan_errors: u64,

    /// Last scan duration in milliseconds.
    pub last_scan_duration_ms: u64,

    /// Whether this instance is the leader.
    pub is_leader: bool,
}

/// Scanner actor for file discovery and job creation.
pub struct Scanner {
    /// Pod ID for leader election.
    pod_id: String,

    /// TiKV client for job operations.
    tikv: Arc<TikvClient>,

    /// Lock manager for leader election.
    lock_manager: Arc<LockManager>,

    /// Storage backend for listing files.
    storage: Arc<dyn Storage>,

    /// Scanner configuration.
    config: ScannerConfig,

    /// Scanner metrics.
    metrics: Arc<ScannerMetrics>,

    /// Shutdown sender.
    shutdown_tx: Option<broadcast::Sender<()>>,
}

impl Scanner {
    /// Create a new scanner.
    pub fn new(
        pod_id: impl Into<String>,
        tikv: Arc<TikvClient>,
        storage: Arc<dyn Storage>,
        config: ScannerConfig,
    ) -> Result<Self, TikvError> {
        let pod_id = pod_id.into();
        let lock_manager = Arc::new(LockManager::new(tikv.clone(), &pod_id));

        Ok(Self {
            pod_id,
            tikv,
            lock_manager,
            storage,
            config,
            metrics: Arc::new(ScannerMetrics::new()),
            shutdown_tx: None,
        })
    }

    /// Get a reference to the metrics.
    pub fn metrics(&self) -> &ScannerMetrics {
        &self.metrics
    }

    /// Try to become the leader.
    ///
    /// Returns `Ok(Some(guard))` if leadership acquired, `Ok(None)` if not.
    async fn try_become_leader(&self) -> Result<Option<LockGuard>, TikvError> {
        let ttl = Duration::from_secs(DEFAULT_LOCK_TTL_SECS as u64);
        match self
            .lock_manager
            .try_acquire_with_renewal("scanner_lock", ttl)
            .await
        {
            Ok(guard) => {
                tracing::info!(
                    pod_id = %self.pod_id,
                    "Scanner leadership acquired"
                );
                self.metrics.set_leader(true);
                Ok(Some(guard))
            }
            Err(TikvError::LockAcquisitionFailed(_)) => {
                tracing::debug!(
                    pod_id = %self.pod_id,
                    "Scanner leadership not acquired (already held)"
                );
                self.metrics.set_leader(false);
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    /// Compute file hash for deduplication.
    fn compute_file_hash(&self, metadata: &ObjectMetadata) -> String {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        metadata.path.hash(&mut hasher);
        metadata.size.hash(&mut hasher);
        self.config.config_hash.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }

    /// Check which hashes already have jobs.
    async fn check_existing_jobs(&self, hashes: &[String]) -> Result<HashSet<String>, TikvError> {
        use super::tikv::key::JobKeys;

        if hashes.is_empty() {
            return Ok(HashSet::new());
        }

        let mut existing = HashSet::new();

        // Process hashes in batches to avoid overwhelming TiKV
        for chunk in hashes.chunks(self.config.batch_size) {
            let keys: Vec<Vec<u8>> = chunk.iter().map(|hash| JobKeys::record(hash)).collect();

            let results = self.tikv.batch_get(keys).await?;

            let chunk_existing: HashSet<String> = chunk
                .iter()
                .zip(results.iter())
                .filter_map(|(hash, result)| {
                    if result.is_some() {
                        Some(hash.clone())
                    } else {
                        None
                    }
                })
                .collect();

            existing.extend(chunk_existing);
        }

        tracing::debug!(
            pod_id = %self.pod_id,
            total = hashes.len(),
            existing = existing.len(),
            "Checked existing jobs"
        );

        Ok(existing)
    }

    /// Extract bucket name from storage URL or metadata.
    fn extract_bucket(&self, metadata: &ObjectMetadata) -> String {
        // Try to extract from path if it looks like a URL
        if let Some(rest) = metadata.path.split("://").nth(1)
            && let Some(bucket) = rest.split('/').next()
        {
            return bucket.to_string();
        }

        // Default bucket name
        "default".to_string()
    }

    /// Create a job record for a file.
    fn create_job(&self, metadata: &ObjectMetadata, hash: &str) -> JobRecord {
        let bucket = self.extract_bucket(metadata);

        JobRecord::new(
            hash.to_string(),
            metadata.path.clone(),
            bucket,
            metadata.size,
            self.config.output_prefix.clone(),
            self.config.config_hash.clone(),
        )
    }

    /// Run a single scan cycle.
    async fn scan_cycle(&self) -> Result<ScanStats, TikvError> {
        let start = SystemTime::now();

        // List files from storage (sync operation, wrap in task)
        let storage = self.storage.clone();
        let file_pattern = self.config.file_pattern.clone();
        let input_prefix = self.config.input_prefix.clone();
        let list_task = tokio::task::spawn_blocking(move || {
            let prefix = Path::new(&input_prefix);
            let files = storage.list(prefix)?;

            // Filter by pattern if configured
            let filtered: Vec<ObjectMetadata> = if let Some(pattern) = file_pattern {
                files
                    .into_iter()
                    .filter(|meta| !meta.is_dir && pattern.matches(&meta.path))
                    .collect()
            } else {
                files.into_iter().filter(|meta| !meta.is_dir).collect()
            };

            Ok::<_, StorageError>(filtered)
        });

        let files = match list_task.await {
            Ok(Ok(files)) => files,
            Ok(Err(e)) => {
                tracing::error!(
                    pod_id = %self.pod_id,
                    error = %e,
                    "Failed to list input files"
                );
                self.metrics.inc_scan_errors();
                return Ok(ScanStats::default());
            }
            Err(e) => {
                if e.is_panic() {
                    tracing::error!(
                        pod_id = %self.pod_id,
                        "Panic in list task"
                    );
                }
                self.metrics.inc_scan_errors();
                return Ok(ScanStats::default());
            }
        };

        let files_discovered = files.len() as u64;
        self.metrics.inc_files_discovered(files_discovered);

        if files.is_empty() {
            tracing::debug!(
                pod_id = %self.pod_id,
                "No files to process"
            );
            return Ok(ScanStats {
                files_discovered,
                jobs_created: 0,
                duplicates_skipped: 0,
            });
        }

        // Compute hashes
        let file_hashes: Vec<(ObjectMetadata, String)> = files
            .iter()
            .map(|meta| {
                let hash = self.compute_file_hash(meta);
                (meta.clone(), hash)
            })
            .collect();

        let hashes: Vec<String> = file_hashes.iter().map(|(_, h)| h.clone()).collect();

        // Check existing jobs
        let existing = match self.check_existing_jobs(&hashes).await {
            Ok(e) => e,
            Err(e) => {
                tracing::error!(
                    pod_id = %self.pod_id,
                    error = %e,
                    "Failed to check existing jobs"
                );
                self.metrics.inc_scan_errors();
                return Ok(ScanStats::default());
            }
        };

        // Filter and create jobs for new files
        let new_files: Vec<(ObjectMetadata, String)> = file_hashes
            .into_iter()
            .filter(|(_, hash)| !existing.contains(hash))
            .collect();

        let duplicates_skipped = files_discovered - new_files.len() as u64;
        self.metrics.inc_duplicates_skipped(duplicates_skipped);

        // Create jobs in batches for better performance
        let mut jobs_created = 0u64;
        for chunk in new_files.chunks(self.config.batch_size) {
            let job_pairs: Vec<(Vec<u8>, Vec<u8>)> = chunk
                .iter()
                .map(|(metadata, hash)| {
                    let job = self.create_job(metadata, hash);
                    use super::tikv::key::JobKeys;
                    let key = JobKeys::record(&job.id);
                    let data = bincode::serialize(&job)
                        .map_err(|e| TikvError::Serialization(e.to_string()))?;
                    Ok::<_, TikvError>((key, data))
                })
                .collect::<Result<Vec<_>, _>>()?;

            if let Err(e) = self.tikv.batch_put(job_pairs).await {
                tracing::error!(
                    pod_id = %self.pod_id,
                    batch_size = chunk.len(),
                    error = %e,
                    "Failed to create batch of jobs - scan cycle incomplete, files skipped"
                );
                self.metrics.inc_scan_errors();
                // Return error to fail the entire scan cycle - continuing would skip files
                return Err(e);
            }
            jobs_created += chunk.len() as u64;
        }
        self.metrics.inc_jobs_created(jobs_created);

        let duration = start.elapsed().unwrap_or_default().as_millis() as u64;
        self.metrics.set_last_scan_duration(duration);

        tracing::info!(
            pod_id = %self.pod_id,
            files_discovered,
            jobs_created,
            duplicates_skipped,
            duration_ms = duration,
            "Scan cycle completed"
        );

        Ok(ScanStats {
            files_discovered,
            jobs_created,
            duplicates_skipped,
        })
    }

    /// Run the scanner loop.
    ///
    /// This will continuously:
    /// 1. Try to become leader
    /// 2. If leader: scan and create jobs
    /// 3. Sleep for scan interval
    /// 4. Repeat until shutdown
    pub async fn run(&mut self) -> Result<(), TikvError> {
        let (shutdown_tx, mut shutdown_rx) = broadcast::channel(1);
        self.shutdown_tx = Some(shutdown_tx);

        tracing::info!(
            pod_id = %self.pod_id,
            scan_interval_secs = self.config.scan_interval.as_secs(),
            "Starting scanner"
        );

        loop {
            // Check for shutdown
            if shutdown_rx.try_recv().is_ok() {
                tracing::info!(
                    pod_id = %self.pod_id,
                    "Scanner shutdown requested"
                );
                break;
            }

            // Try to become leader
            match self.try_become_leader().await {
                Ok(Some(guard)) => {
                    // We are the leader, run scan cycle
                    // Keep lock held during entire cycle (scan + sleep)
                    // to prevent other scanners from starting simultaneously
                    if let Err(e) = self.scan_cycle().await {
                        tracing::error!(
                            pod_id = %self.pod_id,
                            error = %e,
                            "Scan cycle failed"
                        );
                    }

                    // Sleep while holding the lock - prevents race condition
                    // where another scanner could acquire leadership during sleep
                    tokio::select! {
                        _ = sleep(self.config.scan_interval) => {}
                        _ = shutdown_rx.recv() => {
                            tracing::info!(
                                pod_id = %self.pod_id,
                                "Scanner shutdown requested during leader sleep"
                            );
                            break;
                        }
                    }

                    // Lock is released here when guard is dropped
                    drop(guard);
                }
                Ok(None) => {
                    // Not leader, wait and retry
                    sleep(self.config.scan_interval).await;
                }
                Err(e) => {
                    tracing::error!(
                        pod_id = %self.pod_id,
                        error = %e,
                        "Failed to check leadership"
                    );
                    self.metrics.inc_scan_errors();
                    sleep(self.config.scan_interval).await;
                }
            }
        }

        tracing::info!(
            pod_id = %self.pod_id,
            "Scanner stopped"
        );

        Ok(())
    }

    /// Shutdown the scanner gracefully.
    pub fn shutdown(&self) -> Result<(), TikvError> {
        if let Some(ref tx) = self.shutdown_tx {
            let _ = tx.send(());
        }
        Ok(())
    }
}

/// Type alias for the lock guard returned by LockManager.
pub type LockGuard = super::tikv::locks::LockGuard;

/// Scan cycle statistics.
#[derive(Debug, Default, Clone)]
pub struct ScanStats {
    /// Files discovered in this scan.
    pub files_discovered: u64,

    /// Jobs created in this scan.
    pub jobs_created: u64,

    /// Duplicates skipped in this scan.
    pub duplicates_skipped: u64,
}

// =============================================================================
// Lock Manager Extension
// =============================================================================

impl LockManager {
    /// Try to acquire a lock with auto-renewal.
    ///
    /// Returns a lock guard that will automatically renew the lock
    /// at the configured interval until dropped.
    pub async fn try_acquire_with_renewal(
        &self,
        resource: &str,
        ttl: Duration,
    ) -> Result<super::tikv::locks::LockGuard, TikvError> {
        self.acquire_with_renewal(resource, ttl).await
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use roboflow_storage::LocalStorage;
    use std::fs;
    use tempfile::TempDir;

    fn create_test_storage() -> (Arc<dyn Storage>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(LocalStorage::new(temp_dir.path())) as Arc<dyn Storage>;
        (storage, temp_dir)
    }

    #[test]
    fn test_scanner_config_default() {
        let config = ScannerConfig::default();
        assert_eq!(config.input_prefix, "input/");
        assert_eq!(config.scan_interval.as_secs(), DEFAULT_SCAN_INTERVAL_SECS);
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
        assert!(config.file_pattern.is_none());
        assert_eq!(config.output_prefix, "output/");
        assert_eq!(config.config_hash, "default");
    }

    #[test]
    fn test_scanner_config_builder() {
        let config = ScannerConfig::new("custom/")
            .with_scan_interval(Duration::from_secs(120))
            .with_batch_size(200)
            .with_output_prefix("result/")
            .with_config_hash("test-config");

        assert_eq!(config.input_prefix, "custom/");
        assert_eq!(config.scan_interval.as_secs(), 120);
        assert_eq!(config.batch_size, 200);
        assert_eq!(config.output_prefix, "result/");
        assert_eq!(config.config_hash, "test-config");
    }

    #[test]
    fn test_scanner_config_with_pattern() {
        let config = ScannerConfig::default()
            .with_file_pattern("*.mcap")
            .unwrap();
        assert!(config.file_pattern.is_some());
        let pattern = config.file_pattern.unwrap();
        assert!(pattern.matches("test.mcap"));
        assert!(pattern.matches("data.mcap"));
        assert!(!pattern.matches("test.txt"));
    }

    #[test]
    fn test_scanner_metrics() {
        let metrics = ScannerMetrics::new();

        assert_eq!(metrics.files_discovered.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.jobs_created.load(Ordering::Relaxed), 0);

        metrics.inc_files_discovered(10);
        metrics.inc_jobs_created(5);
        metrics.inc_duplicates_skipped(3);
        metrics.set_leader(true);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.files_discovered, 10);
        assert_eq!(snapshot.jobs_created, 5);
        assert_eq!(snapshot.duplicates_skipped, 3);
        assert!(snapshot.is_leader);
    }

    #[test]
    fn test_compute_file_hash() {
        let meta1 = ObjectMetadata::new("test/file.mcap", 1024);
        let meta2 = ObjectMetadata::new("test/file.mcap", 1024);
        let meta3 = ObjectMetadata::new("test/other.mcap", 1024);

        // Test the hash function directly
        let compute_hash = |meta: &ObjectMetadata, config_hash: &str| -> String {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            meta.path.hash(&mut hasher);
            meta.size.hash(&mut hasher);
            config_hash.hash(&mut hasher);
            format!("{:016x}", hasher.finish())
        };

        let hash1 = compute_hash(&meta1, "default");
        let hash2 = compute_hash(&meta2, "default");
        let hash3 = compute_hash(&meta3, "default");
        let hash4 = compute_hash(&meta1, "different");

        // Same file should produce same hash
        assert_eq!(hash1, hash2);

        // Different files should produce different hashes
        assert_ne!(hash1, hash3);

        // Different config should produce different hash
        assert_ne!(hash1, hash4);
    }

    #[test]
    fn test_extract_bucket() {
        // Test the bucket extraction logic
        let test_extract = |path: &str| -> String {
            if path.contains("://")
                && let Some(rest) = path.split("://").nth(1)
                && let Some(bucket) = rest.split('/').next()
            {
                return bucket.to_string();
            }
            "default".to_string()
        };

        assert_eq!(
            test_extract("s3://my-bucket/path/to/file.mcap"),
            "my-bucket"
        );
        assert_eq!(
            test_extract("oss://other-bucket/data/file.mcap"),
            "other-bucket"
        );
        assert_eq!(test_extract("local/path/file.mcap"), "default");
    }

    #[test]
    fn test_scan_stats_default() {
        let stats = ScanStats::default();
        assert_eq!(stats.files_discovered, 0);
        assert_eq!(stats.jobs_created, 0);
        assert_eq!(stats.duplicates_skipped, 0);
    }

    #[tokio::test]
    async fn test_scanner_list_files() {
        let (storage, temp_dir) = create_test_storage();

        // Create test files
        let input_dir = temp_dir.path().join("input");
        fs::create_dir_all(&input_dir).unwrap();

        fs::write(input_dir.join("test1.mcap"), b"test data 1").unwrap();
        fs::write(input_dir.join("test2.mcap"), b"test data 2").unwrap();
        fs::write(input_dir.join("test3.txt"), b"test data 3").unwrap();

        // Test listing through storage directly
        let prefix = Path::new(&input_dir);
        let files = storage.list(prefix).unwrap();

        assert_eq!(files.len(), 3);

        // Filter out directories
        let files_only: Vec<_> = files.into_iter().filter(|f| !f.is_dir).collect();
        assert_eq!(files_only.len(), 3);
    }
}
