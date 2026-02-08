// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Scanner actor for processing batch jobs and creating work units.
//!
//! The scanner uses leader election to ensure only one instance is active
//! at a time. It queries TiKV for pending batch jobs, scans the source URLs
//! specified in each batch, and creates work units for discovered files.
//!
//! # Architecture
//!
//! - **Leader Election**: Only the leader performs scans
//! - **Batch Processing**: Queries Pending batches from TiKV
//! - **File Discovery**: For each batch, scans the source URLs in S3/OSS
//! - **Work Unit Creation**: Creates work units for discovered files, updates batch status
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

use super::batch::{
    BatchIndexKeys, BatchKeys, BatchPhase, BatchSpec, BatchStatus, DiscoveryStatus, WorkFile,
    WorkUnit, WorkUnitKeys,
};
use super::tikv::{TikvError, client::TikvClient, locks::LockManager};
use roboflow_storage::{ObjectMetadata, StorageError, StorageFactory};
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
    /// Batch namespace to query batches from TiKV.
    pub batch_namespace: String,

    /// Scan interval in seconds.
    pub scan_interval: Duration,

    /// Batch size for checking existing jobs.
    pub batch_size: usize,

    /// Maximum number of batches to process per scan cycle.
    pub max_batches_per_cycle: usize,
}

impl Default for ScannerConfig {
    fn default() -> Self {
        Self {
            batch_namespace: String::from("jobs"),
            scan_interval: Duration::from_secs(DEFAULT_SCAN_INTERVAL_SECS),
            batch_size: DEFAULT_BATCH_SIZE,
            max_batches_per_cycle: 10,
        }
    }
}

impl ScannerConfig {
    /// Create a new scanner configuration.
    pub fn new(batch_namespace: impl Into<String>) -> Self {
        Self {
            batch_namespace: batch_namespace.into(),
            ..Default::default()
        }
    }

    /// Create scanner configuration from environment variables.
    ///
    /// - `SCANNER_BATCH_NAMESPACE`: Batch namespace to scan (default: "jobs")
    /// - `SCANNER_SCAN_INTERVAL_SECS`: Scan interval in seconds (default: 60)
    /// - `SCANNER_BATCH_SIZE`: Batch size for job operations (default: 100)
    /// - `SCANNER_MAX_BATCHES_PER_CYCLE`: Max batches to process per cycle (default: 10)
    pub fn from_env() -> Result<Self, String> {
        use std::env;

        let batch_namespace =
            env::var("SCANNER_BATCH_NAMESPACE").unwrap_or_else(|_| String::from("jobs"));

        let scan_interval = env::var("SCANNER_SCAN_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_SCAN_INTERVAL_SECS);

        let batch_size = env::var("SCANNER_BATCH_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_BATCH_SIZE);

        let max_batches_per_cycle = env::var("SCANNER_MAX_BATCHES_PER_CYCLE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10);

        Ok(Self {
            batch_namespace,
            scan_interval: Duration::from_secs(scan_interval),
            batch_size,
            max_batches_per_cycle,
        })
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

    /// Set the max batches per cycle.
    pub fn with_max_batches_per_cycle(mut self, max: usize) -> Self {
        self.max_batches_per_cycle = max;
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

/// Scanner actor for batch processing and job creation.
pub struct Scanner {
    /// Pod ID for leader election.
    pod_id: String,

    /// TiKV client for job operations.
    tikv: Arc<TikvClient>,

    /// Lock manager for leader election.
    lock_manager: Arc<LockManager>,

    /// Storage factory for creating storage backends.
    storage_factory: Arc<StorageFactory>,

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
        storage_factory: Arc<StorageFactory>,
        config: ScannerConfig,
    ) -> Result<Self, TikvError> {
        let pod_id = pod_id.into();
        let lock_manager = Arc::new(LockManager::new(tikv.clone(), &pod_id));

        Ok(Self {
            pod_id,
            tikv,
            lock_manager,
            storage_factory,
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
        // Use a TTL longer than the scan interval to prevent lock expiration during scan
        let scan_interval_secs = self.config.scan_interval.as_secs() as i64;
        let ttl = Duration::from_secs((scan_interval_secs * 3).max(60) as u64);
        match self.lock_manager.try_acquire("scanner_lock", ttl).await {
            Ok(Some(guard)) => {
                tracing::info!(
                    pod_id = %self.pod_id,
                    "Scanner leadership acquired"
                );
                self.metrics.set_leader(true);
                Ok(Some(guard))
            }
            Ok(None) => {
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
    fn compute_file_hash(&self, metadata: &ObjectMetadata, config_hash: &str) -> String {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        metadata.path.hash(&mut hasher);
        metadata.size.hash(&mut hasher);
        config_hash.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }

    /// Check which hashes already have work units for this batch.
    ///
    /// Checks actual work unit keys (`/roboflow/v1/batch/workunits/{batch_id}/{unit_id}`)
    /// to determine if a work unit was already created for this file in this batch.
    async fn check_existing_work_units(
        &self,
        batch_id: &str,
        hashes: &[String],
    ) -> Result<HashSet<String>, TikvError> {
        if hashes.is_empty() {
            return Ok(HashSet::new());
        }

        let mut existing = HashSet::new();

        // Check work unit keys scoped to this batch
        for chunk in hashes.chunks(self.config.batch_size) {
            let keys: Vec<Vec<u8>> = chunk
                .iter()
                .map(|hash| WorkUnitKeys::unit(batch_id, hash))
                .collect();

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
            "Checked existing work units"
        );

        Ok(existing)
    }

    /// Create a work unit for a file.
    fn create_work_unit(
        &self,
        batch_id: &str,
        source_url: &str,
        metadata: &ObjectMetadata,
        hash: &str,
        output_prefix: &str,
        config_hash: &str,
    ) -> WorkUnit {
        let work_file = WorkFile::new(source_url.to_string(), metadata.size);

        WorkUnit::with_id(
            hash.to_string(), // Use hash as ID (like legacy Job system)
            batch_id.to_string(),
            vec![work_file],
            output_prefix.to_string(),
            config_hash.to_string(),
        )
    }

    /// Get pending batches from TiKV using the phase index.
    ///
    /// Scans the phase index for Pending and Discovering batches instead of
    /// scanning all specs. This is O(active batches) instead of O(total batches),
    /// which is critical for long-running clusters where batch records accumulate.
    ///
    /// The scan uses a generous limit (1000) because index entries are tiny
    /// (empty values) and there may be stale entries from before the index was
    /// maintained. The actual number of batches returned is capped by
    /// `max_batches_per_cycle`.
    async fn get_pending_batches(
        &self,
    ) -> Result<Vec<(String, BatchSpec, BatchStatus)>, TikvError> {
        let mut batches = Vec::new();

        // Scan phase index for Pending and Discovering batches only.
        // Use a generous scan limit since index entries are tiny and stale
        // entries need to be skipped. max_batches_per_cycle limits the
        // number of batches we actually process.
        const INDEX_SCAN_LIMIT: u32 = 1000;

        for phase in [BatchPhase::Pending, BatchPhase::Discovering] {
            let prefix = BatchIndexKeys::phase_prefix(phase);
            let results = self.tikv.scan(prefix, INDEX_SCAN_LIMIT).await?;

            for (key, _value) in results {
                let key_str = String::from_utf8_lossy(&key);
                // Key format: /roboflow/v1/batch/index/phase/{phase}/{batch_id}
                let batch_id = match key_str.split('/').next_back() {
                    Some(id) => id.to_string(),
                    None => continue,
                };

                // Get batch spec
                let spec_key = BatchKeys::spec(&batch_id);
                let spec_data = match self.tikv.get(spec_key).await? {
                    Some(d) => d,
                    None => {
                        tracing::warn!(batch_id = %batch_id, "Spec not found for indexed batch - stale index entry");
                        continue;
                    }
                };
                let spec: BatchSpec = match serde_yaml::from_slice(&spec_data) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(batch_id = %batch_id, error = %e, "Failed to deserialize batch spec");
                        continue;
                    }
                };

                // Skip if not in our namespace
                if spec.metadata.namespace != self.config.batch_namespace {
                    continue;
                }

                // Get batch status and verify phase (stale index tolerance)
                let status_key = BatchKeys::status(&batch_id);
                let status: BatchStatus = match self.tikv.get(status_key).await? {
                    Some(d) => bincode::deserialize(&d).unwrap_or_default(),
                    None => BatchStatus::new(),
                };

                // Verify actual phase matches — index may be stale
                if matches!(status.phase, BatchPhase::Pending | BatchPhase::Discovering) {
                    batches.push((batch_id, spec, status));
                } else {
                    // Clean up stale index entry
                    let stale_key = BatchIndexKeys::phase(phase, &batch_id);
                    if let Err(e) = self.tikv.delete(stale_key).await {
                        tracing::warn!(
                            batch_id = %batch_id,
                            indexed_phase = ?phase,
                            actual_phase = ?status.phase,
                            error = %e,
                            "Failed to clean up stale phase index entry"
                        );
                    } else {
                        tracing::debug!(
                            batch_id = %batch_id,
                            indexed_phase = ?phase,
                            actual_phase = ?status.phase,
                            "Cleaned up stale phase index entry"
                        );
                    }
                }
            }

            if batches.len() >= self.config.max_batches_per_cycle {
                break;
            }
        }

        Ok(batches)
    }

    /// Scan a source URL and return discovered files.
    ///
    /// Handles two cases:
    /// 1. Direct file URLs (e.g., s3://bucket/file.mcap) - returns that single file
    /// 2. Directory/prefix URLs (e.g., s3://bucket/path/) - lists all files in that directory
    async fn scan_source_url(&self, source_url: &str) -> Result<Vec<ObjectMetadata>, StorageError> {
        tracing::info!(
            source_url = %source_url,
            "Scanning source URL"
        );

        // Create storage backend from URL
        let storage = self.storage_factory.create(source_url)?;

        // Extract the path after the bucket name.
        // URLs are like: s3://bucket/path/to/file or oss://bucket/file.mcap
        // The format is: scheme://bucket/path
        // We need to skip past :// to find the slash after the bucket name.
        let path_for_list = if let Some(protocol_end) = source_url.find("://") {
            // Start looking after :// (3 characters)
            let after_protocol = &source_url[protocol_end + 3..];
            if let Some(slash_after_bucket) = after_protocol.find('/') {
                // Return everything after the slash following the bucket
                &source_url[protocol_end + 3 + slash_after_bucket + 1..]
            } else {
                // No path after bucket, return empty to list entire bucket
                ""
            }
        } else {
            // No protocol found, use URL as-is (shouldn't happen with valid URLs)
            source_url
        };

        tracing::info!(
            source_url = %source_url,
            path_for_list = %path_for_list,
            "Extracted path for listing"
        );

        let path = Path::new(path_for_list);

        // First try to list the path (directory case)
        let files = storage.list(path)?;

        tracing::info!(
            source_url = %source_url,
            files_found = files.len(),
            "List operation completed"
        );

        // Filter out directories
        let files: Vec<_> = files.into_iter().filter(|m| !m.is_dir).collect();

        // If listing returned files, return them
        if !files.is_empty() {
            return Ok(files);
        }

        // If listing was empty, check if the path itself is a file
        // This handles direct file URLs like s3://bucket/file.mcap
        match storage.metadata(path) {
            Ok(metadata) => {
                // It's a file, return it as a single-element list
                tracing::info!(
                    source_url = %source_url,
                    file_path = %metadata.path,
                    file_size = %metadata.size,
                    "Found direct file via metadata"
                );
                Ok(vec![metadata])
            }
            Err(StorageError::NotFound(_)) => {
                // Neither a directory with files nor a file - return empty
                tracing::warn!(
                    source_url = %source_url,
                    path = %path_for_list,
                    "No files found - neither directory nor file"
                );
                Ok(vec![])
            }
            Err(e) => {
                tracing::error!(
                    source_url = %source_url,
                    error = %e,
                    "Failed to get metadata"
                );
                Err(e)
            }
        }
    }

    /// Process a batch: scan sources, create jobs, update status.
    async fn process_batch(
        &self,
        batch_id: &str,
        spec: &BatchSpec,
        mut status: BatchStatus,
    ) -> Result<ScanStats, TikvError> {
        let mut files_discovered = 0u64;
        let mut jobs_created = 0u64;
        let mut duplicates_skipped = 0u64;

        // Skip if already completed discovery
        if status.phase == BatchPhase::Running {
            return Ok(ScanStats::default());
        }

        // Transition to Discovering if in Pending
        if status.phase == BatchPhase::Pending {
            status.transition_to(BatchPhase::Discovering);
            // Initialize discovery status
            let total_sources = spec.spec.sources.len() as u32;
            status.discovery_status = Some(DiscoveryStatus::new(total_sources));
            // Save status immediately after transition to ensure progress is visible
            self.save_batch_status(batch_id, &status).await?;
            // Update phase index: Pending -> Discovering
            super::batch::update_phase_index(
                &self.tikv,
                batch_id,
                BatchPhase::Pending,
                BatchPhase::Discovering,
            )
            .await?;
        }

        // Track which sources we've already processed
        let sources_processed = status
            .discovery_status
            .as_ref()
            .map(|d| d.sources_scanned as usize)
            .unwrap_or(0);

        tracing::info!(
            batch_id = %batch_id,
            sources_total = spec.spec.sources.len(),
            sources_processed = sources_processed,
            phase = ?status.phase,
            has_discovery_status = status.discovery_status.is_some(),
            "process_batch: starting source iteration"
        );

        // Process each source that hasn't been processed yet
        for source in spec.spec.sources.iter().skip(sources_processed) {
            let source_url = &source.url;

            tracing::debug!(
                batch_id = %batch_id,
                source_url = %source_url,
                "Scanning source"
            );

            // Scan the source URL
            let files = match self.scan_source_url(source_url).await {
                Ok(f) => f,
                Err(e) => {
                    tracing::error!(
                        batch_id = %batch_id,
                        source_url = %source_url,
                        error = %e,
                        "Failed to scan source URL"
                    );
                    self.metrics.inc_scan_errors();
                    continue;
                }
            };

            let source_files_count = files.len() as u64;
            files_discovered += source_files_count;

            // Compute hashes and check for existing jobs
            let config_hash = &spec.spec.config;
            let output_prefix = &spec.spec.output;

            let file_hashes: Vec<(ObjectMetadata, String)> = files
                .iter()
                .map(|meta| {
                    let hash = self.compute_file_hash(meta, config_hash);
                    (meta.clone(), hash)
                })
                .collect();

            let hashes: Vec<String> = file_hashes.iter().map(|(_, h)| h.clone()).collect();

            // Check existing work units for this batch
            let existing = match self.check_existing_work_units(batch_id, &hashes).await {
                Ok(e) => e,
                Err(e) => {
                    tracing::error!(
                        batch_id = %batch_id,
                        error = %e,
                        "Failed to check existing jobs"
                    );
                    self.metrics.inc_scan_errors();
                    continue;
                }
            };

            // Filter new files
            let new_files: Vec<(ObjectMetadata, String)> = file_hashes
                .into_iter()
                .filter(|(_, hash)| !existing.contains(hash))
                .collect();

            let source_duplicates = source_files_count - new_files.len() as u64;
            duplicates_skipped += source_duplicates;

            // Create work units for new files
            let source_work_units_created = if !new_files.is_empty() {
                let mut created = 0u64;
                for chunk in new_files.chunks(self.config.batch_size) {
                    let mut work_unit_pairs = Vec::new();
                    let mut pending_pairs = Vec::new();

                    for (metadata, hash) in chunk {
                        let work_unit = self.create_work_unit(
                            batch_id,
                            source_url,
                            metadata,
                            hash,
                            output_prefix,
                            config_hash,
                        );

                        // Store work unit
                        let unit_key = WorkUnitKeys::unit(&work_unit.batch_id, &work_unit.id);
                        let unit_data = bincode::serialize(&work_unit)
                            .map_err(|e| TikvError::Serialization(e.to_string()))?;
                        work_unit_pairs.push((unit_key, unit_data));

                        // Add to pending queue (scoped by batch_id to prevent cross-batch interference)
                        let pending_key = WorkUnitKeys::pending(batch_id, &work_unit.id);
                        let pending_data = work_unit.batch_id.as_bytes().to_vec();
                        pending_pairs.push((pending_key, pending_data));
                    }

                    // Batch put work units and pending entries together
                    let all_pairs: Vec<(Vec<u8>, Vec<u8>)> =
                        work_unit_pairs.into_iter().chain(pending_pairs.clone()).collect();

                    // Log pending keys being written
                    for (pk, _) in &pending_pairs {
                        tracing::info!(
                            batch_id = %batch_id,
                            pending_key = %String::from_utf8_lossy(pk),
                            "Writing pending queue entry"
                        );
                    }

                    if let Err(e) = self.tikv.batch_put(all_pairs).await {
                        tracing::error!(batch_id = %batch_id, error = %e, "batch_put FAILED for work units + pending");
                        tracing::error!(
                            batch_id = %batch_id,
                            error = %e,
                            "Failed to create work units"
                        );
                        self.metrics.inc_scan_errors();
                        return Err(e);
                    }

                    // Verify pending keys were written successfully
                    for (pk, _) in &pending_pairs {
                        match self.tikv.get(pk.clone()).await {
                            Ok(Some(_)) => tracing::info!(
                                batch_id = %batch_id,
                                pending_key = %String::from_utf8_lossy(pk),
                                "VERIFIED: pending key exists in TiKV"
                            ),
                            Ok(None) => tracing::error!(
                                batch_id = %batch_id,
                                pending_key = %String::from_utf8_lossy(pk),
                                "MISSING: pending key NOT found in TiKV after batch_put!"
                            ),
                            Err(e) => tracing::error!(
                                batch_id = %batch_id,
                                pending_key = %String::from_utf8_lossy(pk),
                                error = %e,
                                "ERROR: failed to verify pending key"
                            ),
                        }
                    }

                    // Also verify via scan
                    let scan_prefix = WorkUnitKeys::pending_prefix();
                    match self.tikv.scan(scan_prefix.clone(), 10).await {
                        Ok(results) => tracing::info!(
                            batch_id = %batch_id,
                            scan_prefix = %String::from_utf8_lossy(&scan_prefix),
                            results = results.len(),
                            "SCAN verification of pending prefix"
                        ),
                        Err(e) => tracing::error!(
                            batch_id = %batch_id,
                            error = %e,
                            "SCAN verification failed"
                        ),
                    }

                    created += chunk.len() as u64;
                }
                created
            } else {
                0
            };

            jobs_created += source_work_units_created;

            // Update batch status
            status.files_total += source_files_count as u32;
            if let Some(ref mut ds) = status.discovery_status {
                ds.add_files(source_files_count as u32);
                ds.increment_scanned();
            }

            // Save updated status after each source
            self.save_batch_status(batch_id, &status).await?;
        }

        // Check if all sources are processed
        let total_sources = spec.spec.sources.len() as u32;
        let processed = status
            .discovery_status
            .as_ref()
            .map(|d| d.sources_scanned)
            .unwrap_or(0);

        if processed >= total_sources {
            // Check if any work units were actually created
            if jobs_created == 0 && files_discovered == 0 {
                // No files found in any source - mark as failed rather than running
                // This prevents the batch from hanging in Running state with no work
                status.transition_to(BatchPhase::Failed);
                status.error = Some(format!(
                    "No files discovered from {} source(s)",
                    total_sources
                ));
                tracing::warn!(
                    batch_id = %batch_id,
                    sources = total_sources,
                    "No files found during discovery, marking batch as failed"
                );
                self.save_batch_status(batch_id, &status).await?;
                // Update phase index: Discovering -> Failed
                super::batch::update_phase_index(
                    &self.tikv,
                    batch_id,
                    BatchPhase::Discovering,
                    BatchPhase::Failed,
                )
                .await?;
            } else {
                // Set work_units_total so is_complete() and progress() work correctly
                status.set_work_units_total(jobs_created as u32);
                // Transition to Running - work units were created successfully
                status.transition_to(BatchPhase::Running);
                self.save_batch_status(batch_id, &status).await?;
                // Update phase index: Discovering -> Running
                super::batch::update_phase_index(
                    &self.tikv,
                    batch_id,
                    BatchPhase::Discovering,
                    BatchPhase::Running,
                )
                .await?;
            }
        }

        self.metrics.inc_files_discovered(files_discovered);
        self.metrics.inc_jobs_created(jobs_created);
        self.metrics.inc_duplicates_skipped(duplicates_skipped);

        Ok(ScanStats {
            files_discovered,
            jobs_created,
            duplicates_skipped,
        })
    }

    /// Save batch status to TiKV.
    async fn save_batch_status(
        &self,
        batch_id: &str,
        status: &BatchStatus,
    ) -> Result<(), TikvError> {
        let key = BatchKeys::status(batch_id);
        let data =
            bincode::serialize(status).map_err(|e| TikvError::Serialization(e.to_string()))?;
        self.tikv.put(key, data).await
    }

    /// Run a single scan cycle.
    ///
    /// Queries pending batches from TiKV, scans their source URLs,
    /// and creates jobs for discovered files.
    async fn scan_cycle(&self) -> Result<ScanStats, TikvError> {
        let start = SystemTime::now();

        // Get pending batches
        let batches = self.get_pending_batches().await?;

        if batches.is_empty() {
            tracing::debug!(
                pod_id = %self.pod_id,
                namespace = %self.config.batch_namespace,
                "No pending batches to process"
            );
            return Ok(ScanStats::default());
        }

        tracing::info!(
            pod_id = %self.pod_id,
            batch_count = batches.len(),
            "Processing batches"
        );

        let mut total_stats = ScanStats::default();

        // Process each batch
        for (batch_id, spec, status) in batches {
            match self.process_batch(&batch_id, &spec, status).await {
                Ok(stats) => {
                    tracing::info!(
                        pod_id = %self.pod_id,
                        batch_id = %batch_id,
                        files_discovered = stats.files_discovered,
                        jobs_created = stats.jobs_created,
                        "Batch processed"
                    );
                    total_stats.files_discovered += stats.files_discovered;
                    total_stats.jobs_created += stats.jobs_created;
                    total_stats.duplicates_skipped += stats.duplicates_skipped;
                }
                Err(e) => {
                    tracing::error!(
                        pod_id = %self.pod_id,
                        batch_id = %batch_id,
                        error = %e,
                        "Failed to process batch"
                    );
                    self.metrics.inc_scan_errors();
                }
            }
        }

        let duration = start.elapsed().unwrap_or_default().as_millis() as u64;
        self.metrics.set_last_scan_duration(duration);

        tracing::info!(
            pod_id = %self.pod_id,
            files_discovered = total_stats.files_discovered,
            jobs_created = total_stats.jobs_created,
            duplicates_skipped = total_stats.duplicates_skipped,
            duration_ms = duration,
            "Scan cycle completed"
        );

        Ok(total_stats)
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

    #[test]
    fn test_scanner_config_default() {
        let config = ScannerConfig::default();
        assert_eq!(config.batch_namespace, "jobs");
        assert_eq!(config.scan_interval.as_secs(), DEFAULT_SCAN_INTERVAL_SECS);
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(config.max_batches_per_cycle, 10);
    }

    #[test]
    fn test_scanner_config_builder() {
        let config = ScannerConfig::new("custom-namespace")
            .with_scan_interval(Duration::from_secs(120))
            .with_batch_size(200)
            .with_max_batches_per_cycle(50);

        assert_eq!(config.batch_namespace, "custom-namespace");
        assert_eq!(config.scan_interval.as_secs(), 120);
        assert_eq!(config.batch_size, 200);
        assert_eq!(config.max_batches_per_cycle, 50);
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
    fn test_scan_stats_default() {
        let stats = ScanStats::default();
        assert_eq!(stats.files_discovered, 0);
        assert_eq!(stats.jobs_created, 0);
        assert_eq!(stats.duplicates_skipped, 0);
    }

    #[test]
    fn test_extract_path_from_source_url() {
        // Helper function to extract path matching scan_source_url logic
        // URLs are like: scheme://bucket/path - we need everything after the bucket
        let extract_path = |url: &str| -> String {
            if let Some(protocol_end) = url.find("://") {
                let after_protocol = &url[protocol_end + 3..];
                if let Some(slash_after_bucket) = after_protocol.find('/') {
                    url[protocol_end + 3 + slash_after_bucket + 1..].to_string()
                } else {
                    "".to_string()
                }
            } else {
                url.to_string()
            }
        };

        // Single file in root
        assert_eq!(extract_path("s3://bucket/file.mcap"), "file.mcap");
        assert_eq!(extract_path("oss://bucket/file.bag"), "file.bag");

        // File with nested path
        assert_eq!(extract_path("s3://bucket/data/file.mcap"), "data/file.mcap");
        assert_eq!(
            extract_path("oss://bucket/data/subdir/file.bag"),
            "data/subdir/file.bag"
        );

        // Deeply nested path
        assert_eq!(
            extract_path("s3://bucket/data/2025/01/31/file.mcap"),
            "data/2025/01/31/file.mcap"
        );
        assert_eq!(
            extract_path("s3://bucket/a/b/c/d/e/file.mcap"),
            "a/b/c/d/e/file.mcap"
        );

        // Directory path
        assert_eq!(extract_path("s3://bucket/data/"), "data/");
        assert_eq!(extract_path("s3://bucket/data/subdir/"), "data/subdir/");
        assert_eq!(extract_path("s3://bucket/a/b/c/"), "a/b/c/");

        // Root (no path after bucket)
        assert_eq!(extract_path("s3://bucket/"), "");
        assert_eq!(extract_path("oss://bucket/"), "");

        // Complex filename with underscores, dates, etc.
        assert_eq!(
            extract_path("s3://roboflow-raw/Rubbish_sorting_P4-278_20250830101558.bag"),
            "Rubbish_sorting_P4-278_20250830101558.bag"
        );
    }
}
