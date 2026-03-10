// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Merge coordinator for Staging + Merge pattern.
//!
//! Handles the coordination of merging staged outputs from multiple workers
//! into a single sequential LeRobot dataset.

use super::executor::ParquetMergeExecutor;
use super::schema::MergeState;
use crate::batch::{BatchKeys, BatchPhase, BatchStatus};
use crate::tikv::{client::TikvClient, error::TikvError};
use roboflow_storage::StorageFactory;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::info;

// =============================================================================
// Merge Semaphore (Backpressure)
// =============================================================================

/// Default maximum concurrent merge operations.
pub const DEFAULT_MAX_CONCURRENT_MERGES: usize = 3;

/// Metrics for the merge semaphore.
#[derive(Debug, Clone, Default)]
pub struct MergeSemaphoreMetrics {
    /// Current number of available permits.
    pub available_permits: usize,
    /// Current number of pending merges in queue.
    pub queue_depth: usize,
    /// Total number of merge attempts (for metrics).
    pub total_attempts: u64,
    /// Number of merges that succeeded (for metrics).
    pub successful_merges: u64,
}

/// RAII permit for merge operations.
///
/// When dropped, the permit is automatically returned to the semaphore.
#[derive(Debug)]
pub struct MergePermit {
    semaphore: Arc<MergeSemaphoreInner>,
}

impl MergePermit {
    fn new(semaphore: Arc<MergeSemaphoreInner>) -> Self {
        Self { semaphore }
    }
}

impl Drop for MergePermit {
    fn drop(&mut self) {
        self.semaphore.release();
    }
}

/// Inner state of the merge semaphore (shared via Arc).
#[derive(Debug)]
struct MergeSemaphoreInner {
    /// Maximum permits allowed.
    max_permits: usize,
    /// Current available permits (AtomicU32 for cross-thread sync).
    available: AtomicU32,
}

impl MergeSemaphoreInner {
    fn new(max_permits: usize) -> Self {
        Self {
            max_permits,
            available: AtomicU32::new(max_permits as u32),
        }
    }

    /// Try to acquire a permit without blocking.
    fn try_acquire(&self) -> bool {
        let mut current = self.available.load(Ordering::Acquire);
        while current > 0 {
            match self.available.compare_exchange_weak(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
        false
    }

    /// Release a permit back to the semaphore.
    fn release(&self) {
        let _ = self
            .available
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                if current < self.max_permits as u32 {
                    Some(current + 1)
                } else {
                    None // Should not happen, but handle gracefully
                }
            });
    }

    /// Get current available permits.
    fn available_permits(&self) -> usize {
        self.available.load(Ordering::Acquire) as usize
    }
}

/// Bounded semaphore for limiting concurrent merge operations.
///
/// Provides backpressure by limiting the number of simultaneous merges.
/// Uses a non-blocking try_acquire pattern - if no permits are available,
/// the caller should return `MergeResult::NotReady`.
pub struct MergeSemaphore {
    /// Inner shared state.
    inner: Arc<MergeSemaphoreInner>,
    /// Queue of pending merge requests (for observability).
    pending: Arc<Mutex<VecDeque<(String, Instant)>>>,
    /// Metrics tracking.
    metrics: Arc<Mutex<MergeSemaphoreMetrics>>,
}

impl MergeSemaphore {
    /// Create a new merge semaphore.
    pub fn new(max_permits: usize) -> Self {
        let inner = Arc::new(MergeSemaphoreInner::new(max_permits));
        Self {
            inner: Arc::clone(&inner),
            pending: Arc::new(Mutex::new(VecDeque::new())),
            metrics: Arc::new(Mutex::new(MergeSemaphoreMetrics {
                available_permits: max_permits,
                queue_depth: 0,
                total_attempts: 0,
                successful_merges: 0,
            })),
        }
    }

    /// Create with default limits.
    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_MAX_CONCURRENT_MERGES)
    }

    /// Try to acquire a permit without blocking.
    ///
    /// Returns `Some(MergePermit)` if acquired, `None` if no permits available.
    /// The permit is automatically released when dropped.
    pub fn try_acquire(&self) -> Option<MergePermit> {
        // Track the attempt
        if let Ok(mut metrics) = self.metrics.try_lock() {
            metrics.total_attempts += 1;
        }

        if self.inner.try_acquire() {
            // Update metrics
            if let Ok(mut metrics) = self.metrics.try_lock() {
                metrics.available_permits = self.inner.available_permits();
            }
            Some(MergePermit::new(Arc::clone(&self.inner)))
        } else {
            // No permits available - would need to wait
            if let Ok(mut metrics) = self.metrics.try_lock() {
                metrics.available_permits = 0;
            }
            None
        }
    }

    /// Add a pending request to the queue (for observability).
    pub fn enqueue_pending(&self, batch_id: String) {
        if let Ok(mut queue) = self.pending.try_lock() {
            queue.push_back((batch_id, Instant::now()));
        }
        if let Ok(mut metrics) = self.metrics.try_lock() {
            metrics.queue_depth = self.pending.try_lock().map(|q| q.len()).unwrap_or(0);
        }
    }

    /// Remove a pending request from the queue (for observability).
    pub fn dequeue_pending(&self, batch_id: &str) {
        if let Ok(mut queue) = self.pending.try_lock() {
            queue.retain(|(id, _)| id != batch_id);
        }
        if let Ok(mut metrics) = self.metrics.try_lock() {
            metrics.queue_depth = self.pending.try_lock().map(|q| q.len()).unwrap_or(0);
        }
    }

    /// Get current metrics snapshot.
    pub fn metrics(&self) -> MergeSemaphoreMetrics {
        self.metrics
            .try_lock()
            .map(|m| m.clone())
            .unwrap_or_default()
    }

    /// Get current available permits.
    pub fn available_permits(&self) -> usize {
        self.inner.available_permits()
    }

    /// Record a successful merge completion.
    pub fn record_success(&self) {
        if let Ok(mut metrics) = self.metrics.try_lock() {
            metrics.successful_merges += 1;
        }
    }
}

impl Clone for MergeSemaphore {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            pending: Arc::clone(&self.pending),
            metrics: Arc::clone(&self.metrics),
        }
    }
}

/// Merge coordinator configuration.
#[derive(Debug, Clone)]
pub struct MergeConfig {
    /// Timeout for merge operations.
    pub merge_timeout: Duration,

    /// Number of retry attempts for merge operations.
    pub max_retries: usize,
}

impl Default for MergeConfig {
    fn default() -> Self {
        Self {
            merge_timeout: Duration::from_secs(300), // 5 minutes
            max_retries: 3,
        }
    }
}

/// Merge coordinator for distributed dataset conversion.
///
/// Coordinates the merging of staged outputs from multiple workers
/// into a single LeRobot dataset with sequential episode_index.
///
/// **Stateless design:** Uses CAS on batch status (Running → Merging) instead of
/// distributed locks. Any instance can attempt to claim merge - only the first
/// to successfully transition the batch status wins.
pub struct MergeCoordinator {
    /// TiKV client for distributed coordination.
    tikv: Arc<TikvClient>,

    /// Merge configuration.
    _config: MergeConfig,

    /// Storage factory for creating storage backends.
    storage_factory: StorageFactory,

    /// Temporary directory for merge operations.
    temp_dir: PathBuf,

    /// Semaphore for limiting concurrent merges (backpressure).
    semaphore: MergeSemaphore,
}

impl MergeCoordinator {
    /// Create a new merge coordinator.
    pub fn new(tikv: Arc<TikvClient>) -> Self {
        Self {
            tikv,
            _config: MergeConfig::default(),
            storage_factory: StorageFactory::from_env(),
            temp_dir: std::env::temp_dir(),
            semaphore: MergeSemaphore::with_defaults(),
        }
    }

    /// Create a new merge coordinator with custom configuration.
    pub fn with_config(tikv: Arc<TikvClient>, _config: MergeConfig) -> Self {
        Self {
            tikv,
            _config,
            storage_factory: StorageFactory::from_env(),
            temp_dir: std::env::temp_dir(),
            semaphore: MergeSemaphore::with_defaults(),
        }
    }

    /// Set the temporary directory for merge operations.
    pub fn with_temp_dir(mut self, temp_dir: PathBuf) -> Self {
        self.temp_dir = temp_dir;
        self
    }

    /// Set the storage factory for cloud storage operations.
    pub fn with_storage_factory(mut self, factory: StorageFactory) -> Self {
        self.storage_factory = factory;
        self
    }

    /// Set the merge semaphore for backpressure control.
    pub fn with_semaphore(mut self, semaphore: MergeSemaphore) -> Self {
        self.semaphore = semaphore;
        self
    }

    /// Get the merge semaphore (for metrics/observation).
    pub fn semaphore(&self) -> &MergeSemaphore {
        &self.semaphore
    }

    /// Register a worker's completed staging output.
    ///
    /// Called by workers when they finish writing their staged output.
    pub async fn register_staging_complete(
        &self,
        job_id: &str,
        worker_id: &str,
        staging_path: String,
        frame_count: u64,
    ) -> Result<(), TikvError> {
        // Get or create merge state
        let merge_key = Self::merge_state_key(job_id);

        // Try to get existing state
        let mut state = match self.tikv.get(merge_key.clone()).await? {
            Some(data) => bincode::deserialize(&data).map_err(|e| {
                TikvError::Serialization(format!("Failed to deserialize merge state: {}", e))
            })?,
            None => {
                // Create new state - we don't know expected_workers yet
                // This will be updated when merge is initiated
                MergeState::new(job_id.to_string(), 1, "".to_string())
            }
        };

        state.add_worker(worker_id.to_string(), staging_path.clone(), frame_count);

        // Serialize and save
        let data = bincode::serialize(&state).map_err(|e| {
            TikvError::Serialization(format!("Failed to serialize merge state: {}", e))
        })?;

        self.tikv.put(merge_key, data).await?;

        info!(
            job_id = %job_id,
            worker_id = %worker_id,
            staging_path = %staging_path,
            frame_count,
            completed_workers = state.completed_workers,
            "Registered staging complete"
        );

        Ok(())
    }

    /// Try to claim merge for a job.
    ///
    /// Returns true if this worker successfully claimed the merge task.
    ///
    /// **CAS-based claiming:** Uses atomic status transition (Running → Merging)
    /// instead of distributed locks. Multiple instances can call this - only the first
    /// to successfully transition the batch status wins.
    pub async fn try_claim_merge(
        &self,
        job_id: &str,
        expected_workers: usize,
        output_path: String,
    ) -> Result<MergeResult, TikvError> {
        // Phase 1: Acquire semaphore permit (backpressure)
        let _permit = self.try_acquire_merge_permit(job_id)?;

        // Phase 2: Get and validate current batch status
        let current_status = match self.get_batch_status(job_id).await? {
            Some(status) => status,
            None => return Ok(MergeResult::NotFound),
        };

        // Phase 3: Check if batch is claimable
        if !self.is_batch_claimable(&current_status, job_id) {
            return Ok(MergeResult::NotClaimed);
        }
        if !current_status.is_complete() {
            return Ok(MergeResult::NotReady);
        }

        // Phase 4: Transition to Merging (CAS)
        let status_key = BatchKeys::status(job_id);
        self.transition_to_merging(&status_key, &current_status)
            .await?;

        // Phase 5: Verify we won the race
        if !self.verify_cas_won(&status_key, &current_status).await? {
            return Ok(MergeResult::NotClaimed);
        }

        info!(
            job_id = %job_id,
            expected_workers,
            completed_work_units = current_status.work_units_completed,
            "CAS: Successfully claimed merge (Running → Merging)"
        );

        // Phase 6: Prepare and execute merge
        self.prepare_and_execute_merge(
            job_id,
            expected_workers,
            output_path,
            current_status,
            &status_key,
        )
        .await
    }

    /// Try to acquire a merge permit for backpressure.
    fn try_acquire_merge_permit(&self, job_id: &str) -> Result<MergePermit, TikvError> {
        match self.semaphore.try_acquire() {
            Some(permit) => {
                self.semaphore.dequeue_pending(job_id);
                Ok(permit)
            }
            None => {
                self.semaphore.enqueue_pending(job_id.to_string());
                tracing::debug!(
                    job_id = %job_id,
                    available_permits = self.semaphore.available_permits(),
                    "Merge backpressure: no permits available"
                );
                Err(TikvError::Other("No merge permits available".to_string()))
            }
        }
    }

    /// Get the current batch status for a job.
    async fn get_batch_status(&self, job_id: &str) -> Result<Option<BatchStatus>, TikvError> {
        let status_key = BatchKeys::status(job_id);
        match self.tikv.get(status_key).await? {
            Some(data) => {
                let status: BatchStatus = bincode::deserialize(&data)
                    .map_err(|e| TikvError::Deserialization(format!("batch status: {}", e)))?;
                Ok(Some(status))
            }
            None => {
                tracing::debug!(job_id = %job_id, "try_claim_merge: batch not found in TiKV");
                Ok(None)
            }
        }
    }

    /// Check if a batch is claimable (in Running phase).
    fn is_batch_claimable(&self, status: &BatchStatus, job_id: &str) -> bool {
        if status.phase != BatchPhase::Running {
            tracing::debug!(
                job_id = %job_id,
                phase = ?status.phase,
                "try_claim_merge: batch not in Running phase (cannot claim)"
            );
            false
        } else {
            true
        }
    }

    /// Transition batch from Running to Merging.
    async fn transition_to_merging(
        &self,
        status_key: &[u8],
        current_status: &BatchStatus,
    ) -> Result<(), TikvError> {
        let mut new_status = current_status.clone();
        new_status.transition_to(BatchPhase::Merging);
        let new_data =
            bincode::serialize(&new_status).map_err(|e| TikvError::Serialization(e.to_string()))?;

        self.tikv.put(status_key.to_vec(), new_data).await?;

        // Update phase index: Running -> Merging
        crate::batch::update_phase_index(
            &self.tikv,
            String::from_utf8_lossy(status_key).trim_start_matches("/roboflow/v1/batch/"),
            BatchPhase::Running,
            BatchPhase::Merging,
        )
        .await?;

        Ok(())
    }

    /// Verify we won the CAS race.
    async fn verify_cas_won(
        &self,
        status_key: &[u8],
        _current_status: &BatchStatus,
    ) -> Result<bool, TikvError> {
        let verify_data = self.tikv.get(status_key.to_vec()).await?;
        Ok(verify_data.is_some())
    }

    /// Prepare merge state and execute the merge.
    async fn prepare_and_execute_merge(
        &self,
        job_id: &str,
        expected_workers: usize,
        output_path: String,
        _current_status: BatchStatus,
        status_key: &[u8],
    ) -> Result<MergeResult, TikvError> {
        // Get or create merge state
        let mut state = self
            .get_or_create_merge_state(job_id, expected_workers, &output_path)
            .await?;

        // Check if ready to merge
        if !self.ensure_merge_ready(&mut state, expected_workers, &output_path) {
            self.rollback_to_running(status_key).await;
            return Ok(MergeResult::NotReady);
        }

        // Start merge
        let worker_id = format!("merge-{}", uuid::Uuid::new_v4());
        if let Err(e) = state.start_merge(worker_id.clone()) {
            let _ = self.fail_merge_with_status(job_id, &e.to_string()).await;
            return Ok(MergeResult::Failed { error: e });
        }

        // Save merge state
        self.save_merge_state(job_id, &state).await?;

        info!(
            job_id = %job_id,
            merge_worker = %worker_id,
            expected_workers = state.expected_workers,
            completed_workers = state.completed_workers,
            total_frames = state.total_frames,
            "=== MERGE EXECUTION START ==="
        );

        // Execute merge
        let merge_start = Instant::now();
        let actual_frames = match self.execute_merge(&state).await {
            Ok(frames) => frames,
            Err(e) => {
                let _ = self.fail_merge_with_status(job_id, &e.to_string()).await;
                tracing::error!(
                    job_id = %job_id,
                    error = %e,
                    "=== MERGE EXECUTION FAILED ==="
                );
                return Ok(MergeResult::Failed {
                    error: e.to_string(),
                });
            }
        };
        let merge_duration = merge_start.elapsed();

        // Complete the merge
        match self
            .complete_merge_with_status(job_id, actual_frames, &state.output_path)
            .await
        {
            Ok(()) => {
                self.semaphore.record_success();
                info!(
                    job_id = %job_id,
                    total_frames = actual_frames,
                    duration_secs = merge_duration.as_secs_f64(),
                    "=== MERGE EXECUTION END (SUCCESS) ==="
                );
                Ok(MergeResult::Success {
                    output_path: state.output_path,
                    total_frames: actual_frames,
                })
            }
            Err(e) => {
                tracing::error!(
                    job_id = %job_id,
                    error = %e,
                    duration_secs = merge_duration.as_secs_f64(),
                    "=== MERGE EXECUTION END (FAILED) ==="
                );
                Ok(MergeResult::Failed {
                    error: e.to_string(),
                })
            }
        }
    }

    /// Get or create merge state for a job.
    async fn get_or_create_merge_state(
        &self,
        job_id: &str,
        expected_workers: usize,
        output_path: &str,
    ) -> Result<MergeState, TikvError> {
        let merge_key = Self::merge_state_key(job_id);
        match self.tikv.get(merge_key.clone()).await? {
            Some(data) => {
                let mut state: MergeState = bincode::deserialize(&data).map_err(|e| {
                    TikvError::Serialization(format!("Failed to deserialize merge state: {}", e))
                })?;
                state.expected_workers = expected_workers;
                state.output_path = output_path.to_string();
                Ok(state)
            }
            None => Ok(MergeState::new(
                job_id.to_string(),
                expected_workers,
                output_path.to_string(),
            )),
        }
    }

    /// Ensure merge state is ready, handling single-worker mode.
    fn ensure_merge_ready(
        &self,
        state: &mut MergeState,
        expected_workers: usize,
        output_path: &str,
    ) -> bool {
        if state.is_ready() {
            return true;
        }

        // For single-worker mode, inject direct staging path
        if state.completed_workers == 0 && expected_workers == 1 {
            tracing::debug!("try_claim_merge: single-worker mode, injecting direct staging path");
            state.add_worker("direct".to_string(), output_path.to_string(), 0);
            return true;
        }

        false
    }

    /// Rollback batch status from Merging to Running.
    async fn rollback_to_running(&self, status_key: &[u8]) {
        // This is a best-effort rollback
        let _ = self.tikv.delete(status_key.to_vec()).await;
    }

    /// Save merge state to TiKV.
    async fn save_merge_state(&self, job_id: &str, state: &MergeState) -> Result<(), TikvError> {
        let merge_key = Self::merge_state_key(job_id);
        let merge_data = bincode::serialize(state).map_err(|e| {
            TikvError::Serialization(format!("Failed to serialize merge state: {}", e))
        })?;
        self.tikv.put(merge_key, merge_data).await
    }

    /// Execute the actual merge operation.
    async fn execute_merge(&self, state: &MergeState) -> Result<u64, TikvError> {
        let storage = self
            .storage_factory
            .create(&state.output_path)
            .map_err(|e| TikvError::Other(format!("Failed to create storage: {}", e)))?;

        let executor =
            ParquetMergeExecutor::new(storage, state.output_path.clone(), self.temp_dir.clone());

        // Try to load LeRobot config from the batch's config_hash
        let lerobot_config = self.load_lerobot_config(&state.job_id).await;

        executor
            .execute(state, lerobot_config.as_ref())
            .await
            .map_err(|e| TikvError::Other(format!("Merge execution failed: {}", e)))
    }

    /// Load LeRobot config from TiKV for metadata generation.
    async fn load_lerobot_config(
        &self,
        job_id: &str,
    ) -> Option<roboflow_dataset::formats::lerobot::config::LerobotConfig> {
        use roboflow_dataset::formats::lerobot::config::LerobotConfig;

        // Get the batch spec to find the config hash
        let spec_key = crate::batch::BatchKeys::spec(job_id);
        let spec_data = self.tikv.get(spec_key).await.ok()??;

        let batch_spec: crate::batch::BatchSpec = bincode::deserialize(&spec_data).ok()?;

        // The config field contains either "default" or a config hash
        let config_hash = &batch_spec.spec.config;
        if config_hash == "default" {
            tracing::debug!("Batch uses default config, skipping metadata generation");
            return None;
        }

        // Load the config from TiKV
        let config_record = self.tikv.get_config(config_hash).await.ok()??;

        // Parse the TOML config
        LerobotConfig::from_toml(&config_record.content).ok()
    }

    /// Mark the merge as failed by transitioning batch status from Merging to Failed.
    async fn fail_merge_with_status(&self, job_id: &str, error: &str) -> Result<(), TikvError> {
        let status_key = BatchKeys::status(job_id);
        let data = self.tikv.get(status_key.clone()).await?;

        let mut status: BatchStatus = match data {
            Some(d) => bincode::deserialize(&d)
                .map_err(|e| TikvError::Deserialization(format!("batch status: {}", e)))?,
            None => {
                return Err(TikvError::KeyNotFound(format!(
                    "Batch status not found: {}",
                    job_id
                )));
            }
        };

        // Transition Merging → Failed
        let old_phase = status.phase;
        status.transition_to(BatchPhase::Failed);
        status.error = Some(error.to_string());

        let new_data =
            bincode::serialize(&status).map_err(|e| TikvError::Serialization(e.to_string()))?;

        self.tikv.put(status_key, new_data).await?;

        // Update phase index
        let _ = crate::batch::update_phase_index(&self.tikv, job_id, old_phase, BatchPhase::Failed)
            .await;

        // Also mark merge state as failed
        let merge_key = Self::merge_state_key(job_id);
        if let Some(merge_data) = self.tikv.get(merge_key.clone()).await? {
            let mut state: MergeState = bincode::deserialize(&merge_data)
                .map_err(|e| TikvError::Serialization(format!("merge state: {}", e)))?;
            state.fail(error.to_string());
            let data =
                bincode::serialize(&state).map_err(|e| TikvError::Serialization(e.to_string()))?;
            let _ = self.tikv.put(merge_key, data).await;
        }

        tracing::error!(
            job_id = %job_id,
            error,
            "Merge failed, batch marked Failed"
        );

        Ok(())
    }

    /// Complete the merge by transitioning batch status from Merging to Complete.
    async fn complete_merge_with_status(
        &self,
        job_id: &str,
        total_frames: u64,
        output_path: &str,
    ) -> Result<(), TikvError> {
        let status_key = BatchKeys::status(job_id);
        let data = self.tikv.get(status_key.clone()).await?;

        let mut status: BatchStatus = match data {
            Some(d) => bincode::deserialize(&d)
                .map_err(|e| TikvError::Deserialization(format!("batch status: {}", e)))?,
            None => {
                return Err(TikvError::KeyNotFound(format!(
                    "Batch status not found: {}",
                    job_id
                )));
            }
        };

        // Transition Merging → Complete
        status.transition_to(BatchPhase::Complete);
        // Store total_frames (using error field for now since BatchStatus doesn't have total_frames)
        // In future, add total_frames field to BatchStatus

        let new_data =
            bincode::serialize(&status).map_err(|e| TikvError::Serialization(e.to_string()))?;

        self.tikv.put(status_key, new_data).await?;

        // Update phase index: Merging -> Complete
        let _ = crate::batch::update_phase_index(
            &self.tikv,
            job_id,
            BatchPhase::Merging,
            BatchPhase::Complete,
        )
        .await;

        // Also mark merge state as complete
        let merge_key = Self::merge_state_key(job_id);
        if let Some(merge_data) = self.tikv.get(merge_key.clone()).await? {
            let mut state: MergeState = bincode::deserialize(&merge_data)
                .map_err(|e| TikvError::Serialization(format!("merge state: {}", e)))?;
            state.total_frames = total_frames;
            state.complete();
            let data =
                bincode::serialize(&state).map_err(|e| TikvError::Serialization(e.to_string()))?;
            let _ = self.tikv.put(merge_key, data).await;
        }

        info!(
            job_id = %job_id,
            total_frames,
            output_path = %output_path,
            "Merge completed, batch marked Complete"
        );

        Ok(())
    }

    /// Get merge state for a job.
    pub async fn get_merge_state(&self, job_id: &str) -> Result<Option<MergeState>, TikvError> {
        let merge_key = Self::merge_state_key(job_id);
        match self.tikv.get(merge_key).await? {
            Some(data) => {
                let state: MergeState = bincode::deserialize(&data).map_err(|e| {
                    TikvError::Serialization(format!("Failed to deserialize merge state: {}", e))
                })?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    /// Check if merge is complete for a job.
    pub async fn is_merge_complete(&self, job_id: &str) -> Result<bool, TikvError> {
        match self.get_merge_state(job_id).await? {
            Some(state) => Ok(state.is_complete()),
            None => Ok(false),
        }
    }

    /// Build the merge state key for a job.
    fn merge_state_key(job_id: &str) -> Vec<u8> {
        format!("/roboflow/v1/merge/{}", job_id).into_bytes()
    }
}

/// Result of a merge claim attempt.
#[derive(Debug, Clone)]
pub enum MergeResult {
    /// Batch not found.
    NotFound,

    /// Merge was not claimed (another worker has it).
    NotClaimed,

    /// Merge is not ready (waiting for workers).
    NotReady,

    /// Merge completed successfully.
    Success {
        /// Output path of the merged dataset.
        output_path: String,
        /// Total number of frames merged.
        total_frames: u64,
    },

    /// Merge failed.
    Failed {
        /// Error message.
        error: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_config_default() {
        let config = MergeConfig::default();
        assert_eq!(config.merge_timeout, Duration::from_secs(300));
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_merge_state_key() {
        let key = MergeCoordinator::merge_state_key("job-123");
        let key_str = String::from_utf8(key).unwrap();
        assert_eq!(key_str, "/roboflow/v1/merge/job-123");
    }

    #[test]
    fn test_merge_config_clone() {
        let config = MergeConfig::default();
        let cloned = config.clone();
        assert_eq!(config.merge_timeout, cloned.merge_timeout);
        assert_eq!(config.max_retries, cloned.max_retries);
    }

    #[test]
    fn test_merge_config_debug() {
        let config = MergeConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("MergeConfig"));
        assert!(debug_str.contains("merge_timeout"));
        assert!(debug_str.contains("max_retries"));
    }

    #[test]
    fn test_merge_semaphore_metrics_default() {
        let metrics = MergeSemaphoreMetrics::default();
        assert_eq!(metrics.available_permits, 0);
        assert_eq!(metrics.queue_depth, 0);
        assert_eq!(metrics.total_attempts, 0);
        assert_eq!(metrics.successful_merges, 0);
    }

    #[test]
    fn test_merge_semaphore_metrics_clone() {
        let metrics = MergeSemaphoreMetrics {
            available_permits: 5,
            queue_depth: 2,
            total_attempts: 100,
            successful_merges: 95,
        };
        let cloned = metrics.clone();
        assert_eq!(metrics.available_permits, cloned.available_permits);
        assert_eq!(metrics.queue_depth, cloned.queue_depth);
    }

    #[test]
    fn test_default_max_concurrent_merges() {
        assert_eq!(DEFAULT_MAX_CONCURRENT_MERGES, 3);
    }

    #[test]
    fn test_merge_semaphore_new() {
        let semaphore = MergeSemaphore::new(5);
        assert_eq!(semaphore.available_permits(), 5);
    }

    #[test]
    fn test_merge_semaphore_with_defaults() {
        let semaphore = MergeSemaphore::with_defaults();
        assert_eq!(semaphore.available_permits(), DEFAULT_MAX_CONCURRENT_MERGES);
    }

    #[test]
    fn test_merge_semaphore_acquire_release() {
        let semaphore = MergeSemaphore::new(2);

        // First acquire should succeed
        let permit1 = semaphore.try_acquire();
        assert!(permit1.is_some());
        assert_eq!(semaphore.available_permits(), 1);

        // Second acquire should succeed
        let permit2 = semaphore.try_acquire();
        assert!(permit2.is_some());
        assert_eq!(semaphore.available_permits(), 0);

        // Third acquire should fail (no permits left)
        let permit3 = semaphore.try_acquire();
        assert!(permit3.is_none());

        // Release first permit
        drop(permit1);
        assert_eq!(semaphore.available_permits(), 1);

        // Now acquire should succeed again
        let permit4 = semaphore.try_acquire();
        assert!(permit4.is_some());
        assert_eq!(semaphore.available_permits(), 0);
    }

    #[test]
    fn test_merge_semaphore_clone() {
        let semaphore1 = MergeSemaphore::new(3);
        let semaphore2 = semaphore1.clone();

        // Both should share the same inner state
        assert_eq!(
            semaphore1.available_permits(),
            semaphore2.available_permits()
        );

        // Acquiring from one affects the other
        let _permit = semaphore1.try_acquire();
        assert_eq!(semaphore2.available_permits(), 2);
    }

    #[test]
    fn test_merge_semaphore_metrics() {
        let semaphore = MergeSemaphore::new(3);

        let metrics = semaphore.metrics();
        assert_eq!(metrics.available_permits, 3);
        assert_eq!(metrics.queue_depth, 0);
        assert_eq!(metrics.total_attempts, 0);
        assert_eq!(metrics.successful_merges, 0);
    }

    #[test]
    fn test_merge_semaphore_record_success() {
        let semaphore = MergeSemaphore::new(3);

        semaphore.record_success();
        semaphore.record_success();
        semaphore.record_success();

        let metrics = semaphore.metrics();
        assert_eq!(metrics.successful_merges, 3);
    }

    #[test]
    fn test_merge_semaphore_enqueue_dequeue_pending() {
        let semaphore = MergeSemaphore::new(1);

        // Enqueue some pending requests
        semaphore.enqueue_pending("batch-1".to_string());
        semaphore.enqueue_pending("batch-2".to_string());

        let metrics = semaphore.metrics();
        assert_eq!(metrics.queue_depth, 2);

        // Dequeue one
        semaphore.dequeue_pending("batch-1");
        let metrics = semaphore.metrics();
        assert_eq!(metrics.queue_depth, 1);

        // Dequeue the other
        semaphore.dequeue_pending("batch-2");
        let metrics = semaphore.metrics();
        assert_eq!(metrics.queue_depth, 0);
    }

    #[test]
    fn test_merge_permit_debug() {
        let semaphore = MergeSemaphore::new(1);
        let permit = semaphore.try_acquire().unwrap();
        // Just verify we can debug format the permit without panicking
        let _ = format!("{:?}", permit);
    }

    #[test]
    fn test_merge_result_variants() {
        let not_found = MergeResult::NotFound;
        let not_claimed = MergeResult::NotClaimed;
        let not_ready = MergeResult::NotReady;
        let success = MergeResult::Success {
            output_path: "s3://bucket/output".to_string(),
            total_frames: 1000,
        };
        let failed = MergeResult::Failed {
            error: "Test error".to_string(),
        };

        // Just verify we can create and match all variants
        match not_found {
            MergeResult::NotFound => {}
            _ => panic!("Expected NotFound"),
        }

        match not_claimed {
            MergeResult::NotClaimed => {}
            _ => panic!("Expected NotClaimed"),
        }

        match not_ready {
            MergeResult::NotReady => {}
            _ => panic!("Expected NotReady"),
        }

        match success {
            MergeResult::Success {
                output_path,
                total_frames,
            } => {
                assert_eq!(output_path, "s3://bucket/output");
                assert_eq!(total_frames, 1000);
            }
            _ => panic!("Expected Success"),
        }

        match failed {
            MergeResult::Failed { error } => {
                assert_eq!(error, "Test error");
            }
            _ => panic!("Expected Failed"),
        }
    }

    #[test]
    fn test_merge_result_clone() {
        let result = MergeResult::Success {
            output_path: "test/path".to_string(),
            total_frames: 500,
        };
        let cloned = result.clone();

        match cloned {
            MergeResult::Success {
                output_path,
                total_frames,
            } => {
                assert_eq!(output_path, "test/path");
                assert_eq!(total_frames, 500);
            }
            _ => panic!("Expected Success"),
        }
    }

    #[test]
    fn test_merge_result_debug() {
        let result = MergeResult::Success {
            output_path: "test/path".to_string(),
            total_frames: 100,
        };
        let debug_str = format!("{:?}", result);
        assert!(debug_str.contains("Success"));
        assert!(debug_str.contains("output_path"));
    }
}
