// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Batch job controller.
//!
//! The controller implements the reconciliation loop that drives the actual state
//! to match the desired state (BatchSpec). This is similar to Kubernetes controllers.

use super::key::{BatchIndexKeys, BatchKeys, WorkUnitKeys};
use super::spec::BatchSpec;
use super::status::{BatchPhase, BatchStatus, DiscoveryStatus, FailedWorkUnit};
use super::work_unit::{WorkUnit, WorkUnitStatus};
use crate::tikv::{TikvClient, TikvError};

use std::sync::Arc;
use tokio::time::{Duration, sleep};

/// Controller configuration.
#[derive(Debug, Clone)]
pub struct ControllerConfig {
    /// Reconciliation interval (how often to check for work).
    pub reconcile_interval: Duration,

    /// Maximum number of batches to reconcile per loop.
    pub max_batches_per_loop: usize,

    /// Maximum number of work units to create per batch.
    pub max_work_units_per_batch: usize,

    /// File discovery timeout.
    pub discovery_timeout: Duration,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            reconcile_interval: Duration::from_secs(5),
            max_batches_per_loop: 100,
            max_work_units_per_batch: 1000,
            discovery_timeout: Duration::from_secs(300), // 5 minutes
        }
    }
}

/// Batch job controller.
///
/// The controller watches for batch specs and reconciles the actual state
/// to match the desired state. It runs a continuous loop that:
/// 1. Scans for pending batch specs
/// 2. Reconciles each batch (phase transitions, work unit creation)
/// 3. Updates batch status
#[derive(Clone)]
pub struct BatchController {
    /// TiKV client for distributed coordination.
    client: Arc<TikvClient>,

    /// Controller configuration.
    config: ControllerConfig,
}

impl BatchController {
    /// Create a new batch controller.
    pub fn new(client: Arc<TikvClient>, config: ControllerConfig) -> Self {
        Self { client, config }
    }

    /// Create a new batch controller with default configuration.
    pub fn with_client(client: Arc<TikvClient>) -> Self {
        Self::new(client, ControllerConfig::default())
    }

    /// Run the reconciliation loop continuously.
    ///
    /// This runs indefinitely until the shutdown signal is received.
    pub async fn run(&self) -> Result<(), TikvError> {
        tracing::info!(
            interval_secs = self.config.reconcile_interval.as_secs(),
            max_batches = self.config.max_batches_per_loop,
            "Starting batch controller"
        );

        loop {
            if let Err(e) = self.reconcile_all().await {
                tracing::error!(error = %e, "Reconciliation failed, will retry");
            }

            sleep(self.config.reconcile_interval).await;
        }
    }

    /// Reconcile all active batch jobs.
    ///
    /// Uses the phase index to find only active batches instead of scanning
    /// all specs. This is O(active batches) instead of O(total batches),
    /// which is critical for long-running clusters where batch records
    /// accumulate over time.
    pub async fn reconcile_all(&self) -> Result<(), TikvError> {
        // Scan phase indexes for active (non-terminal) phases only.
        // This avoids scanning thousands of old Complete/Failed/Cancelled specs.
        let active_phases = [
            BatchPhase::Pending,
            BatchPhase::Discovering,
            BatchPhase::Running,
            BatchPhase::Merging,
            BatchPhase::Suspending,
            BatchPhase::Suspended,
            // Include Failed to allow reconciliation of batches where work units
            // may have been manually retried and completed outside normal flow
            BatchPhase::Failed,
        ];

        let mut batch_ids = Vec::new();

        // Use a generous scan limit since index entries are tiny and stale
        // entries may exist. max_batches_per_loop limits actual processing.
        const INDEX_SCAN_LIMIT: u32 = 1000;

        for phase in active_phases {
            let prefix = BatchIndexKeys::phase_prefix(phase);
            let results = self.client.scan(prefix, INDEX_SCAN_LIMIT).await?;
            for (key, _) in results {
                let key_str = String::from_utf8_lossy(&key);
                if let Some(batch_id) = key_str.split('/').next_back() {
                    batch_ids.push(batch_id.to_string());
                }
            }
            if batch_ids.len() >= self.config.max_batches_per_loop {
                break;
            }
        }

        tracing::debug!(count = batch_ids.len(), "Found active batches to reconcile");

        let mut failed_batches = Vec::new();
        let mut first_error: Option<TikvError> = None;

        for batch_id in batch_ids {
            // Fetch the spec for this batch
            let spec_key = BatchKeys::spec(&batch_id);
            let spec_data = match self.client.get(spec_key.clone()).await? {
                Some(d) => d,
                None => {
                    tracing::warn!(
                        batch_id = %batch_id,
                        "Spec not found for indexed batch - stale index entry"
                    );
                    continue;
                }
            };

            if let Err(e) = self.reconcile_batch(&spec_key, &spec_data).await {
                tracing::error!(
                    error = %e,
                    batch_id = %batch_id,
                    "Failed to reconcile batch"
                );
                failed_batches.push(batch_id);
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }

        if !failed_batches.is_empty() {
            return Err(TikvError::Other(format!(
                "Failed to reconcile {} batch(es): {}",
                failed_batches.len(),
                failed_batches.join(", ")
            )));
        }

        Ok(())
    }

    /// Reconcile a single batch by ID.
    ///
    /// This bypasses phase indexes and directly loads the spec.
    pub async fn reconcile_batch_id(&self, batch_id: &str) -> Result<(), TikvError> {
        let spec_key = BatchKeys::spec(batch_id);
        let spec_data = match self.client.get(spec_key.clone()).await? {
            Some(data) => data,
            None => {
                return Err(TikvError::Other(format!(
                    "Spec not found for batch_id {}",
                    batch_id
                )));
            }
        };

        self.reconcile_batch(&spec_key, &spec_data).await
    }

    /// Reconcile a single batch job.
    ///
    /// This reads the spec and status, then drives the state forward.
    /// Terminal-phase batches (Complete, Failed, Cancelled) are skipped
    /// to avoid unnecessary TiKV writes and WriteConflict contention.
    async fn reconcile_batch(&self, _spec_key: &[u8], spec_data: &[u8]) -> Result<(), TikvError> {
        // Deserialize spec
        let spec: BatchSpec = serde_yaml_ng::from_slice(spec_data)
            .map_err(|e| TikvError::Deserialization(format!("batch spec: {}", e)))?;

        let batch_id = super::batch_id_from_spec(&spec);

        // Get current status
        let status_key = BatchKeys::status(&batch_id);
        let status = match self.client.get(status_key.clone()).await? {
            Some(data) => {
                let s: BatchStatus = bincode::deserialize(&data)
                    .map_err(|e| TikvError::Deserialization(format!("batch status: {}", e)))?;
                s
            }
            None => BatchStatus::new(),
        };

        // Skip terminal phases — nothing to reconcile, avoid unnecessary writes
        // Note: Failed is NOT skipped to allow reconciliation when work units
        // are manually retried and completed outside normal flow
        if matches!(status.phase, BatchPhase::Complete | BatchPhase::Cancelled) {
            tracing::debug!(
                batch_id = %batch_id,
                phase = ?status.phase,
                "Skipping terminal-phase batch"
            );
            return Ok(());
        }

        let old_phase = status.phase;

        tracing::info!(
            batch_id = %batch_id,
            phase = ?old_phase,
            work_units_total = status.work_units_total,
            work_units_completed = status.work_units_completed,
            "reconcile_batch: read status from TiKV"
        );

        // Reconcile based on current phase
        let new_status = self.reconcile_phase(&spec, status).await?;

        // Save updated status
        self.save_status(&batch_id, &new_status).await?;

        // Update phase index if phase changed
        if old_phase != new_status.phase {
            super::update_phase_index(&self.client, &batch_id, old_phase, new_status.phase).await?;
        }

        Ok(())
    }

    /// Reconcile the batch phase.
    ///
    /// This drives the state machine forward based on the current phase.
    async fn reconcile_phase(
        &self,
        spec: &BatchSpec,
        mut status: BatchStatus,
    ) -> Result<BatchStatus, TikvError> {
        match status.phase {
            BatchPhase::Pending => {
                // Validate and transition to Discovering
                if let Err(e) = spec.validate() {
                    status.transition_to(BatchPhase::Failed);
                    status.error = Some(format!("Validation failed: {}", e));
                    return Ok(status);
                }

                status.transition_to(BatchPhase::Discovering);
                status.discovery_status =
                    Some(DiscoveryStatus::new(spec.spec.sources.len() as u32));

                Ok(status)
            }
            BatchPhase::Discovering => {
                // Discover files and create work units
                self.reconcile_discovering(spec, status).await
            }
            BatchPhase::Running | BatchPhase::Failed => {
                // Check if batch is complete
                // Note: Failed is included to allow reconciliation when work units
                // are manually retried and completed outside normal flow
                self.reconcile_running(spec, status).await
            }
            BatchPhase::Merging => {
                // Merge in progress (handled by Finalizer)
                Ok(status)
            }
            BatchPhase::Complete | BatchPhase::Cancelled => {
                // Terminal phases, nothing to do
                Ok(status)
            }
            BatchPhase::Suspending | BatchPhase::Suspended => {
                // Not implemented yet
                Ok(status)
            }
        }
    }

    /// Reconcile the Discovering phase.
    ///
    /// NOTE: File discovery is handled by the Scanner actor, which has
    /// leader election to ensure only one instance performs discovery.
    /// This method only checks for discovery timeout and lets the Scanner
    /// handle the actual file discovery and work unit creation.
    async fn reconcile_discovering(
        &self,
        spec: &BatchSpec,
        mut status: BatchStatus,
    ) -> Result<BatchStatus, TikvError> {
        let batch_id = super::batch_id_from_spec(spec);

        // Defensively handle missing discovery_status (recover from inconsistent state)
        if status.discovery_status.is_none() {
            tracing::warn!(
                batch_id = %batch_id,
                "Discovery phase but no discovery_status - recovering"
            );
            status.discovery_status = Some(DiscoveryStatus::new(spec.spec.sources.len() as u32));
        }

        // Check for discovery timeout - Scanner should complete discovery within
        // the configured timeout. If it doesn't, mark the batch as failed.
        let age_secs = status
            .updated_at
            .signed_duration_since(chrono::Utc::now())
            .num_seconds()
            .abs();
        let timeout_secs = self.config.discovery_timeout.as_secs() as i64;

        if age_secs > timeout_secs {
            tracing::warn!(
                batch_id = %batch_id,
                age_secs = age_secs,
                timeout_secs = timeout_secs,
                "Discovery timeout exceeded"
            );
            status.transition_to(BatchPhase::Failed);
            status.error = Some(format!(
                "Discovery timeout: exceeded {} seconds",
                timeout_secs
            ));
            return Ok(status);
        }

        // Scanner is responsible for file discovery and work unit creation.
        // When Scanner completes discovery, it will transition the batch to Running.
        // This controller just waits and checks for timeout.
        tracing::debug!(
            batch_id = %batch_id,
            age_secs = age_secs,
            "Waiting for Scanner to complete discovery"
        );

        Ok(status)
    }

    /// Reconcile the Running phase.
    ///
    /// This checks if all work units are complete and updates the batch status.
    async fn reconcile_running(
        &self,
        spec: &BatchSpec,
        mut status: BatchStatus,
    ) -> Result<BatchStatus, TikvError> {
        let batch_id = super::batch_id_from_spec(spec);

        // Scan all work units for this batch
        let prefix = WorkUnitKeys::batch_prefix(&batch_id);
        let work_units = self.client.scan(prefix, 10000).await?;

        let mut completed = 0u32;
        let mut failed = 0u32;
        let mut processing = 0u32;
        let mut scan_total = 0u32;
        let mut failed_work_units = Vec::new();

        for (key, value) in work_units {
            match bincode::deserialize::<WorkUnit>(&value) {
                Ok(unit) => {
                    scan_total += 1; // Count all valid work units
                    match unit.status {
                        WorkUnitStatus::Complete => completed += 1,
                        WorkUnitStatus::Failed | WorkUnitStatus::Dead => {
                            failed += 1;
                            // Collect error details from failed work units
                            let error = unit
                                .error
                                .as_ref()
                                .cloned()
                                .unwrap_or_else(|| "Unknown error".to_string());
                            let source_file = unit
                                .files
                                .first()
                                .map(|f| f.url.clone())
                                .unwrap_or_else(|| "unknown".to_string());
                            failed_work_units.push(FailedWorkUnit {
                                id: unit.id.clone(),
                                source_file,
                                error,
                                retries: unit.attempts,
                                failed_at: unit.updated_at,
                            });
                        }
                        WorkUnitStatus::Processing => processing += 1,
                        _ => {} // Pending or other states - count in scan_total but not in other categories
                    }
                }
                Err(e) => {
                    // Log corrupted work units for investigation
                    tracing::error!(
                        error = %e,
                        key = %String::from_utf8_lossy(&key),
                        work_unit_id = %String::from_utf8_lossy(&key),
                        "Failed to deserialize work unit, skipping. Batch completion counts may be incorrect."
                    );
                }
            }
        }
        tracing::info!(
            batch_id = %batch_id,
            work_units_total = status.work_units_total,
            scan_total = scan_total,
            completed = completed,
            failed = failed,
            processing = processing,
            "reconcile_running: work unit scan results"
        );

        // Update counts from scan. Ensure work_units_total matches reality so is_complete() works.
        if scan_total > 0 {
            status.set_work_units_total(scan_total);
            status.set_files_total(scan_total);
        }
        status.work_units_completed = completed;
        status.work_units_failed = failed;
        status.work_units_active = processing;
        status.files_completed = completed;
        status.files_failed = failed;
        status.files_active = processing;
        status.failed_work_units = failed_work_units;

        if matches!(status.phase, BatchPhase::Failed)
            && failed == 0
            && (completed > 0 || processing > 0)
        {
            status.error = None;
            status.failed_work_units.clear();
            status.completed_at = None;
            status.transition_to(BatchPhase::Running);
        }

        // Check if batch should be marked failed
        if status.should_fail(spec.spec.backoff_limit) {
            status.transition_to(BatchPhase::Failed);
            status.error = Some(format!(
                "Backoff limit exceeded: {} work units failed",
                status.work_units_failed
            ));
            return Ok(status);
        }

        // When all work units are done, if any failed the batch is Failed
        // (e.g. 10 files, 1 failed -> Failed, not Complete).
        if status.is_complete() && status.work_units_failed > 0 {
            status.transition_to(BatchPhase::Failed);
            status.error = Some(format!(
                "{} of {} work units failed",
                status.work_units_failed, status.work_units_total
            ));
            return Ok(status);
        }

        // When all work units completed successfully, leave in Running for the
        // finalizer to trigger merge (Running -> Merging -> Complete).

        Ok(status)
    }

    /// Save batch status to TiKV.
    async fn save_status(&self, batch_id: &str, status: &BatchStatus) -> Result<(), TikvError> {
        let key = BatchKeys::status(batch_id);
        let data =
            bincode::serialize(status).map_err(|e| TikvError::Serialization(e.to_string()))?;
        self.client.put(key, data).await
    }

    /// Submit a new batch job.
    ///
    /// This creates the batch spec in TiKV.
    pub async fn submit_batch(&self, spec: &BatchSpec) -> Result<String, TikvError> {
        let batch_id = super::batch_id_from_spec(spec);

        // Validate spec
        spec.validate()
            .map_err(|e| TikvError::Other(format!("Validation failed: {}", e)))?;

        // Create initial status
        let status = BatchStatus::new();

        // Save spec and status in a single transaction
        let spec_key = BatchKeys::spec(&batch_id);
        let spec_data = serde_yaml_ng::to_string(spec)
            .map_err(|e| TikvError::Serialization(format!("yaml: {}", e)))?
            .into_bytes();

        let status_key = BatchKeys::status(&batch_id);
        let status_data =
            bincode::serialize(&status).map_err(|e| TikvError::Serialization(e.to_string()))?;

        // Include phase index in the same transaction for atomicity
        let phase_key = BatchIndexKeys::phase(BatchPhase::Pending, &batch_id);

        self.client
            .batch_put(vec![
                (spec_key, spec_data),
                (status_key, status_data),
                (phase_key, vec![]),
            ])
            .await?;

        tracing::info!(
            batch_id = %batch_id,
            sources = spec.spec.sources.len(),
            output = %spec.spec.output,
            "Batch job submitted"
        );

        Ok(batch_id)
    }

    /// Get batch status.
    pub async fn get_batch_status(&self, batch_id: &str) -> Result<Option<BatchStatus>, TikvError> {
        let key = BatchKeys::status(batch_id);
        let data = self.client.get(key).await?;

        match data {
            Some(bytes) => {
                let status: BatchStatus = bincode::deserialize(&bytes)
                    .map_err(|e| TikvError::Deserialization(e.to_string()))?;
                Ok(Some(status))
            }
            None => Ok(None),
        }
    }

    /// Get batch spec.
    pub async fn get_batch_spec(&self, batch_id: &str) -> Result<Option<BatchSpec>, TikvError> {
        let key = BatchKeys::spec(batch_id);
        let data = self.client.get(key).await?;

        match data {
            Some(bytes) => {
                let spec: BatchSpec = serde_yaml_ng::from_slice(&bytes)
                    .map_err(|e| TikvError::Deserialization(e.to_string()))?;
                Ok(Some(spec))
            }
            None => Ok(None),
        }
    }

    /// List all batch jobs.
    pub async fn list_batches(&self) -> Result<Vec<BatchSummary>, TikvError> {
        let prefix = BatchKeys::specs_prefix();
        let specs = self.client.scan(prefix, 1000).await?;

        let mut summaries = Vec::new();

        for (key, _value) in specs {
            // Extract batch_id from key
            let key_str = String::from_utf8_lossy(&key);
            if let Some(batch_id) = key_str.split('/').next_back()
                && let Some(status) = self.get_batch_status(batch_id).await?
            {
                // Handle missing spec gracefully (inconsistent state)
                let spec = match self.get_batch_spec(batch_id).await? {
                    Some(s) => s,
                    None => {
                        tracing::warn!(
                            batch_id = %batch_id,
                            "Spec missing for batch with status - skipping (inconsistent state)"
                        );
                        continue;
                    }
                };

                summaries.push(BatchSummary {
                    id: batch_id.to_string(),
                    name: spec
                        .metadata
                        .display_name
                        .clone()
                        .unwrap_or(spec.metadata.name.clone()),
                    namespace: spec.metadata.namespace,
                    phase: status.phase,
                    files_total: status.files_total,
                    files_completed: status.files_completed,
                    files_failed: status.files_failed,
                    created_at: spec.metadata.created_at,
                    started_at: status.started_at,
                    completed_at: status.completed_at,
                });
            }
        }

        Ok(summaries)
    }

    /// Cancel a batch job.
    pub async fn cancel_batch(&self, batch_id: &str) -> Result<bool, TikvError> {
        let mut status = match self.get_batch_status(batch_id).await? {
            Some(s) => s,
            None => return Ok(false),
        };

        // Can only cancel active phases
        if !status.phase.is_active() && status.phase != BatchPhase::Pending {
            return Ok(false);
        }

        let old_phase = status.phase;
        status.transition_to(BatchPhase::Cancelled);
        self.save_status(batch_id, &status).await?;

        // Update phase index
        super::update_phase_index(&self.client, batch_id, old_phase, BatchPhase::Cancelled).await?;

        tracing::info!(batch_id = %batch_id, "Batch job cancelled");

        Ok(true)
    }

    /// Claim a work unit for a worker.
    ///
    /// This atomically claims a pending work unit and returns it.
    /// Uses a transaction to prevent race conditions.
    ///
    /// Pending key format: `/roboflow/v1/batch/pending/{batch_id}/{unit_id}`
    pub async fn claim_work_unit(&self, worker_id: &str) -> Result<Option<WorkUnit>, TikvError> {
        use bincode::{deserialize, serialize};

        // Scan for the first pending work unit key
        let pending_prefix_bytes = WorkUnitKeys::pending_prefix();
        tracing::debug!(
            prefix = %String::from_utf8_lossy(&pending_prefix_bytes),
            prefix_hex = ?pending_prefix_bytes,
            "claim_work_unit: scanning pending prefix"
        );
        let pending = self.client.scan(pending_prefix_bytes.clone(), 1).await?;

        tracing::debug!(results = pending.len(), "claim_work_unit: scan completed");

        if pending.is_empty() {
            tracing::debug!("claim_work_unit: no pending work units found");
            return Ok(None);
        }

        let (pending_key, _batch_id_bytes) = &pending[0];
        tracing::info!(
            pending_key = %String::from_utf8_lossy(pending_key),
            "claim_work_unit: found pending entry"
        );

        // Parse batch_id and unit_id from the pending key.
        // Key format: /roboflow/v1/batch/pending/{batch_id}/{unit_id}
        let pending_prefix = String::from_utf8_lossy(&pending_prefix_bytes);
        let pending_key_str = String::from_utf8_lossy(pending_key);
        let suffix = match pending_key_str.strip_prefix(pending_prefix.as_ref()) {
            Some(s) => s.trim_start_matches('/'),
            None => {
                tracing::warn!(
                    pending_key = %pending_key_str,
                    expected_prefix = %pending_prefix,
                    "Invalid pending key format"
                );
                return Ok(None);
            }
        };

        // suffix = "{batch_id}/{unit_id}"
        let (batch_id, unit_id) = match suffix.split_once('/') {
            Some((b, u)) => (b, u),
            None => {
                tracing::warn!(
                    pending_key = %pending_key_str,
                    suffix = %suffix,
                    "Invalid pending key: expected batch_id/unit_id"
                );
                return Ok(None);
            }
        };

        let work_unit_key = WorkUnitKeys::unit(batch_id, unit_id);

        // Pre-flight check: read work unit status before transaction
        match self.client.get(work_unit_key.clone()).await? {
            Some(data) => match deserialize::<WorkUnit>(&data) {
                Ok(unit) => {
                    tracing::info!(
                        batch_id = %batch_id,
                        unit_id = %unit_id,
                        status = ?unit.status,
                        attempts = unit.attempts,
                        max_attempts = unit.max_attempts,
                        is_claimable = unit.is_claimable(),
                        "claim_work_unit: pre-flight work unit check"
                    );
                }
                Err(e) => {
                    tracing::error!(error = %e, "claim_work_unit: failed to deserialize work unit in pre-flight")
                }
            },
            None => {
                tracing::warn!(batch_id = %batch_id, unit_id = %unit_id, "claim_work_unit: work unit not found in pre-flight")
            }
        }

        // Use transaction helper for atomic claim operation
        let result = self
            .client
            .transactional_claim(
                work_unit_key.clone(),
                pending_key.clone(),
                worker_id,
                |data: &[u8]| -> std::result::Result<
                    Option<Vec<u8>>,
                    Box<dyn std::error::Error + Send + Sync>,
                > {
                    // Deserialize the work unit
                    let mut unit: WorkUnit = deserialize(data)
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

                    // Try to claim the work unit
                    if unit.claim(worker_id.to_string()).is_err() {
                        return Ok(None);
                    }

                    // Reserialize with updated state
                    let new_data = serialize(&unit)
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

                    Ok(Some(new_data))
                },
            )
            .await?;

        let Some(data) = result else {
            tracing::warn!(
                batch_id = %batch_id,
                unit_id = %unit_id,
                "claim_work_unit: transaction returned None - work unit may already be claimed"
            );
            return Ok(None);
        };

        tracing::info!(
            batch_id = %batch_id,
            unit_id = %unit_id,
            bytes = data.len(),
            "claim_work_unit: transaction succeeded, deserializing work unit"
        );

        // Deserialize the claimed work unit
        let unit: WorkUnit = match deserialize(&data) {
            Ok(u) => u,
            Err(e) => {
                tracing::error!(
                    batch_id = %batch_id,
                    unit_id = %unit_id,
                    error = %e,
                    "claim_work_unit: failed to deserialize work unit after claim"
                );
                return Err(TikvError::Deserialization(format!("work unit: {}", e)));
            }
        };

        tracing::info!(
            unit_id = %unit.id,
            batch_id = %unit.batch_id,
            worker_id = %worker_id,
            status = ?unit.status,
            "=== WORK UNIT CLAIMED SUCCESSFULLY ==="
        );

        Ok(Some(unit))
    }

    /// Complete a work unit.
    pub async fn complete_work_unit(
        &self,
        batch_id: &str,
        unit_id: &str,
    ) -> Result<bool, TikvError> {
        let key = WorkUnitKeys::unit(batch_id, unit_id);
        let data = self.client.get(key.clone()).await?;

        let data = match data {
            Some(d) => d,
            None => return Ok(false),
        };

        let mut unit: WorkUnit =
            bincode::deserialize(&data).map_err(|e| TikvError::Deserialization(e.to_string()))?;

        unit.complete();

        let new_data =
            bincode::serialize(&unit).map_err(|e| TikvError::Serialization(e.to_string()))?;
        self.client.put(key, new_data).await?;

        Ok(true)
    }

    /// Fail a work unit.
    ///
    /// This marks a work unit as failed. If retryable (attempts < max_attempts),
    /// it's added back to the pending queue for retry. The work unit is saved
    /// before adding to pending to prevent race conditions.
    pub async fn fail_work_unit(
        &self,
        batch_id: &str,
        unit_id: &str,
        error: String,
    ) -> Result<bool, TikvError> {
        let key = WorkUnitKeys::unit(batch_id, unit_id);
        let data = self.client.get(key.clone()).await?;

        let data = match data {
            Some(d) => d,
            None => return Ok(false),
        };

        let mut unit: WorkUnit =
            bincode::deserialize(&data).map_err(|e| TikvError::Deserialization(e.to_string()))?;

        unit.fail(error);

        // Save work unit state first (before adding to pending queue)
        // This prevents race condition where another worker claims from pending
        // before the failed state is persisted
        let new_data =
            bincode::serialize(&unit).map_err(|e| TikvError::Serialization(e.to_string()))?;
        self.client.put(key.clone(), new_data).await?;

        // If retryable, add back to pending queue AFTER saving
        // This ensures claimed workers always see the failed state
        if unit.status == WorkUnitStatus::Failed {
            let pending_key = WorkUnitKeys::pending(batch_id, unit_id);
            let pending_data = batch_id.as_bytes().to_vec();
            self.client.put(pending_key, pending_data).await?;
        }

        Ok(true)
    }
}

/// Summary of a batch job.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BatchSummary {
    /// Unique batch identifier.
    pub id: String,
    /// Human-readable batch name.
    pub name: String,
    /// Namespace for isolation.
    pub namespace: String,
    /// Current phase of the batch.
    pub phase: BatchPhase,
    /// Total number of files to process.
    pub files_total: u32,
    /// Number of files successfully completed.
    pub files_completed: u32,
    /// Number of files that failed.
    pub files_failed: u32,
    /// When the batch was created.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// When processing started (if started).
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    /// When processing completed (if completed).
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::StateLifecycle;
    use chrono::Utc;

    #[test]
    fn test_controller_config_default() {
        let config = ControllerConfig::default();
        assert_eq!(config.reconcile_interval, Duration::from_secs(5));
        assert_eq!(config.max_batches_per_loop, 100);
    }

    #[test]
    fn test_batch_summary_serialization() {
        let summary = BatchSummary {
            id: "test-batch".to_string(),
            name: "test".to_string(),
            namespace: "default".to_string(),
            phase: BatchPhase::Running,
            files_total: 1000,
            files_completed: 500,
            files_failed: 10,
            created_at: Utc::now(),
            started_at: Some(Utc::now()),
            completed_at: None,
        };

        let serialized = serde_json::to_string(&summary).unwrap();
        assert!(serialized.contains("Running"));
    }

    /// Phase workflow: Pending -> Discovering -> Running -> Merging -> Complete.
    /// The controller must NOT transition Running -> Complete; only the merge
    /// coordinator does Merging -> Complete after the merge finishes.
    #[test]
    fn test_phase_workflow_transitions() {
        // Pending -> Discovering: valid (controller/scanner)
        assert!(BatchPhase::Pending.can_transition_to(&BatchPhase::Discovering));

        // Discovering -> Running: valid (scanner after work units created)
        assert!(BatchPhase::Discovering.can_transition_to(&BatchPhase::Running));

        // Running -> Merging: valid (finalizer/merge coordinator claims merge)
        assert!(BatchPhase::Running.can_transition_to(&BatchPhase::Merging));

        // Running -> Complete: INVALID - controller must not skip merge
        assert!(!BatchPhase::Running.can_transition_to(&BatchPhase::Complete));

        // Merging -> Complete: valid (merge coordinator after merge done)
        assert!(BatchPhase::Merging.can_transition_to(&BatchPhase::Complete));
    }

    #[test]
    fn test_is_complete_requires_all_work_units_done() {
        let mut status = BatchStatus::new();
        assert!(!status.is_complete(), "empty status not complete");

        status.set_work_units_total(2);
        status.work_units_completed = 1;
        assert!(!status.is_complete(), "1/2 done not complete");

        status.work_units_completed = 2;
        assert!(status.is_complete(), "2/2 done is complete");

        status.work_units_completed = 1;
        status.work_units_failed = 1;
        assert!(
            status.is_complete(),
            "1 done + 1 failed = all done (batch should be Failed, not Complete)"
        );
    }

    /// When any work unit fails, the batch should transition to Failed, not Complete.
    #[test]
    fn test_any_failure_fails_batch() {
        let mut status = BatchStatus::new();
        status.set_work_units_total(10);
        status.work_units_completed = 9;
        status.work_units_failed = 1;
        assert!(status.is_complete(), "all 10 done");
        assert!(
            status.work_units_failed > 0,
            "1 failed -> batch should be Failed"
        );
    }
}
