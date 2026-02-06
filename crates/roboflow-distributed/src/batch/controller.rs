// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Batch job controller.
//!
//! The controller implements the reconciliation loop that drives the actual state
//! to match the desired state (BatchSpec). This is similar to Kubernetes controllers.

use super::key::{BatchIndexKeys, BatchKeys, WorkUnitKeys};
use super::spec::BatchSpec;
use super::status::{BatchPhase, BatchStatus, DiscoveryStatus};
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

    /// Reconcile all pending batch jobs.
    ///
    /// This scans for batch specs and reconciles each one.
    /// Returns an error if any batch failed to reconcile.
    pub async fn reconcile_all(&self) -> Result<(), TikvError> {
        // Scan for all batch specs
        let prefix = BatchKeys::specs_prefix();
        let specs = self
            .client
            .scan(prefix, self.config.max_batches_per_loop as u32)
            .await?;

        tracing::debug!(count = specs.len(), "Found batch specs to reconcile");

        let mut failed_batches = Vec::new();
        let mut first_error: Option<TikvError> = None;

        for (key, value) in specs {
            if let Err(e) = self.reconcile_batch(&key, &value).await {
                let key_str = String::from_utf8_lossy(&key).to_string();
                tracing::error!(
                    error = %e,
                    key = %key_str,
                    "Failed to reconcile batch"
                );
                failed_batches.push(key_str);
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

    /// Reconcile a single batch job.
    ///
    /// This reads the spec and status, then drives the state forward.
    async fn reconcile_batch(&self, _spec_key: &[u8], spec_data: &[u8]) -> Result<(), TikvError> {
        // Deserialize spec
        let spec: BatchSpec = serde_yaml::from_slice(spec_data)
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

        // Reconcile based on current phase
        let new_status = self.reconcile_phase(&spec, status).await?;

        // Save updated status
        self.save_status(&batch_id, &new_status).await?;

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
            BatchPhase::Running => {
                // Check if batch is complete
                self.reconcile_running(spec, status).await
            }
            BatchPhase::Merging => {
                // Merge in progress (handled by Finalizer)
                Ok(status)
            }
            BatchPhase::Complete | BatchPhase::Failed | BatchPhase::Cancelled => {
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

        for (key, value) in work_units {
            match bincode::deserialize::<WorkUnit>(&value) {
                Ok(unit) => match unit.status {
                    WorkUnitStatus::Complete => completed += 1,
                    WorkUnitStatus::Failed | WorkUnitStatus::Dead => failed += 1,
                    WorkUnitStatus::Processing => processing += 1,
                    _ => {}
                },
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

        // Update counts
        status.work_units_completed = completed;
        status.work_units_failed = failed;
        status.work_units_active = processing;
        status.files_completed = completed;
        status.files_failed = failed;
        status.files_active = processing;

        // Check if batch should be marked failed
        if status.should_fail(spec.spec.backoff_limit) {
            status.transition_to(BatchPhase::Failed);
            status.error = Some(format!(
                "Backoff limit exceeded: {} work units failed",
                status.work_units_failed
            ));
            return Ok(status);
        }

        // Check if all work units are complete
        if status.is_complete() {
            status.transition_to(BatchPhase::Complete);
            tracing::info!(
                batch_id = %batch_id,
                files_completed = status.files_completed,
                "Batch job completed successfully"
            );
        }

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
        let spec_data = serde_yaml::to_string(spec)
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
                let spec: BatchSpec = serde_yaml::from_slice(&bytes)
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

        status.transition_to(BatchPhase::Cancelled);
        self.save_status(batch_id, &status).await?;

        tracing::info!(batch_id = %batch_id, "Batch job cancelled");

        Ok(true)
    }

    /// Claim a work unit for a worker.
    ///
    /// This atomically claims a pending work unit and returns it.
    /// Uses a transaction to prevent race conditions.
    pub async fn claim_work_unit(&self, worker_id: &str) -> Result<Option<WorkUnit>, TikvError> {
        use bincode::{deserialize, serialize};

        // First, get a pending work unit key (outside transaction for scan)
        let pending_prefix_bytes = WorkUnitKeys::pending_prefix();
        let pending = self.client.scan(pending_prefix_bytes.clone(), 1).await?;

        if pending.is_empty() {
            return Ok(None);
        }

        let (pending_key, batch_id_bytes) = &pending[0];
        let batch_id = String::from_utf8_lossy(batch_id_bytes);

        // Extract unit_id from pending key
        // Reuse the same prefix_bytes to avoid duplicate function calls
        let pending_prefix = String::from_utf8_lossy(&pending_prefix_bytes);
        let pending_key_str = String::from_utf8_lossy(pending_key);
        let unit_id = match pending_key_str.strip_prefix(pending_prefix.as_ref()) {
            Some(id) => id,
            None => {
                tracing::warn!(
                    pending_key = %pending_key_str,
                    expected_prefix = %pending_prefix,
                    "Invalid pending key format"
                );
                return Ok(None);
            }
        };

        let work_unit_key = WorkUnitKeys::unit(&batch_id, unit_id);

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

        if result.is_none() {
            return Ok(None);
        }

        // Deserialize the claimed work unit
        let unit: WorkUnit = deserialize(&result.unwrap())
            .map_err(|e| TikvError::Deserialization(format!("work unit: {}", e)))?;

        tracing::debug!(
            unit_id = %unit.id,
            batch_id = %unit.batch_id,
            worker_id = %worker_id,
            "Work unit claimed"
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
            let pending_key = WorkUnitKeys::pending(unit_id);
            let pending_data = batch_id.as_bytes().to_vec();
            self.client.put(pending_key, pending_data).await?;
        }

        Ok(true)
    }
}

/// Summary of a batch job.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BatchSummary {
    pub id: String,
    pub name: String,
    pub namespace: String,
    pub phase: BatchPhase,
    pub files_total: u32,
    pub files_completed: u32,
    pub files_failed: u32,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
