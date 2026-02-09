// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Finalizer for completing merged batches.
//!
//! The finalizer monitors batches and triggers merge operations when all work units
//! are complete. After successful merge, it updates the batch status to Complete.

pub mod config;

use super::batch::{BatchController, BatchKeys, BatchPhase, BatchSpec, BatchStatus, BatchSummary};
use super::merge::MergeCoordinator;
use super::tikv::TikvClient;
use crate::tikv::TikvError;
use std::sync::Arc;
use tokio::time::sleep;
use tracing::{error, info, warn};

pub use config::FinalizerConfig;

/// Finalizer for completing merged batches.
///
/// Monitors batches and triggers merge operations when all work units are complete.
pub struct Finalizer {
    /// Pod ID for this finalizer.
    pod_id: String,

    /// TiKV client for distributed coordination.
    tikv: Arc<TikvClient>,

    /// Batch controller for reading batch state.
    batch_controller: Arc<BatchController>,

    /// Merge coordinator for executing merges.
    merge_coordinator: Arc<MergeCoordinator>,

    /// Finalizer configuration.
    config: FinalizerConfig,
}

impl Finalizer {
    /// Create a new finalizer.
    pub fn new(
        pod_id: String,
        tikv: Arc<TikvClient>,
        batch_controller: Arc<BatchController>,
        merge_coordinator: Arc<MergeCoordinator>,
        config: FinalizerConfig,
    ) -> Result<Self, TikvError> {
        Ok(Self {
            pod_id,
            tikv,
            batch_controller,
            merge_coordinator,
            config,
        })
    }

    /// Run the finalizer loop.
    ///
    /// This continuously polls for batches that need finalization and triggers
    /// the merge operation when all work units are complete.
    pub async fn run(&self, cancel: tokio_util::sync::CancellationToken) -> Result<(), TikvError> {
        info!(
            pod_id = %self.pod_id,
            "Finalizer started"
        );

        while !cancel.is_cancelled() {
            // Look for batches that are Running but all work units are complete
            if let Some((batch, spec)) = self.find_batch_awaiting_finalization().await {
                info!(
                    pod_id = %self.pod_id,
                    batch_id = %batch.id,
                    "Found batch awaiting finalization"
                );

                match self.finalize_batch(&batch, &spec).await {
                    Ok(true) => {
                        info!(
                            pod_id = %self.pod_id,
                            batch_id = %batch.id,
                            "Batch finalized successfully"
                        );
                    }
                    Ok(false) => {
                        // NotReady / NotClaimed / NotFound - will retry next poll
                    }
                    Err(e) => {
                        error!(
                            pod_id = %self.pod_id,
                            batch_id = %batch.id,
                            error = %e,
                            "Failed to finalize batch"
                        );

                        // Mark batch as failed, log if that also fails
                        if let Err(mark_err) = self
                            .mark_batch_failed(&batch.id, format!("Finalization failed: {}", e))
                            .await
                        {
                            error!(
                                pod_id = %self.pod_id,
                                batch_id = %batch.id,
                                error = %mark_err,
                                "CRITICAL: Failed to mark batch as failed - batch may be stuck in Running state"
                            );
                        }
                    }
                }
            }

            // Sleep before next poll
            sleep(self.config.poll_interval).await;
        }

        info!(
            pod_id = %self.pod_id,
            "Finalizer shutting down"
        );

        Ok(())
    }

    /// Find a batch that is Running but all work units are complete.
    ///
    /// Returns (BatchSummary, BatchSpec) if found.
    async fn find_batch_awaiting_finalization(&self) -> Option<(BatchSummary, BatchSpec)> {
        let batches = match self.batch_controller.list_batches().await {
            Ok(b) => b,
            Err(e) => {
                error!("Failed to list batches: {}", e);
                return None;
            }
        };

        for batch in batches {
            // Only process batches in Running phase
            if batch.phase != BatchPhase::Running {
                continue;
            }

            // Check if all work units are complete
            // Calculate total from completed + failed
            let total_done = batch.files_completed + batch.files_failed;
            tracing::debug!(
                batch_id = %batch.id,
                phase = ?batch.phase,
                files_total = batch.files_total,
                files_completed = batch.files_completed,
                files_failed = batch.files_failed,
                total_done = total_done,
                "Finalizer: evaluating batch"
            );
            if total_done >= batch.files_total && batch.files_total > 0 {
                // Get the spec to get output path
                match self.batch_controller.get_batch_spec(&batch.id).await {
                    Ok(Some(spec)) => return Some((batch, spec)),
                    Ok(None) => {
                        warn!(
                            batch_id = %batch.id,
                            "Batch appears complete but spec not found - skipping"
                        );
                    }
                    Err(e) => {
                        error!(
                            batch_id = %batch.id,
                            error = %e,
                            "Failed to get batch spec for completed batch"
                        );
                    }
                }
            }
        }

        None
    }

    /// Finalize a batch by triggering merge and updating status.
    ///
    /// Returns `Ok(true)` if the batch was merged and marked complete,
    /// `Ok(false)` if not ready / not claimed / not found (caller may retry),
    /// `Err` on failure.
    async fn finalize_batch(
        &self,
        batch: &BatchSummary,
        spec: &BatchSpec,
    ) -> Result<bool, TikvError> {
        info!(
            pod_id = %self.pod_id,
            batch_id = %batch.id,
            work_units = batch.files_total,
            "Finalizing batch"
        );

        let output_path = &spec.spec.output;

        // Try to claim merge
        let merge_result = self
            .merge_coordinator
            .try_claim_merge(&batch.id, 1, output_path.clone())
            .await?;

        match merge_result {
            super::merge::MergeResult::Success {
                output_path,
                total_frames,
            } => {
                info!(
                    pod_id = %self.pod_id,
                    batch_id = %batch.id,
                    output_path = %output_path,
                    total_frames,
                    "Merge completed successfully"
                );

                // Mark batch as complete
                self.mark_batch_complete(&batch.id).await?;
                Ok(true)
            }
            super::merge::MergeResult::NotFound => {
                warn!(
                    pod_id = %self.pod_id,
                    batch_id = %batch.id,
                    "Batch not found for merge"
                );
                Ok(false)
            }
            super::merge::MergeResult::NotClaimed => {
                warn!(
                    pod_id = %self.pod_id,
                    batch_id = %batch.id,
                    "Another finalizer claimed the merge"
                );
                Ok(false)
            }
            super::merge::MergeResult::NotReady => {
                warn!(
                    pod_id = %self.pod_id,
                    batch_id = %batch.id,
                    "Merge not ready, will retry"
                );
                Ok(false)
            }
            super::merge::MergeResult::Failed { error } => {
                Err(TikvError::Other(format!("Merge failed: {}", error)))
            }
        }
    }

    /// Mark a batch as complete.
    async fn mark_batch_complete(&self, batch_id: &str) -> Result<(), TikvError> {
        let key = BatchKeys::status(batch_id);
        let data = self.tikv.get(key.clone()).await?;

        let mut status: BatchStatus = match data {
            Some(d) => bincode::deserialize(&d)
                .map_err(|e| TikvError::Deserialization(format!("batch status: {}", e)))?,
            None => return Err(TikvError::Other("Batch status not found".to_string())),
        };

        let old_phase = status.phase;
        status.transition_to(BatchPhase::Complete);

        let new_data =
            bincode::serialize(&status).map_err(|e| TikvError::Serialization(e.to_string()))?;
        self.tikv.put(key, new_data).await?;

        // Update phase index
        super::batch::update_phase_index(&self.tikv, batch_id, old_phase, BatchPhase::Complete)
            .await?;

        info!(batch_id = %batch_id, "Batch marked complete");

        Ok(())
    }

    /// Mark a batch as failed.
    async fn mark_batch_failed(&self, batch_id: &str, error: String) -> Result<(), TikvError> {
        let key = BatchKeys::status(batch_id);
        let data = self.tikv.get(key.clone()).await?;

        let mut status: BatchStatus = match data {
            Some(d) => bincode::deserialize(&d)
                .map_err(|e| TikvError::Deserialization(format!("batch status: {}", e)))?,
            None => return Err(TikvError::Other("Batch status not found".to_string())),
        };

        let old_phase = status.phase;
        status.transition_to(BatchPhase::Failed);
        status.error = Some(error);

        let new_data =
            bincode::serialize(&status).map_err(|e| TikvError::Serialization(e.to_string()))?;
        self.tikv.put(key, new_data).await?;

        // Update phase index
        super::batch::update_phase_index(&self.tikv, batch_id, old_phase, BatchPhase::Failed)
            .await?;

        info!(batch_id = %batch_id, "Batch marked failed");

        Ok(())
    }
}
