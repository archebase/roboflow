// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Coordinator for distributed work unit management.
//!
//! This module handles coordination logic separated from execution:
//! - Finding and claiming work units
//! - Completing or failing work units
//! - Heartbeat management
//! - Shutdown handling

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::time::sleep;

use super::config::WorkerConfig;
use super::executor::TaskExecutor;
use super::metrics::{ProcessingResult, WorkerMetrics};
use crate::batch::{BatchController, WorkUnit};
use crate::shutdown::ShutdownHandler;
use crate::tikv::{
    TikvError,
    client::TikvClient,
    schema::{HeartbeatRecord, WorkerStatus},
};

/// Coordinator for managing distributed work.
///
/// The coordinator is responsible for:
/// - Claiming work units from the batch queue
/// - Delegating execution to the TaskExecutor
/// - Reporting results back to TiKV
/// - Managing heartbeats and shutdown
pub struct Coordinator {
    /// Unique identifier for this worker instance.
    pod_id: String,
    /// TiKV client for coordination.
    tikv: Arc<TikvClient>,
    /// Worker configuration.
    config: WorkerConfig,
    /// Worker metrics.
    metrics: Arc<WorkerMetrics>,
    /// Shutdown handler.
    shutdown_handler: ShutdownHandler,
    /// Batch controller for work unit operations.
    batch_controller: BatchController,
}

impl Coordinator {
    /// Create a new coordinator.
    pub fn new(
        pod_id: impl Into<String>,
        tikv: Arc<TikvClient>,
        config: WorkerConfig,
    ) -> Result<Self, TikvError> {
        let batch_controller = BatchController::with_client(tikv.clone());

        Ok(Self {
            pod_id: pod_id.into(),
            tikv,
            config,
            metrics: Arc::new(WorkerMetrics::new()),
            shutdown_handler: ShutdownHandler::new(),
            batch_controller,
        })
    }

    /// Get the pod ID.
    pub fn pod_id(&self) -> &str {
        &self.pod_id
    }

    /// Get the metrics.
    pub fn metrics(&self) -> &WorkerMetrics {
        &self.metrics
    }

    /// Get the configuration.
    pub fn config(&self) -> &WorkerConfig {
        &self.config
    }

    /// Check if shutdown has been requested.
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_handler.is_requested()
    }

    /// Request shutdown.
    pub fn shutdown(&self) -> Result<(), TikvError> {
        self.shutdown_handler.shutdown();
        Ok(())
    }

    /// Find and claim a work unit.
    ///
    /// Returns the claimed work unit or None if no work is available.
    pub async fn claim_work(&self) -> Result<Option<WorkUnit>, TikvError> {
        match self.batch_controller.claim_work_unit(&self.pod_id).await {
            Ok(Some(unit)) => {
                self.metrics.inc_jobs_claimed();
                self.metrics.inc_active_jobs();
                tracing::info!(
                    pod_id = %self.pod_id,
                    unit_id = %unit.id,
                    batch_id = %unit.batch_id,
                    files = unit.files.len(),
                    "Work unit claimed"
                );
                Ok(Some(unit))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                tracing::warn!(pod_id = %self.pod_id, error = %e, "Failed to claim work unit");
                Err(e)
            }
        }
    }

    /// Complete a work unit.
    pub async fn complete_work(&self, batch_id: &str, unit_id: &str) -> Result<(), TikvError> {
        match self
            .batch_controller
            .complete_work_unit(batch_id, unit_id)
            .await
        {
            Ok(true) => {
                self.metrics.inc_jobs_completed();
                self.metrics.dec_active_jobs();
                tracing::info!(
                    pod_id = %self.pod_id,
                    unit_id = %unit_id,
                    batch_id = %batch_id,
                    "Work unit completed"
                );
                Ok(())
            }
            Ok(false) => {
                tracing::warn!(
                    pod_id = %self.pod_id,
                    unit_id = %unit_id,
                    batch_id = %batch_id,
                    "Work unit not found for completion"
                );
                self.metrics.dec_active_jobs();
                Ok(())
            }
            Err(e) => {
                tracing::error!(
                    pod_id = %self.pod_id,
                    unit_id = %unit_id,
                    batch_id = %batch_id,
                    error = %e,
                    "Failed to complete work unit"
                );
                Err(e)
            }
        }
    }

    /// Fail a work unit with an error message.
    pub async fn fail_work(
        &self,
        batch_id: &str,
        unit_id: &str,
        error: String,
    ) -> Result<(), TikvError> {
        match self
            .batch_controller
            .fail_work_unit(batch_id, unit_id, error.clone())
            .await
        {
            Ok(true) => {
                self.metrics.inc_jobs_failed();
                self.metrics.dec_active_jobs();
                tracing::warn!(
                    pod_id = %self.pod_id,
                    unit_id = %unit_id,
                    batch_id = %batch_id,
                    error = %error,
                    "Work unit failed"
                );
                Ok(())
            }
            Ok(false) => {
                tracing::warn!(
                    pod_id = %self.pod_id,
                    unit_id = %unit_id,
                    batch_id = %batch_id,
                    "Work unit not found for failure"
                );
                self.metrics.dec_active_jobs();
                Ok(())
            }
            Err(e) => {
                tracing::error!(
                    pod_id = %self.pod_id,
                    unit_id = %unit_id,
                    batch_id = %batch_id,
                    error = %e,
                    "Failed to mark work unit as failed"
                );
                Err(e)
            }
        }
    }

    /// Send heartbeat to TiKV.
    pub async fn send_heartbeat(&self) -> Result<(), TikvError> {
        let active = self.metrics.active_jobs.load(Ordering::Relaxed) as u32;
        let total_processed = self.metrics.jobs_completed.load(Ordering::Relaxed);

        let mut heartbeat = self
            .tikv
            .get_heartbeat(&self.pod_id)
            .await?
            .unwrap_or_else(|| HeartbeatRecord::new(self.pod_id.clone()));

        heartbeat.beat();
        heartbeat.active_jobs = active;
        heartbeat.total_processed = total_processed;
        heartbeat.status = if active > 0 {
            WorkerStatus::Busy
        } else {
            WorkerStatus::Idle
        };

        self.tikv.update_heartbeat(&self.pod_id, &heartbeat).await?;

        tracing::debug!(
            pod_id = %self.pod_id,
            active_jobs = active,
            total_processed = total_processed,
            status = ?heartbeat.status,
            "Heartbeat sent"
        );

        Ok(())
    }

    /// Send final draining heartbeat on shutdown.
    pub async fn send_draining_heartbeat(&self) -> Result<(), TikvError> {
        let mut heartbeat = self
            .tikv
            .get_heartbeat(&self.pod_id)
            .await?
            .unwrap_or_else(|| HeartbeatRecord::new(self.pod_id.clone()));

        heartbeat.beat();
        heartbeat.status = WorkerStatus::Draining;

        // Add shutdown metadata
        if let Some(ref mut metadata) = heartbeat.metadata
            && let Some(obj) = metadata.as_object_mut()
        {
            obj.insert(
                "shutdown_at".to_string(),
                serde_json::json!(chrono::Utc::now().to_rfc3339()),
            );
            obj.insert("reason".to_string(), serde_json::json!("graceful_shutdown"));
        }

        self.tikv.update_heartbeat(&self.pod_id, &heartbeat).await?;

        tracing::info!(
            pod_id = %self.pod_id,
            "Final draining heartbeat sent"
        );

        Ok(())
    }

    /// Run the main coordination loop.
    ///
    /// This continuously:
    /// 1. Checks for shutdown signal
    /// 2. Claims work units (if under capacity)
    /// 3. Delegates execution to the executor
    /// 4. Reports results
    /// 5. Sends periodic heartbeats
    pub async fn run(&mut self, executor: &TaskExecutor) -> Result<(), TikvError> {
        // Start signal handler
        let mut shutdown_rx = self.shutdown_handler.start_signal_handler();
        let shutdown_tx = self.shutdown_handler.sender();

        // Start heartbeat task
        let tikv = self.tikv.clone();
        let pod_id = self.pod_id.clone();
        let metrics = self.metrics.clone();
        let heartbeat_interval = self.config.heartbeat_interval;
        let mut heartbeat_rx = shutdown_tx.subscribe();

        let heartbeat_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(heartbeat_interval);
            interval.tick().await; // Skip first tick

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = send_heartbeat_inner(&tikv, &pod_id, &metrics).await {
                            metrics.inc_heartbeat_errors();
                            tracing::error!(pod_id = %pod_id, error = %e, "Heartbeat failed");
                        }
                    }
                    _ = heartbeat_rx.recv() => {
                        tracing::info!(pod_id = %pod_id, "Heartbeat task shutting down");
                        break;
                    }
                }
            }
        });

        tracing::info!(
            pod_id = %self.pod_id,
            max_concurrent_jobs = self.config.max_concurrent_jobs,
            poll_interval_secs = self.config.poll_interval.as_secs(),
            "Starting coordinator"
        );

        // Main loop
        loop {
            let active_count = self.metrics.active_jobs.load(Ordering::Relaxed) as usize;

            if active_count < self.config.max_concurrent_jobs {
                // Try to claim work
                let claimed_unit = match self.claim_work().await {
                    Ok(Some(unit)) => Some(unit),
                    Ok(None) => None,
                    Err(e) => {
                        tracing::error!(pod_id = %self.pod_id, error = %e, "Failed to claim work");
                        None
                    }
                };

                if let Some(unit) = claimed_unit {
                    let unit_id = unit.id.clone();
                    let batch_id = unit.batch_id.clone();

                    // Check for shutdown before processing
                    if self.shutdown_handler.is_requested() {
                        tracing::info!(pod_id = %self.pod_id, "Shutdown requested, releasing work");
                        self.release_on_shutdown(&batch_id, &unit_id).await;
                        break;
                    }

                    // Execute the work unit
                    let result = executor.execute(&unit).await;

                    // Handle result
                    match result {
                        ProcessingResult::Success => {
                            if let Err(e) = self.complete_work(&batch_id, &unit_id).await {
                                tracing::error!(
                                    pod_id = %self.pod_id,
                                    unit_id = %unit_id,
                                    error = %e,
                                    "Failed to complete work unit"
                                );
                                self.metrics.inc_processing_errors();
                            }
                        }
                        ProcessingResult::Failed { error } => {
                            if error.contains("interrupted by shutdown") {
                                tracing::info!(pod_id = %self.pod_id, "Work interrupted by shutdown");
                                self.release_on_shutdown(&batch_id, &unit_id).await;
                                break;
                            }

                            if let Err(e) = self.fail_work(&batch_id, &unit_id, error).await {
                                tracing::error!(
                                    pod_id = %self.pod_id,
                                    unit_id = %unit_id,
                                    error = %e,
                                    "Failed to mark work unit as failed"
                                );
                                self.metrics.inc_processing_errors();
                            }
                        }
                        ProcessingResult::Cancelled => {
                            tracing::info!(
                                pod_id = %self.pod_id,
                                unit_id = %unit_id,
                                "Work unit cancelled"
                            );
                        }
                    }
                } else {
                    // No work available - wait with shutdown handling
                    tokio::select! {
                        _ = sleep(self.config.poll_interval) => {}
                        _ = shutdown_rx.recv() => {
                            tracing::info!(pod_id = %self.pod_id, "Shutdown requested while idle");
                            break;
                        }
                    }
                }
            } else {
                // At capacity - brief sleep with shutdown handling
                tokio::select! {
                    _ = sleep(Duration::from_millis(100)) => {}
                    _ = shutdown_rx.recv() => {
                        tracing::info!(pod_id = %self.pod_id, "Shutdown requested at capacity");
                        break;
                    }
                }
            }
        }

        // Wait for heartbeat task
        let _ = heartbeat_handle.await;

        // Send final draining heartbeat
        if let Err(e) = self.send_draining_heartbeat().await {
            tracing::error!(pod_id = %self.pod_id, error = %e, "Failed to send draining heartbeat");
        }

        tracing::info!(pod_id = %self.pod_id, "Coordinator stopped gracefully");

        Ok(())
    }

    /// Release a work unit back to pending on shutdown.
    async fn release_on_shutdown(&self, batch_id: &str, unit_id: &str) {
        if let Err(e) = self
            .batch_controller
            .fail_work_unit(
                batch_id,
                unit_id,
                "Shutdown requested, releasing back to Pending".to_string(),
            )
            .await
        {
            tracing::error!(
                pod_id = %self.pod_id,
                unit_id = %unit_id,
                error = %e,
                "Failed to release work unit during shutdown"
            );
        }
        self.metrics.dec_active_jobs();
    }
}

/// Send heartbeat (inner function for use in spawned task).
pub async fn send_heartbeat_inner(
    tikv: &TikvClient,
    pod_id: &str,
    metrics: &WorkerMetrics,
) -> Result<(), TikvError> {
    let active = metrics.active_jobs.load(Ordering::Relaxed) as u32;
    let total_processed = metrics.jobs_completed.load(Ordering::Relaxed);

    let mut heartbeat = tikv
        .get_heartbeat(pod_id)
        .await?
        .unwrap_or_else(|| HeartbeatRecord::new(pod_id.to_string()));

    heartbeat.beat();
    heartbeat.active_jobs = active;
    heartbeat.total_processed = total_processed;
    heartbeat.status = if active > 0 {
        WorkerStatus::Busy
    } else {
        WorkerStatus::Idle
    };

    tikv.update_heartbeat(pod_id, &heartbeat).await?;
    Ok(())
}
