// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Worker actor for claiming and processing work units from TiKV batch queue.

mod config;
mod heartbeat;
mod metrics;
mod registry;

pub use config::{
    DEFAULT_CHECKPOINT_INTERVAL_FRAMES, DEFAULT_CHECKPOINT_INTERVAL_SECS,
    DEFAULT_HEARTBEAT_INTERVAL_SECS, DEFAULT_JOB_TIMEOUT_SECS, DEFAULT_MAX_ATTEMPTS,
    DEFAULT_MAX_CONCURRENT_JOBS, DEFAULT_POLL_INTERVAL_SECS, WorkerConfig,
};
pub use metrics::{ProcessingResult, WorkerMetrics, WorkerMetricsSnapshot};

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::batch::{BatchController, WorkUnit};
use super::shutdown::ShutdownHandler;
use super::tikv::{
    TikvError,
    client::TikvClient,
    schema::{HeartbeatRecord, WorkerStatus},
};
use tokio::sync::{Mutex, RwLock};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use lru::LruCache;

// Dataset conversion imports
use roboflow_dataset::lerobot::LerobotConfig;

// Pipeline imports (unified executor from roboflow-dataset)
use roboflow_dataset::streaming::config::StreamingConfig;
use roboflow_dataset::{PipelineConfig, PipelineExecutor};
use roboflow_sources::{SourceConfig, create_source};

// Re-export module items for use within the worker module
pub use heartbeat::send_heartbeat_inner;
pub use registry::JobRegistry;

/// Default cancellation check interval in seconds.
pub const DEFAULT_CANCELLATION_CHECK_INTERVAL_SECS: u64 = 5;

/// Worker actor for claiming and processing work units.
pub struct Worker {
    pod_id: String,
    tikv: Arc<TikvClient>,
    config: WorkerConfig,
    metrics: Arc<WorkerMetrics>,
    shutdown_handler: ShutdownHandler,
    cancellation_token: Arc<CancellationToken>,
    job_registry: Arc<RwLock<JobRegistry>>,
    config_cache: Arc<Mutex<LruCache<String, roboflow_dataset::lerobot::LerobotConfig>>>,
    batch_controller: BatchController,
}

impl Worker {
    pub fn new(
        pod_id: impl Into<String>,
        tikv: Arc<TikvClient>,
        config: WorkerConfig,
    ) -> Result<Self, TikvError> {
        let pod_id = pod_id.into();

        // Create batch controller for work unit processing
        let batch_controller = BatchController::with_client(tikv.clone());

        Ok(Self {
            pod_id,
            tikv,
            config,
            metrics: Arc::new(WorkerMetrics::new()),
            shutdown_handler: ShutdownHandler::new(),
            cancellation_token: Arc::new(CancellationToken::new()),
            job_registry: Arc::new(RwLock::new(JobRegistry::default())),
            config_cache: Arc::new(Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(100).unwrap(), // Cache up to 100 configs
            ))),
            batch_controller,
        })
    }

    /// Get the pod ID.
    pub fn pod_id(&self) -> &str {
        &self.pod_id
    }

    /// Get a reference to the metrics.
    pub fn metrics(&self) -> &WorkerMetrics {
        &self.metrics
    }

    /// Get a reference to the configuration.
    pub fn config(&self) -> &WorkerConfig {
        &self.config
    }

    /// Generate a unique pod ID.
    ///
    /// Uses hostname + UUID, or K8s pod name from environment if available.
    pub fn generate_pod_id() -> String {
        // Check for K8s pod name first
        if let Ok(pod_name) = std::env::var("POD_NAME") {
            return pod_name;
        }

        // Fall back to hostname + UUID
        let hostname = gethostname::gethostname()
            .to_str()
            .unwrap_or("unknown")
            .to_string();

        let uuid = uuid::Uuid::new_v4();
        format!("{}-{}", hostname, uuid)
    }

    /// Find and claim a work unit from batch jobs.
    ///
    /// This allows workers to process work units from batch job submissions.
    /// Returns the claimed work unit or None if no work units are available.
    async fn find_and_claim_work_unit(&self) -> Result<Option<WorkUnit>, TikvError> {
        match self.batch_controller.claim_work_unit(&self.pod_id).await {
            Ok(Some(unit)) => {
                self.metrics.inc_jobs_claimed();
                self.metrics.inc_active_jobs();
                tracing::info!(
                    pod_id = %self.pod_id,
                    unit_id = %unit.id,
                    batch_id = %unit.batch_id,
                    files = unit.files.len(),
                    "Work unit claimed successfully"
                );
                Ok(Some(unit))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                tracing::warn!(
                    pod_id = %self.pod_id,
                    error = %e,
                    "Failed to claim work unit"
                );
                Err(e)
            }
        }
    }

    /// Process a work unit using the new Pipeline API.
    ///
    /// This method uses the unified PipelineExecutor for dataset conversion.
    async fn process_work_unit_with_pipeline(&self, unit: &WorkUnit) -> ProcessingResult {
        use std::collections::HashMap;
        use std::sync::Arc;

        tracing::info!(
            pod_id = %self.pod_id,
            unit_id = %unit.id,
            batch_id = %unit.batch_id,
            files = unit.files.len(),
            "Processing work unit with PipelineExecutor"
        );

        // Get the primary source file
        let source_url = if let Some(url) = unit.primary_source() {
            url
        } else {
            let error_msg = format!("Work unit {} has no primary source", unit.id);
            tracing::error!(unit_id = %unit.id, "No primary source");
            return ProcessingResult::Failed { error: error_msg };
        };

        let output_path = self.build_output_path(unit);
        let unit_id = unit.id.clone();

        // Check for existing checkpoint
        // NOTE: Checkpoint resumption is not yet fully implemented.
        // The PipelineExecutor doesn't support starting from a specific frame offset.
        // When a checkpoint exists, we log it but processing will start from frame 0.
        let _checkpoint_frame = match self.tikv.get_checkpoint(&unit_id).await {
            Ok(Some(checkpoint)) => {
                tracing::warn!(
                    pod_id = %self.pod_id,
                    unit_id = %unit_id,
                    last_frame = checkpoint.last_frame,
                    "Found checkpoint but PipelineExecutor doesn't support resuming from offset. \
                     Starting from frame 0."
                );
                Some(checkpoint.last_frame)
            }
            Ok(None) => {
                tracing::debug!(unit_id = %unit_id, "No checkpoint, starting fresh");
                None
            }
            Err(e) => {
                tracing::warn!(unit_id = %unit_id, error = %e, "Failed to get checkpoint");
                None
            }
        };

        // Load LeRobot config
        let lerobot_config = match self.create_lerobot_config(unit).await {
            Ok(config) => config,
            Err(e) => {
                let error_msg = format!("Failed to load config for work unit {}: {}", unit.id, e);
                tracing::error!(unit_id = %unit.id, error = %e, "Config load failed");
                return ProcessingResult::Failed { error: error_msg };
            }
        };

        // Create source config from input file
        let source_config = if source_url.ends_with(".mcap") {
            SourceConfig::mcap(source_url)
        } else if source_url.ends_with(".bag") {
            SourceConfig::bag(source_url)
        } else if source_url.ends_with(".rrd") {
            SourceConfig::rrd(source_url)
        } else {
            SourceConfig::mcap(source_url)
        };

        // Determine if we need cloud storage
        let (has_cloud_storage, storage, output_prefix) =
            if output_path.starts_with("s3://") || output_path.starts_with("oss://") {
                use std::str::FromStr;
                let output_path_str = output_path.to_string_lossy().to_string();
                let storage: Arc<dyn roboflow_storage::Storage> =
                    match roboflow_storage::StorageFactory::from_env().create(&output_path_str) {
                        Ok(s) => s,
                        Err(e) => {
                            return ProcessingResult::Failed {
                                error: format!("Failed to create storage: {}", e),
                            };
                        }
                    };
                let storage_url = match roboflow_storage::StorageUrl::from_str(&output_path_str) {
                    Ok(url) => url,
                    Err(_) => {
                        return ProcessingResult::Failed {
                            error: format!("Failed to parse storage URL: {}", output_path_str),
                        };
                    }
                };
                let prefix = storage_url.path().trim_end_matches('/').to_string();
                (true, storage, Some(prefix))
            } else {
                let local_storage: Arc<dyn roboflow_storage::Storage> =
                    Arc::new(roboflow_storage::LocalStorage::new(&output_path));
                (false, local_storage, None)
            };

        // Create the source
        let source = match create_source(&source_config) {
            Ok(s) => s,
            Err(e) => {
                return ProcessingResult::Failed {
                    error: format!("Failed to create source: {}", e),
                };
            }
        };

        // Create the writer - use LerobotWriter directly for PipelineExecutor
        let writer = if has_cloud_storage {
            let prefix = output_prefix.as_deref().unwrap_or_default();
            match roboflow_dataset::lerobot::LerobotWriter::new(
                storage.clone(),
                prefix.to_string(),
                &output_path,
                lerobot_config.clone(),
            ) {
                Ok(w) => w,
                Err(e) => {
                    return ProcessingResult::Failed {
                        error: format!("Failed to create writer: {}", e),
                    };
                }
            }
        } else {
            match roboflow_dataset::lerobot::LerobotWriter::new_local(
                &output_path,
                lerobot_config.clone(),
            ) {
                Ok(w) => w,
                Err(e) => {
                    return ProcessingResult::Failed {
                        error: format!("Failed to create writer: {}", e),
                    };
                }
            }
        };

        // Build topic mappings from config
        let mut topic_mappings = HashMap::new();
        for mapping in &lerobot_config.mappings {
            topic_mappings.insert(mapping.topic.clone(), mapping.feature.clone());
        }

        // Create pipeline configuration with streaming settings
        let frame_interval_ns = 1_000_000_000u64 / lerobot_config.dataset.fps as u64;
        let completion_window_ns = frame_interval_ns * 3;

        let mut streaming_config = StreamingConfig::with_fps(lerobot_config.dataset.fps);
        streaming_config.completion_window_ns = completion_window_ns;

        let pipeline_config =
            PipelineConfig::new(streaming_config).with_topic_mappings(topic_mappings);

        // Create cancellation token
        let cancel_token = self.cancellation_token.child_token();
        let cancel_token_for_monitor = Arc::new(cancel_token.clone());

        // Register with cancellation monitor
        {
            let mut registry = self.job_registry.write().await;
            registry.register(unit_id.clone(), cancel_token_for_monitor);
        }

        // Create pipeline executor with concrete writer type
        let mut executor = PipelineExecutor::new(writer, pipeline_config);

        // Run with timeout
        const CONVERSION_TIMEOUT: Duration = Duration::from_secs(3600);

        let unit_id_clone = unit_id.clone();
        let job_registry_for_cleanup = self.job_registry.clone();
        let cancel_token_for_timeout = cancel_token.clone();

        let pipeline_task = tokio::task::spawn(async move {
            let _guard = cancel_token.clone().drop_guard();

            // Initialize source
            let mut source = source;
            let _ = source.initialize(&source_config).await;

            // Process messages from source
            let batch_size = 1000;
            loop {
                // Check for cancellation
                if cancel_token.is_cancelled() {
                    return Err(roboflow_core::RoboflowError::other(
                        "Interrupted by shutdown".to_string(),
                    ));
                }

                match source.read_batch(batch_size).await {
                    Ok(Some(messages)) if !messages.is_empty() => {
                        for msg in messages {
                            executor.process_message(msg)?;
                        }
                    }
                    Ok(Some(_)) => {
                        // Empty batch, continue
                        continue;
                    }
                    Ok(None) => {
                        // End of stream
                        break;
                    }
                    Err(e) => {
                        return Err(roboflow_core::RoboflowError::other(format!(
                            "Source read failed: {}",
                            e
                        )));
                    }
                }
            }

            // Finalize and get stats
            executor.finalize()
        });

        let result = match tokio::time::timeout(CONVERSION_TIMEOUT, pipeline_task).await {
            Ok(Ok(Ok(_stats))) => {
                let mut registry = job_registry_for_cleanup.write().await;
                registry.unregister(&unit_id_clone);
                ProcessingResult::Success
            }
            Ok(Ok(Err(e))) => {
                let mut registry = job_registry_for_cleanup.write().await;
                registry.unregister(&unit_id_clone);

                ProcessingResult::Failed {
                    error: format!(
                        "Pipeline execution failed for work unit {}: {}",
                        unit_id_clone, e
                    ),
                }
            }
            Ok(Err(join_err)) => {
                let mut registry = job_registry_for_cleanup.write().await;
                registry.unregister(&unit_id_clone);

                if join_err.is_cancelled() {
                    ProcessingResult::Cancelled
                } else {
                    ProcessingResult::Failed {
                        error: format!(
                            "Pipeline task panicked for work unit {}: {}",
                            unit_id_clone, join_err
                        ),
                    }
                }
            }
            Err(_) => {
                let mut registry = job_registry_for_cleanup.write().await;
                registry.unregister(&unit_id_clone);

                cancel_token_for_timeout.cancel();
                let error_msg = format!("Pipeline timed out for work unit {}", unit_id_clone);
                tracing::error!(unit_id = %unit_id_clone, "Pipeline timed out");
                return ProcessingResult::Failed { error: error_msg };
            }
        };

        tracing::info!(
            unit_id = %unit.id,
            "Work unit complete with PipelineExecutor"
        );

        result
    }

    /// Complete a work unit.
    async fn complete_work_unit(&self, batch_id: &str, unit_id: &str) -> Result<(), TikvError> {
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
                    "Work unit completed successfully"
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
    async fn fail_work_unit(
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
}

impl Worker {
    /// Build the output path for a work unit.
    ///
    /// Uses the output_path specified in the Batch/WorkUnit from submit.
    /// Falls back to a default pattern if not set.
    fn build_output_path(&self, unit: &WorkUnit) -> PathBuf {
        // Use the output_path from the WorkUnit (specified during submit)
        // If empty, fall back to legacy pattern
        if !unit.output_path.is_empty() {
            PathBuf::from(&unit.output_path)
        } else {
            // Fallback: {output_prefix}/{unit_id}/
            PathBuf::from(format!(
                "{}/{}",
                self.config.output_prefix.trim_end_matches('/'),
                unit.id
            ))
        }
    }

    /// Create a LeRobot configuration for processing a work unit.
    ///
    /// Loads the configuration from TiKV using the config_hash stored in the work unit.
    /// Uses an LRU cache to reduce TiKV round-trips for frequently used configs.
    async fn create_lerobot_config(&self, unit: &WorkUnit) -> Result<LerobotConfig, TikvError> {
        let config_hash = &unit.config_hash;

        // Empty config_hash is a critical error - without mappings, the pipeline
        // will produce no frames, which is not a valid outcome
        if config_hash.is_empty() || config_hash == "default" {
            let error_msg = format!(
                "Work unit {} has no valid config_hash (config_hash is empty or 'default'). \
                 This indicates a bug in the batch submission - config_hash must reference \
                 a valid configuration stored in TiKV.",
                unit.id
            );
            tracing::error!(
                pod_id = %self.pod_id,
                unit_id = %unit.id,
                config_hash = %config_hash,
                "Invalid config_hash - failing work unit"
            );
            return Err(TikvError::Other(error_msg));
        }

        // Check cache first
        {
            let mut cache = self.config_cache.lock().await;
            if let Some(config) = cache.get(config_hash) {
                tracing::debug!(
                    pod_id = %self.pod_id,
                    unit_id = %unit.id,
                    config_hash = %config_hash,
                    "Loaded config from cache"
                );
                return Ok(config.clone());
            }
        }

        // Cache miss - fetch from TiKV
        let config = match self.tikv.get_config(config_hash).await {
            Ok(Some(record)) => {
                tracing::info!(
                    pod_id = %self.pod_id,
                    unit_id = %unit.id,
                    config_hash = %config_hash,
                    "Loaded config from TiKV"
                );
                LerobotConfig::from_toml(&record.content)
                    .map_err(|e| TikvError::Other(format!("Failed to parse config TOML: {}", e)))?
            }
            Ok(None) => {
                // Config not found in TiKV - this is a critical error
                tracing::error!(
                    pod_id = %self.pod_id,
                    unit_id = %unit.id,
                    config_hash = %config_hash,
                    "Config not found in TiKV"
                );
                return Err(TikvError::Other(format!(
                    "Config '{}' not found in TiKV for work unit {}",
                    config_hash, unit.id
                )));
            }
            Err(e) => {
                tracing::error!(
                    pod_id = %self.pod_id,
                    unit_id = %unit.id,
                    config_hash = %config_hash,
                    error = %e,
                    "Failed to fetch config from TiKV"
                );
                return Err(TikvError::Other(format!(
                    "Failed to fetch config '{}' from TiKV: {}",
                    config_hash, e
                )));
            }
        };

        // Store in cache for future use
        {
            let mut cache = self.config_cache.lock().await;
            cache.put(config_hash.clone(), config.clone());
        }

        Ok(config)
    }

    /// Send heartbeat to TiKV.
    ///
    /// This is a public method that can be called externally to trigger
    /// a heartbeat update. It's also called automatically by the worker's
    /// background task during normal operation.
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

    /// Shutdown the worker gracefully.
    pub fn shutdown(&self) -> Result<(), TikvError> {
        self.shutdown_handler.shutdown();
        Ok(())
    }

    /// Check if shutdown has been requested.
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_handler.is_requested()
    }
}

impl Worker {
    /// Run the worker loop.
    ///
    /// This will continuously:
    /// 1. Check for shutdown signal
    /// 2. Find and claim a work unit (if under concurrent limit)
    /// 3. Process the work unit
    /// 4. Complete or fail the work unit
    /// 5. Send periodic heartbeats
    /// 6. Repeat until shutdown
    pub async fn run(&mut self) -> Result<(), TikvError> {
        // Start signal handler for SIGTERM/SIGINT
        let mut shutdown_rx = self.shutdown_handler.start_signal_handler();
        let shutdown_tx = self.shutdown_handler.sender();

        // Start heartbeat task
        let tikv = self.tikv.clone();
        let pod_id_for_heartbeat = self.pod_id.clone();
        let metrics = self.metrics.clone();
        let heartbeat_interval = self.config.heartbeat_interval;
        let mut heartbeat_rx = shutdown_tx.subscribe();

        let heartbeat_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(heartbeat_interval);
            interval.tick().await; // Skip first tick

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = send_heartbeat_inner(&tikv, &pod_id_for_heartbeat, &metrics).await {
                            metrics.inc_heartbeat_errors();
                            tracing::error!(
                                pod_id = %pod_id_for_heartbeat,
                                error = %e,
                                "Failed to send heartbeat"
                            );
                        }
                    }
                    _ = heartbeat_rx.recv() => {
                        tracing::info!(
                            pod_id = %pod_id_for_heartbeat,
                            "Heartbeat task shutting down"
                        );
                        break;
                    }
                }
            }
        });

        // Cancellation is now handled via the CancellationToken in the JobRegistry.
        // The cancellation token is checked during conversion processing.
        // No separate monitor task is needed.

        tracing::info!(
            pod_id = %self.pod_id,
            max_concurrent_jobs = self.config.max_concurrent_jobs,
            poll_interval_secs = self.config.poll_interval.as_secs(),
            "Starting worker"
        );

        // Main loop - use tokio::select! for proper shutdown handling
        loop {
            // Check if we can claim more work units
            let active_count = self.metrics.active_jobs.load(Ordering::Relaxed) as usize;

            if active_count < self.config.max_concurrent_jobs {
                // Try to claim a work unit from batch jobs
                let claimed_unit = match self.find_and_claim_work_unit().await {
                    Ok(Some(unit)) => Some(unit),
                    Ok(None) => None,
                    Err(e) => {
                        tracing::error!(
                            pod_id = %self.pod_id,
                            error = %e,
                            "Failed to claim work unit"
                        );
                        None
                    }
                };

                if let Some(unit) = claimed_unit {
                    let unit_id = unit.id.clone();
                    let batch_id = unit.batch_id.clone();

                    // Check if shutdown was requested before processing
                    if self.shutdown_handler.is_requested() {
                        tracing::info!(
                            pod_id = %self.pod_id,
                            "Shutdown requested, not processing new work unit"
                        );
                        // Release the work unit back to Pending
                        let _ = self
                            .batch_controller
                            .fail_work_unit(
                                &batch_id,
                                &unit_id,
                                "Shutdown requested, releasing back to Pending".to_string(),
                            )
                            .await;
                        self.metrics.dec_active_jobs();
                        break;
                    }

                    // Process the work unit using the pipeline-v2 API.
                    // For cloud URLs, the source streams data directly from S3/OSS
                    // via robocodec's S3Reader -- no prefetch or temp files needed.
                    let result = self.process_work_unit_with_pipeline(&unit).await;

                    match result {
                        ProcessingResult::Success => {
                            if let Err(e) = self.complete_work_unit(&batch_id, &unit_id).await {
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
                            // Check if this was a shutdown interrupt
                            if error.contains("interrupted by shutdown") {
                                tracing::info!(
                                    pod_id = %self.pod_id,
                                    unit_id = %unit_id,
                                    "Work unit interrupted by shutdown, releasing back to Pending"
                                );
                                if let Err(e) = self
                                    .batch_controller
                                    .fail_work_unit(
                                        &batch_id,
                                        &unit_id,
                                        "Shutdown interrupted, releasing back to Pending"
                                            .to_string(),
                                    )
                                    .await
                                {
                                    tracing::error!(
                                        pod_id = %self.pod_id,
                                        unit_id = %unit_id,
                                        error = %e,
                                        "Failed to release work unit during shutdown - unit may be stuck"
                                    );
                                }
                                break;
                            }

                            if let Err(e) = self.fail_work_unit(&batch_id, &unit_id, error).await {
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
                                "Work unit was cancelled"
                            );
                            // Work unit cancellation is handled via the cancellation token
                            // Don't break the loop - continue processing other work units
                        }
                    }
                } else {
                    // No work units available - use tokio::select! to race shutdown against sleep
                    tracing::debug!(
                        pod_id = %self.pod_id,
                        interval_secs = self.config.poll_interval.as_secs(),
                        "No work units available, waiting before next poll"
                    );
                    tokio::select! {
                        _ = sleep(self.config.poll_interval) => {
                            tracing::debug!(
                                pod_id = %self.pod_id,
                                "Waking up to poll for work units"
                            );
                        }
                        _ = shutdown_rx.recv() => {
                            tracing::info!(
                                pod_id = %self.pod_id,
                                "Worker shutdown requested while idle"
                            );
                            break;
                        }
                    }
                }
            } else {
                // At capacity, sleep briefly with shutdown handling
                tokio::select! {
                    _ = sleep(Duration::from_millis(100)) => {}
                    _ = shutdown_rx.recv() => {
                        tracing::info!(
                            pod_id = %self.pod_id,
                            "Worker shutdown requested while at capacity"
                        );
                        break;
                    }
                }
            }
        }

        // Wait for heartbeat task to finish gracefully
        let _ = heartbeat_handle.await;

        // Send final heartbeat with Draining status and shutdown timestamp
        let mut heartbeat = self
            .tikv
            .get_heartbeat(&self.pod_id)
            .await?
            .unwrap_or_else(|| HeartbeatRecord::new(self.pod_id.clone()));
        heartbeat.beat();
        heartbeat.status = WorkerStatus::Draining;

        // Add shutdown metadata for observability
        if let Some(ref mut metadata) = heartbeat.metadata
            && let Some(obj) = metadata.as_object_mut()
        {
            obj.insert(
                "shutdown_at".to_string(),
                serde_json::json!(chrono::Utc::now().to_rfc3339()),
            );
            obj.insert("reason".to_string(), serde_json::json!("graceful_shutdown"));
        }

        if let Err(e) = self.tikv.update_heartbeat(&self.pod_id, &heartbeat).await {
            tracing::error!(
                pod_id = %self.pod_id,
                error = %e,
                "Failed to send final Draining heartbeat - worker shutdown may not be visible to cluster"
            );
        }

        // Heartbeat is retained (not deleted) to mark graceful shutdown
        // This allows ZombieReaper to distinguish clean shutdown from crashes
        tracing::info!(
            pod_id = %self.pod_id,
            "Worker stopped gracefully (heartbeat retained with Draining status)"
        );

        Ok(())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_config_default() {
        let config = WorkerConfig::default();
        assert_eq!(config.max_concurrent_jobs, 1);
        assert_eq!(config.poll_interval.as_secs(), 5);
        assert_eq!(config.max_attempts, 3);
        assert_eq!(config.job_timeout.as_secs(), 3600);
        assert_eq!(config.heartbeat_interval.as_secs(), 30);
        assert_eq!(config.checkpoint_interval_frames, 100);
        assert_eq!(config.checkpoint_interval_seconds, 10);
        assert!(config.checkpoint_async);
        assert_eq!(config.output_prefix, "output/");
    }

    #[test]
    fn test_worker_config_builder() {
        let config = WorkerConfig::new()
            .with_max_concurrent_jobs(5)
            .with_poll_interval(Duration::from_secs(10))
            .with_max_attempts(5)
            .with_job_timeout(Duration::from_secs(7200))
            .with_heartbeat_interval(Duration::from_secs(60))
            .with_output_prefix("results/");

        assert_eq!(config.max_concurrent_jobs, 5);
        assert_eq!(config.poll_interval.as_secs(), 10);
        assert_eq!(config.max_attempts, 5);
        assert_eq!(config.job_timeout.as_secs(), 7200);
        assert_eq!(config.heartbeat_interval.as_secs(), 60);
        assert_eq!(config.output_prefix, "results/");
    }

    #[test]
    fn test_worker_metrics() {
        let metrics = WorkerMetrics::new();

        assert_eq!(metrics.jobs_claimed.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.active_jobs.load(Ordering::Relaxed), 0);

        metrics.inc_jobs_claimed();
        metrics.inc_active_jobs();
        metrics.inc_active_jobs();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.jobs_claimed, 1);
        assert_eq!(snapshot.active_jobs, 2);

        metrics.dec_active_jobs();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.active_jobs, 1);
    }

    #[test]
    fn test_generate_pod_id() {
        // Test that we can generate a pod ID
        let pod_id = Worker::generate_pod_id();
        assert!(!pod_id.is_empty());
        // Should contain a UUID
        assert!(pod_id.len() > 20);
    }
}
