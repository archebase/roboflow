// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Worker actor for claiming and processing work units from TiKV batch queue.

mod checkpoint;
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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::batch::{BatchController, WorkUnit};
use super::merge::MergeCoordinator;
use super::shutdown::ShutdownHandler;
use super::tikv::{
    TikvError,
    checkpoint::{CheckpointConfig, CheckpointManager},
    client::TikvClient,
    schema::{HeartbeatRecord, WorkerStatus},
};
use roboflow_storage::{Storage, StorageFactory};
use tokio::sync::{Mutex, RwLock};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use lru::LruCache;

// Dataset conversion imports
use roboflow_dataset::{
    lerobot::{LerobotConfig, VideoConfig},
    streaming::StreamingDatasetConverter,
};

// Re-export module items for use within the worker module
pub use checkpoint::WorkerCheckpointCallback;
pub use heartbeat::send_heartbeat_inner;
pub use registry::JobRegistry;

/// Default cancellation check interval in seconds.
pub const DEFAULT_CANCELLATION_CHECK_INTERVAL_SECS: u64 = 5;

/// Worker actor for claiming and processing work units.
pub struct Worker {
    pod_id: String,
    tikv: Arc<TikvClient>,
    checkpoint_manager: CheckpointManager,
    storage: Arc<dyn Storage>,
    storage_factory: StorageFactory,
    config: WorkerConfig,
    metrics: Arc<WorkerMetrics>,
    shutdown_handler: ShutdownHandler,
    cancellation_token: Arc<CancellationToken>,
    job_registry: Arc<RwLock<JobRegistry>>,
    config_cache: Arc<Mutex<LruCache<String, roboflow_dataset::lerobot::LerobotConfig>>>,
    merge_coordinator: MergeCoordinator,
    batch_controller: BatchController,
}

impl Worker {
    pub fn new(
        pod_id: impl Into<String>,
        tikv: Arc<TikvClient>,
        storage: Arc<dyn Storage>,
        config: WorkerConfig,
    ) -> Result<Self, TikvError> {
        let pod_id = pod_id.into();

        // Create storage factory from storage URL (for creating output storage backends)
        let storage_factory = StorageFactory::new();

        // Create checkpoint manager with config from WorkerConfig
        let checkpoint_config = CheckpointConfig {
            checkpoint_interval_frames: config.checkpoint_interval_frames,
            checkpoint_interval_seconds: config.checkpoint_interval_seconds,
            checkpoint_async: config.checkpoint_async,
        };
        let checkpoint_manager = CheckpointManager::new(tikv.clone(), checkpoint_config);

        // Create merge coordinator for distributed dataset merge operations
        use super::merge::MergeCoordinator;
        let merge_coordinator = MergeCoordinator::new(tikv.clone(), pod_id.clone());

        // Create batch controller for work unit processing
        let batch_controller = BatchController::with_client(tikv.clone());

        Ok(Self {
            pod_id,
            tikv,
            checkpoint_manager,
            storage,
            storage_factory,
            config,
            metrics: Arc::new(WorkerMetrics::new()),
            shutdown_handler: ShutdownHandler::new(),
            cancellation_token: Arc::new(CancellationToken::new()),
            job_registry: Arc::new(RwLock::new(JobRegistry::default())),
            config_cache: Arc::new(Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(100).unwrap(), // Cache up to 100 configs
            ))),
            merge_coordinator,
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

    /// Process a work unit from a batch job.
    ///
    /// This processes files from a batch work unit, converting them to the output format.
    /// The conversion pipeline (StreamingDatasetConverter, CheckpointManager, etc.)
    /// operates the same way as before, just using WorkUnit data directly.
    async fn process_work_unit(&self, unit: &WorkUnit) -> ProcessingResult {
        tracing::info!(
            pod_id = %self.pod_id,
            unit_id = %unit.id,
            batch_id = %unit.batch_id,
            files = unit.files.len(),
            "Processing work unit"
        );

        // For single-file work units, process the file directly
        if let Some(source_url) = unit.primary_source() {
            // Check for existing checkpoint
            let unit_id = &unit.id;
            match self.tikv.get_checkpoint(unit_id).await {
                Ok(Some(checkpoint)) => {
                    tracing::info!(
                        pod_id = %self.pod_id,
                        unit_id = %unit_id,
                        last_frame = checkpoint.last_frame,
                        total_frames = checkpoint.total_frames,
                        progress = checkpoint.progress_percent(),
                        "Resuming work unit from checkpoint"
                    );
                    // Note: Checkpoint-based resume will be implemented in a follow-up issue.
                    // For Phase 1, we start from beginning even if checkpoint exists.
                }
                Ok(None) => {
                    tracing::debug!(
                        pod_id = %self.pod_id,
                        unit_id = %unit_id,
                        "No existing checkpoint found, starting from beginning"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        pod_id = %self.pod_id,
                        unit_id = %unit_id,
                        error = %e,
                        "Failed to fetch checkpoint - starting from beginning (progress may be lost)"
                    );
                }
            }

            // Use source_url directly - work units are self-contained.
            // The converter detects storage type from the URL scheme (s3://, oss://, file://, or local path).
            tracing::info!(
                pod_id = %self.pod_id,
                unit_id = %unit_id,
                source_url = %source_url,
                "Processing work unit with source URL"
            );

            let input_path = PathBuf::from(&source_url);

            // Build the output path for this work unit
            let output_path = self.build_output_path(unit);

            // Determine output storage and prefix for staging
            // When output_storage_url is configured, use cloud storage with staging pattern
            let (output_storage, staging_prefix) = if let Some(storage_url) =
                &self.config.output_storage_url
            {
                // Create output storage from configured URL
                match self.storage_factory.create(storage_url) {
                    Ok(storage) => {
                        // Staging pattern: {storage_url}/staging/{unit_id}/worker_{pod_id}/
                        // Each worker writes to its own subdirectory for isolation
                        let staging_prefix = format!("staging/{}/worker_{}", unit_id, self.pod_id);
                        tracing::info!(
                            storage_url = %storage_url,
                            staging_prefix = %staging_prefix,
                            "Using cloud storage with staging pattern"
                        );
                        (Some(storage), Some(staging_prefix))
                    }
                    Err(e) => {
                        tracing::warn!(
                            storage_url = %storage_url,
                            error = %e,
                            "Failed to create output storage, falling back to local storage"
                        );
                        (None, None)
                    }
                }
            } else {
                (None, None)
            };

            tracing::info!(
                input = %input_path.display(),
                output = %output_path.display(),
                cloud_output = staging_prefix.is_some(),
                "Starting conversion"
            );

            // Create the LeRobot configuration
            let lerobot_config = match self.create_lerobot_config(unit).await {
                Ok(config) => config,
                Err(e) => {
                    let error_msg =
                        format!("Failed to load config for work unit {}: {}", unit.id, e);
                    tracing::error!(
                        unit_id = %unit.id,
                        original_error = %e,
                        "Failed to load LeRobot config"
                    );
                    return ProcessingResult::Failed { error: error_msg };
                }
            };

            // Create streaming converter with storage backends
            // For cloud storage inputs, pass None for input_storage to let converter
            // download the file. For local storage, pass self.storage for fast path.
            let is_cloud_storage =
                source_url.starts_with("s3://") || source_url.starts_with("oss://");
            let input_storage = if is_cloud_storage {
                None
            } else {
                Some(self.storage.clone())
            };

            // Use cloud output storage if configured, otherwise use local storage
            let output_storage_for_converter = output_storage
                .clone()
                .or_else(|| Some(self.storage.clone()));

            let mut converter = match StreamingDatasetConverter::new_lerobot_with_storage(
                &output_path,
                lerobot_config,
                input_storage,
                output_storage_for_converter,
            ) {
                Ok(c) => c,
                Err(e) => {
                    let error_msg = format!(
                        "Failed to create converter for work unit {} (input: {}, output: {}): {}",
                        unit.id,
                        input_path.display(),
                        output_path.display(),
                        e
                    );
                    tracing::error!(
                        unit_id = %unit.id,
                        input = %input_path.display(),
                        output = %output_path.display(),
                        original_error = %e,
                        "Converter creation failed"
                    );
                    return ProcessingResult::Failed { error: error_msg };
                }
            };

            // Set staging prefix if using cloud storage
            if let Some(ref prefix) = staging_prefix {
                converter = converter.with_output_prefix(prefix.clone());
            }

            // Add checkpoint callback if enabled
            // Estimate total frames from source file size.
            // Heuristic: ~100KB per frame for typical robotics data (images + state).
            // This is approximate; actual frame count is updated as we process.
            let estimated_frame_size = 100_000; // 100KB per frame
            let total_frames = (unit.total_size() / estimated_frame_size).max(1);

            // Create cancellation token for this work unit
            let cancel_token = self.cancellation_token.child_token();
            let cancel_token_for_monitor = Arc::new(cancel_token.clone());
            let cancel_token_for_callback = Arc::new(cancel_token.clone());

            // Create progress callback with cancellation token
            let checkpoint_callback = Arc::new(WorkerCheckpointCallback {
                job_id: unit_id.clone(),
                pod_id: self.pod_id.clone(),
                total_frames,
                checkpoint_manager: self.checkpoint_manager.clone(),
                last_checkpoint_frame: Arc::new(AtomicU64::new(0)),
                last_checkpoint_time: Arc::new(std::sync::Mutex::new(std::time::Instant::now())),
                shutdown_flag: self.shutdown_handler.flag_clone(),
                cancellation_token: Some(cancel_token_for_callback),
            });
            converter = converter.with_progress_callback(checkpoint_callback);

            // Register this work unit with the cancellation monitor
            {
                let mut registry = self.job_registry.write().await;
                registry.register(unit_id.clone(), cancel_token_for_monitor);
            }
            tracing::debug!(
                unit_id = %unit_id,
                "Registered work unit with cancellation monitor"
            );

            // Run the conversion with a timeout to prevent indefinite hangs.
            // Note: This is a synchronous operation that may take significant time.
            // We use spawn_blocking to avoid starving the async runtime.
            // A cancellation token is used to attempt cooperative cancellation on timeout.
            use std::time::Duration;
            const CONVERSION_TIMEOUT: Duration = Duration::from_secs(3600); // 1 hour

            let unit_id_clone = unit_id.clone();
            let cancel_token_for_timeout = cancel_token.clone();
            let job_registry_for_cleanup = self.job_registry.clone();

            let conversion_task = tokio::task::spawn_blocking(move || {
                // Guard cancels the token when dropped (on task completion)
                let _guard = cancel_token.drop_guard();
                converter.convert(input_path)
            });

            let stats = match tokio::time::timeout(CONVERSION_TIMEOUT, conversion_task).await {
                Ok(Ok(Ok(stats))) => {
                    // Unregister from cancellation monitor
                    let mut registry = job_registry_for_cleanup.write().await;
                    registry.unregister(&unit_id_clone);
                    stats
                }
                Ok(Ok(Err(e))) => {
                    // Unregister from cancellation monitor
                    let mut registry = job_registry_for_cleanup.write().await;
                    registry.unregister(&unit_id_clone);

                    let error_msg =
                        format!("Conversion failed for work unit {}: {}", unit_id_clone, e);
                    tracing::error!(
                        unit_id = %unit_id_clone,
                        original_error = %e,
                        "Work unit processing failed"
                    );
                    return ProcessingResult::Failed { error: error_msg };
                }
                Ok(Err(join_err)) => {
                    // Unregister from cancellation monitor
                    let mut registry = job_registry_for_cleanup.write().await;
                    registry.unregister(&unit_id_clone);

                    // Check if this was a cancellation (not timeout)
                    if join_err.is_cancelled() {
                        // Cancellation is handled via the cancellation token
                        tracing::info!(
                            unit_id = %unit_id_clone,
                            "Work unit was cancelled"
                        );
                        return ProcessingResult::Cancelled;
                    }

                    let error_msg = format!(
                        "Conversion task panicked for work unit {}: {}",
                        unit_id_clone, join_err
                    );
                    tracing::error!(
                        unit_id = %unit_id_clone,
                        join_error = %join_err,
                        "Work unit processing task failed"
                    );
                    return ProcessingResult::Failed { error: error_msg };
                }
                Err(_) => {
                    // Unregister from cancellation monitor
                    let mut registry = job_registry_for_cleanup.write().await;
                    registry.unregister(&unit_id_clone);

                    // Timeout: request cancellation to potentially stop the blocking work
                    cancel_token_for_timeout.cancel();
                    let error_msg = format!(
                        "Conversion timed out after {:?} for work unit {}",
                        CONVERSION_TIMEOUT, unit_id_clone
                    );
                    tracing::error!(
                        unit_id = %unit_id_clone,
                        timeout_secs = CONVERSION_TIMEOUT.as_secs(),
                        "Work unit processing timed out"
                    );
                    return ProcessingResult::Failed { error: error_msg };
                }
            };

            tracing::info!(
                unit_id = %unit_id,
                frames_written = stats.frames_written,
                messages = stats.messages_processed,
                duration_sec = stats.duration_sec,
                "Work unit processing complete"
            );

            // Register staging completion and try to claim merge task
            // This is only done when using cloud storage with staging pattern
            if let Some(prefix) = &staging_prefix {
                // Full staging path includes the storage URL
                let storage_url = self.config.output_storage_url.as_deref().unwrap_or("");
                let staging_path = format!("{}/{}", storage_url, prefix);

                tracing::info!(
                    unit_id = %unit_id,
                    staging_path = %staging_path,
                    frame_count = stats.frames_written,
                    "Registering staging completion"
                );

                // Register that this worker has completed staging
                if let Err(e) = self
                    .merge_coordinator
                    .register_staging_complete(
                        unit_id,
                        &self.pod_id,
                        staging_path,
                        stats.frames_written as u64,
                    )
                    .await
                {
                    tracing::error!(
                        unit_id = %unit_id,
                        error = %e,
                        "Failed to register staging completion - data may be orphaned in staging"
                    );
                    return ProcessingResult::Failed {
                        error: format!("Staging registration failed: {}", e),
                    };
                } else {
                    // Try to claim the merge task
                    tracing::info!(
                        unit_id = %unit_id,
                        expected_workers = self.config.expected_workers,
                        merge_output = %self.config.merge_output_path,
                        "Attempting to claim merge task"
                    );

                    match self
                        .merge_coordinator
                        .try_claim_merge(
                            unit_id,
                            self.config.expected_workers,
                            self.config.merge_output_path.clone(),
                        )
                        .await
                    {
                        Ok(super::merge::MergeResult::Success {
                            output_path,
                            total_frames,
                        }) => {
                            tracing::info!(
                                unit_id = %unit_id,
                                output_path = %output_path,
                                total_frames,
                                "Merge completed successfully"
                            );
                        }
                        Ok(super::merge::MergeResult::NotClaimed) => {
                            tracing::debug!(
                                unit_id = %unit_id,
                                "Merge task claimed by another worker"
                            );
                        }
                        Ok(super::merge::MergeResult::NotReady) => {
                            tracing::debug!(
                                unit_id = %unit_id,
                                "Merge not ready, waiting for more workers"
                            );
                        }
                        Ok(super::merge::MergeResult::Failed { error }) => {
                            tracing::error!(
                                unit_id = %unit_id,
                                error = %error,
                                "Merge failed"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                unit_id = %unit_id,
                                error = %e,
                                "Failed to claim merge task"
                            );
                        }
                    }
                }
            }

            ProcessingResult::Success
        } else {
            // Multi-file work units - process each file
            tracing::warn!(
                unit_id = %unit.id,
                file_count = unit.files.len(),
                "Multi-file work units not yet supported"
            );
            ProcessingResult::Failed {
                error: "Multi-file work units not yet supported".to_string(),
            }
        }
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
        use roboflow_dataset::lerobot::config::DatasetConfig;

        let config_hash = &unit.config_hash;

        // Skip empty hash (special case for "default" or legacy behavior)
        if config_hash.is_empty() || config_hash == "default" {
            tracing::warn!(
                pod_id = %self.pod_id,
                unit_id = %unit.id,
                config_hash = %config_hash,
                "Using default empty config (will produce no frames)"
            );
            return Ok(LerobotConfig {
                dataset: DatasetConfig {
                    name: format!("roboflow-episode-{}", unit.id),
                    fps: 30,
                    robot_type: Some("robot".to_string()),
                    env_type: None,
                },
                mappings: Vec::new(),
                video: VideoConfig::default(),
                annotation_file: None,
            });
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

                    // Process the work unit
                    let result = self.process_work_unit(&unit).await;

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
