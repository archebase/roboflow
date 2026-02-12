// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Task executor for processing work units.
//!
//! This module contains the execution logic separated from coordination.
//! The executor is responsible for:
//! - Loading and caching configurations
//! - Creating sources and sinks
//! - Running the conversion pipeline
//! - Reporting execution results

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use lru::LruCache;
use tokio::sync::Mutex;

use roboflow_core::RoboflowError;
use roboflow_dataset::lerobot::{LerobotConfig, LerobotWriter};
use roboflow_dataset::streaming::config::StreamingConfig;
use roboflow_dataset::{PipelineConfig, PipelineExecutor};
use roboflow_sources::{SourceConfig, create_source};
use roboflow_storage::LocalStorage;

use super::metrics::ProcessingResult;
use super::registry::JobRegistry;
use crate::batch::WorkUnit;
use crate::tikv::{TikvClient, TikvError};

/// Result of executing a work unit.
#[derive(Debug)]
pub struct ExecutionResult {
    /// Number of frames processed.
    pub frames_processed: u64,
    /// Number of videos created.
    pub videos_created: usize,
}

/// Storage setup result containing storage backend, optional prefix, and local buffer path.
type StorageSetup = (Arc<dyn roboflow_storage::Storage>, Option<String>, PathBuf);

/// Context for running a pipeline execution.
struct PipelineContext {
    /// The data source.
    source: Box<dyn roboflow_sources::Source>,
    /// Source configuration.
    source_config: SourceConfig,
    /// The writer for output.
    writer: LerobotWriter,
    /// Pipeline configuration.
    pipeline_config: PipelineConfig,
    /// Work unit ID.
    unit_id: String,
    /// Job registry for cancellation support.
    job_registry: Arc<tokio::sync::RwLock<JobRegistry>>,
}

/// Task executor for processing work units.
///
/// This struct handles the actual execution of work units,
/// separated from coordination concerns.
pub struct TaskExecutor {
    /// Configuration cache to reduce TiKV round-trips.
    config_cache: Arc<Mutex<LruCache<String, LerobotConfig>>>,
    /// Shared TiKV client for config loading.
    tikv: Arc<TikvClient>,
    /// Job registry for cancellation tracking.
    job_registry: Arc<tokio::sync::RwLock<JobRegistry>>,
    /// Default output prefix for local files.
    output_prefix: String,
    /// Pipeline timeout.
    timeout: Duration,
}

impl TaskExecutor {
    /// Create a new task executor.
    pub fn new(
        tikv: Arc<TikvClient>,
        job_registry: Arc<tokio::sync::RwLock<JobRegistry>>,
        output_prefix: String,
        timeout: Duration,
    ) -> Self {
        Self {
            config_cache: Arc::new(Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(100).unwrap(),
            ))),
            tikv,
            job_registry,
            output_prefix,
            timeout,
        }
    }

    /// Execute a work unit.
    ///
    /// This method handles:
    /// 1. Loading the configuration
    /// 2. Setting up source and sink
    /// 3. Running the pipeline
    /// 4. Returning the result
    pub async fn execute(&self, unit: &WorkUnit) -> ProcessingResult {
        tracing::info!(
            unit_id = %unit.id,
            batch_id = %unit.batch_id,
            files = unit.files.len(),
            "Executing work unit"
        );

        // Step 1: Get the primary source file
        let source_url = match unit.primary_source() {
            Some(url) => url,
            None => {
                let error = format!("Work unit {} has no primary source", unit.id);
                tracing::error!(unit_id = %unit.id, "No primary source");
                return ProcessingResult::Failed { error };
            }
        };

        // Step 2: Determine output path
        let output_path = self.resolve_output_path(unit);
        let output_path_str = output_path.to_string_lossy();

        // Step 3: Setup storage
        let (storage, output_prefix, local_buffer_path) = {
            // Validate cloud URL format
            if output_path_str.starts_with("s3:") && !output_path_str.starts_with("s3://") {
                return ProcessingResult::Failed {
                    error: format!(
                        "Malformed cloud URL '{}': use s3://bucket/path (double slash required)",
                        output_path_str
                    ),
                };
            }

            if output_path_str.starts_with("s3://") {
                match self.setup_cloud_storage(&output_path_str) {
                    Ok(result) => result,
                    Err(e) => return ProcessingResult::Failed { error: e },
                }
            } else {
                let local_storage: Arc<dyn roboflow_storage::Storage> =
                    Arc::new(LocalStorage::new(std::env::temp_dir()));
                (local_storage, None, output_path.clone())
            }
        };

        // Step 4: Load configuration
        let config = match self.load_config(unit).await {
            Ok(config) => config,
            Err(e) => {
                return ProcessingResult::Failed {
                    error: format!("Failed to load config: {}", e),
                };
            }
        };

        // Step 5: Create source
        let source_config = Self::create_source_config(source_url);
        let source = match create_source(&source_config) {
            Ok(s) => s,
            Err(e) => {
                return ProcessingResult::Failed {
                    error: format!("Failed to create source: {}", e),
                };
            }
        };

        // Step 6: Create writer
        let writer = match self.create_writer(&storage, &output_prefix, &local_buffer_path, &config)
        {
            Ok(w) => w,
            Err(e) => return ProcessingResult::Failed { error: e },
        };

        // Step 7: Build topic mappings and pipeline config
        let topic_mappings = Self::build_topic_mappings(&config);
        let pipeline_config = Self::create_pipeline_config(&config, topic_mappings);

        // Step 8: Execute pipeline with timeout
        let ctx = PipelineContext {
            source,
            source_config,
            writer,
            pipeline_config,
            unit_id: unit.id.clone(),
            job_registry: self.job_registry.clone(),
        };
        self.run_pipeline(ctx, self.timeout).await
    }

    /// Resolve the output path for a work unit.
    fn resolve_output_path(&self, unit: &WorkUnit) -> PathBuf {
        if !unit.output_path.is_empty() {
            PathBuf::from(&unit.output_path)
        } else {
            PathBuf::from(format!(
                "{}/{}",
                self.output_prefix.trim_end_matches('/'),
                unit.id
            ))
        }
    }

    /// Setup cloud storage for S3 output.
    fn setup_cloud_storage(&self, output_path_str: &str) -> Result<StorageSetup, String> {
        let storage: Arc<dyn roboflow_storage::Storage> =
            roboflow_storage::StorageFactory::from_env()
                .create(output_path_str)
                .map_err(|e| format!("Failed to create storage: {}", e))?;

        // Extract prefix from S3 URL
        let prefix = output_path_str
            .strip_prefix("s3://")
            .and_then(|s| s.find('/').map(|i| &s[i + 1..]))
            .unwrap_or("")
            .trim_end_matches('/')
            .to_string();

        let local_buffer = std::env::temp_dir();
        Ok((storage, Some(prefix), local_buffer))
    }

    /// Load configuration for a work unit.
    async fn load_config(&self, unit: &WorkUnit) -> std::result::Result<LerobotConfig, TikvError> {
        let config_hash = &unit.config_hash;

        // Validate config hash
        if config_hash.is_empty() || config_hash == "default" {
            return Err(TikvError::Other(format!(
                "Work unit {} has no valid config_hash",
                unit.id
            )));
        }

        // Check cache
        {
            let mut cache = self.config_cache.lock().await;
            if let Some(config) = cache.get(config_hash) {
                tracing::debug!(
                    unit_id = %unit.id,
                    config_hash = %config_hash,
                    "Loaded config from cache"
                );
                return Ok(config.clone());
            }
        }

        // Fetch from TiKV
        let config = match self.tikv.get_config(config_hash).await? {
            Some(record) => LerobotConfig::from_toml(&record.content)
                .map_err(|e| TikvError::Other(format!("Failed to parse config TOML: {}", e)))?,
            None => {
                return Err(TikvError::Other(format!(
                    "Config '{}' not found in TiKV",
                    config_hash
                )));
            }
        };

        // Cache for future use
        {
            let mut cache = self.config_cache.lock().await;
            cache.put(config_hash.clone(), config.clone());
        }

        tracing::info!(
            unit_id = %unit.id,
            config_hash = %config_hash,
            "Loaded config from TiKV"
        );

        Ok(config)
    }

    /// Create source config from URL.
    fn create_source_config(source_url: &str) -> SourceConfig {
        if source_url.ends_with(".mcap") {
            SourceConfig::mcap(source_url)
        } else if source_url.ends_with(".bag") {
            SourceConfig::bag(source_url)
        } else if source_url.ends_with(".rrd") {
            SourceConfig::rrd(source_url)
        } else {
            SourceConfig::mcap(source_url) // Default to MCAP
        }
    }

    /// Create writer based on storage configuration.
    fn create_writer(
        &self,
        storage: &Arc<dyn roboflow_storage::Storage>,
        output_prefix: &Option<String>,
        local_buffer_path: &PathBuf,
        config: &LerobotConfig,
    ) -> std::result::Result<LerobotWriter, String> {
        if let Some(prefix) = output_prefix {
            LerobotWriter::new(
                Arc::clone(storage),
                prefix.clone(),
                std::env::temp_dir(),
                config.clone(),
            )
            .map_err(|e| format!("Failed to create cloud writer: {}", e))
        } else {
            LerobotWriter::new_local(local_buffer_path, config.clone())
                .map_err(|e| format!("Failed to create local writer: {}", e))
        }
    }

    /// Build topic mappings from config.
    fn build_topic_mappings(config: &LerobotConfig) -> HashMap<String, String> {
        let mut mappings = HashMap::new();
        for mapping in &config.mappings {
            mappings.insert(mapping.topic.clone(), mapping.feature.clone());
        }
        mappings
    }

    /// Create pipeline configuration.
    fn create_pipeline_config(
        config: &LerobotConfig,
        topic_mappings: HashMap<String, String>,
    ) -> PipelineConfig {
        let frame_interval_ns = 1_000_000_000u64 / config.dataset.fps as u64;
        let completion_window_ns = frame_interval_ns * 3;

        let mut streaming_config = StreamingConfig::with_fps(config.dataset.fps);
        streaming_config.completion_window_ns = completion_window_ns;

        PipelineConfig::new(streaming_config).with_topic_mappings(topic_mappings)
    }

    /// Run the pipeline with timeout and cancellation support.
    async fn run_pipeline(&self, ctx: PipelineContext, timeout: Duration) -> ProcessingResult {
        use tokio_util::sync::CancellationToken;

        let PipelineContext {
            mut source,
            source_config,
            writer,
            pipeline_config,
            unit_id,
            job_registry,
        } = ctx;

        // Create cancellation token
        let cancel_token = CancellationToken::new();
        let cancel_token_for_monitor = Arc::new(cancel_token.clone());

        // Register with job registry
        {
            let mut registry = job_registry.write().await;
            registry.register(unit_id.clone(), cancel_token_for_monitor);
        }

        // Create executor
        let mut executor = PipelineExecutor::new(writer, pipeline_config);
        let unit_id_clone = unit_id.clone();
        let job_registry_for_cleanup = job_registry;
        let cancel_token_for_timeout = cancel_token.clone();

        let pipeline_task = tokio::task::spawn(async move {
            let _guard = cancel_token.clone().drop_guard();

            // Initialize source
            let _ = source.initialize(&source_config).await;

            // Process messages
            let batch_size = 1000;
            loop {
                if cancel_token.is_cancelled() {
                    return Err(RoboflowError::other("Interrupted by shutdown".to_string()));
                }

                match source.read_batch(batch_size).await {
                    Ok(Some(messages)) if !messages.is_empty() => {
                        for msg in messages {
                            executor.process_message(msg)?;
                        }
                    }
                    Ok(Some(_)) => continue,
                    Ok(None) => break,
                    Err(e) => {
                        return Err(RoboflowError::other(format!("Source read failed: {}", e)));
                    }
                }
            }

            executor.finalize()
        });

        // Wait with timeout
        let result = match tokio::time::timeout(timeout, pipeline_task).await {
            Ok(Ok(Ok(_stats))) => {
                let mut registry = job_registry_for_cleanup.write().await;
                registry.unregister(&unit_id_clone);
                ProcessingResult::Success
            }
            Ok(Ok(Err(e))) => {
                let mut registry = job_registry_for_cleanup.write().await;
                registry.unregister(&unit_id_clone);
                ProcessingResult::Failed {
                    error: format!("Pipeline execution failed: {}", e),
                }
            }
            Ok(Err(join_err)) => {
                let mut registry = job_registry_for_cleanup.write().await;
                registry.unregister(&unit_id_clone);
                if join_err.is_cancelled() {
                    ProcessingResult::Cancelled
                } else {
                    ProcessingResult::Failed {
                        error: format!("Pipeline task panicked: {}", join_err),
                    }
                }
            }
            Err(_) => {
                let mut registry = job_registry_for_cleanup.write().await;
                registry.unregister(&unit_id_clone);
                cancel_token_for_timeout.cancel();
                ProcessingResult::Failed {
                    error: format!("Pipeline timed out after {:?}", timeout),
                }
            }
        };

        tracing::info!(unit_id = %unit_id, "Pipeline execution complete");
        result
    }
}
