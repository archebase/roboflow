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
use roboflow_sinks::{LerobotWriterConfig, create_lerobot_writer};
use roboflow_sources::{SourceConfig, create_source};

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
        let output_path_str = output_path.to_string_lossy().to_string();

        // Step 3: Load configuration
        let config = match self.load_config(unit).await {
            Ok(config) => config,
            Err(e) => {
                return ProcessingResult::Failed {
                    error: format!("Failed to load config: {}", e),
                };
            }
        };

        // Step 4: Create source
        let source_config = Self::create_source_config(source_url);
        let source = match create_source(&source_config) {
            Ok(s) => s,
            Err(e) => {
                return ProcessingResult::Failed {
                    error: format!("Failed to create source: {}", e),
                };
            }
        };

        // Step 5: Create writer using the consolidated factory
        let factory_config = LerobotWriterConfig::new(&output_path_str, config.clone());
        let writer_result = match create_lerobot_writer(&factory_config) {
            Ok(result) => result,
            Err(e) => {
                return ProcessingResult::Failed {
                    error: format!("Failed to create writer: {}", e),
                };
            }
        };

        // Step 6: Build topic mappings and pipeline config
        let topic_mappings = Self::build_topic_mappings(&config);
        let pipeline_config = Self::create_pipeline_config(&config, topic_mappings);

        // Step 7: Execute pipeline with timeout
        let ctx = PipelineContext {
            source,
            source_config,
            writer: writer_result.writer,
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
    ///
    /// Handles:
    /// - Local files with extensions (.mcap, .bag, .rrd)
    /// - S3/OSS file URLs (s3://bucket/path/file.mcap)
    /// - S3/OSS prefix URLs (s3://bucket/path/to/prefix/)
    fn create_source_config(source_url: &str) -> SourceConfig {
        // Check for cloud URLs
        let is_cloud = source_url.starts_with("s3://") || source_url.starts_with("oss://");

        // Get the lowercase version for extension checking
        let url_lower = source_url.to_lowercase();

        // Check for specific file extensions
        if url_lower.ends_with(".mcap") {
            SourceConfig::mcap(source_url)
        } else if url_lower.ends_with(".bag") {
            SourceConfig::bag(source_url)
        } else if url_lower.ends_with(".rrd") {
            SourceConfig::rrd(source_url)
        } else if is_cloud {
            // Cloud URL without a specific extension - treat as prefix
            SourceConfig::s3_prefix(source_url)
        } else {
            // Default to MCAP for local files
            SourceConfig::mcap(source_url)
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
