// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Task executor for processing work units.
//!
//! This module contains the execution logic separated from coordination.
//! The executor is responsible for:
//! - Loading and caching configurations
//! - Creating sources and sinks
//! - Allocating episode indices for distributed processing
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
use crate::episode::EpisodeAllocator;
use crate::tikv::{TikvClient, TikvError};

/// Default episodes per chunk (LeRobot v2.1 spec).
pub const DEFAULT_EPISODES_PER_CHUNK: u32 = 500;

/// Result of executing a work unit.
#[derive(Debug)]
pub struct ExecutionResult {
    /// Number of frames processed.
    pub frames_processed: u64,
    /// Number of videos created.
    pub videos_created: usize,
    /// Allocated episode index (if distributed processing).
    pub episode_index: Option<u64>,
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
    /// Allocated episode index (if distributed processing).
    episode_index: Option<u64>,
}

/// Task executor for processing work units.
///
/// This struct handles the actual execution of work units,
/// separated from coordination concerns.
///
/// # Episode Allocation
///
/// When an `EpisodeAllocator` is configured, each work unit will:
/// 1. Allocate a unique episode index from TiKV
/// 2. Configure the writer with the correct episode/chunk indices
/// 3. Ensure output files follow the LeRobot v2.1 directory structure
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
    /// Optional episode allocator for distributed processing.
    episode_allocator: Option<Arc<dyn EpisodeAllocator>>,
    /// Episodes per chunk for LeRobot v2.1 format.
    episodes_per_chunk: u32,
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
                std::num::NonZeroUsize::new(100).expect("100 is always a valid non-zero usize"),
            ))),
            tikv,
            job_registry,
            output_prefix,
            timeout,
            episode_allocator: None,
            episodes_per_chunk: DEFAULT_EPISODES_PER_CHUNK,
        }
    }

    /// Create a task executor with episode allocation for distributed processing.
    ///
    /// This enables:
    /// - Centralized episode index allocation via TiKV
    /// - Automatic chunk index calculation
    /// - LeRobot v2.1 compliant output structure
    pub fn with_episode_allocator(
        tikv: Arc<TikvClient>,
        job_registry: Arc<tokio::sync::RwLock<JobRegistry>>,
        output_prefix: String,
        timeout: Duration,
        allocator: Arc<dyn EpisodeAllocator>,
        episodes_per_chunk: u32,
    ) -> Self {
        Self {
            config_cache: Arc::new(Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(100).expect("100 is always a valid non-zero usize"),
            ))),
            tikv,
            job_registry,
            output_prefix,
            timeout,
            episode_allocator: Some(allocator),
            episodes_per_chunk,
        }
    }

    /// Check if episode allocation is enabled.
    pub fn has_episode_allocator(&self) -> bool {
        self.episode_allocator.is_some()
    }

    /// Get the configured episodes per chunk.
    pub fn get_episodes_per_chunk(&self) -> u32 {
        self.episodes_per_chunk
    }

    /// Execute a work unit.
    ///
    /// This method handles:
    /// 1. Allocating an episode index (if distributed processing)
    /// 2. Loading the configuration
    /// 3. Setting up source and sink
    /// 4. Configuring the writer with the allocated episode
    /// 5. Running the pipeline
    /// 6. Returning the result
    pub async fn execute(&self, unit: &WorkUnit) -> ProcessingResult {
        tracing::info!(
            unit_id = %unit.id,
            batch_id = %unit.batch_id,
            files = unit.files.len(),
            "Executing work unit"
        );

        // Step 1: Allocate episode index (if distributed processing)
        let episode_allocation = if let Some(ref allocator) = self.episode_allocator {
            match allocator.allocate().await {
                Ok(alloc) => {
                    tracing::info!(
                        unit_id = %unit.id,
                        episode_index = alloc.episode_index,
                        chunk_index = alloc.chunk_index,
                        chunk_offset = alloc.chunk_offset,
                        "Allocated episode for work unit"
                    );
                    Some(alloc)
                }
                Err(e) => {
                    return ProcessingResult::Failed {
                        error: format!("Failed to allocate episode: {}", e),
                    };
                }
            }
        } else {
            None
        };

        // Step 2: Get the primary source file
        let source_url = match unit.primary_source() {
            Some(url) => url,
            None => {
                let error = format!("Work unit {} has no primary source", unit.id);
                tracing::error!(unit_id = %unit.id, "No primary source");
                return ProcessingResult::Failed { error };
            }
        };

        // Step 3: Determine output path
        let output_path = self.resolve_output_path(unit);
        let output_path_str = output_path.to_string_lossy().to_string();

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
        let source_config = SourceConfig::from_url(source_url);
        let source = match create_source(&source_config) {
            Ok(s) => s,
            Err(e) => {
                return ProcessingResult::Failed {
                    error: format!("Failed to create source: {}", e),
                };
            }
        };

        // Step 6: Create writer using the consolidated factory
        let factory_config = LerobotWriterConfig::new(&output_path_str, config.clone());
        let mut writer_result = match create_lerobot_writer(&factory_config) {
            Ok(result) => result,
            Err(e) => {
                return ProcessingResult::Failed {
                    error: format!("Failed to create writer: {}", e),
                };
            }
        };

        // Step 7: Configure writer with episode allocation (if distributed)
        if let Some(ref alloc) = episode_allocation {
            writer_result
                .writer
                .set_episode_index(alloc.episode_index as usize);
            writer_result
                .writer
                .set_episodes_per_chunk(self.episodes_per_chunk);
            tracing::debug!(
                unit_id = %unit.id,
                episode_index = alloc.episode_index,
                chunk_index = alloc.chunk_index,
                episodes_per_chunk = self.episodes_per_chunk,
                "Configured writer with episode allocation"
            );
        }

        // Step 8: Build topic mappings and pipeline config
        let topic_mappings = Self::build_topic_mappings(&config);
        let pipeline_config = Self::create_pipeline_config(&config, topic_mappings);

        // Step 9: Execute pipeline with timeout
        let ctx = PipelineContext {
            source,
            source_config,
            writer: writer_result.writer,
            pipeline_config,
            unit_id: unit.id.clone(),
            job_registry: self.job_registry.clone(),
            episode_index: episode_allocation.as_ref().map(|a| a.episode_index),
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
            episode_index,
        } = ctx;

        // Log episode allocation if present
        if let Some(ep_idx) = episode_index {
            tracing::info!(
                unit_id = %unit_id,
                episode_index = ep_idx,
                "Starting pipeline with allocated episode"
            );
        }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_result_with_episode() {
        let result = ExecutionResult {
            frames_processed: 1000,
            videos_created: 2,
            episode_index: Some(42),
        };

        assert_eq!(result.frames_processed, 1000);
        assert_eq!(result.videos_created, 2);
        assert_eq!(result.episode_index, Some(42));
    }

    #[test]
    fn test_execution_result_without_episode() {
        let result = ExecutionResult {
            frames_processed: 500,
            videos_created: 1,
            episode_index: None,
        };

        assert_eq!(result.frames_processed, 500);
        assert_eq!(result.videos_created, 1);
        assert!(result.episode_index.is_none());
    }

    #[test]
    fn test_default_episodes_per_chunk() {
        assert_eq!(DEFAULT_EPISODES_PER_CHUNK, 500);
    }
}
