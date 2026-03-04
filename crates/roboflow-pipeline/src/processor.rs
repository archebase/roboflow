// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Bag-to-LeRobot work processor for distributed pipeline.
//!
//! Implements [`WorkProcessor`] to convert bag/MCAP files into LeRobot v2.1
//! format within the distributed worker framework.

use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use roboflow_dataset::formats::common::DatasetBaseConfig;
use roboflow_dataset::formats::common::config::MappingType;
use roboflow_dataset::formats::lerobot::{
    DatasetConfig, LerobotConfig, LerobotWriterConfig, VideoConfig, create_lerobot_writer,
};
use roboflow_dataset::sources::{SourceConfig, create_source};
use roboflow_distributed::batch::{WorkUnitKeys, deserialize_work_unit_compat};
use roboflow_distributed::worker::WorkProcessor;
use roboflow_distributed::{
    EpisodeAllocation, EpisodeAllocator, MergeCoordinator, ProcessingResult, TiKVEpisodeAllocator,
    TikvError, WorkUnit, WorkerConfig,
};
use roboflow_storage::StorageFactory;

use crate::{DatasetPipelineConfig, DatasetPipelineExecutor};

/// Work processor that converts bag/MCAP files to LeRobot v2.1 format.
pub struct BagToLerobotProcessor {
    pod_id: String,
    tikv: Arc<roboflow_distributed::TikvClient>,
    merge_coordinator: Arc<MergeCoordinator>,
    config: WorkerConfig,
}

impl BagToLerobotProcessor {
    /// Create a new processor.
    pub fn new(
        pod_id: String,
        tikv: Arc<roboflow_distributed::TikvClient>,
        merge_coordinator: Arc<MergeCoordinator>,
        config: WorkerConfig,
    ) -> Self {
        Self {
            pod_id,
            tikv,
            merge_coordinator,
            config,
        }
    }

    async fn ensure_episode_allocation(
        &self,
        work_unit: &WorkUnit,
    ) -> Result<EpisodeAllocation, TikvError> {
        if let Some(episode_index) = work_unit.episode_index {
            tracing::debug!(
                batch_id = %work_unit.batch_id,
                unit_id = %work_unit.id,
                episode_index,
                "Reusing persisted episode allocation"
            );
            return Ok(EpisodeAllocation::new(
                episode_index,
                self.config.episodes_per_chunk,
            ));
        }

        let allocator = TiKVEpisodeAllocator::new(
            self.tikv.clone(),
            work_unit.batch_id.clone(),
            self.config.episodes_per_chunk,
        );

        let allocation = allocator
            .allocate()
            .await
            .map_err(|e| TikvError::Other(format!("Allocation failed: {e}")))?;

        let unit_key = WorkUnitKeys::unit(&work_unit.batch_id, &work_unit.id);
        let unit_data = self.tikv.get(unit_key.clone()).await?.ok_or_else(|| {
            TikvError::Other(format!(
                "Work unit not found while persisting episode allocation: {}/{}",
                work_unit.batch_id, work_unit.id
            ))
        })?;

        let mut stored_unit: WorkUnit = deserialize_work_unit_compat(&unit_data)
            .map_err(|e| TikvError::Deserialization(e.to_string()))?;

        if let Some(existing) = stored_unit.episode_index {
            tracing::debug!(
                batch_id = %work_unit.batch_id,
                unit_id = %work_unit.id,
                episode_index = existing,
                "Work unit already has persisted episode allocation"
            );
            return Ok(EpisodeAllocation::new(
                existing,
                self.config.episodes_per_chunk,
            ));
        }

        stored_unit.episode_index = Some(allocation.episode_index);
        let encoded = bincode::serialize(&stored_unit)
            .map_err(|e| TikvError::Serialization(e.to_string()))?;
        self.tikv.put(unit_key, encoded).await?;

        tracing::debug!(
            batch_id = %work_unit.batch_id,
            unit_id = %work_unit.id,
            episode_index = allocation.episode_index,
            "Allocated and persisted episode index for work unit"
        );

        Ok(allocation)
    }
}

#[async_trait::async_trait]
impl WorkProcessor for BagToLerobotProcessor {
    async fn process(&self, work_unit: &WorkUnit) -> Result<ProcessingResult, TikvError> {
        let is_cloud = work_unit.output_path.starts_with("s3://")
            || work_unit.output_path.starts_with("oss://");

        let input_file = work_unit
            .files
            .first()
            .map(|f| f.url.clone())
            .ok_or_else(|| TikvError::Other("No input files".to_string()))?;

        tracing::info!(
            batch_id = %work_unit.batch_id,
            unit_id = %work_unit.id,
            input_file = %input_file,
            is_cloud = is_cloud,
            "BagToLerobotProcessor: starting to process work unit"
        );

        let local_temp = std::env::temp_dir().join(format!(
            "roboflow_worker_{}_{}",
            self.pod_id,
            work_unit.id.replace(|c: char| !c.is_alphanumeric(), "_")
        ));

        let output_dir = if is_cloud {
            local_temp.clone()
        } else {
            PathBuf::from(&work_unit.output_path)
        };

        if is_cloud {
            std::fs::create_dir_all(&local_temp)
                .map_err(|e| TikvError::Other(format!("Temp dir error: {e}")))?;
        }

        let allocation = self.ensure_episode_allocation(work_unit).await?;
        tracing::debug!(
            batch_id = %work_unit.batch_id,
            unit_id = %work_unit.id,
            episode_index = allocation.episode_index,
            "Episode resolved for work unit"
        );

        // Load lerobot config to get topic mappings
        let lerobot_config = match self.tikv.get_config(&work_unit.config_hash).await {
            Ok(Some(config_record)) => LerobotConfig::from_toml(&config_record.content)
                .unwrap_or_else(|_| Self::default_lerobot_config(&allocation)),
            _ => Self::default_lerobot_config(&allocation),
        };

        let image_topics: Vec<String> = lerobot_config
            .mappings
            .iter()
            .filter(|m| m.mapping_type == MappingType::Image)
            .map(|m| m.topic.clone())
            .collect();
        let state_topics: Vec<String> = lerobot_config
            .mappings
            .iter()
            .filter(|m| m.mapping_type == MappingType::State)
            .map(|m| m.topic.clone())
            .collect();

        tracing::debug!(
            batch_id = %work_unit.batch_id,
            unit_id = %work_unit.id,
            image_topics = ?image_topics,
            state_topics = ?state_topics,
            "Extracted topics from config"
        );

        let source_config = if input_file.ends_with(".mcap") {
            SourceConfig::mcap(&input_file)
        } else if input_file.ends_with(".bag") {
            let mut config = SourceConfig::bag(&input_file);
            if !image_topics.is_empty() {
                config = config.with_option("image_topics", serde_json::json!(image_topics));
            }
            if !state_topics.is_empty() {
                config = config.with_option("state_topics", serde_json::json!(state_topics));
            }
            config = config.with_option("fps", serde_json::json!(lerobot_config.dataset.fps));
            config
        } else {
            return Err(TikvError::Other(format!(
                "Unsupported input source: {input_file}"
            )));
        };

        let mut source = create_source(&source_config)
            .map_err(|e| TikvError::Other(format!("Source error: {e}")))?;

        let init_timeout_secs: u64 = env::var("SOURCE_INIT_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(300);
        match tokio::time::timeout(
            Duration::from_secs(init_timeout_secs),
            source.initialize(&source_config),
        )
        .await
        {
            Ok(result) => {
                result.map_err(|e| TikvError::Other(format!("Init error: {e}")))?;
            }
            Err(_) => {
                return Err(TikvError::Other(
                    "Source initialization timed out".to_string(),
                ));
            }
        }

        let progress_log_secs: u64 = env::var("ROBOFLOW_PROGRESS_LOG_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|secs| *secs > 0)
            .unwrap_or(2);
        let log_interval = Duration::from_secs(progress_log_secs);

        // Configure writer
        let mut lerobot_config = lerobot_config;
        lerobot_config.streaming.finalize_metadata_in_coordinator = true;

        let topic_mappings: HashMap<String, String> = lerobot_config
            .mappings
            .iter()
            .map(|m| (m.topic.clone(), m.feature.clone()))
            .collect();

        let episode_output_dir =
            output_dir.join(format!("episode_{:06}", allocation.episode_index));
        std::fs::create_dir_all(&episode_output_dir)
            .map_err(|e| TikvError::Other(format!("Mkdir error: {e}")))?;

        let writer_config = LerobotWriterConfig::new(
            episode_output_dir.to_string_lossy().to_string(),
            lerobot_config,
        );

        let writer = create_lerobot_writer(&writer_config)
            .map_err(|e| TikvError::Other(format!("Writer error: {e}")))?
            .writer;

        let num_threads = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4);

        let mut executor = DatasetPipelineExecutor::parallel(
            writer,
            DatasetPipelineConfig::with_fps(30).with_topic_mappings(topic_mappings),
            num_threads,
        );

        tracing::info!(
            batch_id = %work_unit.batch_id,
            unit_id = %work_unit.id,
            "Starting to read and process messages"
        );

        let mut total_messages: u64 = 0;
        let processing_start = Instant::now();
        let mut last_log_time = Instant::now();

        loop {
            match source.read_batch(100).await {
                Ok(Some(messages)) => {
                    let msg_count = messages.len() as u64;
                    total_messages += msg_count;

                    let now = Instant::now();
                    if now.duration_since(last_log_time) >= log_interval {
                        let elapsed_sec = now.duration_since(processing_start).as_secs_f64();
                        let messages_per_sec = if elapsed_sec > 0.0 {
                            total_messages as f64 / elapsed_sec
                        } else {
                            0.0
                        };

                        tracing::info!(
                            batch_id = %work_unit.batch_id,
                            unit_id = %work_unit.id,
                            total_messages,
                            last_batch_size = msg_count,
                            elapsed_sec,
                            messages_per_sec,
                            "Processing progress"
                        );
                        last_log_time = now;
                    }

                    executor
                        .process_messages(messages)
                        .map_err(|e| TikvError::Other(format!("Pipeline error: {e}")))?;
                }
                Ok(None) => {
                    let elapsed_sec = processing_start.elapsed().as_secs_f64();
                    let messages_per_sec = if elapsed_sec > 0.0 {
                        total_messages as f64 / elapsed_sec
                    } else {
                        0.0
                    };

                    tracing::info!(
                        batch_id = %work_unit.batch_id,
                        unit_id = %work_unit.id,
                        total_messages,
                        elapsed_sec,
                        messages_per_sec,
                        "Finished reading messages, finalizing"
                    );
                    break;
                }
                Err(e) => {
                    return Err(TikvError::Other(format!("Read error: {e}")));
                }
            }
        }

        let pipeline_stats = executor
            .finalize()
            .map_err(|e| TikvError::Other(format!("Finalize error: {e}")))?;

        let frame_count = pipeline_stats.frames_written as u64;

        if is_cloud {
            let storage_factory = StorageFactory::from_env();
            let storage = storage_factory
                .create(&work_unit.output_path)
                .map_err(|e| TikvError::Other(format!("Storage error: {e}")))?;

            let staging_path = roboflow_distributed::build_staging_path(
                &work_unit.output_path,
                &work_unit.batch_id,
                &self.pod_id,
                &work_unit.id,
            )
            .map_err(TikvError::Other)?;

            tracing::info!(
                batch_id = %work_unit.batch_id,
                unit_id = %work_unit.id,
                staging_path = %staging_path,
                "Uploading to cloud storage staging"
            );

            let staging_prefix = staging_path
                .parse::<roboflow_storage::StorageUrl>()
                .map_err(|e| {
                    TikvError::Other(format!("Invalid staging path '{}': {}", staging_path, e))
                })?
                .path()
                .trim_start_matches('/')
                .to_string();

            let uploaded = roboflow_storage::upload::upload_directory_recursive(
                storage,
                &episode_output_dir,
                std::path::Path::new(&staging_prefix),
            )
            .map_err(|e| TikvError::Other(format!("Upload error: {e}")))?;

            tracing::info!(
                batch_id = %work_unit.batch_id,
                unit_id = %work_unit.id,
                staging_path = %staging_path,
                files_uploaded = uploaded.len(),
                total_frames = frame_count,
                "Staging upload complete, registering with merge coordinator"
            );

            self.merge_coordinator
                .register_staging_complete(
                    &work_unit.batch_id,
                    &self.pod_id,
                    staging_path,
                    frame_count,
                )
                .await?;

            if let Err(e) = std::fs::remove_dir_all(&local_temp) {
                tracing::warn!(
                    temp_dir = %local_temp.display(),
                    error = %e,
                    "Failed to clean up temp directory"
                );
            }
        }

        Ok(ProcessingResult::Success {
            episode_index: allocation.episode_index,
            frame_count,
            episode_stats: Some(roboflow_distributed::EpisodeStats {
                episode_index: allocation.episode_index as usize,
                frame_count: pipeline_stats.frames_written,
                feature_stats: HashMap::new(),
                task_indices: Vec::new(),
                recorded_at: Some(chrono::Utc::now().timestamp()),
            }),
        })
    }
}

impl BagToLerobotProcessor {
    fn default_lerobot_config(allocation: &EpisodeAllocation) -> LerobotConfig {
        LerobotConfig {
            dataset: DatasetConfig {
                base: DatasetBaseConfig {
                    name: format!("episode_{:06}", allocation.episode_index),
                    fps: 30,
                    robot_type: None,
                },
                env_type: None,
            },
            mappings: vec![],
            video: VideoConfig::default(),
            annotation_file: None,
            flushing: Default::default(),
            streaming: Default::default(),
        }
    }
}
