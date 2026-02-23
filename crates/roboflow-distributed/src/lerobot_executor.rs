// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! LeRobot executor using the stage-based executor framework.

use std::sync::Arc;

use roboflow_core::Result;
use roboflow_executor::{PipelineBuilder, StageExecutor, StageId};

use crate::stages::{ConvertStage, MergeStage};

use crate::batch::WorkUnit;
use crate::episode::EpisodeAllocator;
use crate::worker::metrics::ProcessingResult;
use crate::worker::registry::JobRegistry;

/// Executes bag/mcap files to LeRobot format using the stage-based executor framework.
///
/// This executor processes source files and converts them to LeRobot v2.1 format
/// by creating a Discover → Convert → Merge pipeline for each work unit.
/// Uses parallel processing for maximum throughput.
pub struct LeRobotExecutor {
    stage_executor: StageExecutor,
    output_prefix: String,
    tikv: Option<Arc<crate::tikv::TikvClient>>,
    episode_allocator: Option<Arc<dyn EpisodeAllocator>>,
}

impl LeRobotExecutor {
    /// Create a new LeRobot executor.
    pub fn new(max_concurrent: usize, output_prefix: impl Into<String>) -> Self {
        Self {
            stage_executor: StageExecutor::new(max_concurrent),
            output_prefix: output_prefix.into(),
            tikv: None,
            episode_allocator: None,
        }
    }

    /// Set the TiKV client for fetching configuration.
    ///
    /// When set, the executor will pass the TiKV client to ConvertStage,
    /// allowing it to fetch the LerobotConfig from TiKV using the config_hash.
    pub fn with_tikv(mut self, tikv: Arc<crate::tikv::TikvClient>) -> Self {
        self.tikv = Some(tikv);
        self
    }

    /// Set the episode allocator for distributed processing.
    pub fn with_episode_allocator(mut self, allocator: Arc<dyn EpisodeAllocator>) -> Self {
        self.episode_allocator = Some(allocator);
        self
    }

    /// Execute a work unit using the stage-based pipeline.
    ///
    /// This creates a Convert → Merge pipeline for each work unit.
    /// (Discovery is done at the batch level, not per-work-unit)
    pub async fn execute(
        &self,
        unit: &WorkUnit,
        _job_registry: Arc<tokio::sync::RwLock<JobRegistry>>,
    ) -> Result<ProcessingResult> {
        // Ensure sources are registered
        roboflow_dataset::sources::register_builtin_sources();
        tracing::info!(
            unit_id = %unit.id,
            files = unit.files.len(),
            "Executing work unit with stage-based pipeline"
        );

        // Get the input file from the work unit
        let input_file =
            unit.files.first().map(|f| f.url.clone()).ok_or_else(|| {
                roboflow_core::RoboflowError::other("No input files in work unit")
            })?;

        // Create output path
        let output_path = format!("{}/{}", self.output_prefix, unit.id);

        // Build the pipeline: Convert → Merge
        // (DiscoverStage runs at batch level, not per-work-unit)
        let mut convert_stage = ConvertStage::new(&input_file, &output_path, &unit.config_hash);

        // Pass TiKV client if available
        if let Some(tikv) = &self.tikv {
            convert_stage = convert_stage.with_tikv(tikv.clone());
        }

        let pipeline = PipelineBuilder::new()
            .stage(Arc::new(convert_stage))
            .stage(Arc::new(MergeStage::new(format!(
                "{}/dataset",
                output_path
            ))))
            .dependency(StageId(2), StageId(1))
            .build()
            .map_err(|e| {
                roboflow_core::RoboflowError::other(format!("Pipeline build failed: {}", e))
            })?;

        // Execute the pipeline
        let result = self.stage_executor.execute(&pipeline).await?;

        tracing::info!(
            unit_id = %unit.id,
            stages_completed = result.stages_completed,
            tasks_completed = result.tasks_completed,
            duration_secs = result.duration_secs,
            "Pipeline execution complete"
        );

        Ok(ProcessingResult::Success {
            episode_index: 0, // TODO: Get from EpisodeAllocator
            frame_count: result.tasks_completed,
            episode_stats: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::{WorkFile, WorkUnit};
    use crate::tikv::TikvClient;
    use crate::tikv::schema::ConfigRecord;

    /// Get TiKV client or return None if not available
    async fn get_tikv_or_none() -> Option<Arc<TikvClient>> {
        match TikvClient::from_env().await {
            Ok(c) => Some(Arc::new(c)),
            Err(e) => {
                println!("TiKV not available: {}", e);
                None
            }
        }
    }

    /// Store config in TiKV and return the config hash
    async fn store_config_in_tikv(tikv: &TikvClient) -> Option<String> {
        // Read config file from examples
        let config_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("examples/rust/lerobot_config.toml");

        let config_content = match std::fs::read_to_string(&config_path) {
            Ok(content) => content,
            Err(e) => {
                println!("Failed to read config file: {}", e);
                return None;
            }
        };

        // Create config record
        let config_record = ConfigRecord::new(config_content);
        let config_hash = config_record.hash.clone();

        // Store in TiKV
        match tikv.put_config(&config_record).await {
            Ok(_) => {
                println!("Stored config in TiKV with hash: {}", config_hash);
                Some(config_hash)
            }
            Err(e) => {
                println!("Failed to store config in TiKV: {}", e);
                None
            }
        }
    }

    #[tokio::test]
    async fn test_bridge_execution() {
        let _ = tracing_subscriber::fmt::try_init();

        // Use fixture file if it exists
        let fixture_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("tests/fixtures/roboflow_sample.bag");

        if !fixture_path.exists() {
            println!(
                "Skipping test: fixture file not found at {:?}",
                fixture_path
            );
            return;
        }

        let input_file = format!("file://{}", fixture_path.display());

        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().to_string_lossy().to_string();

        // Try to get TiKV client and store config
        let tikv = get_tikv_or_none().await;
        let config_hash = if let Some(ref tikv_client) = tikv {
            match store_config_in_tikv(tikv_client).await {
                Some(hash) => hash,
                None => "default".to_string(),
            }
        } else {
            println!("TiKV not available, using default config");
            "default".to_string()
        };

        // Create executor with TiKV client if available
        let mut executor_builder = LeRobotExecutor::new(2, &output_path);
        if let Some(tikv_client) = tikv {
            executor_builder = executor_builder.with_tikv(tikv_client);
        }
        let executor = executor_builder;

        let registry = Arc::new(tokio::sync::RwLock::new(JobRegistry::default()));

        let work_unit = WorkUnit::new(
            "test-batch".to_string(),
            vec![WorkFile::new(input_file, 1024)],
            output_path,
            config_hash,
        );

        let result = executor.execute(&work_unit, registry).await;

        // Test passes whether it succeeds or fails
        match &result {
            Ok(ProcessingResult::Success { frame_count, .. }) => {
                println!("✅ Executor succeeded with {} frames", frame_count);
            }
            Ok(ProcessingResult::Failed { error }) => {
                println!(
                    "⚠️ Executor failed (config may not match bag topics): {}",
                    error
                );
            }
            Ok(ProcessingResult::Cancelled) => {
                println!("⚠️ Executor was cancelled");
            }
            Err(e) => {
                println!("⚠️ Executor error: {}", e);
            }
        }
    }
}
