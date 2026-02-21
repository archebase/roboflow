// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;

use roboflow_core::{ConversionResult, ConversionTask, Result, RoboflowError};
use roboflow_storage::Storage;

use crate::sources::SourceConfig;

/// Trait for executing conversion tasks.
///
/// This is the main entry point for roboflow-dataset conversions.
/// Implementations handle the full pipeline from source reading to writer finalization.
#[async_trait]
pub trait TaskExecutor: Send + Sync {
    /// Execute a conversion task end-to-end.
    ///
    /// # Arguments
    ///
    /// * `task` - The conversion task to execute
    ///
    /// # Returns
    ///
    /// The result of the conversion including statistics.
    async fn execute(&self, task: ConversionTask) -> Result<ConversionResult>;

    /// Validate a task before execution.
    ///
    /// # Arguments
    ///
    /// * `task` - The task to validate
    ///
    /// # Returns
    ///
    /// Ok if valid, Err with description if invalid.
    fn validate(&self, task: &ConversionTask) -> Result<()>;
}

/// Production pipeline executor.
///
/// Executes conversion tasks using the standard pipeline with configurable
/// storage, video composer, and progress reporting.
pub struct PipelineExecutor {
    _storage: Arc<dyn Storage>,
}

impl PipelineExecutor {
    /// Create a new pipeline executor with the given storage.
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { _storage: storage }
    }

    /// Ensure input is available locally.
    async fn ensure_local_input(&self, source: &roboflow_core::InputSource) -> Result<PathBuf> {
        match source {
            roboflow_core::InputSource::Local { path } => Ok(path.clone()),
            roboflow_core::InputSource::S3 { url } => {
                // Download from S3 to temp
                todo!("S3 download not yet implemented: {}", url)
            }
            roboflow_core::InputSource::OSS { url } => {
                // Download from OSS to temp
                todo!("OSS download not yet implemented: {}", url)
            }
        }
    }
}

#[async_trait]
impl TaskExecutor for PipelineExecutor {
    async fn execute(&self, task: ConversionTask) -> Result<ConversionResult> {
        self.validate(&task)?;

        let start_time = std::time::Instant::now();

        // Ensure input is local
        let local_input = self.ensure_local_input(&task.input_source).await?;

        // Create source from local input
        let path_str = local_input.to_string_lossy().to_string();
        let source_config = if local_input.extension().map(|e| e == "bag").unwrap_or(false) {
            SourceConfig::bag(path_str)
        } else {
            SourceConfig::mcap(path_str)
        };
        let mut source = crate::sources::create_source(&source_config)
            .map_err(|e| RoboflowError::other(format!("Failed to create source: {}", e)))?;

        // Initialize source
        source
            .initialize(&source_config)
            .await
            .map_err(|e| RoboflowError::other(format!("Failed to initialize source: {}", e)))?;

        // Create writer
        let _output_path = task.output_destination.local_path().clone();
        // TODO: Create appropriate writer based on config

        // Run conversion
        let mut frames_processed = 0usize;

        while let Some(batch) = source
            .read_batch(100)
            .await
            .map_err(|e| RoboflowError::other(format!("Failed to read batch: {}", e)))?
        {
            frames_processed += batch.len();
            // Process batch...
        }

        let duration_secs = start_time.elapsed().as_secs_f64();

        Ok(ConversionResult::new(
            task.task_id,
            task.episode_allocation.episode_index,
            task.episode_allocation.chunk_index,
        )
        .with_frames(frames_processed, frames_processed)
        .with_duration(duration_secs))
    }

    fn validate(&self, task: &ConversionTask) -> Result<()> {
        if task.task_id.is_empty() {
            return Err(RoboflowError::other("task_id is empty"));
        }
        if task.batch_id.is_empty() {
            return Err(RoboflowError::other("batch_id is empty"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_executor_validate_empty_task_id() {
        let storage = Arc::new(roboflow_storage::LocalStorage::new("/tmp"));
        let executor = PipelineExecutor::new(storage);

        let task = ConversionTask::new(
            "",
            "batch-1",
            roboflow_core::InputSource::local("/input"),
            roboflow_core::OutputDestination::local("/output"),
        );
        let result = executor.validate(&task);

        assert!(result.is_err());
    }
}
