// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Discover stage: scans storage sources to find bag/MCAP files.

use std::path::PathBuf;

use roboflow_core::Result;
use roboflow_executor::{PartitionId, Stage, StageId, Task, TaskContext, TaskResult};
use roboflow_storage::{StorageFactory, discover_files};

/// Pipeline stage that discovers bag/MCAP files in storage.
pub struct DiscoverStage {
    id: StageId,
    source_urls: Vec<String>,
    file_extensions: Vec<String>,
}

impl DiscoverStage {
    /// Create a new discover stage.
    ///
    /// # Arguments
    ///
    /// * `id` - Stage identifier
    /// * `source_urls` - URLs to scan (local paths, s3://, oss://)
    /// * `file_extensions` - File extensions to match (e.g., [".bag", ".mcap"])
    pub fn new(id: StageId, source_urls: Vec<String>, file_extensions: Vec<String>) -> Self {
        Self {
            id,
            source_urls,
            file_extensions,
        }
    }

    /// Create a discover stage for bag/MCAP files with default extensions.
    pub fn for_bags(id: StageId, source_urls: Vec<String>) -> Self {
        Self::new(
            id,
            source_urls,
            vec![".bag".to_string(), ".mcap".to_string()],
        )
    }

    /// Convenience constructor from a single input directory (backward compat).
    pub fn from_dir(id: StageId, input_dir: impl Into<PathBuf>) -> Self {
        let dir: PathBuf = input_dir.into();
        Self::new(
            id,
            vec![format!("file://{}", dir.display())],
            vec![".bag".to_string(), ".mcap".to_string()],
        )
    }
}

impl Stage for DiscoverStage {
    fn id(&self) -> StageId {
        self.id
    }

    fn name(&self) -> &str {
        "discover"
    }

    fn partition_count(&self) -> usize {
        1
    }

    fn create_task(&self, _partition: PartitionId) -> Box<dyn Task> {
        Box::new(DiscoverTask {
            source_urls: self.source_urls.clone(),
            file_extensions: self.file_extensions.clone(),
        })
    }
}

struct DiscoverTask {
    source_urls: Vec<String>,
    file_extensions: Vec<String>,
}

#[async_trait::async_trait]
impl Task for DiscoverTask {
    async fn execute(&mut self, ctx: &TaskContext) -> Result<TaskResult> {
        let start = std::time::Instant::now();
        let factory = StorageFactory::from_env();
        let mut discovered: Vec<String> = Vec::new();

        for source_url in &self.source_urls {
            if ctx.is_cancelled() {
                return Ok(TaskResult {
                    outputs: discovered,
                    metrics: roboflow_executor::TaskMetrics::default(),
                    status: roboflow_executor::TaskStatus::Cancelled,
                });
            }

            let storage = factory.create(source_url).map_err(|e| {
                roboflow_core::RoboflowError::storage(
                    "discover",
                    format!("Failed to create storage for {}: {}", source_url, e),
                    false,
                )
            })?;

            // Build glob pattern from extensions
            let pattern = if self.file_extensions.len() == 1 {
                format!("**/*{}", self.file_extensions[0])
            } else {
                // Use multiple discover calls for multiple extensions
                String::new()
            };

            if !pattern.is_empty() {
                match discover_files(storage.as_ref(), std::path::Path::new(""), &pattern) {
                    Ok(files) => {
                        for file in files {
                            discovered.push(format!(
                                "{}/{}",
                                source_url.trim_end_matches('/'),
                                file.path
                            ));
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            source = %source_url,
                            error = %e,
                            "Failed to discover files"
                        );
                    }
                }
            } else {
                // Multiple extensions: discover each separately
                for ext in &self.file_extensions {
                    let ext_pattern = format!("**/*{}", ext);
                    match discover_files(storage.as_ref(), std::path::Path::new(""), &ext_pattern) {
                        Ok(files) => {
                            for file in files {
                                discovered.push(format!(
                                    "{}/{}",
                                    source_url.trim_end_matches('/'),
                                    file.path
                                ));
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                source = %source_url,
                                extension = %ext,
                                error = %e,
                                "Failed to discover files"
                            );
                        }
                    }
                }
            }
        }

        let duration = start.elapsed();

        if discovered.is_empty() {
            tracing::warn!(
                sources = ?self.source_urls,
                extensions = ?self.file_extensions,
                duration_sec = duration.as_secs_f64(),
                "Discovery found no files — check source URLs and file extensions"
            );
        }

        tracing::info!(
            files_found = discovered.len(),
            duration_sec = duration.as_secs_f64(),
            "Discovery stage completed"
        );

        Ok(TaskResult {
            outputs: discovered,
            metrics: roboflow_executor::TaskMetrics {
                duration_secs: duration.as_secs_f64(),
                ..Default::default()
            },
            status: roboflow_executor::TaskStatus::Success,
        })
    }
}
