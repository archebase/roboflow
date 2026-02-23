// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Discover stage for finding input files.

use std::path::Path;

use roboflow_core::Result;
use roboflow_executor::stage::{PartitionId, Stage, StageId};
use roboflow_executor::task::{Task, TaskContext, TaskResult, TaskStatus};
use roboflow_storage::StorageFactory;

/// Supported file extensions for robotics data files.
const SUPPORTED_EXTENSIONS: [&str; 2] = [".bag", ".mcap"];

/// Stage for discovering input files.
///
/// This stage scans a source prefix (local or cloud storage) and
/// identifies files to be processed. It produces a list of file URLs
/// as output.
///
/// # Output
///
/// A list of discovered file URLs (one per line in a text output).
pub struct DiscoverStage {
    source_prefix: String,
}

impl DiscoverStage {
    /// Create a new discover stage.
    ///
    /// # Arguments
    ///
    /// * `source_prefix` - URL prefix to scan (e.g., `s3://bucket/input/` or `/local/path/`).
    pub fn new(source_prefix: impl Into<String>) -> Self {
        Self {
            source_prefix: source_prefix.into(),
        }
    }

    /// Check if a file has a supported extension.
    fn is_supported_file(path: &str) -> bool {
        let path_lower = path.to_lowercase();
        SUPPORTED_EXTENSIONS
            .iter()
            .any(|ext| path_lower.ends_with(ext))
    }
}

impl Stage for DiscoverStage {
    fn id(&self) -> StageId {
        StageId(0)
    }

    fn name(&self) -> &str {
        "discover"
    }

    fn partition_count(&self) -> usize {
        1
    }

    fn create_task(&self, _partition: PartitionId) -> Box<dyn Task> {
        Box::new(DiscoverTask {
            source_prefix: self.source_prefix.clone(),
        })
    }
}

/// Task for discovering input files.
struct DiscoverTask {
    source_prefix: String,
}

#[async_trait::async_trait]
impl Task for DiscoverTask {
    async fn execute(&mut self, _ctx: &TaskContext) -> Result<TaskResult> {
        tracing::info!(
            source_prefix = %self.source_prefix,
            "Discovering input files"
        );

        // Create storage backend from URL
        let storage = StorageFactory::from_env()
            .create(&self.source_prefix)
            .map_err(|e| {
                roboflow_core::RoboflowError::other(format!(
                    "Failed to create storage for {}: {}",
                    self.source_prefix, e
                ))
            })?;

        // Determine the prefix path for listing
        let prefix_path = if self.source_prefix.starts_with("s3://")
            || self.source_prefix.starts_with("oss://")
        {
            // For S3/OSS, extract the path after the bucket
            let url = self.source_prefix.clone();
            let path = url.trim_start_matches("s3://").trim_start_matches("oss://");
            let parts: Vec<&str> = path.splitn(2, '/').collect();
            if parts.len() > 1 {
                format!("/{}", parts[1])
            } else {
                "/".to_string()
            }
        } else {
            // For local paths
            self.source_prefix.clone()
        };

        // List objects in the prefix
        let objects = storage.list(Path::new(&prefix_path)).map_err(|e| {
            roboflow_core::RoboflowError::other(format!(
                "Failed to list files in {}: {}",
                prefix_path, e
            ))
        })?;

        // Filter to supported files and collect URLs
        let files: Vec<String> = objects
            .into_iter()
            .filter(|obj| !obj.is_dir && DiscoverStage::is_supported_file(&obj.path))
            .map(|obj| {
                if self.source_prefix.starts_with("s3://")
                    || self.source_prefix.starts_with("oss://")
                {
                    // Reconstruct full URL for cloud storage
                    format!("{}{}", self.source_prefix.trim_end_matches('/'), obj.path)
                } else {
                    // Local path
                    obj.path
                }
            })
            .collect();

        tracing::info!(file_count = files.len(), "Discovered input files");

        Ok(TaskResult {
            outputs: files, // Output: list of discovered file URLs
            metrics: Default::default(),
            status: TaskStatus::Success,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_discover_stage() {
        let stage = DiscoverStage::new("s3://bucket/input/");

        assert_eq!(stage.id(), StageId(0));
        assert_eq!(stage.name(), "discover");
        assert_eq!(stage.partition_count(), 1);
    }

    #[test]
    fn test_is_supported_file() {
        assert!(DiscoverStage::is_supported_file("/path/to/file.bag"));
        assert!(DiscoverStage::is_supported_file("/path/to/file.mcap"));
        assert!(DiscoverStage::is_supported_file("/path/to/file.BAG"));
        assert!(!DiscoverStage::is_supported_file("/path/to/file.txt"));
        assert!(!DiscoverStage::is_supported_file("/path/to/file"));
    }
}
