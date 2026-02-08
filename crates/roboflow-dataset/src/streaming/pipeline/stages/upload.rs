// Upload coordinator stage - streaming upload to S3/OSS

use std::io::Write;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Instant;

use crossbeam_channel::Receiver;

use crate::streaming::pipeline::types::{EncodedVideo, PipelineError, PipelineResult};
use roboflow_storage::Storage;

/// Statistics from the upload coordinator stage.
#[derive(Debug, Clone)]
pub struct UploadStats {
    /// Files uploaded
    pub files_uploaded: usize,
    /// Total bytes uploaded
    pub bytes_uploaded: u64,
    /// Processing time in seconds
    pub duration_sec: f64,
}

/// Upload coordinator stage.
///
/// Receives encoded videos and uploads them to cloud storage immediately.
/// Supports S3 and OSS backends via the Storage trait.
pub struct UploadCoordinatorStage {
    /// Episode index (currently unused, reserved for future use)
    _episode_index: usize,
    /// Input receiver for encoded videos
    input_rx: Receiver<EncodedVideo>,
    /// Output storage backend
    storage: Option<Arc<dyn Storage>>,
    /// Output prefix (e.g., "datasets/my_dataset")
    output_prefix: Option<String>,
}

impl UploadCoordinatorStage {
    /// Create a new upload coordinator stage.
    pub fn new(
        _episode_index: usize,
        input_rx: Receiver<EncodedVideo>,
        storage: Option<Arc<dyn Storage>>,
        output_prefix: Option<String>,
    ) -> Self {
        Self {
            _episode_index,
            input_rx,
            storage,
            output_prefix,
        }
    }

    /// Spawn the upload coordinator in a thread.
    pub fn spawn(
        self,
    ) -> JoinHandle<PipelineResult<(UploadStats, crate::streaming::pipeline::StageStats)>> {
        thread::spawn(move || {
            let name = "UploadCoordinator";
            tracing::debug!("{name} starting");

            let start = Instant::now();
            let result = self.run_internal();
            let duration = start.elapsed();

            match &result {
                Ok((upload_stats, _stage_stats)) => {
                    tracing::debug!(
                        duration_sec = duration.as_secs_f64(),
                        files = upload_stats.files_uploaded,
                        bytes = upload_stats.bytes_uploaded,
                        "{name} completed"
                    );
                }
                Err(e) => {
                    tracing::error!(error = %e, "{name} failed");
                }
            }

            result
        })
    }

    fn run_internal(
        &self,
    ) -> PipelineResult<(UploadStats, crate::streaming::pipeline::StageStats)> {
        let mut files_uploaded = 0usize;
        let mut bytes_uploaded = 0u64;

        // If no storage backend configured, skip upload
        let storage = match &self.storage {
            Some(s) => s,
            None => {
                tracing::info!("No storage backend configured, skipping upload");
                // Drain the channel
                while self.input_rx.recv().is_ok() {}
                return Ok((
                    UploadStats {
                        files_uploaded: 0,
                        bytes_uploaded: 0,
                        duration_sec: 0.0,
                    },
                    crate::streaming::pipeline::StageStats {
                        stage: "UploadCoordinator".to_string(),
                        items_processed: 0,
                        items_produced: 0,
                        duration_sec: 0.0,
                        peak_memory_mb: None,
                        metrics: [].into_iter().collect(),
                    },
                ));
            }
        };

        while let Ok(video) = self.input_rx.recv() {
            // Build storage path
            let filename = video
                .local_path
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| PipelineError::ExecutionFailed {
                    stage: "UploadCoordinator".to_string(),
                    reason: "invalid filename".to_string(),
                })?;

            let storage_key = if let Some(prefix) = &self.output_prefix {
                format!("{}/{}", prefix.trim_end_matches('/'), filename)
            } else {
                filename.to_string()
            };

            tracing::debug!(
                local_path = %video.local_path.display(),
                storage_key = %storage_key,
                size = video.size,
                "Uploading video"
            );

            // Upload file using storage.writer()
            let storage_path = std::path::Path::new(&storage_key);

            // Read file content
            let content =
                std::fs::read(&video.local_path).map_err(|e| PipelineError::ExecutionFailed {
                    stage: "UploadCoordinator".to_string(),
                    reason: format!("failed to read video file: {e}"),
                })?;

            // Create writer and upload
            let mut writer =
                storage
                    .writer(storage_path)
                    .map_err(|e| PipelineError::ExecutionFailed {
                        stage: "UploadCoordinator".to_string(),
                        reason: format!("failed to create storage writer: {e}"),
                    })?;

            writer
                .write_all(&content)
                .map_err(|e| PipelineError::ExecutionFailed {
                    stage: "UploadCoordinator".to_string(),
                    reason: format!("failed to write to storage: {e}"),
                })?;

            writer.flush().map_err(|e| PipelineError::ExecutionFailed {
                stage: "UploadCoordinator".to_string(),
                reason: format!("failed to flush storage writer: {e}"),
            })?;

            // Delete local file after successful upload
            std::fs::remove_file(&video.local_path).ok();

            files_uploaded += 1;
            bytes_uploaded += video.size;
        }

        Ok((
            UploadStats {
                files_uploaded,
                bytes_uploaded,
                duration_sec: 0.0,
            },
            crate::streaming::pipeline::StageStats {
                stage: "UploadCoordinator".to_string(),
                items_processed: files_uploaded,
                items_produced: files_uploaded,
                duration_sec: 0.0,
                peak_memory_mb: None,
                metrics: [
                    (
                        "files_uploaded".to_string(),
                        serde_json::json!(files_uploaded),
                    ),
                    (
                        "bytes_uploaded".to_string(),
                        serde_json::json!(bytes_uploaded),
                    ),
                ]
                .into_iter()
                .collect(),
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upload_coordinator_creation() {
        use crossbeam_channel::bounded;
        let (_tx, rx) = bounded(10);
        let stage = UploadCoordinatorStage::new(0, rx, None, None);
        assert_eq!(stage._episode_index, 0);
        assert!(stage.storage.is_none());
        assert!(stage.output_prefix.is_none());
    }
}
