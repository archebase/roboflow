// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! LeRobot sink implementation.
//!
//! This sink writes robotics datasets in LeRobot v2.1 format by delegating
//! to `roboflow_dataset::lerobot::LerobotWriter`. Handles episode boundaries,
//! frame conversion (`DatasetFrame` → `AlignedFrame`), and cloud storage.
//!
//! When the output path is `s3://` or `oss://`, the sink uses a local buffer
//! for all file I/O (Parquet, FFmpeg video encoding) then uploads to cloud.
//! FFmpeg cannot write to S3 URLs directly.

use crate::convert::dataset_frame_to_aligned;
use crate::lerobot_factory::{LerobotWriterConfig, create_lerobot_writer};
use crate::{DatasetFrame, Sink, SinkCheckpoint, SinkConfig, SinkError, SinkResult, SinkStats};
use roboflow_dataset::lerobot::LerobotConfig;
use roboflow_dataset::lerobot::writer::LerobotWriter;
use roboflow_dataset::lerobot::{CameraExtrinsic, CameraIntrinsic};
use std::collections::HashMap;

/// LeRobot dataset sink.
///
/// Writes robotics datasets in LeRobot v2.1 format (Parquet + MP4 video).
/// Delegates to the real `LerobotWriter` from `roboflow-dataset`.
pub struct LerobotSink {
    /// Output directory path
    output_path: String,
    /// The dataset writer (created during initialize)
    writer: Option<LerobotWriter>,
    /// Current episode index for boundary detection
    current_episode: usize,
    /// Whether we've seen any frames yet
    has_frames: bool,
    /// Frames written counter
    frames_written: usize,
    /// Episodes completed counter
    episodes_completed: usize,
    /// Start time for duration calculation
    start_time: Option<std::time::Instant>,
}

impl LerobotSink {
    /// Create a new LeRobot sink.
    pub fn new(path: impl Into<String>) -> SinkResult<Self> {
        Ok(Self {
            output_path: path.into(),
            writer: None,
            current_episode: 0,
            has_frames: false,
            frames_written: 0,
            episodes_completed: 0,
            start_time: None,
        })
    }

    /// Create a new LeRobot sink from a SinkConfig.
    pub fn from_config(config: &SinkConfig) -> SinkResult<Self> {
        match &config.sink_type {
            crate::SinkType::Lerobot { path } => Self::new(path),
            _ => Err(SinkError::InvalidConfig(
                "Invalid config for LerobotSink".to_string(),
            )),
        }
    }

    /// Extract LerobotConfig from SinkConfig options, or create a minimal default.
    fn extract_lerobot_config(config: &SinkConfig) -> LerobotConfig {
        // Try to get config from options (set via SinkConfig::lerobot_with_config)
        if let Some(lerobot_config) = config.get_option::<LerobotConfig>("lerobot_config") {
            return lerobot_config;
        }

        // Extract fps from options if available
        let fps = config.get_option::<u32>("fps").unwrap_or(30);
        let name = config
            .get_option::<String>("dataset_name")
            .unwrap_or_else(|| "dataset".to_string());
        let robot_type = config.get_option::<String>("robot_type");

        // Create minimal config
        LerobotConfig {
            dataset: roboflow_dataset::lerobot::DatasetConfig {
                base: roboflow_dataset::common::DatasetBaseConfig {
                    name,
                    fps,
                    robot_type,
                },
                env_type: None,
            },
            mappings: Vec::new(),
            video: Default::default(),
            annotation_file: None,
            flushing: roboflow_dataset::lerobot::FlushingConfig::default(),
            streaming: roboflow_dataset::lerobot::config::StreamingConfig::default(),
        }
    }
}

#[async_trait::async_trait]
impl Sink for LerobotSink {
    async fn initialize(&mut self, config: &SinkConfig) -> SinkResult<()> {
        let lerobot_config = Self::extract_lerobot_config(config);

        tracing::info!(
            output = %self.output_path,
            fps = lerobot_config.dataset.base.fps,
            name = %lerobot_config.dataset.base.name,
            "Initializing LeRobot sink"
        );

        // Use the consolidated factory to create the writer
        let factory_config = LerobotWriterConfig::new(&self.output_path, lerobot_config);
        let result =
            create_lerobot_writer(&factory_config).map_err(|e| SinkError::CreateFailed {
                path: self.output_path.clone().into(),
                error: Box::new(std::io::Error::other(e)),
            })?;

        self.writer = Some(result.writer);
        self.start_time = Some(std::time::Instant::now());

        Ok(())
    }

    async fn write_frame(&mut self, frame: DatasetFrame) -> SinkResult<()> {
        let writer = self.writer.as_mut().ok_or_else(|| {
            SinkError::WriteFailed("Sink not initialized. Call initialize() first.".to_string())
        })?;

        // Detect episode boundary
        if self.has_frames && frame.episode_index != self.current_episode {
            // Finish the previous episode (flush Parquet + encode video)
            let task_index = frame.task_index;
            writer
                .finish_episode(task_index)
                .map_err(|e| SinkError::WriteFailed(format!("Failed to finish episode: {e}")))?;
            self.episodes_completed += 1;

            tracing::debug!(
                episode = self.current_episode,
                frames = self.frames_written,
                "Episode completed"
            );
        }

        self.current_episode = frame.episode_index;
        self.has_frames = true;

        // Extract camera info on first frame and set it on the writer
        if self.frames_written == 0 && !frame.camera_info.is_empty() {
            for (camera_name, info) in &frame.camera_info {
                tracing::info!(
                    camera = %camera_name,
                    width = info.width,
                    height = info.height,
                    fx = info.k[0],
                    fy = info.k[4],
                    "Setting camera calibration"
                );

                // Create LeRobot CameraIntrinsic from ROS CameraInfo
                let intrinsic = CameraIntrinsic {
                    fx: info.k[0],
                    fy: info.k[4],
                    ppx: info.k[2],
                    ppy: info.k[5],
                    distortion_model: info.distortion_model.clone(),
                    k1: info.d.first().copied().unwrap_or(0.0),
                    k2: info.d.get(1).copied().unwrap_or(0.0),
                    k3: info.d.get(4).copied().unwrap_or(0.0),
                    p1: info.d.get(2).copied().unwrap_or(0.0),
                    p2: info.d.get(3).copied().unwrap_or(0.0),
                };

                writer.set_camera_intrinsics(camera_name.clone(), intrinsic);

                // Handle extrinsics from P matrix if available
                // The P matrix (3x4 projection) contains extrinsic info when combined with K
                // P = K [R|t] where R is rotation and t is translation
                if let Some(p) = &info.p {
                    // Extract extrinsics from P matrix using the relation: P = K * [R|t]
                    // We need to compute [R|t] = K_inv * P
                    let k = &info.k;

                    // Compute K inverse (simplified - K is usually upper triangular for cameras)
                    // K = [fx  0  cx]     K_inv = [1/fx    0     -cx/fx   ]
                    //     [ 0 fy  cy]            [  0   1/fy  -cy/fy   ]
                    //     [ 0  0   1]            [  0     0       1     ]
                    let fx = k[0];
                    let fy = k[4];
                    let cx = k[2];
                    let cy = k[5];

                    // P is 3x4: [P0 P1 P2 P3] where each Pi is a column
                    // After K_inv * P, we get [R|t]
                    let r0 = [p[0] / fx, p[1] / fx, p[2] / fx];
                    let r1 = [p[4] / fy, p[5] / fy, p[6] / fy];
                    let r2 = [
                        p[8] - p[0] * cx / fx - p[4] * cy / fy,
                        p[9] - p[1] * cx / fx - p[5] * cy / fy,
                        p[10] - p[2] * cx / fx - p[6] * cy / fy,
                    ];
                    let t = [
                        p[3] / fx,
                        p[7] / fy,
                        p[11] - p[3] * cx / fx - p[7] * cy / fy,
                    ];

                    let rotation_matrix = [r0, r1, r2];

                    let extrinsic = CameraExtrinsic::new(rotation_matrix, t);
                    writer.set_camera_extrinsics(camera_name.clone(), extrinsic);

                    tracing::debug!(
                        camera = %camera_name,
                        rotation = ?rotation_matrix,
                        translation = ?t,
                        "Set camera extrinsics from P matrix"
                    );
                } else if let Some(_r) = &info.r {
                    tracing::debug!(
                        camera = %camera_name,
                        "Camera rectification matrix (R) available but P matrix needed for extrinsics"
                    );
                }
            }
        }

        // Convert DatasetFrame → AlignedFrame and write
        let aligned = dataset_frame_to_aligned(&frame);

        use roboflow_dataset::DatasetWriter;
        writer.write_frame(&aligned).map_err(|e| {
            SinkError::WriteFailed(format!("LerobotWriter write_frame failed: {e}"))
        })?;

        self.frames_written += 1;

        Ok(())
    }

    async fn flush(&mut self) -> SinkResult<()> {
        // Writer handles buffering internally
        Ok(())
    }

    async fn finalize(&mut self) -> SinkResult<SinkStats> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| SinkError::WriteFailed("Sink not initialized".to_string()))?;

        use roboflow_dataset::DatasetWriter;
        let writer_stats = writer
            .finalize()
            .map_err(|e| SinkError::WriteFailed(format!("LerobotWriter finalize failed: {e}")))?;

        let duration = self
            .start_time
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0);

        tracing::info!(
            frames = writer_stats.frames_written,
            images = writer_stats.images_encoded,
            episodes = self.episodes_completed + 1,
            bytes = writer_stats.output_bytes,
            duration_sec = duration,
            "LeRobot sink finalized"
        );

        // Build metrics including staging path for distributed merge
        let metrics = HashMap::from([
            (
                "images_encoded".to_string(),
                serde_json::json!(writer_stats.images_encoded),
            ),
            (
                "state_records".to_string(),
                serde_json::json!(writer_stats.state_records),
            ),
        ]);

        Ok(SinkStats {
            frames_written: writer_stats.frames_written,
            episodes_written: self.episodes_completed + 1,
            duration_sec: duration,
            total_bytes: Some(writer_stats.output_bytes),
            metrics,
        })
    }

    async fn checkpoint(&self) -> SinkResult<SinkCheckpoint> {
        Ok(SinkCheckpoint {
            last_frame_index: self.frames_written,
            last_episode_index: self.current_episode,
            checkpoint_time: chrono::Utc::now().timestamp(),
            data: HashMap::new(),
        })
    }

    fn supports_checkpointing(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lerobot_sink_creation() {
        let sink = LerobotSink::new("/tmp/output");
        assert!(sink.is_ok());
        let sink = sink.unwrap();
        assert_eq!(sink.output_path, "/tmp/output");
    }

    #[test]
    fn test_lerobot_sink_from_config() {
        let config = SinkConfig::lerobot("/tmp/output");
        let sink = LerobotSink::from_config(&config);
        assert!(sink.is_ok());
    }

    #[test]
    fn test_lerobot_sink_invalid_config() {
        let config = SinkConfig::zarr("/tmp/output");
        let sink = LerobotSink::from_config(&config);
        assert!(sink.is_err());
    }

    #[test]
    fn test_extract_default_config() {
        let config = SinkConfig::lerobot("/tmp/output");
        let lerobot_config = LerobotSink::extract_lerobot_config(&config);
        assert_eq!(lerobot_config.dataset.base.fps, 30);
        assert_eq!(lerobot_config.dataset.base.name, "dataset");
    }

    #[test]
    fn test_extract_config_with_options() {
        let config = SinkConfig::lerobot("/tmp/output")
            .with_option("fps", serde_json::json!(60))
            .with_option("dataset_name", serde_json::json!("my_robot"));
        let lerobot_config = LerobotSink::extract_lerobot_config(&config);
        assert_eq!(lerobot_config.dataset.base.fps, 60);
        assert_eq!(lerobot_config.dataset.base.name, "my_robot");
    }
}
