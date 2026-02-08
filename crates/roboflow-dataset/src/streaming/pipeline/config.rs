// Configuration for the streaming dataset pipeline

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use roboflow_storage::Storage;

use super::stage::ChannelConfig;

/// Configuration for the entire streaming dataset pipeline.
#[derive(Clone)]
pub struct PipelineConfig {
    /// Input file path
    pub input_path: PathBuf,

    /// Output storage (local or cloud)
    pub output_storage: Option<Arc<dyn Storage>>,

    /// Output prefix within storage
    pub output_prefix: Option<String>,

    /// Episode index for this conversion
    pub episode_index: usize,

    /// LeRobot configuration
    pub lerobot_config: crate::lerobot::config::LerobotConfig,

    /// Channel configuration
    pub channels: ChannelConfig,

    /// Stage-specific configurations
    pub decoder: DecoderConfig,
    pub aligner: AlignerConfig,
    pub transformer: TransformerConfig,
    pub video_encoder: VideoEncoderConfig,
    pub parquet_writer: ParquetWriterConfig,
    pub upload: UploadConfig,
}

impl PipelineConfig {
    /// Create a new pipeline config.
    pub fn new(
        input_path: impl Into<PathBuf>,
        lerobot_config: crate::lerobot::config::LerobotConfig,
    ) -> Self {
        Self {
            input_path: input_path.into(),
            output_storage: None,
            output_prefix: None,
            episode_index: 0,
            lerobot_config,
            channels: ChannelConfig::default(),
            decoder: DecoderConfig::default(),
            aligner: AlignerConfig::default(),
            transformer: TransformerConfig::default(),
            video_encoder: VideoEncoderConfig::default(),
            parquet_writer: ParquetWriterConfig::default(),
            upload: UploadConfig::default(),
        }
    }

    /// Set output storage.
    pub fn with_output_storage(mut self, storage: Arc<dyn Storage>) -> Self {
        self.output_storage = Some(storage);
        self
    }

    /// Set output prefix.
    pub fn with_output_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.output_prefix = Some(prefix.into());
        self
    }

    /// Set episode index.
    pub fn with_episode_index(mut self, index: usize) -> Self {
        self.episode_index = index;
        self
    }

    /// Use high-throughput settings.
    pub fn high_throughput(mut self) -> Self {
        self.channels = ChannelConfig::high_throughput();
        self.decoder.num_threads = (num_cpus::get() / 2).max(2);
        self.video_encoder.num_threads = (num_cpus::get() / 2).max(2);
        self
    }

    /// Use low-memory settings.
    pub fn low_memory(mut self) -> Self {
        self.channels = ChannelConfig::low_memory();
        self.decoder.num_threads = 1;
        self.video_encoder.num_threads = 1;
        self
    }

    /// Validate configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.input_path.as_os_str().is_empty() {
            return Err("input_path cannot be empty".to_string());
        }

        if self.decoder.num_threads == 0 {
            return Err("decoder.num_threads must be > 0".to_string());
        }

        if self.video_encoder.num_threads == 0 {
            return Err("video_encoder.num_threads must be > 0".to_string());
        }

        if self.parquet_writer.row_group_size == 0 {
            return Err("parquet_writer.row_group_size must be > 0".to_string());
        }

        // Validate that cloud storage has prefix
        if self.output_storage.is_some() && self.output_prefix.is_none() {
            return Err("output_prefix is required when using cloud storage".to_string());
        }

        Ok(())
    }
}

/// Configuration for the parallel decoder stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecoderConfig {
    /// Number of decoder threads
    pub num_threads: usize,

    /// Chunk size for parallel decoding (bytes)
    pub chunk_size: usize,

    /// Prefetch blocks ahead
    pub prefetch_ahead: usize,
}

impl Default for DecoderConfig {
    fn default() -> Self {
        Self {
            num_threads: (num_cpus::get() / 2).clamp(2, 8),
            chunk_size: 16 * 1024 * 1024, // 16 MB
            prefetch_ahead: 2,
        }
    }
}

/// Configuration for the frame aligner stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlignerConfig {
    /// Target FPS for frame alignment
    pub fps: u32,

    /// Completion window in frames
    pub completion_window_frames: usize,

    /// Maximum buffered frames
    pub max_buffered_frames: usize,

    /// Maximum buffered memory in MB
    pub max_buffered_memory_mb: usize,
}

impl Default for AlignerConfig {
    fn default() -> Self {
        Self {
            fps: 30,
            completion_window_frames: 3,
            max_buffered_frames: 100,
            max_buffered_memory_mb: 500,
        }
    }
}

impl AlignerConfig {
    /// Get completion window in nanoseconds.
    pub fn completion_window_ns(&self) -> u64 {
        let frame_interval_ns = 1_000_000_000u64 / self.fps as u64;
        frame_interval_ns * self.completion_window_frames as u64
    }
}

/// Configuration for the feature transformer stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformerConfig {
    /// Number of transformer threads
    pub num_threads: usize,

    /// Batch size for transformation
    pub batch_size: usize,
}

impl Default for TransformerConfig {
    fn default() -> Self {
        Self {
            num_threads: 2,
            batch_size: 10,
        }
    }
}

/// Configuration for the video encoder stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoEncoderConfig {
    /// Number of encoder threads
    pub num_threads: usize,

    /// Maximum frames queued per camera
    pub max_queue_depth: usize,

    /// Encoder preset
    pub preset: VideoEncoderPreset,
}

impl Default for VideoEncoderConfig {
    fn default() -> Self {
        Self {
            num_threads: (num_cpus::get() / 2).clamp(2, 8),
            max_queue_depth: 100,
            preset: VideoEncoderPreset::default(),
        }
    }
}

/// Video encoder quality preset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum VideoEncoderPreset {
    /// Fast encoding, larger files
    Fast,
    /// Balanced quality and speed
    #[default]
    Balanced,
    /// Best quality, slower encoding
    Quality,
}

/// Configuration for the Parquet writer stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetWriterConfig {
    /// Row group size (rows per group)
    pub row_group_size: usize,

    /// Maximum buffered rows
    pub max_buffered_rows: usize,
}

impl Default for ParquetWriterConfig {
    fn default() -> Self {
        Self {
            row_group_size: 1000,
            max_buffered_rows: 10000,
        }
    }
}

/// Configuration for the upload coordinator stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadConfig {
    /// Number of upload workers
    pub num_workers: usize,

    /// Maximum concurrent uploads
    pub max_concurrent: usize,

    /// Upload timeout
    pub timeout: Duration,

    /// Maximum retries for failed uploads
    pub max_retries: usize,

    /// Initial backoff in milliseconds
    pub initial_backoff_ms: u64,

    /// Delete local files after successful upload
    pub delete_after_upload: bool,
}

impl Default for UploadConfig {
    fn default() -> Self {
        Self {
            num_workers: 4,
            max_concurrent: 8,
            timeout: Duration::from_secs(300), // 5 minutes
            max_retries: 3,
            initial_backoff_ms: 1000,
            delete_after_upload: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validation_empty_input() {
        let lerobot_config = crate::lerobot::config::LerobotConfig {
            dataset: crate::lerobot::config::DatasetConfig {
                name: "test".to_string(),
                fps: 30,
                robot_type: None,
                env_type: None,
            },
            mappings: vec![],
            video: crate::lerobot::config::VideoConfig::default(),
            annotation_file: None,
        };
        let config = PipelineConfig::new("", lerobot_config);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_zero_threads() {
        let lerobot_config = crate::lerobot::config::LerobotConfig {
            dataset: crate::lerobot::config::DatasetConfig {
                name: "test".to_string(),
                fps: 30,
                robot_type: None,
                env_type: None,
            },
            mappings: vec![],
            video: crate::lerobot::config::VideoConfig::default(),
            annotation_file: None,
        };
        let mut config = PipelineConfig::new("input.bag", lerobot_config);
        config.decoder.num_threads = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_cloud_without_prefix() {
        let lerobot_config = crate::lerobot::config::LerobotConfig {
            dataset: crate::lerobot::config::DatasetConfig {
                name: "test".to_string(),
                fps: 30,
                robot_type: None,
                env_type: None,
            },
            mappings: vec![],
            video: crate::lerobot::config::VideoConfig::default(),
            annotation_file: None,
        };
        let config = PipelineConfig::new("input.bag", lerobot_config);
        // Mock storage - we'd need a real storage for full test
        // config.output_storage = Some(mock_storage);
        assert!(config.validate().is_err()); // Missing prefix
    }

    #[test]
    fn test_aligner_completion_window_ns() {
        let config = AlignerConfig {
            fps: 30,
            completion_window_frames: 3,
            ..Default::default()
        };
        // 30 fps = 33.33ms per frame
        // 3 frames = 100ms = 100,000,000 ns
        assert_eq!(config.completion_window_ns(), 100_000_000);
    }
}
