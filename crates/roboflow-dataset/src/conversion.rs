// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! High-level conversion API for transforming robotics data files to dataset formats.
//!
//! This module provides a simple, clean interface for converting input files
//! (bag, MCAP, RRD) to trainable dataset formats (LeRobot). All output is written
//! to local files; cloud upload is handled by the executor.
//!
//! # Example
//!
//! ```rust,ignore
//! use roboflow_dataset::conversion::{convert_file, ConversionConfig};
//! use roboflow_dataset::formats::{DatasetConfig, DatasetFormat};
//!
//! let config = ConversionConfig {
//!     dataset: DatasetConfig::new(DatasetFormat::Lerobot, "my_dataset", 30, None),
//!     ..Default::default()
//! };
//!
//! let result = convert_file(
//!     Path::new("input.bag"),
//!     Path::new("./output"),
//!     &config,
//! )?;
//!
//! println!("Converted {} frames to {}", result.stats.frames, result.output_dir.display());
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use roboflow_core::{Result, RoboflowError};

use crate::formats::dataset_executor::{
    DatasetPipelineConfig, DatasetPipelineExecutor, DatasetPipelineStats, SequentialPolicy,
};
use crate::formats::lerobot::LerobotWriter;
use crate::sources::{SourceConfig, create_source, register_builtin_sources};

/// Configuration for file conversion.
#[derive(Debug, Clone)]
pub struct ConversionConfig {
    /// Dataset format configuration (LeRobot, etc.)
    pub dataset: crate::formats::DatasetConfig,
    /// Output prefix within the output directory (e.g., "episode_001")
    pub output_prefix: Option<String>,
    /// Maximum frames to process (None = unlimited)
    pub max_frames: Option<usize>,
    /// Custom topic mappings (topic -> feature name)
    pub topic_mappings: HashMap<String, String>,
}

impl ConversionConfig {
    /// Create a new conversion config with the given dataset configuration.
    pub fn new(dataset: crate::formats::DatasetConfig) -> Self {
        Self {
            dataset,
            output_prefix: None,
            max_frames: None,
            topic_mappings: HashMap::new(),
        }
    }

    /// Set the output prefix.
    pub fn with_output_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.output_prefix = Some(prefix.into());
        self
    }

    /// Set the maximum frames to process.
    pub fn with_max_frames(mut self, max: usize) -> Self {
        self.max_frames = Some(max);
        self
    }

    /// Add a topic mapping.
    pub fn with_topic_mapping(
        mut self,
        topic: impl Into<String>,
        feature: impl Into<String>,
    ) -> Self {
        self.topic_mappings.insert(topic.into(), feature.into());
        self
    }
}

/// Result of a file conversion operation.
#[derive(Debug, Clone)]
pub struct ConversionResult {
    /// Output directory containing the converted dataset
    pub output_dir: PathBuf,
    /// Conversion statistics
    pub stats: ConversionStats,
    /// Path to output files by type
    pub output_files: OutputFiles,
}

/// Statistics from the conversion process.
#[derive(Debug, Clone)]
pub struct ConversionStats {
    /// Total frames written
    pub frames_written: usize,
    /// Total episodes written
    pub episodes_written: usize,
    /// Total messages processed
    pub messages_processed: usize,
    /// Processing duration in seconds
    pub duration_sec: f64,
    /// Processing throughput in frames per second
    pub fps: f64,
}

impl From<DatasetPipelineStats> for ConversionStats {
    fn from(stats: DatasetPipelineStats) -> Self {
        Self {
            frames_written: stats.frames_written,
            episodes_written: stats.episodes_written,
            messages_processed: stats.messages_processed,
            duration_sec: stats.duration_sec,
            fps: stats.fps,
        }
    }
}

/// Output files from conversion.
#[derive(Debug, Clone, Default)]
pub struct OutputFiles {
    /// Parquet data files
    pub parquet_files: Vec<PathBuf>,
    /// Video files
    pub video_files: Vec<PathBuf>,
    /// Metadata files (JSON)
    pub metadata_files: Vec<PathBuf>,
}

/// Convert a single input file to dataset format.
///
/// This is the main entry point for file conversion. It:
/// 1. Detects the input file type from the extension
/// 2. Creates an appropriate source reader
/// 3. Creates a local dataset writer
/// 4. Processes all messages through the pipeline
/// 5. Returns the output directory and statistics
///
/// # Arguments
///
/// * `input` - Path to input file (bag/mcap/rrd)
/// * `output_dir` - Local directory for output files
/// * `config` - Conversion configuration
///
/// # Returns
///
/// A `ConversionResult` containing the output directory, statistics, and file paths.
///
/// # Example
///
/// ```rust,ignore
/// use roboflow_dataset::conversion::{convert_file, ConversionConfig};
/// use roboflow_dataset::formats::{DatasetConfig, DatasetFormat};
///
/// let config = ConversionConfig::new(
///     DatasetConfig::new(DatasetFormat::Lerobot, "my_dataset", 30, None)
/// );
///
/// let result = convert_file(
///     Path::new("recording.bag"),
///     Path::new("./output"),
///     &config,
/// )?;
///
/// println!("Output: {}", result.output_dir.display());
/// println!("Frames: {}", result.stats.frames_written);
/// ```
pub fn convert_file(
    input: &Path,
    output_dir: &Path,
    config: &ConversionConfig,
) -> Result<ConversionResult> {
    // Ensure builtin sources are registered
    register_builtin_sources();

    // Ensure output directory exists
    std::fs::create_dir_all(output_dir)?;

    // Create source config from input path
    let input_str = input.to_string_lossy();
    let source_config = SourceConfig::from_url(input_str.as_ref());

    // Create the appropriate source
    let mut source = create_source(&source_config)
        .map_err(|e| RoboflowError::other(format!("Failed to create source: {}", e)))?;

    // Initialize source (sync wrapper for async)
    let runtime = tokio::runtime::Runtime::new()
        .map_err(|e| RoboflowError::other(format!("Failed to create runtime: {}", e)))?;

    let metadata = runtime
        .block_on(async { source.initialize(&source_config).await })
        .map_err(|e| RoboflowError::other(format!("Failed to initialize source: {}", e)))?;

    tracing::info!(
        input = %input.display(),
        output = %output_dir.display(),
        format = ?config.dataset.format(),
        topics = metadata.topics.len(),
        "Starting conversion"
    );

    // Create pipeline config
    let mut pipeline_config = DatasetPipelineConfig::with_fps(config.dataset.fps());
    if let Some(max) = config.max_frames {
        pipeline_config = pipeline_config.with_max_frames(max);
    }
    if !config.topic_mappings.is_empty() {
        pipeline_config = pipeline_config.with_topic_mappings(config.topic_mappings.clone());
    }

    // Create the writer based on format
    let lerobot_config = config
        .dataset
        .as_lerobot()
        .ok_or_else(|| RoboflowError::other("Only LeRobot format is currently supported"))?;

    let writer = LerobotWriter::new_local(output_dir, lerobot_config.clone())?;

    // Create and run the pipeline with sequential policy
    let mut executor = DatasetPipelineExecutor::new(writer, pipeline_config, SequentialPolicy);

    // Process messages in batches
    let batch_size = 1000;
    loop {
        let batch = runtime
            .block_on(async { source.read_batch(batch_size).await })
            .map_err(|e| RoboflowError::other(format!("Failed to read batch: {}", e)))?;

        match batch {
            Some(messages) if !messages.is_empty() => {
                for msg in messages {
                    executor.process_message(msg)?;
                }
            }
            Some(_) => {
                // Empty batch, continue
            }
            None => {
                // End of stream
                break;
            }
        }
    }

    // Finalize the pipeline
    let pipeline_stats = executor.finalize()?;

    // Collect output files
    let output_files = collect_output_files(output_dir)?;

    tracing::info!(
        frames = pipeline_stats.frames_written,
        episodes = pipeline_stats.episodes_written,
        messages = pipeline_stats.messages_processed,
        duration_sec = pipeline_stats.duration_sec,
        fps = pipeline_stats.fps,
        policy = pipeline_stats.policy_name,
        "Conversion complete"
    );

    Ok(ConversionResult {
        output_dir: output_dir.to_path_buf(),
        stats: ConversionStats::from(pipeline_stats),
        output_files,
    })
}

/// Collect output files from the conversion directory.
fn collect_output_files(output_dir: &Path) -> Result<OutputFiles> {
    let mut files = OutputFiles::default();

    fn collect_recursive(dir: &Path, files: &mut OutputFiles) -> Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                collect_recursive(&path, files)?;
            } else {
                let ext = path
                    .extension()
                    .and_then(|e| e.to_str())
                    .map(|e| e.to_lowercase());
                match ext.as_deref() {
                    Some("parquet") => files.parquet_files.push(path),
                    Some("mp4") | Some("mkv") => files.video_files.push(path),
                    Some("json") => files.metadata_files.push(path),
                    _ => {}
                }
            }
        }
        Ok(())
    }

    collect_recursive(output_dir, &mut files)?;
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::{DatasetConfig, DatasetFormat};
    use tempfile::tempdir;

    #[test]
    fn test_conversion_config_builder() {
        let config =
            ConversionConfig::new(DatasetConfig::new(DatasetFormat::Lerobot, "test", 30, None))
                .with_output_prefix("episode_001")
                .with_max_frames(1000)
                .with_topic_mapping("/camera", "observation.images.camera");

        assert_eq!(config.output_prefix, Some("episode_001".to_string()));
        assert_eq!(config.max_frames, Some(1000));
        assert_eq!(
            config.topic_mappings.get("/camera"),
            Some(&"observation.images.camera".to_string())
        );
    }

    #[test]
    fn test_collect_output_files() {
        let dir = tempdir().unwrap();

        // Create some test files
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        std::fs::write(dir.path().join("data/data.parquet"), "parquet").unwrap();
        std::fs::create_dir_all(dir.path().join("videos/cam0")).unwrap();
        std::fs::write(dir.path().join("videos/cam0/video.mp4"), "video").unwrap();
        std::fs::write(dir.path().join("meta.json"), "{}").unwrap();

        let files = collect_output_files(dir.path()).unwrap();

        assert_eq!(files.parquet_files.len(), 1);
        assert_eq!(files.video_files.len(), 1);
        assert_eq!(files.metadata_files.len(), 1);
    }

    #[test]
    fn test_conversion_config_new() {
        let dataset_config = DatasetConfig::new(DatasetFormat::Lerobot, "my_dataset", 30, None);
        let config = ConversionConfig::new(dataset_config);

        assert!(config.output_prefix.is_none());
        assert!(config.max_frames.is_none());
        assert!(config.topic_mappings.is_empty());
    }

    #[test]
    fn test_conversion_result_debug() {
        let result = ConversionResult {
            output_dir: PathBuf::from("/output"),
            stats: ConversionStats {
                frames_written: 100,
                episodes_written: 1,
                messages_processed: 1000,
                duration_sec: 10.0,
                fps: 100.0,
            },
            output_files: OutputFiles::default(),
        };

        let debug_str = format!("{:?}", result);
        assert!(debug_str.contains("frames_written: 100"));
    }

    #[test]
    fn test_conversion_stats_from_pipeline_stats() {
        let pipeline_stats = DatasetPipelineStats {
            frames_written: 500,
            episodes_written: 5,
            messages_processed: 5000,
            duration_sec: 50.0,
            fps: 100.0,
            policy_name: "SequentialPolicy",
        };

        let stats = ConversionStats::from(pipeline_stats);

        assert_eq!(stats.frames_written, 500);
        assert_eq!(stats.episodes_written, 5);
        assert_eq!(stats.messages_processed, 5000);
        assert_eq!(stats.duration_sec, 50.0);
        assert_eq!(stats.fps, 100.0);
    }

    #[test]
    fn test_output_files_default() {
        let files = OutputFiles::default();

        assert!(files.parquet_files.is_empty());
        assert!(files.video_files.is_empty());
        assert!(files.metadata_files.is_empty());
    }

    #[test]
    fn test_collect_output_files_mkv() {
        let dir = tempdir().unwrap();

        // Create MKV file
        std::fs::write(dir.path().join("video.mkv"), "mkv").unwrap();

        let files = collect_output_files(dir.path()).unwrap();

        assert_eq!(files.video_files.len(), 1);
    }

    #[test]
    fn test_collect_output_files_empty_dir() {
        let dir = tempdir().unwrap();

        let files = collect_output_files(dir.path()).unwrap();

        assert!(files.parquet_files.is_empty());
        assert!(files.video_files.is_empty());
        assert!(files.metadata_files.is_empty());
    }

    #[test]
    fn test_collect_output_files_nested() {
        let dir = tempdir().unwrap();

        // Create nested structure
        std::fs::create_dir_all(dir.path().join("a/b/c")).unwrap();
        std::fs::write(dir.path().join("a/data.parquet"), "p").unwrap();
        std::fs::write(dir.path().join("a/b/video.mp4"), "v").unwrap();
        std::fs::write(dir.path().join("a/b/c/meta.json"), "m").unwrap();

        let files = collect_output_files(dir.path()).unwrap();

        assert_eq!(files.parquet_files.len(), 1);
        assert_eq!(files.video_files.len(), 1);
        assert_eq!(files.metadata_files.len(), 1);
    }

    #[test]
    fn test_conversion_config_multiple_topic_mappings() {
        let config =
            ConversionConfig::new(DatasetConfig::new(DatasetFormat::Lerobot, "test", 30, None))
                .with_topic_mapping("/camera/left", "observation.images.left")
                .with_topic_mapping("/camera/right", "observation.images.right")
                .with_topic_mapping("/joint_states", "observation.state");

        assert_eq!(config.topic_mappings.len(), 3);
        assert_eq!(
            config.topic_mappings.get("/camera/left"),
            Some(&"observation.images.left".to_string())
        );
        assert_eq!(
            config.topic_mappings.get("/camera/right"),
            Some(&"observation.images.right".to_string())
        );
        assert_eq!(
            config.topic_mappings.get("/joint_states"),
            Some(&"observation.state".to_string())
        );
    }
}
