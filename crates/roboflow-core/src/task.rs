// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Task types for distributed pipeline execution.
//!
//! This module provides the boundary objects between `roboflow-distributed`
//! (orchestration) and `roboflow-dataset` (conversion).

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Allocation result containing episode and chunk indices.
///
/// This is the shared type used by both distributed and dataset crates
/// to communicate episode placement information.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EpisodeAllocation {
    /// The allocated episode index (global across the batch).
    pub episode_index: u64,

    /// The chunk index this episode belongs to.
    pub chunk_index: u32,

    /// Offset within the chunk (0 to episodes_per_chunk - 1).
    pub chunk_offset: u32,
}

/// Input source for conversion tasks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InputSource {
    /// Local file path.
    Local { path: PathBuf },
    /// S3 URL (s3://bucket/key).
    S3 { url: String },
    /// OSS URL (oss://bucket/key).
    OSS { url: String },
}

impl InputSource {
    /// Create a local input source.
    pub fn local(path: impl Into<PathBuf>) -> Self {
        Self::Local { path: path.into() }
    }

    /// Create an S3 input source.
    pub fn s3(url: impl Into<String>) -> Self {
        Self::S3 { url: url.into() }
    }

    /// Create an OSS input source.
    pub fn oss(url: impl Into<String>) -> Self {
        Self::OSS { url: url.into() }
    }

    /// Check if this is a local source.
    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local { .. })
    }

    /// Check if this is a cloud source.
    pub fn is_cloud(&self) -> bool {
        matches!(self, Self::S3 { .. } | Self::OSS { .. })
    }

    /// Get the path or URL as a string.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Local { path } => path.to_str().unwrap_or(""),
            Self::S3 { url } | Self::OSS { url } => url.as_str(),
        }
    }
}

/// Output destination for conversion results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OutputDestination {
    /// Local directory output.
    Local { path: PathBuf },
    /// Cloud storage with local buffer for staging.
    Cloud {
        /// Storage URL (e.g., s3://bucket/prefix/).
        storage_url: String,
        /// Local staging directory for temporary files.
        local_buffer: PathBuf,
    },
}

impl OutputDestination {
    /// Create a local output destination.
    pub fn local(path: impl Into<PathBuf>) -> Self {
        Self::Local { path: path.into() }
    }

    /// Create a cloud output destination with local buffer.
    pub fn cloud(storage_url: impl Into<String>, local_buffer: impl Into<PathBuf>) -> Self {
        Self::Cloud {
            storage_url: storage_url.into(),
            local_buffer: local_buffer.into(),
        }
    }

    /// Check if this is a local destination.
    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local { .. })
    }

    /// Check if this is a cloud destination.
    pub fn is_cloud(&self) -> bool {
        matches!(self, Self::Cloud { .. })
    }

    /// Get the local path (either direct path or buffer).
    pub fn local_path(&self) -> &PathBuf {
        match self {
            Self::Local { path } => path,
            Self::Cloud { local_buffer, .. } => local_buffer,
        }
    }
}

/// Output file metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputFile {
    /// Relative path within the output destination.
    pub relative_path: String,
    /// Type of output file.
    pub file_type: OutputFileType,
    /// Size in bytes.
    pub size_bytes: u64,
}

/// Type of output file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OutputFileType {
    /// Parquet data file.
    Parquet,
    /// Video file with camera identifier.
    Video { camera: String },
    /// Metadata file (JSON/TOML).
    Metadata,
}

/// Self-contained conversion task.
///
/// This is the boundary object between `roboflow-distributed` (orchestration)
/// and `roboflow-dataset` (conversion). It contains all information needed
/// to execute a single conversion task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversionTask {
    /// Unique task identifier.
    pub task_id: String,

    /// Batch ID this task belongs to.
    pub batch_id: String,

    /// Input source (file or cloud URL).
    pub input_source: InputSource,

    /// Output destination.
    pub output_destination: OutputDestination,

    /// Episode allocation from the distributed allocator.
    pub episode_allocation: EpisodeAllocation,

    /// Configuration hash for validation.
    pub config_hash: String,

    /// Conversion configuration (format-specific, serialized as JSON).
    pub config_json: String,
}

impl ConversionTask {
    /// Create a new conversion task.
    pub fn new(
        task_id: impl Into<String>,
        batch_id: impl Into<String>,
        input_source: InputSource,
        output_destination: OutputDestination,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            batch_id: batch_id.into(),
            input_source,
            output_destination,
            episode_allocation: EpisodeAllocation::default(),
            config_hash: String::new(),
            config_json: String::new(),
        }
    }

    /// Set the episode allocation.
    pub fn with_episode_allocation(mut self, allocation: EpisodeAllocation) -> Self {
        self.episode_allocation = allocation;
        self
    }

    /// Set the configuration.
    pub fn with_config(
        mut self,
        config_hash: impl Into<String>,
        config_json: impl Into<String>,
    ) -> Self {
        self.config_hash = config_hash.into();
        self.config_json = config_json.into();
        self
    }
}

/// Result of a conversion task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversionResult {
    /// Task ID that produced this result.
    pub task_id: String,

    /// Episode index that was written.
    pub episode_index: u64,

    /// Chunk index the episode belongs to.
    pub chunk_index: u32,

    /// Number of frames processed from input.
    pub frames_processed: usize,

    /// Number of frames written to output.
    pub frames_written: usize,

    /// Output files produced.
    pub output_files: Vec<OutputFile>,

    /// Total duration in seconds.
    pub duration_secs: f64,
}

impl ConversionResult {
    /// Create a new conversion result.
    pub fn new(task_id: impl Into<String>, episode_index: u64, chunk_index: u32) -> Self {
        Self {
            task_id: task_id.into(),
            episode_index,
            chunk_index,
            frames_processed: 0,
            frames_written: 0,
            output_files: Vec::new(),
            duration_secs: 0.0,
        }
    }

    /// Set the frame counts.
    pub fn with_frames(mut self, processed: usize, written: usize) -> Self {
        self.frames_processed = processed;
        self.frames_written = written;
        self
    }

    /// Add an output file.
    pub fn add_output_file(&mut self, file: OutputFile) {
        self.output_files.push(file);
    }

    /// Set the duration.
    pub fn with_duration(mut self, secs: f64) -> Self {
        self.duration_secs = secs;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_input_source_local() {
        let source = InputSource::local("/path/to/file.bag");
        assert!(source.is_local());
        assert!(!source.is_cloud());
        assert_eq!(source.as_str(), "/path/to/file.bag");
    }

    #[test]
    fn test_input_source_s3() {
        let source = InputSource::s3("s3://bucket/file.bag");
        assert!(!source.is_local());
        assert!(source.is_cloud());
        assert_eq!(source.as_str(), "s3://bucket/file.bag");
    }

    #[test]
    fn test_output_destination_local() {
        let dest = OutputDestination::local("/output/path");
        assert!(dest.is_local());
        assert!(!dest.is_cloud());
    }

    #[test]
    fn test_episode_allocation_default() {
        let alloc = EpisodeAllocation::default();
        assert_eq!(alloc.episode_index, 0);
        assert_eq!(alloc.chunk_index, 0);
        assert_eq!(alloc.chunk_offset, 0);
    }

    #[test]
    fn test_conversion_task_builder() {
        let task = ConversionTask::new(
            "task-001",
            "batch-001",
            InputSource::local("/input.bag"),
            OutputDestination::local("/output"),
        )
        .with_episode_allocation(EpisodeAllocation {
            episode_index: 5,
            chunk_index: 1,
            chunk_offset: 0,
        });

        assert_eq!(task.task_id, "task-001");
        assert_eq!(task.batch_id, "batch-001");
        assert_eq!(task.episode_allocation.episode_index, 5);
    }

    #[test]
    fn test_conversion_result_builder() {
        let result = ConversionResult::new("task-001", 0, 0)
            .with_frames(100, 100)
            .with_duration(1.5);

        assert_eq!(result.task_id, "task-001");
        assert_eq!(result.frames_processed, 100);
        assert_eq!(result.frames_written, 100);
        assert_eq!(result.duration_secs, 1.5);
    }
}
