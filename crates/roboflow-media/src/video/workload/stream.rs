// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Stream configuration and result types for video workload.
//!
//! This module defines the per-stream configuration and result types
//! used by [`EncodingWorkload`](super::EncodingWorkload).

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use super::strategy::EncodingStrategy;
use crate::video::config::VideoEncoderConfig;

/// Unique identifier for a stream in a workload.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StreamId(Arc<str>);

impl StreamId {
    /// Create a new stream identifier.
    pub fn new(id: impl Into<Arc<str>>) -> Self {
        Self(id.into())
    }

    /// Get the stream identifier as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for StreamId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for StreamId {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

/// Configuration for a single output stream in a workload.
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// Unique identifier for this stream.
    pub id: StreamId,

    /// Output destination for this stream.
    pub output: StreamOutput,

    /// Encoding strategy (how to manage memory).
    pub strategy: EncodingStrategy,

    /// Optional per-stream encoder config (overrides global defaults).
    pub encoder_config: Option<VideoEncoderConfig>,
}

impl StreamConfig {
    /// Create a new stream configuration.
    pub fn new(id: impl Into<StreamId>, output: StreamOutput) -> Self {
        Self {
            id: id.into(),
            output,
            strategy: EncodingStrategy::default(),
            encoder_config: None,
        }
    }

    /// Set the encoding strategy for this stream.
    pub fn with_strategy(mut self, strategy: EncodingStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Set the encoder configuration for this stream.
    pub fn with_encoder_config(mut self, config: VideoEncoderConfig) -> Self {
        self.encoder_config = Some(config);
        self
    }

    /// Create a stream configuration for file output.
    pub fn file(id: impl Into<StreamId>, path: impl Into<PathBuf>) -> Self {
        Self::new(id, StreamOutput::file(path))
    }

    /// Create a stream configuration with fragment encoding.
    pub fn fragment(id: impl Into<StreamId>, path: impl Into<PathBuf>, triggers: super::strategy::FragmentTriggers) -> Self {
        Self::new(id, StreamOutput::file(path))
            .with_strategy(EncodingStrategy::fragment(triggers))
    }
}

/// Output destination for a stream.
#[derive(Debug, Clone)]
pub enum StreamOutput {
    /// Write to a file.
    File {
        /// Output file path.
        path: PathBuf,
    },

    /// Write to a channel (streaming mode).
    Channel {
        /// Chunk size before sending.
        chunk_size: usize,
    },
}

impl StreamOutput {
    /// Create a file output.
    pub fn file(path: impl Into<PathBuf>) -> Self {
        Self::File { path: path.into() }
    }

    /// Create a channel output.
    pub fn channel(chunk_size: usize) -> Self {
        Self::Channel { chunk_size }
    }

    /// Check if this is a file output.
    pub fn is_file(&self) -> bool {
        matches!(self, Self::File { .. })
    }

    /// Get the file path if this is a file output.
    pub fn path(&self) -> Option<&PathBuf> {
        match self {
            Self::File { path } => Some(path),
            _ => None,
        }
    }
}

/// Result from encoding a single stream.
#[derive(Debug, Clone)]
pub struct StreamResult {
    /// Stream identifier.
    pub id: StreamId,

    /// Output file path (for file outputs).
    pub output_path: Option<PathBuf>,

    /// Number of frames encoded.
    pub frames_encoded: u64,

    /// Number of frames skipped due to errors.
    pub frames_skipped: u64,

    /// Total bytes written.
    pub bytes_written: u64,

    /// Number of fragments created (for fragment strategy).
    pub fragments: usize,

    /// Whether encoding succeeded.
    pub success: bool,

    /// Error message if encoding failed.
    pub error: Option<String>,
}

impl StreamResult {
    /// Create a successful stream result.
    pub fn success(
        id: StreamId,
        output_path: Option<PathBuf>,
        frames_encoded: u64,
        frames_skipped: u64,
        bytes_written: u64,
        fragments: usize,
    ) -> Self {
        Self {
            id,
            output_path,
            frames_encoded,
            frames_skipped,
            bytes_written,
            fragments,
            success: true,
            error: None,
        }
    }

    /// Create a failed stream result.
    pub fn failure(id: StreamId, error: impl Into<String>) -> Self {
        Self {
            id,
            output_path: None,
            frames_encoded: 0,
            frames_skipped: 0,
            bytes_written: 0,
            fragments: 0,
            success: false,
            error: Some(error.into()),
        }
    }

    /// Check if this result is successful.
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Check if any frames were skipped.
    pub fn has_skipped_frames(&self) -> bool {
        self.frames_skipped > 0
    }
}

/// Frame data for submission to a stream.
#[derive(Debug, Clone)]
pub struct FrameData {
    /// RGB pixel data.
    pub rgb_data: Vec<u8>,
    /// Frame width in pixels.
    pub width: u32,
    /// Frame height in pixels.
    pub height: u32,
}

impl FrameData {
    /// Create new frame data.
    pub fn new(rgb_data: Vec<u8>, width: u32, height: u32) -> Self {
        Self {
            rgb_data,
            width,
            height,
        }
    }

    /// Create frame data from a slice.
    pub fn from_slice(rgb_data: &[u8], width: u32, height: u32) -> Self {
        Self {
            rgb_data: rgb_data.to_vec(),
            width,
            height,
        }
    }

    /// Validate the frame data.
    pub fn validate(&self) -> roboflow_core::Result<()> {
        let expected_size = (self.width as usize) * (self.height as usize) * 3;
        if self.rgb_data.len() != expected_size {
            return Err(roboflow_core::RoboflowError::encode(
                "FrameData",
                format!(
                    "RGB data size mismatch: got {} bytes, expected {} bytes for {}x{}",
                    self.rgb_data.len(),
                    expected_size,
                    self.width,
                    self.height
                ),
            ));
        }
        Ok(())
    }

    /// Get the expected size of the RGB data.
    pub fn expected_size(&self) -> usize {
        (self.width as usize) * (self.height as usize) * 3
    }
}

/// Command sent to encoder threads.
#[derive(Debug)]
pub(crate) enum EncoderCommand {
    /// Encode a frame.
    Frame(FrameData),
    /// Flush and finalize.
    Finalize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_id_new() {
        let id = StreamId::new("camera1");
        assert_eq!(id.as_str(), "camera1");
    }

    #[test]
    fn test_stream_id_display() {
        let id = StreamId::new("camera1");
        assert_eq!(format!("{}", id), "camera1");
    }

    #[test]
    fn test_stream_id_from_str() {
        let id: StreamId = "camera1".into();
        assert_eq!(id.as_str(), "camera1");
    }

    #[test]
    fn test_stream_id_from_string() {
        let id: StreamId = String::from("camera1").into();
        assert_eq!(id.as_str(), "camera1");
    }

    #[test]
    fn test_stream_id_equality() {
        let id1 = StreamId::new("camera1");
        let id2 = StreamId::new("camera1");
        let id3 = StreamId::new("camera2");
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_stream_config_new() {
        let config = StreamConfig::new("cam1", StreamOutput::file("output.mp4"));
        assert_eq!(config.id.as_str(), "cam1");
        assert!(config.output.is_file());
        assert!(matches!(config.strategy, EncodingStrategy::Standard));
    }

    #[test]
    fn test_stream_config_with_strategy() {
        let config = StreamConfig::new("cam1", StreamOutput::file("output.mp4"))
            .with_strategy(EncodingStrategy::fragment_by_frames(100));
        assert!(config.strategy.is_bounded_memory());
    }

    #[test]
    fn test_stream_config_file() {
        let config = StreamConfig::file("cam1", "output.mp4");
        assert_eq!(config.id.as_str(), "cam1");
        assert!(config.output.is_file());
    }

    #[test]
    fn test_stream_output_file() {
        let output = StreamOutput::file("output.mp4");
        assert!(output.is_file());
        assert_eq!(output.path(), Some(&PathBuf::from("output.mp4")));
    }

    #[test]
    fn test_stream_output_channel() {
        let output = StreamOutput::channel(1024);
        assert!(!output.is_file());
        assert!(output.path().is_none());
    }

    #[test]
    fn test_stream_result_success() {
        let result = StreamResult::success(
            StreamId::new("cam1"),
            Some(PathBuf::from("output.mp4")),
            100,
            0,
            1024 * 1024,
            1,
        );
        assert!(result.is_success());
        assert!(!result.has_skipped_frames());
        assert_eq!(result.frames_encoded, 100);
    }

    #[test]
    fn test_stream_result_failure() {
        let result = StreamResult::failure(StreamId::new("cam1"), "Encoding failed");
        assert!(!result.is_success());
        assert!(result.error.is_some());
    }

    #[test]
    fn test_stream_result_skipped_frames() {
        let result = StreamResult::success(
            StreamId::new("cam1"),
            Some(PathBuf::from("output.mp4")),
            100,
            5,
            1024,
            1,
        );
        assert!(result.has_skipped_frames());
    }

    #[test]
    fn test_frame_data_new() {
        let rgb = vec![128u8; 64 * 64 * 3];
        let frame = FrameData::new(rgb.clone(), 64, 64);
        assert_eq!(frame.width, 64);
        assert_eq!(frame.height, 64);
        assert_eq!(frame.rgb_data, rgb);
    }

    #[test]
    fn test_frame_data_validate() {
        let rgb = vec![128u8; 64 * 64 * 3];
        let frame = FrameData::new(rgb, 64, 64);
        assert!(frame.validate().is_ok());
    }

    #[test]
    fn test_frame_data_validate_wrong_size() {
        let rgb = vec![128u8; 100];
        let frame = FrameData::new(rgb, 64, 64);
        assert!(frame.validate().is_err());
    }

    #[test]
    fn test_frame_data_expected_size() {
        let frame = FrameData::new(vec![], 64, 64);
        assert_eq!(frame.expected_size(), 64 * 64 * 3);
    }
}
