//! Base trait and types for Kps dataset writers.
//!
//! This module defines the unified writer abstraction that allows the pipeline
//! to write to different Kps formats (HDF5, Parquet) through a common interface.

use std::collections::HashMap;

use crate::core::Result;
use crate::io::formats::kps::camera_params::CameraParamCollector;
use crate::io::formats::kps::config::KpsConfig;
use crate::io::metadata::ChannelInfo;
use crate::CodecValue;

/// Error type for Kps writer operations.
#[derive(Debug, thiserror::Error)]
pub enum KpsWriterError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HDF5 error: {0}")]
    Hdf5(String),

    #[error("Parquet error: {0}")]
    Parquet(String),

    #[error("Encoding error: {0}")]
    Encoding(String),

    #[error("Invalid message data: {0}")]
    InvalidData(String),

    #[error("Channel not found: {0}")]
    ChannelNotFound(String),

    #[error("Feature not mapped: {0}")]
    FeatureNotMapped(String),
}

/// Statistics from a Kps writer operation.
#[derive(Debug, Clone, Default)]
pub struct WriterStats {
    /// Total number of frames written.
    pub frames_written: usize,

    /// Number of images encoded/written.
    pub images_encoded: usize,

    /// Number of state/action records written.
    pub state_records: usize,

    /// Size of output data in bytes.
    pub output_bytes: u64,

    /// Processing duration in seconds.
    pub duration_sec: f64,
}

/// Image data with metadata.
#[derive(Debug, Clone)]
pub struct ImageData {
    /// Image width in pixels.
    pub width: u32,

    /// Image height in pixels.
    pub height: u32,

    /// Raw image data (RGB8 or encoded).
    pub data: Vec<u8>,

    /// Original timestamp from the message.
    pub original_timestamp: u64,

    /// Whether data is already encoded (e.g., JPEG/PNG).
    pub is_encoded: bool,
}

/// Audio data with metadata.
#[derive(Debug, Clone)]
pub struct AudioData {
    /// Audio samples (interleaved if multi-channel).
    pub samples: Vec<f32>,

    /// Sample rate in Hz.
    pub sample_rate: u32,

    /// Number of channels (1 = mono, 2 = stereo).
    pub channels: u8,

    /// Original timestamp from the message.
    pub original_timestamp: i64,
}

impl AudioData {
    /// Create new audio data.
    pub fn new(samples: Vec<f32>, sample_rate: u32, channels: u8, original_timestamp: i64) -> Self {
        Self {
            samples,
            sample_rate,
            channels,
            original_timestamp,
        }
    }

    /// Get duration in seconds.
    pub fn duration(&self) -> f64 {
        if self.sample_rate > 0 && self.channels > 0 {
            self.samples.len() as f64 / (self.sample_rate as f64 * self.channels as f64)
        } else {
            0.0
        }
    }

    /// Get number of frames (samples per channel).
    pub fn frames(&self) -> usize {
        if self.channels > 0 {
            self.samples.len() / self.channels as usize
        } else {
            0
        }
    }
}

/// Aligned frame data ready for writing to Kps format.
///
/// This represents a single frame in the output dataset, with all
/// observations and actions aligned to the target timestamp.
#[derive(Debug, Clone)]
pub struct AlignedFrame {
    /// Frame index in the episode.
    pub frame_index: usize,

    /// Target timestamp for this frame.
    pub timestamp: u64,

    /// Image observations by feature name (e.g., "observation.camera_0").
    pub images: HashMap<String, ImageData>,

    /// State observations by feature name.
    pub states: HashMap<String, Vec<f32>>,

    /// Action data by feature name.
    pub actions: HashMap<String, Vec<f32>>,

    /// Additional timestamp data.
    pub timestamps: HashMap<String, u64>,

    /// Audio data by feature name.
    pub audio: HashMap<String, AudioData>,
}

impl AlignedFrame {
    /// Create a new aligned frame.
    pub fn new(frame_index: usize, timestamp: u64) -> Self {
        Self {
            frame_index,
            timestamp,
            images: HashMap::new(),
            states: HashMap::new(),
            actions: HashMap::new(),
            timestamps: HashMap::new(),
            audio: HashMap::new(),
        }
    }

    /// Add an image observation.
    pub fn add_image(&mut self, feature: String, data: ImageData) {
        self.images.insert(feature, data);
    }

    /// Add a state observation.
    pub fn add_state(&mut self, feature: String, values: Vec<f32>) {
        self.states.insert(feature, values);
    }

    /// Add action data.
    pub fn add_action(&mut self, feature: String, values: Vec<f32>) {
        self.actions.insert(feature, values);
    }

    /// Add timestamp data.
    pub fn add_timestamp(&mut self, feature: String, timestamp: u64) {
        self.timestamps.insert(feature, timestamp);
    }

    /// Add audio data.
    pub fn add_audio(&mut self, feature: String, data: AudioData) {
        self.audio.insert(feature, data);
    }

    /// Check if the frame has any data.
    pub fn is_empty(&self) -> bool {
        self.images.is_empty()
            && self.states.is_empty()
            && self.actions.is_empty()
            && self.audio.is_empty()
    }
}

/// Unified Kps writer trait.
///
/// This trait defines the interface for writing Kps datasets in different
/// formats (HDF5, Parquet). The pipeline uses this trait to write data
/// without needing to know the specific format details.
pub trait KpsWriter: Send {
    /// Initialize the writer with channel information.
    ///
    /// Called once before any frames are written. Sets up the output
    /// structure and creates datasets based on the channel information.
    fn initialize(
        &mut self,
        config: &KpsConfig,
        channels: &HashMap<u16, ChannelInfo>,
    ) -> Result<()>;

    /// Write a single aligned frame to the dataset.
    ///
    /// This method is called for each frame in the output, in order.
    fn write_frame(&mut self, frame: &AlignedFrame) -> Result<()>;

    /// Write multiple frames in a batch.
    ///
    /// Default implementation calls `write_frame` for each frame.
    /// Implementations may override this for better performance.
    fn write_batch(&mut self, frames: &[AlignedFrame]) -> Result<()> {
        for frame in frames {
            self.write_frame(frame)?;
        }
        Ok(())
    }

    /// Finalize the dataset and write metadata files.
    ///
    /// Called after all frames have been written. Writes metadata
    /// files (info.json, episode.jsonl, camera parameters, etc.).
    fn finalize(
        &mut self,
        config: &KpsConfig,
        camera_params: Option<&CameraParamCollector>,
    ) -> Result<WriterStats>;

    /// Get the number of frames written so far.
    fn frame_count(&self) -> usize;

    /// Check if the writer has been initialized.
    fn is_initialized(&self) -> bool;
}

/// Helper for extracting numeric values from decoded messages.
pub struct MessageExtractor;

impl MessageExtractor {
    /// Extract a float array from a decoded message.
    pub fn extract_float_array(message: &[(String, CodecValue)]) -> Result<Vec<f32>> {
        let mut values = Vec::new();

        for (_key, value) in message.iter() {
            match value {
                CodecValue::UInt8(n) => values.push(*n as f32),
                CodecValue::UInt16(n) => values.push(*n as f32),
                CodecValue::UInt32(n) => values.push(*n as f32),
                CodecValue::UInt64(n) => values.push(*n as f32),
                CodecValue::Int8(n) => values.push(*n as f32),
                CodecValue::Int16(n) => values.push(*n as f32),
                CodecValue::Int32(n) => values.push(*n as f32),
                CodecValue::Int64(n) => values.push(*n as f32),
                CodecValue::Float32(n) => values.push(*n),
                CodecValue::Float64(n) => values.push(*n as f32),
                CodecValue::Array(arr) => {
                    // Try to extract float values from array
                    for v in arr.iter() {
                        match v {
                            CodecValue::UInt8(n) => values.push(*n as f32),
                            CodecValue::UInt16(n) => values.push(*n as f32),
                            CodecValue::UInt32(n) => values.push(*n as f32),
                            CodecValue::Float32(n) => values.push(*n),
                            CodecValue::Float64(n) => values.push(*n as f32),
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }

        if values.is_empty() {
            return Err(crate::core::CodecError::parse(
                "MessageExtractor",
                "No numeric values found in message",
            ));
        }

        Ok(values)
    }

    /// Extract image data from a decoded message.
    pub fn extract_image(message: &[(String, CodecValue)]) -> Option<ImageData> {
        let mut width = 0u32;
        let mut height = 0u32;
        let mut data: Option<Vec<u8>> = None;
        let mut is_encoded = false;

        for (key, value) in message.iter() {
            match key.as_str() {
                "width" => {
                    if let CodecValue::UInt32(w) = value {
                        width = *w;
                    }
                }
                "height" => {
                    if let CodecValue::UInt32(h) = value {
                        height = *h;
                    }
                }
                "data" => {
                    if let CodecValue::Bytes(b) = value {
                        data = Some(b.clone());
                    }
                }
                "format" => {
                    if let CodecValue::String(f) = value {
                        is_encoded = f != "rgb8";
                    }
                }
                _ => {}
            }
        }

        let image_data = data?;

        Some(ImageData {
            width,
            height,
            data: image_data,
            original_timestamp: 0, // Set by caller
            is_encoded,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aligned_frame_empty() {
        let frame = AlignedFrame::new(0, 1000);
        assert!(frame.is_empty());
    }

    #[test]
    fn test_aligned_frame_with_data() {
        let mut frame = AlignedFrame::new(0, 1000);
        frame.add_state("observation.state".to_string(), vec![1.0, 2.0, 3.0]);
        assert!(!frame.is_empty());
    }

    #[test]
    fn test_extract_float_array() {
        let message = vec![(
            "position".to_string(),
            CodecValue::Array(vec![
                CodecValue::Float32(1.0),
                CodecValue::Float32(2.0),
                CodecValue::Float32(3.0),
            ]),
        )];

        let result = MessageExtractor::extract_float_array(&message).unwrap();
        assert_eq!(result, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_extract_image() {
        let message = vec![
            ("width".to_string(), CodecValue::UInt32(640)),
            ("height".to_string(), CodecValue::UInt32(480)),
            ("data".to_string(), CodecValue::Bytes(vec![1, 2, 3, 4])),
            ("format".to_string(), CodecValue::String("rgb8".to_string())),
        ];

        let image = MessageExtractor::extract_image(&message).unwrap();
        assert_eq!(image.width, 640);
        assert_eq!(image.height, 480);
        assert_eq!(image.data, vec![1, 2, 3, 4]);
        assert!(!image.is_encoded);
    }
}
