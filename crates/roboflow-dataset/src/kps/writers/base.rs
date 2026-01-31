// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Base trait and types for Kps dataset writers.
//!
//! This module defines the unified writer abstraction that allows the pipeline
//! to write to different Kps formats (HDF5, Parquet) through a common interface.

use std::collections::HashMap;

use crate::common::{AlignedFrame, ImageData, WriterStats};
use crate::kps::camera_params::CameraParamCollector;
use crate::kps::config::KpsConfig;
use robocodec::CodecValue;
use robocodec::io::metadata::ChannelInfo;
use roboflow_core::Result;

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

/// Unified Kps writer trait.
///
/// This trait defines the interface for writing Kps datasets in different
/// formats (HDF5, Parquet). The pipeline uses this trait to write data
/// without needing to know the specific format details.
///
/// # Relationship to DatasetWriter
///
/// `KpsWriter` is format-specific (uses `KpsConfig` and `ChannelInfo`) while
/// [`crate::common::DatasetWriter`] is format-agnostic. Both traits
/// use the same [`AlignedFrame`] data structure for passing frame data.
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
            return Err(roboflow_core::RoboflowError::parse(
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
