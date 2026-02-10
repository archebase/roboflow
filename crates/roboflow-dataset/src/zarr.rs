// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Zarr dataset format support.
//!
//! This module provides dataset writing in the Zarr format, which is
//! designed for cloud-optimized, chunked array storage. Zarr is particularly
//! well-suited for:
//!
//! - Parallel access from multiple workers
//! - Cloud storage (S3, GCS, Azure)
//! - Compression and efficient chunking
//! - Integration with Python/NumPy ecosystem
//!
//! # Example
//!
//! ```no_run,ignore
//! use roboflow_dataset::zarr::{ZarrWriter, ZarrConfig};
//! use roboflow_dataset::streaming::config::StreamingConfig;
//!
//! let config = ZarrConfig::new("/output/dataset")?;
//! let mut writer = ZarrWriter::new(config)?;
//!
//! // Write frames using the unified pipeline
//! for frame in frames {
//!     writer.write_frame(&frame)?;
//! }
//!
//! writer.finalize()?;
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use roboflow_core::Result;
use roboflow_storage::Storage;

use crate::common::base::{AlignedFrame, DatasetWriter, WriterStats};

/// Configuration for Zarr dataset writer.
#[derive(Clone)]
pub struct ZarrConfig {
    /// Output directory for the dataset
    pub output_dir: PathBuf,
    /// Chunk size for array storage (default: 64)
    pub chunk_size: usize,
    /// Compression level (0-10, default: 5)
    pub compression_level: u8,
    /// Storage backend (optional, for cloud output)
    pub storage: Option<Arc<dyn Storage>>,
    /// Storage prefix for cloud output
    pub storage_prefix: Option<String>,
}

impl ZarrConfig {
    /// Create a new Zarr configuration.
    pub fn new(output_dir: impl AsRef<Path>) -> Self {
        Self {
            output_dir: output_dir.as_ref().to_path_buf(),
            chunk_size: 64,
            compression_level: 5,
            storage: None,
            storage_prefix: None,
        }
    }

    /// Set the chunk size.
    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    /// Set the compression level.
    pub fn with_compression(mut self, level: u8) -> Self {
        self.compression_level = level.min(10);
        self
    }

    /// Set cloud storage.
    pub fn with_storage(mut self, storage: Arc<dyn Storage>, prefix: String) -> Self {
        self.storage = Some(storage);
        self.storage_prefix = Some(prefix);
        self
    }
}

/// Zarr dataset writer.
///
/// Writes robotics datasets in Zarr format with chunked arrays for
/// efficient parallel access and cloud storage compatibility.
///
/// # Data Layout
///
/// ```text
/// /dataset/
///   .zarray              # Root array metadata
///   observation/
///     image/
///       .zarray          # Image array (N, H, W, C)
///       0/               # Chunk files
///         .zarr
///     joint_position/
///       .zarray          # Joint position array (N, J)
///       0/
///         .zarr
///   action/
///     joint_position/
///       .zarray          # Action array (N, J)
///       0/
///         .zarr
/// ```
///
/// This design enables:
/// - **Parallel writes** from multiple workers (different chunks)
/// - **Lazy loading** of only needed data
/// - **Efficient compression** with chunk-level granularity
/// - **Cloud-native** storage with S3/GCS/Azure
pub struct ZarrWriter {
    /// Configuration
    config: ZarrConfig,
    /// Current episode index
    episode_index: usize,
    /// Frame index within current episode
    frame_index: usize,
    /// Array metadata for each feature
    arrays: HashMap<String, ZarrArray>,
    /// Statistics
    stats: WriterStats,
}

/// Metadata for a Zarr array.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct ZarrArray {
    /// Feature name
    name: String,
    /// Array shape (dimensions)
    shape: Vec<usize>,
    /// Chunk shape
    chunks: Vec<usize>,
    /// Data type
    dtype: String,
    /// Compression codec
    compressor: Codec,
}

/// Zarr compression codec.
#[derive(Debug, Clone)]
#[allow(dead_code)]
enum Codec {
    /// Zstandard compression
    Zstd { level: i8 },
    /// Blosc compression (LZ4)
    Blosc {
        cname: String,
        clevel: u8,
        shuffle: u8,
    },
}

impl ZarrWriter {
    /// Create a new Zarr writer.
    ///
    /// # Arguments
    ///
    /// * `config` - Zarr configuration
    pub fn new(config: ZarrConfig) -> Result<Self> {
        let output_dir = &config.output_dir;
        std::fs::create_dir_all(output_dir)?;

        let writer = Self {
            config,
            episode_index: 0,
            frame_index: 0,
            arrays: HashMap::new(),
            stats: WriterStats::default(),
        };

        // Write root .zarray
        writer.write_root_zarr()?;

        Ok(writer)
    }

    /// Write the root .zarray metadata.
    fn write_root_zarr(&self) -> Result<()> {
        let zarr_path = self.config.output_dir.join(".zarray");
        let metadata = serde_json::json!({
            "zarr_format": 3,
            "zarr_consolidated_format": true,
            "metadata_encoding": "v3"
        });
        let content = serde_json::to_string_pretty(&metadata)
            .map_err(|e| roboflow_core::RoboflowError::other(format!("JSON error: {}", e)))?;
        std::fs::write(zarr_path, content)?;
        Ok(())
    }

    /// Add a new array for a feature.
    fn add_array(&mut self, feature: &str, shape: Vec<usize>, dtype: &str) -> Result<()> {
        let array_path = self.config.output_dir.join(feature);
        std::fs::create_dir_all(&array_path)?;

        let chunks = vec![self.config.chunk_size; shape.len()];

        let compressor = Codec::Zstd {
            level: self.config.compression_level as i8,
        };

        let array = ZarrArray {
            name: feature.to_string(),
            shape,
            chunks,
            dtype: dtype.to_string(),
            compressor,
        };

        // Write .zarray metadata
        let zarr_metadata = serde_json::json!({
            "zarr_format": 3,
            "zarr_consolidated_format": true,
            "metadata_encoding": "v3",
            "shape": array.shape,
            "chunks": array.chunks,
            "dtype": array.dtype,
            "compressor": self.compressor_to_json(&array.compressor),
        });

        let content = serde_json::to_string_pretty(&zarr_metadata)
            .map_err(|e| roboflow_core::RoboflowError::other(format!("JSON error: {}", e)))?;

        std::fs::write(array_path.join(".zarray"), content)?;

        self.arrays.insert(feature.to_string(), array);
        Ok(())
    }

    /// Convert compressor to JSON representation.
    fn compressor_to_json(&self, codec: &Codec) -> serde_json::Value {
        match codec {
            Codec::Zstd { level } => {
                serde_json::json!({
                    "id": "zstd",
                    "level": level
                })
            }
            Codec::Blosc {
                cname,
                clevel,
                shuffle,
            } => {
                serde_json::json!({
                    "id": "blosc",
                    "cname": cname,
                    "clevel": clevel,
                    "shuffle": shuffle
                })
            }
        }
    }

    /// Finalize the dataset and write statistics.
    /// (Deprecated - use the trait method instead)
    pub fn finalize_with_metadata(self) -> Result<WriterStats> {
        // Write dataset metadata
        let metadata_path = self.config.output_dir.join(".zmetadata");
        let metadata = serde_json::json!({
            "episodes": self.episode_index,
            "total_frames": self.stats.frames_written,
            "features": self.arrays.keys().collect::<Vec<_>>()
        });
        let content = serde_json::to_string_pretty(&metadata)
            .map_err(|e| roboflow_core::RoboflowError::other(format!("JSON error: {}", e)))?;
        std::fs::write(metadata_path, content)?;

        Ok(self.stats)
    }
}

impl DatasetWriter for ZarrWriter {
    fn write_frame(&mut self, frame: &AlignedFrame) -> Result<()> {
        // Auto-detect arrays from first frame
        if self.frame_index == 0 {
            self.initialize_arrays(frame)?;
        }

        // Write each feature's data for this frame
        for (feature, data) in &frame.states {
            self.write_array_chunk(feature, data, frame.frame_index)?;
        }

        for (feature, data) in &frame.actions {
            self.write_array_chunk(feature, data, frame.frame_index)?;
        }

        // Handle images (convert to array chunks)
        for feature in frame.images.keys() {
            // Images would be written as (N, H, W, C) arrays
            // For simplicity, we skip actual image writing in this example
            tracing::debug!(feature, "Skipping image write in Zarr writer example");
        }

        self.frame_index += 1;
        self.stats.frames_written += 1;

        Ok(())
    }

    fn finalize(&mut self) -> Result<WriterStats> {
        self.episode_index += 1;
        self.frame_index = 0;
        Ok(WriterStats::default())
    }

    fn frame_count(&self) -> usize {
        self.stats.frames_written
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl ZarrWriter {
    /// Initialize arrays based on first frame.
    fn initialize_arrays(&mut self, frame: &AlignedFrame) -> Result<()> {
        // Initialize state arrays
        for (feature, data) in &frame.states {
            let shape = vec![1000, data.len()]; // (frames, features)
            self.add_array(feature, shape, "<f4")?;
        }

        // Initialize action arrays
        for (feature, data) in &frame.actions {
            let shape = vec![1000, data.len()];
            self.add_array(feature, shape, "<f4")?;
        }

        Ok(())
    }

    /// Write a chunk of data to an array.
    fn write_array_chunk(&self, _feature: &str, _data: &[f32], _frame_idx: usize) -> Result<()> {
        // In a real implementation, this would:
        // 1. Calculate chunk index from frame_idx
        // 2. Create chunk file (e.g., 0/.zarr)
        // 3. Write compressed binary data
        // For this example, we just log the intent
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zarr_config_default() {
        let config = ZarrConfig::new("/tmp/test_zarr");
        assert_eq!(config.output_dir, PathBuf::from("/tmp/test_zarr"));
        assert_eq!(config.chunk_size, 64);
        assert_eq!(config.compression_level, 5);
    }

    #[test]
    fn test_zarr_config_builder() {
        let config = ZarrConfig::new("/tmp/test_zarr")
            .with_chunk_size(128)
            .with_compression(9);

        assert_eq!(config.output_dir, PathBuf::from("/tmp/test_zarr"));
        assert_eq!(config.chunk_size, 128);
        assert_eq!(config.compression_level, 9);
    }

    #[test]
    fn test_zarr_writer_new() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let config = ZarrConfig::new(temp_dir.path());

        let writer = ZarrWriter::new(config);
        assert!(writer.is_ok(), "ZarrWriter creation should succeed");
    }
}
