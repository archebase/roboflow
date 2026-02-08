// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # roboflow-dataset
//!
//! Dataset writers for roboflow.
//!
//! This crate provides dataset format writers:
//! - **LeRobot v2.1** - Modern parquet format (always available)
//! - **KPS v1.2** - Knowledge Perspective Systems format (HDF5/Parquet)
//! - **Streaming** - Bounded memory footprint conversion
//!
//! ## Design Philosophy
//!
//! Parquet is the modern format for LeRobot v2.1 and production datasets.
//! **No feature flags** for core dataset writers.

use roboflow_core::Result;
use std::path::Path;

// KPS dataset format
pub mod kps;

// Common dataset writing utilities
pub mod common;

// LeRobot dataset format
pub mod lerobot;

// Streaming conversion (bounded memory footprint)
pub mod streaming;

// Image decoding (JPEG/PNG with GPU support)
pub mod image;

// Re-export common types for convenience
pub use common::{AlignedFrame, AudioData, DatasetWriter, ImageData, WriterStats};

// Re-export commonly used image types
pub use image::{
    DecodedImage, ImageDecoderBackend, ImageDecoderConfig, ImageDecoderFactory, ImageError,
    ImageFormat, ImageFormat as ImageDecoderBackendType, MemoryStrategy, decode_compressed_image,
};

/// Dataset format enumeration.
///
/// Represents the supported output dataset formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatasetFormat {
    /// KPS format (HDF5 or Parquet)
    Kps,
    /// LeRobot v2.1 format
    Lerobot,
}

/// Unified dataset configuration.
///
/// This enum holds either KPS or LeRobot configuration, providing a
/// format-agnostic way to create dataset writers at runtime.
#[derive(Debug, Clone)]
pub enum DatasetConfig {
    /// KPS dataset configuration
    Kps(kps::KpsConfig),
    /// LeRobot dataset configuration
    Lerobot(lerobot::LerobotConfig),
}

impl DatasetConfig {
    /// Create a KPS dataset configuration.
    pub fn kps(config: kps::KpsConfig) -> Self {
        Self::Kps(config)
    }

    /// Create a LeRobot dataset configuration.
    pub fn lerobot(config: lerobot::LerobotConfig) -> Self {
        Self::Lerobot(config)
    }

    /// Load configuration from a TOML file.
    pub fn from_file(path: impl AsRef<Path>, format: DatasetFormat) -> Result<Self> {
        match format {
            DatasetFormat::Kps => {
                let config = kps::KpsConfig::from_file(path.as_ref()).map_err(|e| {
                    roboflow_core::RoboflowError::parse("DatasetConfig", e.to_string())
                })?;
                Ok(Self::Kps(config))
            }
            DatasetFormat::Lerobot => {
                let config = lerobot::LerobotConfig::from_file(path)?;
                Ok(Self::Lerobot(config))
            }
        }
    }

    /// Parse configuration from a TOML string.
    pub fn from_toml(toml_str: &str, format: DatasetFormat) -> Result<Self> {
        match format {
            DatasetFormat::Kps => {
                let config: kps::KpsConfig = toml::from_str(toml_str).map_err(|e| {
                    roboflow_core::RoboflowError::parse("DatasetConfig", e.to_string())
                })?;
                Ok(Self::Kps(config))
            }
            DatasetFormat::Lerobot => {
                let config = lerobot::LerobotConfig::from_toml(toml_str)?;
                Ok(Self::Lerobot(config))
            }
        }
    }

    /// Create a minimal configuration with just basic parameters.
    pub fn new(
        format: DatasetFormat,
        name: impl Into<String>,
        fps: u32,
        robot_type: Option<String>,
    ) -> Self {
        let name = name.into();
        match format {
            DatasetFormat::Kps => Self::Kps(kps::KpsConfig {
                dataset: kps::DatasetConfig {
                    name,
                    fps,
                    robot_type,
                },
                mappings: Vec::new(),
                output: kps::OutputConfig::default(),
            }),
            DatasetFormat::Lerobot => Self::Lerobot(lerobot::LerobotConfig {
                dataset: lerobot::DatasetConfig {
                    base: common::DatasetBaseConfig {
                        name,
                        fps,
                        robot_type,
                    },
                    env_type: None,
                },
                mappings: Vec::new(),
                video: Default::default(),
                annotation_file: None,
            }),
        }
    }

    /// Get the dataset format.
    pub fn format(&self) -> DatasetFormat {
        match self {
            Self::Kps(_) => DatasetFormat::Kps,
            Self::Lerobot(_) => DatasetFormat::Lerobot,
        }
    }

    /// Get the dataset name.
    pub fn name(&self) -> &str {
        match self {
            Self::Kps(c) => &c.dataset.name,
            Self::Lerobot(c) => &c.dataset.name,
        }
    }

    /// Get the frames per second.
    pub fn fps(&self) -> u32 {
        match self {
            Self::Kps(c) => c.dataset.fps,
            Self::Lerobot(c) => c.dataset.fps,
        }
    }

    /// Get the robot type.
    pub fn robot_type(&self) -> Option<&str> {
        match self {
            Self::Kps(c) => c.dataset.robot_type.as_deref(),
            Self::Lerobot(c) => c.dataset.robot_type.as_deref(),
        }
    }

    /// Get the underlying KPS config, if this is a KPS config.
    pub fn as_kps(&self) -> Option<&kps::KpsConfig> {
        match self {
            Self::Kps(c) => Some(c),
            _ => None,
        }
    }

    /// Get the underlying LeRobot config, if this is a LeRobot config.
    pub fn as_lerobot(&self) -> Option<&lerobot::LerobotConfig> {
        match self {
            Self::Lerobot(c) => Some(c),
            _ => None,
        }
    }
}

/// Create a dataset writer from a unified configuration.
///
/// # Arguments
///
/// * `output_dir` - Output directory path (used for local storage or as base path)
/// * `storage` - Optional storage backend for cloud output (S3, OSS, etc.)
/// * `output_prefix` - Output prefix within storage (required when using cloud storage)
/// * `config` - Dataset configuration
pub fn create_writer(
    output_dir: impl AsRef<Path>,
    storage: Option<&std::sync::Arc<dyn roboflow_storage::Storage>>,
    output_prefix: Option<&str>,
    config: &DatasetConfig,
) -> Result<Box<dyn DatasetWriter>> {
    match config {
        DatasetConfig::Kps(kps_config) => {
            use crate::kps::writers::create_kps_writer;
            // KPS writer uses local storage for now
            create_kps_writer(output_dir, 0, kps_config)
        }
        DatasetConfig::Lerobot(lerobot_config) => {
            use crate::lerobot::LerobotWriter;
            // Use cloud storage if provided, otherwise use local storage
            if let (Some(storage), Some(prefix)) = (storage, output_prefix) {
                let writer = LerobotWriter::new(
                    std::sync::Arc::clone(storage),
                    prefix.to_string(),
                    output_dir,
                    lerobot_config.clone(),
                )?;
                Ok(Box::new(writer))
            } else {
                let writer = LerobotWriter::new_local(output_dir, lerobot_config.clone())?;
                Ok(Box::new(writer))
            }
        }
    }
}
