// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # roboflow-dataset
//!
//! Dataset writers for roboflow.
//!
//! This crate provides dataset format writers:
//! - **LeRobot v2.1** - Modern parquet format (always available)
//!
//! ## Design Philosophy
//!
//! Parquet is the modern format for LeRobot v2.1 and production datasets.
//! **No feature flags** for core dataset writers.

use roboflow_core::Result;
use std::path::Path;

// Common dataset writing utilities
pub mod common;

// Hardware detection and strategy selection
pub mod hardware;

// LeRobot dataset format
pub mod lerobot;

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
    /// LeRobot v2.1 format
    Lerobot,
}

/// Unified dataset configuration.
///
/// This enum holds LeRobot configuration.
#[derive(Debug, Clone)]
pub enum DatasetConfig {
    /// LeRobot dataset configuration
    Lerobot(lerobot::LerobotConfig),
}

impl DatasetConfig {
    /// Create a LeRobot dataset configuration.
    pub fn lerobot(config: lerobot::LerobotConfig) -> Self {
        Self::Lerobot(config)
    }

    /// Load configuration from a TOML file.
    pub fn from_file(path: impl AsRef<Path>, format: DatasetFormat) -> Result<Self> {
        match format {
            DatasetFormat::Lerobot => {
                let config = lerobot::LerobotConfig::from_file(path)?;
                Ok(Self::Lerobot(config))
            }
        }
    }

    /// Parse configuration from a TOML string.
    pub fn from_toml(toml_str: &str, format: DatasetFormat) -> Result<Self> {
        match format {
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
                flushing: Default::default(),
            }),
        }
    }

    /// Get the dataset format.
    pub fn format(&self) -> DatasetFormat {
        match self {
            Self::Lerobot(_) => DatasetFormat::Lerobot,
        }
    }

    /// Get the dataset name.
    pub fn name(&self) -> &str {
        match self {
            Self::Lerobot(c) => &c.dataset.base.name,
        }
    }

    /// Get the frames per second.
    pub fn fps(&self) -> u32 {
        match self {
            Self::Lerobot(c) => c.dataset.base.fps,
        }
    }

    /// Get the robot type.
    pub fn robot_type(&self) -> Option<&str> {
        match self {
            Self::Lerobot(c) => c.dataset.base.robot_type.as_deref(),
        }
    }

    /// Get the underlying LeRobot config.
    pub fn as_lerobot(&self) -> Option<&lerobot::LerobotConfig> {
        match self {
            Self::Lerobot(c) => Some(c),
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
