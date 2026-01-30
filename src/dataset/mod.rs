// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Dataset format support.
//!
//! This module provides conversion from robotics data (MCAP, ROS bag) to
//! various ML dataset formats (KPS, LeRobot, etc.).
//!
//! # Architecture
//!
//! - [`common`] - Shared types and traits used by all dataset formats
//! - [`kps`] - KPS dataset format support (HDF5, Parquet, v1.2 spec)
//! - [`lerobot`] - LeRobot v2.1 dataset format support
//! - [`streaming`] - Streaming conversion with bounded memory footprint
//!
//! # Unified Writer Interface
//!
//! All dataset writers implement the [`DatasetWriter`] trait,
//! allowing format-agnostic usage:
//!
//! ```rust,ignore
//! use roboflow::dataset::{DatasetConfig, create_writer};
//!
//! // Create config from TOML file
//! let config = DatasetConfig::from_file("config.toml", DatasetFormat::Kps)?;
//!
//! // Create writer
//! let mut writer = create_writer("/output", &config)?;
//! writer.initialize(&config)?;
//! // ... write frames ...
//! let stats = writer.finalize(&config)?;
//! ```

use crate::core::Result;
use std::path::Path;

// KPS dataset format
pub mod kps;

// Common dataset writing utilities
pub mod common;

// LeRobot dataset format
pub mod lerobot;

// Streaming conversion (bounded memory footprint)
pub mod streaming;

// Re-export common types for convenience
pub use common::{AlignedFrame, AudioData, DatasetWriter, ImageData, WriterStats};

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
///
/// # Example
///
/// ```rust,ignore
/// use roboflow::dataset::{DatasetConfig, DatasetFormat, create_writer};
///
/// // Load from file
/// let config = DatasetConfig::from_file("config.toml", DatasetFormat::Kps)?;
///
/// // Or create programmatically
/// let config = DatasetConfig::kps(
///     kps::KpsConfig { ... }
/// );
///
/// // Create writer using unified API
/// let mut writer = create_writer("/output", &config)?;
/// ```
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
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the TOML configuration file
    /// * `format` - The dataset format to use
    pub fn from_file(path: impl AsRef<Path>, format: DatasetFormat) -> Result<Self> {
        match format {
            DatasetFormat::Kps => {
                let config = kps::KpsConfig::from_file(path.as_ref())
                    .map_err(|e| crate::RoboflowError::parse("DatasetConfig", e.to_string()))?;
                Ok(Self::Kps(config))
            }
            DatasetFormat::Lerobot => {
                let config = lerobot::LerobotConfig::from_file(path)?;
                Ok(Self::Lerobot(config))
            }
        }
    }

    /// Parse configuration from a TOML string.
    ///
    /// # Arguments
    ///
    /// * `toml_str` - TOML configuration string
    /// * `format` - The dataset format to use
    pub fn from_toml(toml_str: &str, format: DatasetFormat) -> Result<Self> {
        match format {
            DatasetFormat::Kps => {
                let config: kps::KpsConfig = toml::from_str(toml_str)
                    .map_err(|e| crate::RoboflowError::parse("DatasetConfig", e.to_string()))?;
                Ok(Self::Kps(config))
            }
            DatasetFormat::Lerobot => {
                let config = lerobot::LerobotConfig::from_toml(toml_str)?;
                Ok(Self::Lerobot(config))
            }
        }
    }

    /// Create a minimal configuration with just basic parameters.
    ///
    /// # Arguments
    ///
    /// * `format` - The dataset format
    /// * `name` - Dataset name
    /// * `fps` - Frames per second
    /// * `robot_type` - Optional robot type
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
                    name,
                    fps,
                    robot_type,
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
/// This is the primary way to create dataset writers. It accepts a [`DatasetConfig`]
/// and returns the appropriate writer implementation.
///
/// # Arguments
///
/// * `output_dir` - Output directory path
/// * `config` - Unified dataset configuration
///
/// # Returns
///
/// A boxed [`DatasetWriter`] trait object
///
/// # Example
///
/// ```rust,ignore
/// use roboflow::dataset::{DatasetConfig, DatasetFormat, create_writer};
///
/// // Load config from file
/// let config = DatasetConfig::from_file("config.toml", DatasetFormat::Kps)?;
///
/// // Create writer
/// let mut writer = create_writer("/output", &config)?;
/// writer.initialize(&config)?;
/// // ... write frames ...
/// let stats = writer.finalize(&config)?;
/// ```
///
/// # Errors
///
/// Returns an error if:
/// - The required feature is not enabled (e.g., `kps-parquet` for KPS)
/// - The output directory cannot be created
/// - The configuration is invalid
pub fn create_writer(
    output_dir: impl AsRef<Path>,
    config: &DatasetConfig,
) -> Result<Box<dyn DatasetWriter>> {
    match config {
        DatasetConfig::Kps(kps_config) => {
            use crate::dataset::kps::writers::create_kps_writer;
            create_kps_writer(output_dir, 0, kps_config)
        }
        DatasetConfig::Lerobot(lerobot_config) => {
            use crate::dataset::lerobot::LerobotWriter;
            let writer = LerobotWriter::create(output_dir, lerobot_config.clone())?;
            Ok(Box::new(writer))
        }
    }
}

/// Create a dataset writer based on the specified format (legacy API).
///
/// This factory function creates a writer for the specified dataset format.
/// The returned writer implements [`DatasetWriter`] for format-agnostic usage.
///
/// **Prefer using [`create_writer`] with [`DatasetConfig`] for new code.**
///
/// # Arguments
///
/// * `format` - The dataset format to use
/// * `output_dir` - Output directory path
/// * `config` - Format-specific configuration (as `dyn Any`)
///
/// # Returns
///
/// A boxed [`DatasetWriter`] trait object
pub fn create_dataset_writer(
    format: DatasetFormat,
    output_dir: impl AsRef<Path>,
    config: &dyn std::any::Any,
) -> Result<Box<dyn DatasetWriter>> {
    match format {
        DatasetFormat::Kps => {
            use crate::dataset::kps::writers::create_kps_writer;
            use crate::dataset::kps::KpsConfig;

            let kps_config = config.downcast_ref::<KpsConfig>().ok_or_else(|| {
                crate::RoboflowError::parse("DatasetWriter", "Expected KpsConfig for KPS format")
            })?;

            create_kps_writer(output_dir, 0, kps_config)
        }
        DatasetFormat::Lerobot => {
            use crate::dataset::lerobot::{LerobotConfig, LerobotWriter};

            let lerobot_config = config.downcast_ref::<LerobotConfig>().ok_or_else(|| {
                crate::RoboflowError::parse(
                    "DatasetWriter",
                    "Expected LerobotConfig for LeRobot format",
                )
            })?;

            let writer = LerobotWriter::create(output_dir, lerobot_config.clone())?;
            Ok(Box::new(writer))
        }
    }
}
