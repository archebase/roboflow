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
//!
//! # Unified Writer Interface
//!
//! All dataset writers implement the [`DatasetWriter`] trait,
//! allowing format-agnostic usage:
//!
//! ```rust,ignore
//! use roboflow::dataset::{DatasetFormat, create_dataset_writer};
//! use roboflow::dataset::common::DatasetWriter;
//!
//! let mut writer = create_dataset_writer(
//!     DatasetFormat::Lerobot,
//!     "/output",
//!     &config,
//! )?;
//! writer.initialize(&config)?;
//! // ... write frames ...
//! let stats = writer.finalize(&config)?;
//! ```

use std::path::Path;
use crate::core::Result;

// KPS dataset format
pub mod kps;

// Common dataset writing utilities
pub mod common;

// LeRobot dataset format
pub mod lerobot;

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

/// Create a dataset writer based on the specified format.
///
/// This factory function creates a writer for the specified dataset format.
/// The returned writer implements [`DatasetWriter`] for format-agnostic usage.
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
///
/// # Example
///
/// ```rust,ignore
/// use roboflow::dataset::{DatasetFormat, create_dataset_writer};
/// use roboflow::dataset::lerobot::LerobotConfig;
///
/// let config = LerobotConfig::from_file("config.toml")?;
/// let mut writer = create_dataset_writer(
///     DatasetFormat::Lerobot,
///     "/output",
///     &config,
/// )?;
/// ```
///
/// # Errors
///
/// Returns an error if:
/// - The required feature is not enabled (e.g., `kps-parquet` for KPS)
/// - The output directory cannot be created
/// - The configuration is invalid
pub fn create_dataset_writer(
    format: DatasetFormat,
    output_dir: impl AsRef<Path>,
    config: &dyn std::any::Any,
) -> Result<Box<dyn DatasetWriter>> {
    match format {
        DatasetFormat::Kps => {
            use crate::dataset::kps::KpsConfig;
            use crate::dataset::kps::writers::create_kps_writer;

            let kps_config = config
                .downcast_ref::<KpsConfig>()
                .ok_or_else(|| {
                    crate::RoboflowError::parse("DatasetWriter", "Expected KpsConfig for KPS format")
                })?;

            create_kps_writer(output_dir, 0, kps_config)
                .map_err(|e| crate::RoboflowError::parse("DatasetWriter", &e.to_string()))
        }
        DatasetFormat::Lerobot => {
            use crate::dataset::lerobot::{LerobotConfig, LerobotWriter};

            let lerobot_config = config
                .downcast_ref::<LerobotConfig>()
                .ok_or_else(|| {
                    crate::RoboflowError::parse("DatasetWriter", "Expected LerobotConfig for LeRobot format")
                })?;

            let writer = LerobotWriter::create(output_dir, lerobot_config.clone())?;
            Ok(Box::new(writer))
        }
    }
}
