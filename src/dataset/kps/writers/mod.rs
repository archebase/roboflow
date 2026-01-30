// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Kps dataset writers.
//!
//! This module provides writers for different Kps dataset formats.
//! All writers implement the unified [`DatasetWriter`] trait.

use crate::core::Result;

pub mod base;

pub use base::{KpsWriterError, MessageExtractor};

// Re-export common types used by KPS writers
pub use crate::dataset::common::{AlignedFrame, AudioData, DatasetWriter, ImageData, WriterStats};

// HDF5 writer
#[cfg(feature = "dataset-hdf5")]
pub mod hdf5;

#[cfg(feature = "dataset-hdf5")]
pub use hdf5::StreamingHdf5Writer;

// v1.2 compliant HDF5 writer
#[cfg(feature = "dataset-hdf5")]
pub mod v12_hdf5;

#[cfg(feature = "dataset-hdf5")]
pub use v12_hdf5::{V12Hdf5Schema, V12Hdf5Writer};

// Parquet writer
#[cfg(feature = "dataset-parquet")]
pub mod parquet;

#[cfg(feature = "dataset-parquet")]
pub use parquet::StreamingParquetWriter;

// Original data HDF5 writer
#[cfg(feature = "dataset-hdf5")]
pub mod original_hdf5;

#[cfg(feature = "dataset-hdf5")]
pub use original_hdf5::OriginalHdf5Writer;

// Audio writer
pub mod audio_writer;

pub use audio_writer::{AudioWriter, AudioWriterFactory};

/// Factory function to create a KPS dataset writer.
///
/// This function examines the Kps config and returns the appropriate
/// writer implementation. If both formats are specified, Parquet is
/// preferred as it's the modern format.
#[allow(unused_variables)]
pub fn create_kps_writer(
    output_dir: impl AsRef<std::path::Path>,
    episode_id: usize,
    config: &crate::dataset::kps::KpsConfig,
) -> Result<Box<dyn DatasetWriter>> {
    use crate::dataset::kps::OutputFormat;

    // Check which formats are requested
    let formats = &config.output.formats;

    if formats.is_empty() || formats.contains(&OutputFormat::Parquet) {
        #[cfg(feature = "dataset-parquet")]
        {
            return Ok(Box::new(StreamingParquetWriter::create(
                output_dir, episode_id, config,
            )?));
        }
        #[cfg(not(feature = "dataset-parquet"))]
        {
            return Err(crate::RoboflowError::parse(
                "DatasetWriter",
                "Parquet support not enabled. Add feature 'dataset-parquet' to Cargo.toml",
            ));
        }
    }

    if formats.contains(&OutputFormat::Hdf5) {
        #[cfg(feature = "dataset-hdf5")]
        {
            return Ok(Box::new(StreamingHdf5Writer::create(
                output_dir, episode_id, config,
            )?));
        }
        #[cfg(not(feature = "dataset-hdf5"))]
        {
            return Err(crate::RoboflowError::parse(
                "DatasetWriter",
                "HDF5 support not enabled. Add feature 'dataset-hdf5' to Cargo.toml",
            ));
        }
    }

    Err(crate::RoboflowError::parse(
        "DatasetWriter",
        "No valid output format specified",
    ))
}

#[cfg(test)]
#[cfg(not(any(feature = "dataset-parquet", feature = "dataset-hdf5")))]
mod tests {
    use super::*;

    #[test]
    fn test_factory_no_feature() {
        let config = crate::dataset::kps::KpsConfig {
            dataset: crate::dataset::kps::DatasetConfig {
                name: "test".to_string(),
                fps: 30,
                robot_type: None,
            },
            mappings: vec![],
            output: crate::dataset::kps::OutputConfig::default(),
        };

        let result = create_kps_writer("/tmp", 0, &config);
        // Should fail without features
        assert!(result.is_err());
    }
}
