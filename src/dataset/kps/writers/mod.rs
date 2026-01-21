//! Kps dataset writers.
//!
//! This module provides writers for different Kps dataset formats.
//! All writers implement the [`KpsWriter`] trait for a unified interface.

pub mod base;

pub use base::{
    AlignedFrame, AudioData, ImageData, KpsWriter, KpsWriterError, MessageExtractor, WriterStats,
};

// HDF5 writer (will be refactored)
#[cfg(feature = "kps-hdf5")]
pub mod hdf5;

#[cfg(feature = "kps-hdf5")]
pub use hdf5::StreamingHdf5Writer;

// v1.2 compliant HDF5 writer
#[cfg(feature = "kps-hdf5")]
pub mod v12_hdf5;

#[cfg(feature = "kps-hdf5")]
pub use v12_hdf5::{V12Hdf5Schema, V12Hdf5Writer};

// Parquet writer (will be refactored)
#[cfg(feature = "kps-parquet")]
pub mod parquet;

#[cfg(feature = "kps-parquet")]
pub use parquet::StreamingParquetWriter;

// Original data HDF5 writer
#[cfg(feature = "kps-hdf5")]
pub mod original_hdf5;

#[cfg(feature = "kps-hdf5")]
pub use original_hdf5::OriginalHdf5Writer;

// Audio writer
pub mod audio_writer;

pub use audio_writer::{AudioWriter, AudioWriterFactory};

/// Factory function to create a writer based on the output format.
///
/// This function examines the Kps config and returns the appropriate
/// writer implementation. If both formats are specified, Parquet is
/// preferred as it's the modern format.
#[allow(unused_variables)]
pub fn create_writer(
    output_dir: impl AsRef<std::path::Path>,
    episode_id: usize,
    config: &crate::dataset::kps::KpsConfig,
) -> Result<Box<dyn KpsWriter>, KpsWriterError> {
    use crate::dataset::kps::OutputFormat;

    // Check which formats are requested
    let formats = &config.output.formats;

    if formats.is_empty() || formats.contains(&OutputFormat::Parquet) {
        #[cfg(feature = "kps-parquet")]
        {
            return Ok(Box::new(StreamingParquetWriter::create(
                output_dir, episode_id, config,
            )?));
        }
        #[cfg(not(feature = "kps-parquet"))]
        {
            return Err(KpsWriterError::Encoding(
                "Parquet support not enabled. Add feature 'kps-parquet' to Cargo.toml".to_string(),
            ));
        }
    }

    if formats.contains(&OutputFormat::Hdf5) {
        #[cfg(feature = "kps-hdf5")]
        {
            return Ok(Box::new(StreamingHdf5Writer::create(
                output_dir, episode_id, config,
            )?));
        }
        #[cfg(not(feature = "kps-hdf5"))]
        {
            return Err(KpsWriterError::Encoding(
                "HDF5 support not enabled. Add feature 'kps-hdf5' to Cargo.toml".to_string(),
            ));
        }
    }

    Err(KpsWriterError::Encoding(
        "No valid output format specified".to_string(),
    ))
}

#[cfg(test)]
#[cfg(not(any(feature = "kps-parquet", feature = "kps-hdf5")))]
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

        let result = create_writer("/tmp", 0, &config);
        // Should fail without features
        assert!(result.is_err());
    }
}
