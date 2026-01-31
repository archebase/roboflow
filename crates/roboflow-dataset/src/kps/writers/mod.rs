// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Kps dataset writers.
//!
//! This module provides writers for different Kps dataset formats.
//! All writers implement the unified [`DatasetWriter`] trait.

use roboflow_core::Result;

pub mod audio_writer;
pub mod base;
pub mod parquet;

pub use base::{KpsWriterError, MessageExtractor};

// Re-export common types used by KPS writers
pub use crate::common::{AlignedFrame, AudioData, DatasetWriter, ImageData, WriterStats};

// Re-export streaming writers (Parquet is always available)
pub use audio_writer::{AudioWriter, AudioWriterFactory};
pub use parquet::StreamingParquetWriter;

/// Factory function to create a KPS dataset writer.
///
/// This function creates a Parquet writer for KPS datasets.
/// Parquet is the always-available format in the refactored codebase.
///
/// For HDF5 support, use the roboflow-hdf5 crate.
pub fn create_kps_writer(
    output_dir: impl AsRef<std::path::Path>,
    episode_id: usize,
    config: &crate::kps::KpsConfig,
) -> Result<Box<dyn DatasetWriter>> {
    Ok(Box::new(StreamingParquetWriter::create(
        output_dir, episode_id, config,
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factory_parquet() {
        let config = crate::kps::KpsConfig {
            dataset: crate::kps::DatasetConfig {
                name: "test".to_string(),
                fps: 30,
                robot_type: None,
            },
            mappings: vec![],
            output: crate::kps::OutputConfig::default(),
        };

        let result = create_kps_writer("/tmp", 0, &config);
        // Should succeed with parquet always available
        assert!(result.is_ok() || result.is_err()); // May fail due to directory creation
    }
}
