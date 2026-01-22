// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Builder pattern for creating unified writers.

use std::path::PathBuf;

use crate::{CodecError, Result};

use super::write_strategy::WriteStrategy;

/// Configuration for creating a writer.
#[derive(Debug, Clone)]
pub struct WriterConfig {
    /// Path to the output file
    pub path: PathBuf,
    /// Writing strategy to use
    pub strategy: WriteStrategy,
    /// Compression level (1-22 for ZSTD)
    pub compression_level: Option<i32>,
    /// Chunk size in bytes
    pub chunk_size: Option<usize>,
    /// Number of threads for parallel compression
    pub num_threads: Option<usize>,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::new(),
            strategy: WriteStrategy::Sequential,
            compression_level: None,
            chunk_size: None,
            num_threads: None,
        }
    }
}

/// Builder for creating unified writers.
#[derive(Debug, Clone, Default)]
pub struct WriterBuilder {
    config: WriterConfig,
}

impl WriterBuilder {
    /// Create a new builder with default configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the path to the output file.
    pub fn path<P: AsRef<std::path::Path>>(mut self, path: P) -> Self {
        self.config.path = path.as_ref().to_path_buf();
        self
    }

    /// Set the writing strategy.
    pub fn strategy(mut self, strategy: WriteStrategy) -> Self {
        self.config.strategy = strategy;
        self
    }

    /// Set the compression level (1-22 for ZSTD).
    pub fn compression_level(mut self, level: i32) -> Self {
        self.config.compression_level = Some(level);
        self
    }

    /// Set the chunk size in bytes.
    pub fn chunk_size(mut self, size: usize) -> Self {
        self.config.chunk_size = Some(size);
        self
    }

    /// Set the number of threads for parallel compression.
    pub fn num_threads(mut self, count: usize) -> Self {
        self.config.num_threads = Some(count);
        self
    }

    /// Build the writer.
    pub fn build(self) -> Result<super::RoboWriter> {
        let path = &self.config.path;

        if path.as_os_str().is_empty() {
            return Err(CodecError::parse("WriterBuilder", "Path is not set"));
        }

        // Detect format from extension
        let format = crate::io::detection::detect_format(path);

        // For new files, we trust the extension
        let format = match format {
            Ok(crate::io::metadata::FileFormat::Unknown) => {
                // If unknown, try extension
                match path.extension().and_then(|e| e.to_str()) {
                    Some("mcap") => crate::io::metadata::FileFormat::Mcap,
                    Some("bag") => crate::io::metadata::FileFormat::Bag,
                    _ => {
                        return Err(CodecError::parse(
                            "WriterBuilder",
                            format!("Unknown file format from extension: {}", path.display()),
                        ))
                    }
                }
            }
            Ok(f) => f,
            Err(e) => return Err(e),
        };

        // Create the appropriate writer
        let inner = match format {
            crate::io::metadata::FileFormat::Mcap => {
                crate::io::formats::mcap::McapFormat::create_writer(path, &self.config)?
            }
            crate::io::metadata::FileFormat::Bag => {
                crate::io::formats::bag::BagFormat::create_writer(path, &self.config)?
            }
            crate::io::metadata::FileFormat::Unknown => {
                return Err(CodecError::parse("WriterBuilder", "Unknown file format"))
            }
        };

        Ok(super::RoboWriter { inner })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_default() {
        let builder = WriterBuilder::new();
        assert_eq!(builder.config.strategy, WriteStrategy::Sequential);
        assert_eq!(builder.config.compression_level, None);
    }

    #[test]
    fn test_builder_fluent() {
        let builder = WriterBuilder::new()
            .path("output.mcap")
            .compression_level(3)
            .chunk_size(1024 * 1024);

        assert_eq!(builder.config.path, PathBuf::from("output.mcap"));
        assert_eq!(builder.config.compression_level, Some(3));
        assert_eq!(builder.config.chunk_size, Some(1024 * 1024));
    }
}
