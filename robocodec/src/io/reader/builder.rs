// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Builder pattern for creating unified readers.
//!
//! The `ReaderBuilder` provides a fluent API for configuring and creating
//! readers with specific strategies and options.

use std::path::PathBuf;

use crate::io::traits::FormatReader;
use crate::{CodecError, Result};

use super::strategy::{
    AutoStrategy, ParallelStrategy, ReadStrategy, ReadStrategyTrait, SequentialStrategy,
};

/// Configuration for creating a reader.
#[derive(Debug, Clone)]
pub struct ReaderConfig {
    /// Path to the file to read
    pub path: PathBuf,
    /// Reading strategy to use
    pub strategy: ReadStrategy,
    /// Number of threads for parallel reading (None = auto-detect)
    pub num_threads: Option<usize>,
}

impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::new(),
            strategy: ReadStrategy::Auto,
            num_threads: None,
        }
    }
}

/// Builder for creating unified readers.
///
/// The builder provides a fluent API for configuring readers before
/// opening them.
///
/// # Example
///
/// ```rust,no_run
/// use roboflow::io::{ReaderBuilder, ReadStrategy};
///
/// // Simple usage with auto-detection
/// let reader = ReaderBuilder::new()
///     .path("data.mcap")
///     .build()?;
///
/// // With specific strategy
/// let reader = ReaderBuilder::new()
///     .path("data.mcap")
///     .strategy(ReadStrategy::Sequential)
///     .build()?;
///
/// // With parallel thread count
/// let reader = ReaderBuilder::new()
///     .path("data.mcap")
///     .strategy(ReadStrategy::Parallel)
///     .num_threads(4)
///     .build()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, Default)]
pub struct ReaderBuilder {
    config: ReaderConfig,
}

impl ReaderBuilder {
    /// Create a new builder with default configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the path to the file.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use roboflow::io::ReaderBuilder;
    ///
    /// let builder = ReaderBuilder::new()
    ///     .path("data.mcap");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn path<P: AsRef<std::path::Path>>(mut self, path: P) -> Self {
        self.config.path = path.as_ref().to_path_buf();
        self
    }

    /// Set the reading strategy.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use roboflow::io::{ReaderBuilder, ReadStrategy};
    ///
    /// let builder = ReaderBuilder::new()
    ///     .path("data.mcap")
    ///     .strategy(ReadStrategy::Sequential);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn strategy(mut self, strategy: ReadStrategy) -> Self {
        self.config.strategy = strategy;
        self
    }

    /// Set the number of threads for parallel reading.
    ///
    /// This only has an effect when using the `Parallel` strategy.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use roboflow::io::{ReaderBuilder, ReadStrategy};
    ///
    /// let builder = ReaderBuilder::new()
    ///     .path("data.mcap")
    ///     .strategy(ReadStrategy::Parallel)
    ///     .num_threads(4);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn num_threads(mut self, count: usize) -> Self {
        self.config.num_threads = Some(count);
        self
    }

    /// Build the reader.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The path is not set
    /// - The file doesn't exist
    /// - The file format is unknown
    /// - The selected strategy is not supported for the file
    pub fn build(self) -> Result<super::RoboReader> {
        let path = &self.config.path;

        if path.as_os_str().is_empty() {
            return Err(CodecError::parse("ReaderBuilder", "Path is not set"));
        }

        if !path.exists() {
            return Err(CodecError::parse(
                "ReaderBuilder",
                format!("File not found: {}", path.display()),
            ));
        }

        // Resolve Auto strategy to concrete strategy
        let format = crate::io::detection::detect_format(path)?;

        // Check for MCAP summary and chunk indexes
        let (has_summary, has_chunk_indexes) = if format == crate::io::metadata::FileFormat::Mcap {
            self.check_mcap_summary(path)?
        } else {
            (false, false)
        };

        let resolved_strategy =
            self.config
                .strategy
                .resolve(format, has_summary, has_chunk_indexes);

        // Create the appropriate reader
        let strategy_impl: Box<dyn ReadStrategyTrait> = match resolved_strategy {
            ReadStrategy::Sequential => Box::new(SequentialStrategy::new()),
            ReadStrategy::Parallel => {
                let mut parallel = ParallelStrategy::new();
                if let Some(threads) = self.config.num_threads {
                    parallel = parallel.with_threads(threads);
                }
                Box::new(parallel)
            }
            ReadStrategy::Auto => Box::new(AutoStrategy::new()),
        };

        let inner = strategy_impl.create_reader(path)?;

        Ok(super::RoboReader {
            inner,
            strategy: resolved_strategy,
        })
    }

    /// Check if an MCAP file has a summary with chunk indexes.
    fn check_mcap_summary(&self, path: &PathBuf) -> Result<(bool, bool)> {
        crate::io::formats::mcap::McapFormat::check_summary(path)
    }
}

impl FormatReader for ReaderBuilder {
    // Note: This is a placeholder - ReaderBuilder itself is not a reader
    // This impl is needed for trait bounds in some contexts

    fn channels(&self) -> &std::collections::HashMap<u16, crate::io::metadata::ChannelInfo> {
        use std::sync::OnceLock;
        static EMPTY: OnceLock<std::collections::HashMap<u16, crate::io::metadata::ChannelInfo>> =
            OnceLock::new();
        EMPTY.get_or_init(Default::default)
    }

    fn message_count(&self) -> u64 {
        0
    }

    fn start_time(&self) -> Option<u64> {
        None
    }

    fn end_time(&self) -> Option<u64> {
        None
    }

    fn path(&self) -> &str {
        self.config.path.to_str().unwrap_or("")
    }

    fn format(&self) -> crate::io::metadata::FileFormat {
        crate::io::metadata::FileFormat::Unknown
    }

    fn file_size(&self) -> u64 {
        0
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_builder_default() {
        let builder = ReaderBuilder::new();
        assert_eq!(builder.config.strategy, ReadStrategy::Auto);
        assert_eq!(builder.config.num_threads, None);
    }

    #[test]
    fn test_builder_fluent() {
        let builder = ReaderBuilder::new()
            .path("test.mcap")
            .strategy(ReadStrategy::Sequential)
            .num_threads(4);

        assert_eq!(builder.config.path, PathBuf::from("test.mcap"));
        assert_eq!(builder.config.strategy, ReadStrategy::Sequential);
        assert_eq!(builder.config.num_threads, Some(4));
    }

    #[test]
    fn test_builder_missing_path() {
        let builder = ReaderBuilder::new();
        let result = builder.build();
        assert!(result.is_err());
    }

    #[test]
    fn test_builder_file_not_found() {
        let builder = ReaderBuilder::new().path("/nonexistent/file.mcap");
        let result = builder.build();
        assert!(result.is_err());
    }

    #[test]
    fn test_check_mcap_summary_with_file() {
        // Create a dummy MCAP-like file
        let path = format!(
            "/tmp/robocodec_test_builder_summary_{}.mcap",
            std::process::id()
        );
        {
            use std::fs::File;
            let mut temp_file = File::create(&path).unwrap();
            temp_file.write_all(b"\x1C\xC1\x41\x50MCAP").unwrap();
            temp_file.flush().unwrap();
        }

        let builder = ReaderBuilder::new();
        let result = builder.check_mcap_summary(&std::path::PathBuf::from(&path));
        // Should succeed but return no summary (not a real MCAP file)
        assert!(result.is_ok());
        let (has_summary, has_indexes) = result.unwrap();
        // A real MCAP file would have summary info
        assert!(!has_summary || !has_indexes); // At least one should be false for dummy file

        let _ = std::fs::remove_file(&path);
    }
}
