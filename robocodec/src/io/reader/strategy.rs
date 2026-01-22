// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Reading strategies for optimal data access.
//!
//! This module defines the strategy pattern for reading robotics data files.
//! Each strategy implements a different approach to reading data:
//!
//! - **Sequential**: Read messages one by one (works for all files)
//! - **Parallel**: Read chunks in parallel using Rayon
//!   - MCAP: Read summary first, then parallel process chunks
//!   - MCAP without summary (>1GB): Two-pass (scan → build index → parallel)
//!   - MCAP without summary (<1GB): Sequential (overhead not worth it)
//!   - BAG: Read chunk info first, then parallel process chunks
//! - **Auto**: Automatically choose based on file capabilities and size

use std::path::Path;

use crate::io::metadata::FileFormat;
use crate::{CodecError, Result};

/// Reading strategy selector.
///
/// Determines how messages are read from the file:
/// - Sequential: Read messages in order (works for all formats)
/// - Parallel: Read chunks in parallel (MCAP with summary only)
/// - Auto: Automatically choose based on file capabilities
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadStrategy {
    /// Sequential reading - processes messages one by one
    Sequential,
    /// Parallel reading - processes chunks concurrently
    Parallel,
    /// Auto-detect - choose optimal strategy based on file
    #[default]
    Auto,
}

impl ReadStrategy {
    /// Check if this strategy can handle the given file format and capabilities.
    ///
    /// # Arguments
    ///
    /// * `format` - The file format
    /// * `has_summary` - Whether the file has a summary section (for MCAP)
    /// * `has_chunk_indexes` - Whether the summary has chunk indexes
    ///
    /// # Returns
    ///
    /// `true` if the strategy can handle the file, `false` otherwise.
    pub fn can_handle(
        &self,
        format: FileFormat,
        has_summary: bool,
        has_chunk_indexes: bool,
    ) -> bool {
        match self {
            ReadStrategy::Sequential => true, // Sequential works for everything
            ReadStrategy::Parallel => {
                // Parallel requires MCAP with summary and chunk indexes
                format == FileFormat::Mcap && has_summary && has_chunk_indexes
            }
            ReadStrategy::Auto => true, // Auto will choose appropriately
        }
    }

    /// Resolve the Auto strategy to a concrete strategy.
    ///
    /// # Arguments
    ///
    /// * `format` - The file format
    /// * `has_summary` - Whether the file has a summary section
    /// * `has_chunk_indexes` - Whether the summary has chunk indexes
    pub fn resolve(
        &self,
        format: FileFormat,
        has_summary: bool,
        has_chunk_indexes: bool,
    ) -> ReadStrategy {
        match self {
            ReadStrategy::Auto => {
                // Choose parallel if available, otherwise sequential
                if ReadStrategy::Parallel.can_handle(format, has_summary, has_chunk_indexes) {
                    ReadStrategy::Parallel
                } else {
                    ReadStrategy::Sequential
                }
            }
            other => *other,
        }
    }

    /// Check if this is the sequential strategy.
    pub fn is_sequential(&self) -> bool {
        matches!(self, ReadStrategy::Sequential)
    }

    /// Check if this is the parallel strategy.
    pub fn is_parallel(&self) -> bool {
        matches!(self, ReadStrategy::Parallel)
    }

    /// Check if this is the auto strategy.
    pub fn is_auto(&self) -> bool {
        matches!(self, ReadStrategy::Auto)
    }
}

/// Trait for reading strategies.
///
/// Each strategy implements a different approach to reading data
/// from robotics files.
pub trait ReadStrategyTrait: Send + Sync {
    /// Check if this strategy can handle the given file.
    fn can_handle(&self, path: &Path) -> Result<bool>;

    /// Create a reader using this strategy.
    ///
    /// This method is responsible for creating the appropriate
    /// format-specific reader configured for this strategy.
    fn create_reader(&self, path: &Path) -> Result<Box<dyn crate::io::traits::FormatReader>>;

    /// Get the name of this strategy.
    fn name(&self) -> &str {
        "unknown"
    }
}

/// Sequential reading strategy.
///
/// Processes messages one by one in order. Works for all file formats
/// and is the fallback when parallel reading is not available.
#[derive(Debug, Clone, Copy, Default)]
pub struct SequentialStrategy;

impl SequentialStrategy {
    /// Create a new sequential strategy.
    pub fn new() -> Self {
        Self
    }
}

impl ReadStrategyTrait for SequentialStrategy {
    fn can_handle(&self, _path: &Path) -> Result<bool> {
        // Sequential works for any file
        Ok(true)
    }

    fn create_reader(&self, path: &Path) -> Result<Box<dyn crate::io::traits::FormatReader>> {
        // Determine format and create appropriate reader
        // Note: We use the parallel readers which also support sequential iteration
        let format = crate::io::detection::detect_format(path)?;

        match format {
            FileFormat::Mcap => Ok(Box::new(crate::io::formats::mcap::McapFormat::open(path)?)),
            FileFormat::Bag => Ok(Box::new(crate::io::formats::bag::BagFormat::open(path)?)),
            FileFormat::Unknown => Err(CodecError::parse(
                "SequentialStrategy",
                format!("Unknown file format: {}", path.display()),
            )),
        }
    }

    fn name(&self) -> &str {
        "sequential"
    }
}

/// Parallel reading strategy.
///
/// Processes chunks concurrently using Rayon. Only works for MCAP files
/// with a summary section containing chunk indexes.
#[derive(Debug, Clone, Copy, Default)]
pub struct ParallelStrategy {
    /// Number of threads to use (None = auto-detect)
    pub num_threads: Option<usize>,
}

impl ParallelStrategy {
    /// Create a new parallel strategy.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the number of threads.
    pub fn with_threads(mut self, num_threads: usize) -> Self {
        self.num_threads = Some(num_threads);
        self
    }

    /// Set the number of threads to auto-detect.
    pub fn with_auto_threads(mut self) -> Self {
        self.num_threads = None;
        self
    }
}

impl ReadStrategyTrait for ParallelStrategy {
    fn can_handle(&self, path: &Path) -> Result<bool> {
        // Check if file is MCAP or BAG - both support parallel reading
        let format = crate::io::detection::detect_format(path)?;

        match format {
            FileFormat::Mcap => {
                // Check if MCAP has summary with chunk indexes
                match crate::io::formats::mcap::McapFormat::check_summary(path) {
                    Ok((_, has_indexes)) => Ok(has_indexes),
                    Err(_) => Ok(false),
                }
            }
            FileFormat::Bag => {
                // BAG files always support parallel reading via chunk indexes
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn create_reader(&self, path: &Path) -> Result<Box<dyn crate::io::traits::FormatReader>> {
        let format = crate::io::detection::detect_format(path)?;

        match format {
            FileFormat::Mcap => Ok(Box::new(crate::io::formats::mcap::McapFormat::open(path)?)),
            FileFormat::Bag => Ok(Box::new(crate::io::formats::bag::BagFormat::open(path)?)),
            FileFormat::Unknown => Err(CodecError::parse(
                "ParallelStrategy",
                format!("Unknown file format: {}", path.display()),
            )),
        }
    }

    fn name(&self) -> &str {
        "parallel"
    }
}

/// Auto-detect strategy.
///
/// Automatically chooses the optimal strategy based on file capabilities:
/// - Parallel for MCAP files with summary and chunk indexes
/// - Sequential for all other cases
#[derive(Debug, Clone, Copy, Default)]
pub struct AutoStrategy;

impl AutoStrategy {
    /// Create a new auto strategy.
    pub fn new() -> Self {
        Self
    }
}

impl ReadStrategyTrait for AutoStrategy {
    fn can_handle(&self, _path: &Path) -> Result<bool> {
        // Auto always works - it will choose appropriately
        Ok(true)
    }

    fn create_reader(&self, path: &Path) -> Result<Box<dyn crate::io::traits::FormatReader>> {
        // Try parallel first, fall back to sequential
        let parallel = ParallelStrategy::new();

        if parallel.can_handle(path)? {
            parallel.create_reader(path)
        } else {
            SequentialStrategy::new().create_reader(path)
        }
    }

    fn name(&self) -> &str {
        "auto"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_strategy_resolve() {
        // Auto with MCAP + summary + indexes -> Parallel
        let strategy = ReadStrategy::Auto.resolve(FileFormat::Mcap, true, true);
        assert_eq!(strategy, ReadStrategy::Parallel);

        // Auto with MCAP but no summary -> Sequential
        let strategy = ReadStrategy::Auto.resolve(FileFormat::Mcap, false, false);
        assert_eq!(strategy, ReadStrategy::Sequential);

        // Auto with BAG -> Sequential
        let strategy = ReadStrategy::Auto.resolve(FileFormat::Bag, false, false);
        assert_eq!(strategy, ReadStrategy::Sequential);

        // Explicit Sequential stays Sequential
        let strategy = ReadStrategy::Sequential.resolve(FileFormat::Mcap, true, true);
        assert_eq!(strategy, ReadStrategy::Sequential);

        // Explicit Parallel with no support -> stays Parallel (will fail at runtime)
        let strategy = ReadStrategy::Parallel.resolve(FileFormat::Mcap, false, false);
        assert_eq!(strategy, ReadStrategy::Parallel);
    }

    #[test]
    fn test_read_strategy_can_handle() {
        // Sequential handles everything
        assert!(ReadStrategy::Sequential.can_handle(FileFormat::Bag, false, false));
        assert!(ReadStrategy::Sequential.can_handle(FileFormat::Mcap, true, true));

        // Parallel only handles MCAP with summary and indexes
        assert!(!ReadStrategy::Parallel.can_handle(FileFormat::Bag, false, false));
        assert!(ReadStrategy::Parallel.can_handle(FileFormat::Mcap, true, true));
        assert!(!ReadStrategy::Parallel.can_handle(FileFormat::Mcap, true, false));
        assert!(!ReadStrategy::Parallel.can_handle(FileFormat::Mcap, false, true));

        // Auto handles everything
        assert!(ReadStrategy::Auto.can_handle(FileFormat::Bag, false, false));
        assert!(ReadStrategy::Auto.can_handle(FileFormat::Mcap, true, true));
    }

    #[test]
    fn test_read_strategy_default() {
        assert_eq!(ReadStrategy::default(), ReadStrategy::Auto);
    }

    #[test]
    fn test_read_strategy_is_methods() {
        assert!(ReadStrategy::Sequential.is_sequential());
        assert!(!ReadStrategy::Sequential.is_parallel());
        assert!(!ReadStrategy::Sequential.is_auto());

        assert!(!ReadStrategy::Parallel.is_sequential());
        assert!(ReadStrategy::Parallel.is_parallel());
        assert!(!ReadStrategy::Parallel.is_auto());

        assert!(!ReadStrategy::Auto.is_sequential());
        assert!(!ReadStrategy::Auto.is_parallel());
        assert!(ReadStrategy::Auto.is_auto());
    }
}
