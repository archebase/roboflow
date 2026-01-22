// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified rewriter support for robotics data formats.
//!
//! This module provides a trait-based abstraction for format-specific rewriters,
//! shared configuration types, and a unified facade that detects the format
//! from file extension.
//!
//! # Architecture
//!
//! - [`FormatRewriter`] - Trait for format-specific rewriter implementations
//! - [`RewriteOptions`] - Configuration for rewrite operations
//! - [`RewriteStats`] - Statistics from rewrite operations
//! - [`RoboRewriter`] - Unified facade that auto-detects format

use std::path::Path;

use crate::core::{CodecError, Result};
use crate::transform::MultiTransform;

/// Options for rewrite operations.
///
/// These options are shared across all format-specific rewriter implementations.
#[derive(Clone, Debug)]
pub struct RewriteOptions {
    /// Whether to validate schemas before rewriting
    pub validate_schemas: bool,

    /// Whether to skip messages that fail to decode
    pub skip_decode_failures: bool,

    /// Whether to pass through non-CDR messages without re-encoding
    pub passthrough_non_cdr: bool,

    /// Optional transformation pipeline for topic/type renaming.
    /// If None, no transformations are applied.
    pub transforms: Option<MultiTransform>,
}

impl Default for RewriteOptions {
    fn default() -> Self {
        Self {
            validate_schemas: true,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: None,
        }
    }
}

impl RewriteOptions {
    /// Add a transform pipeline to the rewrite options.
    pub fn with_transforms(mut self, pipeline: MultiTransform) -> Self {
        self.transforms = Some(pipeline);
        self
    }

    /// Check if transformations are configured.
    pub fn has_transforms(&self) -> bool {
        self.transforms.as_ref().is_some_and(|p| !p.is_empty())
    }
}

/// Statistics from a rewrite operation.
///
/// These statistics are provided by all format-specific rewriter implementations.
#[derive(Debug, Clone, Default)]
pub struct RewriteStats {
    /// Total messages processed
    pub message_count: u64,

    /// Total channels processed
    pub channel_count: u64,

    /// Messages that failed to decode
    pub decode_failures: u64,

    /// Messages that failed to encode
    pub encode_failures: u64,

    /// Messages that were successfully re-encoded
    pub reencoded_count: u64,

    /// Messages passed through without re-encoding
    pub passthrough_count: u64,

    /// Number of topics renamed (if transforms were applied)
    pub topics_renamed: u64,

    /// Number of types renamed (if transforms were applied)
    pub types_renamed: u64,
}

impl RewriteStats {
    /// Create a new empty statistics struct.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Trait for format-specific rewriter implementations.
///
/// This trait defines the common interface that all format-specific rewriters
/// must implement. Each rewriter handles the specifics of reading, transforming,
/// and writing its respective format.
pub trait FormatRewriter: Send + Sync {
    /// Rewrite from input to output with configured transforms.
    ///
    /// # Arguments
    ///
    /// * `input_path` - Path to the input file
    /// * `output_path` - Path to the output file
    ///
    /// # Returns
    ///
    /// Statistics about the rewrite operation.
    fn rewrite<P1, P2>(&mut self, input_path: P1, output_path: P2) -> Result<RewriteStats>
    where
        P1: AsRef<Path>,
        P2: AsRef<Path>;

    /// Get the options used for rewriting.
    fn options(&self) -> &RewriteOptions;

    /// Get as Any for downcasting.
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Detect the format from a file path.
///
/// # Returns
///
/// - `Some("mcap")` for `.mcap` files
/// - `Some("bag")` for `.bag` files
/// - `None` for unknown extensions
pub fn detect_format(path: &Path) -> Option<&'static str> {
    path.extension()
        .and_then(|ext| ext.to_str())
        .and_then(|ext| match ext {
            "mcap" => Some("mcap"),
            "bag" => Some("bag"),
            _ => None,
        })
}

/// Unified rewriter facade that auto-detects format from file extension.
///
/// `RoboRewriter` provides a unified interface for rewriting both MCAP and BAG
/// files. The format is detected from the input file extension.
///
/// # Example
///
/// ```no_run
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use robocodec::RoboRewriter;
///
/// // MCAP format (detected from .mcap extension)
/// let mut rewriter = RoboRewriter::open("data.mcap")?;
/// rewriter.rewrite("output.mcap")?;
///
/// // BAG format (detected from .bag extension)
/// let mut rewriter = RoboRewriter::open("data.bag")?;
/// rewriter.rewrite("output.bag")?;
/// # Ok(())
/// # }
/// ```
pub enum RoboRewriter {
    /// MCAP format rewriter
    Mcap(crate::rewriter::mcap::McapRewriter, std::path::PathBuf),

    /// BAG format rewriter
    Bag(crate::rewriter::bag::BagRewriter, std::path::PathBuf),
}

impl RoboRewriter {
    /// Open a file and create the appropriate rewriter based on format detection.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the input file (format detected from extension)
    ///
    /// # Returns
    ///
    /// A `RoboRewriter` instance for the detected format.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The file extension is not recognized
    /// - The file cannot be opened
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::with_options(path, RewriteOptions::default())
    }

    /// Create a rewriter with custom options for the specified file.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the input file (format detected from extension)
    /// * `options` - Rewrite options including transforms
    ///
    /// # Returns
    ///
    /// A `RoboRewriter` instance for the detected format.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The file extension is not recognized
    /// - The file cannot be opened
    pub fn with_options<P: AsRef<Path>>(path: P, options: RewriteOptions) -> Result<Self> {
        let path_ref = path.as_ref();
        let path_buf = path_ref.to_path_buf();
        match detect_format(path_ref) {
            Some("mcap") => Ok(RoboRewriter::Mcap(
                crate::rewriter::mcap::McapRewriter::with_options(options),
                path_buf,
            )),
            Some("bag") => Ok(RoboRewriter::Bag(
                crate::rewriter::bag::BagRewriter::with_options(options),
                path_buf,
            )),
            _ => Err(CodecError::encode(
                "RoboRewriter",
                format!(
                    "Unknown format: {:?}. Supported extensions: .mcap, .bag",
                    path_ref.extension()
                ),
            )),
        }
    }

    /// Rewrite to an output file.
    ///
    /// # Arguments
    ///
    /// * `output_path` - Path to the output file
    ///
    /// # Returns
    ///
    /// Statistics about the rewrite operation.
    pub fn rewrite<P: AsRef<Path>>(&mut self, output_path: P) -> Result<RewriteStats> {
        match self {
            RoboRewriter::Mcap(rewriter, input_path) => rewriter.rewrite(input_path, output_path),
            RoboRewriter::Bag(rewriter, input_path) => rewriter.rewrite(input_path, output_path),
        }
    }

    /// Get the options used for rewriting.
    pub fn options(&self) -> &RewriteOptions {
        match self {
            RoboRewriter::Mcap(rewriter, _) => rewriter.options(),
            RoboRewriter::Bag(rewriter, _) => rewriter.options(),
        }
    }

    /// Get the input file path.
    pub fn input_path(&self) -> &Path {
        match self {
            RoboRewriter::Mcap(_, path) | RoboRewriter::Bag(_, path) => path,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_format_mcap() {
        let path = Path::new("test.mcap");
        assert_eq!(detect_format(path), Some("mcap"));
    }

    #[test]
    fn test_detect_format_bag() {
        let path = Path::new("test.bag");
        assert_eq!(detect_format(path), Some("bag"));
    }

    #[test]
    fn test_detect_format_unknown() {
        let path = Path::new("test.txt");
        assert_eq!(detect_format(path), None);
    }

    #[test]
    fn test_detect_format_no_extension() {
        let path = Path::new("testfile");
        assert_eq!(detect_format(path), None);
    }

    #[test]
    fn test_rewrite_options_default() {
        let options = RewriteOptions::default();
        assert!(options.validate_schemas);
        assert!(options.skip_decode_failures);
        assert!(options.passthrough_non_cdr);
        assert!(!options.has_transforms());
    }

    #[test]
    fn test_rewrite_stats_default() {
        let stats = RewriteStats::default();
        assert_eq!(stats.message_count, 0);
        assert_eq!(stats.channel_count, 0);
    }
}
