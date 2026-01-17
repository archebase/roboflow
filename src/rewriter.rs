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
//!
//! # Example
//!
//! ```ignore
//! use robocodec::{RoboRewriter, RewriteOptions};
//! use robocodec::format::mcap::transform::TransformBuilder;
//!
//! // Simple rewrite (auto-detect format from extension)
//! let mut rewriter = RoboRewriter::open("input.mcap")?;
//! rewriter.rewrite("output.mcap")?;
//!
//! // With transformations
//! let options = RewriteOptions::default()
//!     .with_transforms(
//!         TransformBuilder::new()
//!             .with_topic_rename("/old", "/new")
//!             .build()
//!     );
//! let mut rewriter = RoboRewriter::with_options("input.mcap", options)?;
//! rewriter.rewrite("output.mcap")?;
//! ```

use std::path::Path;

use crate::core::{CodecError, Result};
use crate::format::mcap::transform::TransformPipeline;

/// Options for rewrite operations.
///
/// These options are shared across all format-specific rewriter implementations.
pub struct RewriteOptions {
    /// Whether to validate schemas before rewriting
    pub validate_schemas: bool,

    /// Whether to skip messages that fail to decode
    pub skip_decode_failures: bool,

    /// Whether to pass through non-CDR messages without re-encoding
    pub passthrough_non_cdr: bool,

    /// Optional transformation pipeline for topic/type renaming.
    /// If None, no transformations are applied.
    pub transforms: Option<TransformPipeline>,
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
    ///
    /// # Example
    ///
    /// ```ignore
    /// use robocodec::RewriteOptions;
    /// use robocodec::format::mcap::transform::TransformBuilder;
    ///
    /// let options = RewriteOptions::default()
    ///     .with_transforms(
    ///         TransformBuilder::new()
    ///             .with_topic_rename("/old", "/new")
    ///             .with_type_rename("foo/Bar", "baz/Bar")
    ///             .build()
    ///     );
    /// ```
    pub fn with_transforms(mut self, pipeline: TransformPipeline) -> Self {
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
/// ```ignore
/// use robocodec::RoboRewriter;
///
/// // MCAP format (detected from .mcap extension)
/// let mut rewriter = RoboRewriter::open("data.mcap")?;
/// rewriter.rewrite("output.mcap")?;
///
/// // BAG format (detected from .bag extension)
/// let mut rewriter = RoboRewriter::open("data.bag")?;
/// rewriter.rewrite("output.bag")?;
/// ```
pub enum RoboRewriter {
    /// MCAP format rewriter
    Mcap(
        crate::format::mcap::rewriter::McapRewriter,
        std::path::PathBuf,
    ),

    /// BAG format rewriter
    Bag(
        crate::format::bag::rewriter::BagRewriter,
        std::path::PathBuf,
    ),
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
                crate::format::mcap::rewriter::McapRewriter::with_options(options),
                path_buf,
            )),
            Some("bag") => Ok(RoboRewriter::Bag(
                crate::format::bag::rewriter::BagRewriter::with_options(options),
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
    use crate::format::mcap::transform::TransformBuilder;

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
        let path = Path::new("test");
        assert_eq!(detect_format(path), None);
    }

    #[test]
    fn test_detect_format_case_insensitive() {
        // Test that detection is case-sensitive (only lowercase is supported)
        let path = Path::new("test.MCAP");
        assert_eq!(
            detect_format(path),
            None,
            "Should not detect uppercase .MCAP"
        );

        let path = Path::new("test.Bag");
        assert_eq!(
            detect_format(path),
            None,
            "Should not detect mixed case .Bag"
        );
    }

    #[test]
    fn test_detect_format_with_dot_in_filename() {
        let path = Path::new("test.v2.mcap");
        assert_eq!(detect_format(path), Some("mcap"));
    }

    #[test]
    fn test_rewrite_options_default() {
        let options = RewriteOptions::default();
        assert!(options.validate_schemas);
        assert!(options.skip_decode_failures);
        assert!(options.passthrough_non_cdr);
        assert!(!options.has_transforms());
        assert!(options.transforms.is_none());
    }

    #[test]
    fn test_rewrite_options_with_transforms() {
        let pipeline = TransformBuilder::new()
            .with_topic_rename("/old", "/new")
            .build();

        let options = RewriteOptions::default().with_transforms(pipeline);
        assert!(options.has_transforms());
        assert!(options.transforms.is_some());
    }

    #[test]
    fn test_rewrite_options_with_empty_transforms() {
        let pipeline = TransformPipeline::new();
        let options = RewriteOptions::default().with_transforms(pipeline);
        assert!(
            !options.has_transforms(),
            "Empty pipeline should return false for has_transforms"
        );
    }

    #[test]
    fn test_rewrite_options_all_false() {
        let options = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: false,
            passthrough_non_cdr: false,
            transforms: None,
        };
        assert!(!options.validate_schemas);
        assert!(!options.skip_decode_failures);
        assert!(!options.passthrough_non_cdr);
        assert!(!options.has_transforms());
    }

    #[test]
    fn test_rewrite_stats_default() {
        let stats = RewriteStats::default();
        assert_eq!(stats.message_count, 0);
        assert_eq!(stats.channel_count, 0);
        assert_eq!(stats.decode_failures, 0);
        assert_eq!(stats.encode_failures, 0);
        assert_eq!(stats.reencoded_count, 0);
        assert_eq!(stats.passthrough_count, 0);
        assert_eq!(stats.topics_renamed, 0);
        assert_eq!(stats.types_renamed, 0);
    }

    #[test]
    fn test_rewrite_stats_new() {
        let stats = RewriteStats::new();
        assert_eq!(stats.message_count, 0);
        assert_eq!(stats.channel_count, 0);
    }

    #[test]
    fn test_rewrite_stats_with_values() {
        let stats = RewriteStats {
            message_count: 100,
            channel_count: 5,
            decode_failures: 2,
            encode_failures: 1,
            reencoded_count: 95,
            passthrough_count: 3,
            topics_renamed: 2,
            types_renamed: 1,
        };
        assert_eq!(stats.message_count, 100);
        assert_eq!(stats.channel_count, 5);
        assert_eq!(stats.decode_failures, 2);
        assert_eq!(stats.encode_failures, 1);
        assert_eq!(stats.reencoded_count, 95);
        assert_eq!(stats.passthrough_count, 3);
        assert_eq!(stats.topics_renamed, 2);
        assert_eq!(stats.types_renamed, 1);
    }

    #[test]
    fn test_rewrite_stats_clone() {
        let stats = RewriteStats {
            message_count: 42,
            channel_count: 3,
            ..Default::default()
        };
        let cloned = stats.clone();
        assert_eq!(cloned.message_count, 42);
        assert_eq!(cloned.channel_count, 3);
    }

    #[test]
    fn test_robo_rewriter_open_returns_error_for_unknown_format() {
        let result = RoboRewriter::open("test.unknown");
        assert!(result.is_err());
        // Verify the error message contains expected text
        if let Err(e) = result {
            assert!(e.to_string().contains("Unknown format"));
        }
    }

    #[test]
    fn test_robo_rewriter_with_options_returns_error_for_unknown_format() {
        let options = RewriteOptions::default();
        let result = RoboRewriter::with_options("test.txt", options);
        assert!(result.is_err());
    }

    #[test]
    fn test_robo_rewriter_open_returns_error_for_no_extension() {
        let result = RoboRewriter::open("testfile");
        assert!(result.is_err());
    }

    #[test]
    fn test_robo_rewriter_with_options_for_mcap_format() {
        let options = RewriteOptions::default();
        // Note: This test doesn't check if the file actually exists
        // because with_options only detects format, doesn't open the file
        let result = RoboRewriter::with_options("test.mcap", options);
        assert!(result.is_ok());
        let rewriter = result.unwrap();
        match rewriter {
            RoboRewriter::Mcap(_, path) => {
                assert_eq!(path, Path::new("test.mcap"));
            }
            RoboRewriter::Bag(_, _) => {
                panic!("Expected MCAP rewriter for .mcap file");
            }
        }
    }

    #[test]
    fn test_robo_rewriter_with_options_for_bag_format() {
        let options = RewriteOptions::default();
        let result = RoboRewriter::with_options("test.bag", options);
        assert!(result.is_ok());
        let rewriter = result.unwrap();
        match rewriter {
            RoboRewriter::Mcap(_, _) => {
                panic!("Expected BAG rewriter for .bag file");
            }
            RoboRewriter::Bag(_, path) => {
                assert_eq!(path, Path::new("test.bag"));
            }
        }
    }

    #[test]
    fn test_robo_rewriter_open_for_mcap() {
        let result = RoboRewriter::open("data.mcap");
        assert!(result.is_ok());
        let rewriter = result.unwrap();
        assert_eq!(rewriter.input_path(), Path::new("data.mcap"));
        match rewriter {
            RoboRewriter::Mcap(_, _) => {}
            RoboRewriter::Bag(_, _) => {
                panic!("Expected MCAP rewriter");
            }
        }
    }

    #[test]
    fn test_robo_rewriter_open_for_bag() {
        let result = RoboRewriter::open("data.bag");
        assert!(result.is_ok());
        let rewriter = result.unwrap();
        assert_eq!(rewriter.input_path(), Path::new("data.bag"));
        match rewriter {
            RoboRewriter::Mcap(_, _) => {
                panic!("Expected BAG rewriter");
            }
            RoboRewriter::Bag(_, _) => {}
        }
    }

    #[test]
    fn test_robo_rewriter_options_delegation() {
        let rewriter = RoboRewriter::open("test.mcap").unwrap();
        let options = rewriter.options();
        assert!(options.validate_schemas);
        assert!(options.skip_decode_failures);
        assert!(options.passthrough_non_cdr);
    }

    #[test]
    fn test_robo_rewriter_with_custom_options() {
        let options = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: true,
            passthrough_non_cdr: false,
            transforms: None,
        };
        let rewriter = RoboRewriter::with_options("test.mcap", options).unwrap();
        let retrieved_options = rewriter.options();
        assert!(!retrieved_options.validate_schemas);
        assert!(retrieved_options.skip_decode_failures);
        assert!(!retrieved_options.passthrough_non_cdr);
    }

    #[test]
    fn test_robo_rewriter_input_path() {
        let rewriter = RoboRewriter::open("some/path/to/file.mcap").unwrap();
        assert_eq!(rewriter.input_path(), Path::new("some/path/to/file.mcap"));
    }
}
