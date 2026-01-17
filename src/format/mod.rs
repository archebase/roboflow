//! High-level format APIs for robotics data.
//!
//! This module provides high-level APIs for working with robotics data formats:
//! - **Reading** with automatic decoding: [`reader`]
//! - **Writing** format files: [`writer`]
//! - **Rewriting** with transformations: [`rewriter`]
//!
//! ## Architecture
//!
//! This is the **high-level API layer**. For low-level I/O with raw message
//! iteration and trait-based polymorphism, use `robocodec::io::formats`.
//!
//! ## Modules
//!
//! - [`reader`] - High-level readers with auto-decoding
//! - [`writer`] - Format-specific writers
//! - [`rewriter`] - File rewriting with transformations
//! - LeRobot support is now at `robocodec::io::lerobot`
//!
//! Transform-related functionality has been moved to `robocodec::transform`.
//!
//! ## Example: Reading with auto-decoding
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use robocodec::format::reader::McapReader;
//!
//! let reader = McapReader::open("file.mcap")?;
//! // decode_messages returns an iterator
//! let iter = reader.decode_messages()?;
//! for result in iter {
//!     let (decoded, channel) = result?;
//!     // decoded is HashMap<String, CodecValue> - already decoded!
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Example: Rewriting with transformations
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use robocodec::format::rewriter::McapRewriter;
//! use robocodec::rewriter::RewriteOptions;
//! use robocodec::transform::TransformBuilder;
//!
//! let transform = TransformBuilder::new().with_topic_rename("/old", "/new").build();
//! let options = RewriteOptions::default().with_transforms(transform);
//! let mut rewriter = McapRewriter::with_options(options);
//! rewriter.rewrite("input.mcap", "output.mcap")?;
//! # Ok(())
//! # }
//! ```

pub mod reader;
pub mod rewriter;
pub mod writer;

// Re-export kps from io::kps for backwards compatibility
// This allows existing code using `format::kps::*` to continue working
pub mod kps {
    pub use crate::io::kps::*;
}

// Re-export transform types for convenience
// Note: The transform module is now at crate::transform
pub use crate::transform::{
    ChannelInfo as TransformChannelInfo, McapTransform, TopicAwareTypeRenameTransform,
    TopicRenameTransform, TransformBuilder, TransformError, TransformPipeline, TransformedChannel,
    TypeNormalization, TypeRenameTransform,
};

// Reader
pub use reader::{ChannelInfo, McapReader, RawMessage};

// Writer
pub use writer::{BagMessage, BagWriter, ParallelMcapWriter};

// Rewriter
pub use rewriter::{BagRewriter, McapRewriteEngine, McapRewriteStats, McapRewriter};

// KPS - re-export from io::kps for backwards compatibility
pub use crate::io::kps::{KpsConfig, Mapping, MappingType, OutputFormat};
