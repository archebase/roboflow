// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # Robofmt
//!
//! Robotics data format library for MCAP and ROS bag files.
//!
//! This library provides format handling for robotics data files, organized by format:
//! - **MCAP** format support in [`io::formats::mcap`](crate::io::formats::mcap) module
//! - **ROS1 bag** format support in [`io::formats::bag`](crate::io::formats::bag) module
//! - **Rewriters** for data transformation in [`rewriter`](crate::rewriter) module
//! - **Transforms** for topic/type renaming in [`transform`](crate::transform) module
//!
//! ## Architecture
//!
//! The library is organized into format-specific modules:
//! - `io/formats/mcap/` - All MCAP-related functionality (readers, writers, high-level APIs)
//! - `io/formats/bag/` - All ROS1 bag-related functionality (readers, writers)
//! - `rewriter/` - Unified rewriter facade with format-specific implementations
//! - `transform/` - Channel and topic/type transformations
//! - `encoding/` - Codec implementations (CDR, Protobuf, JSON)
//! - `schema/` - Schema parsing for ROS/IDL formats
//!
//! ## Example: Reading MCAP
//!
//! ```rust,no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use robocodec::io::formats::mcap::reader::McapReader;
//!
//! let reader = McapReader::open("file.mcap")?;
//! for result in reader.decode_messages()? {
//!     let (decoded, channel) = result?;
//!     println!("Topic: {}", channel.topic);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Example: Rewriting with Transformations
//!
//! ```rust,no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use robocodec::RoboRewriter;
//! use robocodec::transform::TransformBuilder;
//!
//! let mut rewriter = RoboRewriter::open("input.mcap")?;
//! rewriter.rewrite("output.mcap")?;
//! # Ok(())
//! # }
//! ```

// Core types
pub mod core;

// Re-export core types for convenience
pub use core::{CodecError, CodecValue, DecodedMessage, Encoding, PrimitiveType, Result};

// Encoding/decoding
pub mod encoding;

// Schema parsing
pub mod schema;

// Message transformations
pub mod transform;

// Pipeline types (arena, chunk, buffer pool)
pub mod types;

// I/O types (arena, metadata, traits, reader/writer strategies, etc.)
pub mod io;

// Re-export key I/O types
pub use io::metadata::{ChannelInfo, FileFormat, FileInfo, MessageMetadata};
pub use io::traits::{FormatReader, FormatWriter};
pub use io::{MmapArena, MmapArenaRef, RoboReader, RoboWriter};

// Rewriter support (shared types and traits)
pub mod rewriter;

pub use rewriter::{FormatRewriter, RewriteOptions, RewriteStats, RoboRewriter};

pub use transform::{
    MultiTransform, TopicRenameTransform, TransformBuilder, TransformError, TransformedChannel,
    TypeNormalization, TypeRenameTransform,
};

// Backward compatibility re-exports (deprecated - use io::formats::* instead)
#[deprecated(
    since = "0.2.0",
    note = "Use io::formats::bag instead. This re-export will be removed in a future version."
)]
pub use io::formats::bag;
#[deprecated(
    since = "0.2.0",
    note = "Use io::formats::mcap instead. This re-export will be removed in a future version."
)]
pub use io::formats::mcap;

// Re-export format readers (low-level) - keep for backward compatibility
pub use io::formats::bag::{BagFormat, ParallelBagReader, SequentialBagReader};
pub use io::formats::mcap::{
    McapFormat, ParallelMcapReader, ParallelMcapWriter, SequentialMcapReader, TwoPassMcapReader,
};

// Re-export high-level format APIs - keep for backward compatibility
pub use io::formats::bag::{BagMessage, BagWriter};
pub use io::formats::mcap::{McapReader, ParallelMcapWriter as McapWriter, RawMessage};

/// Decoder trait for generic decoding operations.
pub trait Decoder: Send + Sync {
    /// Decode data into a DecodedMessage.
    fn decode(&self, data: &[u8], schema: &str, type_name: Option<&str>) -> Result<DecodedMessage>;
}
