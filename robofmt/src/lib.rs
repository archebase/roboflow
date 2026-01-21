//! # Robofmt
//!
//! Robotics data format library for MCAP and ROS bag files.
//!
//! This library provides low-level format handling for robotics data files,
//! including MCAP and ROS1 bag formats. It is designed to be used as a
//! foundation for higher-level robotics data processing pipelines.

// Core types
pub mod core;

// Re-export core types for convenience
pub use core::{CodecError, CodecValue, DecodedMessage, Encoding, PrimitiveType, Result};

// Encoding/decoding
pub mod encoding;

// Schema parsing
pub mod schema;

// I/O types (arena, metadata, traits, etc.)
pub mod io;

// Re-export key I/O types
pub use io::metadata::{ChannelInfo, FileFormat, FileInfo, MessageMetadata, RawMessage};
pub use io::traits::{FormatReader, FormatWriter};
pub use io::{MmapArena, MmapArenaRef};

// Format-specific readers
pub mod bag;
pub mod mcap;

// Re-export format types
pub use bag::{BagFormat, ParallelBagReader};
pub use mcap::{McapFormat, ParallelMcapReader};

/// Decoder trait for generic decoding operations.
pub trait Decoder: Send + Sync {
    /// Decode data into a DecodedMessage.
    fn decode(&self, data: &[u8], schema: &str, type_name: Option<&str>) -> Result<DecodedMessage>;
}
