//! Pipeline stages for async data processing.
//!
//! This module contains the individual stages of the async pipeline:
//! - ReaderStage: Reads data from input files
//! - TransformStage: Applies transformations (topic rename, type rename, etc.)
//! - CompressionStage: Compresses data chunks in parallel
//! - WriterStage: Writes compressed chunks to output files

pub mod compression;
pub mod reader;
pub mod transform;
pub mod writer;

pub use compression::{CompressionBackend, CompressionStage, CompressionStageConfig};
pub use reader::ReaderStage;
pub use transform::TransformStage;
pub use writer::WriterStage;

use robocodec::transform::ChannelInfo;

/// Configuration for the transform stage.
#[derive(Debug, Clone, Default)]
pub struct TransformStageConfig {
    /// Whether transform is enabled
    pub enabled: bool,
    /// Whether to log verbose output
    pub verbose: bool,
}

/// Output from the transform stage.
pub struct TransformStageOutput {
    /// Transformed channel information
    pub transformed_channels: Vec<ChannelInfo>,
    /// Channel ID mapping (old -> new)
    pub channel_id_map: std::collections::HashMap<u16, u16>,
    /// Number of chunks received
    pub chunks_received: u64,
}
