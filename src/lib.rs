//! # Robocodec
//!
//! A universal, schema-driven runtime decoding engine for Rust.
//!
//! Supports CDR, Protobuf, and JSON message formats with full MCAP support.
//!
//! ## Modules
//!
//! - [`core`] - Core types (CodecValue, errors, registry)
//! - [`schema`] - IDL/MSG schema parser using Pest
//! - [`encoding`] - Message encoding/decoding (CDR, Protobuf, JSON)
//! - [`format`] - File format handlers (MCAP, ROS1 bag, KPS)
//! - [`transform`] - Cross-format transformations
//! - [`io`] - Unified I/O layer
//! - [`pipeline`] - Parallel processing pipeline
//!
//! ## Example
//!
//! ```no_run
//! use robocodec::Reader;
//! use robocodec::io::FormatReader;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Open any format (auto-detected from file extension)
//! let mut reader = Reader::open("data.mcap")?;
//!
//! // Access channel metadata
//! for channel in reader.channels().values() {
//!     println!("Topic: {}, Type: {}, Encoding: {}",
//!         channel.topic, channel.message_type, channel.encoding);
//! }
//! # Ok(())
//! # }
//! ```

// =============================================================================
// Global Allocator
// =============================================================================
// Platform-specific allocator selection:
// - macOS: Use default system allocator (already excellent for concurrent workloads)
// - Linux: Use jemalloc (better than glibc malloc for multi-threaded workloads)
// - Other platforms: Use default
#[cfg(all(feature = "jemalloc", target_os = "linux", not(target_arch = "wasm32")))]
use tikv_jemallocator::Jemalloc;

#[cfg(all(feature = "jemalloc", target_os = "linux", not(target_arch = "wasm32")))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

// =============================================================================
// Core modules (minimal public API - prefer crate::* imports)
// =============================================================================
pub mod config;
pub mod core;
pub mod rewriter;

// =============================================================================
// Parallel processing pipeline
// =============================================================================
pub mod pipeline;

// =============================================================================
// Unified I/O layer
// =============================================================================
pub mod io;

// =============================================================================
// Schema parsing
// =============================================================================
pub mod schema;

// =============================================================================
// Encoding/decoding
// =============================================================================
pub mod encoding;

// =============================================================================
// File formats
// =============================================================================
pub mod format;
pub mod transform;

// =============================================================================
// Re-exports (minimal, focused on user-facing API)
// =============================================================================

// Core types (essential)
pub use core::{CodecError, CodecValue, DecodedMessage, PrimitiveType, Result};

// Schema parsing
pub use schema::{parse_schema, FieldType, MessageSchema};

// High-level readers
pub use format::reader::{ChannelInfo, McapReader, RawMessage};

// Writers
pub use format::writer::{BagMessage, BagWriter, ParallelMcapWriter};

// Transformers
pub use transform::{TopicRenameTransform, TransformBuilder, TransformPipeline};

// Unified I/O
pub use io::{ReaderBuilder, RoboReader, RoboWriter, WriterBuilder};

// KPS dataset format
pub use io::formats::kps::{
    config::{KpsConfig, Mapping, MappingType, OutputFormat},
    delivery_v12::{
        SeriesDeliveryConfig, SeriesDeliveryConfigBuilder, StatisticsCollector, TaskInfo,
        TaskStatistics, V12DeliveryBuilder,
    },
    Hdf5KpsWriter, ParquetKpsWriter,
};

// Unified rewriter
pub use rewriter::{detect_format as io_detect_format, RewriteOptions, RoboRewriter};

// Configuration
pub use config::NormalizeConfig;

// Pipeline
#[cfg(feature = "gpu")]
pub use pipeline::gpu::GpuCompressionConfig;
pub use pipeline::{AsyncPipeline, CompressionConfig};

// Fluent API
pub use pipeline::fluent::{BatchReport, CompressionPreset, PipelineMode, ReadOptions, Robocodec};

// =============================================================================
// Python bindings (conditional compilation)
// =============================================================================
#[cfg(feature = "python")]
pub mod python;

// =============================================================================
// Common types (for public API)
// =============================================================================

// Simplified type aliases for the unified API
/// Unified reader that auto-detects format (MCAP or BAG) from file extension.
///
/// This is a type alias for [`RoboReader`](crate::io::RoboReader).
/// Use this for the simplest API when opening robotics data files.
///
/// # Example
///
/// ```no_run
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use robocodec::Reader;
///
/// let reader = Reader::open("data.mcap")?;
/// // or
/// let reader = Reader::open("data.bag")?;
/// # Ok(())
/// # }
/// ```
pub type Reader = io::RoboReader;

/// Unified writer that auto-detects format from file extension.
///
/// This is a type alias for [`RoboWriter`](crate::io::RoboWriter).
/// Use this for the simplest API when writing robotics data files.
///
/// # Example
///
/// ```no_run
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use robocodec::Writer;
///
/// let mut writer = Writer::create("output.mcap")?;
/// // or
/// let mut writer = Writer::create("output.bag")?;
/// # Ok(())
/// # }
/// ```
pub type Writer = io::RoboWriter;

/// Decoder trait for generic decoding operations.
pub trait Decoder: Send + Sync {
    /// Decode data into a DecodedMessage.
    fn decode(&self, data: &[u8], schema: &str, type_name: Option<&str>) -> Result<DecodedMessage>;
}

/// Encoding format identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Encoding {
    /// CDR (Common Data Representation) encoding
    Cdr,
    /// Protobuf encoding
    Protobuf,
    /// JSON encoding
    Json,
}

impl Encoding {
    /// Check if this encoding is CDR.
    pub fn is_cdr(&self) -> bool {
        matches!(self, Encoding::Cdr)
    }

    /// Check if this encoding is Protobuf.
    pub fn is_protobuf(&self) -> bool {
        matches!(self, Encoding::Protobuf)
    }

    /// Check if this encoding is JSON.
    pub fn is_json(&self) -> bool {
        matches!(self, Encoding::Json)
    }

    /// Convert to string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            Encoding::Cdr => "cdr",
            Encoding::Protobuf => "protobuf",
            Encoding::Json => "json",
        }
    }
}
