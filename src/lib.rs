// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # Roboflow
//!
//! A universal, schema-driven runtime decoding engine for Rust.
//!
//! Supports CDR, Protobuf, and JSON message formats with full MCAP support.
//!
//! ## Modules
//!
//! - [`core`] - Core types (CodecValue, errors, registry)
//! - [`encoding`] - Message encoding/decoding (CDR, Protobuf, JSON) - from robocodec
//! - [`schema`] - IDL/MSG schema parser using Pest - from robocodec
//! - [`pipeline`] - Parallel processing pipeline
//! - [`dataset::kps`] - KPS dataset format (experimental)
//!
//! ## Example
//!
//! ```no_run
//! use roboflow::Robocodec;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Convert between formats
//! Robocodec::open(vec!["input.bag"])?
//!     .write_to("output.mcap")
//!     .run()?;
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

// =============================================================================
// Parallel processing pipeline
// =============================================================================
pub mod pipeline;

// =============================================================================
// Schema parsing and encoding (re-exported from robocodec)
// =============================================================================
// Schema and encoding are provided by robofmt - use robocodec::schema::* and robocodec::encoding::*

// =============================================================================
// Dataset structures (KPS)
// =============================================================================
pub mod dataset;

// =============================================================================
// Storage abstraction layer (cloud storage support)
// =============================================================================
#[cfg(feature = "cloud-storage")]
pub mod storage;

// Re-export storage types when feature is enabled
#[cfg(feature = "cloud-storage")]
pub use storage::{
    LocalStorage, OssStorage, ObjectMetadata, SeekRead, SeekableStorage, Storage, StorageError,
};

// =============================================================================
// Re-exports (minimal, focused on user-facing API)
// =============================================================================

// Core types (essential)
pub use core::{CodecError, CodecValue, DecodedMessage, PrimitiveType, Result, RoboflowError};

// Schema parsing (re-exported from robocodec)
pub use robocodec::schema::{FieldType, MessageSchema, parse_schema};

// I/O types (re-exported from robocodec)
pub use robocodec::io::{
    ChannelInfo,
    metadata::RawMessage,
    reader::{ReaderBuilder, RoboReader},
    traits::{FormatReader, FormatWriter},
    writer::RoboWriter,
};
pub use robocodec::transform::TransformBuilder;

// KPS dataset format
pub use dataset::kps::{
    Hdf5KpsWriter, ParquetKpsWriter,
    config::{KpsConfig, Mapping, MappingType, OutputFormat},
    delivery_v12::{
        SeriesDeliveryConfig, SeriesDeliveryConfigBuilder, StatisticsCollector, TaskInfo,
        TaskStatistics, V12DeliveryBuilder,
    },
};

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
// TODO: Re-add high-level reader/writer type aliases once API is stabilized
// pub type Reader = io::RoboReader;
// pub type Writer = io::RoboWriter;

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
