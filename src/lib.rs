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
//! - [`roboflow_core::CodecValue`] - Core value types
//! - [`roboflow_core::RoboflowError`] - Error handling
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

// Re-export from roboflow-core
pub use roboflow_core::{
    CodecValue, DecodedMessage, Encoding, ErrorCategory, PrimitiveType, Result, RoboflowError,
    SchemaProvider, TypeAccessor, TypeRegistry,
};

// Re-export CodecError from robocodec
pub use robocodec::core::CodecError;

// Legacy: keep the old `core::` module path for backward compatibility
// This will be deprecated in a future release
pub mod core {
    pub use robocodec::core::CodecError;
    pub use roboflow_core::{
        CodecValue, DecodedMessage, Encoding, ErrorCategory, PrimitiveType, Result, RoboflowError,
        SchemaProvider, TypeAccessor, TypeRegistry,
    };
}

// =============================================================================
// Parallel processing pipeline
// =============================================================================
// Pipeline is now provided by roboflow-pipeline crate
pub use roboflow_pipeline::{
    auto_config::PerformanceMode,
    config::CompressionConfig,
    fluent::{BatchReport, CompressionPreset, PipelineMode, ReadOptions, Robocodec},
    hyper::{HyperPipeline, HyperPipelineConfig, HyperPipelineReport},
};

// =============================================================================
// Schema parsing and encoding (re-exported from robocodec)
// =============================================================================
// Schema and encoding are provided by robocodec - use robocodec::schema::* and robocodec::encoding::*

// =============================================================================
// Dataset structures
// =============================================================================
// Dataset is now provided by roboflow-dataset crate
pub use roboflow_dataset::{
    DatasetConfig, DatasetFormat, DatasetWriter, ImageData,
    kps::{
        ParquetKpsWriter,
        config::{KpsConfig, Mapping, MappingType, OutputFormat},
        delivery_v12::{
            SeriesDeliveryConfig, SeriesDeliveryConfigBuilder, StatisticsCollector, TaskInfo,
            TaskStatistics, V12DeliveryBuilder,
        },
    },
    lerobot::{
        LerobotConfig, LerobotWriter, LerobotWriterTrait,
        config::{DatasetConfig as LerobotDatasetConfig, VideoConfig},
    },
    streaming::StreamingDatasetConverter,
};

// Re-export the full kps module for test access
pub use roboflow_dataset::kps;

// Re-export lerobot and streaming modules for test access
pub use roboflow_dataset::lerobot;
pub use roboflow_dataset::streaming;

// =============================================================================
// Storage abstraction layer (always available via roboflow-storage)
// =============================================================================
pub use roboflow_storage::{
    CacheConfig, CacheStats, CachedStorage, EvictionPolicy, LocalStorage, MultipartConfig,
    MultipartStats, ObjectMetadata, OssConfig, OssStorage, RetryConfig, RetryingStorage, SeekRead,
    SeekableStorage, Storage, StorageConfig, StorageError, StorageFactory, StorageResult,
    StorageUrl,
};

// =============================================================================
// Distributed coordination (TiKV backend)
// =============================================================================
#[cfg(feature = "distributed")]
pub use roboflow_distributed::{
    DEFAULT_CONNECTION_TIMEOUT_SECS, DEFAULT_PD_ENDPOINTS, KEY_PREFIX,
    tikv::{
        CheckpointState, HeartbeatRecord, JobRecord, JobStatus, LockRecord, TikvClient, TikvConfig,
        TikvError, WorkerStatus,
    },
};

// Schema parsing (re-exported from robocodec)
pub use robocodec::schema::{FieldType, MessageSchema, parse_schema};

// I/O types (re-exported from robocodec)
pub use robocodec::io::{
    ChannelInfo,
    metadata::RawMessage,
    reader::RoboReader,
    traits::{FormatReader, FormatWriter},
    writer::RoboWriter,
};
pub use robocodec::transform::TransformBuilder;

// Configuration
pub use config::NormalizeConfig;

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
