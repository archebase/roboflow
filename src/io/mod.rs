//! Unified I/O layer for robotics data formats.
//!
//! This module provides a clean, trait-based interface for reading and writing
//! robotics data formats (MCAP, ROS1 bag, etc.) with automatic format detection
//! and optimal reading strategy selection.
//!
//! # Architecture
//!
//! The I/O layer is organized into:
//! - **Core types**: arena, traits, detection, metadata, filter, channel_iterator
//! - **Reader**: Reading strategies (sequential, parallel, auto)
//! - **Writer**: Writing implementations
//!
//! # Key Design Principles
//!
//! 1. **Arena-based ownership**: `MmapArena` owns all file data, eliminating
//!    unsafe lifetime transmutes
//! 2. **Strategy pattern**: Automatic selection of optimal reading strategy
//!    (sequential vs parallel) based on file capabilities
//! 3. **First-class format detection**: Uses magic numbers, not just extensions
//! 4. **Clean separation of concerns**: Each module has a single responsibility
//!
//! # Example
//!
//! ```rust,no_run
//! use robocodec::io::{ReaderBuilder, ReadStrategy};
//!
//! // Auto-detect format and strategy
//! let reader = ReaderBuilder::new()
//!     .path("data.mcap")
//!     .build()?;
//!
//! // Force specific strategy
//! let reader = ReaderBuilder::new()
//!     .path("data.mcap")
//!     .strategy(ReadStrategy::Parallel)
//!     .build()?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

// Core modules (now directly under io/)
pub mod arena;
pub mod channel_iterator;
pub mod detection;
pub mod filter;
pub mod metadata;
pub mod traits;

// Reader implementations
pub mod reader;

// Writer implementations
pub mod writer;

// Re-exports for convenience
pub use arena::MmapArena;
pub use detection::detect_format;
pub use metadata::{ChannelInfo, FileFormat, FileInfo, MessageMetadata, RawMessage};
pub use traits::{FormatReader, FormatWriter};

pub use reader::{
    builder::ReaderBuilder,
    strategy::{AutoStrategy, ParallelStrategy, ReadStrategy, SequentialStrategy},
    RoboReader,
};

pub use writer::{
    builder::WriterBuilder,
    write_strategy::{ParallelWrite, SequentialWrite, WriteStrategy},
    RoboWriter,
};

// Re-export from crate::formats for backwards compatibility
pub use crate::formats::{
    bag::{BagFormat, ParallelBagReader, SequentialBagReader, SequentialBagRawIter},
    mcap::{McapFormat, ParallelMcapReader, SequentialMcapReader, SequentialRawIter},
};

// Keep the formats module for KPS (which remains at io::formats::kps)
pub mod formats;

