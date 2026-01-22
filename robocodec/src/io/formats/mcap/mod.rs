// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! MCAP format implementation.
//!
//! This module provides a complete MCAP reader/writer implementation with:
//! - Parallel chunk-based reading for optimal performance
//! - Sequential reading using the mcap crate
//! - Automatic encoding detection and decoding
//! - Custom writer with manual chunk control for parallel compression
//!
//! **Note:** This implementation uses a custom MCAP parser with no external dependencies
//! for the parallel reader. The sequential reader uses the mcap crate for compatibility.

// Re-export constants at module level for convenience
pub use constants::{
    MCAP_MAGIC, OP_CHANNEL, OP_CHUNK, OP_CHUNK_INDEX, OP_DATA_END, OP_FOOTER, OP_HEADER,
    OP_MESSAGE, OP_SCHEMA, OP_STATISTICS, OP_SUMMARY_OFFSET,
};

// Constants module (pub for format/writer/mcap.rs access)
pub mod constants;

// Parallel reader implementation
pub mod parallel;

// Sequential reader implementation
pub mod sequential;

// Two-pass reader for files without summary
pub mod two_pass;

// Raw reader implementation
pub mod reader_raw;

// High-level API (auto-decoding reader + custom writer)
pub mod reader;
pub mod writer;

// Re-exports
pub use parallel::{ChunkIndex, McapFormat, ParallelMcapReader};
pub use reader::{ChannelInfo, McapReader, RawMessage, TimestampedDecodedMessage};
pub use sequential::{SequentialMcapReader, SequentialRawIter};
pub use two_pass::TwoPassMcapReader;
pub use writer::ParallelMcapWriter;

// Re-export DecodedMessage from core
pub use crate::core::DecodedMessage;
