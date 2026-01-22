// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! I/O layer for robotics data formats.
//!
//! This module provides the foundational types and traits for reading
//! and writing robotics data files.

pub mod arena;
pub mod detection;
pub mod formats;
pub mod metadata;

// Re-exports
pub use arena::{MmapArena, MmapArenaRef};
pub use detection::{detect_format, is_bag_file, is_mcap_file, FormatDetector};
pub use metadata::{ChannelInfo, FileFormat, FileInfo, MessageMetadata, RawMessage};

// Channel iterator (tightly coupled with pipeline - keep in roboflow)
// pub mod channel_iterator;

// Traits for format readers and writers
pub mod traits;
pub use traits::{FormatReader, FormatWriter};

// Re-export parallel reader types
pub use traits::{MessageChunkData, ParallelReader, ParallelReaderConfig, ParallelReaderStats};

// Filter for topic filtering
pub mod filter;
pub use filter::{ChannelFilter, TopicFilter};

// Unified reader/writer with auto-detection
pub mod reader;
pub mod writer;
pub use reader::RoboReader;
pub use writer::RoboWriter;
