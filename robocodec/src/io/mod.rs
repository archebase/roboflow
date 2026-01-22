// Copyright (c) 2026 ArcheBase
// Roboflow is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//     http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

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
