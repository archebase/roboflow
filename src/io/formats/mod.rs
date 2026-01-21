//! Low-level format implementations.
//!
//! This module contains format-specific readers that implement the
//! [`FormatReader`] trait from `crate::io::traits`.
//!
//! These readers provide:
//! - Parallel chunk-based reading (default)
//! - Sequential reading using proven crates (mcap, rosbag)
//! - Arena-based memory management for safety
//! - Memory-mapped file access for performance
//!
//! For high-level APIs with automatic decoding, use `crate::format`.
//!
//! ## Available Formats
//!
//! - **MCAP**: `mcap`, `mcap_two_pass`, `sequential_mcap`
//! - **BAG**: `bag`, `bag_parallel`, `sequential_bag`
//! - **KPS**: `kps` (dataset format for robotics learning)
//!
//! ## Example: Parallel reading
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use robocodec::io::formats::McapFormat;
//! use robocodec::io::traits::ParallelReader;
//! use crossbeam_channel::unbounded;
//!
//! let reader = McapFormat::open("file.mcap")?;
//! let (sender, receiver) = unbounded();
//! reader.read_parallel(robocodec::io::traits::ParallelReaderConfig::default(), sender)?;
//! # Ok(())
//! # }
//! ```

pub mod bag;
pub mod bag_parallel;
pub mod bag_parser;
pub mod kps;
pub mod mcap;
pub mod mcap_constants;
pub mod mcap_two_pass;
pub mod sequential_bag;
pub mod sequential_mcap;

// Re-exports
pub use bag::{BagFormat, BagRawIter, ParallelBagReader};
pub use bag_parallel::ParallelBagReader as BagParallelReader;
pub use mcap::{McapFormat, ParallelMcapReader};
pub use mcap_two_pass::TwoPassMcapReader;
pub use sequential_bag::{BagSequentialFormat, SequentialBagRawIter, SequentialBagReader};
pub use sequential_mcap::{SequentialMcapReader, SequentialRawIter, SequentialRawStream};
