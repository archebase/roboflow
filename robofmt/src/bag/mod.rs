//! ROS1 bag format support for robofmt.
//!
//! This module provides readers and writers for ROS1 bag files.

pub mod parser;
pub mod reader;

pub use reader::{BagFormat, ParallelBagReader, BagRawIter};
pub use parser::{BagConnection, BagChunkInfo, BagParser, BagMessageData};
