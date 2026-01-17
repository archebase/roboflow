//! ROS1 bag file format support.
//!
//! Provides reading and writing of ROS1 bag files.

pub mod writer;
pub mod rewriter;

pub use writer::{BagMessage, BagWriter};
pub use rewriter::BagRewriter;

// Re-export bag-related types from the reader module
pub use crate::reader::{BagRawMessageIter, BagRawMessageStream};
