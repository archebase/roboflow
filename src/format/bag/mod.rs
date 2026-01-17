//! ROS1 bag file format support.
//!
//! Provides reading and writing of ROS1 bag files.

pub mod rewriter;
pub mod writer;

pub use rewriter::BagRewriter;
pub use writer::{BagMessage, BagWriter};

// Re-export bag-related types from the reader module
pub use crate::reader::{BagRawMessageIter, BagRawMessageStream};
