//! MCAP format support for robofmt.
//!
//! This module provides readers and writers for MCAP files,
//! a high-performance robotics data format.

pub mod constants;
pub mod reader;

pub use reader::{McapFormat, ParallelMcapReader};
pub use constants::MCAP_MAGIC;
