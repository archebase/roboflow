//! High-level readers with automatic decoding.
//!
//! This module provides format-specific readers that handle:
//! - Automatic encoding detection (CDR, Protobuf, JSON)
//! - Message decoding into CodecValue
//! - Convenience iterators for decoded messages
//!
//! For low-level raw message iteration, use `robocodec::io::formats`.

pub mod mcap;

// Re-exports for convenience
pub use mcap::{ChannelInfo, McapReader, RawMessage};
