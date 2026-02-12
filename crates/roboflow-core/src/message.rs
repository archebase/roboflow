// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Message types for inter-module communication.
//!
//! These types define the data structures passed between sources, sinks,
//! and processing stages in the roboflow pipeline.

use robocodec::CodecValue;

/// A decoded message from a source.
///
/// This is the primary output type for all sources, providing a unified
/// interface regardless of the underlying file format (MCAP, Bag, HDF5, etc.).
///
/// # Fields
///
/// - `topic`: Channel/topic name identifying the message source
/// - `log_time`: Log timestamp in nanoseconds
/// - `data`: Decoded message data as a codec value
///
/// # Example
///
/// ```ignore
/// use roboflow_core::TimestampedMessage;
/// use robocodec::CodecValue;
///
/// let msg = TimestampedMessage {
///     topic: "/camera/image".to_string(),
///     log_time: 1_000_000_000,
///     data: CodecValue::Struct(/* ... */),
/// };
/// ```
#[derive(Debug, Clone)]
pub struct TimestampedMessage {
    /// Channel/topic name
    pub topic: String,
    /// Log timestamp (nanoseconds)
    pub log_time: u64,
    /// Decoded message data
    pub data: CodecValue,
}
