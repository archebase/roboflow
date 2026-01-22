// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified reader with automatic strategy selection.
//!
//! This module provides a high-level reader that automatically selects
//! the optimal reading strategy (sequential vs parallel) based on file
//! capabilities and configuration.
//!
//! # Strategy Selection
//!
//! The reader supports three strategies:
//! - **Auto**: Automatically choose parallel for MCAP with summary, sequential otherwise
//! - **Sequential**: Always read sequentially (fallback)
//! - **Parallel**: Force parallel reading (requires MCAP with summary)
//!
//! # Example
//!
//! ```rust,no_run
//! use roboflow::io::{ReaderBuilder, ReadStrategy};
//!
//! // Auto-detect strategy
//! let reader = ReaderBuilder::new()
//!     .path("data.mcap")
//!     .build()?;
//!
//! // Force specific strategy
//! let reader = ReaderBuilder::new()
//!     .path("data.mcap")
//!     .strategy(ReadStrategy::Parallel)
//!     .build()?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

pub mod builder;
pub mod strategy;

pub use builder::{ReaderBuilder, ReaderConfig};
pub use strategy::{AutoStrategy, ParallelStrategy, ReadStrategy, SequentialStrategy};

use crate::io::traits::FormatReader;
use crate::Result;
use std::path::Path;

/// Unified reader that delegates to the optimal strategy.
///
/// This type provides a consistent API regardless of the underlying
/// strategy (sequential or parallel). Supports auto-detection of
/// BAG and MCAP formats.
pub struct RoboReader {
    /// The inner format-specific reader
    inner: Box<dyn FormatReader>,
    /// The strategy being used
    strategy: ReadStrategy,
}

impl RoboReader {
    /// Open a file with automatic strategy detection.
    ///
    /// This is the simplest way to open a file - the reader will
    /// automatically detect the format and choose the optimal strategy.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file to open
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use roboflow::io::RoboReader;
    ///
    /// let reader = RoboReader::open("data.mcap")?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        ReaderBuilder::new().path(path).build()
    }

    /// Open a file with a specific strategy.
    ///
    /// Use this when you want to force a particular reading strategy
    /// instead of relying on automatic detection.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file to open
    /// * `strategy` - The strategy to use for reading
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use roboflow::io::{RoboReader, ReadStrategy};
    ///
    /// let reader = RoboReader::open_with_strategy(
    ///     "data.mcap",
    ///     ReadStrategy::Sequential
    /// )?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn open_with_strategy<P: AsRef<Path>>(path: P, strategy: ReadStrategy) -> Result<Self> {
        ReaderBuilder::new().path(path).strategy(strategy).build()
    }

    /// Get the reading strategy being used.
    pub fn strategy(&self) -> &ReadStrategy {
        &self.strategy
    }

    /// Downcast to the inner reader for format-specific operations.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use robocodec::io::RoboReader;
    /// # use robocodec::io::formats::mcap::McapFormat;
    /// # fn test() -> Result<(), Box<dyn std::error::Error>> {
    /// # let reader = RoboReader::open("data.mcap")?;
    /// if let Some(mcap) = reader.downcast_ref::<McapFormat>() {
    ///     // Access MCAP-specific methods
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.inner.as_any().downcast_ref::<T>()
    }

    /// Downcast mutably to the inner reader.
    pub fn downcast_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.inner.as_any_mut().downcast_mut::<T>()
    }

    /// Decode messages from the reader.
    ///
    /// This method requires the inner reader to support message decoding
    /// (e.g., McapReader). Returns an error if the inner reader doesn't
    /// support this operation.
    ///
    /// # Returns
    ///
    /// An iterator yielding `(DecodedMessage, ChannelInfo)` tuples.
    pub fn decode_messages(
        &self,
    ) -> Result<crate::io::formats::mcap::reader::DecodedMessageIter<'_>> {
        use crate::io::formats::mcap::reader::McapReader;

        // Try to downcast to McapReader (which has decode_messages)
        if let Some(mcap) = self.downcast_ref::<McapReader>() {
            return mcap.decode_messages();
        }

        Err(crate::CodecError::parse(
            "RoboReader",
            "decode_messages not supported for this format",
        ))
    }

    /// Decode messages with timestamps from the reader.
    ///
    /// Similar to `decode_messages` but includes log_time and publish_time
    /// for each message.
    pub fn decode_messages_with_timestamp(
        &self,
    ) -> Result<crate::io::formats::mcap::reader::DecodedMessageWithTimestampIter<'_>> {
        use crate::io::formats::mcap::reader::McapReader;

        if let Some(mcap) = self.downcast_ref::<McapReader>() {
            return mcap.decode_messages_with_timestamp();
        }

        Err(crate::CodecError::parse(
            "RoboReader",
            "decode_messages_with_timestamp not supported for this format",
        ))
    }
}

impl FormatReader for RoboReader {
    fn channels(&self) -> &std::collections::HashMap<u16, crate::io::metadata::ChannelInfo> {
        self.inner.channels()
    }

    fn channel_by_topic(&self, topic: &str) -> Option<&crate::io::metadata::ChannelInfo> {
        self.inner.channel_by_topic(topic)
    }

    fn channels_by_topic(&self, topic: &str) -> Vec<&crate::io::metadata::ChannelInfo> {
        self.inner.channels_by_topic(topic)
    }

    fn message_count(&self) -> u64 {
        self.inner.message_count()
    }

    fn start_time(&self) -> Option<u64> {
        self.inner.start_time()
    }

    fn end_time(&self) -> Option<u64> {
        self.inner.end_time()
    }

    fn path(&self) -> &str {
        self.inner.path()
    }

    fn format(&self) -> crate::io::metadata::FileFormat {
        self.inner.format()
    }

    fn file_size(&self) -> u64 {
        self.inner.file_size()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.inner.as_any()
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self.inner.as_any_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unified_reader_creation() {
        // This is a placeholder test - real tests would use actual files
        // The structure demonstrates the API usage
        let _builder = ReaderBuilder::new();
    }
}
