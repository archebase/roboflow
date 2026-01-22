// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified writer for robotics data formats.
//!
//! This module provides a high-level writer that supports different
//! formats and writing strategies.

pub mod builder;
pub mod write_strategy;

pub use builder::{WriterBuilder, WriterConfig};
pub use write_strategy::{ParallelWrite, SequentialWrite, WriteStrategy};

use crate::io::metadata::RawMessage;
use crate::io::traits::FormatWriter;
use crate::Result;
use std::path::Path;

/// Unified writer that delegates to format-specific implementations.
pub struct RoboWriter {
    /// The inner format-specific writer
    inner: Box<dyn FormatWriter>,
}

impl RoboWriter {
    /// Create a new writer with automatic format detection based on file extension.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the output file
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use roboflow::io::RoboWriter;
    ///
    /// let writer = RoboWriter::create("output.mcap")?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        WriterBuilder::new().path(path).build()
    }

    /// Create a writer with a specific strategy.
    pub fn create_with_strategy<P: AsRef<Path>>(path: P, _strategy: WriteStrategy) -> Result<Self> {
        // For now, strategy doesn't affect writer creation
        Self::create(path)
    }

    /// Downcast to the inner writer for format-specific operations.
    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.inner.as_any().downcast_ref::<T>()
    }

    /// Downcast mutably to the inner writer.
    pub fn downcast_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.inner.as_any_mut().downcast_mut::<T>()
    }
}

impl FormatWriter for RoboWriter {
    fn path(&self) -> &str {
        self.inner.path()
    }

    fn add_channel(
        &mut self,
        topic: &str,
        message_type: &str,
        encoding: &str,
        schema: Option<&str>,
    ) -> Result<u16> {
        self.inner
            .add_channel(topic, message_type, encoding, schema)
    }

    fn write(&mut self, message: &RawMessage) -> Result<()> {
        self.inner.write(message)
    }

    fn write_batch(&mut self, messages: &[RawMessage]) -> Result<()> {
        self.inner.write_batch(messages)
    }

    fn finish(&mut self) -> Result<()> {
        self.inner.finish()
    }

    fn message_count(&self) -> u64 {
        self.inner.message_count()
    }

    fn channel_count(&self) -> usize {
        self.inner.channel_count()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.inner.as_any()
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self.inner.as_any_mut()
    }
}
