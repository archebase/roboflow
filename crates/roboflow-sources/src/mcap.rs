// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! MCAP source implementation.
//!
//! This module provides a Source implementation for reading MCAP files
//! using the robocodec library.

use crate::{Source, SourceConfig, SourceMetadata, SourceResult, TimestampedMessage};

/// MCAP source reader.
///
/// This source reads robotics data from MCAP files, which are a
/// log file format for robotics applications.
pub struct McapSource {
    /// Path to the MCAP file
    path: String,
    /// Metadata cached after initialization
    metadata: Option<SourceMetadata>,
    /// The reader is stored in an async-friendly way
    _reader_private: (),
}

impl McapSource {
    /// Create a new MCAP source from a file path.
    pub fn new(path: impl Into<String>) -> SourceResult<Self> {
        let path = path.into();
        Ok(Self {
            path,
            metadata: None,
            _reader_private: (),
        })
    }

    /// Create a new MCAP source from a SourceConfig.
    pub fn from_config(config: &SourceConfig) -> SourceResult<Self> {
        match &config.source_type {
            crate::SourceType::Mcap { path } => Self::new(path),
            _ => Err(crate::SourceError::InvalidConfig(
                "Invalid config for McapSource".to_string(),
            )),
        }
    }
}

#[async_trait::async_trait]
impl Source for McapSource {
    async fn initialize(&mut self, _config: &SourceConfig) -> SourceResult<SourceMetadata> {
        // Open the MCAP file to get metadata
        let reader = robocodec::RoboReader::open(&self.path).map_err(|e| {
            crate::SourceError::OpenFailed {
                path: self.path.clone().into(),
                error: Box::new(e),
            }
        })?;

        // Extract metadata using the FormatReader trait
        use robocodec::io::traits::FormatReader;

        let message_count = reader.message_count();

        // Create basic metadata
        // Note: topic information would require iterating through channels
        let metadata = SourceMetadata::new("mcap".to_string(), self.path.clone())
            .with_message_count(message_count);

        self.metadata = Some(metadata.clone());

        Ok(metadata)
    }

    async fn read_batch(
        &mut self,
        _batch_size: usize,
    ) -> SourceResult<Option<Vec<TimestampedMessage>>> {
        // This is a simplified implementation that demonstrates the API.
        // A production implementation would:
        // 1. Open the reader
        // 2. Use the decoded() iterator
        // 3. Collect up to batch_size messages
        // 4. Return them

        // For now, return end of stream
        Err(crate::SourceError::ReadFailed(
            "MCAP source read not yet implemented - use robocodec::RoboReader directly".to_string(),
        ))
    }

    async fn seek(&mut self, _timestamp: u64) -> SourceResult<()> {
        Err(crate::SourceError::SeekNotSupported)
    }

    async fn metadata(&self) -> SourceResult<SourceMetadata> {
        self.metadata
            .clone()
            .ok_or_else(|| crate::SourceError::EndOfStream)
    }

    fn supports_seeking(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mcap_source_creation() {
        let source = McapSource::new("test.mcap");
        assert!(source.is_ok());
        let source = source.unwrap();
        assert_eq!(source.path, "test.mcap");
    }

    #[test]
    fn test_mcap_source_from_config() {
        let config = SourceConfig::mcap("test.mcap");
        let source = McapSource::from_config(&config);
        assert!(source.is_ok());
    }

    #[test]
    fn test_mcap_source_invalid_config() {
        let config = SourceConfig::bag("test.bag");
        let source = McapSource::from_config(&config);
        assert!(source.is_err());
    }
}
