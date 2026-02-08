// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! ROS Bag source implementation.
//!
//! This module provides a Source implementation for reading ROS bag files
//! using the robocodec library.

use crate::{Source, SourceConfig, SourceMetadata, SourceResult, TimestampedMessage};

/// ROS Bag source reader.
///
/// This source reads robotics data from ROS bag files.
pub struct BagSource {
    /// Path to the bag file
    path: String,
    /// Metadata cached after initialization
    metadata: Option<SourceMetadata>,
    /// Placeholder for future reader storage
    _reader_private: (),
}

impl BagSource {
    /// Create a new Bag source from a file path.
    pub fn new(path: impl Into<String>) -> SourceResult<Self> {
        let path = path.into();
        Ok(Self {
            path,
            metadata: None,
            _reader_private: (),
        })
    }

    /// Create a new Bag source from a SourceConfig.
    pub fn from_config(config: &SourceConfig) -> SourceResult<Self> {
        match &config.source_type {
            crate::SourceType::Bag { path } => Self::new(path),
            _ => Err(crate::SourceError::InvalidConfig(
                "Invalid config for BagSource".to_string(),
            )),
        }
    }
}

#[async_trait::async_trait]
impl Source for BagSource {
    async fn initialize(&mut self, _config: &SourceConfig) -> SourceResult<SourceMetadata> {
        // Open the bag file to get metadata
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
        let metadata = SourceMetadata::new("bag".to_string(), self.path.clone())
            .with_message_count(message_count);

        self.metadata = Some(metadata.clone());

        Ok(metadata)
    }

    async fn read_batch(
        &mut self,
        _batch_size: usize,
    ) -> SourceResult<Option<Vec<TimestampedMessage>>> {
        // This is a simplified implementation that demonstrates the API.
        // A production implementation would use robocodec::RoboReader directly
        Err(crate::SourceError::ReadFailed(
            "Bag source read not yet implemented - use robocodec::RoboReader directly".to_string(),
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
    fn test_bag_source_creation() {
        let source = BagSource::new("test.bag");
        assert!(source.is_ok());
        let source = source.unwrap();
        assert_eq!(source.path, "test.bag");
    }

    #[test]
    fn test_bag_source_from_config() {
        let config = SourceConfig::bag("test.bag");
        let source = BagSource::from_config(&config);
        assert!(source.is_ok());
    }

    #[test]
    fn test_bag_source_invalid_config() {
        let config = SourceConfig::mcap("test.mcap");
        let source = BagSource::from_config(&config);
        assert!(source.is_err());
    }
}
