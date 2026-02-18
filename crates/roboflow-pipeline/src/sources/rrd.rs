// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Rerun Data (.rrd) source implementation.
//!
//! RRD is the native recording format of the [Rerun](https://rerun.io) visualization
//! SDK. This module provides a Source scaffold for reading `.rrd` files.
//!
//! **Status**: Scaffold only — full decoding requires the `re_sdk` / `re_log_types`
//! crates which are not yet integrated.

use crate::sources::{Source, SourceConfig, SourceError, SourceMetadata, SourceResult, TimestampedMessage};

/// Rerun Data (.rrd) source reader.
///
/// Reads robotics/sensor data captured by the Rerun SDK.
///
/// **Note**: RRD decoding is not yet implemented. This source will return
/// an informative error when `initialize()` is called.
pub struct RrdSource {
    path: String,
    metadata: Option<SourceMetadata>,
}

impl RrdSource {
    /// Create a new RRD source from a file path or URL.
    pub fn new(path: impl Into<String>) -> SourceResult<Self> {
        Ok(Self {
            path: path.into(),
            metadata: None,
        })
    }

    /// Create a new RRD source from a SourceConfig.
    pub fn from_config(config: &SourceConfig) -> SourceResult<Self> {
        match &config.source_type {
            crate::SourceType::Rrd { path } => Self::new(path),
            _ => Err(SourceError::InvalidConfig(
                "Invalid config for RrdSource".to_string(),
            )),
        }
    }
}

#[async_trait::async_trait]
impl Source for RrdSource {
    async fn initialize(&mut self, config: &SourceConfig) -> SourceResult<SourceMetadata> {
        // Update path from config if provided
        if let crate::SourceType::Rrd { path } = &config.source_type {
            self.path = path.clone();
        }

        Err(SourceError::UnsupportedFormat(format!(
            "RRD format is not yet supported (file: {}). \
             RRD decoding requires the re_sdk crate. \
             Convert to MCAP first: `rerun export --input {} --output output.mcap`",
            self.path, self.path
        )))
    }

    async fn read_batch(
        &mut self,
        _batch_size: usize,
    ) -> SourceResult<Option<Vec<TimestampedMessage>>> {
        Err(SourceError::UnsupportedFormat(
            "RRD source: not yet implemented".to_string(),
        ))
    }

    async fn metadata(&self) -> SourceResult<SourceMetadata> {
        self.metadata
            .clone()
            .ok_or_else(|| SourceError::ReadFailed("Source not initialized".to_string()))
    }

    fn supports_seeking(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rrd_source_creation() {
        let source = RrdSource::new("test.rrd");
        assert!(source.is_ok());
    }

    #[test]
    fn test_rrd_source_from_config() {
        let config = SourceConfig::rrd("test.rrd");
        let source = RrdSource::from_config(&config);
        assert!(source.is_ok());
    }

    #[test]
    fn test_rrd_source_invalid_config() {
        let config = SourceConfig::mcap("test.mcap");
        let source = RrdSource::from_config(&config);
        assert!(source.is_err());
    }
}
