// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! S3 prefix source implementation.
//!
//! This module provides a source that reads multiple files from an S3/OSS prefix.
//! It lists all supported files (MCAP, Bag, RRD) in the prefix and aggregates
//! their messages in chronological order.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use roboflow_storage::StorageFactory;

use crate::{
    Source, SourceConfig, SourceError, SourceMetadata, SourceResult, TimestampedMessage,
    create_source,
};

/// Supported file extensions for S3 prefix source.
const SUPPORTED_EXTENSIONS: [&str; 3] = [".mcap", ".bag", ".rrd"];

/// S3 prefix source reader.
///
/// Reads robotics data from multiple files in an S3/OSS prefix.
/// Files are processed in lexicographic order by name.
pub struct S3PrefixSource {
    /// The S3/OSS prefix URL.
    prefix_url: String,
    /// Storage backend for listing files.
    storage: Arc<dyn roboflow_storage::Storage>,
    /// List of files to process.
    files: Vec<String>,
    /// Current file index.
    current_file_index: usize,
    /// Current source being read.
    current_source: Option<Box<dyn Source>>,
    /// Current source config.
    current_config: Option<SourceConfig>,
    /// Combined metadata from all files.
    metadata: Option<SourceMetadata>,
}

impl S3PrefixSource {
    /// Create a new S3 prefix source.
    ///
    /// # Arguments
    ///
    /// * `prefix_url` - S3/OSS prefix URL (e.g., "s3://bucket/data/episode_001/")
    ///
    /// # Returns
    ///
    /// A new S3PrefixSource instance, or an error if the URL is invalid.
    pub fn new(prefix_url: impl Into<String>) -> SourceResult<Self> {
        let prefix_url = prefix_url.into();

        // Validate URL scheme
        if !prefix_url.starts_with("s3://") && !prefix_url.starts_with("oss://") {
            return Err(SourceError::InvalidConfig(format!(
                "S3PrefixSource requires s3:// or oss:// URL, got: {}",
                prefix_url
            )));
        }

        // Create storage backend
        let storage = StorageFactory::from_env()
            .create(&prefix_url)
            .map_err(|e| {
                SourceError::Storage(format!("Failed to create storage for {}: {}", prefix_url, e))
            })?;

        Ok(Self {
            prefix_url,
            storage,
            files: Vec::new(),
            current_file_index: 0,
            current_source: None,
            current_config: None,
            metadata: None,
        })
    }

    /// Check if a file has a supported extension.
    fn is_supported_file(path: &str) -> bool {
        let path_lower = path.to_lowercase();
        SUPPORTED_EXTENSIONS.iter().any(|ext| path_lower.ends_with(ext))
    }

    /// Determine the source type from a file path.
    fn get_source_type(path: &str) -> Option<&'static str> {
        let path_lower = path.to_lowercase();
        if path_lower.ends_with(".mcap") {
            Some("mcap")
        } else if path_lower.ends_with(".bag") {
            Some("bag")
        } else if path_lower.ends_with(".rrd") {
            Some("rrd")
        } else {
            None
        }
    }

    /// Create a source config for a file.
    fn create_config_for_file(url: &str) -> Option<SourceConfig> {
        let source_type = Self::get_source_type(url)?;
        match source_type {
            "mcap" => Some(SourceConfig::mcap(url)),
            "bag" => Some(SourceConfig::bag(url)),
            "rrd" => Some(SourceConfig::rrd(url)),
            _ => None,
        }
    }

    /// List all supported files in the prefix.
    fn list_files(&self) -> SourceResult<Vec<String>> {
        // Extract the prefix path from the URL
        let prefix_path = self.extract_prefix_path()?;

        // List objects in the prefix
        let objects = self.storage.list(Path::new(&prefix_path)).map_err(|e| {
            SourceError::Storage(format!("Failed to list prefix {}: {}", prefix_path, e))
        })?;

        // Filter to supported files and sort
        let mut files: Vec<String> = objects
            .into_iter()
            .filter(|obj| !obj.is_dir && Self::is_supported_file(&obj.path))
            .map(|obj| {
                // Reconstruct full URL
                format!("{}/{}", self.prefix_url.trim_end_matches('/'), obj.path.trim_start_matches('/'))
            })
            .collect();

        // Sort files lexicographically for consistent ordering
        files.sort();

        Ok(files)
    }

    /// Extract the prefix path from a URL.
    ///
    /// For "s3://bucket/path/to/prefix/", returns "path/to/prefix/"
    fn extract_prefix_path_from_url(url: &str) -> SourceResult<String> {
        // Remove scheme
        let without_scheme = url
            .strip_prefix("s3://")
            .or_else(|| url.strip_prefix("oss://"))
            .ok_or_else(|| {
                SourceError::InvalidConfig(format!("Invalid URL scheme: {}", url))
            })?;

        // Find the first slash to separate bucket from path
        let slash_pos = without_scheme.find('/').ok_or_else(|| {
            SourceError::InvalidConfig(format!("No path in URL: {}", url))
        })?;

        // Return everything after the bucket
        Ok(without_scheme[slash_pos + 1..].to_string())
    }

    /// Extract the prefix path from the URL.
    fn extract_prefix_path(&self) -> SourceResult<String> {
        Self::extract_prefix_path_from_url(&self.prefix_url)
    }

    /// Open the next file in the list.
    async fn open_next_file(&mut self) -> SourceResult<bool> {
        // Close current source if any
        self.current_source = None;
        self.current_config = None;

        // Check if there are more files
        if self.current_file_index >= self.files.len() {
            return Ok(false);
        }

        let file_url = &self.files[self.current_file_index];
        self.current_file_index += 1;

        tracing::debug!(
            prefix = %self.prefix_url,
            file = %file_url,
            index = self.current_file_index,
            total = self.files.len(),
            "Opening next file in S3 prefix"
        );

        // Create source config
        let config = Self::create_config_for_file(file_url)
            .ok_or_else(|| SourceError::UnsupportedFormat(file_url.clone()))?;

        // Create source
        let mut source = create_source(&config)?;

        // Initialize source
        source.initialize(&config).await?;

        self.current_source = Some(source);
        self.current_config = Some(config);

        Ok(true)
    }
}

#[async_trait]
impl Source for S3PrefixSource {
    async fn initialize(&mut self, config: &SourceConfig) -> SourceResult<SourceMetadata> {
        // Get the prefix URL from config
        let prefix_url = match &config.source_type {
            crate::SourceType::S3Prefix { url } => url.clone(),
            _ => {
                return Err(SourceError::InvalidConfig(format!(
                    "Expected S3Prefix config, got {:?}",
                    config.source_type.name()
                )));
            }
        };

        // Update prefix URL
        self.prefix_url = prefix_url.clone();

        // Re-create storage with new URL
        self.storage = StorageFactory::from_env()
            .create(&prefix_url)
            .map_err(|e| {
                SourceError::Storage(format!("Failed to create storage for {}: {}", prefix_url, e))
            })?;

        // List files in the prefix
        self.files = self.list_files()?;

        if self.files.is_empty() {
            tracing::warn!(
                prefix = %self.prefix_url,
                "No supported files found in S3 prefix"
            );
        } else {
            tracing::info!(
                prefix = %self.prefix_url,
                file_count = self.files.len(),
                "Found files in S3 prefix"
            );
        }

        // Reset state
        self.current_file_index = 0;
        self.current_source = None;
        self.current_config = None;

        // Open first file if available
        if !self.files.is_empty() {
            self.open_next_file().await?;
        }

        // Build combined metadata
        // For now, use a simple metadata structure
        // In a full implementation, we'd aggregate metadata from all files
        let mut metadata = SourceMetadata::new(
            "s3-prefix".to_string(),
            self.prefix_url.clone(),
        );
        metadata.metadata.insert(
            "file_count".to_string(),
            serde_json::json!(self.files.len()),
        );

        self.metadata = Some(metadata.clone());
        Ok(metadata)
    }

    async fn read_batch(&mut self, size: usize) -> SourceResult<Option<Vec<TimestampedMessage>>> {
        // If no files, return None
        if self.files.is_empty() {
            return Ok(None);
        }

        let mut all_messages = Vec::with_capacity(size);

        // Read from current file(s) until we have enough messages
        while all_messages.len() < size {
            // Check if we have a current source
            let source = match &mut self.current_source {
                Some(s) => s,
                None => {
                    // Try to open next file
                    if !self.open_next_file().await? {
                        // No more files
                        break;
                    }
                    self.current_source.as_mut().unwrap()
                }
            };

            // Read from current source
            let remaining = size - all_messages.len();
            match source.read_batch(remaining).await {
                Ok(Some(messages)) => {
                    all_messages.extend(messages);
                }
                Ok(None) => {
                    // End of current file, try next
                    tracing::debug!(
                        prefix = %self.prefix_url,
                        index = self.current_file_index,
                        total = self.files.len(),
                        "Finished reading file, moving to next"
                    );
                    self.current_source = None;
                    self.current_config = None;
                }
                Err(e) => {
                    // Log error but continue to next file
                    tracing::warn!(
                        prefix = %self.prefix_url,
                        index = self.current_file_index,
                        error = %e,
                        "Error reading from file, skipping to next"
                    );
                    self.current_source = None;
                    self.current_config = None;
                }
            }
        }

        if all_messages.is_empty() && self.current_file_index >= self.files.len() {
            Ok(None)
        } else {
            Ok(Some(all_messages))
        }
    }

    async fn metadata(&self) -> SourceResult<SourceMetadata> {
        self.metadata.clone().ok_or_else(|| {
            SourceError::InvalidConfig("S3PrefixSource not initialized".to_string())
        })
    }

    fn supports_seeking(&self) -> bool {
        // S3 prefix source doesn't support seeking across files
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_supported_file() {
        assert!(S3PrefixSource::is_supported_file("test.mcap"));
        assert!(S3PrefixSource::is_supported_file("test.MCAP"));
        assert!(S3PrefixSource::is_supported_file("path/to/test.bag"));
        assert!(S3PrefixSource::is_supported_file("test.rrd"));
        assert!(!S3PrefixSource::is_supported_file("test.txt"));
        assert!(!S3PrefixSource::is_supported_file("test.mp4"));
    }

    #[test]
    fn test_get_source_type() {
        assert_eq!(S3PrefixSource::get_source_type("test.mcap"), Some("mcap"));
        assert_eq!(S3PrefixSource::get_source_type("test.MCAP"), Some("mcap"));
        assert_eq!(S3PrefixSource::get_source_type("test.bag"), Some("bag"));
        assert_eq!(S3PrefixSource::get_source_type("test.rrd"), Some("rrd"));
        assert_eq!(S3PrefixSource::get_source_type("test.txt"), None);
    }

    #[test]
    fn test_create_config_for_file() {
        let config = S3PrefixSource::create_config_for_file("s3://bucket/test.mcap");
        assert!(config.is_some());
        let config = config.unwrap();
        assert_eq!(config.source_type.name(), "mcap");

        let config = S3PrefixSource::create_config_for_file("s3://bucket/test.bag");
        assert!(config.is_some());
        let config = config.unwrap();
        assert_eq!(config.source_type.name(), "bag");

        let config = S3PrefixSource::create_config_for_file("s3://bucket/test.txt");
        assert!(config.is_none());
    }

    #[test]
    fn test_new_with_invalid_url() {
        let result = S3PrefixSource::new("/local/path");
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_prefix_path() {
        let path = S3PrefixSource::extract_prefix_path_from_url("s3://bucket/path/to/prefix/").unwrap();
        assert_eq!(path, "path/to/prefix/");

        let path = S3PrefixSource::extract_prefix_path_from_url("oss://bucket/data/").unwrap();
        assert_eq!(path, "data/");

        // Test with no trailing slash
        let path = S3PrefixSource::extract_prefix_path_from_url("s3://bucket/path/to/data").unwrap();
        assert_eq!(path, "path/to/data");

        // Test error cases
        let result = S3PrefixSource::extract_prefix_path_from_url("/local/path");
        assert!(result.is_err());

        let result = S3PrefixSource::extract_prefix_path_from_url("s3://bucket");
        assert!(result.is_err()); // No path after bucket
    }
}
