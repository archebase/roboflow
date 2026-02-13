// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Builder for creating [`LerobotWriter`] instances.
//!
//! This module provides a fluent builder API for configuring and creating
//! LeRobot dataset writers.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use roboflow_core::{Result, RoboflowError};

use super::writer_impl::LerobotWriter;
use crate::lerobot::config::LerobotConfig;

/// Builder for creating [`LerobotWriter`] instances.
///
/// # Example
///
/// ```ignore
/// use roboflow::dataset::lerobot::{LerobotWriter, LerobotConfig};
///
/// let config = LerobotConfig::default();
/// let writer = LerobotWriter::builder()
///     .output_dir("/output")
///     .config(config)
///     .build()?;
/// ```
pub struct LerobotWriterBuilder {
    output_dir: Option<PathBuf>,
    storage: Option<Arc<dyn roboflow_storage::Storage>>,
    output_prefix: Option<String>,
    local_buffer: Option<PathBuf>,
    config: Option<LerobotConfig>,
}

impl Default for LerobotWriterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl LerobotWriterBuilder {
    /// Create a new builder with default settings.
    pub fn new() -> Self {
        Self {
            output_dir: None,
            storage: None,
            output_prefix: None,
            local_buffer: None,
            config: None,
        }
    }

    /// Set the output directory for local filesystem output.
    ///
    /// When this is set without `storage`, the writer uses LocalStorage.
    pub fn output_dir(mut self, path: impl AsRef<Path>) -> Self {
        self.output_dir = Some(path.as_ref().to_path_buf());
        self
    }

    /// Set the storage backend for cloud storage support.
    ///
    /// When set, the writer will use this storage backend for writing data.
    /// The `output_dir` is still used as a local buffer for temporary files.
    pub fn storage(mut self, storage: Arc<dyn roboflow_storage::Storage>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Set the output prefix within storage.
    ///
    /// This is used as a prefix for all files written to cloud storage.
    /// For example, "datasets/my_dataset" would result in files at
    /// "datasets/my_dataset/data/chunk-000/...".
    pub fn output_prefix(mut self, prefix: String) -> Self {
        self.output_prefix = Some(prefix);
        self
    }

    /// Set the local buffer directory for temporary files.
    ///
    /// This is where Parquet files and videos are created before being
    /// uploaded to cloud storage (if a storage backend is configured).
    pub fn local_buffer(mut self, path: impl AsRef<Path>) -> Self {
        self.local_buffer = Some(path.as_ref().to_path_buf());
        self
    }

    /// Set the LeRobot configuration.
    pub fn config(mut self, config: LerobotConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Build the writer.
    ///
    /// # Errors
    ///
    /// Returns an error if required fields are not set.
    pub fn build(self) -> Result<LerobotWriter> {
        let config = self.config.ok_or_else(|| {
            RoboflowError::required("LerobotWriterBuilder", "config")
        })?;

        // Determine if we're using cloud storage
        let use_cloud_storage = self.storage.is_some();

        let (storage, output_prefix, local_buffer, _output_dir) = if let Some(storage) =
            self.storage
        {
            let local_buffer = self.local_buffer.ok_or_else(|| {
                RoboflowError::required("LerobotWriterBuilder", "local_buffer")
            })?;
            let output_dir = local_buffer.clone();
            let output_prefix = self.output_prefix.unwrap_or_default();
            (storage, output_prefix, local_buffer, output_dir)
        } else {
            // Local storage mode
            let output_dir = self.output_dir.ok_or_else(|| {
                RoboflowError::required("LerobotWriterBuilder", "output_dir")
            })?;

            // Validate output_dir is not a cloud storage URL
            validate_not_cloud_url(&output_dir)?;

            let storage = Arc::new(roboflow_storage::LocalStorage::new(&output_dir)) as _;
            let local_buffer = output_dir.clone();
            let output_prefix = self.output_prefix.unwrap_or_default();
            (storage, output_prefix, local_buffer, output_dir)
        };

        LerobotWriter::new_internal(
            storage,
            output_prefix,
            local_buffer,
            config,
            use_cloud_storage,
        )
    }
}

/// Validate that the output directory is not a cloud storage URL.
fn validate_not_cloud_url(output_dir: &Path) -> Result<()> {
    let output_dir_str = output_dir.as_os_str().to_string_lossy();

    // Check lowercase
    let lower = output_dir_str.to_lowercase();
    if lower.starts_with("s3://") || lower.starts_with("oss://") {
        return Err(RoboflowError::config(
            "LerobotWriterBuilder",
            format!(
                "output_dir cannot be a cloud storage URL ({}). \
                 Use storage() method with local_buffer() instead. \
                 Example: let storage = StorageFactory::new().create(\"{}\")?; \
                 LerobotWriter::builder().storage(storage).output_prefix(\"datasets\") \
                 .local_buffer(\"/tmp/roboflow_buffer\").config(config)",
                output_dir_str, output_dir_str
            ),
        ));
    }

    // Check case-sensitive variants
    if output_dir_str.starts_with("s3://")
        || output_dir_str.starts_with("oss://")
        || output_dir_str.starts_with("S3://")
        || output_dir_str.starts_with("OSS://")
    {
        return Err(RoboflowError::config(
            "LerobotWriterBuilder",
            format!(
                "output_dir appears to be a cloud storage URL ('{}'). \
                 For cloud storage, use the storage() method with StorageFactory instead. \
                 Example: let storage = StorageFactory::new().create(\"{}\")?; \
                 LerobotWriter::builder().storage(storage).output_prefix(\"datasets\") \
                 .local_buffer(\"/tmp/roboflow_buffer\").config(config)",
                output_dir_str, output_dir_str
            ),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_new() {
        let builder = LerobotWriterBuilder::new();
        assert!(builder.output_dir.is_none());
        assert!(builder.storage.is_none());
        assert!(builder.config.is_none());
    }

    #[test]
    fn test_builder_requires_config() {
        let result = LerobotWriterBuilder::new()
            .output_dir("/tmp/test")
            .build();

        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("config"));
        }
    }

    #[test]
    fn test_builder_requires_output_dir_for_local() {
        let config = LerobotConfig::from_toml(
            r#"[dataset]
name = "test"
fps = 30
"#,
        )
        .unwrap();
        let result = LerobotWriterBuilder::new().config(config).build();

        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("output_dir"));
        }
    }

    #[test]
    fn test_builder_requires_local_buffer_for_cloud() {
        let storage = Arc::new(roboflow_storage::mock::MockStorage::new());
        let config = LerobotConfig::from_toml(
            r#"[dataset]
name = "test"
fps = 30
"#,
        )
        .unwrap();

        let result = LerobotWriterBuilder::new()
            .storage(storage)
            .config(config)
            .build();

        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("local_buffer"));
        }
    }

    #[test]
    fn test_validate_rejects_s3_url() {
        let result = validate_not_cloud_url(Path::new("s3://bucket/path"));
        assert!(result.is_err());

        let result = validate_not_cloud_url(Path::new("S3://bucket/path"));
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_rejects_oss_url() {
        let result = validate_not_cloud_url(Path::new("oss://bucket/path"));
        assert!(result.is_err());

        let result = validate_not_cloud_url(Path::new("OSS://bucket/path"));
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_accepts_local_path() {
        let result = validate_not_cloud_url(Path::new("/tmp/output"));
        assert!(result.is_ok());

        let result = validate_not_cloud_url(Path::new("./relative/path"));
        assert!(result.is_ok());
    }
}
