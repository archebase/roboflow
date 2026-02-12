// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Factory for creating LeRobot writers.
//!
//! This module consolidates the LeRobot writer creation logic in one place,
//! ensuring consistent behavior between `LerobotSink` and `TaskExecutor`.
//!
//! # Example
//!
//! ```rust,ignore
//! use roboflow_sinks::lerobot_factory::{create_lerobot_writer, LerobotWriterConfig};
//! use roboflow_dataset::lerobot::LerobotConfig;
//!
//! let config = LerobotWriterConfig {
//!     output_path: "s3://bucket/output/".to_string(),
//!     lerobot_config,
//! };
//!
//! let writer = create_lerobot_writer(&config)?;
//! ```

use roboflow_dataset::lerobot::LerobotConfig;
use roboflow_dataset::lerobot::writer::LerobotWriter;
use roboflow_storage::StorageUrl;
use std::path::PathBuf;
use std::str::FromStr;

/// Configuration for creating a LeRobot writer.
#[derive(Debug, Clone)]
pub struct LerobotWriterConfig {
    /// Output path (local path or s3://bucket/path or oss://bucket/path)
    pub output_path: String,
    /// LeRobot dataset configuration
    pub lerobot_config: LerobotConfig,
}

impl LerobotWriterConfig {
    /// Create a new configuration.
    pub fn new(output_path: impl Into<String>, lerobot_config: LerobotConfig) -> Self {
        Self {
            output_path: output_path.into(),
            lerobot_config,
        }
    }
}

/// Result of creating a LeRobot writer.
pub struct LerobotWriterResult {
    /// The created writer
    pub writer: LerobotWriter,
    /// Storage backend (for reference)
    pub storage: std::sync::Arc<dyn roboflow_storage::Storage>,
    /// Output prefix (key within bucket for S3, or path for local)
    pub output_prefix: String,
    /// Local buffer path (for cloud storage)
    pub local_buffer: Option<PathBuf>,
}

/// Create a LeRobot writer from configuration.
///
/// This function handles:
/// - Cloud URL validation (s3://, oss://)
/// - Storage backend creation
/// - Output prefix extraction
/// - Local buffer setup for cloud storage
///
/// # Errors
///
/// Returns an error if:
/// - The URL scheme is malformed (e.g., "s3:" instead of "s3://")
/// - Storage creation fails
/// - Directory creation fails
pub fn create_lerobot_writer(config: &LerobotWriterConfig) -> Result<LerobotWriterResult, String> {
    let output_path = &config.output_path;

    // Reject malformed cloud URLs (e.g. "s3:" or "s3:/bucket" missing "//")
    if (output_path.starts_with("s3:") && !output_path.starts_with("s3://"))
        || (output_path.starts_with("oss:") && !output_path.starts_with("oss://"))
    {
        return Err(format!(
            "Malformed cloud URL '{}': use s3://bucket/path or oss://bucket/path (double slash required)",
            output_path
        ));
    }

    if output_path.starts_with("s3://") || output_path.starts_with("oss://") {
        create_cloud_writer(config)
    } else {
        create_local_writer(config)
    }
}

/// Create a writer for cloud storage (S3/OSS).
fn create_cloud_writer(config: &LerobotWriterConfig) -> Result<LerobotWriterResult, String> {
    let output_path = &config.output_path;

    // Create storage backend
    let storage = roboflow_storage::StorageFactory::from_env()
        .create(output_path)
        .map_err(|e| format!("Failed to create storage for {}: {}", output_path, e))?;

    // Extract the key (path within bucket) as output_prefix
    let output_prefix = StorageUrl::from_str(output_path)
        .map(|u| u.path().trim_end_matches('/').to_string())
        .unwrap_or_default();

    // Create unique local buffer directory
    let local_buffer = std::env::temp_dir().join("roboflow").join(format!(
        "{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or(std::time::Duration::ZERO)
            .as_nanos()
    ));
    std::fs::create_dir_all(&local_buffer).map_err(|e| {
        format!(
            "Failed to create local buffer {}: {}",
            local_buffer.display(),
            e
        )
    })?;

    tracing::info!(
        output_path = %output_path,
        output_prefix = %output_prefix,
        local_buffer = %local_buffer.display(),
        "Creating cloud-based LeRobot writer"
    );

    let writer = LerobotWriter::new(
        std::sync::Arc::clone(&storage),
        output_prefix.clone(),
        &local_buffer,
        config.lerobot_config.clone(),
    )
    .map_err(|e| format!("Failed to create LeRobot writer: {}", e))?;

    Ok(LerobotWriterResult {
        writer,
        storage,
        output_prefix,
        local_buffer: Some(local_buffer),
    })
}

/// Create a writer for local storage.
fn create_local_writer(config: &LerobotWriterConfig) -> Result<LerobotWriterResult, String> {
    let output_path = &config.output_path;

    tracing::info!(
        output_path = %output_path,
        "Creating local LeRobot writer"
    );

    // For local storage, create a simple LocalStorage wrapper
    let storage: std::sync::Arc<dyn roboflow_storage::Storage> =
        std::sync::Arc::new(roboflow_storage::LocalStorage::new(
            std::path::PathBuf::from(output_path)
                .parent()
                .unwrap_or(std::path::Path::new("")),
        ));

    let writer = LerobotWriter::new_local(output_path, config.lerobot_config.clone())
        .map_err(|e| format!("Failed to create local LeRobot writer: {}", e))?;

    Ok(LerobotWriterResult {
        writer,
        storage,
        output_prefix: output_path.clone(),
        local_buffer: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> LerobotConfig {
        LerobotConfig {
            dataset: roboflow_dataset::lerobot::DatasetConfig {
                base: roboflow_dataset::common::DatasetBaseConfig {
                    name: "test".to_string(),
                    fps: 30,
                    robot_type: None,
                },
                env_type: None,
            },
            mappings: Vec::new(),
            video: Default::default(),
            annotation_file: None,
            flushing: roboflow_dataset::lerobot::FlushingConfig::default(),
            streaming: roboflow_dataset::lerobot::config::StreamingConfig::default(),
        }
    }

    #[test]
    fn test_malformed_s3_url() {
        let config = LerobotWriterConfig::new("s3:bucket/path", test_config());
        let result = create_lerobot_writer(&config);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.contains("Malformed cloud URL"));
        }
    }

    #[test]
    fn test_malformed_oss_url() {
        let config = LerobotWriterConfig::new("oss:/bucket/path", test_config());
        let result = create_lerobot_writer(&config);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.contains("Malformed cloud URL"));
        }
    }

    #[test]
    fn test_local_writer_creation() {
        let temp_dir = std::env::temp_dir().join(format!("test_lerobot_{}", std::process::id()));
        let config =
            LerobotWriterConfig::new(temp_dir.to_string_lossy().to_string(), test_config());
        let result = create_lerobot_writer(&config);
        // This should succeed (creating a local writer)
        assert!(result.is_ok());
    }
}
