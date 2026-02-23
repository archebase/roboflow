// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Factory for creating LeRobot writers.

use crate::formats::lerobot::LerobotConfig;
use crate::formats::lerobot::writer::LerobotWriter;
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
    /// Storage backend
    pub storage: std::sync::Arc<dyn roboflow_storage::Storage>,
    /// Output prefix (key within bucket for S3, or path for local)
    pub output_prefix: String,
    /// Local buffer path (for cloud storage)
    pub local_buffer: Option<PathBuf>,
}

/// Create a LeRobot writer from configuration.
pub fn create_lerobot_writer(config: &LerobotWriterConfig) -> Result<LerobotWriterResult, String> {
    let output_path = &config.output_path;

    // Reject malformed cloud URLs
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

#[allow(deprecated)]
fn create_cloud_writer(config: &LerobotWriterConfig) -> Result<LerobotWriterResult, String> {
    let output_path = &config.output_path;

    let storage = roboflow_storage::StorageFactory::from_env()
        .create(output_path)
        .map_err(|e| format!("Failed to create storage for {}: {}", output_path, e))?;

    let output_prefix = StorageUrl::from_str(output_path)
        .map(|u| u.path().trim_end_matches('/').to_string())
        .unwrap_or_default();

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

fn create_local_writer(config: &LerobotWriterConfig) -> Result<LerobotWriterResult, String> {
    let output_path = &config.output_path;

    tracing::info!(
        output_path = %output_path,
        "Creating local LeRobot writer"
    );

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

// ============================================================================
// FormatFactory Implementation for Registry
// ============================================================================

use crate::core::traits::{FormatContext, FormatFactory, FormatWriter};

/// Factory for creating LeRobot writers via the registry system.
///
/// This implements the [`FormatFactory`] trait, allowing LeRobot format
/// to be registered and created dynamically.
pub struct LerobotFactory;

impl FormatFactory for LerobotFactory {
    fn format_name(&self) -> &'static str {
        "lerobot"
    }

    fn description(&self) -> &'static str {
        "LeRobot v2.1 dataset format - Hugging Face's robotics learning format"
    }

    fn create_writer(
        &self,
        config: &serde_json::Value,
        context: &FormatContext,
    ) -> roboflow_core::Result<Box<dyn FormatWriter>> {
        // Deserialize LerobotConfig from JSON
        let lerobot_config: LerobotConfig =
            serde_json::from_value(config.clone()).map_err(|e| {
                roboflow_core::RoboflowError::other(format!(
                    "Failed to parse LeRobot config: {}",
                    e
                ))
            })?;

        // Create the writer config
        let writer_config = LerobotWriterConfig::new(&context.output_url, lerobot_config);

        // Create the writer using the existing factory function
        let result = create_lerobot_writer(&writer_config).map_err(|e| {
            roboflow_core::RoboflowError::other(format!("Failed to create LeRobot writer: {}", e))
        })?;

        Ok(Box::new(result.writer))
    }

    fn is_available(&self) -> bool {
        // LeRobot is always available (no optional dependencies)
        true
    }
}

/// Get the LeRobot factory instance for registration.
pub fn lerobot_factory() -> &'static LerobotFactory {
    static FACTORY: LerobotFactory = LerobotFactory;
    &FACTORY
}

/// Register the LeRobot format with the global registry.
///
/// This should be called during crate initialization.
pub fn register_lerobot_format() {
    crate::core::registry::register_format(crate::core::registry::FormatDescriptor {
        name: "lerobot",
        description: "LeRobot v2.1 dataset format - Hugging Face's robotics learning format",
        file_extension: "parquet",
        feature_flag: None,
        factory: |config, context| {
            let factory = LerobotFactory;
            factory.create_writer(config, context)
        },
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> LerobotConfig {
        LerobotConfig {
            dataset: crate::formats::lerobot::DatasetConfig {
                base: crate::formats::common::DatasetBaseConfig {
                    name: "test".to_string(),
                    fps: 30,
                    robot_type: None,
                },
                env_type: None,
            },
            mappings: Vec::new(),
            video: Default::default(),
            annotation_file: None,
            flushing: crate::formats::lerobot::FlushingConfig::default(),
            streaming: crate::formats::lerobot::config::StreamingConfig::default(),
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
        assert!(result.is_ok());
    }

    #[test]
    fn test_lerobot_factory() {
        let factory = LerobotFactory;
        assert_eq!(factory.format_name(), "lerobot");
        assert!(factory.is_available());
        assert!(factory.description().contains("LeRobot"));
    }

    #[test]
    fn test_factory_create_writer() {
        let factory = LerobotFactory;
        let temp_dir = std::env::temp_dir().join(format!("test_factory_{}", std::process::id()));

        let config = test_config();
        let json_config = serde_json::to_value(&config).unwrap();

        let context = FormatContext {
            output_url: temp_dir.to_string_lossy().to_string(),
            storage: None,
            base_path: temp_dir.clone(),
            num_workers: 1,
        };

        let writer = factory.create_writer(&json_config, &context);
        assert!(writer.is_ok());
    }
}
