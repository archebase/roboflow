// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Finalizer configuration.

use std::time::Duration;
use tracing::warn;

/// Dataset configuration for metadata generation.
#[derive(Debug, Clone)]
pub struct DatasetMetadataConfig {
    /// Dataset name.
    pub name: String,

    /// Robot type (e.g., "panda", "ur5").
    pub robot_type: Option<String>,

    /// Frame rate (fps).
    pub fps: u32,
}

impl DatasetMetadataConfig {
    /// Create a new dataset metadata configuration.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            robot_type: None,
            fps: 30,
        }
    }

    /// Set the robot type.
    pub fn with_robot_type(mut self, robot_type: impl Into<String>) -> Self {
        self.robot_type = Some(robot_type.into());
        self
    }

    /// Set the frame rate.
    pub fn with_fps(mut self, fps: u32) -> Self {
        self.fps = fps;
        self
    }
}

/// Default poll interval for checking completed batches (seconds).
pub const DEFAULT_POLL_INTERVAL_SECS: u64 = 30;

/// Default merge operation timeout (seconds).
pub const DEFAULT_MERGE_TIMEOUT_SECS: u64 = 600;

/// Finalizer configuration.
#[derive(Debug, Clone)]
pub struct FinalizerConfig {
    /// Poll interval for checking completed batches.
    pub poll_interval: Duration,

    /// Merge operation timeout.
    pub merge_timeout: Duration,

    /// Dataset metadata configuration.
    pub dataset_config: Option<DatasetMetadataConfig>,
}

impl Default for FinalizerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(DEFAULT_POLL_INTERVAL_SECS),
            merge_timeout: Duration::from_secs(DEFAULT_MERGE_TIMEOUT_SECS),
            dataset_config: None,
        }
    }
}

impl FinalizerConfig {
    /// Create a new finalizer configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create from environment variables.
    ///
    /// - `FINALIZER_POLL_INTERVAL_SECS`: Poll interval (default: 30)
    /// - `FINALIZER_MERGE_TIMEOUT_SECS`: Merge timeout (default: 600)
    pub fn from_env() -> Result<Self, String> {
        let poll_interval = match std::env::var("FINALIZER_POLL_INTERVAL_SECS") {
            Ok(ref s) => match s.parse::<u64>() {
                Ok(val) => val,
                Err(_) => {
                    warn!(
                        env_var = "FINALIZER_POLL_INTERVAL_SECS",
                        provided = s,
                        default = DEFAULT_POLL_INTERVAL_SECS,
                        "Invalid value for FINALIZER_POLL_INTERVAL_SECS, using default"
                    );
                    DEFAULT_POLL_INTERVAL_SECS
                }
            },
            Err(_) => DEFAULT_POLL_INTERVAL_SECS,
        };

        let merge_timeout = match std::env::var("FINALIZER_MERGE_TIMEOUT_SECS") {
            Ok(ref s) => match s.parse::<u64>() {
                Ok(val) => val,
                Err(_) => {
                    warn!(
                        env_var = "FINALIZER_MERGE_TIMEOUT_SECS",
                        provided = s,
                        default = DEFAULT_MERGE_TIMEOUT_SECS,
                        "Invalid value for FINALIZER_MERGE_TIMEOUT_SECS, using default"
                    );
                    DEFAULT_MERGE_TIMEOUT_SECS
                }
            },
            Err(_) => DEFAULT_MERGE_TIMEOUT_SECS,
        };

        Ok(Self {
            poll_interval: Duration::from_secs(poll_interval),
            merge_timeout: Duration::from_secs(merge_timeout),
            dataset_config: None,
        })
    }

    /// Set the dataset metadata configuration.
    pub fn with_dataset_config(mut self, config: DatasetMetadataConfig) -> Self {
        self.dataset_config = Some(config);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dataset_metadata_config() {
        let config = DatasetMetadataConfig::new("test_dataset")
            .with_robot_type("panda")
            .with_fps(60);

        assert_eq!(config.name, "test_dataset");
        assert_eq!(config.robot_type, Some("panda".to_string()));
        assert_eq!(config.fps, 60);
    }

    #[test]
    fn test_dataset_metadata_config_defaults() {
        let config = DatasetMetadataConfig::new("test_dataset");

        assert_eq!(config.name, "test_dataset");
        assert_eq!(config.robot_type, None);
        assert_eq!(config.fps, 30);
    }

    #[test]
    fn test_finalizer_config_default() {
        let config = FinalizerConfig::default();

        assert_eq!(config.poll_interval, Duration::from_secs(30));
        assert_eq!(config.merge_timeout, Duration::from_secs(600));
        assert!(config.dataset_config.is_none());
    }

    #[test]
    fn test_finalizer_config_new() {
        let config = FinalizerConfig::new();

        assert_eq!(config.poll_interval, Duration::from_secs(30));
        assert_eq!(config.merge_timeout, Duration::from_secs(600));
    }

    #[test]
    fn test_finalizer_config_with_dataset() {
        let dataset_config = DatasetMetadataConfig::new("my_dataset")
            .with_robot_type("ur5")
            .with_fps(30);

        let config = FinalizerConfig::new().with_dataset_config(dataset_config);

        assert!(config.dataset_config.is_some());
        let ds = config.dataset_config.unwrap();
        assert_eq!(ds.name, "my_dataset");
        assert_eq!(ds.robot_type, Some("ur5".to_string()));
    }
}
