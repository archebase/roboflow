// Sink configuration types

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for creating a sink.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkConfig {
    /// Type of sink
    #[serde(flatten)]
    pub sink_type: SinkType,
    /// Additional options
    #[serde(default)]
    pub options: HashMap<String, serde_json::Value>,
}

impl SinkConfig {
    /// Create a LeRobot sink configuration.
    pub fn lerobot(path: impl Into<String>) -> Self {
        Self {
            sink_type: SinkType::Lerobot { path: path.into() },
            options: HashMap::new(),
        }
    }

    /// Create a LeRobot sink configuration with a custom LeRobot config.
    ///
    /// The config is serialized and stored in the options for later retrieval.
    pub fn lerobot_with_config(
        path: impl Into<String>,
        config: &roboflow_dataset::lerobot::LerobotConfig,
    ) -> Self {
        let mut options = HashMap::new();
        if let Ok(config_json) = serde_json::to_value(config) {
            options.insert("lerobot_config".to_string(), config_json);
        }
        Self {
            sink_type: SinkType::Lerobot { path: path.into() },
            options,
        }
    }

    /// Create a KPS sink configuration.
    pub fn kps(path: impl Into<String>) -> Self {
        Self {
            sink_type: SinkType::Kps { path: path.into() },
            options: HashMap::new(),
        }
    }

    /// Create a Zarr sink configuration.
    pub fn zarr(path: impl Into<String>) -> Self {
        Self {
            sink_type: SinkType::Zarr { path: path.into() },
            options: HashMap::new(),
        }
    }

    /// Get the path for this sink.
    pub fn path(&self) -> &str {
        match &self.sink_type {
            SinkType::Lerobot { path } => path,
            SinkType::Kps { path } => path,
            SinkType::Zarr { path } => path,
        }
    }

    /// Add an option to the configuration.
    pub fn with_option(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.options.insert(key.into(), value);
        self
    }

    /// Get an option value.
    pub fn get_option<T>(&self, key: &str) -> Option<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        self.options
            .get(key)
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }
}

/// The type of sink.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SinkType {
    /// LeRobot dataset format
    Lerobot {
        /// Path to the output directory
        path: String,
    },
    /// KPS dataset format
    Kps {
        /// Path to the output directory
        path: String,
    },
    /// Zarr dataset format
    Zarr {
        /// Path to the output directory
        path: String,
    },
}

impl SinkType {
    /// Get the name of this sink type.
    pub fn name(&self) -> &str {
        match self {
            Self::Lerobot { .. } => "lerobot",
            Self::Kps { .. } => "kps",
            Self::Zarr { .. } => "zarr",
        }
    }

    /// Get the path for this sink type.
    pub fn path(&self) -> &str {
        match self {
            Self::Lerobot { path } => path,
            Self::Kps { path } => path,
            Self::Zarr { path } => path,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sink_config_lerobot() {
        let config =
            SinkConfig::lerobot("/path/to/output").with_option("fps", serde_json::json!(30));

        assert_eq!(config.path(), "/path/to/output");
        assert_eq!(config.get_option::<u32>("fps"), Some(30));
        assert_eq!(config.get_option::<u32>("invalid"), None);
    }

    #[test]
    fn test_sink_config_kps() {
        let config = SinkConfig::kps("/path/to/output");

        assert_eq!(config.path(), "/path/to/output");
    }

    #[test]
    fn test_sink_type_name() {
        assert_eq!(
            SinkType::Lerobot {
                path: "test".to_string()
            }
            .name(),
            "lerobot"
        );
        assert_eq!(
            SinkType::Kps {
                path: "test".to_string()
            }
            .name(),
            "kps"
        );
        assert_eq!(
            SinkType::Zarr {
                path: "test".to_string()
            }
            .name(),
            "zarr"
        );
    }
}
