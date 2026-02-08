// Source configuration types

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for creating a source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    /// Type of source
    #[serde(flatten)]
    pub source_type: SourceType,
    /// Additional options
    #[serde(default)]
    pub options: HashMap<String, serde_json::Value>,
}

impl SourceConfig {
    /// Create an MCAP source configuration.
    pub fn mcap(path: impl Into<String>) -> Self {
        Self {
            source_type: SourceType::Mcap { path: path.into() },
            options: HashMap::new(),
        }
    }

    /// Create a ROS bag source configuration.
    pub fn bag(path: impl Into<String>) -> Self {
        Self {
            source_type: SourceType::Bag { path: path.into() },
            options: HashMap::new(),
        }
    }

    /// Create a Rerun Data (.rrd) source configuration.
    pub fn rrd(path: impl Into<String>) -> Self {
        Self {
            source_type: SourceType::Rrd { path: path.into() },
            options: HashMap::new(),
        }
    }

    /// Create an HDF5 source configuration.
    #[cfg(feature = "hdf5")]
    pub fn hdf5(path: impl Into<String>) -> Self {
        Self {
            source_type: SourceType::Hdf5 { path: path.into() },
            options: HashMap::new(),
        }
    }

    /// Get the path for this source.
    pub fn path(&self) -> &str {
        match &self.source_type {
            SourceType::Mcap { path } => path,
            SourceType::Bag { path } => path,
            SourceType::Rrd { path } => path,
            #[cfg(feature = "hdf5")]
            SourceType::Hdf5 { path } => path,
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

/// The type of source.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SourceType {
    /// MCAP file format
    Mcap {
        /// Path to the MCAP file
        path: String,
    },
    /// ROS1 bag file format
    Bag {
        /// Path to the bag file
        path: String,
    },
    /// Rerun Data (.rrd) file format
    Rrd {
        /// Path to the .rrd file
        path: String,
    },
    /// HDF5 file format (when feature is enabled)
    #[cfg(feature = "hdf5")]
    Hdf5 {
        /// Path to the HDF5 file
        path: String,
    },
}

impl SourceType {
    /// Get the name of this source type.
    pub fn name(&self) -> &str {
        match self {
            Self::Mcap { .. } => "mcap",
            Self::Bag { .. } => "bag",
            Self::Rrd { .. } => "rrd",
            #[cfg(feature = "hdf5")]
            Self::Hdf5 { .. } => "hdf5",
        }
    }

    /// Get the path for this source type.
    pub fn path(&self) -> &str {
        match self {
            Self::Mcap { path } => path,
            Self::Bag { path } => path,
            Self::Rrd { path } => path,
            #[cfg(feature = "hdf5")]
            Self::Hdf5 { path } => path,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_config_mcap() {
        let config = SourceConfig::mcap("/path/to/data.mcap")
            .with_option("batch_size", serde_json::json!(100));

        assert_eq!(config.path(), "/path/to/data.mcap");
        assert_eq!(config.get_option::<usize>("batch_size"), Some(100));
        assert_eq!(config.get_option::<usize>("invalid"), None);
    }

    #[test]
    fn test_source_config_bag() {
        let config = SourceConfig::bag("/path/to/data.bag");

        assert_eq!(config.path(), "/path/to/data.bag");
    }

    #[test]
    fn test_source_type_name() {
        assert_eq!(
            SourceType::Mcap {
                path: "test".to_string()
            }
            .name(),
            "mcap"
        );
        assert_eq!(
            SourceType::Bag {
                path: "test".to_string()
            }
            .name(),
            "bag"
        );
    }
}
