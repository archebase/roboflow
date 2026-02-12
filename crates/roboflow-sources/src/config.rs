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

    /// Create an S3 prefix source configuration.
    pub fn s3_prefix(url: impl Into<String>) -> Self {
        Self {
            source_type: SourceType::S3Prefix { url: url.into() },
            options: HashMap::new(),
        }
    }

    /// Get the path for this source.
    ///
    /// For S3Prefix sources, returns the URL.
    pub fn path(&self) -> &str {
        match &self.source_type {
            SourceType::Mcap { path } => path,
            SourceType::Bag { path } => path,
            SourceType::Rrd { path } => path,
            SourceType::S3Prefix { url } => url,
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

    /// Create a source configuration from a URL/path.
    ///
    /// Auto-detects the source type based on:
    /// - URL scheme (s3://, oss://)
    /// - File extension (.mcap, .bag, .rrd)
    ///
    /// # Examples
    ///
    /// ```
    /// use roboflow_sources::SourceConfig;
    ///
    /// // Local files
    /// let config = SourceConfig::from_url("/path/to/data.mcap");
    /// assert_eq!(config.source_type.name(), "mcap");
    ///
    /// // S3 files
    /// let config = SourceConfig::from_url("s3://bucket/data/file.bag");
    /// assert_eq!(config.source_type.name(), "bag");
    ///
    /// // S3 prefix (no extension)
    /// let config = SourceConfig::from_url("s3://bucket/data/prefix/");
    /// assert_eq!(config.source_type.name(), "s3-prefix");
    /// ```
    pub fn from_url(url: impl AsRef<str>) -> Self {
        let url = url.as_ref();

        // Check for cloud URLs
        let is_cloud = url.starts_with("s3://") || url.starts_with("oss://");
        let url_lower = url.to_lowercase();

        // Check for specific file extensions
        if url_lower.ends_with(".mcap") {
            Self::mcap(url)
        } else if url_lower.ends_with(".bag") {
            Self::bag(url)
        } else if url_lower.ends_with(".rrd") {
            Self::rrd(url)
        } else if is_cloud {
            // Cloud URL without specific extension - treat as prefix
            Self::s3_prefix(url)
        } else {
            // Default to MCAP for local files
            Self::mcap(url)
        }
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
    /// S3/OSS prefix containing multiple files
    S3Prefix {
        /// S3/OSS URL prefix (e.g., "s3://bucket/path/to/data/")
        url: String,
    },
}

impl SourceType {
    /// Get the name of this source type.
    pub fn name(&self) -> &str {
        match self {
            Self::Mcap { .. } => "mcap",
            Self::Bag { .. } => "bag",
            Self::Rrd { .. } => "rrd",
            Self::S3Prefix { .. } => "s3-prefix",
        }
    }

    /// Get the path for this source type.
    ///
    /// For S3Prefix, returns the URL.
    pub fn path(&self) -> &str {
        match self {
            Self::Mcap { path } => path,
            Self::Bag { path } => path,
            Self::Rrd { path } => path,
            Self::S3Prefix { url } => url,
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
        assert_eq!(
            SourceType::S3Prefix {
                url: "s3://bucket/prefix/".to_string()
            }
            .name(),
            "s3-prefix"
        );
    }

    #[test]
    fn test_source_config_s3_prefix() {
        let config = SourceConfig::s3_prefix("s3://bucket/data/episode_001/");

        assert_eq!(config.path(), "s3://bucket/data/episode_001/");
        assert_eq!(config.source_type.name(), "s3-prefix");
    }

    #[test]
    fn test_from_url_local_files() {
        let config = SourceConfig::from_url("/path/to/file.mcap");
        assert_eq!(config.source_type.name(), "mcap");

        let config = SourceConfig::from_url("/path/to/file.bag");
        assert_eq!(config.source_type.name(), "bag");

        let config = SourceConfig::from_url("/path/to/file.rrd");
        assert_eq!(config.source_type.name(), "rrd");

        // Case insensitive
        let config = SourceConfig::from_url("/path/to/file.MCAP");
        assert_eq!(config.source_type.name(), "mcap");
    }

    #[test]
    fn test_from_url_cloud_files() {
        let config = SourceConfig::from_url("s3://bucket/path/file.mcap");
        assert_eq!(config.source_type.name(), "mcap");

        let config = SourceConfig::from_url("s3://bucket/path/prefix/");
        assert_eq!(config.source_type.name(), "s3-prefix");

        let config = SourceConfig::from_url("oss://bucket/path/file.bag");
        assert_eq!(config.source_type.name(), "bag");

        let config = SourceConfig::from_url("oss://bucket/path/prefix/");
        assert_eq!(config.source_type.name(), "s3-prefix");
    }

    #[test]
    fn test_from_url_default() {
        // Unknown extension defaults to MCAP
        let config = SourceConfig::from_url("/path/to/unknown");
        assert_eq!(config.source_type.name(), "mcap");
    }
}
