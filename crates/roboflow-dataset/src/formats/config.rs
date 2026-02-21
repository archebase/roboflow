// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Output format configuration types.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for dataset output format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    /// Type of output format
    #[serde(flatten)]
    pub format: OutputFormat,
    /// Additional options
    #[serde(default)]
    pub options: HashMap<String, serde_json::Value>,
}

impl OutputConfig {
    /// Create a LeRobot output configuration.
    pub fn lerobot(path: impl Into<String>) -> Self {
        Self {
            format: OutputFormat::Lerobot { path: path.into() },
            options: HashMap::new(),
        }
    }

    /// Create a LeRobot output configuration with custom config.
    pub fn lerobot_with_config(
        path: impl Into<String>,
        config: &crate::formats::lerobot::LerobotConfig,
    ) -> Self {
        let mut options = HashMap::new();
        if let Ok(config_json) = serde_json::to_value(config) {
            options.insert("lerobot_config".to_string(), config_json);
        }
        Self {
            format: OutputFormat::Lerobot { path: path.into() },
            options,
        }
    }

    /// Get the output path.
    pub fn path(&self) -> &str {
        match &self.format {
            OutputFormat::Lerobot { path } => path,
        }
    }

    /// Add an option.
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

/// Output format type.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum OutputFormat {
    /// LeRobot dataset format
    Lerobot {
        /// Path to output directory
        path: String,
    },
}

impl OutputFormat {
    /// Get the format name.
    pub fn name(&self) -> &str {
        match self {
            Self::Lerobot { .. } => "lerobot",
        }
    }

    /// Get the output path.
    pub fn path(&self) -> &str {
        match self {
            Self::Lerobot { path } => path,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_output_config_lerobot() {
        let config =
            OutputConfig::lerobot("/path/to/output").with_option("fps", serde_json::json!(30));

        assert_eq!(config.path(), "/path/to/output");
        assert_eq!(config.get_option::<u32>("fps"), Some(30));
        assert_eq!(config.get_option::<u32>("invalid"), None);
    }

    #[test]
    fn test_output_format_name() {
        assert_eq!(
            OutputFormat::Lerobot {
                path: "test".to_string()
            }
            .name(),
            "lerobot"
        );
    }
}
