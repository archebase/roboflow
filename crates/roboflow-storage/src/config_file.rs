// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Configuration file support for storage credentials.
//!
//! Loads default credentials from `~/.roboflow/config.toml`.

use serde::Deserialize;
use std::path::PathBuf;

/// Default configuration file path.
const DEFAULT_CONFIG_PATH: &str = ".roboflow/config.toml";

/// Roboflow configuration file structure.
#[derive(Debug, Clone, Deserialize)]
pub struct RoboflowConfig {
    /// OSS credential configuration.
    #[serde(default)]
    pub oss: Option<OssConfigSection>,
}

/// OSS credential section in config file.
#[derive(Debug, Clone, Deserialize)]
pub struct OssConfigSection {
    /// OSS access key ID.
    pub access_key_id: Option<String>,
    /// OSS access key secret.
    pub access_key_secret: Option<String>,
    /// OSS endpoint (e.g., oss-cn-hangzhou.aliyuncs.com).
    pub endpoint: Option<String>,
    /// OSS region.
    pub region: Option<String>,
}

impl RoboflowConfig {
    /// Load configuration from the default path (`~/.roboflow/config.toml`).
    ///
    /// Returns `Ok(None)` if the file doesn't exist.
    pub fn load_default() -> Result<Option<Self>, ConfigError> {
        let home = std::env::var("HOME")
            .or_else(|_| std::env::var("USERPROFILE"))
            .map_err(|_| ConfigError::HomeDirNotFound)?;

        let config_path = PathBuf::from(home).join(DEFAULT_CONFIG_PATH);
        Self::load_from(&config_path)
    }

    /// Load configuration from a specific path.
    ///
    /// Returns `Ok(None)` if the file doesn't exist.
    pub fn load_from(path: &PathBuf) -> Result<Option<Self>, ConfigError> {
        if !path.exists() {
            return Ok(None);
        }

        let contents =
            std::fs::read_to_string(path).map_err(|e| ConfigError::ReadError(path.clone(), e))?;

        // Check file permissions (warn if readable by others)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(metadata) = std::fs::metadata(path) {
                let permissions = metadata.permissions().mode();
                if permissions & 0o044 != 0 {
                    eprintln!(
                        "Warning: Config file {} is world-readable. Consider: chmod 600 {}",
                        path.display(),
                        path.display()
                    );
                }
            }
        }

        let config: RoboflowConfig =
            toml::from_str(&contents).map_err(|e| ConfigError::ParseError(path.clone(), e))?;

        Ok(Some(config))
    }

    /// Get OSS access key ID from config.
    pub fn oss_access_key_id(&self) -> Option<&str> {
        self.oss.as_ref()?.access_key_id.as_deref()
    }

    /// Get OSS access key secret from config.
    pub fn oss_access_key_secret(&self) -> Option<&str> {
        self.oss.as_ref()?.access_key_secret.as_deref()
    }

    /// Get OSS endpoint from config.
    pub fn oss_endpoint(&self) -> Option<&str> {
        self.oss.as_ref()?.endpoint.as_deref()
    }

    /// Get OSS region from config.
    pub fn oss_region(&self) -> Option<&str> {
        self.oss.as_ref()?.region.as_deref()
    }
}

/// Configuration file error type.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// Home directory not found.
    #[error("HOME directory not found")]
    HomeDirNotFound,

    /// Error reading config file.
    #[error("Failed to read config file {0}: {1}")]
    ReadError(PathBuf, std::io::Error),

    /// Error parsing config file.
    #[error("Failed to parse config file {0}: {1}")]
    ParseError(PathBuf, toml::de::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_nonexistent_config() {
        let result = RoboflowConfig::load_from(&PathBuf::from("/nonexistent/config.toml"));
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }
}
