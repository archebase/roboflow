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
    /// S3 credential configuration.
    #[serde(default)]
    pub s3: Option<S3ConfigSection>,
}

/// S3 credential section in config file.
#[derive(Debug, Clone, Deserialize)]
pub struct S3ConfigSection {
    /// S3 access key ID.
    pub access_key_id: Option<String>,
    /// S3 access key secret.
    pub access_key_secret: Option<String>,
    /// S3 endpoint (e.g., s3.amazonaws.com, oss-cn-hangzhou.aliyuncs.com).
    pub endpoint: Option<String>,
    /// S3 region.
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
                // Warn if group or others can read (best practice: 0600 for config files)
                if permissions & 0o077 != 0 {
                    eprintln!(
                        "Warning: Config file {} has group/other permissions. Consider: chmod 600 {}",
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

    /// Get S3 access key ID from config.
    pub fn s3_access_key_id(&self) -> Option<&str> {
        self.s3.as_ref()?.access_key_id.as_deref()
    }

    /// Get S3 access key secret from config.
    pub fn s3_access_key_secret(&self) -> Option<&str> {
        self.s3.as_ref()?.access_key_secret.as_deref()
    }

    /// Get S3 endpoint from config.
    pub fn s3_endpoint(&self) -> Option<&str> {
        self.s3.as_ref()?.endpoint.as_deref()
    }

    /// Get S3 region from config.
    pub fn s3_region(&self) -> Option<&str> {
        self.s3.as_ref()?.region.as_deref()
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
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_load_nonexistent_config() {
        let result = RoboflowConfig::load_from(&PathBuf::from("/nonexistent/config.toml"));
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_load_valid_config() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let config_content = r#"
[s3]
access_key_id = "test_access_key"
access_key_secret = "test_secret"
endpoint = "https://s3.amazonaws.com"
region = "us-east-1"
"#;
        temp_file.write_all(config_content.as_bytes()).unwrap();

        let result = RoboflowConfig::load_from(&PathBuf::from(temp_file.path()));
        assert!(result.is_ok());
        let config = result.unwrap().expect("Config should be present");
        assert!(config.s3.is_some());

        let s3 = config.s3.as_ref().unwrap();
        assert_eq!(s3.access_key_id, Some("test_access_key".to_string()));
        assert_eq!(s3.access_key_secret, Some("test_secret".to_string()));
        assert_eq!(s3.endpoint, Some("https://s3.amazonaws.com".to_string()));
        assert_eq!(s3.region, Some("us-east-1".to_string()));
    }

    #[test]
    fn test_load_config_with_partial_s3() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let config_content = r#"
[s3]
access_key_id = "only_key"
"#;
        temp_file.write_all(config_content.as_bytes()).unwrap();

        let result = RoboflowConfig::load_from(&PathBuf::from(temp_file.path()));
        assert!(result.is_ok());
        let config = result.unwrap().expect("Config should be present");

        let s3 = config.s3.as_ref().unwrap();
        assert_eq!(s3.access_key_id, Some("only_key".to_string()));
        assert_eq!(s3.access_key_secret, None);
        assert_eq!(s3.endpoint, None);
        assert_eq!(s3.region, None);
    }

    #[test]
    fn test_load_config_without_s3_section() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let config_content = r#"
# Just a comment
"#;
        temp_file.write_all(config_content.as_bytes()).unwrap();

        let result = RoboflowConfig::load_from(&PathBuf::from(temp_file.path()));
        assert!(result.is_ok());
        let config = result.unwrap().expect("Config should be present");
        assert!(config.s3.is_none());
    }

    #[test]
    fn test_load_config_empty_file() {
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"").unwrap();

        let result = RoboflowConfig::load_from(&PathBuf::from(temp_file.path()));
        assert!(result.is_ok());
        let config = result.unwrap().expect("Config should be present");
        assert!(config.s3.is_none());
    }

    #[test]
    fn test_load_invalid_toml() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let invalid_content = r#"
[s3
access_key_id = "unclosed bracket
"#;
        temp_file.write_all(invalid_content.as_bytes()).unwrap();

        let result = RoboflowConfig::load_from(&PathBuf::from(temp_file.path()));
        assert!(result.is_err());
        match result.unwrap_err() {
            ConfigError::ParseError(_, _) => {}
            _ => panic!("Expected ParseError"),
        }
    }

    #[test]
    fn test_s3_access_key_id_with_config() {
        let config = RoboflowConfig {
            s3: Some(S3ConfigSection {
                access_key_id: Some("my_key".to_string()),
                access_key_secret: None,
                endpoint: None,
                region: None,
            }),
        };
        assert_eq!(config.s3_access_key_id(), Some("my_key"));
    }

    #[test]
    fn test_s3_access_key_id_without_s3_section() {
        let config = RoboflowConfig { s3: None };
        assert_eq!(config.s3_access_key_id(), None);
    }

    #[test]
    fn test_s3_access_key_id_without_key_field() {
        let config = RoboflowConfig {
            s3: Some(S3ConfigSection {
                access_key_id: None,
                access_key_secret: Some("secret".to_string()),
                endpoint: None,
                region: None,
            }),
        };
        assert_eq!(config.s3_access_key_id(), None);
    }

    #[test]
    fn test_s3_access_key_secret_with_config() {
        let config = RoboflowConfig {
            s3: Some(S3ConfigSection {
                access_key_id: None,
                access_key_secret: Some("my_secret".to_string()),
                endpoint: None,
                region: None,
            }),
        };
        assert_eq!(config.s3_access_key_secret(), Some("my_secret"));
    }

    #[test]
    fn test_s3_access_key_secret_without_s3_section() {
        let config = RoboflowConfig { s3: None };
        assert_eq!(config.s3_access_key_secret(), None);
    }

    #[test]
    fn test_s3_endpoint_with_config() {
        let config = RoboflowConfig {
            s3: Some(S3ConfigSection {
                access_key_id: None,
                access_key_secret: None,
                endpoint: Some("https://oss-cn-hangzhou.aliyuncs.com".to_string()),
                region: None,
            }),
        };
        assert_eq!(
            config.s3_endpoint(),
            Some("https://oss-cn-hangzhou.aliyuncs.com")
        );
    }

    #[test]
    fn test_s3_endpoint_without_s3_section() {
        let config = RoboflowConfig { s3: None };
        assert_eq!(config.s3_endpoint(), None);
    }

    #[test]
    fn test_s3_region_with_config() {
        let config = RoboflowConfig {
            s3: Some(S3ConfigSection {
                access_key_id: None,
                access_key_secret: None,
                endpoint: None,
                region: Some("cn-hangzhou".to_string()),
            }),
        };
        assert_eq!(config.s3_region(), Some("cn-hangzhou"));
    }

    #[test]
    fn test_s3_region_without_s3_section() {
        let config = RoboflowConfig { s3: None };
        assert_eq!(config.s3_region(), None);
    }

    #[test]
    fn test_config_error_home_dir_not_found_display() {
        let error = ConfigError::HomeDirNotFound;
        let display = format!("{}", error);
        assert!(display.contains("HOME directory not found"));
    }

    #[test]
    fn test_config_error_read_error_display() {
        let error = ConfigError::ReadError(
            PathBuf::from("/some/path.toml"),
            std::io::Error::new(std::io::ErrorKind::PermissionDenied, "permission denied"),
        );
        let display = format!("{}", error);
        assert!(display.contains("/some/path.toml"));
        assert!(display.contains("permission denied"));
    }

    #[test]
    fn test_config_error_parse_error_display() {
        let toml_error = toml::from_str::<RoboflowConfig>("invalid").unwrap_err();
        let error = ConfigError::ParseError(PathBuf::from("/config.toml"), toml_error);
        let display = format!("{}", error);
        assert!(display.contains("/config.toml"));
    }

    #[test]
    fn test_s3_config_section_debug() {
        let section = S3ConfigSection {
            access_key_id: Some("key".to_string()),
            access_key_secret: Some("secret".to_string()),
            endpoint: Some("endpoint".to_string()),
            region: Some("region".to_string()),
        };
        let debug_str = format!("{:?}", section);
        assert!(debug_str.contains("S3ConfigSection"));
        assert!(debug_str.contains("access_key_id"));
    }

    #[test]
    fn test_roboflow_config_debug() {
        let config = RoboflowConfig { s3: None };
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("RoboflowConfig"));
    }

    #[test]
    fn test_roboflow_config_clone() {
        let config = RoboflowConfig {
            s3: Some(S3ConfigSection {
                access_key_id: Some("key".to_string()),
                access_key_secret: None,
                endpoint: None,
                region: None,
            }),
        };
        let cloned = config.clone();
        assert_eq!(
            config.s3.as_ref().unwrap().access_key_id,
            cloned.s3.as_ref().unwrap().access_key_id
        );
    }
}
