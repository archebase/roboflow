//! Configuration file parser for type normalization.
//!
//! Loads type mappings from TOML config files.

use crate::transform::{TransformBuilder, TransformPipeline, TypeRenameTransform};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

/// Topic-aware type mapping.
#[derive(Debug, Deserialize, Clone)]
pub struct TopicTypeMapping {
    /// Topic pattern
    pub topic: String,
    /// Source type name
    pub from: String,
    /// Target type name
    pub to: String,
}

/// Topic name mapping.
#[derive(Debug, Deserialize, Clone)]
pub struct TopicMapping {
    /// Source topic pattern
    pub from: String,
    /// Target topic pattern
    pub to: String,
}

/// Type normalization configuration from TOML file.
#[derive(Debug, Deserialize, Default)]
pub struct NormalizeConfig {
    /// Global type mappings: source_type -> target_type
    #[serde(default)]
    pub type_mappings: HashMap<String, String>,

    /// Wildcard type mappings (e.g., "genie_msgs/msg/*" = "robocodec.msg.*")
    #[serde(default)]
    pub wildcard_mappings: HashMap<String, String>,

    /// Topic-specific type mappings
    #[serde(default)]
    pub topic_type_mappings: Vec<TopicTypeMapping>,

    /// Topic name mappings
    #[serde(default)]
    pub topic_mappings: Vec<TopicMapping>,
}

impl NormalizeConfig {
    /// Load configuration from a TOML file.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: NormalizeConfig = toml::from_str(&content)?;
        // Validate all type names
        config.validate()?;
        Ok(config)
    }

    /// Load configuration from the default location.
    ///
    /// Looks for `normalize.toml` in:
    /// 1. Current directory
    /// 2. `tests/fixtures/` directory (for robocodec)
    pub fn load_default() -> Result<Self, Box<dyn std::error::Error>> {
        // Try current directory first
        if Path::new("normalize.toml").exists() {
            return Self::from_file("normalize.toml");
        }

        // Try tests/fixtures/ directory
        let fixtures_path = "tests/fixtures/normalize.toml";
        if Path::new(fixtures_path).exists() {
            return Self::from_file(fixtures_path);
        }

        Err("normalize.toml not found in current directory or tests/fixtures/".into())
    }

    /// Validate all type names in the configuration.
    fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Validate type mappings
        for from in self.type_mappings.keys() {
            Self::validate_type_name(from)?;
        }

        // Validate wildcard mappings
        for pattern in self.wildcard_mappings.keys() {
            // Validate the pattern up to the wildcard
            if let Some(base) = pattern.strip_suffix('*') {
                Self::validate_type_name(base)?;
            } else {
                Self::validate_type_name(pattern)?;
            }
        }

        // Validate topic type mappings
        for mapping in &self.topic_type_mappings {
            Self::validate_type_name(&mapping.from)?;
            Self::validate_type_name(&mapping.to)?;
        }

        Ok(())
    }

    /// Validate a single type name based on its format.
    pub fn validate_type_name(type_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        let format = Self::detect_format(type_name);

        match format {
            TypeFormat::Proto => {
                // Proto: package.Type format where the last dot separates package from type
                // Examples: nmx.msg.JointStates, robocodec.msg.CameraIntrinsic
                // The package can have dots (e.g., nmx.msg), but there should be exactly
                // ONE "separator" dot between the package and the type.
                //
                // Valid: "nmx.msg.JointStates" (package=nmx.msg, type=JointStates)
                // Valid: "robocodec.msg.CameraIntrinsic" (package=robocodec.msg, type=CameraIntrinsic)
                // Invalid: "nmx.msg.camid_1.intrinsic" (implies package=nmx.msg.camid_1 - breaks consistency)

                // Count total dots - a valid proto type has at least 2 dots
                // (one for the package namespace like "nmx.msg", one for the separator)
                let dots = type_name.matches('.').count();

                // Validation rule: Proto type names must have exactly 1-2 dots total.
                // Format: "package.Type" (1 dot) or "package.subpackage.Type" (2 dots)
                // Examples: "nmx.msg.JointStates", "robocodec.msg.CameraIntrinsic"
                // Invalid: "nmx.msg.camid_1.intrinsic" (3+ dots - too much nesting)

                if dots == 1 {
                    // Format: "pkg.Type" - single package, no nested namespace
                    Ok(())
                } else if dots == 2 {
                    // Format: "pkg.nested.Type" - nested package like "nmx.msg.JointStates"
                    Ok(())
                } else {
                    Err(format!(
                        "Invalid proto type '{type_name}': proto types should use 'package.Type' format. \
                         Examples: 'nmx.msg.JointStates' (package=nmx.msg, type=JointStates), \
                         'robocodec.msg.CameraIntrinsic'. \
                         For nested types within the same package, use underscore \
                         (e.g., 'robocodec.msg.Type_Name' not 'robocodec.msg.nested.Type')"
                    ).into())
                }
            }
            TypeFormat::Ros1 | TypeFormat::Ros2 | TypeFormat::Unknown => Ok(()),
        }
    }

    /// Detect the format of a type name.
    fn detect_format(type_name: &str) -> TypeFormat {
        if type_name.contains('/') {
            if type_name.contains("/msg/") {
                TypeFormat::Ros2
            } else {
                TypeFormat::Ros1
            }
        } else if type_name.contains('.') {
            TypeFormat::Proto
        } else {
            TypeFormat::Unknown
        }
    }

    /// Convert config to a TransformPipeline.
    pub fn to_pipeline(&self) -> TransformPipeline {
        let mut builder = TransformBuilder::new();

        // Add global type mappings
        for (from, to) in &self.type_mappings {
            builder = builder.with_type_rename(from, to);
        }

        // Add wildcard type mappings
        for (pattern, target) in &self.wildcard_mappings {
            builder = builder.with_type_rename_wildcard(pattern, target);
        }

        // Add topic-specific type mappings
        for mapping in &self.topic_type_mappings {
            builder = builder.with_topic_type_rename(&mapping.topic, &mapping.from, &mapping.to);
        }

        // Add topic name mappings (detect wildcards)
        for mapping in &self.topic_mappings {
            if mapping.from.ends_with('*') || mapping.to.ends_with('*') {
                // Wildcard mapping
                builder = builder.with_topic_rename_wildcard(&mapping.from, &mapping.to);
            } else {
                // Exact mapping
                builder = builder.with_topic_rename(&mapping.from, &mapping.to);
            }
        }

        builder.build()
    }

    /// Create a TypeRenameTransform from this config.
    pub fn to_type_rename_transform(&self) -> TypeRenameTransform {
        let mut transform = TypeRenameTransform::new();

        // Add global type mappings
        for (from, to) in &self.type_mappings {
            transform.add_mapping(from, to);
        }

        // Add wildcard mappings
        for (pattern, target) in &self.wildcard_mappings {
            transform.add_wildcard_mapping(pattern, target);
        }

        transform
    }
}

/// Detected type name format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TypeFormat {
    Ros1,
    Ros2,
    Proto,
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_config() {
        let toml_content = r#"
[type_mappings]
"foo/Bar" = "baz/Bar"
"#;

        let config: NormalizeConfig = toml::from_str(toml_content).unwrap();
        assert!(config.validate().is_ok());
        assert_eq!(config.type_mappings.len(), 1);
    }

    #[test]
    fn test_parse_wildcard_config() {
        let toml_content = r#"
[wildcard_mappings]
"genie_msgs/msg/*" = "robocodec.msg.*"
"#;

        let config: NormalizeConfig = toml::from_str(toml_content).unwrap();
        assert!(config.validate().is_ok());
        assert_eq!(config.wildcard_mappings.len(), 1);
    }

    #[test]
    fn test_parse_topic_type_mapping() {
        let toml_content = r#"
[[topic_type_mappings]]
topic = "/lowdim/joint"
from = "nmx.msg.LowdimData"
to = "robocodec.msg.JointStates"
"#;

        let config: NormalizeConfig = toml::from_str(toml_content).unwrap();
        assert!(config.validate().is_ok());
        assert_eq!(config.topic_type_mappings.len(), 1);
        assert_eq!(config.topic_type_mappings[0].topic, "/lowdim/joint");
    }

    #[test]
    fn test_parse_topic_mapping() {
        let toml_content = r#"
[[topic_mappings]]
from = "/old/topic"
to = "/new/topic"
"#;

        let config: NormalizeConfig = toml::from_str(toml_content).unwrap();
        assert!(config.validate().is_ok());
        assert_eq!(config.topic_mappings.len(), 1);
    }

    #[test]
    fn test_valid_proto_type() {
        assert!(NormalizeConfig::validate_type_name("nmx.msg.JointStates").is_ok());
        assert!(NormalizeConfig::validate_type_name("robocodec.msg.CameraIntrinsic").is_ok());
    }

    #[test]
    fn test_invalid_proto_type() {
        let result = NormalizeConfig::validate_type_name("nmx.msg.camid_1.intrinsic");
        assert!(result.is_err());
        let err = result.unwrap_err();
        let err_msg = err.to_string();
        // Error should mention the invalid format and suggest using underscore
        assert!(err_msg.contains("Invalid proto type"));
        assert!(err_msg.contains("underscore"));
    }

    #[test]
    fn test_empty_config() {
        let toml_content = "";
        let config: NormalizeConfig = toml::from_str(toml_content).unwrap();
        assert!(config.validate().is_ok());
        assert!(config.type_mappings.is_empty());
        assert!(config.wildcard_mappings.is_empty());
    }
}
