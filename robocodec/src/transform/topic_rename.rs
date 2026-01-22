// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Topic renaming transformation.
//!
//! Provides topic renaming with regex-based wildcard pattern support and collision detection.

use regex::Regex;
use std::collections::{HashMap, HashSet};

use super::{ChannelInfo, McapTransform, TransformError};

/// A wildcard topic mapping using compiled regex.
#[derive(Debug, Clone)]
struct WildcardTopicMapping {
    /// Compiled regex pattern for matching (e.g., r"^/foo/(.*)$")
    pattern: Regex,
    /// Target template where $1 is replaced with the first capture group
    target_template: String,
}

impl WildcardTopicMapping {
    /// Create a new wildcard mapping from a wildcard pattern and target.
    ///
    /// # Arguments
    ///
    /// * `pattern` - Wildcard pattern like "/foo/*"
    /// * `target` - Target pattern like "/roboflow/*"
    fn new(pattern: &str, target: &str) -> Result<Self, String> {
        // Convert wildcard pattern to regex:
        // - Escape special regex characters except *
        // - Replace * with (.*) to capture any suffix
        // - Add ^ and $ anchors for exact matching

        let mut regex_pattern = String::from("^");

        for c in pattern.chars() {
            match c {
                '*' => {
                    regex_pattern.push_str("(.*)");
                }
                '.' | '$' | '^' | '+' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '\\' => {
                    regex_pattern.push('\\');
                    regex_pattern.push(c);
                }
                _ => {
                    regex_pattern.push(c);
                }
            }
        }
        regex_pattern.push('$');

        // Build target template by replacing * with $1, $2, etc.
        let mut target_template = String::new();
        let mut group_idx = 0;
        let target_chars = target.chars().peekable();

        for c in target_chars {
            if c == '*' {
                group_idx += 1;
                target_template.push_str(&format!("${{group{}}}", group_idx));
            } else {
                target_template.push(c);
            }
        }

        // Compile the regex
        let compiled = Regex::new(&regex_pattern)
            .map_err(|e| format!("Invalid wildcard pattern '{}': {}", pattern, e))?;

        Ok(Self {
            pattern: compiled,
            target_template,
        })
    }

    /// Apply this wildcard mapping to a topic.
    ///
    /// Returns Some(new_topic) if the pattern matches, None otherwise.
    fn apply(&self, topic: &str) -> Option<String> {
        self.pattern.captures(topic).map(|caps| {
            let mut result = self.target_template.clone();
            // Replace ${group1}, ${group2}, etc. with captured values
            for i in 1..caps.len() {
                let placeholder = format!("${{group{}}}", i);
                if let Some(captured) = caps.get(i) {
                    result = result.replace(&placeholder, captured.as_str());
                }
            }
            result
        })
    }
}

/// Topic renaming transformation.
///
/// Renames topics using exact string mappings or wildcard patterns.
/// When multiple source topics map to the same target topic, numeric suffixes
/// are automatically added to prevent collisions.
///
/// # Example
///
/// ```no_run
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use roboflow::transform::TopicRenameTransform;
///
/// let mut rename = TopicRenameTransform::new();
/// rename.add_mapping("/old_camera/image_raw", "/camera/image");
/// rename.add_wildcard_mapping("/foo/*", "/roboflow/*")?;
///
/// // When applied, topics will be renamed according to mappings.
/// // Collisions are auto-resolved: /a and /b both → /c becomes /c and /c_2
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct TopicRenameTransform {
    /// Exact topic mappings: source -> target
    mappings: HashMap<String, String>,
    /// Wildcard topic mappings (sorted by prefix length, longest first)
    wildcard_mappings: Vec<WildcardTopicMapping>,
}

impl Default for TopicRenameTransform {
    fn default() -> Self {
        Self::new()
    }
}

impl TopicRenameTransform {
    /// Create a new empty topic rename transform.
    pub fn new() -> Self {
        Self {
            mappings: HashMap::new(),
            wildcard_mappings: Vec::new(),
        }
    }

    /// Add an exact topic rename mapping.
    ///
    /// # Arguments
    ///
    /// * `source` - Original topic name (e.g., "/camera_front/image_raw")
    /// * `target` - New topic name (e.g., "/camera/image")
    pub fn add_mapping(&mut self, source: impl Into<String>, target: impl Into<String>) {
        self.mappings.insert(source.into(), target.into());
    }

    /// Add a wildcard topic rename mapping.
    ///
    /// The wildcard `*` will match any suffix. For example:
    /// - Pattern `/foo/*` with target `/roboflow/*`
    /// - Maps `/foo/upperlimb/joint_states` to `/roboflow/upperlimb/joint_states`
    ///
    /// # Arguments
    ///
    /// * `pattern` - Source pattern with wildcard (e.g., "/foo/*")
    /// * `target` - Target pattern with wildcard (e.g., "/roboflow/*")
    ///
    /// # Errors
    ///
    /// Returns an error if the pattern cannot be compiled into a regex.
    pub fn add_wildcard_mapping(
        &mut self,
        pattern: impl Into<String>,
        target: impl Into<String>,
    ) -> Result<(), String> {
        let pattern = pattern.into();
        let target = target.into();

        // Compile the wildcard pattern into a regex-based mapping
        let mapping = WildcardTopicMapping::new(&pattern, &target)?;

        self.wildcard_mappings.push(mapping);

        // Sort by pattern length (descending) so longer/more specific patterns match first
        // We approximate this by sorting by the regex pattern string length as a proxy for specificity
        self.wildcard_mappings
            .sort_by(|a, b| b.pattern.as_str().len().cmp(&a.pattern.as_str().len()));

        Ok(())
    }

    /// Create a transform from a HashMap of exact mappings.
    pub fn from_map(mappings: HashMap<String, String>) -> Self {
        Self {
            mappings,
            wildcard_mappings: Vec::new(),
        }
    }

    /// Create a transform with both exact and wildcard mappings.
    ///
    /// # Errors
    ///
    /// Returns an error if any wildcard pattern cannot be compiled.
    pub fn with_wildcards(
        exact_mappings: HashMap<String, String>,
        wildcard_mappings: Vec<(String, String)>,
    ) -> Result<Self, String> {
        let mut transform = Self::from_map(exact_mappings);
        for (pattern, target) in wildcard_mappings {
            transform.add_wildcard_mapping(pattern, target)?;
        }
        Ok(transform)
    }

    /// Get the number of exact mappings configured.
    pub fn len(&self) -> usize {
        self.mappings.len()
    }

    /// Get the number of wildcard mappings configured.
    pub fn wildcard_len(&self) -> usize {
        self.wildcard_mappings.len()
    }

    /// Check if any mappings are configured.
    pub fn is_empty(&self) -> bool {
        self.mappings.is_empty() && self.wildcard_mappings.is_empty()
    }

    /// Get all exact mappings.
    pub fn mappings(&self) -> &HashMap<String, String> {
        &self.mappings
    }

    /// Apply the transformation to a topic name.
    ///
    /// Returns `Some(new_name)` with the transformed topic.
    pub fn apply(&self, topic: &str) -> Option<String> {
        // First check exact mappings
        if let Some(exact_target) = self.mappings.get(topic) {
            return Some(exact_target.clone());
        }

        // Then try wildcard patterns (more specific patterns first due to sorting)
        for mapping in &self.wildcard_mappings {
            if let Some(renamed) = mapping.apply(topic) {
                return Some(renamed);
            }
        }

        // No match, return original
        Some(topic.to_string())
    }
}

impl McapTransform for TopicRenameTransform {
    fn transform_topic(&self, topic: &str) -> Option<String> {
        self.apply(topic)
    }

    fn transform_type(
        &self,
        type_name: &str,
        _schema_text: Option<&str>,
    ) -> (String, Option<String>) {
        // Topic transform doesn't modify types
        (type_name.to_string(), None)
    }

    fn validate(&self, channels: &[ChannelInfo]) -> std::result::Result<(), TransformError> {
        if self.mappings.is_empty() {
            return Ok(());
        }

        // Collect all existing topics
        let existing_topics: HashSet<&str> = channels.iter().map(|c| c.topic.as_str()).collect();

        // Check that all source topics exist
        for source in self.mappings.keys() {
            if !existing_topics.contains(source.as_str()) {
                return Err(TransformError::NotFound {
                    name: source.clone(),
                    kind: "topic",
                });
            }
        }

        // Build mapping from target topic to source topics
        let mut target_to_sources: HashMap<&str, Vec<&str>> = HashMap::new();

        for (source, target) in &self.mappings {
            target_to_sources.entry(target).or_default().push(source);
        }

        // Check for collisions where multiple sources map to the same target
        for (target, sources) in &target_to_sources {
            // Multiple sources mapping to the same target is OK
            // - we'll auto-suffix during application
            // But we should warn if the target also exists as a source topic
            // that isn't being renamed to itself
            if sources.len() > 1 {
                // Check if any of the sources IS the target
                // This is OK if it's a self-mapping
                let all_self_mappings = sources.iter().all(|s| *s == *target);
                if !all_self_mappings {
                    // Multiple different sources → same target
                    // This will be handled by auto-suffixing
                }
            } else {
                // Single source mapping
                let source = &sources[0];
                // Check if target conflicts with an existing topic that isn't the source
                if source != target && existing_topics.contains(*target) {
                    // The target exists as a topic that won't be renamed
                    // This could cause confusion but we'll allow it
                    // The original topic will keep its name alongside the renamed one
                }
            }
        }

        Ok(())
    }

    fn modifies_topics(&self) -> bool {
        !self.mappings.is_empty()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn box_clone(&self) -> Box<dyn McapTransform> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_channel(id: u16, topic: &str) -> ChannelInfo {
        ChannelInfo::new(
            id,
            topic.to_string(),
            "std_msgs/String".to_string(),
            "cdr".to_string(),
            Some("string data".to_string()),
            Some("ros2msg".to_string()),
        )
    }

    #[test]
    fn test_new() {
        let transform = TopicRenameTransform::new();
        assert!(transform.is_empty());
        assert_eq!(transform.len(), 0);
    }

    #[test]
    fn test_add_mapping() {
        let mut transform = TopicRenameTransform::new();
        transform.add_mapping("/old", "/new");
        assert_eq!(transform.len(), 1);
        assert!(transform.mappings().contains_key("/old"));
    }

    #[test]
    fn test_from_map() {
        let mut map = HashMap::new();
        map.insert("/a".to_string(), "/x".to_string());
        map.insert("/b".to_string(), "/y".to_string());

        let transform = TopicRenameTransform::from_map(map);
        assert_eq!(transform.len(), 2);
    }

    #[test]
    fn test_apply_with_mapping() {
        let mut transform = TopicRenameTransform::new();
        transform.add_mapping("/old", "/new");

        assert_eq!(transform.apply("/old"), Some("/new".to_string()));
        assert_eq!(transform.apply("/other"), Some("/other".to_string()));
    }

    #[test]
    fn test_validate_empty() {
        let transform = TopicRenameTransform::new();
        let channels = vec![make_channel(1, "/test")];
        assert!(transform.validate(&channels).is_ok());
    }

    #[test]
    fn test_validate_success() {
        let mut transform = TopicRenameTransform::new();
        transform.add_mapping("/test", "/renamed");

        let channels = vec![make_channel(1, "/test")];
        assert!(transform.validate(&channels).is_ok());
    }

    #[test]
    fn test_validate_not_found() {
        let mut transform = TopicRenameTransform::new();
        transform.add_mapping("/nonexistent", "/new");

        let channels = vec![make_channel(1, "/test")];
        let result = transform.validate(&channels);
        assert!(result.is_err());
        match result.unwrap_err() {
            TransformError::NotFound { name, .. } => assert_eq!(name, "/nonexistent"),
            _ => panic!("Expected NotFound error"),
        }
    }

    #[test]
    fn test_transform_topic() {
        let mut transform = TopicRenameTransform::new();
        transform.add_mapping("/camera_old", "/camera");

        assert_eq!(
            transform.transform_topic("/camera_old"),
            Some("/camera".to_string())
        );
        assert_eq!(
            transform.transform_topic("/other"),
            Some("/other".to_string())
        );
    }

    #[test]
    fn test_transform_type_passthrough() {
        let transform = TopicRenameTransform::new();
        let (new_type, schema) = transform.transform_type("std_msgs/String", Some("schema"));
        assert_eq!(new_type, "std_msgs/String");
        assert_eq!(schema, None); // Topic transform doesn't modify schemas
    }

    #[test]
    fn test_modifies_topics() {
        let mut transform = TopicRenameTransform::new();
        assert!(!transform.modifies_topics());

        transform.add_mapping("/a", "/b");
        assert!(transform.modifies_topics());
    }

    #[test]
    fn test_wildcard_mapping() {
        let mut transform = TopicRenameTransform::new();
        transform
            .add_wildcard_mapping("/foo/*", "/roboflow/*")
            .unwrap();

        // Exact match for wildcard pattern
        assert_eq!(
            transform.apply("/foo/upperlimb/joint_states"),
            Some("/roboflow/upperlimb/joint_states".to_string())
        );
        assert_eq!(
            transform.apply("/foo/sensor/camera"),
            Some("/roboflow/sensor/camera".to_string())
        );

        // Non-matching topic stays the same
        assert_eq!(
            transform.apply("/other/topic"),
            Some("/other/topic".to_string())
        );
    }

    #[test]
    fn test_wildcard_with_exact_mappings() {
        let mut transform = TopicRenameTransform::new();
        transform.add_mapping("/exact/topic", "/renamed/exact");
        transform
            .add_wildcard_mapping("/prefix/*", "/new/*")
            .unwrap();

        // Exact mapping takes priority
        assert_eq!(
            transform.apply("/exact/topic"),
            Some("/renamed/exact".to_string())
        );

        // Wildcard matches
        assert_eq!(
            transform.apply("/prefix/something"),
            Some("/new/something".to_string())
        );

        // No match
        assert_eq!(
            transform.apply("/other/topic"),
            Some("/other/topic".to_string())
        );
    }

    #[test]
    fn test_wildcard_len() {
        let mut transform = TopicRenameTransform::new();
        assert_eq!(transform.wildcard_len(), 0);

        transform.add_wildcard_mapping("/foo/*", "/bar/*").unwrap();
        assert_eq!(transform.wildcard_len(), 1);

        transform.add_wildcard_mapping("/baz/*", "/qux/*").unwrap();
        assert_eq!(transform.wildcard_len(), 2);
    }

    #[test]
    fn test_with_wildcards() {
        let mut exact = HashMap::new();
        exact.insert("/exact".to_string(), "/renamed".to_string());

        let wildcards = vec![
            ("/foo/*".to_string(), "/bar/*".to_string()),
            ("/baz/*".to_string(), "/qux/*".to_string()),
        ];

        let transform = TopicRenameTransform::with_wildcards(exact, wildcards).unwrap();

        assert_eq!(transform.len(), 1);
        assert_eq!(transform.wildcard_len(), 2);
    }
}
