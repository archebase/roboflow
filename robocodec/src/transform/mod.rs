// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! MCAP transformation system.
//!
//! This module provides a trait-based transformation system for normalizing
//! MCAP files. Transformations can rename topics, message types, and rewrite
//! schema definitions.
//!
//! # Example
//!
//! ```no_run
//! # fn main() {
//! use roboflow::transform::{TopicRenameTransform, MultiTransform};
//!
//! let mut topic_rename = TopicRenameTransform::new();
//! topic_rename.add_mapping("/old_topic", "/new_topic");
//!
//! let mut pipeline = MultiTransform::new();
//! pipeline.add_transform(Box::new(topic_rename));
//!
//! // Use pipeline with McapRewriter...
//! # }
//! ```

pub mod normalization;
pub mod pipeline;
pub mod topic_rename;
pub mod type_rename;

use std::collections::HashMap;
use std::fmt;

pub use normalization::TypeNormalization;
pub use pipeline::MultiTransform;
pub use topic_rename::TopicRenameTransform;
pub use type_rename::{TopicAwareTypeRenameTransform, TypeRenameTransform};

/// Information about a channel in an MCAP file.
///
/// This is a simplified version of ChannelInfo for use in transforms.
#[derive(Debug, Clone)]
pub struct ChannelInfo {
    /// Channel ID
    pub id: u16,
    /// Topic name (e.g., "/joint_states")
    pub topic: String,
    /// Message type (e.g., "sensor_msgs/msg/JointState")
    pub message_type: String,
    /// Encoding (e.g., "cdr", "protobuf", "json")
    pub encoding: String,
    /// Schema definition (message definition text)
    pub schema: Option<String>,
    /// Schema encoding (e.g., "ros2msg", "protobuf")
    pub schema_encoding: Option<String>,
}

/// Result of applying a transform to a channel.
#[derive(Debug, Clone)]
pub struct TransformedChannel {
    /// Original channel ID
    pub original_id: u16,
    /// Transformed topic name
    pub topic: String,
    /// Transformed message type name
    pub message_type: String,
    /// Transformed schema text (if modified)
    pub schema: Option<String>,
    /// Encoding (unchanged)
    pub encoding: String,
    /// Schema encoding (unchanged)
    pub schema_encoding: Option<String>,
}

impl ChannelInfo {
    /// Create a new ChannelInfo.
    pub fn new(
        id: u16,
        topic: String,
        message_type: String,
        encoding: String,
        schema: Option<String>,
        schema_encoding: Option<String>,
    ) -> Self {
        Self {
            id,
            topic,
            message_type,
            encoding,
            schema,
            schema_encoding,
        }
    }

    /// Convert from the McapReader's ChannelInfo.
    pub fn from_reader_info(info: &crate::mcap::reader::ChannelInfo) -> Self {
        Self {
            id: info.id,
            topic: info.topic.clone(),
            message_type: info.message_type.clone(),
            encoding: info.encoding.clone(),
            schema: info.schema.clone(),
            schema_encoding: info.schema_encoding.clone(),
        }
    }
}

/// Error types for transformations.
#[derive(Debug, Clone)]
pub enum TransformError {
    /// Topic name collision detected
    TopicCollision {
        /// Original topic names that map to the same target
        sources: Vec<String>,
        /// Target topic name
        target: String,
    },

    /// Type name collision detected
    TypeCollision {
        /// Original type names that map to the same target
        sources: Vec<String>,
        /// Target type name
        target: String,
    },

    /// Invalid transformation rule
    InvalidRule {
        /// Description of the rule
        rule: String,
        /// Reason why it's invalid
        reason: String,
    },

    /// Schema text transformation failed
    SchemaTransformError {
        /// Type name being transformed
        type_name: String,
        /// Error description
        reason: String,
    },

    /// Source topic or type not found in MCAP
    NotFound {
        /// Name that wasn't found
        name: String,
        /// Whether this is a topic or type
        kind: &'static str,
    },
}

impl fmt::Display for TransformError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransformError::TopicCollision { sources, target } => write!(
                f,
                "Topic rename collision: multiple topics map to '{target}': {sources:?}"
            ),
            TransformError::TypeCollision { sources, target } => write!(
                f,
                "Type rename collision: multiple types map to '{target}': {sources:?}"
            ),
            TransformError::InvalidRule { rule, reason } => {
                write!(f, "Invalid rule '{rule}': {reason}")
            }
            TransformError::SchemaTransformError { type_name, reason } => {
                write!(f, "Schema transform error for '{type_name}': {reason}")
            }
            TransformError::NotFound { name, kind } => {
                write!(f, "Cannot rename {kind} '{name}': not found in MCAP")
            }
        }
    }
}

impl std::error::Error for TransformError {}

impl From<TransformError> for crate::CodecError {
    fn from(err: TransformError) -> Self {
        crate::CodecError::encode("Transform", err.to_string())
    }
}

/// Core transformation trait for MCAP metadata.
///
/// Each transformation can modify topic names, message types, or schema text.
/// Transformations are applied in sequence during rewrite.
///
/// # Example
///
/// ```no_run
/// # use roboflow::transform::{McapTransform, TransformError, ChannelInfo};
/// # use std::any::Any;
/// struct MyTransform;
///
/// impl McapTransform for MyTransform {
///     fn transform_topic(&self, topic: &str) -> Option<String> {
///         Some(topic.to_uppercase())
///     }
///
///     fn transform_type(&self, type_name: &str, schema_text: Option<&str>)
///         -> (String, Option<String>)
///     {
///         (type_name.to_string(), schema_text.map(|s| s.to_string()))
///     }
///
///     fn validate(&self, channels: &[ChannelInfo]) -> Result<(), TransformError> {
///         Ok(()) // or Err(TransformError::...)
///     }
///
///     fn as_any(&self) -> &dyn Any { self }
///
///     fn box_clone(&self) -> Box<dyn McapTransform> {
///         Box::new(MyTransform)
///     }
/// }
/// ```
pub trait McapTransform: Send + Sync + 'static {
    /// Transform a topic name.
    ///
    /// Returns `None` if the topic should be dropped (not implemented by default transforms).
    fn transform_topic(&self, topic: &str) -> Option<String> {
        Some(topic.to_string())
    }

    /// Transform a message type name and optionally its schema text.
    ///
    /// Returns a tuple of (new_type_name, new_schema_text).
    /// The schema text is `Some(rewritten)` if modified, `Some(original)` if unchanged,
    /// or `None` if there was no schema.
    fn transform_type(
        &self,
        type_name: &str,
        schema_text: Option<&str>,
    ) -> (String, Option<String>);

    /// Validate that the transformation won't cause collisions or other issues.
    ///
    /// This is called before the rewrite begins to fail fast on invalid configurations.
    fn validate(&self, channels: &[ChannelInfo]) -> std::result::Result<(), TransformError>;

    /// Check if this transform modifies topics.
    fn modifies_topics(&self) -> bool {
        false
    }

    /// Check if this transform modifies types.
    fn modifies_types(&self) -> bool {
        false
    }

    /// Check if this transform modifies schemas.
    fn modifies_schemas(&self) -> bool {
        false
    }

    /// Get a reference as `Any` for downcasting to concrete transform types.
    ///
    /// This enables runtime type checking for specialized transform behavior,
    /// such as topic-aware type transformations.
    ///
    /// Each concrete transform type must implement this method.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Clone this transform into a boxed trait object.
    ///
    /// This enables cloning of `MultiTransform` which contains
    /// `Vec<Box<dyn McapTransform>>`. Each concrete transform type
    /// must implement this method to support batch processing with transforms.
    fn box_clone(&self) -> Box<dyn McapTransform>;
}

/// Builder helper for creating common transformations.
pub struct TransformBuilder {
    topic_mappings: HashMap<String, String>,
    /// Wildcard topic mappings: (pattern, target) where pattern is like "/foo/*"
    topic_wildcards: Vec<(String, String)>,
    type_mappings: HashMap<String, String>,
    /// Wildcard type mappings: (pattern, target) where pattern is like "foo/*"
    type_wildcards: Vec<(String, String)>,
    /// Topic-specific type mappings: (topic, source_type) -> target_type
    topic_type_mappings: HashMap<(String, String), String>,
}

impl Default for TransformBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TransformBuilder {
    /// Create a new builder with no mappings.
    pub fn new() -> Self {
        Self {
            topic_mappings: HashMap::new(),
            topic_wildcards: Vec::new(),
            type_mappings: HashMap::new(),
            type_wildcards: Vec::new(),
            topic_type_mappings: HashMap::new(),
        }
    }

    /// Add a topic rename mapping.
    pub fn with_topic_rename(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.topic_mappings.insert(from.into(), to.into());
        self
    }

    /// Add a wildcard topic rename mapping.
    ///
    /// The wildcard `*` matches any topic suffix. For example:
    /// - `"/foo/*"` → `"/roboflow/*"` will rename all topics starting with "/foo/"
    ///
    /// # Arguments
    ///
    /// * `pattern` - Wildcard pattern like "/foo/*"
    /// * `target` - Target pattern like "/bar/*"
    pub fn with_topic_rename_wildcard(
        mut self,
        pattern: impl Into<String>,
        target: impl Into<String>,
    ) -> Self {
        self.topic_wildcards.push((pattern.into(), target.into()));
        self
    }

    /// Add a type rename mapping.
    pub fn with_type_rename(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.type_mappings.insert(from.into(), to.into());
        self
    }

    /// Add a wildcard type rename mapping.
    ///
    /// The wildcard `*` matches any type name suffix. For example:
    /// - `"foo/*"` → `"bar/*"` will rename all types starting with "foo/" to "bar/"
    ///
    /// # Arguments
    ///
    /// * `pattern` - Wildcard pattern like "foo/*"
    /// * `target` - Target pattern like "bar/*"
    pub fn with_type_rename_wildcard(
        mut self,
        pattern: impl Into<String>,
        target: impl Into<String>,
    ) -> Self {
        self.type_wildcards.push((pattern.into(), target.into()));
        self
    }

    /// Add a topic-specific type rename mapping.
    ///
    /// This allows the same source type to map to different target types based on the topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic name (exact match)
    /// * `source_type` - Original type name (e.g., "nmx.msg.LowdimData")
    /// * `target_type` - New type name (e.g., "nmx.msg.JointStates")
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn main() {
    /// use roboflow::transform::TransformBuilder;
    ///
    /// let pipeline = TransformBuilder::new()
    ///     .with_topic_type_rename(
    ///         "/lowdim/joint",
    ///         "nmx.msg.LowdimData",
    ///         "nmx.msg.JointStates"
    ///     )
    ///     .build();
    /// # }
    /// ```
    pub fn with_topic_type_rename(
        mut self,
        topic: impl Into<String>,
        source_type: impl Into<String>,
        target_type: impl Into<String>,
    ) -> Self {
        self.topic_type_mappings
            .insert((topic.into(), source_type.into()), target_type.into());
        self
    }

    /// Build a MultiTransform from this builder.
    pub fn build(self) -> MultiTransform {
        let mut pipeline = MultiTransform::new();

        // Add topic rename transform (exact + wildcard)
        if !self.topic_mappings.is_empty() || !self.topic_wildcards.is_empty() {
            pipeline.add_transform(Box::new(
                TopicRenameTransform::with_wildcards(self.topic_mappings, self.topic_wildcards)
                    .map_err(|e| TransformError::InvalidRule {
                        rule: "topic wildcard pattern".to_string(),
                        reason: format!("Failed to compile regex: {}", e),
                    })
                    .expect("Invalid topic wildcard pattern in TransformBuilder"),
            ));
        }

        // Add type rename transform (exact + wildcard)
        if !self.type_mappings.is_empty() || !self.type_wildcards.is_empty() {
            let mut type_transform = TypeRenameTransform::from_map(self.type_mappings);
            for (pattern, target) in self.type_wildcards {
                type_transform.add_wildcard_mapping(pattern, target);
            }
            pipeline.add_transform(Box::new(type_transform));
        }

        if !self.topic_type_mappings.is_empty() {
            pipeline.add_transform(Box::new(TopicAwareTypeRenameTransform::from_map(
                self.topic_type_mappings,
            )));
        }

        pipeline
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transform_error_display() {
        let err = TransformError::TopicCollision {
            sources: vec!["/a".to_string(), "/b".to_string()],
            target: "/c".to_string(),
        };
        assert!(err.to_string().contains("Topic rename collision"));
    }

    #[test]
    fn test_channel_info_new() {
        let info = ChannelInfo::new(
            1,
            "/test".to_string(),
            "std_msgs/String".to_string(),
            "cdr".to_string(),
            Some("string data".to_string()),
            Some("ros2msg".to_string()),
        );
        assert_eq!(info.id, 1);
        assert_eq!(info.topic, "/test");
        assert_eq!(info.message_type, "std_msgs/String");
    }

    #[test]
    fn test_transform_builder_empty() {
        let pipeline = TransformBuilder::new().build();
        assert_eq!(pipeline.transform_count(), 0);
    }

    #[test]
    fn test_transform_builder_with_mappings() {
        let pipeline = TransformBuilder::new()
            .with_topic_rename("/old", "/new")
            .with_type_rename("old_pkg/Msg", "new_pkg/Msg")
            .build();
        assert_eq!(pipeline.transform_count(), 2);
    }
}
