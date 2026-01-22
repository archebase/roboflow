// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Transformation pipeline for applying multiple transforms in sequence.

use std::collections::HashMap;
use std::fmt;

use super::{ChannelInfo, McapTransform, TransformError, TransformedChannel};

/// Multi-transform that applies multiple transforms in sequence.
///
/// Transforms are applied in the order they were added. Each transform
/// receives the output of the previous transform.
///
/// # Example
///
/// ```no_run
/// use roboflow::transform::{MultiTransform, TopicRenameTransform, TypeRenameTransform};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut pipeline = MultiTransform::new();
/// pipeline.add_transform(Box::new(TopicRenameTransform::new()));
/// pipeline.add_transform(Box::new(TypeRenameTransform::new()));
///
/// // Validate against channels
/// // pipeline.validate(&channels)?;
///
/// // Apply transformations
/// // let transformed = pipeline.transform_channel(&channel);
/// // // Access: transformed.topic, transformed.message_type, transformed.schema
/// # Ok(())
/// # }
/// ```
pub struct MultiTransform {
    transforms: Vec<Box<dyn McapTransform>>,
}

impl Clone for MultiTransform {
    fn clone(&self) -> Self {
        Self {
            transforms: self.transforms.iter().map(|t| t.box_clone()).collect(),
        }
    }
}

impl fmt::Debug for MultiTransform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiTransform")
            .field("transform_count", &self.transforms.len())
            .finish()
    }
}

impl Default for MultiTransform {
    fn default() -> Self {
        Self::new()
    }
}

impl MultiTransform {
    /// Create a new empty pipeline.
    pub fn new() -> Self {
        Self {
            transforms: Vec::new(),
        }
    }

    /// Add a transform to the pipeline.
    ///
    /// Transforms are applied in the order they are added.
    pub fn add_transform(&mut self, transform: Box<dyn McapTransform>) {
        self.transforms.push(transform);
    }

    /// Get the number of transforms in the pipeline.
    pub fn transform_count(&self) -> usize {
        self.transforms.len()
    }

    /// Check if the pipeline is empty.
    pub fn is_empty(&self) -> bool {
        self.transforms.is_empty()
    }

    /// Validate all transforms against the channels.
    ///
    /// This checks for collisions, missing sources, and other validation issues.
    pub fn validate(&self, channels: &[ChannelInfo]) -> std::result::Result<(), TransformError> {
        for transform in &self.transforms {
            transform.validate(channels)?;
        }
        Ok(())
    }

    /// Apply all transforms to a topic name.
    ///
    /// Returns `None` if any transform drops the topic.
    pub fn transform_topic(&self, topic: &str) -> Option<String> {
        let mut current = topic.to_string();
        for transform in &self.transforms {
            current = transform.transform_topic(&current)?;
        }
        Some(current)
    }

    /// Apply all transforms to a type name and schema text.
    ///
    /// Returns (new_type_name, new_schema_text).
    /// The schema text is `Some(rewritten)` if modified, `Some(original)` if unchanged,
    /// or `None` if there was no schema.
    pub fn transform_type(
        &self,
        type_name: &str,
        schema_text: Option<&str>,
    ) -> (String, Option<String>) {
        let mut current_type = type_name.to_string();
        let mut current_schema = schema_text.map(|s| s.to_string());

        for transform in &self.transforms {
            let (new_type, new_schema) =
                transform.transform_type(&current_type, current_schema.as_deref());
            current_type = new_type;
            // Only update schema if the transform provided a new one
            if let Some(schema) = new_schema {
                current_schema = Some(schema);
            }
        }

        (current_type, current_schema)
    }

    /// Apply all transforms to a type name with topic context.
    ///
    /// This method enables topic-specific type transformations. If a TopicAwareTypeRenameTransform
    /// is present in the pipeline, it will be queried for (topic, type) specific mappings.
    ///
    /// All transforms are applied in sequence, allowing both topic-aware and global
    /// type transformations to work together.
    ///
    /// Returns (new_type_name, new_schema_text).
    ///
    /// # Arguments
    ///
    /// * `topic` - The channel topic
    /// * `type_name` - Original type name
    /// * `schema_text` - Optional schema text
    ///
    /// # Example
    ///
    /// ```no_run
    /// use roboflow::TransformBuilder;
    ///
    /// # fn main() {
    /// let pipeline = TransformBuilder::new()
    ///     .with_topic_type_rename("/lowdim/joint", "nmx.msg.LowdimData", "nmx.msg.JointStates")
    ///     .build();
    ///
    /// let (new_type, schema) = pipeline.transform_type_with_topic("/lowdim/joint", "nmx.msg.LowdimData", Some("schema"));
    /// assert_eq!(new_type, "nmx.msg.JointStates");
    /// # }
    /// ```
    pub fn transform_type_with_topic(
        &self,
        topic: &str,
        type_name: &str,
        schema_text: Option<&str>,
    ) -> (String, Option<String>) {
        use super::TopicAwareTypeRenameTransform;

        let mut current_type = type_name.to_string();
        let mut current_schema = schema_text.map(|s| s.to_string());

        for transform in &self.transforms {
            // Try topic-aware transformation first
            if let Some(aware) = transform
                .as_any()
                .downcast_ref::<TopicAwareTypeRenameTransform>()
            {
                let (new_type, new_schema) = aware.apply_for_topic_with_schema(
                    topic,
                    &current_type,
                    current_schema.as_deref(),
                );
                current_type = new_type;
                if let Some(schema) = new_schema {
                    current_schema = Some(schema);
                }
            } else {
                // Apply regular transform
                let (new_type, new_schema) =
                    transform.transform_type(&current_type, current_schema.as_deref());
                current_type = new_type;
                if let Some(schema) = new_schema {
                    current_schema = Some(schema);
                }
            }
        }

        (current_type, current_schema)
    }

    /// Apply all transforms to a channel, returning the transformed metadata.
    ///
    /// This is the main entry point for transforming channel information.
    pub fn transform_channel(&self, channel: &ChannelInfo) -> TransformedChannel {
        let topic = self.transform_topic(&channel.topic).unwrap_or_default();
        let (message_type, schema) =
            self.transform_type(&channel.message_type, channel.schema.as_deref());

        TransformedChannel {
            original_id: channel.id,
            topic,
            message_type,
            schema,
            encoding: channel.encoding.clone(),
            schema_encoding: channel.schema_encoding.clone(),
        }
    }

    /// Build a map from original topic to transformed topic.
    ///
    /// Useful for quick lookups during message processing.
    pub fn build_topic_map(&self, channels: &[ChannelInfo]) -> HashMap<String, String> {
        channels
            .iter()
            .filter_map(|ch| {
                let transformed = self.transform_topic(&ch.topic)?;
                Some((ch.topic.clone(), transformed))
            })
            .collect()
    }

    /// Build a map from original type to transformed type.
    pub fn build_type_map(&self, channels: &[ChannelInfo]) -> HashMap<String, String> {
        channels
            .iter()
            .map(|ch| {
                let (transformed, _) = self.transform_type(&ch.message_type, None);
                (ch.message_type.clone(), transformed)
            })
            .collect()
    }

    /// Check if any transform in the pipeline modifies topics.
    pub fn modifies_topics(&self) -> bool {
        self.transforms.iter().any(|t| t.modifies_topics())
    }

    /// Check if any transform in the pipeline modifies types.
    pub fn modifies_types(&self) -> bool {
        self.transforms.iter().any(|t| t.modifies_types())
    }

    /// Check if any transform in the pipeline modifies schemas.
    pub fn modifies_schemas(&self) -> bool {
        self.transforms.iter().any(|t| t.modifies_schemas())
    }
}

#[cfg(test)]
mod tests {
    use super::super::{TopicRenameTransform, TypeRenameTransform};
    use super::*;

    fn make_channel(id: u16, topic: &str, msg_type: &str) -> ChannelInfo {
        ChannelInfo::new(
            id,
            topic.to_string(),
            msg_type.to_string(),
            "cdr".to_string(),
            Some("schema text".to_string()),
            Some("ros2msg".to_string()),
        )
    }

    #[test]
    fn test_new() {
        let pipeline = MultiTransform::new();
        assert!(pipeline.is_empty());
        assert_eq!(pipeline.transform_count(), 0);
    }

    #[test]
    fn test_add_transform() {
        let mut pipeline = MultiTransform::new();
        pipeline.add_transform(Box::new(TopicRenameTransform::new()));
        assert_eq!(pipeline.transform_count(), 1);
    }

    #[test]
    fn test_transform_topic_empty() {
        let pipeline = MultiTransform::new();
        assert_eq!(pipeline.transform_topic("/test"), Some("/test".to_string()));
    }

    #[test]
    fn test_transform_topic_single() {
        let mut pipeline = MultiTransform::new();
        let mut rename = TopicRenameTransform::new();
        rename.add_mapping("/old", "/new");
        pipeline.add_transform(Box::new(rename));

        assert_eq!(pipeline.transform_topic("/old"), Some("/new".to_string()));
        assert_eq!(
            pipeline.transform_topic("/other"),
            Some("/other".to_string())
        );
    }

    #[test]
    fn test_transform_topic_chained() {
        let mut pipeline = MultiTransform::new();

        let mut rename1 = TopicRenameTransform::new();
        rename1.add_mapping("/a", "/b");
        pipeline.add_transform(Box::new(rename1));

        let mut rename2 = TopicRenameTransform::new();
        rename2.add_mapping("/b", "/c");
        pipeline.add_transform(Box::new(rename2));

        // /a -> /b -> /c
        assert_eq!(pipeline.transform_topic("/a"), Some("/c".to_string()));
        // /b -> /c
        assert_eq!(pipeline.transform_topic("/b"), Some("/c".to_string()));
        // /x unchanged
        assert_eq!(pipeline.transform_topic("/x"), Some("/x".to_string()));
    }

    #[test]
    fn test_transform_type_empty() {
        let pipeline = MultiTransform::new();
        let (new_type, schema) = pipeline.transform_type("old/Type", Some("schema"));
        assert_eq!(new_type, "old/Type");
        assert_eq!(schema, Some("schema".to_string()));
    }

    #[test]
    fn test_transform_type_single() {
        let mut pipeline = MultiTransform::new();
        let mut rename = TypeRenameTransform::new();
        rename.add_mapping("old/Type", "new/Type");
        pipeline.add_transform(Box::new(rename));

        let (new_type, schema) = pipeline.transform_type("old/Type", Some("schema text"));
        assert_eq!(new_type, "new/Type");
        assert_eq!(schema, Some("schema text".to_string()));
    }

    #[test]
    fn test_transform_channel() {
        let mut pipeline = MultiTransform::new();
        let mut topic_rename = TopicRenameTransform::new();
        topic_rename.add_mapping("/old_topic", "/new_topic");
        pipeline.add_transform(Box::new(topic_rename));

        let mut type_rename = TypeRenameTransform::new();
        type_rename.add_mapping("old/Type", "new/Type");
        pipeline.add_transform(Box::new(type_rename));

        let channel = make_channel(1, "/old_topic", "old/Type");
        let transformed = pipeline.transform_channel(&channel);

        assert_eq!(transformed.original_id, 1);
        assert_eq!(transformed.topic, "/new_topic");
        assert_eq!(transformed.message_type, "new/Type");
        assert_eq!(transformed.encoding, "cdr");
        assert!(transformed.schema.is_some());
    }

    #[test]
    fn test_validate_empty() {
        let pipeline = MultiTransform::new();
        let channels = vec![make_channel(1, "/test", "std_msgs/String")];
        assert!(pipeline.validate(&channels).is_ok());
    }

    #[test]
    fn test_validate_success() {
        let mut pipeline = MultiTransform::new();
        let mut rename = TopicRenameTransform::new();
        rename.add_mapping("/test", "/renamed");
        pipeline.add_transform(Box::new(rename));

        let channels = vec![make_channel(1, "/test", "std_msgs/String")];
        assert!(pipeline.validate(&channels).is_ok());
    }

    #[test]
    fn test_build_topic_map() {
        let mut pipeline = MultiTransform::new();
        let mut rename = TopicRenameTransform::new();
        rename.add_mapping("/a", "/x");
        rename.add_mapping("/b", "/y");
        pipeline.add_transform(Box::new(rename));

        let channels = vec![
            make_channel(1, "/a", "std_msgs/String"),
            make_channel(2, "/b", "std_msgs/String"),
            make_channel(3, "/c", "std_msgs/String"),
        ];

        let map = pipeline.build_topic_map(&channels);
        assert_eq!(map.get("/a"), Some(&"/x".to_string()));
        assert_eq!(map.get("/b"), Some(&"/y".to_string()));
        assert_eq!(map.get("/c"), Some(&"/c".to_string()));
    }

    #[test]
    fn test_build_type_map() {
        let mut pipeline = MultiTransform::new();
        let mut rename = TypeRenameTransform::new();
        rename.add_mapping("a/Type", "x/Type");
        rename.add_mapping("b/Type", "y/Type");
        pipeline.add_transform(Box::new(rename));

        let channels = vec![
            make_channel(1, "/test1", "a/Type"),
            make_channel(2, "/test2", "b/Type"),
            make_channel(3, "/test3", "c/Type"),
        ];

        let map = pipeline.build_type_map(&channels);
        assert_eq!(map.get("a/Type"), Some(&"x/Type".to_string()));
        assert_eq!(map.get("b/Type"), Some(&"y/Type".to_string()));
        assert_eq!(map.get("c/Type"), Some(&"c/Type".to_string()));
    }

    #[test]
    fn test_modifies_topics() {
        let mut pipeline = MultiTransform::new();
        assert!(!pipeline.modifies_topics());

        let mut rename = TopicRenameTransform::new();
        rename.add_mapping("/a", "/b");
        pipeline.add_transform(Box::new(rename));
        assert!(pipeline.modifies_topics());
    }

    #[test]
    fn test_modifies_types() {
        let mut pipeline = MultiTransform::new();
        assert!(!pipeline.modifies_types());

        let mut rename = TypeRenameTransform::new();
        rename.add_mapping("a/A", "b/A");
        pipeline.add_transform(Box::new(rename));
        assert!(pipeline.modifies_types());
    }

    #[test]
    fn test_modifies_schemas() {
        let mut pipeline = MultiTransform::new();
        assert!(!pipeline.modifies_schemas());

        let mut rename = TypeRenameTransform::new();
        rename.add_mapping("a/A", "b/A");
        pipeline.add_transform(Box::new(rename));
        assert!(pipeline.modifies_schemas());
    }

    #[test]
    fn test_clone() {
        let mut pipeline = MultiTransform::new();
        let mut topic_rename = TopicRenameTransform::new();
        topic_rename.add_mapping("/a", "/b");
        pipeline.add_transform(Box::new(topic_rename));

        let mut type_rename = TypeRenameTransform::new();
        type_rename.add_mapping("old/Type", "new/Type");
        pipeline.add_transform(Box::new(type_rename));

        // Clone the pipeline
        let cloned = pipeline.clone();

        // Verify both have same number of transforms
        assert_eq!(pipeline.transform_count(), 2);
        assert_eq!(cloned.transform_count(), 2);

        // Verify both produce the same results
        assert_eq!(pipeline.transform_topic("/a"), Some("/b".to_string()));
        assert_eq!(cloned.transform_topic("/a"), Some("/b".to_string()));

        let (type1, _) = pipeline.transform_type("old/Type", None);
        let (type2, _) = cloned.transform_type("old/Type", None);
        assert_eq!(type1, "new/Type");
        assert_eq!(type2, "new/Type");
    }
}
