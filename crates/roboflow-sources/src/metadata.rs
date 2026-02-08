// Source metadata types

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Metadata about a data source.
///
/// This provides information about the source file/stream, including
/// available topics, message types, and timing information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceMetadata {
    /// Type of the source (mcap, bag, hdf5, etc.)
    pub source_type: String,
    /// Path or URL to the source
    pub path: String,
    /// Total duration in nanoseconds (if known)
    pub duration_ns: Option<u64>,
    /// Start time in nanoseconds (if known)
    pub start_time_ns: Option<u64>,
    /// End time in nanoseconds (if known)
    pub end_time_ns: Option<u64>,
    /// Total message count (if known)
    pub message_count: Option<u64>,
    /// Topics available in the source
    pub topics: Vec<TopicMetadata>,
    /// Additional metadata
    pub metadata: HashMap<String, serde_json::Value>,
}

impl SourceMetadata {
    /// Create new source metadata.
    pub fn new(source_type: String, path: String) -> Self {
        Self {
            source_type,
            path,
            duration_ns: None,
            start_time_ns: None,
            end_time_ns: None,
            message_count: None,
            topics: Vec::new(),
            metadata: HashMap::new(),
        }
    }

    /// Add duration information.
    pub fn with_duration(mut self, start_ns: u64, end_ns: u64) -> Self {
        self.start_time_ns = Some(start_ns);
        self.end_time_ns = Some(end_ns);
        self.duration_ns = Some(end_ns.saturating_sub(start_ns));
        self
    }

    /// Add message count.
    pub fn with_message_count(mut self, count: u64) -> Self {
        self.message_count = Some(count);
        self
    }

    /// Add topics.
    pub fn with_topics(mut self, topics: Vec<TopicMetadata>) -> Self {
        self.topics = topics;
        self
    }

    /// Get topic metadata by name.
    pub fn topic(&self, name: &str) -> Option<&TopicMetadata> {
        self.topics.iter().find(|t| t.name == name)
    }

    /// Check if a topic exists.
    pub fn has_topic(&self, name: &str) -> bool {
        self.topic(name).is_some()
    }
}

/// Metadata about a specific topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMetadata {
    /// Topic name
    pub name: String,
    /// Message type name
    pub message_type: String,
    /// Message count for this topic
    pub message_count: Option<u64>,
    /// Frequency in Hz (if known)
    pub frequency_hz: Option<f64>,
    /// MD5 hash of the message type definition (ROS1)
    pub md5sum: Option<String>,
    /// Additional topic metadata
    pub metadata: HashMap<String, serde_json::Value>,
}

impl TopicMetadata {
    /// Create new topic metadata.
    pub fn new(name: String, message_type: String) -> Self {
        Self {
            name,
            message_type,
            message_count: None,
            frequency_hz: None,
            md5sum: None,
            metadata: HashMap::new(),
        }
    }

    /// Add message count.
    pub fn with_message_count(mut self, count: u64) -> Self {
        self.message_count = Some(count);
        self
    }

    /// Add frequency.
    pub fn with_frequency(mut self, hz: f64) -> Self {
        self.frequency_hz = Some(hz);
        self
    }

    /// Add MD5 sum.
    pub fn with_md5sum(mut self, md5sum: String) -> Self {
        self.md5sum = Some(md5sum);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_metadata_builder() {
        let metadata = SourceMetadata::new("mcap".to_string(), "test.mcap".to_string())
            .with_duration(0, 1_000_000_000)
            .with_message_count(1000);

        assert_eq!(metadata.source_type, "mcap");
        assert_eq!(metadata.path, "test.mcap");
        assert_eq!(metadata.duration_ns, Some(1_000_000_000));
        assert_eq!(metadata.message_count, Some(1000));
    }

    #[test]
    fn test_topic_metadata_builder() {
        let topic = TopicMetadata::new("/camera".to_string(), "sensor_msgs/Image".to_string())
            .with_message_count(500)
            .with_frequency(30.0);

        assert_eq!(topic.name, "/camera");
        assert_eq!(topic.message_type, "sensor_msgs/Image");
        assert_eq!(topic.message_count, Some(500));
        assert_eq!(topic.frequency_hz, Some(30.0));
    }

    #[test]
    fn test_topic_lookup() {
        let topics = vec![
            TopicMetadata::new("/camera".to_string(), "sensor_msgs/Image".to_string()),
            TopicMetadata::new("/lidar".to_string(), "sensor_msgs/PointCloud2".to_string()),
        ];

        let metadata =
            SourceMetadata::new("mcap".to_string(), "test.mcap".to_string()).with_topics(topics);

        assert!(metadata.has_topic("/camera"));
        assert!(metadata.has_topic("/lidar"));
        assert!(!metadata.has_topic("/imu"));

        let camera_topic = metadata.topic("/camera").unwrap();
        assert_eq!(camera_topic.message_type, "sensor_msgs/Image");
    }
}
