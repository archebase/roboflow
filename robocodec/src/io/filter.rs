// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Topic and connection filtering for parallel readers.
//!
//! This module provides filtering capabilities for parallel readers,
//! allowing efficient selection of specific topics/channels during
//! concurrent chunk processing.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use crate::io::metadata::ChannelInfo;

/// Filter for selecting topics/connections during parallel reading.
#[derive(Clone, Default)]
pub enum TopicFilter {
    /// Read all topics (no filtering)
    #[default]
    All,
    /// Read only specific topics
    Include(Vec<String>),
    /// Exclude specific topics
    Exclude(Vec<String>),
    /// Include topics matching regex pattern
    RegexInclude(Arc<regex::Regex>),
    /// Exclude topics matching regex pattern
    RegexExclude(Arc<regex::Regex>),
    /// Custom filter function
    Custom(Arc<dyn Fn(&str) -> bool + Send + Sync>),
}

impl fmt::Debug for TopicFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::All => f.debug_tuple("All").finish(),
            Self::Include(v) => f.debug_tuple("Include").field(v).finish(),
            Self::Exclude(v) => f.debug_tuple("Exclude").field(v).finish(),
            Self::RegexInclude(_) => f.debug_tuple("RegexInclude").field(&"<regex>").finish(),
            Self::RegexExclude(_) => f.debug_tuple("RegexExclude").field(&"<regex>").finish(),
            Self::Custom(_) => f.debug_tuple("Custom").field(&"<fn>").finish(),
        }
    }
}

impl TopicFilter {
    /// Check if a topic should be included.
    pub fn should_include(&self, topic: &str) -> bool {
        match self {
            TopicFilter::All => true,
            TopicFilter::Include(topics) => topics.contains(&topic.to_string()),
            TopicFilter::Exclude(topics) => !topics.contains(&topic.to_string()),
            TopicFilter::RegexInclude(re) => re.is_match(topic),
            TopicFilter::RegexExclude(re) => !re.is_match(topic),
            TopicFilter::Custom(f) => f(topic),
        }
    }

    /// Create an include filter from topic names.
    pub fn include(topics: Vec<String>) -> Self {
        Self::Include(topics)
    }

    /// Create an exclude filter from topic names.
    pub fn exclude(topics: Vec<String>) -> Self {
        Self::Exclude(topics)
    }

    /// Create a regex include filter.
    pub fn regex_include(pattern: &str) -> Result<Self, regex::Error> {
        regex::Regex::new(pattern).map(|re| Self::RegexInclude(Arc::new(re)))
    }

    /// Create a regex exclude filter.
    pub fn regex_exclude(pattern: &str) -> Result<Self, regex::Error> {
        regex::Regex::new(pattern).map(|re| Self::RegexExclude(Arc::new(re)))
    }

    /// Create a custom filter from a function.
    pub fn custom<F>(f: F) -> Self
    where
        F: Fn(&str) -> bool + Send + Sync + 'static,
    {
        Self::Custom(Arc::new(f))
    }
}

/// Channel filter mapping topic names to channel IDs.
#[derive(Debug, Clone)]
pub struct ChannelFilter {
    /// Allowed channel IDs
    pub allowed_channels: HashSet<u16>,
    /// Topic to channel ID mapping
    pub topic_to_channels: HashMap<String, Vec<u16>>,
}

impl ChannelFilter {
    /// Create a channel filter from topic filter and channel info.
    pub fn from_topic_filter(filter: &TopicFilter, channels: &HashMap<u16, ChannelInfo>) -> Self {
        let mut allowed_channels = HashSet::new();
        let mut topic_to_channels: HashMap<String, Vec<u16>> = HashMap::new();

        for (&id, channel) in channels {
            if filter.should_include(&channel.topic) {
                allowed_channels.insert(id);
                topic_to_channels
                    .entry(channel.topic.clone())
                    .or_default()
                    .push(id);
            }
        }

        Self {
            allowed_channels,
            topic_to_channels,
        }
    }

    /// Create a filter that includes all channels.
    pub fn all(channels: &HashMap<u16, ChannelInfo>) -> Self {
        let mut allowed_channels = HashSet::new();
        let mut topic_to_channels: HashMap<String, Vec<u16>> = HashMap::new();

        for (&id, channel) in channels {
            allowed_channels.insert(id);
            topic_to_channels
                .entry(channel.topic.clone())
                .or_default()
                .push(id);
        }

        Self {
            allowed_channels,
            topic_to_channels,
        }
    }

    /// Check if a channel ID is allowed.
    pub fn allows_channel(&self, channel_id: u16) -> bool {
        self.allowed_channels.contains(&channel_id)
    }

    /// Get the number of allowed channels.
    pub fn channel_count(&self) -> usize {
        self.allowed_channels.len()
    }

    /// Get all channel IDs for a topic.
    pub fn channels_for_topic(&self, topic: &str) -> &[u16] {
        self.topic_to_channels
            .get(topic)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_filter_all() {
        let filter = TopicFilter::All;
        assert!(filter.should_include("/any_topic"));
        assert!(filter.should_include("/another_topic"));
    }

    #[test]
    fn test_topic_filter_include() {
        let filter = TopicFilter::include(vec!["/camera/image_raw".into(), "/lidar/points".into()]);
        assert!(filter.should_include("/camera/image_raw"));
        assert!(filter.should_include("/lidar/points"));
        assert!(!filter.should_include("/imu/data"));
    }

    #[test]
    fn test_topic_filter_exclude() {
        let filter = TopicFilter::exclude(vec!["/tf".into()]);
        assert!(!filter.should_include("/tf"));
        assert!(filter.should_include("/camera"));
    }

    #[test]
    fn test_topic_filter_regex() {
        let filter = TopicFilter::regex_include("/camera/.*").unwrap();
        assert!(filter.should_include("/camera/image_raw"));
        assert!(filter.should_include("/camera/info"));
        assert!(!filter.should_include("/lidar/points"));
    }

    #[test]
    fn test_channel_filter_from_topic_filter() {
        let mut channels = HashMap::new();
        channels.insert(0, ChannelInfo::new(0, "/camera", "sensor_msgs/Image"));
        channels.insert(1, ChannelInfo::new(1, "/lidar", "sensor_msgs/PointCloud2"));
        channels.insert(2, ChannelInfo::new(2, "/imu", "sensor_msgs/Imu"));

        let filter = TopicFilter::include(vec!["/camera".into()]);
        let channel_filter = ChannelFilter::from_topic_filter(&filter, &channels);

        assert!(channel_filter.allows_channel(0));
        assert!(!channel_filter.allows_channel(1));
        assert!(!channel_filter.allows_channel(2));
        assert_eq!(channel_filter.channel_count(), 1);
    }

    #[test]
    fn test_channel_filter_all() {
        let mut channels = HashMap::new();
        channels.insert(0, ChannelInfo::new(0, "/camera", "sensor_msgs/Image"));
        channels.insert(1, ChannelInfo::new(1, "/lidar", "sensor_msgs/PointCloud2"));

        let filter = ChannelFilter::all(&channels);
        assert!(filter.allows_channel(0));
        assert!(filter.allows_channel(1));
        assert_eq!(filter.channel_count(), 2);
    }
}
