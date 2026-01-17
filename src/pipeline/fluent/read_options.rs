//! Read options for the fluent pipeline API.
//!
//! Provides filtering and configuration for input file reading.

use crate::io::filter::TopicFilter;

/// Read options for configuring input file processing.
///
/// Use the builder pattern to configure filtering options.
///
/// # Examples
///
/// ```no_run
/// use robocodec::pipeline::fluent::ReadOptions;
/// use robocodec::io::filter::TopicFilter;
///
/// let options = ReadOptions::new()
///     .topic_filter(TopicFilter::include(vec!["/camera".into()]))
///     .time_range(1000000000, 2000000000)
///     .message_limit(10000);
/// ```
#[derive(Debug, Clone, Default)]
pub struct ReadOptions {
    /// Topic filter for selecting which topics to process.
    pub topic_filter: Option<TopicFilter>,
    /// Time range filter (start_ns, end_ns).
    pub time_range: Option<(u64, u64)>,
    /// Specific channel IDs to include.
    pub channel_ids: Option<Vec<u16>>,
    /// Maximum number of messages to read.
    pub message_limit: Option<u64>,
}

impl ReadOptions {
    /// Create a new read options builder with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the topic filter.
    ///
    /// # Arguments
    ///
    /// * `filter` - Topic filter to apply (Include, Exclude, Regex, etc.)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use robocodec::pipeline::fluent::ReadOptions;
    /// use robocodec::io::filter::TopicFilter;
    ///
    /// // Include specific topics
    /// let _opts = ReadOptions::new()
    ///     .topic_filter(TopicFilter::include(vec!["/camera".into(), "/lidar".into()]));
    ///
    /// // Exclude topics
    /// let _opts = ReadOptions::new()
    ///     .topic_filter(TopicFilter::exclude(vec!["/tf".into()]));
    ///
    /// // Regex pattern
    /// let _opts = ReadOptions::new()
    ///     .topic_filter(TopicFilter::regex_include("/camera/.*").unwrap());
    /// ```
    pub fn topic_filter(mut self, filter: TopicFilter) -> Self {
        self.topic_filter = Some(filter);
        self
    }

    /// Set the time range filter.
    ///
    /// Only messages with timestamps within this range (inclusive) will be processed.
    ///
    /// # Arguments
    ///
    /// * `start_ns` - Start timestamp in nanoseconds
    /// * `end_ns` - End timestamp in nanoseconds
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use robocodec::pipeline::fluent::ReadOptions;
    ///
    /// // Read messages from 1 second to 5 seconds
    /// let _opts = ReadOptions::new()
    ///     .time_range(1_000_000_000, 5_000_000_000);
    /// ```
    pub fn time_range(mut self, start_ns: u64, end_ns: u64) -> Self {
        self.time_range = Some((start_ns, end_ns));
        self
    }

    /// Set specific channel IDs to include.
    ///
    /// Only messages from these channels will be processed.
    ///
    /// # Arguments
    ///
    /// * `ids` - List of channel IDs to include
    pub fn channel_ids(mut self, ids: Vec<u16>) -> Self {
        self.channel_ids = Some(ids);
        self
    }

    /// Set the maximum number of messages to read.
    ///
    /// Processing stops after this many messages have been read.
    ///
    /// # Arguments
    ///
    /// * `limit` - Maximum number of messages
    pub fn message_limit(mut self, limit: u64) -> Self {
        self.message_limit = Some(limit);
        self
    }

    /// Check if any filtering is configured.
    pub fn has_filters(&self) -> bool {
        self.topic_filter.is_some()
            || self.time_range.is_some()
            || self.channel_ids.is_some()
            || self.message_limit.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default() {
        let opts = ReadOptions::default();
        assert!(opts.topic_filter.is_none());
        assert!(opts.time_range.is_none());
        assert!(opts.channel_ids.is_none());
        assert!(opts.message_limit.is_none());
        assert!(!opts.has_filters());
    }

    #[test]
    fn test_builder_chain() {
        let opts = ReadOptions::new()
            .topic_filter(TopicFilter::include(vec!["/camera".into()]))
            .time_range(1000, 2000)
            .channel_ids(vec![1, 2, 3])
            .message_limit(100);

        assert!(opts.topic_filter.is_some());
        assert_eq!(opts.time_range, Some((1000, 2000)));
        assert_eq!(opts.channel_ids, Some(vec![1, 2, 3]));
        assert_eq!(opts.message_limit, Some(100));
        assert!(opts.has_filters());
    }

    #[test]
    fn test_partial_config() {
        let opts = ReadOptions::new().message_limit(500);
        assert!(opts.has_filters());
        assert!(opts.topic_filter.is_none());
        assert_eq!(opts.message_limit, Some(500));
    }
}
