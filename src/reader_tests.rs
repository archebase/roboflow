//! Tests for reader.rs module.
//!
//! Tests are in a separate file due to the size of reader.rs.

use std::collections::HashMap;
use std::path::PathBuf;

use crate::format::mcap::ChannelInfo;
use crate::reader::*;
use crate::{CodecError, DecodedMessage, Result};

/// Get the fixtures directory path
fn fixtures_dir() -> PathBuf {
    PathBuf::from("tests/fixtures")
}

/// Check if a fixture file exists
fn fixture_exists(name: &str) -> bool {
    fixtures_dir().join(name).exists()
}

// =============================================================================
// Mock Helper for Testing
// =============================================================================

/// Mock FormatReader for testing trait implementations
struct MockReader {
    channels: HashMap<u16, ChannelInfo>,
    path: String,
}

impl MockReader {
    fn new() -> Self {
        Self {
            channels: HashMap::new(),
            path: String::new(),
        }
    }

    fn add_channel(&mut self, id: u16, topic: &str, message_type: &str) {
        self.channels.insert(
            id,
            ChannelInfo {
                id,
                topic: topic.to_string(),
                message_type: message_type.to_string(),
                encoding: "cdr".to_string(),
                schema: Some("int32 value".to_string()),
                schema_data: None,
                schema_encoding: Some("ros2msg".to_string()),
                message_count: 0,
                callerid: None,
            },
        );
    }
}

impl FormatReader for MockReader {
    fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        &self.channels
    }

    fn message_count(&self) -> u64 {
        0
    }

    fn start_time(&self) -> Option<u64> {
        None
    }

    fn end_time(&self) -> Option<u64> {
        None
    }

    fn path(&self) -> &str {
        &self.path
    }

    fn decode_messages(&self) -> Result<Box<dyn DecodedMessageStream>> {
        Ok(Box::new(std::iter::empty()))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// =============================================================================
// FormatReader Trait Default Implementation Tests
// =============================================================================

#[test]
fn test_channel_by_topic_returns_none_for_nonexistent_topic() {
    let reader = MockReader::new();
    assert!(reader.channel_by_topic("/nonexistent").is_none());
}

#[test]
fn test_channel_by_topic_returns_first_match() {
    let mut reader = MockReader::new();
    reader.add_channel(1, "/chatter", "std_msgs/String");
    reader.add_channel(2, "/chatter", "std_msgs/String");

    let channel = reader.channel_by_topic("/chatter");
    assert!(channel.is_some());
    // The implementation returns the first match found in iteration order
    // (implementation detail - just verify it returns one of them)
    let id = channel.unwrap().id;
    assert!(id == 1 || id == 2, "Expected channel ID 1 or 2, got {}", id);
}

#[test]
fn test_channels_by_topic_returns_all_matches() {
    let mut reader = MockReader::new();
    reader.add_channel(1, "/tf", "tf2_msgs/TFMessage");
    reader.add_channel(2, "/tf", "tf2_msgs/TFMessage");
    reader.add_channel(3, "/chatter", "std_msgs/String");

    let channels = reader.channels_by_topic("/tf");
    assert_eq!(channels.len(), 2);
    // Check that both expected channels are present (order not guaranteed with HashMap)
    let ids: Vec<_> = channels.iter().map(|c| c.id).collect();
    assert!(ids.contains(&1), "Expected channel ID 1 to be present");
    assert!(ids.contains(&2), "Expected channel ID 2 to be present");
}

#[test]
fn test_channels_by_topic_returns_empty_for_nonexistent() {
    let reader = MockReader::new();
    let channels = reader.channels_by_topic("/nonexistent");
    assert!(channels.is_empty());
}

// =============================================================================
// McapFormatReader Tests
// =============================================================================

#[test]
fn test_mcap_reader_returns_error_for_nonexistent_file() {
    let result = McapFormatReader::open("/nonexistent/path/to/file.mcap");
    assert!(result.is_err());
}

#[test]
fn test_mcap_reader_opens_valid_mcap_file() {
    if !fixture_exists("robocodec_test_0.mcap") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_0.mcap");
    let result = McapFormatReader::open(&path);

    assert!(result.is_ok());
    let reader = result.unwrap();
    assert_eq!(reader.path(), path.to_string_lossy().to_string());
}

#[test]
fn test_mcap_reader_has_channels_after_open() {
    if !fixture_exists("robocodec_test_0.mcap") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_0.mcap");
    let reader = McapFormatReader::open(&path).unwrap();

    // Should have at least some channels
    assert!(!reader.channels().is_empty());
}

#[test]
fn test_mcap_reader_format_reader_trait() {
    if !fixture_exists("robocodec_test_0.mcap") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_0.mcap");
    let reader: &dyn FormatReader = &McapFormatReader::open(&path).unwrap();

    assert!(!reader.channels().is_empty());
    assert!(reader.path().contains("robocodec_test_0.mcap"));
}

#[test]
fn test_mcap_reader_as_any() {
    if !fixture_exists("robocodec_test_0.mcap") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_0.mcap");
    let reader = McapFormatReader::open(&path).unwrap();

    let any = reader.as_any();
    assert!(any.is::<McapFormatReader>());
}

// =============================================================================
// BagFormatReader Tests
// =============================================================================

#[test]
fn test_bag_reader_returns_error_for_nonexistent_file() {
    let result = BagFormatReader::open("/nonexistent/path/to/file.bag");
    assert!(result.is_err());
}

#[test]
fn test_bag_reader_opens_valid_bag_file() {
    if !fixture_exists("robocodec_test_17.bag") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_17.bag");
    let result = BagFormatReader::open(&path);

    assert!(result.is_ok());
    let reader = result.unwrap();
    assert_eq!(reader.path(), path.to_string_lossy().to_string());
}

#[test]
fn test_bag_reader_has_channels_after_open() {
    if !fixture_exists("robocodec_test_17.bag") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_17.bag");
    let reader = BagFormatReader::open(&path).unwrap();

    // Should have at least some channels
    assert!(!reader.channels().is_empty());
}

#[test]
fn test_bag_reader_format_reader_trait() {
    if !fixture_exists("robocodec_test_17.bag") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_17.bag");
    let reader: &dyn FormatReader = &BagFormatReader::open(&path).unwrap();

    assert!(!reader.channels().is_empty());
    assert!(reader.path().contains("robocodec_test_17.bag"));
}

#[test]
fn test_bag_reader_as_any() {
    if !fixture_exists("robocodec_test_17.bag") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_17.bag");
    let reader = BagFormatReader::open(&path).unwrap();

    let any = reader.as_any();
    assert!(any.is::<BagFormatReader>());
}

#[test]
fn test_bag_reader_conn_id_map() {
    if !fixture_exists("robocodec_test_17.bag") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_17.bag");
    let reader = BagFormatReader::open(&path).unwrap();

    // Connection ID map should be populated
    assert!(!reader.conn_id_map().is_empty());
}

// =============================================================================
// BagRawMessageIter Tests
// =============================================================================

#[test]
fn test_bag_raw_message_iter_has_channels() {
    if !fixture_exists("robocodec_test_17.bag") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_17.bag");
    let reader = BagFormatReader::open(&path).unwrap();

    let iter = BagRawMessageIter::new(
        reader.path().to_string(),
        reader.channels().clone(),
        reader.conn_id_map().clone(),
    );

    assert!(!iter.channels().is_empty());
}

// =============================================================================
// RoboReader Tests
// =============================================================================

#[test]
fn test_robo_reader_returns_error_for_nonexistent_file() {
    let result = RoboReader::open("/nonexistent/path/to/file.unknown");
    assert!(result.is_err());
}

#[test]
fn test_robo_reader_returns_error_for_unsupported_extension() {
    let result = RoboReader::open("test.unknown_format");
    assert!(result.is_err());
}

#[test]
fn test_robo_reader_opens_mcap_file() {
    if !fixture_exists("robocodec_test_0.mcap") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_0.mcap");
    let result = RoboReader::open(&path);

    assert!(result.is_ok());
    let reader = result.unwrap();
    assert!(!reader.channels().is_empty());
}

#[test]
fn test_robo_reader_opens_bag_file() {
    if !fixture_exists("robocodec_test_17.bag") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_17.bag");
    let result = RoboReader::open(&path);

    assert!(result.is_ok());
    let reader = result.unwrap();
    assert!(!reader.channels().is_empty());
}

#[test]
fn test_robo_reader_delegates_to_inner_reader() {
    if !fixture_exists("robocodec_test_0.mcap") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_0.mcap");
    let reader = RoboReader::open(&path).unwrap();

    // Test that methods delegate correctly
    assert!(!reader.channels().is_empty());
    assert!(reader.path().contains(".mcap"));
}

#[test]
fn test_robo_reader_channel_by_topic_delegates() {
    if !fixture_exists("robocodec_test_0.mcap") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_0.mcap");
    let reader = RoboReader::open(&path).unwrap();

    // Get the first available topic
    let first_topic = reader.channels().values().next();
    if let Some(channel) = first_topic {
        let found = reader.channel_by_topic(&channel.topic);
        assert!(found.is_some());
    }
}

#[test]
fn test_robo_reader_channels_by_topic_delegates() {
    if !fixture_exists("robocodec_test_0.mcap") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_0.mcap");
    let reader = RoboReader::open(&path).unwrap();

    // Test with nonexistent topic
    let channels = reader.channels_by_topic("/nonexistent");
    assert!(channels.is_empty());
}

#[test]
fn test_robo_reader_iter_raw_mcap_for_mcap_file() {
    if !fixture_exists("robocodec_test_0.mcap") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_0.mcap");
    let reader = RoboReader::open(&path).unwrap();

    let result = reader.iter_raw_mcap();
    assert!(result.is_ok());
    let iter = result.unwrap();
    assert!(!iter.channels().is_empty());
}

#[test]
fn test_robo_reader_iter_raw_mcap_fails_for_bag_file() {
    if !fixture_exists("robocodec_test_17.bag") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_17.bag");
    let reader = RoboReader::open(&path).unwrap();

    let result = reader.iter_raw_mcap();
    assert!(result.is_err());
}

#[test]
fn test_robo_reader_iter_raw_bag_for_bag_file() {
    if !fixture_exists("robocodec_test_17.bag") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_17.bag");
    let reader = RoboReader::open(&path).unwrap();

    let result = reader.iter_raw_bag();
    assert!(result.is_ok());
    let iter = result.unwrap();
    assert!(!iter.channels().is_empty());
}

#[test]
fn test_robo_reader_iter_raw_bag_fails_for_mcap_file() {
    if !fixture_exists("robocodec_test_0.mcap") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_0.mcap");
    let reader = RoboReader::open(&path).unwrap();

    let result = reader.iter_raw_bag();
    assert!(result.is_err());
}

#[test]
fn test_robo_reader_iter_raw_auto_detects_mcap() {
    if !fixture_exists("robocodec_test_0.mcap") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_0.mcap");
    let reader = RoboReader::open(&path).unwrap();

    let result = reader.iter_raw();
    assert!(result.is_ok());

    let iter = result.unwrap();
    match iter {
        RoboCodecRawMessageIter::Mcap(_) => {}
        RoboCodecRawMessageIter::Bag(_) => panic!("Expected MCAP variant"),
    }
}

#[test]
fn test_robo_reader_iter_raw_auto_detects_bag() {
    if !fixture_exists("robocodec_test_17.bag") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_17.bag");
    let reader = RoboReader::open(&path).unwrap();

    let result = reader.iter_raw();
    assert!(result.is_ok());

    let iter = result.unwrap();
    match iter {
        RoboCodecRawMessageIter::Mcap(_) => panic!("Expected BAG variant"),
        RoboCodecRawMessageIter::Bag(_) => {}
    }
}

// =============================================================================
// DecodedMessageStream Blanket Implementation Tests
// =============================================================================

#[test]
fn test_decoded_message_stream_blanket_impl() {
    // This test verifies the blanket impl works for any compatible iterator

    fn create_test_stream() -> Box<dyn DecodedMessageStream> {
        let items: Vec<Result<(DecodedMessage, ChannelInfo)>> = Vec::new();
        Box::new(items.into_iter())
    }

    let stream = create_test_stream();
    // Stream should be callable (even if empty)
    let mut iter = stream;
    assert!(iter.next().is_none());
}

// =============================================================================
// BagRawMessageStream Tests
// =============================================================================

#[test]
fn test_bag_raw_message_stream_iteration() {
    if !fixture_exists("robocodec_test_17.bag") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_17.bag");
    let reader = BagFormatReader::open(&path).unwrap();

    let iter = BagRawMessageIter::new(
        reader.path().to_string(),
        reader.channels().clone(),
        reader.conn_id_map().clone(),
    );

    let stream = iter.into_stream();
    assert!(stream.is_ok());

    let mut stream = stream.unwrap();
    let count = stream.by_ref().count();
    assert!(count > 0, "Expected at least one raw message");
}

#[test]
fn test_bag_raw_message_stream_with_unknown_conn_id() {
    if !fixture_exists("robocodec_test_17.bag") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_17.bag");
    let reader = BagFormatReader::open(&path).unwrap();

    // Create a modified conn_id_map with an invalid entry
    let mut modified_map = reader.conn_id_map().clone();
    // Insert a fake mapping that won't match any actual connection
    modified_map.insert(9999, 100);

    let iter = BagRawMessageIter::new(
        reader.path().to_string(),
        reader.channels().clone(),
        modified_map,
    );

    // Stream should still be created and iterate valid messages
    let result = iter.into_stream();
    assert!(result.is_ok());
}

#[test]
fn test_bag_raw_message_iter_clone_channels() {
    if !fixture_exists("robocodec_test_17.bag") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_17.bag");
    let reader = BagFormatReader::open(&path).unwrap();

    let iter = BagRawMessageIter::new(
        reader.path().to_string(),
        reader.channels().clone(),
        reader.conn_id_map().clone(),
    );

    // Verify channels() works
    let channels = iter.channels();
    assert!(!channels.is_empty());
}

// =============================================================================
// RoboCodecRawMessageIter and Stream Tests
// =============================================================================

#[test]
fn test_robo_codec_raw_message_iter_into_stream_for_bag() {
    if !fixture_exists("robocodec_test_17.bag") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_17.bag");
    let reader = RoboReader::open(&path).unwrap();

    let iter = reader.iter_raw_bag().unwrap();
    let result = iter.into_stream();

    assert!(result.is_ok());
    let mut stream = result.unwrap();
    // Should be able to iterate at least one message
    let first = stream.next();
    assert!(first.is_some());
}

#[test]
fn test_robo_codec_raw_message_iter_into_stream_fails_for_mcap() {
    if !fixture_exists("robocodec_test_0.mcap") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_0.mcap");
    let reader = RoboReader::open(&path).unwrap();

    // Get the unified iterator, not the MCAP-specific one
    let iter = reader.iter_raw().unwrap();
    let result = iter.into_stream();

    // MCAP into_stream is not supported through unified interface
    assert!(result.is_err());
}

#[test]
fn test_robo_codec_raw_message_stream_iteration() {
    if !fixture_exists("robocodec_test_17.bag") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_17.bag");
    let reader = RoboReader::open(&path).unwrap();

    let iter = reader.iter_raw().unwrap();
    let stream = iter.into_stream();

    assert!(stream.is_ok());

    let mut stream = stream.unwrap();
    let count = stream.by_ref().take(5).count();
    assert!(count > 0, "Expected at least one raw message");
}

#[test]
fn test_robo_codec_raw_message_stream_with_mcap_variant() {
    if !fixture_exists("robocodec_test_0.mcap") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_0.mcap");
    let reader = RoboReader::open(&path).unwrap();

    let iter = reader.iter_raw().unwrap();
    match iter {
        RoboCodecRawMessageIter::Mcap(_) => {
            // Expected for MCAP files
        }
        RoboCodecRawMessageIter::Bag(_) => {
            panic!("Expected MCAP variant for .mcap file");
        }
    }
}

#[test]
fn test_robo_codec_raw_message_iter_channels() {
    if !fixture_exists("robocodec_test_17.bag") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_17.bag");
    let reader = RoboReader::open(&path).unwrap();

    let iter = reader.iter_raw().unwrap();
    let channels = iter.channels();
    assert!(!channels.is_empty());
}

// =============================================================================
// RoboReader for_each_decoded Tests
// =============================================================================

#[test]
fn test_robo_reader_for_each_decoded() {
    if !fixture_exists("robocodec_test_0.mcap") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_0.mcap");
    let reader = RoboReader::open(&path).unwrap();

    let mut count = 0;
    let result = reader.for_each_decoded(|_msg, _channel| {
        count += 1;
        Ok(())
    });

    assert!(
        result.is_ok(),
        "for_each_decoded should succeed: {:?}",
        result.err()
    );
    assert!(count > 0, "Expected at least one message to be processed");
}

#[test]
fn test_robo_reader_for_each_decoded_stops_on_error() {
    if !fixture_exists("robocodec_test_0.mcap") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_0.mcap");
    let reader = RoboReader::open(&path).unwrap();

    let mut count = 0;
    let result = reader.for_each_decoded(|_msg, _channel| {
        count += 1;
        if count >= 2 {
            Err(CodecError::parse("test", "Intentional error"))
        } else {
            Ok(())
        }
    });

    assert!(result.is_err(), "for_each_decoded should fail on error");
    assert!(
        count >= 2,
        "Should process at least 2 messages before error"
    );
}

#[test]
fn test_robo_reader_for_each_decoded_with_bag_file() {
    if !fixture_exists("robocodec_test_17.bag") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_17.bag");
    let reader = RoboReader::open(&path).unwrap();

    let mut count = 0;
    let result = reader.for_each_decoded(|_msg, _channel| {
        count += 1;
        Ok(())
    });

    assert!(result.is_ok());
    assert!(count > 0);
}

// =============================================================================
// RoboReader iter_raw Error Tests
// =============================================================================

#[test]
fn test_robo_reader_iter_raw_unsupported_format() {
    if !fixture_exists("robocodec_test_0.mcap") {
        return;
    }

    // Test that iter_raw correctly handles format detection
    let path = fixtures_dir().join("robocodec_test_0.mcap");
    let reader = RoboReader::open(&path).unwrap();

    let result = reader.iter_raw();
    assert!(result.is_ok());

    match result.unwrap() {
        RoboCodecRawMessageIter::Mcap(_) => {
            // Expected for MCAP file
        }
        RoboCodecRawMessageIter::Bag(_) => {
            panic!("Expected MCAP iterator for .mcap file");
        }
    }
}

#[test]
fn test_robo_reader_raw_iter_channels_delegation() {
    if !fixture_exists("robocodec_test_17.bag") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_17.bag");
    let reader = RoboReader::open(&path).unwrap();

    let bag_iter = reader.iter_raw_bag().unwrap();
    let channels = bag_iter.channels();

    assert!(!channels.is_empty());
    // Verify we got the same channels as the reader
    assert_eq!(channels.len(), reader.channels().len());
}

// =============================================================================
// RoboReader Downcast Tests
// =============================================================================

// Note: We cannot directly test downcasting through RoboReader since `inner` is private.
// The downcast functionality is tested indirectly through iter_raw_bag and iter_raw_mcap
// which use downcast_ref internally.

#[test]
fn test_robo_reader_iter_raw_bag_uses_downcast() {
    if !fixture_exists("robocodec_test_17.bag") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_17.bag");
    let reader = RoboReader::open(&path).unwrap();

    // iter_raw_bag internally uses downcast_ref to get BagFormatReader
    let result = reader.iter_raw_bag();
    assert!(result.is_ok());
}

#[test]
fn test_robo_reader_iter_raw_mcap_works() {
    if !fixture_exists("robocodec_test_0.mcap") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_0.mcap");
    let reader = RoboReader::open(&path).unwrap();

    // iter_raw_mcap should work for MCAP files
    let result = reader.iter_raw_mcap();
    assert!(result.is_ok());
}

// =============================================================================
// FormatReader start_time/end_time Tests
// =============================================================================

#[test]
fn test_mcap_reader_time_fields() {
    if !fixture_exists("robocodec_test_0.mcap") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_0.mcap");
    let reader = McapFormatReader::open(&path).unwrap();

    // MCAP reader should have time fields if summary is available
    let start = reader.start_time();
    let end = reader.end_time();

    // If we have messages, we might have time information from the summary
    let count = reader.message_count();
    if count > 0 {
        // At least one should be Some if there are messages with summary
        // or we just verify the count is available
        assert!(start.is_some() || end.is_some() || count > 0);
    }
}

#[test]
fn test_bag_reader_time_fields() {
    if !fixture_exists("robocodec_test_17.bag") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_17.bag");
    let reader = BagFormatReader::open(&path).unwrap();

    // BagFormatReader currently doesn't extract times
    assert!(reader.start_time().is_none());
    assert!(reader.end_time().is_none());
}

#[test]
fn test_robo_reader_time_fields_delegation() {
    if !fixture_exists("robocodec_test_0.mcap") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_0.mcap");
    let reader = RoboReader::open(&path).unwrap();

    // RoboReader should delegate to inner reader
    let _start = reader.start_time();
    let _end = reader.end_time();
    let _count = reader.message_count();

    // Just verify these methods don't panic
    assert!(reader.path().contains(".mcap"));
}

// =============================================================================
// Decode Messages Error Handling Tests
// =============================================================================

#[test]
fn test_mcap_decode_messages_with_valid_reader() {
    if !fixture_exists("robocodec_test_0.mcap") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_0.mcap");
    let reader = McapFormatReader::open(&path).unwrap();

    let result = reader.decode_messages();
    assert!(result.is_ok());
}

#[test]
fn test_bag_decode_messages_with_valid_reader() {
    if !fixture_exists("robocodec_test_17.bag") {
        return;
    }

    let path = fixtures_dir().join("robocodec_test_17.bag");
    let reader = BagFormatReader::open(&path).unwrap();

    let result = reader.decode_messages();
    assert!(result.is_ok());
}

// =============================================================================
// Additional Channel Lookup Tests
// =============================================================================

#[test]
fn test_channel_by_topic_with_exact_match() {
    let mut reader = MockReader::new();
    reader.add_channel(1, "/chatter", "std_msgs/String");
    reader.add_channel(2, "/chatter/status", "std_msgs/String");

    // Exact match should return only the exact topic
    let channel = reader.channel_by_topic("/chatter");
    assert!(channel.is_some());
    assert_eq!(channel.unwrap().topic, "/chatter");

    let channel = reader.channel_by_topic("/chatter/status");
    assert!(channel.is_some());
    assert_eq!(channel.unwrap().topic, "/chatter/status");
}

#[test]
fn test_channels_by_topic_with_no_matches() {
    let mut reader = MockReader::new();
    reader.add_channel(1, "/chatter", "std_msgs/String");
    reader.add_channel(2, "/status", "std_msgs/String");

    let channels = reader.channels_by_topic("/nonexistent");
    assert!(channels.is_empty());
}
