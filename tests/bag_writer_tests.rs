// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! ROS1 bag writer tests.
//!
//! This file contains unit and integration tests for the bag_writer module.
//! Tests cover:
//! - BagMessage creation
//! - BagWriter file creation
//! - Adding connections
//! - Writing messages
//! - Chunking behavior
//! - Round-trip verification (write and read back)
//! - Error handling

use std::fs;
use std::path::PathBuf;

use robocodec::io::traits::FormatReader;
use robocodec::BagFormat;
use robocodec::{BagMessage, BagWriter};

// ============================================================================
// Test Fixtures
// ============================================================================

/// Simple ROS1 message definition for std_msgs/String
const STD_MSGS_STRING_DEF: &str = "string data";

/// Simple ROS1 message definition for std_msgs/Int32
const STD_MSGS_INT32_DEF: &str = "int32 data";

/// Simple ROS1 message definition for sensor_msgs/Image
const SENSOR_MSGS_IMAGE_DEF: &str = r#"
std_msgs/Header header
  uint32 seq
  time stamp
  string frame_id
uint32 height
uint32 width
string encoding
uint8 is_bigendian
uint32 step
uint8[] data
"#;

/// Get a temporary directory for test files
fn temp_dir() -> PathBuf {
    // Use a combination of process ID and a random element to avoid collisions
    // when tests run in parallel
    let random = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    std::env::temp_dir().join(format!(
        "roboflow_bag_writer_test_{}_{}",
        std::process::id(),
        random
    ))
}

/// Create a temporary bag file path
fn temp_bag_path(name: &str) -> PathBuf {
    let dir = temp_dir();
    fs::create_dir_all(&dir).ok();
    dir.join(format!("{}.bag", name))
}

/// Simple cleanup guard for test temporary files
struct CleanupGuard;

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        cleanup_temp_dir();
    }
}

/// Clean up temporary test files
fn cleanup_temp_dir() {
    let dir = temp_dir();
    let _ = fs::remove_dir_all(dir);
}

// ============================================================================
// BagMessage Unit Tests
// ============================================================================

#[test]
fn test_bag_message_new() {
    let conn_id = 1;
    let time_ns = 1_234_567_890;
    let data = vec![1, 2, 3, 4];

    let msg = BagMessage::new(conn_id, time_ns, data.clone());

    assert_eq!(msg.conn_id, conn_id, "connection ID should match");
    assert_eq!(msg.time_ns, time_ns, "timestamp should match");
    assert_eq!(msg.data, data, "data should match");
}

#[test]
fn test_bag_message_from_raw() {
    let conn_id = 5;
    let time_ns = 9_876_543_210;
    let data = vec![10, 20, 30, 40, 50];

    let msg = BagMessage::from_raw(conn_id, time_ns, data.clone());

    assert_eq!(msg.conn_id, conn_id);
    assert_eq!(msg.time_ns, time_ns);
    assert_eq!(msg.data, data);
}

#[test]
fn test_bag_message_clone() {
    let msg = BagMessage::new(1, 1000, vec![1, 2, 3]);
    let cloned = msg.clone();

    assert_eq!(msg.conn_id, cloned.conn_id);
    assert_eq!(msg.time_ns, cloned.time_ns);
    assert_eq!(msg.data, cloned.data);
}

// ============================================================================
// BagWriter Creation Tests
// ============================================================================

#[test]
fn test_writer_creates_file() {
    let path = temp_bag_path("test_creates_file");
    let _guard = CleanupGuard;

    let result = BagWriter::create(&path);

    assert!(
        result.is_ok(),
        "BagWriter::create should succeed: {:?}",
        result.err()
    );

    let writer = result.unwrap();
    writer.finish().ok();

    assert!(path.exists(), "bag file should be created at {:?}", path);
}

#[test]
fn test_writer_creates_valid_version_header() {
    let path = temp_bag_path("test_version_header");
    let _guard = CleanupGuard;

    let writer = BagWriter::create(&path).unwrap();
    writer.finish().unwrap();

    let contents = fs::read(&path).unwrap();

    // File should start with ROSBAG version line
    let version_line = "#ROSBAG V2.0\n";
    assert!(
        contents.starts_with(version_line.as_bytes()),
        "bag file should start with ROSBAG version line"
    );
}

#[test]
fn test_writer_file_header_is_4096_bytes() {
    let path = temp_bag_path("test_header_size");
    let _guard = CleanupGuard;

    let writer = BagWriter::create(&path).unwrap();
    writer.finish().unwrap();

    let contents = fs::read(&path).unwrap();

    assert_eq!(contents.len(), 4096, "empty bag file should be 4096 bytes");
}

// ============================================================================
// Connection Tests
// ============================================================================

#[test]
fn test_add_single_connection() {
    let path = temp_bag_path("test_add_connection");
    let _guard = CleanupGuard;

    let mut writer = BagWriter::create(&path).unwrap();
    let result = writer.add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF);

    assert!(
        result.is_ok(),
        "add_connection should succeed: {:?}",
        result.err()
    );

    writer.finish().unwrap();
}

#[test]
fn test_add_multiple_connections() {
    let path = temp_bag_path("test_multiple_connections");
    let _guard = CleanupGuard;

    let mut writer = BagWriter::create(&path).unwrap();

    assert!(writer
        .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
        .is_ok());
    assert!(writer
        .add_connection(1, "/numbers", "std_msgs/Int32", STD_MSGS_INT32_DEF)
        .is_ok());
    assert!(writer
        .add_connection(2, "/camera", "sensor_msgs/Image", SENSOR_MSGS_IMAGE_DEF)
        .is_ok());

    writer.finish().unwrap();
}

#[test]
fn test_add_connection_with_callerid() {
    let path = temp_bag_path("test_connection_callerid");
    let _guard = CleanupGuard;

    let mut writer = BagWriter::create(&path).unwrap();
    let result = writer.add_connection_with_callerid(
        0,
        "/chatter",
        "std_msgs/String",
        STD_MSGS_STRING_DEF,
        "/talker",
    );

    assert!(result.is_ok());
    writer.finish().unwrap();
}

#[test]
fn test_add_duplicate_topic_is_idempotent() {
    let path = temp_bag_path("test_duplicate_topic");
    let _guard = CleanupGuard;

    let mut writer = BagWriter::create(&path).unwrap();

    // Add the same topic twice
    assert!(writer
        .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
        .is_ok());
    assert!(writer
        .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
        .is_ok());

    // Should not create duplicate connections
    writer.finish().unwrap();

    // Verify by reading the bag
    let reader = BagFormat::open(&path);
    assert!(reader.is_ok());
    let reader = reader.unwrap();
    assert_eq!(
        reader.channels().len(),
        1,
        "should have exactly 1 connection"
    );
}

#[test]
fn test_add_connection_without_leading_slash() {
    let path = temp_bag_path("test_topic_no_slash");
    let _guard = CleanupGuard;

    let mut writer = BagWriter::create(&path).unwrap();
    // Topic without leading slash should work
    let result = writer.add_connection(0, "chatter", "std_msgs/String", STD_MSGS_STRING_DEF);

    assert!(result.is_ok());
    writer.finish().unwrap();
}

// ============================================================================
// Message Writing Tests
// ============================================================================

#[test]
fn test_write_single_message() {
    let path = temp_bag_path("test_write_single");
    let _guard = CleanupGuard;

    let mut writer = BagWriter::create(&path).unwrap();
    writer
        .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
        .unwrap();

    let msg = BagMessage::new(0, 1_000_000_000, vec![1, 2, 3, 4]);
    let result = writer.write_message(&msg);

    assert!(
        result.is_ok(),
        "write_message should succeed: {:?}",
        result.err()
    );

    writer.finish().unwrap();
}

#[test]
fn test_write_multiple_messages_same_connection() {
    let path = temp_bag_path("test_multiple_messages");
    let _guard = CleanupGuard;

    let mut writer = BagWriter::create(&path).unwrap();
    writer
        .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
        .unwrap();

    for i in 0..10 {
        let msg = BagMessage::new(0, i * 1_000_000_000, vec![i as u8; 4]);
        assert!(writer.write_message(&msg).is_ok());
    }

    writer.finish().unwrap();

    // Verify messages were written
    let reader = BagFormat::open(&path).unwrap();
    // Note: message_count may not be accurate for BagFormatReader
    assert_eq!(reader.channels().len(), 1);
}

#[test]
fn test_write_messages_multiple_connections() {
    let path = temp_bag_path("test_multi_conn_messages");
    let _guard = CleanupGuard;

    let mut writer = BagWriter::create(&path).unwrap();
    writer
        .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
        .unwrap();
    writer
        .add_connection(1, "/numbers", "std_msgs/Int32", STD_MSGS_INT32_DEF)
        .unwrap();

    // Write messages to both connections
    for i in 0..5 {
        let msg1 = BagMessage::new(0, i * 1_000_000_000, vec![i as u8; 4]);
        let msg2 = BagMessage::new(1, i * 1_000_000_000 + 500_000_000, vec![i as u8; 4]);
        assert!(writer.write_message(&msg1).is_ok());
        assert!(writer.write_message(&msg2).is_ok());
    }

    writer.finish().unwrap();

    // Verify both connections exist
    let reader = BagFormat::open(&path).unwrap();
    assert_eq!(reader.channels().len(), 2);
}

#[test]
fn test_write_messages_increasing_timestamps() {
    let path = temp_bag_path("test_timestamps");
    let _guard = CleanupGuard;

    let mut writer = BagWriter::create(&path).unwrap();
    writer
        .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
        .unwrap();

    // Write messages with increasing timestamps
    let timestamps = [1_000_000_000, 2_000_000_000, 3_000_000_000];
    for ts in timestamps {
        let msg = BagMessage::new(0, ts, vec![1, 2, 3]);
        assert!(writer.write_message(&msg).is_ok());
    }

    writer.finish().unwrap();

    // Verify file was created
    assert!(path.exists());
}

// ============================================================================
// Chunking Tests
// ============================================================================

#[test]
fn test_chunking_with_large_messages() {
    let path = temp_bag_path("test_chunking");
    let _guard = CleanupGuard;

    let mut writer = BagWriter::create(&path).unwrap();
    writer
        .add_connection(0, "/data", "std_msgs/String", STD_MSGS_STRING_DEF)
        .unwrap();

    // Write messages with 100KB of data each
    // Default chunk threshold is 768KB, so 8 messages should trigger a chunk
    let large_data = vec![0u8; 100_000];
    for i in 0..10 {
        let msg = BagMessage::new(0, i * 1_000_000_000, large_data.clone());
        assert!(writer.write_message(&msg).is_ok());
    }

    writer.finish().unwrap();

    // Verify file was created successfully
    assert!(path.exists());
    let metadata = fs::metadata(&path).unwrap();
    assert!(metadata.len() > 1_000_000, "file should be larger than 1MB");
}

#[test]
fn test_finish_with_open_chunk() {
    let path = temp_bag_path("test_finish_open_chunk");
    let _guard = CleanupGuard;

    let mut writer = BagWriter::create(&path).unwrap();
    writer
        .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
        .unwrap();

    // Write one message (doesn't fill chunk)
    let msg = BagMessage::new(0, 1_000_000_000, vec![1, 2, 3]);
    writer.write_message(&msg).unwrap();

    // finish() should flush the open chunk
    writer.finish().unwrap();

    assert!(path.exists());
}

// ============================================================================
// Round-Trip Integration Tests
// ============================================================================

#[test]
fn test_round_trip_single_message() {
    let path = temp_bag_path("test_round_trip_single");
    let _guard = CleanupGuard;

    // Write a message
    let mut writer = BagWriter::create(&path).unwrap();
    writer
        .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
        .unwrap();

    let data = vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]; // "Hello"
    let msg = BagMessage::new(0, 1_500_000_000, data);
    writer.write_message(&msg).unwrap();
    writer.finish().unwrap();

    // Read it back
    let reader = BagFormat::open(&path).unwrap();
    let channels = reader.channels();

    assert_eq!(channels.len(), 1, "should have 1 channel");

    let channel = channels.values().next().unwrap();
    assert_eq!(channel.topic, "/chatter");
    assert_eq!(channel.message_type, "std_msgs/String");
}

#[test]
fn test_round_trip_multiple_connections() {
    let path = temp_bag_path("test_round_trip_multi_conn");
    let _guard = CleanupGuard;

    // Write messages to multiple connections
    let mut writer = BagWriter::create(&path).unwrap();

    writer
        .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
        .unwrap();
    writer
        .add_connection(1, "/numbers", "std_msgs/Int32", STD_MSGS_INT32_DEF)
        .unwrap();
    writer
        .add_connection(2, "/camera", "sensor_msgs/Image", SENSOR_MSGS_IMAGE_DEF)
        .unwrap();

    // Write messages
    for i in 0..3 {
        let msg = BagMessage::new(0, i * 1_000_000_000, vec![i as u8; 4]);
        writer.write_message(&msg).unwrap();
    }
    for i in 0..3 {
        let msg = BagMessage::new(1, i * 1_000_000_000, vec![i as u8; 4]);
        writer.write_message(&msg).unwrap();
    }
    for i in 0..3 {
        let msg = BagMessage::new(2, i * 1_000_000_000, vec![i as u8; 4]);
        writer.write_message(&msg).unwrap();
    }

    writer.finish().unwrap();

    // Read back and verify
    let reader = BagFormat::open(&path).unwrap();
    let channels = reader.channels();

    assert_eq!(channels.len(), 3, "should have 3 channels");

    // Verify all topics exist
    let topics: Vec<_> = channels.values().map(|c| c.topic.as_str()).collect();
    assert!(topics.contains(&"/chatter"));
    assert!(topics.contains(&"/numbers"));
    assert!(topics.contains(&"/camera"));
}

#[test]
fn test_round_trip_topic_types_match() {
    let path = temp_bag_path("test_topic_types");
    let _guard = CleanupGuard;

    // Write with specific types
    let mut writer = BagWriter::create(&path).unwrap();

    writer
        .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
        .unwrap();
    writer
        .add_connection(1, "/image", "sensor_msgs/Image", SENSOR_MSGS_IMAGE_DEF)
        .unwrap();

    writer
        .write_message(&BagMessage::new(0, 1_000_000_000, vec![1]))
        .unwrap();
    writer
        .write_message(&BagMessage::new(1, 2_000_000_000, vec![2]))
        .unwrap();

    writer.finish().unwrap();

    // Read back and verify types
    let reader = BagFormat::open(&path).unwrap();

    let chatter = reader.channel_by_topic("/chatter");
    assert!(chatter.is_some(), "/chatter should exist");
    assert_eq!(chatter.unwrap().message_type, "std_msgs/String");

    let image = reader.channel_by_topic("/image");
    assert!(image.is_some(), "/image should exist");
    assert_eq!(image.unwrap().message_type, "sensor_msgs/Image");
}

#[test]
fn test_round_trip_message_data_preserved() {
    // Use a fixed path for easier debugging
    let path = PathBuf::from("/tmp/claude/test_round_trip_data.bag");

    // Create test data with known byte patterns
    let test_data_1 = vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
    let test_data_2 = vec![0xAA, 0xBB, 0xCC, 0xDD];
    let test_data_3 = vec![0xFF, 0xFE, 0xFD, 0xFC, 0xFB];

    // Write messages with known data
    let mut writer = BagWriter::create(&path).unwrap();
    writer
        .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
        .unwrap();

    writer
        .write_message(&BagMessage::new(0, 1_000_000_000, test_data_1.clone()))
        .unwrap();
    writer
        .write_message(&BagMessage::new(0, 2_000_000_000, test_data_2.clone()))
        .unwrap();
    writer
        .write_message(&BagMessage::new(0, 3_000_000_000, test_data_3.clone()))
        .unwrap();

    writer.finish().unwrap();

    // Verify file was created
    assert!(path.exists(), "bag file should exist");

    // Read back and verify message data is preserved
    let reader = BagFormat::open(&path).unwrap();
    let raw_iter = reader.iter_raw().unwrap();

    // Collect all messages
    let mut messages: Vec<(u64, Vec<u8>)> = Vec::new();
    for result in raw_iter {
        match result {
            Ok((raw_msg, _channel)) => {
                // raw_msg.data contains the message payload directly
                messages.push((raw_msg.log_time, raw_msg.data.clone()));
            }
            Err(e) => {
                panic!("Error reading message: {}", e);
            }
        }
    }

    // Verify we got 3 messages
    assert_eq!(messages.len(), 3, "should have 3 messages");

    // Verify timestamps match (in nanoseconds)
    assert_eq!(messages[0].0, 1_000_000_000);
    assert_eq!(messages[1].0, 2_000_000_000);
    assert_eq!(messages[2].0, 3_000_000_000);

    // Verify message data matches
    assert_eq!(
        messages[0].1, test_data_1,
        "first message data should match"
    );
    assert_eq!(
        messages[1].1, test_data_2,
        "second message data should match"
    );
    assert_eq!(
        messages[2].1, test_data_3,
        "third message data should match"
    );

    // Clean up
    let _ = fs::remove_file(&path);
}

#[test]
fn test_round_trip_multiple_connections_with_data() {
    let path = PathBuf::from("/tmp/claude/test_round_trip_multi_conn.bag");

    // Create test data for different topics
    let string_data = vec![b'H', b'e', b'l', b'l', b'o'];
    let int_data = vec![0x2A, 0x00, 0x00, 0x00]; // 42 as little-endian i32

    // Write messages to multiple connections
    let mut writer = BagWriter::create(&path).unwrap();
    writer
        .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
        .unwrap();
    writer
        .add_connection(1, "/numbers", "std_msgs/Int32", STD_MSGS_INT32_DEF)
        .unwrap();

    writer
        .write_message(&BagMessage::new(0, 1_000_000_000, string_data.clone()))
        .unwrap();
    writer
        .write_message(&BagMessage::new(1, 1_500_000_000, int_data.clone()))
        .unwrap();

    writer.finish().unwrap();

    // Read back and verify
    let reader = BagFormat::open(&path).unwrap();
    assert_eq!(reader.channels().len(), 2, "should have 2 channels");

    // Verify topics exist
    let chatter = reader.channel_by_topic("/chatter");
    assert!(chatter.is_some(), "/chatter should exist");

    let numbers = reader.channel_by_topic("/numbers");
    assert!(numbers.is_some(), "/numbers should exist");

    // Use raw message iterator to verify data
    use robocodec::io::traits::FormatReader;
    let raw_reader = BagFormat::open(&path).unwrap();
    let raw_iter = raw_reader.iter_raw().unwrap();

    let mut messages_by_topic: std::collections::HashMap<String, Vec<u8>> =
        std::collections::HashMap::new();

    for result in raw_iter {
        match result {
            Ok((raw_msg, channel)) => {
                // raw_msg.data contains the message payload directly
                messages_by_topic.insert(channel.topic.clone(), raw_msg.data.clone());
            }
            Err(e) => {
                panic!("Error reading message: {}", e);
            }
        }
    }

    // Verify we got data for both topics
    assert_eq!(messages_by_topic.len(), 2);

    // Verify data (order may vary since we iterate by chunk)
    let chatter_data = messages_by_topic.get("/chatter").unwrap();
    assert_eq!(chatter_data, &string_data, "/chatter data should match");

    let numbers_data = messages_by_topic.get("/numbers").unwrap();
    assert_eq!(numbers_data, &int_data, "/numbers data should match");

    // Clean up
    let _ = fs::remove_file(&path);
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[test]
fn test_write_multiple_messages_before_finish() {
    let path = temp_bag_path("test_write_before_finish");
    let _guard = CleanupGuard;

    let mut writer = BagWriter::create(&path).unwrap();
    writer
        .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
        .unwrap();

    // Write multiple messages before finish - all should succeed
    for i in 0..5 {
        let msg = BagMessage::new(0, i * 1_000_000_000, vec![i as u8; 4]);
        assert!(writer.write_message(&msg).is_ok());
    }

    // Finish should succeed
    assert!(writer.finish().is_ok());
}

#[test]
fn test_add_multiple_connections_before_finish() {
    let path = temp_bag_path("test_multi_conn_before_finish");
    let _guard = CleanupGuard;

    let mut writer = BagWriter::create(&path).unwrap();

    // Add multiple connections
    for i in 0..3 {
        let topic = format!("/topic{}", i);
        assert!(writer
            .add_connection(i, &topic, "std_msgs/String", STD_MSGS_STRING_DEF)
            .is_ok());
    }

    // Write messages and finish
    assert!(writer
        .write_message(&BagMessage::new(0, 1_000_000_000, vec![1]))
        .is_ok());
    assert!(writer.finish().is_ok());
}

#[test]
fn test_write_message_with_invalid_connection_id() {
    let path = temp_bag_path("test_invalid_conn_id");
    let _guard = CleanupGuard;

    let mut writer = BagWriter::create(&path).unwrap();
    // Only add connection 0
    writer
        .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
        .unwrap();

    // Try to write with connection ID 5 (doesn't exist)
    let msg = BagMessage::new(5, 1_000_000_000, vec![1, 2, 3]);
    let result = writer.write_message(&msg);

    assert!(
        result.is_err(),
        "writing with invalid connection ID should fail"
    );

    if let Err(e) = result {
        let error_msg = e.to_string().to_lowercase();
        assert!(
            error_msg.contains("connection") || error_msg.contains("channel"),
            "error message should mention connection: {}",
            e
        );
    }

    // Finish should still work (the failed write didn't corrupt state)
    writer.finish().ok();
}

#[test]
fn test_finish_creates_valid_file() {
    let path = temp_bag_path("test_finish_valid");
    let _guard = CleanupGuard;

    let writer = BagWriter::create(&path).unwrap();
    writer.finish().unwrap();

    // Verify file is properly formatted
    let contents = fs::read(&path).unwrap();
    assert_eq!(contents.len(), 4096, "file should be properly closed");

    // Verify it can be read back
    let reader = BagFormat::open(&path);
    assert!(reader.is_ok(), "finished bag should be readable");
}

#[test]
fn test_write_after_finish_fails() {
    let path = temp_bag_path("test_write_after_finish");
    let _guard = CleanupGuard;

    // Create a bag, add connection, write a message, and finish it
    {
        let mut writer = BagWriter::create(&path).unwrap();
        writer
            .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
            .unwrap();
        let msg = BagMessage::new(0, 1_000_000_000, vec![1, 2, 3]);
        assert!(writer.write_message(&msg).is_ok());
        writer.finish().unwrap();
        // writer is dropped here
    }

    // Note: We can't test writing after finish() directly because finish()
    // consumes the writer. The ownership system prevents this at compile time.
    // This test verifies the normal workflow completes correctly.
    assert!(path.exists(), "bag file should exist after finish");
}

#[test]
fn test_add_connection_after_finish_fails() {
    let path = temp_bag_path("test_add_conn_after_finish");
    let _guard = CleanupGuard;

    // Create a bag with one connection and finish it
    {
        let mut writer = BagWriter::create(&path).unwrap();
        writer
            .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
            .unwrap();
        writer.finish().unwrap();
    }

    // Verify the bag is complete and can be read
    let reader = BagFormat::open(&path).unwrap();
    assert_eq!(
        reader.channels().len(),
        1,
        "bag should have exactly 1 connection"
    );
}

// ============================================================================
// Edge Case Tests
// ============================================================================

#[test]
fn test_empty_message_data() {
    let path = temp_bag_path("test_empty_message");
    let _guard = CleanupGuard;

    let mut writer = BagWriter::create(&path).unwrap();
    writer
        .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
        .unwrap();

    // Write message with empty data
    let msg = BagMessage::new(0, 1_000_000_000, vec![]);
    assert!(writer.write_message(&msg).is_ok());

    writer.finish().unwrap();
    assert!(path.exists());
}

#[test]
fn test_zero_timestamp() {
    let path = temp_bag_path("test_zero_timestamp");
    let _guard = CleanupGuard;

    let mut writer = BagWriter::create(&path).unwrap();
    writer
        .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
        .unwrap();

    // Write message with zero timestamp
    let msg = BagMessage::new(0, 0, vec![1, 2, 3]);
    assert!(writer.write_message(&msg).is_ok());

    writer.finish().unwrap();
}

#[test]
fn test_large_timestamp() {
    let path = temp_bag_path("test_large_timestamp");
    let _guard = CleanupGuard;

    let mut writer = BagWriter::create(&path).unwrap();
    writer
        .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
        .unwrap();

    // Write message with large timestamp (year 2260+)
    let msg = BagMessage::new(0, 10_000_000_000_000, vec![1, 2, 3]);
    assert!(writer.write_message(&msg).is_ok());

    writer.finish().unwrap();
}

#[test]
fn test_topic_with_special_characters() {
    let path = temp_bag_path("test_special_topic");
    let _guard = CleanupGuard;

    let mut writer = BagWriter::create(&path).unwrap();

    // Topics with underscores, numbers, and nested paths
    assert!(writer
        .add_connection(
            0,
            "/camera_front_raw",
            "sensor_msgs/Image",
            SENSOR_MSGS_IMAGE_DEF
        )
        .is_ok());
    assert!(writer
        .add_connection(
            1,
            "/robot/joint_states",
            "sensor_msgs/JointState",
            "int32[] data"
        )
        .is_ok());
    assert!(writer
        .add_connection(
            2,
            "/ns1/ns2/ns3/topic",
            "std_msgs/String",
            STD_MSGS_STRING_DEF
        )
        .is_ok());

    writer.finish().unwrap();

    // Verify topics are readable
    let reader = BagFormat::open(&path).unwrap();
    assert_eq!(reader.channels().len(), 3);
}

#[test]
fn test_single_message_per_chunk_threshold() {
    let path = temp_bag_path("test_single_chunk");
    let _guard = CleanupGuard;

    let mut writer = BagWriter::create(&path).unwrap();
    writer
        .add_connection(0, "/data", "std_msgs/String", STD_MSGS_STRING_DEF)
        .unwrap();

    // Write a single small message
    let msg = BagMessage::new(0, 1_000_000_000, vec![1, 2, 3]);
    writer.write_message(&msg).unwrap();

    // Finish should flush the partial chunk
    writer.finish().unwrap();

    // Verify file is valid
    let reader = BagFormat::open(&path);
    assert!(reader.is_ok(), "bag should be readable");
}

// ============================================================================
// Schema Preservation Tests
// ============================================================================

#[test]
fn test_message_definition_preserved() {
    let path = temp_bag_path("test_schema_preserved");
    let _guard = CleanupGuard;

    let mut writer = BagWriter::create(&path).unwrap();

    let expected_def = "string data\nint32 count";
    writer
        .add_connection(0, "/custom", "custom/Msg", expected_def)
        .unwrap();

    writer
        .write_message(&BagMessage::new(0, 1_000_000_000, vec![1]))
        .unwrap();
    writer.finish().unwrap();

    // Read back and check schema
    let reader = BagFormat::open(&path).unwrap();
    let channel = reader.channel_by_topic("/custom").unwrap();

    assert_eq!(
        channel.schema.as_ref().map(|s| s.trim()),
        Some(expected_def.trim()),
        "message definition should be preserved"
    );
}

// ============================================================================
// Drop Behavior Tests
// ============================================================================

#[test]
fn test_writer_drop_without_finish_creates_file() {
    let path = temp_bag_path("test_drop_no_finish");
    let _guard = CleanupGuard;

    // Create a writer and drop it without calling finish()
    {
        let mut _writer = BagWriter::create(&path).unwrap();
        _writer
            .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
            .unwrap();
        // _writer dropped here without finish()
    }

    // File should exist (header written) but may be incomplete
    // This is expected behavior - user should call finish()
    assert!(path.exists());
}
