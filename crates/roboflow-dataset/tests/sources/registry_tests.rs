// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Source layer tests as defined in ADR-004.
//!
//! Tests cover:
//! - Source registry functionality
//! - Mock source behavior
//! - Source configuration parsing

use roboflow_dataset::sources::{SourceConfig, SourceType, SourceMetadata};
use roboflow_dataset::testing::{MockSource, count_messages, MessageBuilder};

#[tokio::test]
async fn test_mock_source_reads_all_messages() {
    let messages: Vec<_> = (0..100)
        .map(|i| MessageBuilder::new("/test")
            .with_timestamp(i as u64 * 1_000_000_000)
            .float_array(vec![i as f32])
            .build())
        .collect();

    let source = MockSource::with_messages(messages);
    let mut source = source;

    let count = count_messages(&mut source).await;
    assert_eq!(count, 100);
}

#[tokio::test]
async fn test_mock_source_handles_batches() {
    let messages: Vec<_> = (0..1000)
        .map(|i| MessageBuilder::new("/test")
            .with_timestamp(i as u64 * 33_333_333)
            .float_array(vec![i as f32])
            .build())
        .collect();

    let mut source = MockSource::with_messages(messages);

    let batch = source.read_batch(100).await.unwrap().unwrap();
    assert_eq!(batch.len(), 100);

    let batch = source.read_batch(100).await.unwrap().unwrap();
    assert_eq!(batch.len(), 100);

    // Read remaining
    let mut remaining = 0;
    while let Some(batch) = source.read_batch(100).await.unwrap() {
        remaining += batch.len();
    }
    assert_eq!(remaining, 800);
}

#[tokio::test]
async fn test_mock_source_topic_filtering() {
    let camera_msgs: Vec<_> = (0..10)
        .map(|i| MessageBuilder::new("/camera/image")
            .with_timestamp(i as u64 * 33_333_333)
            .image(640, 480)
            .build())
        .collect();

    let state_msgs: Vec<_> = (0..10)
        .map(|i| MessageBuilder::new("/state")
            .with_timestamp(i as u64 * 33_333_333)
            .float_array(vec![i as f32])
            .build())
        .collect();

    let mut all_messages = camera_msgs;
    all_messages.extend(state_msgs);

    let mut source = MockSource::with_messages(all_messages);

    // All messages should be returned
    let count = count_messages(&mut source).await;
    assert_eq!(count, 20);
}

#[tokio::test]
async fn test_mock_source_with_camera_images() {
    let source = MockSource::with_camera_images("camera_0", 30, 30.0);
    let mut source = source;

    let batch = source.read_batch(10).await.unwrap().unwrap();
    assert_eq!(batch.len(), 10);

    // All should be camera messages
    for msg in &batch {
        assert!(msg.topic.contains("camera_0"));
        if let roboflow_core::CodecValue::Bytes(data) = &msg.data {
            // Check JPEG header
            assert!(data.len() > 4);
            assert_eq!(data[0], 0xFF);
            assert_eq!(data[1], 0xD8);
        } else {
            panic!("Expected bytes data");
        }
    }
}

#[tokio::test]
async fn test_mock_source_with_state_messages() {
    let source = MockSource::with_state_messages("/joint_states", 100, 100.0, 7);
    let mut source = source;

    let batch = source.read_batch(50).await.unwrap().unwrap();
    assert_eq!(batch.len(), 50);

    // Check message structure
    for (i, msg) in batch.iter().enumerate() {
        assert_eq!(msg.topic, "/joint_states");
        if let roboflow_core::CodecValue::Float32Array(values) = &msg.data {
            assert_eq!(values.len(), 7);
            // Values should match pattern: i * 7 + j
            for j in 0..7 {
                assert_eq!(values[j], ((i * 7 + j) as f32));
            }
        } else {
            panic!("Expected float array data");
        }
    }
}

#[tokio::test]
async fn test_mock_source_with_multi_topic() {
    let source = MockSource::with_multi_topic(10, 30.0);
    let mut source = source;

    let mut topics = std::collections::HashSet::new();
    while let Some(batch) = source.read_batch(100).await.unwrap() {
        for msg in batch {
            topics.insert(msg.topic.clone());
        }
    }

    // Should have camera, state, and action topics
    assert!(topics.contains("/camera/image"));
    assert!(topics.contains("/state"));
    assert!(topics.contains("/action"));
}

#[tokio::test]
async fn test_mock_source_metadata() {
    let mut source = MockSource::with_count(100);

    let config = SourceConfig::default();
    let metadata = source.initialize(&config).await.unwrap();

    assert_eq!(metadata.message_count, Some(100));
    assert_eq!(metadata.source_type, "mock");
}

#[tokio::test]
async fn test_mock_source_error_simulation() {
    let mut source = MockSource::with_error_at(5);
    source.messages = (0..10)
        .map(|i| MessageBuilder::new("/test")
            .with_timestamp(i as u64)
            .build())
        .collect();

    // First 5 reads should succeed
    for _ in 0..5 {
        let result = source.read_batch(1).await;
        assert!(result.is_ok());
    }

    // 6th read should fail
    let result = source.read_batch(1).await;
    assert!(result.is_err());
}

#[test]
fn test_source_metadata_builder() {
    let metadata = SourceMetadata::new("bag".to_string(), "/path/to/file.bag".to_string())
        .with_message_count(1000)
        .with_duration_ns(60_000_000_000)
        .with_topic("/camera/image", 500)
        .with_topic("/state", 500);

    assert_eq!(metadata.source_type, "bag");
    assert_eq!(metadata.message_count, Some(1000));
    assert_eq!(metadata.duration_ns, Some(60_000_000_000));
    assert_eq!(metadata.topics.len(), 2);
}

#[test]
fn test_source_config_default() {
    let config = SourceConfig::default();
    assert!(config.path.is_empty());
}
