// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Pipeline layer tests as defined in ADR-004.
//!
//! Tests cover:
//! - Frame alignment with gaps
//! - Frame completion criteria
//! - Parallel pipeline processing

use roboflow_dataset::formats::alignment::{
    buffer::{FrameAlignmentBuffer, PartialFrame},
    completion::FrameCompletionCriteria,
    config::StreamingConfig,
};
use roboflow_dataset::testing::MessageBuilder;
use roboflow_core::CodecValue;

#[test]
fn test_partial_frame_new() {
    let frame = PartialFrame::new(0, 1_000_000_000);

    assert_eq!(frame.index, 0);
    assert_eq!(frame.timestamp, 1_000_000_000);
    assert_eq!(frame.feature_count(), 0);
}

#[test]
fn test_partial_frame_add_feature() {
    let mut frame = PartialFrame::new(0, 0);

    frame.add_feature("/camera/image".to_string(), CodecValue::Bytes(vec![1, 2, 3]));
    assert!(frame.has_feature("/camera/image"));
    assert_eq!(frame.feature_count(), 1);

    frame.add_feature("/state".to_string(), CodecValue::Float32Array(vec![1.0]));
    assert!(frame.has_feature("/state"));
    assert_eq!(frame.feature_count(), 2);
}

#[test]
fn test_partial_frame_buffer_time() {
    let frame = PartialFrame::new(0, 0);
    // Buffer time should be near 0 for newly created frame
    assert!(frame.buffer_time_ms() < 100);
}

// ============================================================================
// Frame Completion Criteria Tests
// ============================================================================

#[test]
fn test_completion_criteria_new() {
    let criteria = FrameCompletionCriteria::new();

    // Empty criteria - any frame is complete
    assert!(criteria.required_feature_count() == 0);
}

#[test]
fn test_completion_criteria_require_feature() {
    let criteria = FrameCompletionCriteria::new()
        .require_feature("/camera/image")
        .require_feature("/state");

    assert_eq!(criteria.required_feature_count(), 2);
}

#[test]
fn test_completion_criteria_is_complete() {
    let criteria = FrameCompletionCriteria::new()
        .require_feature("/camera/image")
        .require_feature("/state");

    // Partial frame - not complete
    let mut frame = PartialFrame::new(0, 0);
    frame.add_feature("/camera/image".to_string(), CodecValue::Bytes(vec![]));
    assert!(!criteria.is_complete(&frame));

    // Complete frame
    frame.add_feature("/state".to_string(), CodecValue::Float32Array(vec![]));
    assert!(criteria.is_complete(&frame));
}

#[test]
fn test_completion_criteria_min_completeness() {
    let criteria = FrameCompletionCriteria::new()
        .require_feature("/camera/image")
        .require_feature("/state")
        .require_feature("/action")
        .with_min_completeness(0.67); // 2/3 features

    let mut frame = PartialFrame::new(0, 0);
    frame.add_feature("/camera/image".to_string(), CodecValue::Bytes(vec![]));
    frame.add_feature("/state".to_string(), CodecValue::Float32Array(vec![]));

    // With 2/3 features, should be complete with 0.67 threshold
    assert!(criteria.is_complete(&frame));
}

#[test]
fn test_completion_criteria_any_feature_sufficient() {
    let criteria = FrameCompletionCriteria::new()
        .require_feature("/camera/image")
        .require_feature("/state")
        .with_min_completeness(0.0); // Any feature is sufficient

    let mut frame = PartialFrame::new(0, 0);
    frame.add_feature("/camera/image".to_string(), CodecValue::Bytes(vec![]));

    assert!(criteria.is_complete(&frame));
}

// ============================================================================
// Streaming Config Tests
// ============================================================================

#[test]
fn test_streaming_config_fps() {
    let config = StreamingConfig::with_fps(30.0);

    // At 30fps, frame interval is ~33.33ms
    let interval_ns = config.frame_interval_ns();
    let expected = (1_000_000_000.0 / 30.0) as u64;
    assert!((interval_ns as i64 - expected as i64).abs() < 1000);
}

#[test]
fn test_streaming_config_completion_window() {
    let config = StreamingConfig::with_fps(30.0)
        .with_completion_window(std::time::Duration::from_millis(100));

    assert_eq!(config.completion_window_ns(), 100_000_000);
}

#[test]
fn test_streaming_config_require_feature() {
    let config = StreamingConfig::with_fps(30.0)
        .require_feature("/camera/image")
        .require_feature("/state");

    // Features should be recorded
    assert!(!config.feature_requirements.is_empty());
}

// ============================================================================
// Frame Alignment Buffer Tests
// ============================================================================

#[test]
fn test_frame_alignment_buffer_new() {
    let config = StreamingConfig::with_fps(30.0);
    let buffer = FrameAlignmentBuffer::new(config);

    assert!(buffer.is_empty());
    assert_eq!(buffer.len(), 0);
}

#[test]
fn test_frame_alignment_buffer_estimated_memory() {
    let config = StreamingConfig::with_fps(30.0);
    let buffer = FrameAlignmentBuffer::new(config);

    // Empty buffer should have minimal memory
    let memory = buffer.estimated_memory_bytes();
    assert!(memory < 10_000); // Less than 10KB for empty buffer
}

// ============================================================================
// Integration: Message Processing
// ============================================================================

#[tokio::test]
async fn test_frame_alignment_with_sequential_messages() {
    let config = StreamingConfig::with_fps(30.0);
    let mut buffer = FrameAlignmentBuffer::new(config);

    // Create messages at 30fps intervals
    let ns_per_frame = (1_000_000_000.0 / 30.0) as u64;

    for i in 0..10 {
        let msg = MessageBuilder::new("/camera/image")
            .with_timestamp(i as u64 * ns_per_frame)
            .image(640, 480)
            .build();

        buffer.process_message(msg.log_time, msg.topic, msg.data).unwrap();
    }

    // Should have processed all messages
    assert!(buffer.stats().messages_processed >= 10);
}

#[tokio::test]
async fn test_frame_alignment_with_gaps() {
    let config = StreamingConfig::with_fps(30.0);
    let mut buffer = FrameAlignmentBuffer::new(config);

    let ns_per_frame = (1_000_000_000.0 / 30.0) as u64;

    // Messages with gaps
    let timestamps = vec![0, 1, 3, 5, 8]; // Missing frames 2, 4, 6, 7

    for (i, &frame_idx) in timestamps.iter().enumerate() {
        let msg = MessageBuilder::new("/camera/image")
            .with_timestamp(frame_idx as u64 * ns_per_frame)
            .image(640, 480)
            .build();

        buffer.process_message(msg.log_time, msg.topic, msg.data).unwrap();
    }

    // Buffer should handle gaps gracefully
    assert!(buffer.stats().messages_processed >= 5);
}

#[tokio::test]
async fn test_frame_alignment_multi_topic() {
    let config = StreamingConfig::with_fps(30.0)
        .require_feature("/camera/image")
        .require_feature("/state");

    let completion = FrameCompletionCriteria::new()
        .require_feature("/camera/image")
        .require_feature("/state");

    let mut buffer = FrameAlignmentBuffer::new(config)
        .with_completion_criteria(completion);

    let ns_per_frame = (1_000_000_000.0 / 30.0) as u64;

    for i in 0..10 {
        // Camera message
        let msg = MessageBuilder::new("/camera/image")
            .with_timestamp(i as u64 * ns_per_frame)
            .image(640, 480)
            .build();
        buffer.process_message(msg.log_time, msg.topic, msg.data).unwrap();

        // State message (slightly offset)
        let msg = MessageBuilder::new("/state")
            .with_timestamp(i as u64 * ns_per_frame + 5_000_000)
            .float_array(vec![i as f32])
            .build();
        buffer.process_message(msg.log_time, msg.topic, msg.data).unwrap();
    }

    // Flush to complete any pending frames
    let frames = buffer.flush().unwrap();

    // Should have aligned frames
    assert!(!frames.is_empty());
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_partial_frame_empty_features() {
    let frame = PartialFrame::new(0, 0);

    // Empty frame should have 0 features
    assert!(!frame.has_feature("/any"));
    assert_eq!(frame.feature_count(), 0);
}

#[test]
fn test_completion_criteria_empty_features_empty_received() {
    let criteria = FrameCompletionCriteria::new();
    let frame = PartialFrame::new(0, 0);

    // With no requirements, any frame is complete
    assert!(criteria.is_complete(&frame));
}

#[test]
fn test_completion_criteria_clamp_min_completeness() {
    // Negative values should be clamped to 0
    let criteria = FrameCompletionCriteria::new()
        .with_min_completeness(-1.0);

    let mut frame = PartialFrame::new(0, 0);
    frame.add_feature("/test".to_string(), CodecValue::Null);

    // With 0 threshold, frame with any feature should be complete
    assert!(criteria.is_complete(&frame));
}

#[test]
fn test_completion_criteria_max_completeness() {
    // Values > 1 should be clamped to 1
    let criteria = FrameCompletionCriteria::new()
        .require_feature("/camera/image")
        .require_feature("/state")
        .with_min_completeness(2.0); // Should be clamped to 1.0

    let mut frame = PartialFrame::new(0, 0);
    frame.add_feature("/camera/image".to_string(), CodecValue::Bytes(vec![]));
    frame.add_feature("/state".to_string(), CodecValue::Float32Array(vec![]));

    // With all features, should be complete even with > 1 threshold
    assert!(criteria.is_complete(&frame));
}
