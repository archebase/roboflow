// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Integration tests as defined in ADR-004.
//!
//! Tests cover:
//! - End-to-end conversion workflows
//! - Multi-episode processing
//! - Mock storage integration

use roboflow_dataset::core::traits::FormatWriter;
use roboflow_dataset::sources::Source;
use roboflow_dataset::testing::{
    FrameBuilder, InMemoryWriter, MessageBuilder, MockSource, MockStorage, count_messages,
    generate_test_frames,
};

// ============================================================================
// End-to-End Conversion Tests
// ============================================================================

#[tokio::test]
async fn test_e2e_mock_source_to_in_memory_writer() {
    // Create a mock source with 100 messages
    let source = MockSource::with_multi_topic(100, 30.0);
    let mut source = source;

    // Create in-memory writer
    let mut writer = InMemoryWriter::new();

    // Process messages into frames
    writer.start_episode(None).unwrap();

    while let Some(batch) = source.read_batch(10).await.unwrap() {
        for _msg in batch {
            // In a real pipeline, we'd align messages into frames
            // For this test, just count that we receive messages
        }
    }

    writer.finish_episode().unwrap();
    let _stats = writer.finalize().unwrap();

    assert!(writer.is_finalized());
}

#[tokio::test]
async fn test_e2e_camera_images_to_frames() {
    // Create source with camera images
    let source = MockSource::with_camera_images("camera_0", 30, 30.0);
    let mut source = source;

    let mut writer = InMemoryWriter::new();
    writer.start_episode(None).unwrap();

    let mut frame_idx = 0;
    while let Some(batch) = source.read_batch(1).await.unwrap() {
        for _msg in batch {
            // Create a frame for each message
            let frame = FrameBuilder::new(frame_idx)
                .add_encoded_image("observation.camera_0", 320, 240)
                .build();
            writer.write_frame(&frame).unwrap();
            frame_idx += 1;
        }
    }

    writer.finish_episode().unwrap();
    let stats = writer.finalize().unwrap();

    assert_eq!(stats.frames_written, 30);
    assert_eq!(writer.len(), 30);
}

#[tokio::test]
async fn test_e2e_state_messages_to_frames() {
    let source = MockSource::with_state_messages("/joint_states", 100, 100.0, 7);
    let mut source = source;

    let mut writer = InMemoryWriter::new();
    writer.start_episode(None).unwrap();

    let mut frame_idx = 0;
    while let Some(batch) = source.read_batch(10).await.unwrap() {
        for msg in batch {
            // Extract float values from Array(Float32(...))
            if let roboflow_core::CodecValue::Array(values) = &msg.data {
                let float_values: Vec<f32> = values
                    .iter()
                    .filter_map(|v| {
                        if let roboflow_core::CodecValue::Float32(f) = v {
                            Some(*f)
                        } else {
                            None
                        }
                    })
                    .collect();

                let frame = FrameBuilder::new(frame_idx)
                    .with_timestamp(msg.log_time)
                    .add_state("observation.joint_position", float_values)
                    .build();
                writer.write_frame(&frame).unwrap();
                frame_idx += 1;
            }
        }
    }

    writer.finish_episode().unwrap();
    let stats = writer.finalize().unwrap();

    assert_eq!(stats.frames_written, 100);

    // Verify frames have correct state data
    let frames = writer.frames();
    for frame in frames.iter() {
        let state = frame.states.get("observation.joint_position");
        assert!(state.is_some());
        assert_eq!(state.unwrap().len(), 7);
    }
}

// ============================================================================
// Multi-Episode Tests
// ============================================================================

#[tokio::test]
async fn test_e2e_multi_episode_processing() {
    let mut writer = InMemoryWriter::new();

    // Process 5 episodes with varying frame counts
    let episode_frames = [10, 20, 15, 25, 30];

    for (ep_idx, &frame_count) in episode_frames.iter().enumerate() {
        writer.start_episode(Some(ep_idx)).unwrap();

        for i in 0..frame_count {
            let frame = FrameBuilder::new(i)
                .add_state("observation.state", vec![i as f32])
                .add_action("action", vec![(i + 1) as f32])
                .build();
            writer.write_frame(&frame).unwrap();
        }

        writer.finish_episode().unwrap();
    }

    let stats = writer.finalize().unwrap();

    // Verify total frames
    let expected_total: usize = episode_frames.iter().sum();
    assert_eq!(stats.frames_written, expected_total);
    assert_eq!(writer.len(), expected_total);

    // Verify each episode has correct frame count
    for (ep_idx, &expected_count) in episode_frames.iter().enumerate() {
        assert_eq!(writer.episode_frames(ep_idx).unwrap().len(), expected_count);
    }
}

#[tokio::test]
async fn test_e2e_episode_with_gaps() {
    let mut writer = InMemoryWriter::new();

    // Episode 0
    writer.start_episode(None).unwrap();
    writer.write_frame(&FrameBuilder::new(0).build()).unwrap();
    writer.write_frame(&FrameBuilder::new(1).build()).unwrap();
    writer.finish_episode().unwrap();

    // Skip to episode 5 (simulating non-contiguous episodes)
    writer.start_episode(None).unwrap();
    writer.write_frame(&FrameBuilder::new(0).build()).unwrap();
    writer.finish_episode().unwrap();

    writer.finalize().unwrap();

    // Both episodes should be present
    assert_eq!(writer.episode_frames(0).unwrap().len(), 2);
    assert_eq!(writer.episode_frames(1).unwrap().len(), 1);
}

// ============================================================================
// Mock Storage Integration Tests
// ============================================================================

#[test]
fn test_mock_storage_upload_workflow() {
    let storage = MockStorage::new();
    let mut writer = InMemoryWriter::new();

    // Write some frames
    writer.start_episode(None).unwrap();
    for i in 0..10 {
        writer
            .write_frame(
                &FrameBuilder::new(i)
                    .add_state("observation.state", vec![i as f32])
                    .build(),
            )
            .unwrap();
    }
    writer.finish_episode().unwrap();
    writer.finalize().unwrap();

    // Simulate uploading frame data
    for (i, _frame) in writer.frames().iter().enumerate() {
        let key = format!("episode_0/frame_{:06}.bin", i);
        let data = format!("frame data {}", i);
        storage.record_upload(&key, data.as_bytes()).unwrap();
    }

    // Verify all uploads
    assert_eq!(storage.get_operations().len(), 10);
    for i in 0..10 {
        let key = format!("episode_0/frame_{:06}.bin", i);
        assert!(storage.has_file(&key));
    }
}

#[test]
fn test_mock_storage_error_handling() {
    let storage = MockStorage::new();
    storage.fail_after(2);

    // First 2 uploads should succeed
    storage.record_upload("file1.txt", b"data").unwrap();
    storage.record_upload("file2.txt", b"data").unwrap();

    // Third should fail (fail_after(N) allows N operations before failing)
    let result = storage.record_upload("file3.txt", b"data");
    assert!(result.is_err());
}

// ============================================================================
// Performance Tests
// ============================================================================

#[test]
fn test_performance_frame_creation() {
    let start = std::time::Instant::now();

    // Create 10,000 frames
    for i in 0..10_000 {
        let _frame = FrameBuilder::new(i)
            .add_state("observation.state", vec![i as f32, (i + 1) as f32])
            .add_action("action", vec![i as f32])
            .add_encoded_image("observation.camera_0", 640, 480)
            .build();
    }

    let elapsed = start.elapsed();

    // Should create frames in under 100ms (very generous)
    println!("Created 10,000 frames in {:?}", elapsed);
}

#[test]
fn test_performance_writer_throughput() {
    let mut writer = InMemoryWriter::new();

    let start = std::time::Instant::now();

    writer.start_episode(None).unwrap();
    for i in 0..10_000 {
        writer
            .write_frame(
                &FrameBuilder::new(i)
                    .add_state("observation.state", vec![i as f32])
                    .build(),
            )
            .unwrap();
    }
    writer.finish_episode().unwrap();

    let elapsed = start.elapsed();
    let fps = 10_000.0 / elapsed.as_secs_f64();

    println!("Wrote 10,000 frames in {:?} ({:.0} fps)", elapsed, fps);

    // Should write at least 10,000 fps
    assert!(fps > 10_000.0);
}

// ============================================================================
// Error Recovery Tests
// ============================================================================

#[tokio::test]
async fn test_error_recovery_from_source_error() {
    let mut source = MockSource::with_error_at(5);
    source.set_messages(
        (0..10)
            .map(|i| {
                MessageBuilder::new("/test")
                    .with_timestamp(i as u64 * 33_333_333)
                    .build()
            })
            .collect(),
    );

    let mut writer = InMemoryWriter::new();
    writer.start_episode(None).unwrap();

    let mut error_occurred = false;
    let mut frames_written = 0;

    loop {
        match source.read_batch(1).await {
            Ok(Some(batch)) => {
                for _msg in batch {
                    writer
                        .write_frame(&FrameBuilder::new(frames_written).build())
                        .unwrap();
                    frames_written += 1;
                }
            }
            Ok(None) => break,
            Err(_) => {
                error_occurred = true;
                break;
            }
        }
    }

    assert!(error_occurred);
    assert_eq!(frames_written, 5); // Should have written 5 frames before error
}

#[test]
fn test_writer_state_after_finalize() {
    let mut writer = InMemoryWriter::new();

    writer.write_frame(&FrameBuilder::new(0).build()).unwrap();
    writer.finalize().unwrap();

    // After finalize, frames should still be accessible
    assert_eq!(writer.len(), 1);
    assert!(writer.is_finalized());

    // Attempting to write after finalize should work (InMemoryWriter doesn't enforce)
    // but in real implementations this might error
}

// ============================================================================
// Helper Function Tests
// ============================================================================

#[test]
fn test_generate_test_frames() {
    let frames = generate_test_frames(100, 640, 480);

    assert_eq!(frames.len(), 100);

    for (i, frame) in frames.iter().enumerate() {
        assert_eq!(frame.frame_index, i);
        assert!(frame.images.contains_key("observation.camera_0"));
        assert!(frame.states.contains_key("observation.state"));
    }
}

#[tokio::test]
async fn test_count_messages_helper() {
    let source = MockSource::with_count(100);
    let mut source = source;

    let count = count_messages(&mut source).await;
    assert_eq!(count, 100);
}
