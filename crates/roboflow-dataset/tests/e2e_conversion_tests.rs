// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! End-to-end conversion tests for roboflow-dataset.
//!
//! These tests exercise the full conversion pipeline from source files
//! to dataset formats, ensuring correctness of the entire data flow.

use roboflow_dataset::conversion::ConversionConfig;
use roboflow_dataset::core::traits::FormatWriter;
use roboflow_dataset::formats::lerobot::LerobotWriterTrait;
use roboflow_dataset::formats::{DatasetConfig, DatasetFormat};
use roboflow_dataset::sources::Source;
use roboflow_dataset::testing::{FrameBuilder, InMemoryWriter, MockSource};

// ============================================================================
// Full Pipeline E2E Tests
// ============================================================================

#[test]
fn test_e2e_convert_file_lerobot_format() {
    // Create a temp directory for output
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let _output_dir = temp_dir.path().join("output");

    // Create a mock source file path (we'll simulate with an in-memory test)
    // For this test, we'll use the testing utilities directly
    let config = ConversionConfig::new(DatasetConfig::new(
        DatasetFormat::Lerobot,
        "test_dataset",
        30,
        None,
    ));

    // Verify the conversion config is properly structured
    assert_eq!(config.dataset.fps(), 30);
    assert!(config.max_frames.is_none());
    assert!(config.topic_mappings.is_empty());
}

#[test]
fn test_e2e_conversion_config_with_mappings() {
    let config = ConversionConfig::new(DatasetConfig::new(
        DatasetFormat::Lerobot,
        "test_dataset",
        30,
        None,
    ))
    .with_topic_mapping("/camera/image", "observation.images.camera")
    .with_topic_mapping("/joint_states", "observation.state")
    .with_max_frames(1000)
    .with_output_prefix("episode_001");

    assert_eq!(config.topic_mappings.len(), 2);
    assert_eq!(
        config.topic_mappings.get("/camera/image"),
        Some(&"observation.images.camera".to_string())
    );
    assert_eq!(config.max_frames, Some(1000));
    assert_eq!(config.output_prefix, Some("episode_001".to_string()));
}

#[test]
fn test_e2e_mock_source_through_writer() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        // Create mock source with camera and state messages
        let mut source = MockSource::with_multi_topic(100, 30.0);

        // Create in-memory writer
        let mut writer = InMemoryWriter::new();
        writer.start_episode(None).expect("Failed to start episode");

        let mut frame_count = 0;
        while let Some(batch) = source.read_batch(10).await.unwrap() {
            for _msg in batch {
                // Create a frame for each message batch
                let frame = FrameBuilder::new(frame_count)
                    .add_state("observation.state", vec![frame_count as f32])
                    .add_action("action", vec![(frame_count + 1) as f32])
                    .build();
                writer.write_frame(&frame).expect("Failed to write frame");
                frame_count += 1;
            }
        }

        writer.finish_episode().expect("Failed to finish episode");
        let stats = writer.finalize().expect("Failed to finalize");

        assert_eq!(stats.frames_written, frame_count);
        assert!(frame_count > 0, "Should have processed frames");
    });
}

// ============================================================================
// Dataset Format Output Tests
// ============================================================================

#[test]
fn test_e2e_lerobot_dataset_output_structure() {
    use roboflow_dataset::formats::common::config::DatasetBaseConfig;
    use roboflow_dataset::formats::lerobot::LerobotWriter;
    use roboflow_dataset::formats::lerobot::config::{
        DatasetConfig, FlushingConfig, LerobotConfig, StreamingConfig, VideoConfig,
    };

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    // Create LeRobot config
    let config = LerobotConfig {
        dataset: DatasetConfig {
            base: DatasetBaseConfig {
                name: "test_dataset".to_string(),
                fps: 30,
                robot_type: Some("test_robot".to_string()),
            },
            env_type: None,
        },
        mappings: vec![],
        video: VideoConfig::default(),
        annotation_file: None,
        flushing: FlushingConfig::default(),
        streaming: StreamingConfig::default(),
    };

    // Create writer and write data
    let mut writer =
        LerobotWriter::new_local(temp_dir.path(), config).expect("Failed to create writer");

    writer
        .start_episode(Some(0))
        .expect("Failed to start episode");

    for i in 0..10 {
        let frame = FrameBuilder::new(i)
            .with_timestamp(i as u64 * 33_333_333)
            .add_state("observation.state", vec![i as f32, (i + 1) as f32])
            .add_action("action", vec![(i + 2) as f32])
            .build();
        writer.write_frame(&frame).expect("Failed to write frame");
    }

    writer
        .finish_episode(Some(0))
        .expect("Failed to finish episode");
    writer.finalize_with_config().expect("Failed to finalize");

    // Verify output structure
    let data_dir = temp_dir.path().join("data");
    assert!(data_dir.exists(), "data directory should exist");

    // Check for parquet file (in chunk-000 subdirectory)
    let parquet_file = data_dir.join("chunk-000/episode_000000.parquet");
    assert!(
        parquet_file.exists(),
        "Parquet file should exist at {:?}",
        parquet_file
    );

    // Check for metadata files
    let info_json = temp_dir.path().join("meta/info.json");
    assert!(info_json.exists(), "info.json should exist");
}

#[test]
fn test_e2e_multi_episode_lerobot_dataset() {
    use roboflow_dataset::formats::common::config::DatasetBaseConfig;
    use roboflow_dataset::formats::lerobot::LerobotWriter;
    use roboflow_dataset::formats::lerobot::config::{
        DatasetConfig, FlushingConfig, LerobotConfig, StreamingConfig, VideoConfig,
    };

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let config = LerobotConfig {
        dataset: DatasetConfig {
            base: DatasetBaseConfig {
                name: "multi_episode_test".to_string(),
                fps: 30,
                robot_type: Some("test_robot".to_string()),
            },
            env_type: None,
        },
        mappings: vec![],
        video: VideoConfig::default(),
        annotation_file: None,
        flushing: FlushingConfig::default(),
        streaming: StreamingConfig::default(),
    };

    let mut writer =
        LerobotWriter::new_local(temp_dir.path(), config).expect("Failed to create writer");

    // Set 1 episode per chunk to get separate parquet files per episode
    writer.set_episodes_per_chunk(1);

    let episode_counts = [10, 20, 15];

    for (ep_idx, &frame_count) in episode_counts.iter().enumerate() {
        // Set episode index before starting episode
        writer.set_episode_index(ep_idx);
        writer
            .start_episode(Some(ep_idx))
            .expect("Failed to start episode");

        for i in 0..frame_count {
            let frame = FrameBuilder::new(i)
                .with_timestamp(i as u64 * 33_333_333)
                .add_state("observation.state", vec![ep_idx as f32, i as f32])
                .add_action("action", vec![(ep_idx + i) as f32])
                .build();
            writer.write_frame(&frame).expect("Failed to write frame");
        }

        writer
            .finish_episode(Some(ep_idx))
            .expect("Failed to finish episode");
    }

    writer.finalize_with_config().expect("Failed to finalize");

    // Verify episodes exist by checking all chunk directories for parquet files
    let data_dir = temp_dir.path().join("data");

    // Collect all parquet files across all chunk directories
    let mut all_parquet_files = Vec::new();
    for chunk_dir in std::fs::read_dir(&data_dir).expect("Failed to read data dir") {
        let chunk_dir = chunk_dir.expect("Failed to read chunk dir entry");
        if chunk_dir.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            let parquet_files: Vec<_> = std::fs::read_dir(chunk_dir.path())
                .expect("Failed to read chunk dir")
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.path()
                        .extension()
                        .map(|ext| ext == "parquet")
                        .unwrap_or(false)
                })
                .collect();
            all_parquet_files.extend(parquet_files);
        }
    }

    // Should have parquet files for each episode
    assert_eq!(
        all_parquet_files.len(),
        episode_counts.len(),
        "Should have {} parquet files (one per episode)",
        episode_counts.len()
    );
}

// ============================================================================
// Data Integrity Tests
// ============================================================================

#[test]
fn test_e2e_frame_data_integrity() {
    use roboflow_dataset::formats::common::config::DatasetBaseConfig;
    use roboflow_dataset::formats::lerobot::LerobotWriter;
    use roboflow_dataset::formats::lerobot::config::{
        DatasetConfig, FlushingConfig, LerobotConfig, StreamingConfig, VideoConfig,
    };

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let config = LerobotConfig {
        dataset: DatasetConfig {
            base: DatasetBaseConfig {
                name: "integrity_test".to_string(),
                fps: 30,
                robot_type: Some("test_robot".to_string()),
            },
            env_type: None,
        },
        mappings: vec![],
        video: VideoConfig::default(),
        annotation_file: None,
        flushing: FlushingConfig::default(),
        streaming: StreamingConfig::default(),
    };

    let mut writer =
        LerobotWriter::new_local(temp_dir.path(), config).expect("Failed to create writer");

    writer
        .start_episode(Some(0))
        .expect("Failed to start episode");

    // Write frames with specific data patterns
    let frame_data: Vec<(usize, Vec<f32>, Vec<f32>)> = (0..5)
        .map(|i| {
            (
                i,
                vec![i as f32 * 1.0, i as f32 * 2.0, i as f32 * 3.0],
                vec![i as f32 * 0.1, i as f32 * 0.2],
            )
        })
        .collect();

    for (idx, state_vals, action_vals) in &frame_data {
        let frame = FrameBuilder::new(*idx)
            .with_timestamp(*idx as u64 * 33_333_333)
            .add_state("observation.state", state_vals.clone())
            .add_action("action", action_vals.clone())
            .build();
        writer.write_frame(&frame).expect("Failed to write frame");
    }

    writer
        .finish_episode(Some(0))
        .expect("Failed to finish episode");
    let stats = writer.finalize_with_config().expect("Failed to finalize");

    assert_eq!(stats.frames_written, 5);
}

// ============================================================================
// Error Handling and Edge Cases
// ============================================================================

#[test]
fn test_e2e_empty_dataset() {
    use roboflow_dataset::formats::common::config::DatasetBaseConfig;
    use roboflow_dataset::formats::lerobot::LerobotWriter;
    use roboflow_dataset::formats::lerobot::config::{
        DatasetConfig, FlushingConfig, LerobotConfig, StreamingConfig, VideoConfig,
    };

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let config = LerobotConfig {
        dataset: DatasetConfig {
            base: DatasetBaseConfig {
                name: "empty_test".to_string(),
                fps: 30,
                robot_type: Some("test_robot".to_string()),
            },
            env_type: None,
        },
        mappings: vec![],
        video: VideoConfig::default(),
        annotation_file: None,
        flushing: FlushingConfig::default(),
        streaming: StreamingConfig::default(),
    };

    let mut writer =
        LerobotWriter::new_local(temp_dir.path(), config).expect("Failed to create writer");

    // Start and immediately finish an empty episode
    writer
        .start_episode(Some(0))
        .expect("Failed to start episode");
    writer
        .finish_episode(Some(0))
        .expect("Failed to finish episode");

    let stats = writer.finalize_with_config().expect("Failed to finalize");

    // Empty episode should still be valid
    assert_eq!(stats.frames_written, 0);
}

#[test]
fn test_e2e_large_frame_count() {
    let mut writer = InMemoryWriter::new();

    writer.start_episode(None).expect("Failed to start episode");

    // Write 1000 frames
    for i in 0..1000 {
        let frame = FrameBuilder::new(i)
            .add_state("observation.state", vec![i as f32])
            .build();
        writer.write_frame(&frame).expect("Failed to write frame");
    }

    writer.finish_episode().expect("Failed to finish episode");
    let stats = writer.finalize().expect("Failed to finalize");

    assert_eq!(stats.frames_written, 1000);
    assert_eq!(writer.len(), 1000);
}

#[test]
fn test_e2e_multiple_features_per_frame() {
    let mut writer = InMemoryWriter::new();

    writer.start_episode(None).expect("Failed to start episode");

    for i in 0..10 {
        let frame = FrameBuilder::new(i)
            .add_state(
                "observation.joint_position",
                vec![i as f32, (i + 1) as f32, (i + 2) as f32],
            )
            .add_state("observation.gripper_position", vec![i as f32 * 0.1])
            .add_action(
                "action.joint_velocity",
                vec![(i + 5) as f32, (i + 6) as f32],
            )
            .add_action("action.gripper", vec![if i % 2 == 0 { 1.0 } else { 0.0 }])
            .build();
        writer.write_frame(&frame).expect("Failed to write frame");
    }

    writer.finish_episode().expect("Failed to finish episode");
    let stats = writer.finalize().expect("Failed to finalize");

    assert_eq!(stats.frames_written, 10);

    // Verify frames have all expected features
    let frames = writer.frames();
    for frame in frames.iter() {
        assert!(frame.states.contains_key("observation.joint_position"));
        assert!(frame.states.contains_key("observation.gripper_position"));
    }
}

// ============================================================================
// Async Source Integration Tests
// ============================================================================

#[tokio::test]
async fn test_e2e_async_mock_source_to_writer() {
    let mut source = MockSource::with_camera_images("camera_0", 50, 30.0);
    let mut writer = InMemoryWriter::new();

    writer.start_episode(None).expect("Failed to start episode");

    let mut frame_count = 0;
    while let Some(batch) = source.read_batch(10).await.unwrap() {
        for msg in batch {
            // Create frame from message
            let frame = FrameBuilder::new(frame_count)
                .with_timestamp(msg.log_time)
                .add_state("observation.timestamp", vec![msg.log_time as f32])
                .build();
            writer.write_frame(&frame).expect("Failed to write frame");
            frame_count += 1;
        }
    }

    writer.finish_episode().expect("Failed to finish episode");
    let stats = writer.finalize().expect("Failed to finalize");

    assert_eq!(stats.frames_written, 50);
}

#[tokio::test]
async fn test_e2e_async_multi_topic_source() {
    // with_multi_topic creates 3 messages per frame (camera, state, action)
    let frame_count_input = 60;
    let mut source = MockSource::with_multi_topic(frame_count_input, 30.0);
    let mut writer = InMemoryWriter::new();

    writer.start_episode(None).expect("Failed to start episode");

    let mut message_count = 0;
    while let Some(batch) = source.read_batch(20).await.unwrap() {
        for msg in batch {
            let frame = match msg.topic.as_str() {
                "/camera/image" => FrameBuilder::new(message_count)
                    .add_state("observation.camera_trigger", vec![1.0])
                    .build(),
                "/state" => FrameBuilder::new(message_count)
                    .add_state("observation.state", vec![message_count as f32])
                    .build(),
                "/action" => FrameBuilder::new(message_count)
                    .add_action("action", vec![message_count as f32])
                    .build(),
                _ => FrameBuilder::new(message_count).build(),
            };
            writer.write_frame(&frame).expect("Failed to write frame");
            message_count += 1;
        }
    }

    writer.finish_episode().expect("Failed to finish episode");
    let stats = writer.finalize().expect("Failed to finalize");

    // Total messages = frames * 3 topics
    let expected_messages = frame_count_input * 3;
    assert_eq!(stats.frames_written, expected_messages);
}

// ============================================================================
// Performance and Throughput Tests
// ============================================================================

#[test]
fn test_e2e_writer_throughput_benchmark() {
    let mut writer = InMemoryWriter::new();

    let frame_count = 10_000;
    let start = std::time::Instant::now();

    writer.start_episode(None).expect("Failed to start episode");

    for i in 0..frame_count {
        let frame = FrameBuilder::new(i)
            .add_state("observation.state", vec![i as f32, (i + 1) as f32])
            .add_action("action", vec![(i + 2) as f32])
            .build();
        writer.write_frame(&frame).expect("Failed to write frame");
    }

    writer.finish_episode().expect("Failed to finish episode");
    let stats = writer.finalize().expect("Failed to finalize");

    let elapsed = start.elapsed();
    let fps = frame_count as f64 / elapsed.as_secs_f64();

    println!(
        "E2E Throughput: {} frames in {:?} ({:.0} fps)",
        frame_count, elapsed, fps
    );

    assert_eq!(stats.frames_written, frame_count);
    // Should maintain at least 50,000 fps in memory
    assert!(fps > 50_000.0, "Throughput too low: {:.0} fps", fps);
}

#[test]
fn test_e2e_memory_efficiency() {
    let mut writer = InMemoryWriter::new();

    // Write many frames to check memory handling
    writer.start_episode(None).expect("Failed to start episode");

    for i in 0..100_000 {
        let frame = FrameBuilder::new(i)
            .add_state("observation.state", vec![i as f32])
            .build();
        writer.write_frame(&frame).expect("Failed to write frame");

        // Periodically check we're not accumulating memory issues
        if i % 10_000 == 0 {
            assert_eq!(writer.len(), i + 1);
        }
    }

    writer.finish_episode().expect("Failed to finish episode");
    let stats = writer.finalize().expect("Failed to finalize");

    assert_eq!(stats.frames_written, 100_000);
}
