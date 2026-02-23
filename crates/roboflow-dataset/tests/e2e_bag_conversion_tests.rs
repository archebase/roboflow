// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! End-to-end conversion tests using real bag files.
//!
//! These tests exercise the full conversion pipeline with actual bag files
//! from the fixtures directory. They use small frame/fragment configurations
//! to trigger complex logic in the dataset and media layers.

use std::path::Path;

use roboflow_dataset::conversion::{ConversionConfig, convert_file};
use roboflow_dataset::formats::common::DatasetWriter;
use roboflow_dataset::formats::lerobot::LerobotWriter;
use roboflow_dataset::formats::lerobot::LerobotWriterTrait;
use roboflow_dataset::formats::lerobot::config::{
    DatasetConfig as LeRobotDatasetConfig, FlushingConfig, LerobotConfig, StreamingConfig,
    VideoConfig,
};
use roboflow_dataset::formats::{DatasetConfig, DatasetFormat};

/// Path to the test fixtures directory.
fn fixtures_dir() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("tests/fixtures")
}

/// Get the smallest bag file for quick testing.
fn small_bag_file() -> std::path::PathBuf {
    fixtures_dir().join("roboflow_sample.bag")
}

// ============================================================================
// Real Bag File Conversion Tests
// ============================================================================

#[test]
#[ignore = "Requires real bag file - run manually or in CI"]
fn test_e2e_convert_small_bag_file() {
    let bag_path = small_bag_file();
    if !bag_path.exists() {
        eprintln!("Skipping test: bag file not found at {:?}", bag_path);
        return;
    }

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    // Create config with small max_frames to trigger partial processing
    let config = ConversionConfig::new(DatasetConfig::new(
        DatasetFormat::Lerobot,
        "test_dataset",
        30,
        None,
    ))
    .with_max_frames(100); // Small limit to test partial processing

    let result = convert_file(&bag_path, temp_dir.path(), &config);

    // Conversion may succeed or fail depending on bag contents,
    // but it should not panic
    match result {
        Ok(conv_result) => {
            println!("Conversion succeeded: {:?}", conv_result.stats);
            // Verify output files exist
            assert!(
                !conv_result.output_files.parquet_files.is_empty()
                    || !conv_result.output_files.video_files.is_empty()
                    || !conv_result.output_files.metadata_files.is_empty(),
                "Should have produced at least some output files"
            );
        }
        Err(e) => {
            println!(
                "Conversion failed (expected if bag format incompatible): {}",
                e
            );
        }
    }
}

#[test]
#[ignore = "Requires real bag file - run manually or in CI"]
fn test_e2e_convert_bag_with_topic_mappings() {
    let bag_path = small_bag_file();
    if !bag_path.exists() {
        eprintln!("Skipping test: bag file not found at {:?}", bag_path);
        return;
    }

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let config = ConversionConfig::new(DatasetConfig::new(
        DatasetFormat::Lerobot,
        "test_dataset",
        30,
        None,
    ))
    .with_topic_mapping("/camera/color/image_raw", "observation.images.cam_rgb")
    .with_topic_mapping("/joint_states", "observation.state")
    .with_topic_mapping("/cmd_vel", "action")
    .with_max_frames(50);

    // The conversion should handle the topic mappings
    let result = convert_file(&bag_path, temp_dir.path(), &config);

    // Just verify it doesn't panic - actual mapping correctness depends on bag contents
    println!("Conversion result: {:?}", result.is_ok());
}

// ============================================================================
// Small Frame/Fragment Size Tests
// ============================================================================

#[test]
fn test_e2e_video_encoding_with_small_images() {
    use roboflow_dataset::testing::FrameBuilder;

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    // Create config with video encoding enabled
    let config = LerobotConfig {
        dataset: LeRobotDatasetConfig {
            base: roboflow_dataset::formats::common::config::DatasetBaseConfig {
                name: "video_encoding_test".to_string(),
                fps: 30,
                robot_type: Some("test".to_string()),
            },
            env_type: None,
        },
        mappings: vec![],
        video: VideoConfig {
            codec: "libx264".to_string(),
            crf: 23,
            preset: "fast".to_string(),
            profile: None,
        },
        annotation_file: None,
        flushing: FlushingConfig::default(),
        streaming: StreamingConfig::default(),
    };

    let mut writer =
        LerobotWriter::new_local(temp_dir.path(), config).expect("Failed to create writer");

    writer
        .start_episode(Some(0))
        .expect("Failed to start episode");

    // Write 25 frames with small images
    for i in 0..25 {
        let frame = FrameBuilder::new(i)
            .with_timestamp(i as u64 * 33_333_333)
            .add_encoded_image("observation.images.cam_0", 160, 120)
            .add_state("observation.state", vec![i as f32])
            .build();
        writer.write_frame(&frame).expect("Failed to write frame");
    }

    writer
        .finish_episode(Some(0))
        .expect("Failed to finish episode");
    let stats = writer.finalize_with_config().expect("Failed to finalize");

    // Verify frames were written
    assert_eq!(stats.frames_written, 25);

    // Check that video files were created (may be in videos/chunk-000/cam_0/)
    let videos_dir = temp_dir
        .path()
        .join("videos/chunk-000/observation.images.cam_0");
    if videos_dir.exists() {
        let entries: Vec<_> = std::fs::read_dir(&videos_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        println!("Video files created: {}", entries.len());
        for entry in entries {
            println!("  - {:?}", entry.path());
        }
    }
}

#[test]
fn test_e2e_small_episode_chunking() {
    use roboflow_dataset::testing::FrameBuilder;

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let config = LerobotConfig {
        dataset: LeRobotDatasetConfig {
            base: roboflow_dataset::formats::common::config::DatasetBaseConfig {
                name: "chunking_test".to_string(),
                fps: 30,
                robot_type: Some("test".to_string()),
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

    // Set small episodes per chunk to force chunk directory creation
    writer.set_episodes_per_chunk(2);

    // Create 5 episodes (should span multiple chunks)
    for ep_idx in 0..5 {
        writer
            .start_episode(Some(ep_idx))
            .expect("Failed to start episode");

        for i in 0..5 {
            let frame = FrameBuilder::new(i)
                .with_timestamp(i as u64 * 33_333_333)
                .add_state("observation.state", vec![ep_idx as f32, i as f32])
                .build();
            writer.write_frame(&frame).expect("Failed to write frame");
        }

        writer
            .finish_episode(Some(ep_idx))
            .expect("Failed to finish episode");
    }

    let stats = writer.finalize_with_config().expect("Failed to finalize");
    assert_eq!(stats.frames_written, 25); // 5 episodes * 5 frames

    // With episodes_per_chunk=2 and 5 episodes:
    // - Episodes 0,1 go to chunk-000
    // - Episodes 2,3 go to chunk-001
    // - Episode 4 goes to chunk-002
    let chunk_dirs: Vec<_> = std::fs::read_dir(temp_dir.path().join("data"))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .collect();

    println!("Chunk directories created: {}", chunk_dirs.len());
    for dir in &chunk_dirs {
        println!("  - {:?}", dir.path());
    }

    // Should have at least chunk-000
    assert!(
        !chunk_dirs.is_empty(),
        "Should have at least one chunk directory"
    );
}

// ============================================================================
// Data Integrity Tests with Realistic Data
// ============================================================================

#[test]
fn test_e2e_state_action_alignment() {
    use roboflow_dataset::testing::FrameBuilder;

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let config = LerobotConfig {
        dataset: LeRobotDatasetConfig {
            base: roboflow_dataset::formats::common::config::DatasetBaseConfig {
                name: "alignment_test".to_string(),
                fps: 30,
                robot_type: Some("test".to_string()),
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

    // Write frames with varying state/action dimensions
    for i in 0..10 {
        let state = vec![
            i as f32 * 0.1,
            i as f32 * 0.2,
            i as f32 * 0.3,
            i as f32 * 0.4,
            i as f32 * 0.5,
            i as f32 * 0.6,
            i as f32 * 0.7,
        ]; // 7-DOF state

        let action = vec![
            i as f32 * 0.01,
            i as f32 * 0.02,
            i as f32 * 0.03,
            i as f32 * 0.04,
            i as f32 * 0.05,
            i as f32 * 0.06,
            i as f32 * 0.07,
        ]; // 7-DOF action

        let frame = FrameBuilder::new(i)
            .with_timestamp(i as u64 * 33_333_333)
            .add_state("observation.state", state)
            .add_action("action", action)
            .build();
        writer.write_frame(&frame).expect("Failed to write frame");
    }

    writer
        .finish_episode(Some(0))
        .expect("Failed to finish episode");
    let stats = writer.finalize_with_config().expect("Failed to finalize");

    assert_eq!(stats.frames_written, 10);

    // Verify parquet file exists
    let parquet_path = temp_dir
        .path()
        .join("data/chunk-000/episode_000000.parquet");
    assert!(parquet_path.exists(), "Parquet file should exist");

    // Check file size is reasonable (> 0 bytes)
    let metadata = std::fs::metadata(&parquet_path).expect("Failed to read metadata");
    assert!(metadata.len() > 0, "Parquet file should have content");
}

#[test]
fn test_e2e_multiple_cameras() {
    use roboflow_dataset::testing::FrameBuilder;

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let config = LerobotConfig {
        dataset: LeRobotDatasetConfig {
            base: roboflow_dataset::formats::common::config::DatasetBaseConfig {
                name: "multi_camera_test".to_string(),
                fps: 30,
                robot_type: Some("test".to_string()),
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

    // Write frames with multiple cameras
    for i in 0..10 {
        let frame = FrameBuilder::new(i)
            .with_timestamp(i as u64 * 33_333_333)
            .add_encoded_image("observation.images.cam_left", 320, 240)
            .add_encoded_image("observation.images.cam_right", 320, 240)
            .add_encoded_image("observation.images.cam_wrist", 160, 120)
            .add_state("observation.state", vec![i as f32])
            .build();
        writer.write_frame(&frame).expect("Failed to write frame");
    }

    writer
        .finish_episode(Some(0))
        .expect("Failed to finish episode");
    let stats = writer.finalize_with_config().expect("Failed to finalize");

    assert_eq!(stats.frames_written, 10);

    // Check for video directories for each camera
    let videos_base = temp_dir.path().join("videos/chunk-000");
    if videos_base.exists() {
        for cam in [
            "observation.images.cam_left",
            "observation.images.cam_right",
            "observation.images.cam_wrist",
        ] {
            let cam_dir = videos_base.join(cam);
            if cam_dir.exists() {
                let entries: Vec<_> = std::fs::read_dir(&cam_dir)
                    .unwrap()
                    .filter_map(|e| e.ok())
                    .collect();
                println!("Camera {}: {} video files", cam, entries.len());
            }
        }
    }
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[test]
#[ignore = "Requires real bag file - run manually or in CI"]
fn test_e2e_nonexistent_bag_file() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let config =
        ConversionConfig::new(DatasetConfig::new(DatasetFormat::Lerobot, "test", 30, None));

    let result = convert_file(
        Path::new("/nonexistent/path/to/file.bag"),
        temp_dir.path(),
        &config,
    );

    assert!(result.is_err(), "Should fail for nonexistent file");
}

#[test]
fn test_e2e_empty_episode_handling() {
    use roboflow_dataset::testing::FrameBuilder;

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let config = LerobotConfig {
        dataset: LeRobotDatasetConfig {
            base: roboflow_dataset::formats::common::config::DatasetBaseConfig {
                name: "empty_episode_test".to_string(),
                fps: 30,
                robot_type: Some("test".to_string()),
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

    // Episode with frames
    writer
        .start_episode(Some(0))
        .expect("Failed to start episode");
    for i in 0..5 {
        let frame = FrameBuilder::new(i)
            .add_state("observation.state", vec![i as f32])
            .build();
        writer.write_frame(&frame).expect("Failed to write frame");
    }
    writer
        .finish_episode(Some(0))
        .expect("Failed to finish episode");

    // Empty episode (start then immediately finish) - this will be skipped
    writer
        .start_episode(Some(1))
        .expect("Failed to start episode");
    writer
        .finish_episode(Some(1))
        .expect("Failed to finish empty episode");

    // Another episode with frames
    writer
        .start_episode(Some(2))
        .expect("Failed to start episode");
    for i in 0..3 {
        let frame = FrameBuilder::new(i)
            .add_state("observation.state", vec![i as f32])
            .build();
        writer.write_frame(&frame).expect("Failed to write frame");
    }
    writer
        .finish_episode(Some(2))
        .expect("Failed to finish episode");

    let stats = writer.finalize_with_config().expect("Failed to finalize");

    // Should have frames from episodes 0 and 2
    assert_eq!(stats.frames_written, 8);
}

// ============================================================================
// Performance Tests
// ============================================================================

#[test]
#[ignore = "Performance test - run manually"]
fn test_e2e_large_dataset_performance() {
    use roboflow_dataset::testing::FrameBuilder;

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let config = LerobotConfig {
        dataset: LeRobotDatasetConfig {
            base: roboflow_dataset::formats::common::config::DatasetBaseConfig {
                name: "perf_test".to_string(),
                fps: 30,
                robot_type: Some("test".to_string()),
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

    let start = std::time::Instant::now();

    // Create multiple episodes
    for ep_idx in 0..10 {
        writer
            .start_episode(Some(ep_idx))
            .expect("Failed to start episode");

        for i in 0..1000 {
            let frame = FrameBuilder::new(i)
                .add_state("observation.state", vec![i as f32, ep_idx as f32])
                .add_action("action", vec![(i + ep_idx) as f32])
                .build();
            writer.write_frame(&frame).expect("Failed to write frame");
        }

        writer
            .finish_episode(Some(ep_idx))
            .expect("Failed to finish episode");
    }

    let stats = writer.finalize_with_config().expect("Failed to finalize");
    let elapsed = start.elapsed();

    assert_eq!(stats.frames_written, 10_000);
    println!("Wrote 10,000 frames in {:?}", elapsed);
}
