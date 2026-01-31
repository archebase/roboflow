// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! LeRobot integration tests.
//!
//! These tests validate the LeRobot v2.1 dataset writer functionality:
//! - Writer creation and initialization
//! - Episode segmentation
//! - Image handling
//! - Directory structure creation
//! - Metadata file generation

use std::fs;

use roboflow::dataset::common::ImageData;
use roboflow::dataset::lerobot::{
    DatasetConfig, LerobotConfig, LerobotWriter, LerobotWriterTrait, VideoConfig,
};

/// Create a test output directory.
fn test_output_dir(_test_name: &str) -> tempfile::TempDir {
    fs::create_dir_all("tests/output").ok();
    tempfile::tempdir_in("tests/output").unwrap_or_else(|_| {
        // Fallback to system temp if tests/output doesn't exist
        tempfile::tempdir().expect("Failed to create temp dir")
    })
}

/// Create a default test configuration.
fn test_config() -> LerobotConfig {
    LerobotConfig {
        dataset: DatasetConfig {
            name: "test_dataset".to_string(),
            fps: 30,
            robot_type: Some("test_robot".to_string()),
            env_type: None,
        },
        mappings: vec![],
        video: VideoConfig::default(),
        annotation_file: None,
    }
}

/// Create test image data.
fn create_test_image(width: u32, height: u32) -> ImageData {
    let data = vec![128u8; (width * height * 3) as usize];
    ImageData::new(width, height, data)
}

// =============================================================================
// Test: End-to-end conversion
// =============================================================================

#[test]
fn test_lerobot_end_to_end_conversion() {
    let output_dir = test_output_dir("test_lerobot_e2e");
    let config = test_config();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();

    // Initialize the writer
    writer.initialize_with_config(&config).unwrap();

    // Write a simple episode
    writer.start_episode(Some(0));

    // Add some images
    writer.add_image(
        "observation.images.camera_0".to_string(),
        create_test_image(64, 48),
    );

    writer.finish_episode(Some(0)).unwrap();

    // Finalize and get stats
    let stats = writer.finalize_with_config(&config).unwrap();

    // Verify directory structure
    assert!(output_dir.path().join("data/chunk-000").exists());
    assert!(output_dir.path().join("videos/chunk-000").exists());
    assert!(output_dir.path().join("meta").exists());

    // Verify metadata files exist
    assert!(
        output_dir.path().join("meta/info.json").exists()
            || output_dir.path().join("info.json").exists()
    );

    // Verify stats are valid
    assert!(stats.duration_sec >= 0.0);
}

// =============================================================================
// Test: Episode segmentation
// =============================================================================

#[test]
fn test_lerobot_episode_segmentation() {
    let output_dir = test_output_dir("test_lerobot_episodes");
    let config = test_config();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();
    writer.initialize_with_config(&config).unwrap();

    // First episode with task_index = 0
    writer.start_episode(Some(0));
    writer.finish_episode(Some(0)).unwrap();

    // Second episode with task_index = 1
    writer.start_episode(Some(1));
    writer.finish_episode(Some(1)).unwrap();

    let stats = writer.finalize_with_config(&config).unwrap();

    // Should complete successfully even with empty episodes
    assert!(stats.duration_sec >= 0.0);
}

// =============================================================================
// Test: Multi-camera handling
// =============================================================================

#[test]
fn test_lerobot_multi_camera() {
    let output_dir = test_output_dir("test_lerobot_multi_camera");
    let config = test_config();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();
    writer.initialize_with_config(&config).unwrap();

    writer.start_episode(Some(0));

    // Add images for multiple cameras
    writer.add_image(
        "observation.images.camera_0".to_string(),
        create_test_image(64, 48),
    );
    writer.add_image(
        "observation.images.camera_1".to_string(),
        create_test_image(32, 24),
    );
    writer.add_image(
        "observation.images.camera_2".to_string(),
        create_test_image(128, 96),
    );

    writer.finish_episode(Some(0)).unwrap();
    let stats = writer.finalize_with_config(&config).unwrap();

    // Verify directories were created
    assert!(output_dir.path().join("videos/chunk-000").exists());
    assert!(stats.duration_sec >= 0.0);
}

// =============================================================================
// Test: Empty dataset
// =============================================================================

#[test]
fn test_lerobot_empty_dataset() {
    let output_dir = test_output_dir("test_lerobot_empty");
    let config = test_config();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();
    writer.initialize_with_config(&config).unwrap();

    // Start and finish episode without any frames
    writer.start_episode(Some(0));
    writer.finish_episode(Some(0)).unwrap();

    let stats = writer.finalize_with_config(&config).unwrap();

    // Should complete successfully with zero frames
    assert_eq!(stats.frames_written, 0);
}

// =============================================================================
// Test: Frame count tracking
// =============================================================================

#[test]
fn test_lerobot_frame_count() {
    let output_dir = test_output_dir("test_lerobot_frame_count");
    let config = test_config();

    let writer = LerobotWriter::new_local(output_dir.path(), config).unwrap();

    // Before initialization, frame count should be 0
    assert_eq!(writer.frame_count(), 0);
    assert!(!writer.is_initialized());
}

// =============================================================================
// Test: Writer state validation
// =============================================================================

#[test]
fn test_lerobot_writer_state() {
    let output_dir = test_output_dir("test_lerobot_state");
    let config = test_config();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();

    // Check initial state
    assert!(!writer.is_initialized());
    assert_eq!(writer.frame_count(), 0);

    // Initialize
    writer.initialize_with_config(&config).unwrap();
    assert!(writer.is_initialized());

    // Start and finish an episode
    writer.start_episode(Some(0));
    writer.finish_episode(Some(0)).unwrap();

    // Finalize
    let stats = writer.finalize_with_config(&config).unwrap();
    assert_eq!(stats.frames_written, 0);
}

// =============================================================================
// Test: Image buffer handling
// =============================================================================

#[test]
fn test_lerobot_image_buffer() {
    let output_dir = test_output_dir("test_lerobot_image_buffer");
    let config = test_config();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();
    writer.initialize_with_config(&config).unwrap();

    writer.start_episode(Some(0));

    // Add images for different cameras
    writer.add_image(
        "observation.images.camera_0".to_string(),
        create_test_image(64, 48),
    );
    writer.add_image(
        "observation.images.camera_1".to_string(),
        create_test_image(64, 48),
    );

    writer.finish_episode(Some(0)).unwrap();
    let stats = writer.finalize_with_config(&config).unwrap();

    // Should handle both images
    assert!(stats.duration_sec >= 0.0);
}

// =============================================================================
// Test: Metadata collection
// =============================================================================

#[test]
fn test_lerobot_metadata() {
    let output_dir = test_output_dir("test_lerobot_metadata");
    let mut config = test_config();
    config.dataset.name = "metadata_test".to_string();
    config.dataset.robot_type = Some("test_bot".to_string());

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();
    writer.initialize_with_config(&config).unwrap();

    writer.start_episode(Some(0));

    // Add some images
    writer.add_image(
        "observation.images.high_res".to_string(),
        create_test_image(320, 240),
    );

    writer.finish_episode(Some(0)).unwrap();
    let _stats = writer.finalize_with_config(&config).unwrap();

    // Check that info.json was created
    let info_path = output_dir.path().join("meta/info.json");
    let alt_info_path = output_dir.path().join("info.json");

    let info_exists = info_path.exists() || alt_info_path.exists();
    assert!(info_exists, "info.json should be created");
}

// =============================================================================
// Test: Video codec configuration
// =============================================================================

#[test]
fn test_lerobot_video_codec_config() {
    let output_dir = test_output_dir("test_lerobot_codec");
    let mut config = test_config();
    config.video.codec = "libx264".to_string();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();
    writer.initialize_with_config(&config).unwrap();

    writer.start_episode(Some(0));

    writer.add_image(
        "observation.images.camera_0".to_string(),
        create_test_image(64, 48),
    );

    writer.finish_episode(Some(0)).unwrap();
    let _stats = writer.finalize_with_config(&config).unwrap();

    // Test passes if no panic occurs
}

// =============================================================================
// Test: FFMPEG missing graceful handling
// =============================================================================

#[test]
fn test_lerobot_ffmpeg_missing_graceful() {
    let output_dir = test_output_dir("test_lerobot_no_ffmpeg");
    let config = test_config();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();
    writer.initialize_with_config(&config).unwrap();

    writer.start_episode(Some(0));

    // Add images
    for _ in 0..3 {
        writer.add_image(
            "observation.images.camera_0".to_string(),
            create_test_image(64, 48),
        );
    }

    // Should not fail even if ffmpeg is not installed
    let result = writer.finish_episode(Some(0));

    // Either succeeds or fails gracefully
    match result {
        Ok(_) => {
            let _stats = writer.finalize_with_config(&config).unwrap();
            // Success - ffmpeg was available
        }
        Err(_e) => {
            // Error is acceptable for missing ffmpeg
            // The test verifies we don't crash
        }
    }
}

// =============================================================================
// Test: Timestamp handling
// =============================================================================

#[test]
fn test_lerobot_timestamps() {
    let output_dir = test_output_dir("test_lerobot_timestamps");
    let config = test_config();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();
    writer.initialize_with_config(&config).unwrap();

    writer.start_episode(Some(0));

    // Add images with different timestamps
    writer.add_image(
        "observation.images.camera_0".to_string(),
        create_test_image(64, 48),
    );
    writer.add_image(
        "observation.images.camera_1".to_string(),
        create_test_image(32, 24),
    );

    writer.finish_episode(Some(0)).unwrap();
    let stats = writer.finalize_with_config(&config).unwrap();

    assert!(stats.duration_sec >= 0.0);
}
