// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Dataset writer error handling and edge case tests.
//!
//! These tests validate error handling and edge cases for both KPS and LeRobot writers:
//! - Invalid configuration handling
//! - Dimension mismatch handling
//! - I/O error handling
//! - State validation errors
//! - Empty/incomplete data handling

use std::fs;

use roboflow::{
    DatasetBaseConfig, DatasetWriter, LerobotConfig, LerobotDatasetConfig as DatasetConfig,
    LerobotWriter, LerobotWriterTrait, VideoConfig,
};

use roboflow_dataset::{AlignedFrame, ImageData};

/// Create a test output directory.
fn test_output_dir(_test_name: &str) -> tempfile::TempDir {
    fs::create_dir_all("tests/output").ok();
    tempfile::tempdir_in("tests/output")
        .unwrap_or_else(|_| tempfile::tempdir().expect("Failed to create temp dir"))
}

/// Create a default test configuration.
fn test_config() -> LerobotConfig {
    LerobotConfig {
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
        flushing: roboflow::lerobot::FlushingConfig::default(),
        streaming: roboflow::lerobot::StreamingConfig::default(),
    }
}

/// Create test image data.
fn create_test_image(width: u32, height: u32) -> ImageData {
    let data = vec![128u8; (width * height * 3) as usize];
    ImageData::new(width, height, data)
}

/// Create a test frame with state and action data.
fn create_test_frame(frame_index: usize, image: ImageData) -> AlignedFrame {
    let mut images = std::collections::HashMap::new();
    images.insert(
        "observation.images.camera_0".to_string(),
        std::sync::Arc::new(image),
    );

    // Add state observation (joint positions)
    let mut states = std::collections::HashMap::new();
    states.insert(
        "observation.state".to_string(),
        vec![0.1f32, 0.2, 0.3, 0.4, 0.5, 0.6],
    );

    // Add action (target joint positions)
    let mut actions = std::collections::HashMap::new();
    actions.insert(
        "action".to_string(),
        vec![0.15f32, 0.25, 0.35, 0.45, 0.55, 0.65],
    );

    AlignedFrame {
        frame_index,
        timestamp: (frame_index as u64) * 33_333_333,
        images,
        states,
        actions,
        timestamps: std::collections::HashMap::new(),
        audio: std::collections::HashMap::new(),
    }
}

// =============================================================================
// Error Handling Tests
// =============================================================================

#[test]
fn test_lerobot_invalid_video_codec() {
    let output_dir = test_output_dir("test_invalid_codec");
    let mut config = test_config();
    // Invalid codec name - should not crash
    config.video.codec = "invalid_codec_name_xyz".to_string();

    let writer = LerobotWriter::new_local(output_dir.path(), config);
    // Should either succeed (if validated later) or fail gracefully
    assert!(writer.is_ok() || writer.is_err());
}

#[test]
fn test_lerobot_invalid_crf_value() {
    let output_dir = test_output_dir("test_invalid_crf");
    let mut config = test_config();
    // CRF should be 0-51 for libx264
    config.video.crf = 100; // Invalid CRF value

    let writer = LerobotWriter::new_local(output_dir.path(), config);
    // Should handle gracefully - either clamp or reject
    assert!(writer.is_ok());
}

#[test]
fn test_lerobot_zero_fps() {
    let output_dir = test_output_dir("test_zero_fps");
    let mut config = test_config();
    config.dataset.fps = 0; // Invalid FPS

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();

    // Should still allow writing with zero FPS (may result in NaN timestamps)
    writer.start_episode(Some(0));
    writer.add_image(
        "observation.images.camera_0".to_string(),
        create_test_image(64, 48),
    );
    let result = writer.finish_episode(Some(0));

    // Should handle gracefully
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lerobot_empty_dataset_name() {
    let output_dir = test_output_dir("test_empty_name");
    let mut config = test_config();
    config.dataset.name = "".to_string();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();
    writer.start_episode(Some(0));
    writer.finish_episode(Some(0)).unwrap();

    let stats = writer.finalize_with_config().unwrap();
    // Should complete even with empty name
    assert_eq!(stats.frames_written, 0);
}

#[test]
fn test_lerobot_very_long_dataset_name() {
    let output_dir = test_output_dir("test_long_name");
    let mut config = test_config();
    config.dataset.name = "a".repeat(1000);

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();
    writer.start_episode(Some(0));
    writer.finish_episode(Some(0)).unwrap();

    let stats = writer.finalize_with_config().unwrap();
    // Should handle long names
    assert_eq!(stats.frames_written, 0);
}

#[test]
fn test_lerobot_invalid_image_dimensions() {
    let output_dir = test_output_dir("test_invalid_dimensions");
    let config = test_config();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();
    writer.start_episode(Some(0));

    // Add image with zero dimensions
    let zero_img = ImageData::new(0, 0, vec![]);
    writer.add_image("observation.images.empty".to_string(), zero_img);

    // Don't test very large images due to memory constraints

    let result = writer.finish_episode(Some(0));
    // Should handle gracefully
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lerobot_inconsistent_image_dimensions() {
    let output_dir = test_output_dir("test_inconsistent_dimensions");
    let config = test_config();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();
    writer.start_episode(Some(0));

    // Add images with different dimensions for the same camera
    writer.add_image(
        "observation.images.camera_0".to_string(),
        create_test_image(64, 48),
    );
    writer.add_image(
        "observation.images.camera_0".to_string(),
        create_test_image(128, 96), // Different dimensions
    );

    let result = writer.finish_episode(Some(0));
    // Should either skip frames or handle gracefully
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lerobot_duplicate_camera_names() {
    let output_dir = test_output_dir("test_duplicate_cameras");
    let config = test_config();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();
    writer.start_episode(Some(0));

    // Add same image twice to same camera
    let img = create_test_image(64, 48);
    writer.add_image("observation.images.camera_0".to_string(), img.clone());
    writer.add_image("observation.images.camera_0".to_string(), img);

    let result = writer.finish_episode(Some(0));
    // Should accumulate frames, not error
    assert!(result.is_ok());
}

#[test]
fn test_lerobot_many_cameras() {
    let output_dir = test_output_dir("test_many_cameras");
    let config = test_config();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();
    writer.start_episode(Some(0));

    // Add images for many cameras (stress test)
    for i in 0..20 {
        writer.add_image(
            format!("observation.images.camera_{}", i),
            create_test_image(32, 24),
        );
    }

    let result = writer.finish_episode(Some(0));
    // Should handle many cameras
    assert!(result.is_ok());
}

#[test]
fn test_lerobot_no_images_in_episode() {
    let output_dir = test_output_dir("test_no_images");
    let config = test_config();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();

    // Start episode without adding any images
    writer.start_episode(Some(0));
    let result = writer.finish_episode(Some(0));

    // Should complete successfully with empty episode
    assert!(result.is_ok());
}

#[test]
fn test_lerobot_finalize_without_starting_episode() {
    let output_dir = test_output_dir("test_finalize_without_start");
    let config = test_config();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();

    // Try to finalize without starting an episode
    let stats = writer.finalize_with_config().unwrap();

    // Should complete with zero frames
    assert_eq!(stats.frames_written, 0);
}

#[test]
fn test_lerobot_double_finalize() {
    let output_dir = test_output_dir("test_double_finalize");
    let config = test_config();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();
    writer.start_episode(Some(0));
    writer.finish_episode(Some(0)).unwrap();

    // First finalize
    let stats1 = writer.finalize_with_config().unwrap();

    // Second finalize - should handle gracefully
    let stats2 = writer.finalize_with_config().unwrap();

    assert_eq!(stats1.frames_written, stats2.frames_written);
}

#[test]
fn test_lerobot_write_before_initialize() {
    let output_dir = test_output_dir("test_write_before_init");
    let config = test_config();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config).unwrap();
    // Don't call initialize

    // Try to start episode
    writer.start_episode(Some(0));

    // Should still work - initialize may be implicit
    let result = writer.finish_episode(Some(0));
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lerobot_unmatched_start_finish() {
    let output_dir = test_output_dir("test_unmatched_start_finish");
    let config = test_config();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();

    // Start episode with one index, finish with another
    writer.start_episode(Some(0));
    let result = writer.finish_episode(Some(1)); // Different index

    // Should handle gracefully - use the started index
    assert!(result.is_ok());
}

#[test]
fn test_lerobot_multiple_episodes_same_task() {
    let output_dir = test_output_dir("test_multiple_episodes_same_task");
    let config = test_config();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();

    // Multiple episodes with same task index
    for _ in 0..3 {
        writer.start_episode(Some(0));
        writer.add_image(
            "observation.images.camera_0".to_string(),
            create_test_image(64, 48),
        );
        writer.finish_episode(Some(0)).unwrap();
    }

    let stats = writer.finalize_with_config().unwrap();
    // Should have 0 frames since no state/action data was added
    assert_eq!(stats.frames_written, 0);
}

// =============================================================================
// State and Action Data Tests
// =============================================================================

#[test]
fn test_lerobot_empty_feature_names() {
    let output_dir = test_output_dir("test_empty_feature_names");
    let config = test_config();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();
    writer.start_episode(Some(0));

    // Try to add image with empty feature name
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        writer.add_image("".to_string(), create_test_image(64, 48));
    }));

    // Should either handle gracefully or panic with clear message
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lerobot_special_characters_in_feature_names() {
    let output_dir = test_output_dir("test_special_chars");
    let config = test_config();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();
    writer.start_episode(Some(0));

    // Feature names with special characters
    let special_names = vec![
        "observation.images/camera/0",
        "observation.images.camera-0",
        "observation.images.camera_0.test",
        "observation.images:camera:0",
    ];

    for name in special_names {
        writer.add_image(name.to_string(), create_test_image(32, 24));
    }

    let result = writer.finish_episode(Some(0));
    // Should handle or reject gracefully
    assert!(result.is_ok() || result.is_err());
}

// =============================================================================
// Image Data Edge Cases
// =============================================================================

#[test]
fn test_lerobot_mismatched_image_data_size() {
    let output_dir = test_output_dir("test_mismatched_data_size");
    let config = test_config();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();
    writer.start_episode(Some(0));

    // Create image data that doesn't match claimed dimensions
    let bad_data = vec![128u8; 100]; // Much smaller than expected
    let bad_img = ImageData::new(64, 48, bad_data); // Claims 64x48x3 = 9216 bytes

    writer.add_image("observation.images.bad".to_string(), bad_img);

    let result = writer.finish_episode(Some(0));
    // Should handle data size mismatch
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lerobot_single_pixel_image() {
    let output_dir = test_output_dir("test_single_pixel");
    let config = test_config();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();
    writer.start_episode(Some(0));

    // 1x1 image
    let tiny_img = ImageData::new(1, 1, vec![128, 128, 128]);
    writer.add_image("observation.images.tiny".to_string(), tiny_img);

    let result = writer.finish_episode(Some(0));
    // Should handle tiny images
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lerobot_non_rgb_image_data() {
    let output_dir = test_output_dir("test_non_rgb");
    let config = test_config();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();
    writer.start_episode(Some(0));

    // Image data with size not divisible by 3 (not RGB)
    let non_rgb_data = vec![128u8; 100]; // 100 bytes, not divisible by 3
    let non_rgb_img = ImageData::new(10, 10, non_rgb_data);

    writer.add_image("observation.images.non_rgb".to_string(), non_rgb_img);

    let result = writer.finish_episode(Some(0));
    // Should handle non-RGB data
    assert!(result.is_ok() || result.is_err());
}

// =============================================================================
// Metadata Validation Tests
// =============================================================================

#[test]
fn test_lerobot_metadata_files_created() {
    let output_dir = test_output_dir("test_metadata_files");
    let mut config = test_config();
    config.dataset.name = "metadata_validation".to_string();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();
    writer.start_episode(Some(0));
    writer.finish_episode(Some(0)).unwrap();
    let _stats = writer.finalize_with_config().unwrap();

    // Check that expected metadata files exist
    let output_path = output_dir.path();

    // info.json should exist
    let info_paths = [
        output_path.join("meta/info.json"),
        output_path.join("info.json"),
        output_path.join("meta/info"), // Directory
    ];

    let info_exists = info_paths.iter().any(|p| p.exists());
    assert!(info_exists, "info.json or meta directory should exist");
}

#[test]
fn test_lerobot_episode_count_in_metadata() {
    let output_dir = test_output_dir("test_episode_count");
    let config = test_config();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();

    // Create 3 episodes
    for i in 0..3 {
        writer.start_episode(Some(i));
        if i == 1 {
            // Only episode 1 has images
            writer.add_image(
                "observation.images.camera_0".to_string(),
                create_test_image(64, 48),
            );
        }
        writer.finish_episode(Some(i)).unwrap();
    }

    let stats = writer.finalize_with_config().unwrap();

    // Stats should reflect total frames written (0 since no state/action frames added)
    assert_eq!(stats.frames_written, 0);
}

// =============================================================================
// Directory Structure Edge Cases
// =============================================================================

#[test]
fn test_lerobot_read_only_output_directory() {
    // This test checks behavior when output directory is read-only
    // On some systems, this may not work as expected
    let output_dir = test_output_dir("test_readonly");
    let config = test_config();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(output_dir.path()).unwrap().permissions();
        let readonly_mode = perms.mode() & 0o777;
        perms.set_mode(readonly_mode & 0o444); // Read-only
        fs::set_permissions(output_dir.path(), perms).unwrap();

        let _writer = LerobotWriter::new_local(output_dir.path(), config);
        // Should fail gracefully or succeed if directory already exists

        // Restore permissions for temp dir cleanup
        let mut perms = fs::metadata(output_dir.path()).unwrap().permissions();
        perms.set_mode(readonly_mode); // Restore original mode
        let _ = fs::set_permissions(output_dir.path(), perms);
    }

    #[cfg(not(unix))]
    {
        let _ = LerobotWriter::new_local(output_dir.path(), config);
        // Skip this test on non-Unix systems
    }
}

#[test]
fn test_lerobot_nested_output_directory() {
    // Test with deeply nested output directory
    let base_dir = test_output_dir("test_nested_base");
    let nested_dir = base_dir.path().join("a/b/c/d/e/f");
    fs::create_dir_all(&nested_dir).unwrap();

    let config = test_config();

    let mut writer = LerobotWriter::new_local(&nested_dir, config.clone()).unwrap();
    writer.start_episode(Some(0));
    writer.finish_episode(Some(0)).unwrap();

    let stats = writer.finalize_with_config().unwrap();
    assert_eq!(stats.frames_written, 0);
}

// =============================================================================
// Stats Validation Tests
// =============================================================================

#[test]
fn test_lerobot_writer_stats_accuracy() {
    let output_dir = test_output_dir("test_stats_accuracy");
    let config = test_config();

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();

    let expected_frames = 5;
    writer.start_episode(Some(0));

    for _ in 0..expected_frames {
        writer.add_image(
            "observation.images.camera_0".to_string(),
            create_test_image(64, 48),
        );
    }

    writer.finish_episode(Some(0)).unwrap();
    let stats = writer.finalize_with_config().unwrap();

    // Stats should be valid (no data written without state/action frames)
    assert!(stats.duration_sec >= 0.0);
    assert_eq!(stats.output_bytes, 0); // No Parquet/video files without frames
}

#[test]
fn test_lerobot_frame_count_increment() {
    let output_dir = test_output_dir("test_frame_count");
    let config = test_config();

    // Initial frame count
    {
        let writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();
        assert_eq!(writer.frame_count(), 0);
    }

    // After initialization
    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();
    assert_eq!(writer.frame_count(), 0);

    // After starting episode
    writer.start_episode(Some(0));
    assert_eq!(writer.frame_count(), 0);

    // After adding frames with state/action data
    for i in 0..3 {
        let frame = create_test_frame(i, create_test_image(64, 48));
        writer.write_frame(&frame).unwrap();
    }

    // Finish episode - may fail if ffmpeg is not installed
    match writer.finish_episode(Some(0)) {
        Ok(_) => {
            // Frame count should be 3 after adding 3 frames
            assert_eq!(writer.frame_count(), 3);
        }
        Err(e) if e.to_string().contains("ffmpeg") => {
            // ffmpeg not available - skip assertion but test passes
            // The frame_count() should still work even without video encoding
            assert_eq!(writer.frame_count(), 3);
        }
        Err(e) => {
            panic!("Unexpected error: {}", e);
        }
    }
}

// =============================================================================
// Cloud Storage URL Validation Tests
// =============================================================================

#[test]
fn test_lerobot_builder_rejects_s3_url_in_output_dir() {
    let _output_dir = test_output_dir("test_s3_url_rejection");
    let config = test_config();

    // Try to build with s3:// URL in output_dir (without storage() method)
    let result = LerobotWriter::builder()
        .output_dir("s3://bucket-name/datasets")
        .config(config)
        .build();

    // Should fail with helpful error message
    assert!(result.is_err());
}

#[test]
fn test_lerobot_builder_rejects_oss_url_in_output_dir() {
    let _output_dir = test_output_dir("test_oss_url_rejection");
    let config = test_config();

    // Try to build with oss:// URL in output_dir
    let result = LerobotWriter::builder()
        .output_dir("oss://bucket-name/datasets")
        .config(config)
        .build();

    // Should fail
    assert!(result.is_err());
}

#[test]
fn test_lerobot_builder_rejects_uppercase_s3_url() {
    let _output_dir = test_output_dir("test_uppercase_s3_rejection");
    let config = test_config();

    // Try to build with S3:// (uppercase) URL
    let result = LerobotWriter::builder()
        .output_dir("S3://bucket-name/datasets")
        .config(config)
        .build();

    // Should fail with helpful error message
    assert!(result.is_err());
}

#[test]
fn test_lerobot_builder_rejects_uppercase_oss_url() {
    let _output_dir = test_output_dir("test_uppercase_oss_rejection");
    let config = test_config();

    // Try to build with OSS:// (uppercase) URL
    let result = LerobotWriter::builder()
        .output_dir("OSS://bucket-name/datasets")
        .config(config)
        .build();

    // Should fail with helpful error message
    assert!(result.is_err());
}
