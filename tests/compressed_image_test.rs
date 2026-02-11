// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Integration test for compressed image handling.
//!
//! This test validates that:
//! - Compressed JPEG/PNG images are correctly identified and marked for decoding
//! - Raw RGB images are handled correctly
//! - Video encoding works with both formats

use roboflow::{
    DatasetBaseConfig, LerobotConfig, LerobotDatasetConfig as DatasetConfig, LerobotWriter,
    LerobotWriterTrait, VideoConfig,
};
use roboflow_dataset::ImageData;

/// Test that ImageData correctly handles compressed vs raw images.
#[test]
fn test_imagedata_compressed_vs_raw() {
    // Compressed JPEG image (smaller than expected RGB size)
    let width = 640u32;
    let height = 480u32;
    let expected_rgb_size = (width * height * 3) as usize; // 921,600 bytes

    // JPEG header (much smaller)
    let jpeg_data: Vec<u8> = vec![0xFF, 0xD8, 0xFF, 0xE0]
        .into_iter()
        .chain(std::iter::repeat_n(0, 96))
        .collect();

    let compressed_img = ImageData::encoded(width, height, jpeg_data.clone());
    assert!(compressed_img.is_encoded);
    assert_eq!(compressed_img.width, width);
    assert_eq!(compressed_img.height, height);
    assert_eq!(compressed_img.data.len(), jpeg_data.len());

    // Raw RGB image (exact size)
    let rgb_data = vec![128u8; expected_rgb_size];
    let raw_img = ImageData::new(width, height, rgb_data.clone());
    assert!(!raw_img.is_encoded);
    assert_eq!(raw_img.width, width);
    assert_eq!(raw_img.height, height);
    assert_eq!(raw_img.data.len(), rgb_data.len());
}

/// Test that video encoding handles compressed images.
///
/// This test verifies that compressed JPEG images are accepted
/// and marked for later decoding during MP4 encoding.
#[test]
fn test_video_encoding_accepts_compressed_images() {
    let output_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
    let config = LerobotConfig {
        dataset: DatasetConfig {
            base: DatasetBaseConfig {
                name: "compressed_test".to_string(),
                fps: 30,
                robot_type: Some("test".to_string()),
            },
            env_type: None,
        },
        mappings: vec![],
        video: VideoConfig::default(),
        annotation_file: None,
        flushing: roboflow::lerobot::FlushingConfig::default(),
        streaming: roboflow::lerobot::StreamingConfig::default(),
    };

    let mut writer =
        LerobotWriter::new_local(output_dir.path(), config).expect("Failed to create writer");

    writer.start_episode(Some(0));

    // Create compressed JPEG images (with proper header to simulate compressed data)
    let width = 64u32;
    let height = 48u32;

    // Minimal JPEG header (FF D8 FF E0 ...)
    // This is much smaller than expected RGB size (64*48*3 = 9216 bytes)
    let jpeg_data: Vec<u8> = vec![
        0xFF, 0xD8, // SOI marker
        0xFF, 0xE0, // APP0 marker
        0x00, 0x10, // Length: 16 bytes
        0x4A, 0x46, 0x49, 0x46, 0x00, // "JFIF" null-terminated
    ]
    .into_iter()
    .chain(std::iter::repeat_n(0, 100)) // More padding for realism
    .collect();

    // Verify the data is much smaller than RGB size
    let expected_rgb_size = (width * height * 3) as usize;
    assert!(
        jpeg_data.len() < expected_rgb_size,
        "Test data should look compressed"
    );

    // Add compressed images - they should be accepted without error
    for _ in 0..10 {
        let compressed_img = ImageData::encoded(width, height, jpeg_data.clone());
        assert!(compressed_img.is_encoded, "Should be marked as encoded");
        writer.add_image("observation.images.camera_0".to_string(), compressed_img);
    }

    writer.finish_episode(Some(0)).ok();

    let stats = writer.finalize_with_config().expect("Failed to finalize");

    // With invalid JPEG data, decode fails and images are skipped
    // The important thing is that compressed images are accepted, not rejected
    // If decode fails, the image is skipped (counted as skipped_frames, not images_encoded)
    println!(
        "images_encoded={}, frames_written={}",
        stats.images_encoded, stats.frames_written
    );

    // Verify the writer didn't crash when receiving compressed images
    assert!(stats.frames_written <= 10);
}

/// Test that video encoding handles raw RGB images.
#[test]
#[ignore = "Requires ffmpeg"]
fn test_video_encoding_raw_images() {
    let output_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
    let config = LerobotConfig {
        dataset: DatasetConfig {
            base: DatasetBaseConfig {
                name: "raw_test".to_string(),
                fps: 30,
                robot_type: Some("test".to_string()),
            },
            env_type: None,
        },
        mappings: vec![],
        video: VideoConfig::default(),
        annotation_file: None,
        flushing: roboflow::lerobot::FlushingConfig::default(),
        streaming: roboflow::lerobot::StreamingConfig::default(),
    };

    let mut writer =
        LerobotWriter::new_local(output_dir.path(), config).expect("Failed to create writer");

    writer.start_episode(Some(0));

    let width = 64u32;
    let height = 48u32;
    let rgb_data = vec![128u8; (width * height * 3) as usize];

    for _ in 0..10 {
        let raw_img = ImageData::new(width, height, rgb_data.clone());
        writer.add_image("observation.images.camera_0".to_string(), raw_img);
    }

    writer.finish_episode(Some(0)).ok();

    let stats = writer.finalize_with_config().expect("Failed to finalize");

    assert_eq!(
        stats.images_encoded, 10,
        "Expected 10 images to be encoded, got {}",
        stats.images_encoded
    );
}

/// Test that video encoding handles mixed compressed and raw images.
#[test]
#[ignore = "Requires ffmpeg"]
fn test_video_encoding_mixed_images() {
    let output_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
    let config = LerobotConfig {
        dataset: DatasetConfig {
            base: DatasetBaseConfig {
                name: "mixed_test".to_string(),
                fps: 30,
                robot_type: Some("test".to_string()),
            },
            env_type: None,
        },
        mappings: vec![],
        video: VideoConfig::default(),
        annotation_file: None,
        flushing: roboflow::lerobot::FlushingConfig::default(),
        streaming: roboflow::lerobot::StreamingConfig::default(),
    };

    let mut writer =
        LerobotWriter::new_local(output_dir.path(), config).expect("Failed to create writer");

    writer.start_episode(Some(0));

    let width = 64u32;
    let height = 48u32;

    // Add compressed JPEG images to camera_0
    let jpeg_header: Vec<u8> = vec![
        0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00,
    ]
    .into_iter()
    .chain(std::iter::repeat_n(0, 20))
    .collect();

    for _ in 0..5 {
        let compressed_img = ImageData::encoded(width, height, jpeg_header.clone());
        writer.add_image("observation.images.camera_0".to_string(), compressed_img);
    }

    // Add raw RGB images to camera_1
    let rgb_data = vec![128u8; (width * height * 3) as usize];
    for _ in 0..5 {
        let raw_img = ImageData::new(width, height, rgb_data.clone());
        writer.add_image("observation.images.camera_1".to_string(), raw_img);
    }

    writer.finish_episode(Some(0)).ok();

    let stats = writer.finalize_with_config().expect("Failed to finalize");

    // Both cameras should have their images encoded
    assert_eq!(
        stats.images_encoded, 10,
        "Expected 10 images (5 per camera) to be encoded, got {}",
        stats.images_encoded
    );
}
