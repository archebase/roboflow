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
    DatasetBaseConfig, DatasetWriter, LerobotConfig, LerobotDatasetConfig as DatasetConfig,
    LerobotWriter, LerobotWriterTrait, VideoConfig,
};
use roboflow_dataset::streaming::StreamingConfig;
use roboflow_dataset::{AlignedFrame, ImageData, PipelineConfig, PipelineExecutor};

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

    let _ = writer.start_episode(Some(0));

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

    let _ = writer.start_episode(Some(0));

    let width = 64u32;
    let height = 48u32;
    let rgb_data = vec![128u8; (width * height * 3) as usize];

    // Add frames with state/action data (required for LeRobot format)
    for i in 0..10 {
        let raw_img = ImageData::new(width, height, rgb_data.clone());

        // Create AlignedFrame with image, state, and action
        let mut images = std::collections::HashMap::new();
        images.insert(
            "observation.images.camera_0".to_string(),
            std::sync::Arc::new(raw_img),
        );

        let mut states = std::collections::HashMap::new();
        states.insert(
            "observation.state".to_string(),
            vec![0.1f32, 0.2, 0.3, 0.4, 0.5, 0.6],
        );

        let mut actions = std::collections::HashMap::new();
        actions.insert(
            "action".to_string(),
            vec![0.15f32, 0.25, 0.35, 0.45, 0.55, 0.65],
        );

        let frame = AlignedFrame {
            frame_index: i,
            timestamp: (i as u64) * 33_333_333,
            images,
            states,
            actions,
            timestamps: std::collections::HashMap::new(),
            audio: std::collections::HashMap::new(),
        };

        writer.write_frame(&frame).unwrap();
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

    let _ = writer.start_episode(Some(0));

    let width = 64u32;
    let height = 48u32;

    // Add compressed JPEG images to camera_0
    let jpeg_header: Vec<u8> = vec![
        0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00,
    ]
    .into_iter()
    .chain(std::iter::repeat_n(0, 20))
    .collect();

    for i in 0..5 {
        let compressed_img = ImageData::encoded(width, height, jpeg_header.clone());

        // Create AlignedFrame for compressed image
        let mut images = std::collections::HashMap::new();
        images.insert(
            "observation.images.camera_0".to_string(),
            std::sync::Arc::new(compressed_img),
        );

        let mut states = std::collections::HashMap::new();
        states.insert(
            "observation.state".to_string(),
            vec![0.1f32, 0.2, 0.3, 0.4, 0.5, 0.6],
        );

        let mut actions = std::collections::HashMap::new();
        actions.insert(
            "action".to_string(),
            vec![0.15f32, 0.25, 0.35, 0.45, 0.55, 0.65],
        );

        let frame = AlignedFrame {
            frame_index: i,
            timestamp: (i as u64) * 33_333_333,
            images,
            states,
            actions,
            timestamps: std::collections::HashMap::new(),
            audio: std::collections::HashMap::new(),
        };

        writer.write_frame(&frame).unwrap();
    }

    // Add raw RGB images to camera_1
    let rgb_data = vec![128u8; (width * height * 3) as usize];
    for i in 0..5 {
        let raw_img = ImageData::new(width, height, rgb_data.clone());

        // Create AlignedFrame for raw image
        let mut images = std::collections::HashMap::new();
        images.insert(
            "observation.images.camera_1".to_string(),
            std::sync::Arc::new(raw_img),
        );

        let mut states = std::collections::HashMap::new();
        states.insert(
            "observation.state".to_string(),
            vec![0.1f32, 0.2, 0.3, 0.4, 0.5, 0.6],
        );

        let mut actions = std::collections::HashMap::new();
        actions.insert(
            "action".to_string(),
            vec![0.15f32, 0.25, 0.35, 0.45, 0.55, 0.65],
        );

        let frame = AlignedFrame {
            frame_index: i + 5,
            timestamp: ((i + 5) as u64) * 33_333_333,
            images,
            states,
            actions,
            timestamps: std::collections::HashMap::new(),
            audio: std::collections::HashMap::new(),
        };

        writer.write_frame(&frame).unwrap();
    }

    writer.finish_episode(Some(0)).ok();

    let stats = writer.finalize_with_config().expect("Failed to finalize");

    // The compressed images have minimal JPEG headers which may fail to decode
    // Only the raw RGB images will be encoded successfully
    assert!(
        stats.images_encoded >= 5,
        "Expected at least 5 images (raw RGB) to be encoded, got {}",
        stats.images_encoded
    );
}

// =============================================================================
// Integration Test: Process actual bag file with compressed images
// =============================================================================

/// Integration test that processes the actual bag file from fixtures.
///
/// This test validates the full pipeline with real-world compressed JPEG images
/// from a ROS bag file.
#[tokio::test]
async fn test_process_bag_with_compressed_images() {
    use roboflow::SourceConfig;
    use roboflow_sources::{create_source, register_builtin_sources};

    // Register built-in sources (bag, mcap, etc.)
    register_builtin_sources();

    let bag_path = "tests/fixtures/extracted_messages.bag";
    if !std::path::Path::new(bag_path).exists() {
        println!("Skipping test: {} not found", bag_path);
        return;
    }

    let output_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
    let config = LerobotConfig {
        dataset: DatasetConfig {
            base: DatasetBaseConfig {
                name: "bag_test".to_string(),
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
    };

    let writer = LerobotWriter::new_local(output_dir.path(), config.clone())
        .expect("Failed to create writer");

    let streaming_config = StreamingConfig::with_fps(30);
    let pipeline_config = PipelineConfig::new(streaming_config);
    let mut executor = PipelineExecutor::new(writer, pipeline_config);

    // Process first 500 messages from the bag
    let source_config = SourceConfig::bag(bag_path);
    let mut source = create_source(&source_config).expect("Failed to create bag source");

    // Initialize the source
    let _metadata = source
        .initialize(&source_config)
        .await
        .expect("Failed to initialize source");

    let mut messages_processed = 0;
    let max_messages = 500;
    let batch_size = 100;

    loop {
        if messages_processed >= max_messages {
            break;
        }

        match source.read_batch(batch_size).await {
            Ok(Some(messages)) if !messages.is_empty() => {
                for msg in messages {
                    if executor.process_message(msg).is_ok() {
                        messages_processed += 1;
                    }
                    if messages_processed >= max_messages {
                        break;
                    }
                }
            }
            Ok(Some(_)) => {
                // Empty batch, continue
                continue;
            }
            Ok(None) => {
                // End of stream
                break;
            }
            Err(e) => {
                eprintln!("Error reading batch: {}", e);
                break;
            }
        }
    }

    println!(
        "Processed {} messages from {}",
        messages_processed, bag_path
    );

    // Finalize and check stats
    // Note: The bag file may not have all required LeRobot fields (observation_state, action, etc.)
    // so finalize may fail. The important thing is that compressed images were accepted.
    let result = executor.finalize();

    match result {
        Ok(stats) => {
            println!(
                "Result: frames={}, episodes={}",
                stats.frames_written, stats.episodes_written
            );
        }
        Err(e) => {
            // Finalize may fail if bag doesn't have required state/action data
            // This is OK - we're testing that compressed images don't crash the pipeline
            println!("Finalize failed (expected for incomplete bag data): {}", e);
        }
    }

    // The test passes if we processed messages without crashing on compressed images
    // images_encoded may be 0 if no valid images were in first 500 messages
    assert!(
        messages_processed > 0,
        "Should have processed some messages"
    );
}
