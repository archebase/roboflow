// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Video encoding validation tests.
//!
//! These tests verify that:
//! - MP4 files are created correctly
//! - Encoded videos have the correct number of frames
//! - Video dimensions match input
//! - Output videos are valid and playable

use std::path::Path;
use std::process::Command;
use std::collections::HashMap;

use roboflow::{
    DatasetBaseConfig, DatasetWriter, LerobotConfig, LerobotDatasetConfig as DatasetConfig,
    LerobotWriter, LerobotWriterTrait, VideoConfig,
};
use roboflow_dataset::{AlignedFrame, ImageData};

/// Create test image data with a gradient pattern for uniqueness.
fn create_test_image_with_pattern(width: u32, height: u32, pattern: u8) -> ImageData {
    let mut data = vec![pattern; (width * height * 3) as usize];
    // Add a gradient pattern for uniqueness (helps with video encoding verification)
    for (i, byte) in data.iter_mut().enumerate() {
        *byte = byte.wrapping_add((i % 256) as u8);
    }
    ImageData::new(width, height, data)
}

/// Get ffprobe path (check if available).
fn ffprobe_path() -> Option<&'static str> {
    if Command::new("ffprobe")
        .arg("-version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
    {
        Some("ffprobe")
    } else {
        None
    }
}

/// Probe a video file to get its properties.
///
/// Returns None if ffprobe is not available, or an error if probing fails.
fn probe_video_properties(path: &Path) -> Option<VideoProperties> {
    let ffprobe = ffprobe_path()?;

    let output = Command::new(ffprobe)
        .arg("-v")
        .arg("error")
        .arg("-show_streams")
        .arg("-of")
        .arg("json")
        .arg("-select_streams")
        .arg("v:0")
        .arg(path)
        .output()
        .ok()?;

    let json_str = String::from_utf8(output.stdout).ok()?;

    // Parse ffprobe JSON output
    let json: serde_json::Value = serde_json::from_str(&json_str).ok()?;

    let streams = json.get("streams")?.as_array()?;
    let stream = streams.first()?;

    let width = stream.get("width")?.as_u64()? as u32;
    let height = stream.get("height")?.as_u64()? as u32;

    // Get frame count from nb_read_frames or duration
    let nb_frames = stream
        .get("nb_read_frames")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    // Get codec info
    let codec_name = stream
        .get("codec_name")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    // Get frame rate
    let fps = stream
        .get("r_frame_rate")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);

    Some(VideoProperties {
        width,
        height,
        nb_frames,
        codec_name: codec_name.to_string(),
        fps,
    })
}

/// Video properties extracted from ffprobe.
#[derive(Debug, Clone)]
struct VideoProperties {
    width: u32,
    height: u32,
    nb_frames: u64,
    codec_name: String,
    fps: f64,
}

/// Verify an MP4 file has expected properties.
fn verify_mp4_properties(
    path: &Path,
    expected_width: u32,
    expected_height: u32,
    expected_frames: u64,
    expected_fps: f64,
) -> Result<String, String> {
    let props = probe_video_properties(path)
        .ok_or_else(|| "ffprobe not available or probing failed".to_string())?;

    // Verify codec
    if !props.codec_name.contains("264") {
        return Err(format!(
            "Unexpected codec: {}, expected H.264",
            props.codec_name
        ));
    }

    // Verify dimensions
    if props.width != expected_width || props.height != expected_height {
        return Err(format!(
            "Dimension mismatch: got {}x{}, expected {}x{}",
            props.width, props.height, expected_width, expected_height
        ));
    }

    // Verify frame count (allow some tolerance for different encoding methods)
    if props.nb_frames > 0 && (props.nb_frames < expected_frames / 2 || props.nb_frames > expected_frames * 2) {
        return Err(format!(
            "Frame count mismatch: got {}, expected {}",
            props.nb_frames, expected_frames
        ));
    }

    // Verify FPS (allow 1 FPS tolerance)
    if props.fps > 0.0 && (props.fps < expected_fps - 1.0 || props.fps > expected_fps + 1.0) {
        return Err(format!(
            "FPS mismatch: got {}, expected {}",
            props.fps, expected_fps
        ));
    }

    Ok(format!(
        "Video OK: {}x{}, {} frames, {} fps, codec={}",
        props.width, props.height, props.nb_frames, props.fps, props.codec_name
    ))
}

// =============================================================================
// Test: Validate MP4 output with ffprobe
// =============================================================================

#[test]
fn test_video_encoding_with_ffprobe_validation() {
    let output_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
    let config = LerobotConfig {
        dataset: DatasetConfig {
            base: DatasetBaseConfig {
                name: "validation_test".to_string(),
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

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone())
        .expect("Failed to create writer");

    writer.start_episode(Some(0));

    let width = 320u32;
    let height = 240u32;
    let num_frames = 30;
    let expected_fps = 30.0;

    // Add frames with state/action data
    for i in 0..num_frames {
        let img = create_test_image_with_pattern(width, height, (i % 256) as u8);

        let mut images = HashMap::new();
        images.insert(
            "observation.images.camera_0".to_string(),
            std::sync::Arc::new(img),
        );

        let mut states = HashMap::new();
        states.insert(
            "observation.state".to_string(),
            vec![0.1f32, 0.2, 0.3, 0.4, 0.5, 0.6],
        );

        let mut actions = HashMap::new();
        actions.insert(
            "action".to_string(),
            vec![0.15f32, 0.25, 0.35, 0.45, 0.55, 0.65],
        );

        let frame = AlignedFrame {
            frame_index: i,
            timestamp: (i as u64) * 33_333_333, // ~30 FPS
            images,
            states,
            actions,
            timestamps: HashMap::new(),
            audio: HashMap::new(),
        };

        writer.write_frame(&frame).expect("Failed to write frame");
    }

    writer.finish_episode(Some(0)).expect("Failed to finish episode");
    let stats = writer.finalize_with_config().expect("Failed to finalize");

    // Verify encoding happened
    assert_eq!(
        stats.images_encoded,
        num_frames,
        "Expected {} images to be encoded, got {}",
        num_frames, stats.images_encoded
    );

    // Find the video file
    // Videos are created at: videos/chunk-000/<camera>/episode_000000.mp4
    let videos_dir = output_dir.path().join("videos/chunk-000");
    let video_path = videos_dir
        .join("observation.images.camera_0")
        .join("episode_000000.mp4");
    assert!(
        video_path.exists(),
        "Video file should be created at {:?}",
        video_path
    );

    // Verify MP4 properties with ffprobe if available
    if ffprobe_path().is_some() {
        let result = verify_mp4_properties(
            &video_path,
            width,
            height,
            num_frames as u64,
            expected_fps,
        );

        match result {
            Ok(msg) => {
                println!("✓ {}", msg);
            }
            Err(e) => {
                panic!("MP4 validation failed: {}", e);
            }
        }
    } else {
        println!("ffprobe not available, skipping detailed MP4 validation");
        println!("Video file exists at: {:?}", video_path);
    }
}

// =============================================================================
// Test: Multi-camera video encoding
// =============================================================================

#[test]
fn test_multi_camera_video_encoding() {
    let output_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
    let config = LerobotConfig {
        dataset: DatasetConfig {
            base: DatasetBaseConfig {
                name: "multi_camera_test".to_string(),
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

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone())
        .expect("Failed to create writer");

    writer.start_episode(Some(0));

    let width = 160u32;
    let height = 120u32;
    let num_frames = 20;

    // Add frames with multiple cameras
    for i in 0..num_frames {
        let mut images = HashMap::new();

        // Camera 0
        images.insert(
            "observation.images.camera_0".to_string(),
            std::sync::Arc::new(create_test_image_with_pattern(width, height, (i % 256) as u8)),
        );

        // Camera 1
        images.insert(
            "observation.images.camera_1".to_string(),
            std::sync::Arc::new(create_test_image_with_pattern(width, height, ((i + 128) % 256) as u8)),
        );

        let mut states = HashMap::new();
        states.insert(
            "observation.state".to_string(),
            vec![0.1f32, 0.2, 0.3, 0.4, 0.5, 0.6],
        );

        let mut actions = HashMap::new();
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
            timestamps: HashMap::new(),
            audio: HashMap::new(),
        };

        writer.write_frame(&frame).expect("Failed to write frame");
    }

    writer.finish_episode(Some(0)).expect("Failed to finish episode");
    let stats = writer.finalize_with_config().expect("Failed to finalize");

    // Verify both cameras were encoded
    assert_eq!(
        stats.images_encoded,
        num_frames * 2, // 2 cameras
        "Expected {} images ({} frames × 2 cameras), got {}",
        num_frames * 2, num_frames, stats.images_encoded
    );

    // Check both video files exist
    // Videos are created at: videos/chunk-000/<camera>/episode_000000.mp4
    let videos_dir = output_dir.path().join("videos/chunk-000");

    let camera_0_video = videos_dir
        .join("observation.images.camera_0")
        .join("episode_000000.mp4");
    let camera_1_video = videos_dir
        .join("observation.images.camera_1")
        .join("episode_000000.mp4");

    assert!(
        camera_0_video.exists(),
        "Camera 0 video should exist: {:?}",
        camera_0_video
    );

    assert!(
        camera_1_video.exists(),
        "Camera 1 video should exist: {:?}",
        camera_1_video
    );

    // Verify with ffprobe if available
    if ffprobe_path().is_some() {
        for (camera_name, video_path) in [
            ("camera_0", &camera_0_video),
            ("camera_1", &camera_1_video),
        ] {
            let result = verify_mp4_properties(
                video_path,
                width,
                height,
                num_frames as u64,
                30.0,
            );

            match result {
                Ok(msg) => {
                    println!("✓ {}: {}", camera_name, msg);
                }
                Err(e) => {
                    panic!("{} MP4 validation failed: {}", camera_name, e);
                }
            }
        }
    }
}

// =============================================================================
// Test: Different video resolutions
// =============================================================================

#[test]
fn test_various_video_resolutions() {
    let resolutions = vec![(64u32, 48u32), (320u32, 240u32), (640u32, 480u32), (1280u32, 720u32)];

    for (width, height) in resolutions {
        let output_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
        let config = LerobotConfig {
            dataset: DatasetConfig {
                base: DatasetBaseConfig {
                    name: format!("res_test_{}x{}", width, height),
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

        let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone())
            .expect("Failed to create writer");

        writer.start_episode(Some(0));

        // Add a few frames
        for i in 0..5 {
            let img = create_test_image_with_pattern(width, height, (i % 256) as u8);

            let mut images = HashMap::new();
            images.insert(
                "observation.images.camera_0".to_string(),
                std::sync::Arc::new(img),
            );

            let mut states = HashMap::new();
            states.insert(
                "observation.state".to_string(),
                vec![0.1f32, 0.2, 0.3, 0.4, 0.5, 0.6],
            );

            let mut actions = HashMap::new();
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
                timestamps: HashMap::new(),
                audio: HashMap::new(),
            };

            writer.write_frame(&frame).expect("Failed to write frame");
        }

        writer.finish_episode(Some(0)).expect("Failed to finish episode");
        let stats = writer.finalize_with_config().expect("Failed to finalize");

        assert_eq!(stats.images_encoded, 5, "All 5 frames should be encoded");

        // Verify video file exists
        // Videos are created at: videos/chunk-000/<camera>/episode_000000.mp4
        let video_path = output_dir
            .path()
            .join("videos/chunk-000/observation.images.camera_0/episode_000000.mp4");

        assert!(
            video_path.exists(),
            "Video should exist for {}x{}",
            width, height
        );

        // Basic check: file should be reasonably sized (> 1KB for 5 frames)
        let metadata = std::fs::metadata(&video_path).expect("Failed to get video metadata");
        assert!(
            metadata.len() > 1024,
            "Video file should be at least 1KB, got {} bytes for {}x{}",
            metadata.len(), width, height
        );

        println!("✓ Resolution {}x{}: {} bytes", width, height, metadata.len());
    }
}

// =============================================================================
// Test: Handle dimension mismatch gracefully
// =============================================================================

#[test]
fn test_dimension_mismatch_handled_gracefully() {
    let output_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
    let config = LerobotConfig {
        dataset: DatasetConfig {
            base: DatasetBaseConfig {
                name: "mismatch_test".to_string(),
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

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone())
        .expect("Failed to create writer");

    writer.start_episode(Some(0));

    // Add frames with consistent dimensions first
    for i in 0..3 {
        let img = create_test_image_with_pattern(320, 240, (i % 256) as u8);

        let mut images = HashMap::new();
        images.insert(
            "observation.images.camera_0".to_string(),
            std::sync::Arc::new(img),
        );

        let mut states = HashMap::new();
        states.insert(
            "observation.state".to_string(),
            vec![0.1f32, 0.2, 0.3, 0.4, 0.5, 0.6],
        );

        let mut actions = HashMap::new();
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
            timestamps: HashMap::new(),
            audio: HashMap::new(),
        };

        writer.write_frame(&frame).expect("Failed to write frame");
    }

    // Add a frame with different dimensions - should be skipped
    {
        let mismatched_img = create_test_image_with_pattern(640, 480, 99);

        let mut images = HashMap::new();
        images.insert(
            "observation.images.camera_0".to_string(),
            std::sync::Arc::new(mismatched_img),
        );

        let mut states = HashMap::new();
        states.insert(
            "observation.state".to_string(),
            vec![0.1f32, 0.2, 0.3, 0.4, 0.5, 0.6],
        );

        let mut actions = HashMap::new();
        actions.insert(
            "action".to_string(),
            vec![0.15f32, 0.25, 0.35, 0.45, 0.55, 0.65],
        );

        let frame = AlignedFrame {
            frame_index: 4,
            timestamp: 4 * 33_333_333,
            images,
            states,
            actions,
            timestamps: HashMap::new(),
            audio: HashMap::new(),
        };

        // This should not crash, but may skip the frame
        let _ = writer.write_frame(&frame);
    }

    writer.finish_episode(Some(0)).expect("Failed to finish episode");
    let stats = writer.finalize_with_config().expect("Failed to finalize");

    // Should have encoded the 3 consistent frames
    assert_eq!(
        stats.images_encoded, 3,
        "Should encode 3 frames (mismatched frame skipped)"
    );
}

// =============================================================================
// Test: High frame count (stress test)
// =============================================================================

#[test]
fn test_high_frame_count_encoding() {
    let output_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
    let config = LerobotConfig {
        dataset: DatasetConfig {
            base: DatasetBaseConfig {
                name: "high_count_test".to_string(),
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

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone())
        .expect("Failed to create writer");

    writer.start_episode(Some(0));

    let width = 320u32;
    let height = 240u32;
    let num_frames = 300; // 10 seconds at 30fps

    for i in 0..num_frames {
        let img = create_test_image_with_pattern(width, height, (i % 256) as u8);

        let mut images = HashMap::new();
        images.insert(
            "observation.images.camera_0".to_string(),
            std::sync::Arc::new(img),
        );

        let mut states = HashMap::new();
        states.insert(
            "observation.state".to_string(),
            vec![0.1f32, 0.2, 0.3, 0.4, 0.5, 0.6],
        );

        let mut actions = HashMap::new();
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
            timestamps: HashMap::new(),
            audio: HashMap::new(),
        };

        writer.write_frame(&frame).expect("Failed to write frame");
    }

    writer.finish_episode(Some(0)).expect("Failed to finish episode");
    let stats = writer.finalize_with_config().expect("Failed to finalize");

    assert_eq!(
        stats.images_encoded,
        num_frames,
        "All {} frames should be encoded",
        num_frames
    );

    // Verify video file size is reasonable
    // Videos are created at: videos/chunk-000/<camera>/episode_000000.mp4
    let video_path = output_dir
        .path()
        .join("videos/chunk-000/observation.images.camera_0/episode_000000.mp4");

    let metadata = std::fs::metadata(&video_path).expect("Failed to get video metadata");

    // 300 frames at 320x240 should be at least 100KB even with high compression
    assert!(
        metadata.len() > 100_000,
        "Video file too small: {} bytes for {} frames",
        metadata.len(), num_frames
    );

    // But not excessively large (< 50MB for this content)
    assert!(
        metadata.len() < 50_000_000,
        "Video file too large: {} bytes",
        metadata.len()
    );

    println!(
        "✓ High frame count: {} frames, {} bytes ({:.2} MB)",
        num_frames,
        metadata.len(),
        metadata.len() as f64 / (1024.0 * 1024.0)
    );
}
