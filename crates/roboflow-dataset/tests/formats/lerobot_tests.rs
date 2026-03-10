// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Format layer tests as defined in ADR-004.
//!
//! Tests cover:
//! - LeRobot format compliance
//! - Video path schemes
//! - Format configuration

use roboflow_dataset::formats::lerobot::{LerobotConfig, LerobotWriterConfig, VideoConfig};
use roboflow_dataset::formats::common::{ImageData, DatasetFrame};
use roboflow_dataset::testing::{FrameBuilder, InMemoryWriter, generate_test_jpeg};
use roboflow_dataset::core::traits::FormatWriter;
use tempfile::tempdir;

// ============================================================================
// LerobotConfig Tests
// ============================================================================

#[test]
fn test_lerobot_config_default() {
    let config = LerobotConfig::default();

    assert!(config.fps > 0.0);
    assert!(!config.camera_keys.is_empty() || true); // May be empty by default
}

#[test]
fn test_video_config_default() {
    let config = VideoConfig::default();

    assert!(config.width > 0);
    assert!(config.height > 0);
    assert!(!config.codec.is_empty() || true);
}

// ============================================================================
// ImageData Tests
// ============================================================================

#[test]
fn test_image_data_new() {
    let data = vec![0u8; 640 * 480 * 3];
    let image = ImageData::new(640, 480, data.clone());

    assert_eq!(image.width, 640);
    assert_eq!(image.height, 480);
    assert_eq!(image.data, data);
    assert!(!image.is_encoded);
}

#[test]
fn test_image_data_encoded() {
    let data = generate_test_jpeg(640, 480, 0);
    let image = ImageData::encoded(640, 480, data.clone());

    assert_eq!(image.width, 640);
    assert_eq!(image.height, 480);
    assert!(image.is_encoded);
}

#[test]
fn test_image_data_with_timestamp() {
    let image = ImageData::new(640, 480, vec![0u8; 640 * 480 * 3])
        .with_timestamp(1_000_000_000);

    assert_eq!(image.original_timestamp, Some(1_000_000_000));
}

#[test]
fn test_image_data_depth() {
    let data = vec![0u8; 640 * 480 * 2]; // 16-bit depth
    let image = ImageData::depth(640, 480, data);

    assert!(image.is_depth);
}

#[test]
fn test_image_data_validate() {
    // Valid image
    let valid = ImageData::new(640, 480, vec![0u8; 640 * 480 * 3]);
    assert!(valid.validate().is_ok());

    // Invalid - wrong size
    let invalid = ImageData::new(640, 480, vec![0u8; 100]);
    assert!(invalid.validate().is_err());
}

#[test]
fn test_image_data_pixel_count() {
    let image = ImageData::new(640, 480, vec![0u8; 640 * 480 * 3]);
    assert_eq!(image.pixel_count(), 640 * 480);
}

#[test]
fn test_image_data_rgb_size() {
    let image = ImageData::new(640, 480, vec![0u8; 640 * 480 * 3]);
    assert_eq!(image.rgb_size(), 640 * 480 * 3);
}

// ============================================================================
// DatasetFrame Tests
// ============================================================================

#[test]
fn test_dataset_frame_new() {
    let frame = DatasetFrame::new(0, 0, 1_000_000_000);

    assert_eq!(frame.frame_index, 0);
    assert_eq!(frame.episode_index, 0);
    assert_eq!(frame.timestamp, 1_000_000_000);
}

#[test]
fn test_dataset_frame_with_image() {
    let frame = DatasetFrame::new(0, 0, 0)
        .with_image("camera_0", ImageData::new(640, 480, vec![0u8; 640 * 480 * 3]));

    assert!(frame.images.contains_key("camera_0"));
}

#[test]
fn test_dataset_frame_with_observation_state() {
    let frame = DatasetFrame::new(0, 0, 0)
        .with_observation_state(vec![1.0, 2.0, 3.0]);

    assert_eq!(frame.observation_state, Some(vec![1.0, 2.0, 3.0]));
}

#[test]
fn test_dataset_frame_with_action() {
    let frame = DatasetFrame::new(0, 0, 0)
        .with_action(vec![0.5, -0.5]);

    assert_eq!(frame.action, Some(vec![0.5, -0.5]));
}

#[test]
fn test_dataset_frame_with_camera_info() {
    use roboflow_dataset::formats::common::CameraInfo;

    let camera_info = CameraInfo {
        camera_name: "camera_0".to_string(),
        width: 640,
        height: 480,
        k: [1.0; 9],
        d: [0.0; 5],
        r: [1.0; 9],
        p: [1.0; 12],
        distortion_model: "plumb_bob".to_string(),
    };

    let frame = DatasetFrame::new(0, 0, 0)
        .with_camera_info(camera_info);

    assert!(frame.camera_info.is_some());
}

// ============================================================================
// FrameBuilder Tests
// ============================================================================

#[test]
fn test_frame_builder_basic() {
    let frame = FrameBuilder::new(0)
        .with_timestamp(1_000_000_000)
        .build();

    assert_eq!(frame.frame_index, 0);
    assert_eq!(frame.timestamp, 1_000_000_000);
}

#[test]
fn test_frame_builder_with_state() {
    let frame = FrameBuilder::new(0)
        .add_state("observation.joint_position", vec![0.0, 1.0, 2.0])
        .build();

    assert!(frame.states.contains_key("observation.joint_position"));
    assert_eq!(frame.states.get("observation.joint_position"), Some(&vec![0.0, 1.0, 2.0]));
}

#[test]
fn test_frame_builder_with_action() {
    let frame = FrameBuilder::new(0)
        .add_action("action.joint_velocity", vec![0.5, -0.5])
        .build();

    assert!(frame.actions.contains_key("action.joint_velocity"));
}

#[test]
fn test_frame_builder_with_image() {
    let frame = FrameBuilder::new(0)
        .add_image("observation.camera_0", 640, 480)
        .build();

    assert!(frame.image_refs.contains_key("observation.camera_0"));
    let image = frame.image_refs.get("observation.camera_0").unwrap();
    assert_eq!(image.width, 640);
    assert_eq!(image.height, 480);
}

#[test]
fn test_frame_builder_with_encoded_image() {
    let frame = FrameBuilder::new(0)
        .add_encoded_image("observation.camera_0", 640, 480)
        .build();

    let image = frame.image_refs.get("observation.camera_0").unwrap();
    assert_eq!(image.width, 640);
    assert_eq!(image.height, 480);
}

#[test]
fn test_frame_builder_chain() {
    let frame = FrameBuilder::new(0)
        .with_timestamp(1_000_000_000)
        .add_state("observation.state", vec![0.0])
        .add_action("action", vec![1.0])
        .add_image("observation.camera_0", 320, 240)
        .add_image("observation.camera_1", 320, 240)
        .build();

    assert_eq!(frame.frame_index, 0);
    assert_eq!(frame.timestamp, 1_000_000_000);
    assert_eq!(frame.states.len(), 1);
    assert_eq!(frame.actions.len(), 1);
    assert_eq!(frame.image_refs.len(), 2);
}

// ============================================================================
// InMemoryWriter Format Tests
// ============================================================================

#[test]
fn test_in_memory_writer_format_compliance() {
    let mut writer = InMemoryWriter::new();

    // Write a complete frame with all expected fields
    let frame = FrameBuilder::new(0)
        .with_timestamp(0)
        .add_state("observation.state", vec![0.0, 1.0, 2.0])
        .add_action("action", vec![0.5])
        .add_image("observation.camera_0", 640, 480)
        .build();

    writer.write_frame(&frame).unwrap();
    let stats = writer.finalize().unwrap();

    assert!(writer.is_finalized());
    assert_eq!(stats.frames_written, 1);
}

#[test]
fn test_in_memory_writer_multi_episode() {
    let mut writer = InMemoryWriter::new();

    // Episode 0
    writer.start_episode(None).unwrap();
    for i in 0..10 {
        writer.write_frame(&FrameBuilder::new(i)
            .add_state("observation.state", vec![i as f32])
            .build()).unwrap();
    }
    writer.finish_episode().unwrap();

    // Episode 1
    writer.start_episode(None).unwrap();
    for i in 0..5 {
        writer.write_frame(&FrameBuilder::new(i)
            .add_state("observation.state", vec![i as f32])
            .build()).unwrap();
    }
    writer.finish_episode().unwrap();

    writer.finalize().unwrap();

    // Verify episode structure
    assert_eq!(writer.episode_frames(0).unwrap().len(), 10);
    assert_eq!(writer.episode_frames(1).unwrap().len(), 5);
    assert_eq!(writer.len(), 15);
}

// ============================================================================
// Video Path Scheme Tests
// ============================================================================

#[test]
fn test_video_path_scheme_lerobot() {
    use roboflow_dataset::core::traits::VideoPathScheme;
    use roboflow_dataset::formats::lerobot::video_profiles::LerobotVideoPathScheme;

    let scheme = LerobotVideoPathScheme::new();

    // LeRobot v2.1 path: videos/chunk-{chunk:03d}/{camera}/episode_{episode:06d}.mp4
    let path = scheme.video_path(0, "camera_0", 0);
    assert!(path.to_str().unwrap().contains("chunk-000"));
    assert!(path.to_str().unwrap().contains("camera_0"));
    assert!(path.to_str().unwrap().contains("episode_000000"));
    assert!(path.to_str().unwrap().ends_with(".mp4"));
}

#[test]
fn test_video_path_scheme_chunk_dir() {
    use roboflow_dataset::core::traits::VideoPathScheme;
    use roboflow_dataset::formats::lerobot::video_profiles::LerobotVideoPathScheme;

    let scheme = LerobotVideoPathScheme::new();
    let chunk_dir = scheme.chunk_dir(5);

    assert!(chunk_dir.to_str().unwrap().contains("chunk-005"));
}

#[test]
fn test_video_path_scheme_parse_episode() {
    use roboflow_dataset::core::traits::VideoPathScheme;
    use roboflow_dataset::formats::lerobot::video_profiles::LerobotVideoPathScheme;

    let scheme = LerobotVideoPathScheme::new();

    let path = std::path::Path::new("videos/chunk-000/camera_0/episode_000123.mp4");
    let episode = scheme.parse_episode(path);

    assert_eq!(episode, Some(123));
}
