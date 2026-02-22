// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Integration tests for video encoding functionality.

use roboflow_media::ImageData;
use roboflow_media::video::{
    ConcurrentEncoderConfig, ConcurrentVideoEncoder, EncodingResult, OutputConfig, PixelFormat,
    VideoEncoder, VideoEncoderConfig,
};
use std::path::PathBuf;

/// Helper to create test RGB image data.
fn create_test_rgb_image(width: u32, height: u32, value: u8) -> Vec<u8> {
    vec![value; width as usize * height as usize * 3]
}

/// Helper to create test ImageData.
fn create_test_image_data(width: u32, height: u32, value: u8) -> ImageData {
    let data = create_test_rgb_image(width, height, value);
    ImageData::new(width, height, data)
}

#[test]
fn test_video_encoder_config_default() {
    let config = VideoEncoderConfig::default();
    assert!(!config.codec.is_empty());
    assert!(!config.pixel_format.is_empty());
    assert_eq!(config.fps, 30);
    assert_eq!(config.crf, 23);
}

#[test]
fn test_concurrent_encoder_config_default() {
    let config = ConcurrentEncoderConfig::new();
    // Just verify it creates without panic
    let _ = &config.video_config;
}

#[test]
fn test_concurrent_encoder_config_with_video_config() {
    let video_config = VideoEncoderConfig::default();
    let config = ConcurrentEncoderConfig::with_video_config(video_config.clone());
    assert_eq!(config.video_config.fps, video_config.fps);
}

#[test]
fn test_concurrent_encoder_basic() {
    let config = ConcurrentEncoderConfig::new();
    let result = ConcurrentVideoEncoder::new(config);

    assert!(result.is_ok(), "Should create concurrent encoder");
}

#[test]
fn test_concurrent_encoder_single_camera() {
    let temp_dir = tempfile::tempdir().unwrap();
    let output_path = temp_dir.path().join("test.mp4");

    let config = ConcurrentEncoderConfig::new();
    let mut encoder = ConcurrentVideoEncoder::new(config).unwrap();

    let result = encoder.add_camera("cam0", output_path.clone());
    assert!(result.is_ok(), "Should add camera successfully");

    // Add some frames
    for i in 0..5 {
        let image = create_test_image_data(64, 64, (i * 50) as u8);
        let result = encoder.add_frame("cam0", image);
        assert!(result.is_ok(), "Should add frame {}", i);
    }

    // Finalize
    let results = encoder.finalize().unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].frames_encoded, 5);
}

#[test]
fn test_concurrent_encoder_multiple_cameras() {
    let temp_dir = tempfile::tempdir().unwrap();

    let config = ConcurrentEncoderConfig::new();
    let mut encoder = ConcurrentVideoEncoder::new(config).unwrap();

    // Add multiple cameras
    for cam in ["cam0", "cam1", "cam2"] {
        let output_path = temp_dir.path().join(format!("{}.mp4", cam));
        encoder.add_camera(cam, output_path).unwrap();
    }

    // Add frames to each camera
    for cam in ["cam0", "cam1", "cam2"] {
        for i in 0..3 {
            let image = create_test_image_data(64, 64, (i * 50) as u8);
            encoder.add_frame(cam, image).unwrap();
        }
    }

    let results = encoder.finalize().unwrap();
    assert_eq!(results.len(), 3);

    for result in &results {
        assert_eq!(result.frames_encoded, 3);
    }
}

#[test]
fn test_single_video_encoder_file_output() {
    let temp_dir = tempfile::tempdir().unwrap();
    let output_path = temp_dir.path().join("output.mp4");

    let config = VideoEncoderConfig::default();
    let output = OutputConfig::file(&output_path);

    let mut encoder = VideoEncoder::new(config, output).unwrap();

    // Add frames
    for i in 0..10 {
        let data = create_test_rgb_image(320, 240, (i * 25) as u8);
        encoder.encode_frame(&data, 320, 240).unwrap();
    }

    let result = encoder.finalize().unwrap();

    assert!(result.output_path.is_some());
    assert_eq!(result.frames_encoded, 10);
    assert_eq!(result.output_path.unwrap(), output_path);
    assert!(output_path.exists(), "Output file should exist");
}

#[test]
fn test_single_video_encoder_dimension_mismatch_error() {
    let temp_dir = tempfile::tempdir().unwrap();
    let output_path = temp_dir.path().join("dimension_test.mp4");

    let config = VideoEncoderConfig::default();
    let output = OutputConfig::file(&output_path);

    let mut encoder = VideoEncoder::new(config, output).unwrap();

    // Add first frame to establish dimensions
    let data1 = create_test_rgb_image(160, 120, 128);
    encoder.encode_frame(&data1, 160, 120).unwrap();

    // Try to add a frame with different dimensions - should error
    let data2 = create_test_rgb_image(320, 240, 128);
    let result = encoder.encode_frame(&data2, 320, 240);

    assert!(result.is_err(), "Should error on dimension mismatch");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("dimension mismatch") || err_msg.contains("Dimension"),
        "Error should mention dimension mismatch"
    );
}

#[test]
fn test_pixel_format_exists() {
    // Test that PixelFormat enum values exist
    let _ = PixelFormat::Rgb24;
    let _ = PixelFormat::Yuv420p;
    let _ = PixelFormat::Nv12;
}

#[test]
fn test_concurrent_encoder_invalid_camera() {
    let _temp_dir = tempfile::tempdir().unwrap();
    let config = ConcurrentEncoderConfig::new();
    let mut encoder = ConcurrentVideoEncoder::new(config).unwrap();

    // Add frame to non-existent camera
    let image = create_test_image_data(64, 64, 128);
    let result = encoder.add_frame("nonexistent", image);

    assert!(
        result.is_err(),
        "Should fail to add frame to non-existent camera"
    );
}

#[test]
fn test_concurrent_encoder_add_camera_after_finalize() {
    let temp_dir = tempfile::tempdir().unwrap();
    let output_path = temp_dir.path().join("test.mp4");

    let config = ConcurrentEncoderConfig::new();
    let mut encoder = ConcurrentVideoEncoder::new(config).unwrap();
    encoder.add_camera("cam0", output_path).unwrap();

    // Add a frame so the encoder has work
    let image = create_test_image_data(64, 64, 128);
    encoder.add_frame("cam0", image).unwrap();

    // Finalize consumes the encoder - this test verifies finalize works
    let results = encoder.finalize().unwrap();
    assert_eq!(results.len(), 1);

    // After finalize, the encoder is gone - we can't test operations on it
    // This is correct behavior - finalize takes ownership
}

#[test]
fn test_concurrent_encoder_empty_frames() {
    let temp_dir = tempfile::tempdir().unwrap();
    let output_path = temp_dir.path().join("empty.mp4");

    let config = ConcurrentEncoderConfig::new();
    let mut encoder = ConcurrentVideoEncoder::new(config).unwrap();
    encoder.add_camera("cam0", output_path).unwrap();

    // Finalize without adding any frames
    let results = encoder.finalize().unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].frames_encoded, 0);
}

#[test]
fn test_concurrent_encoder_with_large_images() {
    let temp_dir = tempfile::tempdir().unwrap();
    let output_path = temp_dir.path().join("large.mp4");

    let config = ConcurrentEncoderConfig::new();
    let mut encoder = ConcurrentVideoEncoder::new(config).unwrap();
    encoder.add_camera("cam0", output_path).unwrap();

    // Add some 1080p frames
    for i in 0..3 {
        let data = create_test_rgb_image(1920, 1080, (i * 80) as u8);
        let image = ImageData::new(1920, 1080, data);
        encoder.add_frame("cam0", image).unwrap();
    }

    let results = encoder.finalize().unwrap();
    assert_eq!(results[0].frames_encoded, 3);
}

#[test]
fn test_concurrent_encoder_multiple_episodes() {
    let temp_dir = tempfile::tempdir().unwrap();

    for episode in 0..2 {
        let output_path = temp_dir.path().join(format!("episode_{}.mp4", episode));

        let config = ConcurrentEncoderConfig::new();
        let mut encoder = ConcurrentVideoEncoder::new(config).unwrap();
        encoder.add_camera("cam0", output_path).unwrap();

        // Add frames
        for i in 0..3 {
            let image = create_test_image_data(64, 64, (episode * 100 + i * 20) as u8);
            encoder.add_frame("cam0", image).unwrap();
        }

        let results = encoder.finalize().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].frames_encoded, 3);
    }
}

#[test]
fn test_concurrent_encoder_result_fields() {
    let result = roboflow_media::video::ConcurrentEncoderResult {
        camera: "cam0".to_string(),
        output_path: PathBuf::from("/test.mp4"),
        frames_encoded: 100,
        frames_skipped: 2,
    };

    assert_eq!(result.camera, "cam0");
    assert_eq!(result.frames_encoded, 100);
    assert_eq!(result.frames_skipped, 2);
}

#[test]
fn test_image_data_for_video_encoding() {
    // Test creating ImageData suitable for video encoding
    let width = 640u32;
    let height = 480u32;
    let data = create_test_rgb_image(width, height, 128);

    let image = ImageData::new(width, height, data);
    assert_eq!(image.width, width);
    assert_eq!(image.height, height);
    assert_eq!(image.data.len(), (width * height * 3) as usize);
    assert!(image.validate());
}

#[test]
fn test_output_config_file() {
    let path = PathBuf::from("/tmp/test_output.mp4");
    let config = OutputConfig::file(&path);

    match config {
        OutputConfig::File { path: p } => assert_eq!(p, path),
        _ => panic!("Expected File output config"),
    }
}

#[test]
fn test_output_config_channel() {
    let (tx, _rx) = std::sync::mpsc::channel();
    let chunk_size = 1024usize;
    let config = OutputConfig::channel(tx, chunk_size);

    match config {
        OutputConfig::Channel {
            tx: _,
            chunk_size: cs,
        } => {
            assert_eq!(cs, chunk_size);
        }
        _ => panic!("Expected Channel output config"),
    }
}

#[test]
fn test_encoding_result_structure() {
    let result = EncodingResult {
        output_path: Some(PathBuf::from("/test.mp4")),
        frames_encoded: 100,
        bytes_written: 50000,
        dimensions: (1920, 1080),
        codec: "h264".to_string(),
    };

    assert_eq!(result.frames_encoded, 100);
    assert_eq!(result.bytes_written, 50000);
    assert_eq!(result.dimensions, (1920, 1080));
    assert_eq!(result.codec, "h264");
    assert!(result.output_path.is_some());
}

#[test]
fn test_concurrent_encoder_result_ordering() {
    let temp_dir = tempfile::tempdir().unwrap();

    let config = ConcurrentEncoderConfig::new();
    let mut encoder = ConcurrentVideoEncoder::new(config).unwrap();

    // Add cameras in specific order
    let cameras = vec!["cam2", "cam0", "cam1"];
    for cam in &cameras {
        let output_path = temp_dir.path().join(format!("{}.mp4", cam));
        encoder.add_camera(cam, output_path).unwrap();
    }

    // Add frames
    for cam in &cameras {
        let image = create_test_image_data(64, 64, 128);
        encoder.add_frame(cam, image).unwrap();
    }

    let results = encoder.finalize().unwrap();
    assert_eq!(results.len(), 3);

    // Results should be returned for all cameras
    let camera_names: Vec<&str> = results.iter().map(|r| r.camera.as_str()).collect();
    for cam in &cameras {
        assert!(
            camera_names.contains(cam),
            "Camera {} should be in results",
            cam
        );
    }
}

#[test]
fn test_video_encoder_config_fields() {
    let config = VideoEncoderConfig::default();

    // Verify all expected fields exist
    assert!(!config.codec.is_empty());
    assert!(!config.pixel_format.is_empty());
    assert!(config.fps > 0);
    assert!(config.crf <= 51); // CRF range is 0-51
    assert!(!config.preset.is_empty());
}

#[test]
fn test_concurrent_encoder_config_clone() {
    let config = ConcurrentEncoderConfig::new();
    let cloned = config.clone();

    // Both should have same video_config
    assert_eq!(config.video_config.fps, cloned.video_config.fps);
    assert_eq!(config.video_config.crf, cloned.video_config.crf);
}

#[test]
fn test_single_video_encoder_to_file_exists() {
    let temp_dir = tempfile::tempdir().unwrap();
    let output_path = temp_dir.path().join("existence_test.mp4");

    let config = VideoEncoderConfig::default();
    let output = OutputConfig::file(&output_path);

    let mut encoder = VideoEncoder::new(config, output).unwrap();

    let data = create_test_rgb_image(160, 120, 128);
    encoder.encode_frame(&data, 160, 120).unwrap();

    let result = encoder.finalize().unwrap();
    assert!(result.output_path.is_some());
    assert!(
        result.output_path.unwrap().exists(),
        "Output file should exist on disk"
    );
}

#[test]
fn test_video_encoder_with_channel_output() {
    let (tx, rx) = std::sync::mpsc::channel();
    let config = VideoEncoderConfig::default();
    let output = OutputConfig::channel(tx, 512);

    let mut encoder = VideoEncoder::new(config, output).unwrap();

    let data = create_test_rgb_image(160, 120, 128);
    encoder.encode_frame(&data, 160, 120).unwrap();

    let result = encoder.finalize().unwrap();
    assert_eq!(result.frames_encoded, 1);

    // Channel should be closed after finalize
    let _ = rx.try_recv(); // Channel should be closed
}

#[test]
fn test_concurrent_encoder_different_fps() {
    let temp_dir = tempfile::tempdir().unwrap();

    // Test with different FPS settings
    for fps in [15, 30, 60] {
        let output_path = temp_dir.path().join(format!("fps_{}.mp4", fps));
        let video_config = VideoEncoderConfig {
            fps,
            ..Default::default()
        };
        let config = ConcurrentEncoderConfig::with_video_config(video_config);
        let mut encoder = ConcurrentVideoEncoder::new(config).unwrap();
        encoder.add_camera("cam0", output_path).unwrap();

        // Add a few frames
        for i in 0..3 {
            let image = create_test_image_data(64, 64, i as u8);
            encoder.add_frame("cam0", image).unwrap();
        }

        let results = encoder.finalize().unwrap();
        assert_eq!(results[0].frames_encoded, 3);
    }
}

#[test]
fn test_video_encoder_with_high_crf() {
    let temp_dir = tempfile::tempdir().unwrap();
    let output_path = temp_dir.path().join("high_quality.mp4");

    let config = VideoEncoderConfig {
        crf: 18,
        ..Default::default()
    };
    let output = OutputConfig::file(&output_path);

    let mut encoder = VideoEncoder::new(config, output).unwrap();

    let data = create_test_rgb_image(160, 120, 128);
    encoder.encode_frame(&data, 160, 120).unwrap();

    let result = encoder.finalize().unwrap();
    assert_eq!(result.frames_encoded, 1);
}

#[test]
fn test_video_encoder_with_low_crf() {
    let temp_dir = tempfile::tempdir().unwrap();
    let output_path = temp_dir.path().join("low_quality.mp4");

    let config = VideoEncoderConfig {
        crf: 28,
        ..Default::default()
    };
    let output = OutputConfig::file(&output_path);

    let mut encoder = VideoEncoder::new(config, output).unwrap();

    let data = create_test_rgb_image(160, 120, 128);
    encoder.encode_frame(&data, 160, 120).unwrap();

    let result = encoder.finalize().unwrap();
    assert_eq!(result.frames_encoded, 1);
}

#[test]
fn test_concurrent_encoder_with_rgba_data() {
    // Test with RGB data (3 bytes per pixel)
    let temp_dir = tempfile::tempdir().unwrap();
    let output_path = temp_dir.path().join("rgb_test.mp4");

    let config = ConcurrentEncoderConfig::new();
    let mut encoder = ConcurrentVideoEncoder::new(config).unwrap();
    encoder.add_camera("cam0", output_path).unwrap();

    // Create proper RGB data
    let rgb_data: Vec<u8> = (0..64 * 64 * 3).map(|i| (i % 256) as u8).collect();
    let image = ImageData::new(64, 64, rgb_data);
    encoder.add_frame("cam0", image).unwrap();

    let results = encoder.finalize().unwrap();
    assert_eq!(results[0].frames_encoded, 1);
}

#[test]
fn test_encoding_result_debug() {
    let result = EncodingResult {
        output_path: Some(PathBuf::from("/test.mp4")),
        frames_encoded: 50,
        bytes_written: 25000,
        dimensions: (1280, 720),
        codec: "h264".to_string(),
    };

    let debug_str = format!("{:?}", result);
    assert!(debug_str.contains("50"));
    assert!(debug_str.contains("25000"));
}

#[test]
fn test_video_encoder_config_with_codec() {
    let config = VideoEncoderConfig::default();
    // The codec should be detected and set based on available hardware
    // Just verify it's a valid codec name
    assert!(!config.codec.is_empty());
}

#[test]
fn test_multiple_encoders_sequential() {
    // Test creating multiple encoders sequentially
    for i in 0..3 {
        let temp_dir = tempfile::tempdir().unwrap();
        let output_path = temp_dir.path().join(format!("seq_{}.mp4", i));

        let config = VideoEncoderConfig::default();
        let output = OutputConfig::file(&output_path);

        let mut encoder = VideoEncoder::new(config, output).unwrap();

        let data = create_test_rgb_image(64, 64, i as u8);
        encoder.encode_frame(&data, 64, 64).unwrap();

        let result = encoder.finalize().unwrap();
        assert_eq!(result.frames_encoded, 1);
        assert!(output_path.exists());
    }
}

#[test]
fn test_concurrent_encoder_skips_no_frames() {
    let temp_dir = tempfile::tempdir().unwrap();
    let output_path = temp_dir.path().join("no_frames.mp4");

    let config = ConcurrentEncoderConfig::new();
    let mut encoder = ConcurrentVideoEncoder::new(config).unwrap();
    encoder.add_camera("cam0", output_path).unwrap();

    // Don't add any frames, just finalize
    let results = encoder.finalize().unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].frames_encoded, 0);
}
