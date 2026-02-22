// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Integration tests for video encoding functionality.

use roboflow_media::ImageData;
use roboflow_media::video::{
    EncodingResult, EncodingStrategy, EncodingWorkload, FragmentConfig, FragmentEncoder,
    FragmentOutputConfig, FragmentTriggers, OutputConfig, PixelFormat, StreamConfig, StreamOutput,
    VideoEncoder, VideoEncoderConfig, WorkloadConfig,
};
use std::path::PathBuf;

/// Helper to create test RGB image data.
fn create_test_rgb_image(width: u32, height: u32, value: u8) -> Vec<u8> {
    vec![value; width as usize * height as usize * 3]
}

// =============================================================================
// VideoEncoder Tests (Simple API)
// =============================================================================

#[test]
fn test_video_encoder_config_default() {
    let config = VideoEncoderConfig::default();
    assert!(!config.codec.is_empty());
    assert!(!config.pixel_format.is_empty());
    assert_eq!(config.fps, 30);
    assert_eq!(config.crf, 23);
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

// =============================================================================
// FragmentEncoder Tests (Bounded Memory API)
// =============================================================================

#[test]
fn test_fragment_config_default() {
    let config = FragmentConfig::default();
    assert!(config.max_frames.is_none());
    assert!(config.max_memory_bytes.is_none());
    assert!(config.max_duration_secs.is_none());
}

#[test]
fn test_fragment_config_with_max_frames() {
    let config = FragmentConfig::with_max_frames(300);
    assert_eq!(config.max_frames, Some(300));
}

#[test]
fn test_fragment_encoder_single_file() {
    let temp_dir = tempfile::tempdir().unwrap();
    let output_path = temp_dir.path().join("fragment_output.mp4");

    let config = FragmentConfig::with_max_frames(10);
    let output = FragmentOutputConfig::SingleFile {
        path: output_path.clone(),
    };

    let mut encoder = FragmentEncoder::new(VideoEncoderConfig::default(), output, config).unwrap();

    // Add 5 frames (less than threshold)
    for i in 0..5 {
        let data = create_test_rgb_image(64, 64, (i * 50) as u8);
        encoder.encode_frame(&data, 64, 64).unwrap();
    }

    let result = encoder.finalize().unwrap();
    assert_eq!(result.frames_encoded, 5);
    assert!(result.output_path.is_some());
}

#[test]
fn test_fragment_encoder_auto_flush() {
    let temp_dir = tempfile::tempdir().unwrap();
    let output_path = temp_dir.path().join("auto_flush.mp4");

    // Auto-flush every 5 frames
    let config = FragmentConfig::with_max_frames(5);
    let output = FragmentOutputConfig::SingleFile {
        path: output_path.clone(),
    };

    let mut encoder = FragmentEncoder::new(VideoEncoderConfig::default(), output, config).unwrap();

    // Add 15 frames (should trigger 3 auto-flushes)
    for i in 0..15 {
        let data = create_test_rgb_image(64, 64, (i * 17) as u8);
        encoder.encode_frame(&data, 64, 64).unwrap();
    }

    let result = encoder.finalize().unwrap();
    assert_eq!(result.frames_encoded, 15);
    assert_eq!(result.fragments, 3); // 15 frames / 5 per fragment = 3 fragments
}

#[test]
fn test_fragment_encoder_explicit_flush() {
    let temp_dir = tempfile::tempdir().unwrap();
    let output_path = temp_dir.path().join("explicit_flush.mp4");

    // No auto-flush
    let config = FragmentConfig::default();
    let output = FragmentOutputConfig::SingleFile {
        path: output_path.clone(),
    };

    let mut encoder = FragmentEncoder::new(VideoEncoderConfig::default(), output, config).unwrap();

    // Add 3 frames
    for i in 0..3 {
        let data = create_test_rgb_image(64, 64, (i * 80) as u8);
        encoder.encode_frame(&data, 64, 64).unwrap();
    }

    // Explicit flush
    encoder.flush_fragment().unwrap();

    // Add more frames
    for i in 0..2 {
        let data = create_test_rgb_image(64, 64, ((i + 3) * 50) as u8);
        encoder.encode_frame(&data, 64, 64).unwrap();
    }

    let result = encoder.finalize().unwrap();
    assert_eq!(result.frames_encoded, 5);
    assert_eq!(result.fragments, 2);
}

// =============================================================================
// EncodingWorkload Tests (Unified Multi-Stream API)
// =============================================================================

#[test]
fn test_workload_config_default() {
    let config = WorkloadConfig::default();
    // Verify config exists
    let _ = &config;
}

#[test]
fn test_encoding_strategy_default() {
    let strategy = EncodingStrategy::default();
    assert!(matches!(strategy, EncodingStrategy::Standard));
}

#[test]
fn test_encoding_workload_new() {
    let workload = EncodingWorkload::new(WorkloadConfig::default());
    assert!(workload.is_ok());
}

#[test]
fn test_workload_single_stream() {
    let temp_dir = tempfile::tempdir().unwrap();
    let output_path = temp_dir.path().join("workload_single.mp4");

    let mut workload = EncodingWorkload::new(WorkloadConfig::default()).unwrap();

    // Add a stream
    let stream_config = StreamConfig::new("cam0", StreamOutput::file(output_path.clone()));
    workload.add_stream(stream_config).unwrap();

    // Submit frames
    for i in 0..5 {
        let data = create_test_rgb_image(64, 64, (i * 50) as u8);
        workload.submit_frame("cam0", &data, 64, 64).unwrap();
    }

    // Finalize
    let result = workload.finalize().unwrap();
    assert_eq!(result.streams.len(), 1);
    assert_eq!(result.total_frames, 5);
}

#[test]
fn test_workload_multiple_streams() {
    let temp_dir = tempfile::tempdir().unwrap();

    let mut workload = EncodingWorkload::new(WorkloadConfig::default()).unwrap();

    // Add multiple streams
    for cam in ["cam0", "cam1", "cam2"] {
        let output_path = temp_dir.path().join(format!("{}.mp4", cam));
        let stream_config = StreamConfig::new(cam, StreamOutput::file(output_path));
        workload.add_stream(stream_config).unwrap();
    }

    // Submit frames to each stream
    for cam in ["cam0", "cam1", "cam2"] {
        for i in 0..3 {
            let data = create_test_rgb_image(64, 64, (i * 50) as u8);
            workload.submit_frame(cam, &data, 64, 64).unwrap();
        }
    }

    // Finalize
    let result = workload.finalize().unwrap();
    assert_eq!(result.streams.len(), 3);
    assert_eq!(result.total_frames, 9); // 3 streams * 3 frames each
}

#[test]
fn test_workload_with_fragment_strategy() {
    let temp_dir = tempfile::tempdir().unwrap();
    let output_path = temp_dir.path().join("fragment_workload.mp4");

    let mut workload = EncodingWorkload::new(WorkloadConfig::default()).unwrap();

    // Add a stream with fragment strategy
    let stream_config = StreamConfig::new("cam0", StreamOutput::file(output_path))
        .with_strategy(EncodingStrategy::fragment_by_frames(5));
    workload.add_stream(stream_config).unwrap();

    // Submit frames
    for i in 0..15 {
        let data = create_test_rgb_image(64, 64, (i * 17) as u8);
        workload.submit_frame("cam0", &data, 64, 64).unwrap();
    }

    // Finalize
    let result = workload.finalize().unwrap();
    assert_eq!(result.streams.len(), 1);
    assert_eq!(result.total_frames, 15);
}

#[test]
fn test_workload_invalid_stream() {
    let workload = EncodingWorkload::new(WorkloadConfig::default()).unwrap();
    let data = create_test_rgb_image(64, 64, 128);
    let result = workload.submit_frame("nonexistent", &data, 64, 64);

    assert!(
        result.is_err(),
        "Should fail to submit to non-existent stream"
    );
}

#[test]
fn test_stream_config_builder() {
    let temp_dir = tempfile::tempdir().unwrap();
    let output_path = temp_dir.path().join("builder_test.mp4");

    // Test builder pattern
    let config = StreamConfig::new("cam0", StreamOutput::file(output_path.clone()))
        .with_strategy(EncodingStrategy::fragment_by_frames(100));

    assert_eq!(config.id.as_str(), "cam0");
    assert!(matches!(config.strategy, EncodingStrategy::Fragment { .. }));
}

#[test]
fn test_fragment_triggers() {
    let triggers = FragmentTriggers {
        frame_count: Some(300),
        memory_bytes: Some(100 * 1024 * 1024),
        duration_secs: Some(10.0),
    };

    assert_eq!(triggers.frame_count, Some(300));
    assert_eq!(triggers.memory_bytes, Some(100 * 1024 * 1024));
    assert_eq!(triggers.duration_secs, Some(10.0));
}

#[test]
fn test_workload_empty_finalize() {
    let mut workload = EncodingWorkload::new(WorkloadConfig::default()).unwrap();

    // Add a stream but don't submit any frames
    let temp_dir = tempfile::tempdir().unwrap();
    let output_path = temp_dir.path().join("empty.mp4");
    let stream_config = StreamConfig::new("cam0", StreamOutput::file(output_path));
    workload.add_stream(stream_config).unwrap();

    // Finalize without frames
    let result = workload.finalize().unwrap();
    assert_eq!(result.streams.len(), 1);
    assert_eq!(result.total_frames, 0);
}

#[test]
fn test_workload_large_frames() {
    let temp_dir = tempfile::tempdir().unwrap();
    let output_path = temp_dir.path().join("large.mp4");

    let mut workload = EncodingWorkload::new(WorkloadConfig::default()).unwrap();

    let stream_config = StreamConfig::new("cam0", StreamOutput::file(output_path));
    workload.add_stream(stream_config).unwrap();

    // Submit 1080p frames
    for i in 0..3 {
        let data = create_test_rgb_image(1920, 1080, (i * 80) as u8);
        workload.submit_frame("cam0", &data, 1920, 1080).unwrap();
    }

    let result = workload.finalize().unwrap();
    assert_eq!(result.total_frames, 3);
}
