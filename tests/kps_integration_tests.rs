// Copyright (c) 2026 ArcheBase
// Roboflow is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//     http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

//! KPS integration tests.
//!
//! These tests validate the KPS video encoding and related functionality.

/// Create a test output directory.
fn test_output_dir(_test_name: &str) -> tempfile::TempDir {
    tempfile::tempdir_in("tests/output").unwrap_or_else(|_| {
        // Fallback to system temp if tests/output doesn't exist
        tempfile::tempdir().expect("Failed to create temp dir")
    })
}

// Tests below are commented out - they depend on deleted `pipeline::kps` module
// TODO: Rewrite these tests to use the new KPS writer API directly

/*
/// Test basic KPS pipeline creation.
#[test]
fn test_kps_pipeline_creation() {
    let config = KpsPipelineConfig::default();
    assert_eq!(config.time_aligner.target_fps, 30);
    assert_eq!(config.channel_capacity, 16);
}

/// Test KPS config from file.
#[test]
fn test_kps_config_from_file() {
    let config_path = Path::new("tests/fixtures/kps.toml");
    skip_if_missing!(config_path, "kps.toml");

    let result = KpsPipelineConfig::from_file(config_path);
    if let Ok(config) = result {
        assert_eq!(config.time_aligner.target_fps, 30);
    }
}

/// Test KPS pipeline with a real MCAP file.
#[test]
fn test_kps_pipeline_with_mcap() {
    let fixture_path = Path::new(FIXTURES_DIR).join("robocodec_test_2.mcap");
    skip_if_missing!(fixture_path, "robocodec_test_2.mcap");

    let output_dir = test_output_dir("test_kps_pipeline_with_mcap");

    let kps_config = test_kps_config();
    let pipeline_config = KpsPipelineConfig::from_kps_config(kps_config).with_channel_capacity(16);

    let pipeline = match KpsPipeline::new(&fixture_path, output_dir.path(), pipeline_config) {
        Ok(p) => p,
        Err(e) => {
            eprintln!(
                "Failed to create pipeline (may be expected for some fixtures): {}",
                e
            );
            return;
        }
    };

    match pipeline.run() {
        Ok(report) => {
            println!(
                "KPS conversion complete: {} frames, {} images encoded",
                report.frames_written, report.images_encoded
            );
        }
        Err(e) => {
            eprintln!("Pipeline execution failed (may be expected): {}", e);
        }
    }
}

/// Test KPS pipeline with camera extraction enabled.
#[test]
fn test_kps_pipeline_with_camera_extraction() {
    let fixture_path = Path::new(FIXTURES_DIR).join("robocodec_test_14.mcap");
    skip_if_missing!(fixture_path, "robocodec_test_14.mcap");

    let output_dir = test_output_dir("test_kps_pipeline_with_camera_extraction");

    let kps_config = test_kps_config();

    let mut camera_topics = HashMap::new();
    camera_topics.insert("camera_high".to_string(), "/camera/high".to_string());

    let pipeline_config = KpsPipelineConfig {
        kps_config,
        time_aligner: TimeAlignerConfig::default(),
        camera_extractor: CameraExtractorConfig {
            enabled: true,
            camera_topics,
            parent_frame: "base_link".to_string(),
            camera_info_suffix: "/camera_info".to_string(),
            tf_topic: "/tf".to_string(),
        },
        channel_capacity: 16,
    };

    let pipeline = match KpsPipeline::new(&fixture_path, output_dir.path(), pipeline_config) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Failed to create pipeline: {}", e);
            return;
        }
    };

    match pipeline.run() {
        Ok(report) => {
            println!(
                "KPS conversion with camera extraction: {} frames",
                report.frames_written
            );
        }
        Err(e) => {
            eprintln!("Pipeline execution failed: {}", e);
        }
    }
}

/// Test time alignment configuration.
#[test]
fn test_time_alignment_config() {
    let config = TimeAlignerConfig::default();
    assert_eq!(config.target_fps, 30);
    assert_eq!(config.state_interpolation_max_gap_ns, 100_000_000);
    assert_eq!(config.image_sync_tolerance_ns, 33_333_333);
}

/// Test different time alignment strategies.
#[test]
fn test_time_alignment_strategies() {
    use roboflow::pipeline::kps::traits::time_alignment::{
        HoldLastValue, LinearInterpolation, NearestNeighbor, TimeAlignmentStrategy,
    };

    let linear = LinearInterpolation::new();
    let times = linear
        .generate_target_timestamps(0, 1_000_000_000, 30)
        .unwrap();
    assert!(!times.is_empty());

    let hold = HoldLastValue::new();
    let times = hold
        .generate_target_timestamps(0, 1_000_000_000, 30)
        .unwrap();
    assert!(!times.is_empty());

    let nearest = NearestNeighbor::new();
    let times = nearest
        .generate_target_timestamps(0, 1_000_000_000, 30)
        .unwrap();
    assert!(!times.is_empty());
}
*/

/// Test video encoder with fallback.
#[test]
fn test_video_encoder_fallback() {
    use roboflow::dataset::kps::video_encoder::{
        Mp4Encoder, VideoEncoderConfig, VideoFrame, VideoFrameBuffer,
    };

    let encoder = Mp4Encoder::with_config(VideoEncoderConfig::default());

    let mut buffer = VideoFrameBuffer::new();
    buffer
        .add_frame(VideoFrame::new(2, 2, vec![0u8; 12]))
        .unwrap();
    buffer
        .add_frame(VideoFrame::new(2, 2, vec![255u8; 12]))
        .unwrap();

    let output_dir = test_output_dir("test_video_encoder");

    // This should work (either encode as MP4 or save as individual files)
    match encoder.encode_buffer_or_save_images(&buffer, output_dir.path(), "test_camera") {
        Ok(paths) => {
            println!("Video encoding produced {} output files", paths.len());
            assert!(!paths.is_empty());
        }
        Err(e) => {
            eprintln!("Video encoding failed (ffmpeg may not be installed): {}", e);
        }
    }
}
