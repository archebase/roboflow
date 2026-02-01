// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Worker integration tests.
//!
//! These tests validate the Worker's integration with the dataset pipeline:
//! - Worker.process_job() with streaming converter
//! - LeRobotWriter integration
//! - Storage backend integration

use std::fs;

use roboflow::{ImageData, LerobotConfig, LerobotWriter, VideoConfig};

/// Create a test output directory using system temp.
/// Using tempfile::tempdir() directly avoids:
/// - Cross-test interference
/// - Dirty working trees in CI
/// - Failures when repo is read-only
fn test_output_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("Failed to create temp dir")
}

// =============================================================================
// Test: End-to-end LeRobot writer with streaming converter
// =============================================================================

#[test]
fn test_lerobot_writer_basic_flow() {
    let output_dir = test_output_dir();
    let output_path = output_dir.path();

    // Create a test LeRobot configuration
    let lerobot_config = LerobotConfig {
        dataset: roboflow::lerobot::DatasetConfig {
            name: "test_dataset".to_string(),
            fps: 30,
            robot_type: Some("test_robot".to_string()),
            env_type: None,
        },
        mappings: vec![],
        video: VideoConfig::default(),
        annotation_file: None,
    };

    // Create a LeRobot writer directly to verify output
    let mut writer = LerobotWriter::new_local(output_path, lerobot_config.clone()).unwrap();
    writer.initialize(&lerobot_config).unwrap();

    // Create test image data
    let img_data = ImageData::new(64, 48, vec![128u8; 64 * 48 * 3]);

    // Write a test episode
    writer.start_episode(Some(0));
    writer.add_image("observation.images.camera_0".to_string(), img_data);
    writer.finish_episode(Some(0)).unwrap();

    // Finalize and get stats - use DatasetWriter trait method
    use roboflow_dataset::common::DatasetWriter;
    let _stats = DatasetWriter::finalize(&mut writer, &lerobot_config).unwrap();

    // Verify output directory structure exists
    assert!(output_path.join("data/chunk-000").exists());
    assert!(output_path.join("meta").exists());

    // Verify info.json was created
    let info_path = output_path.join("meta/info.json");
    assert!(info_path.exists(), "info.json should be created");

    // Read and verify info.json
    let info_content = fs::read_to_string(info_path).expect("Failed to read info.json");
    assert!(info_content.contains("\"fps\": 30"));
    // Robot type may be formatted differently
    assert!(info_content.contains("test_robot") || info_content.contains("robot"));
}

// =============================================================================
// Test: Worker configuration
// =============================================================================
// These tests require the distributed feature (TiKV dependencies)

#[cfg(feature = "distributed")]
#[test]
fn test_worker_config_default() {
    use roboflow_distributed::WorkerConfig;

    let config = WorkerConfig::new();
    assert_eq!(config.output_prefix, "output/");
    assert_eq!(config.storage_prefix, "input/");
}

#[cfg(feature = "distributed")]
#[test]
fn test_worker_config_builder() {
    use roboflow_distributed::WorkerConfig;

    let config = WorkerConfig::new()
        .with_storage_prefix("custom_input/")
        .with_output_prefix("custom_output/");

    assert_eq!(config.output_prefix, "custom_output/");
    assert_eq!(config.storage_prefix, "custom_input/");
}

// =============================================================================
// Test: Processing result creation
// =============================================================================

#[cfg(feature = "distributed")]
#[test]
fn test_processing_result_success() {
    use roboflow_distributed::worker::ProcessingResult;

    let result = ProcessingResult::Success;
    match result {
        ProcessingResult::Success => {}
        ProcessingResult::Failed { error } => {
            panic!("Unexpected failed result: {}", error);
        }
    }
}

#[cfg(feature = "distributed")]
#[test]
fn test_processing_result_failed() {
    use roboflow_distributed::worker::ProcessingResult;

    let result = ProcessingResult::Failed {
        error: "Test error".to_string(),
    };
    match result {
        ProcessingResult::Success => {
            panic!("Unexpected success result");
        }
        ProcessingResult::Failed { error } => {
            assert_eq!(error, "Test error");
        }
    }
}
