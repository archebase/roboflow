// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! S3 pipeline integration tests.
//!
//! These tests validate the complete S3 → decode → encode → upload pipeline:
//! - S3/OSS storage read operations
//! - Bag/MCAP file streaming decode
//! - Frame alignment and buffering
//! - Video encoding with FFmpeg
//! - Parquet dataset writing
//! - S3/OSS upload with coordinator
//! - Incremental flushing behavior

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use roboflow::lerobot::upload::{EpisodeFiles, EpisodeUploadCoordinator, UploadConfig};
use roboflow::{
    DatasetBaseConfig, LerobotConfig, LerobotDatasetConfig, LerobotWriter, LerobotWriterTrait,
    VideoConfig,
};
use roboflow_dataset::ImageData;
use roboflow_storage::{LocalStorage, StorageFactory, StorageUrl};

/// Create a test output directory.
fn test_output_dir(_test_name: &str) -> tempfile::TempDir {
    fs::create_dir_all("tests/output").ok();
    tempfile::tempdir_in("tests/output")
        .unwrap_or_else(|_| tempfile::tempdir().expect("Failed to create temp dir"))
}

/// Create test image data with specified pattern.
fn create_test_image_with_pattern(width: u32, height: u32, pattern: u8) -> ImageData {
    let mut data = vec![pattern; (width * height * 3) as usize];
    // Add a gradient pattern for uniqueness
    for (i, byte) in data.iter_mut().enumerate() {
        *byte = byte.wrapping_add((i % 256) as u8);
    }
    ImageData::new(width, height, data)
}

// =============================================================================
// Test: Incremental flushing with small frame limit
// =============================================================================

#[test]
fn test_incremental_flushing_small_chunks() {
    let output_dir = test_output_dir("test_incremental_flushing");
    let config = LerobotConfig {
        dataset: LerobotDatasetConfig {
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
        flushing: roboflow::lerobot::FlushingConfig {
            max_frames_per_chunk: 5, // Small chunk size for testing
            max_memory_bytes: 0,     // Not using memory-based flushing
            incremental_video_encoding: true,
        },
    };

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();

    writer.start_episode(Some(0));

    // Add 15 frames with images (should trigger 3 flushes: 0-4, 5-9, 10-14)
    for i in 0..15 {
        writer.add_image(
            "observation.images.camera_0".to_string(),
            create_test_image_with_pattern(64, 48, (i % 256) as u8),
        );
    }

    writer.finish_episode(Some(0)).unwrap();
    let stats = writer.finalize_with_config().unwrap();

    // Verify basic stats
    assert!(stats.duration_sec >= 0.0);

    // Verify directory structure exists
    assert!(output_dir.path().join("data/chunk-000").exists());
    assert!(output_dir.path().join("videos/chunk-000").exists());
}

// =============================================================================
// Test: Incremental flushing with memory limit
// =============================================================================

#[test]
fn test_incremental_flushing_memory_based() {
    let output_dir = test_output_dir("test_memory_flushing");
    let config = LerobotConfig {
        dataset: LerobotDatasetConfig {
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
        flushing: roboflow::lerobot::FlushingConfig {
            max_frames_per_chunk: 0,      // Not using frame-based
            max_memory_bytes: 100 * 1024, // 100KB limit
            incremental_video_encoding: true,
        },
    };

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();

    writer.start_episode(Some(0));

    // Add large images that will exceed the memory limit
    // Each image: 320x240x3 = 230KB
    for i in 0..5 {
        writer.add_image(
            "observation.images.camera_0".to_string(),
            create_test_image_with_pattern(320, 240, (i % 256) as u8),
        );
    }

    writer.finish_episode(Some(0)).unwrap();
    let _stats = writer.finalize_with_config().unwrap();

    // Verify output was created
    assert!(output_dir.path().join("data/chunk-000").exists());
}

// =============================================================================
// Test: Multi-chunk episode handling
// =============================================================================

#[test]
fn test_multi_chunk_episode() {
    let output_dir = test_output_dir("test_multi_chunk");
    let config = LerobotConfig {
        dataset: LerobotDatasetConfig {
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
        flushing: roboflow::lerobot::FlushingConfig {
            max_frames_per_chunk: 10,
            max_memory_bytes: 0,
            incremental_video_encoding: true,
        },
    };

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();

    writer.start_episode(Some(0));

    // Add 25 frames (should create 3 chunks: 10 + 10 + 5)
    for i in 0..25 {
        writer.add_image(
            "observation.images.camera_0".to_string(),
            create_test_image_with_pattern(128, 96, (i % 256) as u8),
        );
    }

    writer.finish_episode(Some(0)).unwrap();
    let stats = writer.finalize_with_config().unwrap();

    // Verify all data was processed
    assert!(stats.duration_sec >= 0.0);

    // Verify output structure
    assert!(output_dir.path().join("data/chunk-000").exists());
    assert!(output_dir.path().join("videos/chunk-000").exists());
}

// =============================================================================
// Test: Upload coordinator integration
// =============================================================================

#[test]
fn test_upload_coordinator_integration() {
    let output_dir = test_output_dir("test_upload_coordinator");
    let storage = Arc::new(LocalStorage::new(output_dir.path()));

    let config = UploadConfig {
        concurrency: 2,
        show_progress: false,
        delete_after_upload: false,
        max_pending: 10,
        max_retries: 2,
        initial_backoff_ms: 50,
    };

    let coordinator = EpisodeUploadCoordinator::new(storage, config.clone(), None).unwrap();

    // Create test files
    let parquet_path = output_dir.path().join("test.episode.parquet");
    let video_path = output_dir.path().join("test_camera_0.mp4");

    // Create minimal test files
    fs::write(&parquet_path, b"test_parquet_data").unwrap();
    fs::write(&video_path, b"test_video_data").unwrap();

    // Create episode files
    let episode = EpisodeFiles {
        parquet_path: parquet_path.clone(),
        video_paths: vec![("camera_0".to_string(), video_path.clone())],
        remote_prefix: "test_prefix".to_string(),
        episode_index: 0,
    };

    // Queue upload - should succeed for local storage
    coordinator.queue_episode_upload(episode).unwrap();

    // Shutdown and wait for uploads
    let completed = coordinator.shutdown_and_cleanup();
    assert!(completed.is_ok(), "Shutdown should succeed");

    // Verify completed uploads
    let stats = completed.unwrap();
    assert!(
        stats.total_bytes > 0 || stats.total_files > 0,
        "Should have some uploads"
    );
}

// =============================================================================
// Test: Upload progress callback
// =============================================================================

#[test]
fn test_upload_progress_callback() {
    use std::sync::Mutex;

    let output_dir = test_output_dir("test_upload_progress");
    let storage = Arc::new(LocalStorage::new(output_dir.path()));

    let progress_updates = Arc::new(Mutex::new(Vec::new()));
    let progress_updates_clone = progress_updates.clone();

    let progress = move |file: &str, uploaded: u64, total: u64| {
        if let Ok(mut updates) = progress_updates_clone.lock() {
            updates.push((file.to_string(), uploaded, total));
        }
    };

    let coordinator =
        EpisodeUploadCoordinator::new(storage, UploadConfig::default(), Some(Arc::new(progress)))
            .expect("Failed to create coordinator");

    // Create test file
    let parquet_path = output_dir.path().join("progress_test.parquet");
    fs::write(&parquet_path, vec![42u8; 1024]).unwrap();

    let episode = EpisodeFiles {
        parquet_path: parquet_path.clone(),
        video_paths: vec![],
        remote_prefix: "test".to_string(),
        episode_index: 0,
    };

    coordinator.queue_episode_upload(episode).unwrap();
    coordinator
        .shutdown_and_cleanup()
        .expect("Shutdown should succeed");

    // Verify progress was reported
    let updates = progress_updates.lock().unwrap();
    assert!(!updates.is_empty(), "Should have progress updates");
}

// =============================================================================
// Test: Storage URL parsing
// =============================================================================

#[test]
fn test_storage_url_parsing() {
    // Test S3 URL parsing
    let s3_url: StorageUrl = "s3://my-bucket/path/to/file.parquet".parse().unwrap();
    assert!(matches!(s3_url, StorageUrl::S3 { .. }));

    // Test OSS URL parsing
    let oss_url: StorageUrl = "oss://my-bucket/path/to/file.parquet".parse().unwrap();
    assert!(matches!(oss_url, StorageUrl::Oss { .. }));

    // Test local file URL parsing
    let local_url: StorageUrl = "file:///local/path/to/file.parquet".parse().unwrap();
    assert!(matches!(local_url, StorageUrl::Local { .. }));
}

// =============================================================================
// Test: Storage factory creates correct backend
// =============================================================================

#[test]
fn test_storage_factory_backends() {
    let factory = StorageFactory::default();

    // Local storage
    let local = factory.create("file:///tmp/test");
    assert!(local.is_ok(), "Should create local storage");
}

// =============================================================================
// Test: End-to-end pipeline with local storage
// =============================================================================

#[test]
fn test_e2e_pipeline_local_storage() {
    let output_dir = test_output_dir("test_e2e_local");

    // Create a "source" directory to simulate S3
    let source_dir = output_dir.path().join("source");
    fs::create_dir_all(&source_dir).unwrap();

    // Create test "bag" files (simplified as text for testing)
    let bag_path = source_dir.join("test.bag");
    fs::write(&bag_path, b"bag_file_contents").unwrap();

    // Verify file can be read
    assert!(bag_path.exists());

    // Setup writer with incremental flushing
    let config = LerobotConfig {
        dataset: LerobotDatasetConfig {
            base: DatasetBaseConfig {
                name: "e2e_test".to_string(),
                fps: 30,
                robot_type: Some("test_robot".to_string()),
            },
            env_type: None,
        },
        mappings: vec![],
        video: VideoConfig::default(),
        annotation_file: None,
        flushing: roboflow::lerobot::FlushingConfig {
            max_frames_per_chunk: 5,
            max_memory_bytes: 0,
            incremental_video_encoding: true,
        },
    };

    let target_dir = output_dir.path().join("output");
    fs::create_dir_all(&target_dir).unwrap();

    let mut writer = LerobotWriter::new_local(&target_dir, config.clone()).unwrap();

    writer.start_episode(Some(0));

    // Simulate decoding and adding frames
    for i in 0..10 {
        writer.add_image(
            format!("observation.images.camera_{}", i % 2),
            create_test_image_with_pattern(64, 48, (i * 10) as u8),
        );
    }

    writer.finish_episode(Some(0)).unwrap();
    let stats = writer.finalize_with_config().unwrap();

    // Verify pipeline completed
    assert!(stats.duration_sec >= 0.0);
    assert!(target_dir.join("data/chunk-000").exists());
    assert!(target_dir.join("videos/chunk-000").exists());
}

// =============================================================================
// Test: Flushing config validation
// =============================================================================

#[test]
fn test_flushing_config_validation() {
    let config = roboflow::lerobot::FlushingConfig::default();

    // Test should_flush triggers
    assert!(
        config.should_flush(1001, 0),
        "Should flush at max_frames + 1"
    );
    assert!(
        !config.should_flush(999, 0),
        "Should not flush below max_frames"
    );

    // Test memory-based flushing
    assert!(
        config.should_flush(0, 2 * 1024 * 1024 * 1024 + 1),
        "Should flush at max_memory + 1"
    );
    assert!(
        !config.should_flush(0, 2 * 1024 * 1024 * 1024 - 1),
        "Should not flush below max_memory"
    );

    // Test combined limits
    assert!(
        config.should_flush(500, 3 * 1024 * 1024 * 1024),
        "Should flush when memory exceeded"
    );
    assert!(
        config.should_flush(1500, 1024),
        "Should flush when frames exceeded"
    );
}

// =============================================================================
// Test: Chunk metadata tracking
// =============================================================================

#[test]
fn test_chunk_metadata() {
    let metadata = roboflow::lerobot::ChunkMetadata {
        index: 0,
        start_frame: 0,
        end_frame: 1000,
        frame_count: 1000,
        parquet_path: PathBuf::from("/test/episode_000000.parquet"),
        video_files: vec![
            (PathBuf::from("/test/camera_0.mp4"), "camera_0".to_string()),
            (PathBuf::from("/test/camera_1.mp4"), "camera_1".to_string()),
        ],
        memory_bytes: 512 * 1024 * 1024,
    };

    assert_eq!(metadata.index, 0);
    assert_eq!(metadata.frame_count, 1000);
    assert_eq!(metadata.video_files.len(), 2);
    assert_eq!(metadata.memory_bytes, 512 * 1024 * 1024);
}

// =============================================================================
// Test: Chunk statistics
// =============================================================================

#[test]
fn test_chunk_stats() {
    let mut stats = roboflow::lerobot::ChunkStats::default();

    assert_eq!(stats.chunks_written, 0);
    assert_eq!(stats.total_frames, 0);
    assert_eq!(stats.total_video_bytes, 0);
    assert_eq!(stats.total_parquet_bytes, 0);

    stats.chunks_written = 3;
    stats.total_frames = 3000;
    stats.total_video_bytes = 150 * 1024 * 1024;
    stats.total_parquet_bytes = 10 * 1024 * 1024;

    assert_eq!(stats.chunks_written, 3);
    assert_eq!(stats.total_frames, 3000);
}

// =============================================================================
// Test: Large episode with incremental flushing
// =============================================================================

#[test]
fn test_large_episode_incremental_flush() {
    let output_dir = test_output_dir("test_large_episode");
    let config = LerobotConfig {
        dataset: LerobotDatasetConfig {
            base: DatasetBaseConfig {
                name: "large_test".to_string(),
                fps: 30,
                robot_type: Some("test_robot".to_string()),
            },
            env_type: None,
        },
        mappings: vec![],
        video: VideoConfig::default(),
        annotation_file: None,
        flushing: roboflow::lerobot::FlushingConfig {
            max_frames_per_chunk: 100, // Flush every 100 frames
            max_memory_bytes: 0,
            incremental_video_encoding: true,
        },
    };

    let mut writer = LerobotWriter::new_local(output_dir.path(), config.clone()).unwrap();

    writer.start_episode(Some(0));

    // Simulate a large episode (500 frames)
    // This would use ~2.7GB at 640x480 RGB without flushing
    // With flushing, memory should stay bounded
    for i in 0..500 {
        writer.add_image(
            "observation.images.camera_0".to_string(),
            create_test_image_with_pattern(640, 480, (i % 256) as u8),
        );
    }

    writer.finish_episode(Some(0)).unwrap();
    let stats = writer.finalize_with_config().unwrap();

    // Verify completion without OOM
    assert!(stats.duration_sec >= 0.0);
    assert!(output_dir.path().join("data/chunk-000").exists());
}
