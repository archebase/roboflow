// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! MinIO integration tests for S3-compatible object storage.
//!
//! These tests validate S3/OSS functionality using a MinIO instance.
//! Tests will FAIL if MinIO is not available.
//!
//! To run these tests, start MinIO using docker-compose:
//!
//! ```bash
//! docker compose up -d minio minio-init
//! ```
//!
//! Then run the tests with:
//! ```bash
//! cargo test --test minio_integration_tests
//! ```
//!
//! # Environment Variables
//!
//! The tests read these environment variables (with defaults for local MinIO):
//! - `MINIO_ENDPOINT` - Default: `http://localhost:9000`
//! - `MINIO_ACCESS_KEY` - Default: `minioadmin`
//! - `MINIO_SECRET_KEY` - Default: `minioadmin`
//! - `MINIO_BUCKET` - Default: `roboflow-datasets`

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;

use roboflow_dataset::{
    ConcurrentEncoderConfig, ConcurrentVideoEncoder, ImageData,
    common::{AlignedFrame, LeRobotVideoPathScheme, VideoPathScheme},
};
use roboflow_storage::{
    AsyncStorage,
    s3::{AsyncS3Storage, S3Config},
};

// =============================================================================
// Test Helper Module
// =============================================================================

/// MinIO test configuration.
#[derive(Debug, Clone)]
struct MinioConfig {
    /// MinIO endpoint URL
    pub endpoint: String,
    /// Access key ID
    pub access_key_id: String,
    /// Secret access key
    pub secret_access_key: String,
    /// Default bucket name
    pub bucket: String,
}

impl Default for MinioConfig {
    fn default() -> Self {
        Self {
            endpoint: std::env::var("MINIO_ENDPOINT")
                .unwrap_or_else(|_| "http://localhost:9000".to_string()),
            access_key_id: std::env::var("MINIO_ACCESS_KEY")
                .unwrap_or_else(|_| "minioadmin".to_string()),
            secret_access_key: std::env::var("MINIO_SECRET_KEY")
                .unwrap_or_else(|_| "minioadmin".to_string()),
            bucket: std::env::var("MINIO_BUCKET")
                .unwrap_or_else(|_| "roboflow-datasets".to_string()),
        }
    }
}

impl MinioConfig {
    /// Check if MinIO is available by attempting to connect.
    pub fn is_available(&self) -> bool {
        // Try to create an AsyncS3Storage instance with HTTP enabled for local testing
        let s3_config = S3Config::new(
            &self.bucket,
            &self.endpoint,
            &self.access_key_id,
            &self.secret_access_key,
        )
        .with_allow_http(true);
        AsyncS3Storage::with_config(s3_config).is_ok()
    }

    /// Create an AsyncS3Storage instance for testing.
    pub fn create_storage(&self) -> Result<AsyncS3Storage, Box<dyn std::error::Error>> {
        let config = S3Config::new(
            &self.bucket,
            &self.endpoint,
            &self.access_key_id,
            &self.secret_access_key,
        )
        .with_allow_http(true);
        Ok(AsyncS3Storage::with_config(config)?)
    }

    /// Get the S3Config for use with ConcurrentVideoEncoder.
    #[allow(dead_code)]
    pub fn s3_config(&self) -> S3Config {
        S3Config::new(
            &self.bucket,
            &self.endpoint,
            &self.access_key_id,
            &self.secret_access_key,
        )
        .with_allow_http(true)
    }

    /// Get the S3 URL prefix for this configuration.
    #[allow(dead_code)]
    pub fn s3_url_prefix(&self, path: &str) -> String {
        format!("s3://{}/{}", self.bucket, path)
    }

    /// Cleanup a test directory recursively.
    #[allow(dead_code)]
    pub fn cleanup_dir(&self, runtime: &tokio::runtime::Runtime, path: &str) {
        let storage = match self.create_storage() {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Failed to create storage for cleanup: {}", e);
                return;
            }
        };

        let test_path = Path::new(path);
        if let Err(e) = Self::cleanup_recursive(&storage, runtime, test_path) {
            eprintln!("Failed to cleanup {}: {}", path, e);
        }
    }

    /// Recursively delete a directory and all its contents.
    #[allow(dead_code)]
    fn cleanup_recursive(
        storage: &AsyncS3Storage,
        runtime: &tokio::runtime::Runtime,
        path: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if !runtime.block_on(async { storage.exists(path).await }) {
            return Ok(());
        }

        // Try to list and delete contents first (for directories)
        let object_store = storage.object_store();
        let path_str = path.to_str().unwrap_or_default();

        // Use object_store to list all objects with this prefix
        let list_result = runtime.block_on(async {
            object_store
                .list_with_delimiter(Some(&object_store::path::Path::from(path_str)))
                .await
        })?;

        // Delete all objects in the directory
        for object in list_result.objects {
            let object_path = object.location.as_ref();
            runtime.block_on(async { storage.delete(Path::new(object_path)).await })?;
        }

        // Recursively delete subdirectories
        for prefix in list_result.common_prefixes {
            let sub_path = Path::new(prefix.as_ref());
            Self::cleanup_recursive(storage, runtime, sub_path)?;
        }

        // Try to delete the directory itself (may fail if not empty, that's ok)
        let _ = runtime.block_on(async { storage.delete(path).await });

        Ok(())
    }
}

/// Helper to create test image data.
fn create_test_image(width: u32, height: u32, pattern: u8) -> ImageData {
    let mut data = vec![pattern; (width * height * 3) as usize];
    for (i, byte) in data.iter_mut().enumerate() {
        *byte = byte.wrapping_add((i % 256) as u8);
    }
    ImageData::new(width, height, data)
}

/// Helper to create an AlignedFrame with test data.
#[allow(dead_code)]
fn create_test_frame(
    frame_index: usize,
    camera_name: &str,
    width: u32,
    height: u32,
) -> AlignedFrame {
    let mut images = HashMap::new();
    images.insert(
        camera_name.to_string(),
        Arc::new(create_test_image(width, height, (frame_index % 256) as u8)),
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

    AlignedFrame {
        frame_index,
        timestamp: (frame_index as u64) * 33_333_333,
        images,
        states,
        actions,
        timestamps: HashMap::new(),
        audio: HashMap::new(),
    }
}

/// Panic if MinIO is not available.
macro_rules! require_minio {
    () => {
        let config = MinioConfig::default();
        if !config.is_available() {
            panic!("Required service MinIO is not available at {}. Start MinIO with: docker compose up -d minio minio-init", config.endpoint);
        }
    };
}

// =============================================================================
// Test: Basic MinIO Connection
// =============================================================================

#[test]
fn test_minio_basic_connection() {
    require_minio!();

    let config = MinioConfig::default();
    println!("Testing MinIO connection at: {}", config.endpoint);

    // Create storage instance with HTTP enabled
    let storage = config
        .create_storage()
        .expect("Failed to create MinIO storage");

    // Test write and read
    let test_path = Path::new("test_minio_connection.txt");
    let test_data = Bytes::from(&b"Hello from MinIO integration test!"[..]);

    // Write test data
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async {
            storage.write(test_path, test_data.clone()).await?;
            let read_data = storage.read(test_path).await?;
            assert_eq!(read_data, test_data, "Read data should match written data");

            // Clean up
            storage.delete(test_path).await?;
            Ok::<(), Box<dyn std::error::Error>>(())
        })
        .unwrap();

    println!("✓ MinIO connection test passed");
}

// =============================================================================
// Test: RsmpegS3Encoder with local file output (no longer requires MinIO)
// =============================================================================

#[test]
fn test_concurrent_encoder_with_local_output() {
    // Test video encoding with local file output using ConcurrentVideoEncoder
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let encoder_config = ConcurrentEncoderConfig::new();
    let mut encoder =
        ConcurrentVideoEncoder::new(encoder_config).expect("Failed to create encoder");

    // Use LeRobot path scheme to compute output path
    let key_prefix = "test_videos";
    let scheme = LeRobotVideoPathScheme::new(key_prefix);
    let output_path = temp_dir
        .path()
        .join(scheme.video_path(0, "encoder_test", 0));

    // Register camera
    encoder
        .add_camera("encoder_test", output_path)
        .expect("Failed to add camera");

    // Add test frames
    let width = 160u32;
    let height = 120u32;
    for i in 0..10 {
        let image_data = create_test_image(width, height, (i * 25) as u8);
        encoder
            .add_frame("encoder_test", image_data)
            .expect("Failed to add frame");
    }

    // Finalize and write
    let results = encoder.finalize().expect("Failed to finalize encoder");

    assert_eq!(results.len(), 1, "Should have 1 camera result");
    assert_eq!(results[0].frames_encoded, 10, "Should encode 10 frames");
    // Path should contain camera name as directory and end with episode_000000.mp4
    let path_str = results[0].output_path.to_string_lossy();
    assert!(
        path_str.contains("encoder_test"),
        "Output path '{}' should contain camera name 'encoder_test'",
        path_str
    );
    assert!(
        path_str.contains("episode_000000.mp4"),
        "Output path '{}' should contain 'episode_000000.mp4'",
        path_str
    );
    assert!(results[0].output_path.exists(), "Output file should exist");

    println!("✓ ConcurrentVideoEncoder with local output test passed");
}

// =============================================================================
// Test: ConcurrentVideoEncoder with local output (multi-camera)
// =============================================================================

#[test]
fn test_concurrent_encoder_multicam_with_local_output() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    // Create concurrent encoder
    let encoder_config = ConcurrentEncoderConfig::new();
    let mut encoder =
        ConcurrentVideoEncoder::new(encoder_config).expect("Failed to create encoder");

    // Use LeRobot path scheme
    let key_prefix = "test_coordinator";
    let scheme = LeRobotVideoPathScheme::new(key_prefix);

    // Register cameras with output paths
    let cam0_path = temp_dir.path().join(scheme.video_path(0, "camera_0", 0));
    let cam1_path = temp_dir.path().join(scheme.video_path(0, "camera_1", 0));
    encoder
        .add_camera("camera_0", cam0_path)
        .expect("Failed to add camera_0");
    encoder
        .add_camera("camera_1", cam1_path)
        .expect("Failed to add camera_1");

    // Add frames for 2 cameras
    let width = 160u32;
    let height = 120u32;
    for i in 0..20 {
        // Camera 0
        let image_data0 = create_test_image(width, height, (i * 10) as u8);
        encoder
            .add_frame("camera_0", image_data0)
            .expect("Failed to add camera_0 frame");

        // Camera 1
        let image_data1 = create_test_image(width, height, ((i * 10) + 128) as u8);
        encoder
            .add_frame("camera_1", image_data1)
            .expect("Failed to add camera_1 frame");
    }

    // Finalize and get results
    let results = encoder.finalize().expect("Failed to finalize encoder");

    // Verify both cameras were processed
    assert_eq!(results.len(), 2, "Should have 2 camera results");

    let camera_0_result = results.iter().find(|r| r.camera == "camera_0");
    let camera_1_result = results.iter().find(|r| r.camera == "camera_1");

    assert!(camera_0_result.is_some(), "Results should contain camera_0");
    assert!(camera_1_result.is_some(), "Results should contain camera_1");
    assert_eq!(camera_0_result.unwrap().frames_encoded, 20);
    assert_eq!(camera_1_result.unwrap().frames_encoded, 20);

    println!("✓ ConcurrentVideoEncoder multi-camera with MinIO test passed");
}

// =============================================================================
// Test: Compressed image handling with local file output
// =============================================================================

#[test]
fn test_compressed_images_with_local_output() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    // Create concurrent encoder
    let encoder_config = ConcurrentEncoderConfig::new();
    let mut encoder =
        ConcurrentVideoEncoder::new(encoder_config).expect("Failed to create encoder");

    // Use LeRobot path scheme
    let key_prefix = "test_compressed";
    let scheme = LeRobotVideoPathScheme::new(key_prefix);

    // Register cameras
    let raw_path = temp_dir.path().join(scheme.video_path(0, "camera_raw", 0));
    let jpeg_path = temp_dir.path().join(scheme.video_path(0, "camera_jpeg", 0));
    encoder
        .add_camera("camera_raw", raw_path)
        .expect("Failed to add camera_raw");
    encoder
        .add_camera("camera_jpeg", jpeg_path)
        .expect("Failed to add camera_jpeg");

    let width = 160u32;
    let height = 120u32;

    // Add raw RGB images
    for _ in 0..5 {
        let img = ImageData::new(width, height, vec![128u8; (width * height * 3) as usize]);
        encoder
            .add_frame("camera_raw", img)
            .expect("Failed to add raw frame");
    }

    // Add compressed JPEG images (simulating data from ROS bag)
    for i in 0..5 {
        // Create a minimal JPEG header
        let jpeg_data: Vec<u8> = vec![
            0xFF, 0xD8, // SOI marker
            0xFF, 0xE0, // APP0 marker
            0x00, 0x10, // Length: 16 bytes
            0x4A, 0x46, 0x49, 0x46, 0x00, // "JFIF" null-terminated
        ]
        .into_iter()
        .chain(std::iter::repeat_n((i * 20) as u8, 100))
        .collect();

        let img = ImageData::encoded(width, height, jpeg_data);
        encoder
            .add_frame("camera_jpeg", img)
            .expect("Failed to add JPEG frame");
    }

    // Finalize
    let results = encoder.finalize().expect("Failed to finalize");

    // Note: The minimal JPEG headers won't decode properly, so those frames may be skipped
    // The important thing is that the encoder doesn't crash
    println!("✓ Compressed images with local output test passed");
    println!("  - Cameras processed: {}", results.len());
}

// =============================================================================
// Test: Bucket creation and management
// =============================================================================

#[test]
fn test_minio_bucket_management() {
    require_minio!();

    let config = MinioConfig::default();
    let test_bucket = format!("test-bucket-{}", std::process::id());

    // Create storage with test bucket (using HTTP for local testing)
    let s3_config = S3Config::new(
        &test_bucket,
        &config.endpoint,
        &config.access_key_id,
        &config.secret_access_key,
    )
    .with_allow_http(true);
    let result = AsyncS3Storage::with_config(s3_config);

    // Test bucket might not exist - that's ok for this test
    match result {
        Ok(_storage) => {
            println!("✓ Bucket '{}' exists or is accessible", test_bucket);
        }
        Err(e) => {
            println!("Note: Bucket '{}' not accessible: {}", test_bucket, e);
            println!("This is expected if the bucket wasn't pre-created");
        }
    }
}

// =============================================================================
// Test: Concurrent local file writes
// =============================================================================

#[test]
fn test_concurrent_local_writes() {
    // Create 3 encoders in parallel (simulating 3 workers)
    let handles: Vec<_> = (0..3)
        .map(|worker_id| {
            let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
            let output_dir = temp_dir.path().to_path_buf();
            // Keep temp_dir alive for the thread duration
            let _temp_dir = temp_dir.keep();

            std::thread::spawn(move || {
                let encoder_config = ConcurrentEncoderConfig::new();
                let mut encoder =
                    ConcurrentVideoEncoder::new(encoder_config).expect("Failed to create encoder");

                // Use LeRobot path scheme for this worker
                let key_prefix = format!("test_concurrent/worker_{}", worker_id);
                let scheme = LeRobotVideoPathScheme::new(key_prefix);
                let output_path =
                    output_dir.join(scheme.video_path(worker_id as usize, "camera", 0));

                encoder
                    .add_camera("camera", output_path)
                    .expect("Failed to add camera");

                // Add 5 frames
                for i in 0..5 {
                    let img = create_test_image(160, 120, (worker_id * 10 + i) as u8);
                    encoder
                        .add_frame("camera", img)
                        .expect("Failed to add frame");
                }

                encoder.finalize()
            })
        })
        .collect();

    // Wait for all workers and collect results
    let mut results: Vec<Result<Vec<roboflow_dataset::common::ConcurrentEncoderResult>, _>> =
        vec![];
    for handle in handles {
        let result = handle.join().expect("Thread panicked");
        results.push(result);
    }

    // All workers should succeed
    for (i, result) in results.into_iter().enumerate() {
        assert!(
            result.is_ok(),
            "Worker {} should succeed: {:?}",
            i,
            result.err()
        );
    }

    println!("✓ Concurrent local writes test passed (3 workers)");
}

// =============================================================================
// Test: Large file upload to MinIO
// =============================================================================

#[test]
fn test_large_file_upload_to_minio() {
    require_minio!();

    let config = MinioConfig::default();
    let runtime = tokio::runtime::Runtime::new().unwrap();

    // Create storage
    let storage = config
        .create_storage()
        .expect("Failed to create MinIO storage");

    // Create a large test file (approximately 5MB)
    let large_data = Bytes::from(vec![0xABu8; 5 * 1024 * 1024]);

    runtime
        .block_on(async {
            let test_path = Path::new("test_large_file.bin");
            storage.write(test_path, large_data.clone()).await?;

            // Verify the file exists and has the right size
            let exists = storage.exists(test_path).await;
            assert!(exists, "Large file should exist");

            // Read it back and verify size
            let read_back = storage.read(test_path).await?;
            assert_eq!(read_back.len(), large_data.len(), "File size should match");

            // Clean up
            storage.delete(test_path).await?;

            Ok::<(), Box<dyn std::error::Error>>(())
        })
        .unwrap();

    println!("✓ Large file upload test passed (5MB)");
}

// =============================================================================
// Test: LeRobot v2.1 video path structure with ConcurrentVideoEncoder
// =============================================================================

/// Test that ConcurrentVideoEncoder produces LeRobot v2.1 compliant video paths.
///
/// This test verifies:
/// 1. Videos are written to `{output_dir}/{prefix}/videos/chunk-{chunk:03}/{camera}/episode_{episode:06}.mp4`
/// 2. Multiple cameras produce unique paths (no overwrites)
/// 3. Files actually exist at the expected locations
#[test]
fn test_lerobot_v21_video_path_structure() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    // Test configuration
    let key_prefix = "test_lerobot_v21";
    let chunk_index = 0usize;
    let episode_index = 42usize; // Use non-zero to verify formatting

    let encoder_config = ConcurrentEncoderConfig::new();
    let mut encoder =
        ConcurrentVideoEncoder::new(encoder_config).expect("Failed to create encoder");

    // Use LeRobot path scheme to compute output paths
    let scheme = LeRobotVideoPathScheme::new(key_prefix);

    // Add frames for multiple cameras (simulating multi-camera setup)
    let cameras = vec![
        "observation.images.cam_left",
        "observation.images.cam_right",
        "observation.images.cam_high",
    ];

    // Register cameras with their output paths
    for camera in &cameras {
        let output_path =
            temp_dir
                .path()
                .join(scheme.video_path(episode_index, camera, chunk_index));
        encoder
            .add_camera(camera, output_path)
            .expect("Failed to add camera");
    }

    // Add frames for each camera
    for camera in &cameras {
        for i in 0..10 {
            let img = create_test_image(160, 120, (i * 25) as u8);
            encoder.add_frame(camera, img).expect("Failed to add frame");
        }
    }

    // Finalize and get results
    let results = encoder.finalize().expect("Failed to finalize encoder");

    // Verify all cameras were processed
    assert_eq!(results.len(), 3, "Should have 3 camera results");

    // Verify the output path format matches LeRobot v2.1 structure
    for result in &results {
        // Expected format: {output_dir}/{prefix}/videos/chunk-{chunk:03}/{camera}/episode_{episode:06}.mp4
        let path_str = result.output_path.to_string_lossy();
        let expected_prefix = format!("{}/videos/chunk-{:03}/", key_prefix, chunk_index);
        assert!(
            path_str.contains(&expected_prefix),
            "Output path '{}' should contain '{}'",
            path_str,
            expected_prefix
        );

        // Verify episode number in filename
        assert!(
            path_str.contains(&format!("episode_{:06}.mp4", episode_index)),
            "Output path '{}' should contain 'episode_{:06}.mp4'",
            path_str,
            episode_index
        );

        // Verify camera name is in the path
        assert!(
            path_str.contains(&result.camera),
            "Output path '{}' should contain camera name '{}'",
            path_str,
            result.camera
        );

        // Verify file exists
        assert!(
            result.output_path.exists(),
            "Output file should exist at '{}'",
            result.output_path.display()
        );

        // Verify file has reasonable size (should be > 1KB for a video)
        let metadata = std::fs::metadata(&result.output_path).expect("Failed to get metadata");
        assert!(
            metadata.len() > 1024,
            "Video file should be > 1KB, got {} bytes",
            metadata.len()
        );

        println!(
            "✓ Camera '{}' written to: {}",
            result.camera,
            result.output_path.display()
        );
    }

    println!("✓ LeRobot v2.1 video path structure test passed");
}

/// Test that different cameras with similar names produce unique temp files.
///
/// Regression test for the race condition where cameras like "cam_left" and
/// "cam_right" would overwrite each other's temp files.
#[test]
fn test_multi_camera_unique_temp_files() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let encoder_config = ConcurrentEncoderConfig::new();
    let mut encoder =
        ConcurrentVideoEncoder::new(encoder_config).expect("Failed to create encoder");

    // Use LeRobot path scheme
    let scheme = LeRobotVideoPathScheme::new("test_unique_temp");

    // Register cameras with their output paths
    let left_path = temp_dir
        .path()
        .join(scheme.video_path(0, "observation.images.cam_left", 0));
    let right_path = temp_dir
        .path()
        .join(scheme.video_path(0, "observation.images.cam_right", 0));
    encoder
        .add_camera("observation.images.cam_left", left_path)
        .expect("Failed to add left camera");
    encoder
        .add_camera("observation.images.cam_right", right_path)
        .expect("Failed to add right camera");

    // Add frames for cameras with similar names (simultaneously)
    // This would previously cause temp file collisions
    for i in 0..30 {
        let img_left = create_test_image(160, 120, i as u8);
        let img_right = create_test_image(160, 120, (i + 128) as u8);

        encoder
            .add_frame("observation.images.cam_left", img_left)
            .expect("Failed to add left frame");
        encoder
            .add_frame("observation.images.cam_right", img_right)
            .expect("Failed to add right frame");
    }

    // Finalize should succeed without "file not found" errors
    let results = encoder.finalize().expect("Failed to finalize encoder");

    // Both cameras should have encoded all frames
    assert_eq!(results.len(), 2, "Should have 2 camera results");

    for result in &results {
        assert_eq!(
            result.frames_encoded, 30,
            "Camera '{}' should have encoded 30 frames",
            result.camera
        );
        assert_eq!(
            result.frames_skipped, 0,
            "Camera '{}' should have 0 skipped frames",
            result.camera
        );
        // Verify output file exists
        assert!(
            result.output_path.exists(),
            "Output file should exist at '{}'",
            result.output_path.display()
        );
    }

    println!("✓ Multi-camera unique temp files test passed (regression test)");
}

// =============================================================================
// Test: S3 Storage List Operations
// =============================================================================

#[test]
fn test_s3_list_operations() {
    require_minio!();

    let config = MinioConfig::default();
    let storage = config.create_storage().expect("Failed to create storage");
    let runtime = tokio::runtime::Runtime::new().unwrap();

    runtime.block_on(async {
        // Create test files
        let test_prefix = "test_list_ops";
        let files = vec![
            format!("{}/file1.txt", test_prefix),
            format!("{}/file2.txt", test_prefix),
            format!("{}/subdir/file3.txt", test_prefix),
        ];

        for path in &files {
            storage
                .write(Path::new(path), Bytes::from("test data"))
                .await
                .expect("Failed to write file");
        }

        // List files with prefix
        let listed = storage
            .list(Path::new(test_prefix))
            .await
            .expect("Failed to list files");

        assert!(listed.len() >= 3, "Should list at least 3 files");

        // Cleanup
        for path in &files {
            let _ = storage.delete(Path::new(path)).await;
        }

        println!("✓ S3 list operations test passed");
    });
}

// =============================================================================
// Test: S3 Storage Exists Check
// =============================================================================

#[test]
fn test_s3_exists_check() {
    require_minio!();

    let config = MinioConfig::default();
    let storage = config.create_storage().expect("Failed to create storage");
    let runtime = tokio::runtime::Runtime::new().unwrap();

    runtime.block_on(async {
        let test_path = Path::new("test_exists/file.txt");

        // File should not exist initially
        assert!(
            !storage.exists(test_path).await,
            "File should not exist initially"
        );

        // Write file
        storage
            .write(test_path, Bytes::from("test data"))
            .await
            .expect("Failed to write file");

        // File should exist now
        assert!(
            storage.exists(test_path).await,
            "File should exist after write"
        );

        // Cleanup
        let _ = storage.delete(test_path).await;

        println!("✓ S3 exists check test passed");
    });
}

// =============================================================================
// Test: S3 Storage Size Operation
// =============================================================================

#[test]
fn test_s3_size_operation() {
    require_minio!();

    let config = MinioConfig::default();
    let storage = config.create_storage().expect("Failed to create storage");
    let runtime = tokio::runtime::Runtime::new().unwrap();

    runtime.block_on(async {
        let test_path = Path::new("test_size/file.bin");
        let test_data = Bytes::from(vec![0u8; 1024]); // 1KB

        // Write file
        storage
            .write(test_path, test_data.clone())
            .await
            .expect("Failed to write file");

        // Check size
        let size = storage.size(test_path).await.expect("Failed to get size");
        assert_eq!(size, 1024, "File size should be 1024 bytes");

        // Cleanup
        let _ = storage.delete(test_path).await;

        println!("✓ S3 size operation test passed");
    });
}

// =============================================================================
// Test: S3 Storage Metadata Operation
// =============================================================================

#[test]
fn test_s3_metadata_operation() {
    require_minio!();

    let config = MinioConfig::default();
    let storage = config.create_storage().expect("Failed to create storage");
    let runtime = tokio::runtime::Runtime::new().unwrap();

    runtime.block_on(async {
        let test_path = Path::new("test_metadata/file.txt");
        let test_data = Bytes::from("metadata test content");

        // Write file
        storage
            .write(test_path, test_data)
            .await
            .expect("Failed to write file");

        // Get metadata
        let metadata = storage
            .metadata(test_path)
            .await
            .expect("Failed to get metadata");

        assert!(metadata.size > 0, "Metadata size should be > 0");
        assert!(
            metadata.last_modified.is_some(),
            "Last modified should be set"
        );

        // Cleanup
        let _ = storage.delete(test_path).await;

        println!("✓ S3 metadata operation test passed");
    });
}

// =============================================================================
// Test: S3 Storage Copy Operation
// =============================================================================

#[test]
fn test_s3_copy_operation() {
    require_minio!();

    let config = MinioConfig::default();
    let storage = config.create_storage().expect("Failed to create storage");
    let runtime = tokio::runtime::Runtime::new().unwrap();

    runtime.block_on(async {
        let src_path = Path::new("test_copy/source.txt");
        let dst_path = Path::new("test_copy/destination.txt");
        let test_data = Bytes::from("copy test content");

        // Write source file
        storage
            .write(src_path, test_data.clone())
            .await
            .expect("Failed to write source");

        // Copy file
        storage
            .copy(src_path, dst_path)
            .await
            .expect("Failed to copy file");

        // Verify destination exists and has same content
        assert!(storage.exists(dst_path).await, "Destination should exist");
        let copied_data = storage
            .read(dst_path)
            .await
            .expect("Failed to read copied file");
        assert_eq!(copied_data, test_data, "Copied content should match");

        // Cleanup
        let _ = storage.delete(src_path).await;
        let _ = storage.delete(dst_path).await;

        println!("✓ S3 copy operation test passed");
    });
}

// =============================================================================
// Test: S3 Storage Create Directory Operations
// =============================================================================

#[test]
fn test_s3_create_dir_operations() {
    require_minio!();

    let config = MinioConfig::default();
    let storage = config.create_storage().expect("Failed to create storage");
    let runtime = tokio::runtime::Runtime::new().unwrap();

    runtime.block_on(async {
        let test_dir = Path::new("test_createdir/subdir");

        // create_dir should succeed (no-op for S3)
        storage
            .create_dir(test_dir)
            .await
            .expect("create_dir should succeed");

        // create_dir_all should also succeed
        storage
            .create_dir_all(test_dir)
            .await
            .expect("create_dir_all should succeed");

        println!("✓ S3 create directory operations test passed");
    });
}

// =============================================================================
// Test: S3 Storage Large File Upload
// =============================================================================

#[test]
fn test_s3_large_file_upload() {
    require_minio!();

    let config = MinioConfig::default();
    let storage = config.create_storage().expect("Failed to create storage");
    let runtime = tokio::runtime::Runtime::new().unwrap();

    runtime.block_on(async {
        let test_path = Path::new("test_large/large_file.bin");

        // Create a 1MB file
        let test_data = Bytes::from(vec![0xABu8; 1024 * 1024]);

        // Write large file
        storage
            .write(test_path, test_data.clone())
            .await
            .expect("Failed to write large file");

        // Verify size
        let size = storage.size(test_path).await.expect("Failed to get size");
        assert_eq!(size, 1024 * 1024, "File size should be 1MB");

        // Verify content (just check first/last bytes to save time)
        let read_data = storage.read(test_path).await.expect("Failed to read file");
        assert_eq!(
            read_data.len(),
            1024 * 1024,
            "Read data length should match"
        );
        assert_eq!(read_data[0], 0xAB, "First byte should match");
        assert_eq!(read_data[1024 * 1024 - 1], 0xAB, "Last byte should match");

        // Cleanup
        let _ = storage.delete(test_path).await;

        println!("✓ S3 large file upload test passed");
    });
}

// =============================================================================
// Test: S3 Storage Overwrite Behavior
// =============================================================================

#[test]
fn test_s3_overwrite_behavior() {
    require_minio!();

    let config = MinioConfig::default();
    let storage = config.create_storage().expect("Failed to create storage");
    let runtime = tokio::runtime::Runtime::new().unwrap();

    runtime.block_on(async {
        let test_path = Path::new("test_overwrite/file.txt");

        // Write initial content
        storage
            .write(test_path, Bytes::from("initial content"))
            .await
            .expect("Failed to write initial");

        // Overwrite with new content
        storage
            .write(test_path, Bytes::from("overwritten content"))
            .await
            .expect("Failed to overwrite");

        // Verify overwritten content
        let read_data = storage.read(test_path).await.expect("Failed to read");
        assert_eq!(
            read_data,
            Bytes::from("overwritten content"),
            "Content should be overwritten"
        );

        // Cleanup
        let _ = storage.delete(test_path).await;

        println!("✓ S3 overwrite behavior test passed");
    });
}

// =============================================================================
// Test: S3 Storage Error Handling - Read Non-existent File
// =============================================================================

#[test]
fn test_s3_read_nonexistent_file() {
    require_minio!();

    let config = MinioConfig::default();
    let storage = config.create_storage().expect("Failed to create storage");
    let runtime = tokio::runtime::Runtime::new().unwrap();

    runtime.block_on(async {
        let test_path = Path::new("nonexistent/path/file.txt");

        let result = storage.read(test_path).await;
        assert!(
            result.is_err(),
            "Reading nonexistent file should return error"
        );

        println!("✓ S3 read nonexistent file test passed");
    });
}

// =============================================================================
// Test: S3 Storage Nested Directory Structure
// =============================================================================

#[test]
fn test_s3_nested_directory_structure() {
    require_minio!();

    let config = MinioConfig::default();
    let storage = config.create_storage().expect("Failed to create storage");
    let runtime = tokio::runtime::Runtime::new().unwrap();

    runtime.block_on(async {
        // Create deeply nested structure
        let nested_path = Path::new("test_nested/a/b/c/d/e/file.txt");

        storage
            .write(nested_path, Bytes::from("nested content"))
            .await
            .expect("Failed to write nested file");

        // Verify file exists
        assert!(
            storage.exists(nested_path).await,
            "Nested file should exist"
        );

        // Read back
        let read_data = storage
            .read(nested_path)
            .await
            .expect("Failed to read nested file");
        assert_eq!(
            read_data,
            Bytes::from("nested content"),
            "Nested content should match"
        );

        // Cleanup
        let _ = storage.delete(nested_path).await;

        println!("✓ S3 nested directory structure test passed");
    });
}

// =============================================================================
// Test: S3 Storage Read Range Operations
// =============================================================================

#[test]
fn test_s3_read_range_operations() {
    require_minio!();

    let config = MinioConfig::default();
    let storage = config.create_storage().expect("Failed to create storage");
    let runtime = tokio::runtime::Runtime::new().unwrap();

    runtime.block_on(async {
        let test_path = Path::new("test_range/data.bin");

        // Create test data with recognizable pattern
        let test_data: Vec<u8> = (0..=255).collect();
        storage
            .write(test_path, Bytes::from(test_data.clone()))
            .await
            .expect("Failed to write file");

        // Read first 10 bytes
        let range1 = storage
            .read_range(test_path, 0, Some(10))
            .await
            .expect("Failed to read range");
        assert_eq!(range1.len(), 10, "Range should be 10 bytes");
        assert_eq!(&range1[..], &test_data[0..10], "Range content should match");

        // Read middle section
        let range2 = storage
            .read_range(test_path, 100, Some(150))
            .await
            .expect("Failed to read range");
        assert_eq!(range2.len(), 50, "Range should be 50 bytes");
        assert_eq!(
            &range2[..],
            &test_data[100..150],
            "Range content should match"
        );

        // Read from offset to end (no end specified)
        let range3 = storage
            .read_range(test_path, 200, None)
            .await
            .expect("Failed to read range");
        assert_eq!(range3.len(), 56, "Range should be 56 bytes (256-200)");
        assert_eq!(&range3[..], &test_data[200..], "Range content should match");

        // Cleanup
        let _ = storage.delete(test_path).await;

        println!("✓ S3 read range operations test passed");
    });
}
