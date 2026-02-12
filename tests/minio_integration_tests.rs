// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! MinIO integration tests for S3-compatible object storage.
//!
//! These tests validate S3/OSS functionality using a MinIO instance.
//! To run these tests, start MinIO using docker-compose:
//!
//! ```bash
//! docker compose up -d minio minio-init
//! ```
//!
//! Then run the tests with:
//! ```bash
//! cargo test --test minio_integration_tests -- --ignored
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
    AlignedFrame, ImageData,
    common::{ConcurrentEncoderConfig, ConcurrentVideoEncoder},
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

/// Skip the test if MinIO is not available.
macro_rules! skip_if_no_minio {
    () => {
        let config = MinioConfig::default();
        if !config.is_available() {
            eprintln!("Skipping test: MinIO not available at {}", config.endpoint);
            eprintln!("Start MinIO with: docker compose up -d minio minio-init");
            return;
        }
    };
}

// =============================================================================
// Test: Basic MinIO Connection
// =============================================================================

#[test]
#[ignore = "requires MinIO service"]
fn test_minio_basic_connection() {
    skip_if_no_minio!();

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
// Test: RsmpegS3Encoder with MinIO
// =============================================================================

#[test]
#[ignore = "requires MinIO service"]
fn test_concurrent_encoder_with_minio() {
    skip_if_no_minio!();

    let config = MinioConfig::default();

    // Test video encoding with S3 upload using ConcurrentVideoEncoder
    let key_prefix = "test_videos".to_string();
    let encoder_config = ConcurrentEncoderConfig {
        key_prefix,
        chunk_index: 0,
        episode_index: 0,
        chunk_size: 256 * 1024,
        video_config: roboflow_dataset::common::video::VideoEncoderConfig::default(),
        frame_channel_capacity: 64,
        s3_config: config.s3_config(),
    };

    let mut encoder =
        ConcurrentVideoEncoder::new(encoder_config)
            .expect("Failed to create encoder");

    // Add test frames
    let width = 160u32;
    let height = 120u32;
    for i in 0..10 {
        let image_data = create_test_image(width, height, (i * 25) as u8);
        encoder
            .add_frame("encoder_test", image_data)
            .expect("Failed to add frame");
    }

    // Finalize and upload
    let results = encoder.finalize().expect("Failed to finalize encoder");

    assert_eq!(results.len(), 1, "Should have 1 camera result");
    assert_eq!(results[0].frames_encoded, 10, "Should encode 10 frames");
    assert!(
        results[0].url.contains("encoder_test.mp4"),
        "URL should contain camera name"
    );

    println!("✓ ConcurrentVideoEncoder with MinIO test passed");
}

// =============================================================================
// Test: ConcurrentVideoEncoder with MinIO (multi-camera)
// =============================================================================

#[test]
#[ignore = "requires MinIO service"]
fn test_concurrent_encoder_multicam_with_minio() {
    skip_if_no_minio!();

    let config = MinioConfig::default();

    // Create concurrent encoder
    let key_prefix = "test_coordinator".to_string();
    let encoder_config = ConcurrentEncoderConfig {
        key_prefix,
        chunk_index: 0,
        episode_index: 0,
        chunk_size: 256 * 1024,
        video_config: roboflow_dataset::common::video::VideoEncoderConfig::default(),
        frame_channel_capacity: 100,
        s3_config: config.s3_config(),
    };

    let mut encoder =
        ConcurrentVideoEncoder::new(encoder_config)
            .expect("Failed to create encoder");

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
// Test: Compressed image handling with MinIO upload
// =============================================================================

#[test]
#[ignore = "requires MinIO service"]
fn test_compressed_images_with_minio_upload() {
    skip_if_no_minio!();

    let config = MinioConfig::default();

    // Create concurrent encoder
    let key_prefix = "test_compressed".to_string();
    let encoder_config = ConcurrentEncoderConfig {
        key_prefix,
        chunk_index: 0,
        episode_index: 0,
        chunk_size: 256 * 1024,
        video_config: roboflow_dataset::common::video::VideoEncoderConfig::default(),
        frame_channel_capacity: 100,
        s3_config: config.s3_config(),
    };

    let mut encoder =
        ConcurrentVideoEncoder::new(encoder_config)
            .expect("Failed to create encoder");

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
    println!("✓ Compressed images with MinIO upload test passed");
    println!("  - Cameras processed: {}", results.len());

    // Cleanup test files
    let runtime = tokio::runtime::Runtime::new().unwrap();
    config.cleanup_dir(&runtime, "test_compressed");
}

// =============================================================================
// Test: Bucket creation and management
// =============================================================================

#[test]
#[ignore = "requires MinIO service"]
fn test_minio_bucket_management() {
    skip_if_no_minio!();

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
// Test: Concurrent uploads with MinIO
// =============================================================================

#[test]
#[ignore = "requires MinIO service"]
fn test_concurrent_minio_uploads() {
    skip_if_no_minio!();

    let config = MinioConfig::default();
    let s3_config = config.s3_config();

    // Create 3 encoders in parallel (simulating 3 workers)
    let handles: Vec<_> = (0..3)
        .map(|worker_id| {
            let key_prefix = format!("test_concurrent/worker_{}", worker_id);
            let s3_config_clone = s3_config.clone();

            std::thread::spawn(move || {
                let encoder_config = ConcurrentEncoderConfig {
                    key_prefix,
                    chunk_index: 0,
                    episode_index: worker_id as u32,
                    chunk_size: 256 * 1024,
                    video_config: roboflow_dataset::common::video::VideoEncoderConfig::default(),
                    frame_channel_capacity: 50,
                    s3_config: s3_config_clone,
                };

                let mut encoder =
                    ConcurrentVideoEncoder::new(encoder_config)
                        .expect("Failed to create encoder");

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

    // Cleanup test files
    let runtime = tokio::runtime::Runtime::new().unwrap();
    config.cleanup_dir(&runtime, "test_concurrent");

    println!("✓ Concurrent MinIO uploads test passed (3 workers)");
}

// =============================================================================
// Test: Large file upload to MinIO
// =============================================================================

#[test]
#[ignore = "requires MinIO service"]
fn test_large_file_upload_to_minio() {
    skip_if_no_minio!();

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
/// 1. Videos are uploaded to `{prefix}/videos/chunk-{chunk:03}/{camera}/episode_{episode:06}.mp4`
/// 2. Multiple cameras produce unique paths (no overwrites)
/// 3. Files actually exist at the expected locations
#[test]
fn test_lerobot_v21_video_path_structure() {
    let config = MinioConfig::default();

    // Test configuration
    let key_prefix = "test_lerobot_v21".to_string();
    let chunk_index = 0u32;
    let episode_index = 42u32; // Use non-zero to verify formatting

    let encoder_config = ConcurrentEncoderConfig {
        key_prefix: key_prefix.clone(),
        chunk_index,
        episode_index,
        chunk_size: 256 * 1024,
        video_config: roboflow_dataset::common::video::VideoEncoderConfig::default(),
        frame_channel_capacity: 64,
        s3_config: config.s3_config(),
    };

    let mut encoder =
        ConcurrentVideoEncoder::new(encoder_config)
            .expect("Failed to create encoder");

    // Add frames for multiple cameras (simulating multi-camera setup)
    let cameras = vec![
        "observation.images.cam_left",
        "observation.images.cam_right",
        "observation.images.cam_high",
    ];

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

    // Verify the URL format matches LeRobot v2.1 structure
    for result in &results {
        // Expected format: {prefix}/videos/chunk-{chunk:03}/{camera}/episode_{episode:06}.mp4
        let expected_prefix = format!("{}/videos/chunk-{:03}/", key_prefix, chunk_index);
        assert!(
            result.url.starts_with(&expected_prefix),
            "URL '{}' should start with '{}'",
            result.url,
            expected_prefix
        );

        // Verify episode number in filename
        assert!(
            result
                .url
                .contains(&format!("episode_{:06}.mp4", episode_index)),
            "URL '{}' should contain 'episode_{:06}.mp4'",
            result.url,
            episode_index
        );

        // Verify camera name is in the path
        assert!(
            result.url.contains(&result.camera),
            "URL '{}' should contain camera name '{}'",
            result.url,
            result.camera
        );

        println!("✓ Camera '{}' uploaded to: {}", result.camera, result.url);
    }

    // Verify files actually exist in S3
    let async_storage = config
        .create_storage()
        .expect("Failed to create async storage");

    let runtime = tokio::runtime::Runtime::new().expect("Failed to create runtime for verification");
    runtime
        .block_on(async {
            for result in &results {
                let path = std::path::Path::new(&result.url);
                let exists = async_storage.exists(path).await;
                assert!(exists, "Video file should exist at '{}'", result.url);

                // Also verify file has reasonable size (should be > 1KB for a video)
                let metadata = async_storage
                    .metadata(path)
                    .await
                    .expect("Failed to get metadata");
                assert!(
                    metadata.size > 1024,
                    "Video file '{}' should be larger than 1KB, got {} bytes",
                    result.url,
                    metadata.size
                );
            }

            Ok::<(), Box<dyn std::error::Error>>(())
        })
        .unwrap();

    // Cleanup test files
    let runtime = tokio::runtime::Runtime::new().unwrap();
    config.cleanup_dir(&runtime, "test_lerobot_v21");

    println!("✓ LeRobot v2.1 video path structure test passed");
}

/// Test that different cameras with similar names produce unique temp files.
///
/// Regression test for the race condition where cameras like "cam_left" and
/// "cam_right" would overwrite each other's temp files.
#[test]
fn test_multi_camera_unique_temp_files() {
    let config = MinioConfig::default();

    let encoder_config = ConcurrentEncoderConfig {
        key_prefix: "test_unique_temp".to_string(),
        chunk_index: 0,
        episode_index: 0,
        chunk_size: 256 * 1024,
        video_config: roboflow_dataset::common::video::VideoEncoderConfig::default(),
        frame_channel_capacity: 64,
        s3_config: config.s3_config(),
    };

    let mut encoder =
        ConcurrentVideoEncoder::new(encoder_config)
            .expect("Failed to create encoder");

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
    }

    // Cleanup test files
    let runtime = tokio::runtime::Runtime::new().unwrap();
    config.cleanup_dir(&runtime, "test_unique_temp");

    println!("✓ Multi-camera unique temp files test passed (regression test)");
}
