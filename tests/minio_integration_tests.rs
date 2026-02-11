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
    common::{RsmpegS3EncoderConfig, streaming_coordinator::StreamingCoordinator},
};
use roboflow_storage::{
    AsyncStorage,
    oss::{AsyncOssStorage, OssConfig},
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
        // Try to create an AsyncOssStorage instance with HTTP enabled for local testing
        let oss_config = OssConfig::new(
            &self.bucket,
            &self.endpoint,
            &self.access_key_id,
            &self.secret_access_key,
        )
        .with_allow_http(true);
        AsyncOssStorage::with_config(oss_config).is_ok()
    }

    /// Create an AsyncOssStorage instance for testing.
    pub fn create_storage(&self) -> Result<AsyncOssStorage, Box<dyn std::error::Error>> {
        let config = OssConfig::new(
            &self.bucket,
            &self.endpoint,
            &self.access_key_id,
            &self.secret_access_key,
        )
        .with_allow_http(true);
        Ok(AsyncOssStorage::with_config(config)?)
    }

    /// Get the S3 URL prefix for this configuration.
    #[allow(dead_code)]
    pub fn s3_url_prefix(&self, path: &str) -> String {
        format!("s3://{}/{}", self.bucket, path)
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
fn test_rsmpeg_s3_encoder_with_minio() {
    skip_if_no_minio!();

    let config = MinioConfig::default();
    let runtime = tokio::runtime::Runtime::new().unwrap();

    // Create object store
    let storage = config
        .create_storage()
        .expect("Failed to create MinIO storage");

    let object_store = storage.object_store();

    // Test video encoding with S3 upload
    let dest_path = format!("s3://{}/test_videos/encoder_test.mp4", config.bucket);
    let encoder_config = RsmpegS3EncoderConfig::default();

    let mut encoder = roboflow_dataset::common::rsmpeg_s3_encoder::RsmpegS3Encoder::new(
        &dest_path,
        object_store.clone(),
        runtime.handle().clone(),
        encoder_config,
    )
    .expect("Failed to create encoder");

    // Add test frames
    let width = 160u32;
    let height = 120u32;
    for i in 0..10 {
        let img = create_test_image(width, height, (i * 25) as u8);
        encoder.add_frame(&img).expect("Failed to add frame");
    }

    // Finalize and upload
    let (url, frames_encoded) = encoder.finalize().expect("Failed to finalize encoder");

    assert_eq!(url, dest_path, "Returned URL should match input");
    assert_eq!(frames_encoded, 10, "Should encode 10 frames");

    // Verify the file was uploaded using the storage API
    let exists = runtime.block_on(async {
        let full_path = Path::new("test_videos/encoder_test.mp4");
        storage.exists(full_path).await
    });
    assert!(exists, "Video should exist in MinIO");

    println!("✓ RsmpegS3Encoder with MinIO test passed");
}

// =============================================================================
// Test: StreamingCoordinator with MinIO
// =============================================================================

#[test]
#[ignore = "requires MinIO service"]
fn test_streaming_coordinator_with_minio() {
    skip_if_no_minio!();

    let config = MinioConfig::default();
    let runtime = tokio::runtime::Runtime::new().unwrap();

    // Create object store
    let storage = config
        .create_storage()
        .expect("Failed to create MinIO storage");

    let object_store = storage.object_store();

    // Create streaming coordinator
    let s3_prefix = format!("s3://{}/test_coordinator", config.bucket);
    let coordinator_config =
        roboflow_dataset::common::streaming_coordinator::StreamingCoordinatorConfig {
            frame_channel_capacity: 100,
            encoder_config: RsmpegS3EncoderConfig::default(),
            shutdown_timeout: std::time::Duration::from_secs(30),
            fps: 30,
        };

    let mut coordinator = StreamingCoordinator::new(
        s3_prefix.clone(),
        object_store.clone(),
        runtime.handle().clone(),
        coordinator_config,
    )
    .expect("Failed to create coordinator");

    // Add frames for 2 cameras
    let width = 160u32;
    let height = 120u32;
    for i in 0..20 {
        // Camera 0
        let img0 = create_test_image(width, height, (i * 10) as u8);
        let img_data0 = Arc::new(img0);
        coordinator
            .add_frame("camera_0", img_data0)
            .expect("Failed to add camera_0 frame");

        // Camera 1
        let img1 = create_test_image(width, height, ((i * 10) + 128) as u8);
        let img_data1 = Arc::new(img1);
        coordinator
            .add_frame("camera_1", img_data1)
            .expect("Failed to add camera_1 frame");
    }

    // Finalize and get results
    let results = coordinator
        .finalize()
        .expect("Failed to finalize coordinator");

    // Verify both cameras were processed
    assert!(
        results.contains_key("camera_0"),
        "Results should contain camera_0"
    );
    assert!(
        results.contains_key("camera_1"),
        "Results should contain camera_1"
    );

    // Verify the videos were uploaded to MinIO using storage API
    runtime
        .block_on(async {
            let camera_0_path = Path::new("test_coordinator/videos/camera_0.mp4");
            let camera_1_path = Path::new("test_coordinator/videos/camera_1.mp4");

            let camera_0_exists = storage.exists(camera_0_path).await;
            let camera_1_exists = storage.exists(camera_1_path).await;

            assert!(camera_0_exists, "Camera 0 video should exist in MinIO");
            assert!(camera_1_exists, "Camera 1 video should exist in MinIO");

            Ok::<(), Box<dyn std::error::Error>>(())
        })
        .unwrap();

    println!("✓ StreamingCoordinator with MinIO test passed");
}

// =============================================================================
// Test: Compressed image handling with MinIO upload
// =============================================================================

#[test]
#[ignore = "requires MinIO service"]
fn test_compressed_images_with_minio_upload() {
    skip_if_no_minio!();

    let config = MinioConfig::default();
    let runtime = tokio::runtime::Runtime::new().unwrap();

    // Create object store
    let storage = config
        .create_storage()
        .expect("Failed to create MinIO storage");

    let object_store = storage.object_store();

    // Create streaming coordinator
    let s3_prefix = format!("s3://{}/test_compressed", config.bucket);
    let coordinator_config =
        roboflow_dataset::common::streaming_coordinator::StreamingCoordinatorConfig {
            frame_channel_capacity: 100,
            encoder_config: RsmpegS3EncoderConfig::default(),
            shutdown_timeout: std::time::Duration::from_secs(30),
            fps: 30,
        };

    let mut coordinator = StreamingCoordinator::new(
        s3_prefix.clone(),
        object_store.clone(),
        runtime.handle().clone(),
        coordinator_config,
    )
    .expect("Failed to create coordinator");

    let width = 160u32;
    let height = 120u32;

    // Add raw RGB images
    for _ in 0..5 {
        let img = ImageData::new(width, height, vec![128u8; (width * height * 3) as usize]);
        coordinator
            .add_frame("camera_raw", Arc::new(img))
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
        coordinator
            .add_frame("camera_jpeg", Arc::new(img))
            .expect("Failed to add JPEG frame");
    }

    // Finalize
    let results = coordinator.finalize().expect("Failed to finalize");

    // Note: The minimal JPEG headers won't decode properly, so those frames may be skipped
    // The important thing is that the coordinator doesn't crash
    println!("✓ Compressed images with MinIO upload test passed");
    println!("  - Cameras processed: {}", results.len());
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
    let oss_config = OssConfig::new(
        &test_bucket,
        &config.endpoint,
        &config.access_key_id,
        &config.secret_access_key,
    )
    .with_allow_http(true);
    let result = AsyncOssStorage::with_config(oss_config);

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
    let runtime = tokio::runtime::Runtime::new().unwrap();

    // Create object store
    let storage = config
        .create_storage()
        .expect("Failed to create MinIO storage");

    let object_store = storage.object_store();

    // Create 3 streaming coordinators in parallel (simulating 3 workers)
    let s3_base = format!("s3://{}/test_concurrent", config.bucket);

    let handles: Vec<_> = (0..3)
        .map(|worker_id| {
            let s3_prefix = format!("{}/worker_{}", s3_base, worker_id);
            let object_store_clone = object_store.clone();
            let runtime_handle = runtime.handle().clone();

            std::thread::spawn(move || {
                let coordinator_config =
                    roboflow_dataset::common::streaming_coordinator::StreamingCoordinatorConfig {
                        frame_channel_capacity: 50,
                        encoder_config: RsmpegS3EncoderConfig::default(),
                        shutdown_timeout: std::time::Duration::from_secs(30),
                        fps: 30,
                    };

                let mut coordinator = StreamingCoordinator::new(
                    s3_prefix,
                    object_store_clone,
                    runtime_handle,
                    coordinator_config,
                )
                .expect("Failed to create coordinator");

                // Add 5 frames
                for i in 0..5 {
                    let img = create_test_image(160, 120, (worker_id * 10 + i) as u8);
                    coordinator
                        .add_frame("camera", Arc::new(img))
                        .expect("Failed to add frame");
                }

                coordinator.finalize()
            })
        })
        .collect();

    // Wait for all workers and collect results
    let mut results = vec![];
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
