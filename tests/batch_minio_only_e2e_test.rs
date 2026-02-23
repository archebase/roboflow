// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! MinIO-only batch workflow tests (no TiKV required).
//!
//! These tests verify the batch processing pipeline using MinIO for storage
//! and local state management (no distributed coordination).
//!
//! # Prerequisites
//!
//! ```bash
//! docker compose up -d minio minio-init
//! ```
//!
//! Tests will FAIL if MinIO is not available.
//!
//! # Running
//!
//! ```bash
//! cargo test --test batch_minio_only_e2e_test -- --nocapture
//! ```

use std::path::{Path, PathBuf};

use bytes::Bytes;

use roboflow_dataset::{
    formats::common::DatasetWriter,
    formats::common::config::DatasetBaseConfig,
    formats::lerobot::config::{
        DatasetConfig as LeRobotDatasetConfig, FlushingConfig, LerobotConfig, StreamingConfig,
        VideoConfig,
    },
    formats::lerobot::{LerobotWriter, LerobotWriterTrait},
    testing::FrameBuilder,
};
use roboflow_storage::{
    AsyncStorage,
    s3::{AsyncS3Storage, S3Config},
};

// =============================================================================
// Test Configuration
// =============================================================================

/// MinIO test configuration.
#[derive(Debug, Clone)]
struct MinioConfig {
    endpoint: String,
    access_key: String,
    secret_key: String,
    input_bucket: String,
    output_bucket: String,
}

impl Default for MinioConfig {
    fn default() -> Self {
        Self {
            endpoint: std::env::var("MINIO_ENDPOINT")
                .unwrap_or_else(|_| "http://localhost:9000".to_string()),
            access_key: std::env::var("MINIO_ACCESS_KEY")
                .unwrap_or_else(|_| "minioadmin".to_string()),
            secret_key: std::env::var("MINIO_SECRET_KEY")
                .unwrap_or_else(|_| "minioadmin".to_string()),
            input_bucket: "roboflow-raw".to_string(),
            output_bucket: "roboflow-datasets".to_string(),
        }
    }
}

impl MinioConfig {
    fn create_input_storage(&self) -> Result<AsyncS3Storage, Box<dyn std::error::Error>> {
        let config = S3Config::new(
            &self.input_bucket,
            &self.endpoint,
            &self.access_key,
            &self.secret_key,
        )
        .with_allow_http(true);
        Ok(AsyncS3Storage::with_config(config)?)
    }

    fn create_output_storage(&self) -> Result<AsyncS3Storage, Box<dyn std::error::Error>> {
        let config = S3Config::new(
            &self.output_bucket,
            &self.endpoint,
            &self.access_key,
            &self.secret_key,
        )
        .with_allow_http(true);
        Ok(AsyncS3Storage::with_config(config)?)
    }

    async fn is_available(&self) -> bool {
        match self.create_input_storage() {
            Ok(storage) => {
                let test_path = Path::new("__test__/health-check.txt");
                let test_data = Bytes::from("test");
                storage.write(test_path, test_data).await.is_ok()
            }
            Err(_) => false,
        }
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn get_available_bag_files() -> Vec<PathBuf> {
    let fixtures = fixtures_dir();
    let candidates = vec![
        fixtures.join("roboflow_sample.bag"),
        fixtures.join("roboflow_extracted.bag"),
    ];
    candidates.into_iter().filter(|p| p.exists()).collect()
}

async fn upload_file(
    storage: &AsyncS3Storage,
    local_path: &Path,
    remote_path: &Path,
) -> Result<String, Box<dyn std::error::Error>> {
    let data = tokio::fs::read(local_path).await?;
    let size = data.len();
    storage.write(remote_path, Bytes::from(data)).await?;
    Ok(format!(
        "s3://{}/{} ({} bytes)",
        storage.bucket(),
        remote_path.display(),
        size
    ))
}

// =============================================================================
// E2E Tests
// =============================================================================

/// Test LeRobot dataset generation and upload to MinIO.
///
/// This test creates a LeRobot dataset locally and uploads it to MinIO
/// to verify the storage layer works correctly.
#[tokio::test]
async fn test_e2e_lerobot_dataset_to_minio() {
    let _ = tracing_subscriber::fmt::try_init();

    let config = MinioConfig::default();
    if !config.is_available().await {
        panic!("Required service MinIO is not available.");
    }

    println!("✓ MinIO is available");

    // Create a local LeRobot dataset with 1 episode per chunk
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let lerobot_config = LerobotConfig {
        dataset: LeRobotDatasetConfig {
            base: DatasetBaseConfig {
                name: "minio_upload_test".to_string(),
                fps: 30,
                robot_type: Some("test_robot".to_string()),
            },
            env_type: None,
        },
        mappings: vec![],
        video: VideoConfig::default(),
        annotation_file: None,
        flushing: FlushingConfig::default(),
        streaming: StreamingConfig::default(),
    };

    let mut writer =
        LerobotWriter::new_local(temp_dir.path(), lerobot_config).expect("Failed to create writer");

    // Set 1 episode per chunk
    writer.set_episodes_per_chunk(1);

    // Create 2 episodes
    for ep_idx in 0..2 {
        writer.set_episode_index(ep_idx);
        writer
            .start_episode(Some(ep_idx))
            .expect("Failed to start episode");

        for i in 0..5 {
            let frame = FrameBuilder::new(i)
                .with_timestamp(i as u64 * 33_333_333)
                .add_state("observation.state", vec![ep_idx as f32, i as f32])
                .add_action("action", vec![(ep_idx + i) as f32])
                .build();
            writer.write_frame(&frame).expect("Failed to write frame");
        }

        writer
            .finish_episode(Some(ep_idx))
            .expect("Failed to finish episode");
    }

    let stats = writer.finalize_with_config().expect("Failed to finalize");
    println!("Created dataset with {} frames", stats.frames_written);

    // Upload to MinIO
    let output_storage = config
        .create_output_storage()
        .expect("Failed to create storage");
    let test_prefix = format!("test-datasets/lerobot-{}", uuid::Uuid::new_v4());

    println!(
        "Uploading dataset to MinIO at s3://{}/{}...",
        config.output_bucket, test_prefix
    );

    // Walk the temp directory and upload all files (using stack to avoid recursion)
    let mut uploaded_count = 0;
    let mut dirs = vec![temp_dir.path().to_path_buf()];
    let base_path = temp_dir.path().to_path_buf();

    while let Some(dir) = dirs.pop() {
        let mut entries = tokio::fs::read_dir(&dir).await.expect("Failed to read dir");
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.is_file() {
                let relative_path = path.strip_prefix(&base_path).unwrap();
                let remote_path = Path::new(&test_prefix).join(relative_path);
                match upload_file(&output_storage, &path, &remote_path).await {
                    Ok(url) => {
                        println!("  Uploaded: {}", url);
                        uploaded_count += 1;
                    }
                    Err(e) => {
                        println!("  Failed to upload {}: {}", path.display(), e);
                    }
                }
            } else if path.is_dir() {
                dirs.push(path);
            }
        }
    }

    println!("✓ Uploaded {} files to MinIO", uploaded_count);

    // Verify files exist in MinIO by checking each uploaded file
    println!("Verifying upload by checking individual files...");
    let mut verified_count = 0;

    // Check that the key files exist
    let expected_files = vec![
        format!("{}/meta/info.json", test_prefix),
        format!("{}/meta/episodes.jsonl", test_prefix),
    ];

    for file_path in &expected_files {
        match output_storage.exists(Path::new(file_path)).await {
            true => {
                println!("  ✓ Found: {}", file_path);
                verified_count += 1;
            }
            false => {
                println!("  ✗ Missing: {}", file_path);
            }
        }
    }

    // Check for expected files
    let data_dir = temp_dir.path().join("data");
    let chunk_dirs: Vec<_> = std::fs::read_dir(&data_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .collect();

    println!("Chunk directories in dataset: {}", chunk_dirs.len());
    for dir in &chunk_dirs {
        println!("  - {:?}", dir.file_name());
    }

    assert!(
        verified_count > 0,
        "Should have uploaded and verified files in MinIO"
    );

    println!("✓ LeRobot dataset to MinIO test passed");
}

/// Test bag file upload to MinIO and verify integrity.
#[tokio::test]
async fn test_e2e_bag_file_upload_to_minio() {
    let _ = tracing_subscriber::fmt::try_init();

    let config = MinioConfig::default();
    if !config.is_available().await {
        panic!("Required service MinIO is not available");
    }

    let bag_files = get_available_bag_files();
    if bag_files.is_empty() {
        panic!("No bag files found in tests/fixtures/");
    }

    let input_storage = config
        .create_input_storage()
        .expect("Failed to create storage");
    let test_prefix = format!("test-inputs/bag-files-{}", uuid::Uuid::new_v4());

    println!("Uploading {} bag files to MinIO...", bag_files.len());

    for (i, bag_file) in bag_files.iter().enumerate() {
        let bag_name = format!("episode_{:03}.bag", i);
        let remote_path = Path::new(&test_prefix).join(&bag_name);

        match upload_file(&input_storage, bag_file, &remote_path).await {
            Ok(url) => println!("  Uploaded: {}", url),
            Err(e) => println!("  Failed: {}", e),
        }
    }

    // Verify uploads
    let object_store = input_storage.object_store();
    let list_result = object_store
        .list_with_delimiter(Some(&object_store::path::Path::from(test_prefix.clone())))
        .await
        .expect("Failed to list objects");

    println!("Verified {} files in MinIO", list_result.objects.len());
    assert_eq!(
        list_result.objects.len(),
        bag_files.len(),
        "All bag files should be uploaded"
    );

    println!("✓ Bag file upload test passed");
}

/// Test batch workflow simulation without TiKV.
///
/// This test simulates the batch processing workflow using local state
/// instead of distributed coordination.
#[tokio::test]
async fn test_e2e_batch_workflow_local_coordination() {
    let _ = tracing_subscriber::fmt::try_init();

    let config = MinioConfig::default();
    if !config.is_available().await {
        panic!("Required service MinIO is not available");
    }

    let bag_files = get_available_bag_files();
    if bag_files.is_empty() {
        panic!("No bag files found in tests/fixtures/");
    }

    println!(
        "Simulating batch workflow with {} bag files...",
        bag_files.len()
    );

    // Step 1: Upload bag files to MinIO (input)
    let input_storage = config
        .create_input_storage()
        .expect("Failed to create input storage");
    let input_prefix = format!("batch-test-{}/input", uuid::Uuid::new_v4());

    println!("1. Uploading bag files...");
    let mut uploaded_urls = Vec::new();
    for (i, bag_file) in bag_files.iter().enumerate() {
        let bag_name = format!("episode_{:03}.bag", i);
        let remote_path = Path::new(&input_prefix).join(&bag_name);

        match upload_file(&input_storage, bag_file, &remote_path).await {
            Ok(url) => {
                println!("   ✓ {}", bag_name);
                uploaded_urls.push((bag_name, url));
            }
            Err(e) => {
                println!("   ✗ {}: {}", bag_name, e);
            }
        }
    }

    // Step 2: Create work units (simulated - no TiKV)
    println!("2. Creating work units (local simulation)...");
    let work_units: Vec<_> = uploaded_urls
        .iter()
        .enumerate()
        .map(|(i, (name, url))| {
            println!("   ✓ Work unit {}: {}", i, name);
            (i, url.clone())
        })
        .collect();

    // Step 3: Process each work unit
    println!("3. Processing work units...");
    let temp_output = tempfile::tempdir().expect("Failed to create temp dir");

    for (i, url) in &work_units {
        println!("   Processing unit {} (S3: {})...", i, url);
        // In a real test, we would download from S3 and process
        // For this simulation, we just verify the URL format
        assert!(url.starts_with("s3://"), "URL should be S3 format");
        println!("   ✓ Unit {} would be processed here", i);
    }

    // Step 4: Upload output to MinIO
    println!("4. Uploading output to MinIO...");
    let output_storage = config
        .create_output_storage()
        .expect("Failed to create output storage");
    let output_prefix = format!("batch-test-{}/output", uuid::Uuid::new_v4());

    // Create a simple output file to simulate dataset output
    let output_file = temp_output.path().join("test_output.txt");
    tokio::fs::write(&output_file, b"Test dataset output")
        .await
        .expect("Failed to write output");

    let remote_output = Path::new(&output_prefix).join("test_output.txt");
    match upload_file(&output_storage, &output_file, &remote_output).await {
        Ok(_) => println!("   ✓ Output uploaded"),
        Err(e) => println!("   ✗ Upload failed: {}", e),
    }

    println!("\n✓ Batch workflow simulation complete");
    println!("  Input: s3://{}/{}/", config.input_bucket, input_prefix);
    println!("  Output: s3://{}/{}/", config.output_bucket, output_prefix);
}

/// Test dataset integrity after MinIO round-trip.
#[tokio::test]
async fn test_e2e_dataset_minio_roundtrip() {
    let _ = tracing_subscriber::fmt::try_init();

    let config = MinioConfig::default();
    if !config.is_available().await {
        panic!("Required service MinIO is not available");
    }

    let output_storage = config
        .create_output_storage()
        .expect("Failed to create storage");
    let test_prefix = format!("roundtrip-test-{}", uuid::Uuid::new_v4());

    // Create a dataset
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let lerobot_config = LerobotConfig {
        dataset: LeRobotDatasetConfig {
            base: DatasetBaseConfig {
                name: "roundtrip_test".to_string(),
                fps: 30,
                robot_type: Some("test_robot".to_string()),
            },
            env_type: None,
        },
        mappings: vec![],
        video: VideoConfig::default(),
        annotation_file: None,
        flushing: FlushingConfig::default(),
        streaming: StreamingConfig::default(),
    };

    let mut writer =
        LerobotWriter::new_local(temp_dir.path(), lerobot_config).expect("Failed to create writer");
    writer.set_episodes_per_chunk(1);

    // Create 1 episode with 3 frames
    writer.set_episode_index(0);
    writer
        .start_episode(Some(0))
        .expect("Failed to start episode");

    for i in 0..3 {
        let frame = FrameBuilder::new(i)
            .with_timestamp(i as u64 * 33_333_333)
            .add_state("observation.state", vec![i as f32])
            .add_action("action", vec![(i + 1) as f32])
            .build();
        writer.write_frame(&frame).expect("Failed to write frame");
    }

    writer
        .finish_episode(Some(0))
        .expect("Failed to finish episode");
    let stats = writer.finalize_with_config().expect("Failed to finalize");

    println!("Created dataset: {} frames", stats.frames_written);

    // Upload to MinIO
    println!("Uploading to MinIO...");
    let mut uploaded_files: Vec<(PathBuf, PathBuf, String)> = Vec::new();

    // Use stack-based approach to avoid recursion
    let mut dirs = vec![temp_dir.path().to_path_buf()];
    let base_path = temp_dir.path().to_path_buf();

    while let Some(dir) = dirs.pop() {
        let mut entries = tokio::fs::read_dir(&dir).await.expect("Failed to read dir");
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.is_file() {
                let relative_path = path.strip_prefix(&base_path).unwrap();
                let remote_path = Path::new(&test_prefix).join(relative_path);

                match upload_file(&output_storage, &path, &remote_path).await {
                    Ok(url) => {
                        uploaded_files.push((path.clone(), remote_path, url));
                    }
                    Err(e) => {
                        println!("  Failed: {}", e);
                    }
                }
            } else if path.is_dir() {
                dirs.push(path);
            }
        }
    }

    println!("Uploaded {} files", uploaded_files.len());

    // Download and verify
    println!("Downloading and verifying...");
    for (local_path, remote_path, _) in &uploaded_files {
        let downloaded = output_storage
            .read(remote_path)
            .await
            .expect("Failed to download");
        let original = tokio::fs::read(local_path)
            .await
            .expect("Failed to read local file");

        assert_eq!(
            downloaded.len(),
            original.len(),
            "File size mismatch for {:?}",
            remote_path
        );
        assert_eq!(
            downloaded.as_ref(),
            original.as_slice(),
            "File content mismatch for {:?}",
            remote_path
        );
        println!("  ✓ {:?} verified", remote_path.file_name().unwrap());
    }

    println!("✓ Round-trip test passed - all files verified");
}
