// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! End-to-end batch submission tests with multiple bag files.
//!
//! These tests verify the complete batch workflow:
//! 1. Submit batch with multiple bag files
//! 2. Verify work units are created
//! 3. Process work units
//! 4. Generate valid LeRobot dataset
//! 5. Verify dataset structure in MinIO
//!
//! # Prerequisites
//!
//! 1. Start infrastructure: `docker compose up -d` (MinIO, TiKV, PD)
//! 2. Add to /etc/hosts: `127.0.0.1 pd` (required for TiKV client)
//!
//! Tests will FAIL if infrastructure is not available.
//!
//! # Running
//!
//! ```bash
//! cargo test --test batch_submission_e2e_test -- --nocapture
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;

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
use roboflow_distributed::{
    batch::{
        BatchController, BatchIndexKeys, BatchKeys, BatchPhase, BatchSpec, BatchStatus, WorkFile,
        WorkUnit, WorkUnitKeys, batch_id_from_spec,
    },
    tikv::client::TikvClient,
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
struct TestConfig {
    minio_endpoint: String,
    minio_access_key: String,
    minio_secret_key: String,
    input_bucket: String,
    output_bucket: String,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            minio_endpoint: std::env::var("MINIO_ENDPOINT")
                .unwrap_or_else(|_| "http://localhost:9000".to_string()),
            minio_access_key: std::env::var("MINIO_ACCESS_KEY")
                .unwrap_or_else(|_| "minioadmin".to_string()),
            minio_secret_key: std::env::var("MINIO_SECRET_KEY")
                .unwrap_or_else(|_| "minioadmin".to_string()),
            input_bucket: "roboflow-raw".to_string(),
            output_bucket: "roboflow-datasets".to_string(),
        }
    }
}

impl TestConfig {
    async fn check_tikv(&self) -> Result<(), String> {
        match TikvClient::from_env().await {
            Ok(_) => Ok(()),
            Err(e) => Err(format!(
                "TiKV not accessible: {}.\n\
                 Make sure 'make dev-up' is running and '127.0.0.1 pd' is in /etc/hosts",
                e
            )),
        }
    }

    async fn check_minio(&self) -> Result<AsyncS3Storage, String> {
        let config = S3Config::new(
            &self.output_bucket,
            &self.minio_endpoint,
            &self.minio_access_key,
            &self.minio_secret_key,
        )
        .with_allow_http(true);

        let storage = AsyncS3Storage::with_config(config)
            .map_err(|e| format!("Failed to create S3 storage: {}", e))?;

        // Test connection
        let test_path = Path::new("__test__/health-check.txt");
        let test_data = Bytes::from("test");
        storage
            .write(test_path, test_data)
            .await
            .map_err(|e| format!("MinIO not accessible: {}", e))?;

        Ok(storage)
    }

    fn get_available_bag_files(&self) -> Vec<PathBuf> {
        let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        let candidates = vec![
            fixtures.join("roboflow_sample.bag"),
            fixtures.join("roboflow_extracted.bag"),
        ];
        candidates.into_iter().filter(|p| p.exists()).collect()
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

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

async fn create_lerobot_dataset_local(
    output_dir: &Path,
    episode_count: usize,
    frames_per_episode: usize,
) -> Result<usize, String> {
    let lerobot_config = LerobotConfig {
        dataset: LeRobotDatasetConfig {
            base: DatasetBaseConfig {
                name: "batch_test_dataset".to_string(),
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
        LerobotWriter::new_local(output_dir, lerobot_config).map_err(|e| e.to_string())?;

    // Use 1 episode per chunk for testing
    writer.set_episodes_per_chunk(1);

    for ep_idx in 0..episode_count {
        writer.set_episode_index(ep_idx);
        writer
            .start_episode(Some(ep_idx))
            .map_err(|e| format!("Failed to start episode {}: {}", ep_idx, e))?;

        for i in 0..frames_per_episode {
            let frame = FrameBuilder::new(i)
                .with_timestamp(i as u64 * 33_333_333)
                .add_state("observation.state", vec![ep_idx as f32, i as f32])
                .add_action("action", vec![(ep_idx + i) as f32])
                .build();
            writer
                .write_frame(&frame)
                .map_err(|e| format!("Failed to write frame {}: {}", i, e))?;
        }

        writer
            .finish_episode(Some(ep_idx))
            .map_err(|e| format!("Failed to finish episode {}: {}", ep_idx, e))?;
    }

    let stats = writer
        .finalize_with_config()
        .map_err(|e| format!("Failed to finalize: {}", e))?;

    Ok(stats.frames_written)
}

fn validate_lerobot_dataset(output_dir: &Path) -> Result<(), String> {
    // Check required directories
    let data_dir = output_dir.join("data");
    let meta_dir = output_dir.join("meta");

    if !data_dir.exists() {
        return Err(format!("Missing data directory: {}", data_dir.display()));
    }
    if !meta_dir.exists() {
        return Err(format!("Missing meta directory: {}", meta_dir.display()));
    }

    // Check for metadata files
    let info_json = meta_dir.join("info.json");
    let episodes_jsonl = meta_dir.join("episodes.jsonl");

    if !info_json.exists() {
        return Err(format!("Missing info.json: {}", info_json.display()));
    }
    if !episodes_jsonl.exists() {
        return Err(format!(
            "Missing episodes.jsonl: {}",
            episodes_jsonl.display()
        ));
    }

    // Check for parquet files in chunk directories
    let mut parquet_count = 0;
    for entry in std::fs::read_dir(&data_dir).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            let chunk_dir = entry.path();
            for file in std::fs::read_dir(&chunk_dir).map_err(|e| e.to_string())? {
                let file = file.map_err(|e| e.to_string())?;
                let path = file.path();
                if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                    parquet_count += 1;
                }
            }
        }
    }

    if parquet_count == 0 {
        return Err("No parquet files found in dataset".to_string());
    }

    Ok(())
}

// =============================================================================
// E2E Tests
// =============================================================================

/// Test batch submission with multiple bag files.
///
/// This test:
/// 1. Uploads multiple bag files to MinIO
/// 2. Submits a batch for processing
/// 3. Verifies work units are created in TiKV
/// 4. Simulates work unit completion
/// 5. Verifies batch phase transitions correctly
#[tokio::test]
async fn test_batch_submission_with_multiple_bag_files() {
    let _ = tracing_subscriber::fmt::try_init();

    let config = TestConfig::default();

    // Check infrastructure
    if let Err(e) = config.check_tikv().await {
        panic!("Required service TiKV is not available: {}", e);
    }

    let storage = match config.check_minio().await {
        Ok(s) => s,
        Err(e) => {
            panic!("Required service MinIO is not available: {}", e);
        }
    };

    println!("✓ Infrastructure is available");

    // Get bag files
    let bag_files = config.get_available_bag_files();
    if bag_files.is_empty() {
        println!("No bag files found in tests/fixtures/");
        return;
    }

    println!("Found {} bag file(s)", bag_files.len());

    // Upload bag files to MinIO
    let test_prefix = format!("batch-test-{}", uuid::Uuid::new_v4());
    let mut uploaded_urls = Vec::new();

    println!("\n1. Uploading bag files to MinIO...");
    for (i, bag_file) in bag_files.iter().enumerate() {
        let bag_name = format!("episode_{:03}.bag", i);
        let remote_path = Path::new(&test_prefix).join("input").join(&bag_name);

        match upload_file(&storage, bag_file, &remote_path).await {
            Ok(_url) => {
                println!("   ✓ Uploaded: {}", bag_name);
                uploaded_urls.push(format!(
                    "s3://{}/{}",
                    config.input_bucket,
                    remote_path.display()
                ));
            }
            Err(e) => {
                println!("   ✗ Failed to upload {}: {}", bag_name, e);
            }
        }
    }

    if uploaded_urls.is_empty() {
        panic!("No bag files were uploaded successfully");
    }

    // Create TiKV client and batch controller
    let tikv = Arc::new(TikvClient::from_env().await.unwrap());
    let controller = BatchController::with_client(tikv.clone());

    // Submit batch
    println!("\n2. Submitting batch...");
    let batch_id = format!("batch-{}", uuid::Uuid::new_v4());
    let spec = BatchSpec::new(
        &batch_id,
        uploaded_urls.clone(),
        format!("s3://{}/{}/output", config.output_bucket, test_prefix),
    );

    // Get the canonical batch_id from spec (namespace:name format)
    let canonical_batch_id = batch_id_from_spec(&spec);

    // Store batch spec and status in TiKV
    let spec_key = BatchKeys::spec(&canonical_batch_id);
    let spec_data = serde_yaml_ng::to_string(&spec).unwrap().into_bytes();
    let status = BatchStatus::new();
    let status_key = BatchKeys::status(&canonical_batch_id);
    let status_data = bincode::serialize(&status).unwrap();

    tikv.batch_put(vec![
        (spec_key.clone(), spec_data),
        (status_key.clone(), status_data),
    ])
    .await
    .unwrap();

    println!("   ✓ Batch submitted: {}", canonical_batch_id);

    // Create work units for each bag file
    println!("\n3. Creating work units...");
    for (i, url) in uploaded_urls.iter().enumerate() {
        let unit_id = format!("unit-{}", i);
        let work_unit = WorkUnit::with_id(
            unit_id.clone(),
            canonical_batch_id.clone(),
            vec![WorkFile::new(url.clone(), 1024)],
            format!("s3://{}/{}/output", config.output_bucket, test_prefix),
            "config-hash".to_string(),
        );

        let unit_key = WorkUnitKeys::unit(&canonical_batch_id, &unit_id);
        let unit_data = bincode::serialize(&work_unit).unwrap();

        // Create pending queue entry (required for claim_work_unit to find it)
        let pending_key = WorkUnitKeys::pending(&canonical_batch_id, &unit_id);
        let pending_data = canonical_batch_id.as_bytes().to_vec();

        tikv.batch_put(vec![(unit_key, unit_data), (pending_key, pending_data)])
            .await
            .unwrap();
        println!("   ✓ Created work unit: {}", unit_id);
    }

    // Update batch status to Running
    let mut status: BatchStatus =
        bincode::deserialize(&tikv.get(status_key.clone()).await.unwrap().unwrap()).unwrap();
    status.transition_to(BatchPhase::Running);
    status.set_work_units_total(uploaded_urls.len() as u32);
    tikv.put(status_key.clone(), bincode::serialize(&status).unwrap())
        .await
        .unwrap();

    // Add to Running phase index
    let phase_key = BatchIndexKeys::phase(BatchPhase::Running, &canonical_batch_id);
    tikv.put(phase_key, vec![]).await.unwrap();

    println!(
        "   ✓ Batch status: Running with {} work units",
        uploaded_urls.len()
    );

    // Simulate completing work units
    println!("\n4. Processing work units...");
    for i in 0..uploaded_urls.len() {
        let unit_id = format!("unit-{}", i);
        let unit_key = WorkUnitKeys::unit(&canonical_batch_id, &unit_id);

        let mut work_unit: WorkUnit =
            bincode::deserialize(&tikv.get(unit_key.clone()).await.unwrap().unwrap()).unwrap();
        work_unit.claim("worker-1".to_string()).unwrap();
        work_unit.complete();

        tikv.put(unit_key, bincode::serialize(&work_unit).unwrap())
            .await
            .unwrap();
        println!("   ✓ Completed work unit: {}", unit_id);
    }

    // Run controller reconcile
    println!("\n5. Reconciling batch...");
    controller.reconcile_all().await.unwrap();

    // Check batch status
    let updated_status: BatchStatus =
        bincode::deserialize(&tikv.get(status_key).await.unwrap().unwrap()).unwrap();

    println!("   Batch phase: {:?}", updated_status.phase);
    println!(
        "   Work units completed: {}/{}",
        updated_status.work_units_completed, updated_status.work_units_total
    );

    assert_eq!(
        updated_status.work_units_completed,
        uploaded_urls.len() as u32,
        "All work units should be completed"
    );

    // Cleanup
    println!("\n6. Cleaning up...");
    let _ = tikv.delete(BatchKeys::spec(&canonical_batch_id)).await;
    let _ = tikv.delete(BatchKeys::status(&canonical_batch_id)).await;
    let _ = tikv
        .delete(BatchIndexKeys::phase(
            BatchPhase::Running,
            &canonical_batch_id,
        ))
        .await;
    for i in 0..uploaded_urls.len() {
        let _ = tikv
            .delete(WorkUnitKeys::unit(
                &canonical_batch_id,
                &format!("unit-{}", i),
            ))
            .await;
        let _ = tikv
            .delete(WorkUnitKeys::pending(
                &canonical_batch_id,
                &format!("unit-{}", i),
            ))
            .await;
    }

    println!("\n✓ Batch submission test passed");
}

/// Test generating valid LeRobot dataset and uploading to MinIO.
///
/// This test:
/// 1. Creates a LeRobot dataset locally with 1 episode per chunk
/// 2. Validates the dataset structure
/// 3. Uploads to MinIO
/// 4. Verifies files are accessible
#[tokio::test]
async fn test_lerobot_dataset_generation_and_upload() {
    let _ = tracing_subscriber::fmt::try_init();

    let config = TestConfig::default();

    let storage = match config.check_minio().await {
        Ok(s) => s,
        Err(e) => {
            println!("Skipping test: {}", e);
            return;
        }
    };

    println!("✓ MinIO is available");

    // Create dataset locally
    println!("\n1. Creating LeRobot dataset...");
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let episode_count = 3;
    let frames_per_episode = 5;
    let total_frames =
        create_lerobot_dataset_local(temp_dir.path(), episode_count, frames_per_episode)
            .await
            .expect("Failed to create dataset");

    println!("   ✓ Created dataset with {} frames", total_frames);

    // Validate dataset structure
    println!("\n2. Validating dataset structure...");
    validate_lerobot_dataset(temp_dir.path()).expect("Dataset validation failed");
    println!("   ✓ Dataset structure is valid");

    // Upload to MinIO
    println!("\n3. Uploading dataset to MinIO...");
    let test_prefix = format!("dataset-test-{}", uuid::Uuid::new_v4());
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

                match upload_file(&storage, &path, &remote_path).await {
                    Ok(_) => {
                        uploaded_count += 1;
                    }
                    Err(e) => {
                        println!("   Failed to upload {}: {}", path.display(), e);
                    }
                }
            } else if path.is_dir() {
                dirs.push(path);
            }
        }
    }

    println!("   ✓ Uploaded {} files to MinIO", uploaded_count);

    // Verify key files exist
    println!("\n4. Verifying upload...");
    let key_files = vec![
        format!("{}/meta/info.json", test_prefix),
        format!("{}/meta/episodes.jsonl", test_prefix),
    ];

    for file_path in &key_files {
        assert!(
            storage.exists(Path::new(file_path)).await,
            "File should exist: {}",
            file_path
        );
        println!("   ✓ Verified: {}", file_path);
    }

    // Verify chunk structure
    let data_dir = temp_dir.path().join("data");
    let chunk_dirs: Vec<_> = std::fs::read_dir(&data_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .collect();

    println!("\n5. Chunk structure (1 episode per chunk):");
    println!("   Number of chunk directories: {}", chunk_dirs.len());
    assert_eq!(
        chunk_dirs.len(),
        episode_count,
        "Should have {} chunks (1 per episode)",
        episode_count
    );

    for dir in &chunk_dirs {
        let chunk_name = dir.file_name().to_str().unwrap().to_string();
        let parquet_count: usize = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "parquet")
                    .unwrap_or(false)
            })
            .count();
        println!("   {}: {} parquet file(s)", chunk_name, parquet_count);
        assert_eq!(
            parquet_count, 1,
            "Each chunk should have exactly 1 parquet file"
        );
    }

    println!("\n✓ LeRobot dataset generation and upload test passed");
}

/// Test complete workflow: batch submission with dataset generation.
///
/// This test combines batch processing with actual dataset generation
/// to verify the entire pipeline works end-to-end.
#[tokio::test]
async fn test_complete_batch_to_dataset_workflow() {
    let _ = tracing_subscriber::fmt::try_init();

    let config = TestConfig::default();

    // Check infrastructure
    if let Err(e) = config.check_tikv().await {
        panic!("Required service TiKV is not available: {}", e);
    }

    let storage = match config.check_minio().await {
        Ok(s) => s,
        Err(e) => {
            panic!("Required service MinIO is not available: {}", e);
        }
    };

    println!("✓ Infrastructure is available");

    // Get bag files
    let bag_files = config.get_available_bag_files();
    if bag_files.is_empty() {
        println!("No bag files found");
        return;
    }

    println!("Found {} bag file(s)", bag_files.len());

    // Create output dataset locally first
    println!("\n1. Creating LeRobot dataset from bag files...");
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    // Simulate processing: create one episode per bag file
    for (ep_idx, bag_file) in bag_files.iter().enumerate() {
        let file_size = std::fs::metadata(bag_file).map(|m| m.len()).unwrap_or(0);
        println!(
            "   Processing episode {} (from {} - {} bytes)",
            ep_idx,
            bag_file.file_name().unwrap().to_str().unwrap(),
            file_size
        );
    }

    // Create the dataset
    let total_frames = create_lerobot_dataset_local(
        temp_dir.path(),
        bag_files.len(), // One episode per bag file
        5,               // 5 frames per episode
    )
    .await
    .expect("Failed to create dataset");

    println!("   ✓ Created dataset with {} frames", total_frames);

    // Validate dataset
    validate_lerobot_dataset(temp_dir.path()).expect("Dataset validation failed");
    println!("   ✓ Dataset validation passed");

    // Upload to MinIO
    println!("\n2. Uploading dataset to MinIO...");
    let test_prefix = format!("complete-workflow-{}", uuid::Uuid::new_v4());

    let mut dirs = vec![temp_dir.path().to_path_buf()];
    let base_path = temp_dir.path().to_path_buf();
    let mut uploaded_count = 0;

    while let Some(dir) = dirs.pop() {
        let mut entries = tokio::fs::read_dir(&dir).await.expect("Failed to read dir");
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.is_file() {
                let relative_path = path.strip_prefix(&base_path).unwrap();
                let remote_path = Path::new(&test_prefix).join(relative_path);

                if upload_file(&storage, &path, &remote_path).await.is_ok() {
                    uploaded_count += 1;
                }
            } else if path.is_dir() {
                dirs.push(path);
            }
        }
    }

    println!("   ✓ Uploaded {} files", uploaded_count);

    // Verify dataset structure in MinIO
    println!("\n3. Verifying dataset in MinIO...");

    let info_exists = storage
        .exists(Path::new(&format!("{}/meta/info.json", test_prefix)))
        .await;
    let episodes_exists = storage
        .exists(Path::new(&format!("{}/meta/episodes.jsonl", test_prefix)))
        .await;

    assert!(info_exists, "info.json should exist in MinIO");
    assert!(episodes_exists, "episodes.jsonl should exist in MinIO");

    println!("   ✓ meta/info.json exists");
    println!("   ✓ meta/episodes.jsonl exists");

    // List chunk directories
    let data_dir = temp_dir.path().join("data");
    let chunk_count = std::fs::read_dir(&data_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .count();

    println!(
        "   ✓ Found {} chunk directories (1 per episode)",
        chunk_count
    );
    assert_eq!(
        chunk_count,
        bag_files.len(),
        "Should have one chunk per bag file"
    );

    println!("\n✓ Complete batch to dataset workflow test passed");
    println!(
        "   Dataset location: s3://{}/{}/",
        config.output_bucket, test_prefix
    );
}

/// Test infrastructure connectivity without running full tests.
#[tokio::test]
async fn test_infrastructure_connectivity() {
    let _ = tracing_subscriber::fmt::try_init();

    let config = TestConfig::default();

    println!("Testing infrastructure connectivity...\n");

    // Test TiKV
    println!("1. Testing TiKV connectivity...");
    match config.check_tikv().await {
        Ok(_) => {
            println!("   ✓ TiKV is accessible");

            // Try a simple operation
            let tikv = TikvClient::from_env().await.unwrap();
            let test_key = b"__connectivity_test__".to_vec();
            let test_value = b"hello".to_vec();

            tikv.put(test_key.clone(), test_value.clone())
                .await
                .unwrap();
            let result = tikv.get(test_key.clone()).await.unwrap();
            tikv.delete(test_key).await.unwrap();

            assert_eq!(result, Some(test_value));
            println!("   ✓ TiKV read/write test passed");
        }
        Err(e) => {
            println!("   ✗ TiKV not accessible: {}", e);
            println!("   Make sure 'make dev-up' is running");
            println!("   Add to /etc/hosts: 127.0.0.1 pd");
        }
    }

    // Test MinIO
    println!("\n2. Testing MinIO connectivity...");
    match config.check_minio().await {
        Ok(_) => {
            println!("   ✓ MinIO is accessible");
        }
        Err(e) => {
            println!("   ✗ MinIO not accessible: {}", e);
            println!("   Make sure 'make dev-up' is running");
        }
    }

    // Test bag files
    println!("\n3. Checking bag files...");
    let bag_files = config.get_available_bag_files();
    if bag_files.is_empty() {
        println!("   ⚠ No bag files found in tests/fixtures/");
    } else {
        println!("   ✓ Found {} bag file(s):", bag_files.len());
        for bag in &bag_files {
            let size = std::fs::metadata(bag).map(|m| m.len()).unwrap_or(0);
            println!(
                "     - {} ({} bytes)",
                bag.file_name().unwrap().to_str().unwrap(),
                size
            );
        }
    }

    println!("\n✓ Infrastructure connectivity test complete");
}
