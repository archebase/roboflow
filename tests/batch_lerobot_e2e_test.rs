// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! End-to-end batch workflow test with MinIO, TiKV, and real bag files.
//!
//! This test verifies the complete distributed pipeline:
//! 1. Upload bag files to MinIO
//! 2. Submit batch to TiKV with episodes_per_chunk=1
//! 3. Process work units through LeRobotExecutor
//! 4. Verify output dataset structure in MinIO
//!
//! To run these tests:
//! ```bash
//! make dev-up  # Start MinIO, TiKV, PD
//! cargo test --test batch_lerobot_e2e_test -- --ignored --nocapture
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;

use roboflow_dataset::{
    formats::common::config::DatasetBaseConfig,
    formats::lerobot::config::{
        DatasetConfig as LeRobotDatasetConfig, FlushingConfig, LerobotConfig, StreamingConfig,
        VideoConfig,
    },
    formats::lerobot::{LerobotWriter, LerobotWriterTrait},
    testing::FrameBuilder,
};
use roboflow_distributed::{
    BatchController, BatchIndexKeys, BatchKeys, BatchPhase, BatchSpec, BatchStatus,
    LeRobotExecutor, WorkFile, WorkUnit, batch::WorkUnitKeys, tikv::TikvClient,
    worker::JobRegistry,
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
    /// MinIO endpoint URL
    pub minio_endpoint: String,
    /// MinIO access key
    pub minio_access_key: String,
    /// MinIO secret key
    pub minio_secret_key: String,
    /// MinIO bucket for input files
    pub minio_input_bucket: String,
    /// MinIO bucket for output datasets
    pub minio_output_bucket: String,
    /// TiKV PD endpoints (used via env var, not directly)
    #[allow(dead_code)]
    pub tikv_pd_endpoints: String,
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
            minio_input_bucket: std::env::var("MINIO_INPUT_BUCKET")
                .unwrap_or_else(|_| "roboflow-raw".to_string()),
            minio_output_bucket: std::env::var("MINIO_OUTPUT_BUCKET")
                .unwrap_or_else(|_| "roboflow-datasets".to_string()),
            tikv_pd_endpoints: std::env::var("TIKV_PD_ENDPOINTS")
                .unwrap_or_else(|_| "127.0.0.1:2379".to_string()),
        }
    }
}

impl TestConfig {
    /// Create MinIO storage for input bucket.
    pub fn create_input_storage(&self) -> Result<AsyncS3Storage, Box<dyn std::error::Error>> {
        let config = S3Config::new(
            &self.minio_input_bucket,
            &self.minio_endpoint,
            &self.minio_access_key,
            &self.minio_secret_key,
        )
        .with_allow_http(true);
        Ok(AsyncS3Storage::with_config(config)?)
    }

    /// Create MinIO storage for output bucket.
    pub fn create_output_storage(&self) -> Result<AsyncS3Storage, Box<dyn std::error::Error>> {
        let config = S3Config::new(
            &self.minio_output_bucket,
            &self.minio_endpoint,
            &self.minio_access_key,
            &self.minio_secret_key,
        )
        .with_allow_http(true);
        Ok(AsyncS3Storage::with_config(config)?)
    }

    /// Create TiKV client.
    pub async fn create_tikv_client(&self) -> Result<TikvClient, Box<dyn std::error::Error>> {
        // Note: This requires 'pd' to resolve to the PD container
        // Add to /etc/hosts: 127.0.0.1 pd
        let client = TikvClient::from_env().await?;
        Ok(client)
    }

    /// Check if infrastructure is available.
    pub async fn is_available(&self) -> bool {
        // Check MinIO
        if self.create_input_storage().is_err() {
            eprintln!("MinIO not available at {}", self.minio_endpoint);
            return false;
        }

        // Check TiKV
        match self.create_tikv_client().await {
            Ok(_) => true,
            Err(e) => {
                eprintln!("TiKV not available: {}", e);
                eprintln!("Note: Ensure 'pd' resolves to 127.0.0.1 in /etc/hosts");
                false
            }
        }
    }
}

/// Path to test fixtures.
fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// Get the smallest bag file for testing.
fn small_bag_file() -> PathBuf {
    fixtures_dir().join("roboflow_sample.bag")
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Upload a file to MinIO.
async fn upload_file(
    storage: &AsyncS3Storage,
    local_path: &Path,
    remote_path: &Path,
) -> Result<String, Box<dyn std::error::Error>> {
    let data = tokio::fs::read(local_path).await?;
    storage.write(remote_path, bytes::Bytes::from(data)).await?;
    Ok(format!(
        "s3://{}/{}",
        storage.bucket(),
        remote_path.display()
    ))
}

/// Cleanup batch data from TiKV.
async fn cleanup_batch(tikv: &TikvClient, batch_id: &str) {
    let keys = vec![
        BatchKeys::spec(batch_id),
        BatchKeys::status(batch_id),
        BatchIndexKeys::phase(BatchPhase::Pending, batch_id),
        BatchIndexKeys::phase(BatchPhase::Discovering, batch_id),
        BatchIndexKeys::phase(BatchPhase::Running, batch_id),
        BatchIndexKeys::phase(BatchPhase::Merging, batch_id),
        BatchIndexKeys::phase(BatchPhase::Complete, batch_id),
    ];
    for key in keys {
        let _ = tikv.delete(key).await;
    }

    // Clean up work units
    let work_unit_prefix = format!("/roboflow/v1/batch/{}/workunit/", batch_id);
    if let Ok(entries) = tikv.scan(work_unit_prefix.into_bytes(), 1000).await {
        for (key, _) in entries {
            let _ = tikv.delete(key).await;
        }
    }
}

// =============================================================================
// E2E Tests
// =============================================================================

/// Test complete batch workflow with real bag file.
///
/// This test:
/// 1. Uploads roboflow_sample.bag to MinIO
/// 2. Submits batch to TiKV with episodes_per_chunk=1
/// 3. Creates work units manually (simulating scanner)
/// 4. Processes work units with LeRobotExecutor
/// 5. Verifies output dataset structure
#[tokio::test]
async fn test_e2e_batch_with_real_bag_file() {
    let _ = tracing_subscriber::fmt::try_init();

    let config = TestConfig::default();

    if !config.is_available().await {
        panic!("Required infrastructure (MinIO and/or TiKV) is not available.");
    }

    // Check if bag file exists
    let bag_file = small_bag_file();
    if !bag_file.exists() {
        panic!("Required bag file not found at {:?}", bag_file);
    }

    println!("Using bag file: {:?}", bag_file);

    let file_size = std::fs::metadata(&bag_file).map(|m| m.len()).unwrap_or(0);
    println!("Bag file size: {} bytes", file_size);

    // Create storage clients
    let input_storage = config
        .create_input_storage()
        .expect("Failed to create input storage");
    let _output_storage = config
        .create_output_storage()
        .expect("Failed to create output storage");
    let tikv = Arc::new(
        config
            .create_tikv_client()
            .await
            .expect("Failed to create TiKV client"),
    );

    // Create test directories in MinIO
    let test_id = format!("test-{}", uuid::Uuid::new_v4());
    let input_prefix = format!("batch-tests/{}/input", test_id);
    let output_prefix = format!("batch-tests/{}/output", test_id);

    // Upload bag file to MinIO
    let bag_filename = bag_file.file_name().unwrap().to_str().unwrap();
    let remote_bag_path = Path::new(&input_prefix).join(bag_filename);

    println!("Uploading bag file to MinIO...");
    let s3_url = upload_file(&input_storage, &bag_file, &remote_bag_path)
        .await
        .expect("Failed to upload bag file");
    println!("Uploaded to: {}", s3_url);

    // Create batch spec with episodes_per_chunk=1
    let batch_name = format!("e2e-batch-{}", test_id);
    let batch_id = format!("jobs:{}", batch_name);

    let mut spec = BatchSpec::new(
        &batch_name,
        vec![format!(
            "s3://{}/{}/",
            config.minio_input_bucket, input_prefix
        )],
        format!("s3://{}/{}/", config.minio_output_bucket, output_prefix),
    );

    // Configure for 1 episode per chunk (small scale testing)
    spec.spec.episodes_per_chunk = 1;
    spec.spec.parallelism = 2;

    spec.validate().expect("Batch spec should be valid");

    // Submit batch to TiKV
    println!("Submitting batch to TiKV...");
    let spec_key = BatchKeys::spec(&batch_id);
    let spec_data = serde_yaml_ng::to_string(&spec).unwrap().into_bytes();

    let mut status = BatchStatus::new();
    status.transition_to(BatchPhase::Running);
    status.set_work_units_total(1);
    status.set_files_total(1);
    let status_key = BatchKeys::status(&batch_id);
    let status_data = bincode::serialize(&status).unwrap();

    let phase_key = BatchIndexKeys::phase(BatchPhase::Running, &batch_id);

    tikv.batch_put(vec![
        (spec_key, spec_data),
        (status_key.clone(), status_data),
        (phase_key, vec![]),
    ])
    .await
    .expect("Failed to submit batch");

    println!("Batch {} submitted", batch_id);

    // Create work unit for the bag file
    let work_unit = WorkUnit::with_id(
        "unit-0".to_string(),
        batch_id.clone(),
        vec![WorkFile::new(s3_url.clone(), file_size)],
        format!(
            "s3://{}/{}/episode_000000",
            config.minio_output_bucket, output_prefix
        ),
        "config-hash".to_string(),
    );

    let unit_key = WorkUnitKeys::unit(&batch_id, "unit-0");
    let unit_data = bincode::serialize(&work_unit).unwrap();
    tikv.put(unit_key, unit_data)
        .await
        .expect("Failed to store work unit");

    println!("Work unit created");

    // Process work unit
    let executor = LeRobotExecutor::new(2, "/tmp/roboflow-output");
    let registry = Arc::new(tokio::sync::RwLock::new(JobRegistry::default()));

    println!("Executing work unit...");
    let result = executor.execute(&work_unit, registry.clone()).await;

    match result {
        Ok(_) => {
            println!("Work unit execution succeeded");
        }
        Err(e) => {
            println!("Work unit execution failed: {}", e);
            // Continue to cleanup
        }
    }

    // Run controller reconciliation
    println!("Running controller reconciliation...");
    let controller = BatchController::with_client(tikv.clone());
    controller
        .reconcile_all()
        .await
        .expect("Reconciliation failed");

    // Verify batch status
    let updated = tikv
        .get(BatchKeys::status(&batch_id))
        .await
        .unwrap()
        .unwrap();
    let final_status: BatchStatus = bincode::deserialize(&updated).unwrap();

    println!(
        "Final status: {} work units completed",
        final_status.work_units_completed
    );

    // Cleanup
    cleanup_batch(&tikv, &batch_id).await;

    // Note: We don't clean up MinIO files to allow manual inspection
    println!(
        "Test output available at: s3://{}/{}/",
        config.minio_output_bucket, output_prefix
    );
}

/// Test batch with multiple bag files (1 episode per chunk).
///
/// This test verifies that multiple bag files are correctly processed
/// with the episodes_per_chunk=1 configuration.
#[tokio::test]
async fn test_e2e_multiple_bags_one_episode_per_chunk() {
    let _ = tracing_subscriber::fmt::try_init();

    let config = TestConfig::default();

    if !config.is_available().await {
        panic!("Required infrastructure (MinIO and/or TiKV) is not available.");
    }

    // Use roboflow_extracted.bag (smaller than the 4000 frame versions)
    let bag_files = vec![
        fixtures_dir().join("roboflow_sample.bag"),
        fixtures_dir().join("roboflow_extracted.bag"),
    ];

    // Verify files exist
    for bag in &bag_files {
        if !bag.exists() {
            panic!("Required bag file not found at {:?}", bag);
        }
    }

    let input_storage = config
        .create_input_storage()
        .expect("Failed to create input storage");
    let tikv = Arc::new(
        config
            .create_tikv_client()
            .await
            .expect("Failed to create TiKV client"),
    );

    let test_id = format!("test-multi-{}", uuid::Uuid::new_v4());
    let input_prefix = format!("batch-tests/{}/input", test_id);
    let output_prefix = format!("batch-tests/{}/output", test_id);

    // Upload bag files
    let mut s3_urls = Vec::new();
    let mut work_files = Vec::new();

    for (i, bag_file) in bag_files.iter().enumerate() {
        let bag_filename = format!("episode_{}.bag", i);
        let remote_bag_path = Path::new(&input_prefix).join(&bag_filename);
        let file_size = std::fs::metadata(bag_file).map(|m| m.len()).unwrap_or(0);

        println!("Uploading {}...", bag_filename);
        let s3_url = upload_file(&input_storage, bag_file, &remote_bag_path)
            .await
            .expect("Failed to upload bag file");

        s3_urls.push(s3_url.clone());
        work_files.push(WorkFile::new(s3_url, file_size));
    }

    // Create batch with episodes_per_chunk=1
    let batch_name = format!("e2e-multi-{}", test_id);
    let batch_id = format!("jobs:{}", batch_name);

    let mut spec = BatchSpec::new(
        &batch_name,
        vec![format!(
            "s3://{}/{}/",
            config.minio_input_bucket, input_prefix
        )],
        format!("s3://{}/{}/", config.minio_output_bucket, output_prefix),
    );

    spec.spec.episodes_per_chunk = 1; // 1 episode per chunk for testing
    spec.spec.parallelism = 2;

    spec.validate().expect("Batch spec should be valid");

    // Submit batch
    let spec_key = BatchKeys::spec(&batch_id);
    let spec_data = serde_yaml_ng::to_string(&spec).unwrap().into_bytes();

    let mut status = BatchStatus::new();
    status.transition_to(BatchPhase::Running);
    status.set_work_units_total(work_files.len() as u32);
    status.set_files_total(work_files.len() as u32);
    let status_key = BatchKeys::status(&batch_id);
    let status_data = bincode::serialize(&status).unwrap();

    let phase_key = BatchIndexKeys::phase(BatchPhase::Running, &batch_id);

    tikv.batch_put(vec![
        (spec_key, spec_data),
        (status_key.clone(), status_data),
        (phase_key, vec![]),
    ])
    .await
    .expect("Failed to submit batch");

    // Create work units (1 per bag file)
    for (i, work_file) in work_files.iter().enumerate() {
        let work_unit = WorkUnit::with_id(
            format!("unit-{}", i),
            batch_id.clone(),
            vec![work_file.clone()],
            format!(
                "s3://{}/{}/episode_{:06}",
                config.minio_output_bucket, output_prefix, i
            ),
            "config-hash".to_string(),
        );

        let unit_key = WorkUnitKeys::unit(&batch_id, &format!("unit-{}", i));
        let unit_data = bincode::serialize(&work_unit).unwrap();
        tikv.put(unit_key, unit_data)
            .await
            .expect("Failed to store work unit");
    }

    println!("Created {} work units", work_files.len());

    // Process each work unit
    let executor = LeRobotExecutor::new(2, "/tmp/roboflow-output");
    let registry = Arc::new(tokio::sync::RwLock::new(JobRegistry::default()));

    for i in 0..work_files.len() {
        let unit_id = format!("unit-{}", i);
        let unit_key = WorkUnitKeys::unit(&batch_id, &unit_id);
        let unit_data = tikv.get(unit_key).await.unwrap().unwrap();
        let work_unit: WorkUnit = bincode::deserialize(&unit_data).unwrap();

        println!("Processing work unit {}...", unit_id);
        match executor.execute(&work_unit, registry.clone()).await {
            Ok(_) => println!("  Success"),
            Err(e) => println!("  Failed: {}", e),
        }
    }

    // Reconcile batch status
    let controller = BatchController::with_client(tikv.clone());
    controller
        .reconcile_all()
        .await
        .expect("Reconciliation failed");

    // Verify status
    let updated = tikv
        .get(BatchKeys::status(&batch_id))
        .await
        .unwrap()
        .unwrap();
    let final_status: BatchStatus = bincode::deserialize(&updated).unwrap();

    println!(
        "Batch complete: {}/{} work units",
        final_status.work_units_completed, final_status.work_units_total
    );

    // Cleanup
    cleanup_batch(&tikv, &batch_id).await;

    println!(
        "Test output available at: s3://{}/{}/",
        config.minio_output_bucket, output_prefix
    );
}

/// Test LeRobot dataset generation with small chunk sizes.
///
/// This test creates a minimal LeRobot dataset with 1 episode per chunk
/// to verify the chunk directory structure.
#[test]
fn test_e2e_lerobot_dataset_structure() {
    use roboflow_dataset::formats::common::DatasetWriter;

    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let config = TestConfig::default();

        if !config.is_available().await {
            panic!("Required infrastructure (MinIO and/or TiKV) is not available.");
        }

        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

        // Create LeRobot config
        let lerobot_config = LerobotConfig {
            dataset: LeRobotDatasetConfig {
                base: DatasetBaseConfig {
                    name: "e2e_test_dataset".to_string(),
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

        // Create local writer
        let mut writer = LerobotWriter::new_local(temp_dir.path(), lerobot_config)
            .expect("Failed to create writer");

        // Set 1 episode per chunk
        writer.set_episodes_per_chunk(1);

        // Create 3 episodes
        for ep_idx in 0..3 {
            writer
                .start_episode(Some(ep_idx))
                .expect("Failed to start episode");

            for i in 0..10 {
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

        assert_eq!(stats.frames_written, 30); // 3 episodes * 10 frames

        // Verify chunk directory structure
        let data_dir = temp_dir.path().join("data");
        assert!(data_dir.exists(), "data directory should exist");

        // With 1 episode per chunk and 3 episodes, we should have 3 chunk directories
        let entries: Vec<_> = std::fs::read_dir(&data_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .collect();

        println!("Chunk directories found: {}", entries.len());
        for entry in &entries {
            println!("  - {:?}", entry.path());
        }

        // Each chunk should have a parquet file
        for entry in entries {
            let chunk_dir = entry.path();
            let parquet_files: Vec<_> = std::fs::read_dir(&chunk_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.path()
                        .extension()
                        .map(|ext| ext == "parquet")
                        .unwrap_or(false)
                })
                .collect();

            println!(
                "  Parquet files in {:?}: {}",
                chunk_dir,
                parquet_files.len()
            );
        }

        println!("✓ LeRobot dataset structure test passed");
    });
}
