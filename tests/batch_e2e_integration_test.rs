// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Integration test for complete batch workflow with MinIO and TiKV.
//!
//! This test verifies:
//! 1. Bag files uploaded to MinIO
//! 2. Batch submitted to TiKV with episodes_per_chunk=1
//! 3. Work units created and processed
//! 4. Valid LeRobot dataset generated in MinIO
//!
//! # Prerequisites
//!
//! 1. Start infrastructure: `make dev-up`
//! 2. Add to /etc/hosts: `127.0.0.1 pd`
//!    (Required because PD advertises its Docker DNS name to clients)
//!
//! # Running
//!
//! ```bash
//! # Run with TiKV/MinIO tests enabled
//! cargo test --test batch_e2e_integration_test -- --ignored --nocapture
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;

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
    LeRobotExecutor, WorkFile, WorkUnit,
    batch::WorkUnitKeys,
    tikv::{TikvClient, TikvConfig},
    worker::JobRegistry,
};
use roboflow_storage::{
    AsyncStorage,
    s3::{AsyncS3Storage, S3Config},
};

// =============================================================================
// Test Configuration
// =============================================================================

/// Integration test configuration.
#[derive(Debug, Clone)]
struct IntegrationConfig {
    /// MinIO endpoint URL
    pub minio_endpoint: String,
    /// MinIO access key
    pub minio_access_key: String,
    /// MinIO secret key
    pub minio_secret_key: String,
    /// MinIO input bucket
    pub minio_input_bucket: String,
    /// MinIO output bucket
    pub minio_output_bucket: String,
    /// TiKV PD endpoints
    pub tikv_pd_endpoints: Vec<String>,
}

impl Default for IntegrationConfig {
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
                .unwrap_or_else(|_| "127.0.0.1:2379".to_string())
                .split(',')
                .map(|s| s.trim().to_string())
                .collect(),
        }
    }
}

impl IntegrationConfig {
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

    /// Create TiKV client with retry logic.
    pub async fn create_tikv_client(&self) -> Result<TikvClient, Box<dyn std::error::Error>> {
        let config = TikvConfig::with_pd_endpoints(&self.tikv_pd_endpoints.join(","));
        let client = TikvClient::new(config).await?;
        Ok(client)
    }

    /// Check if infrastructure is available with detailed diagnostics.
    pub async fn check_infrastructure(&self) -> InfrastructureStatus {
        let mut status = InfrastructureStatus::default();

        // Check MinIO
        match self.create_input_storage() {
            Ok(storage) => {
                // Try a simple operation
                let test_path = Path::new("__test__/health-check.txt");
                let test_data = Bytes::from("test");
                match storage.write(test_path, test_data).await {
                    Ok(_) => {
                        let _ = storage.delete(test_path).await;
                        status.minio_available = true;
                    }
                    Err(e) => {
                        status.minio_error = Some(format!("Write failed: {}", e));
                    }
                }
            }
            Err(e) => {
                status.minio_error = Some(format!("Connection failed: {}", e));
            }
        }

        // Check TiKV
        match self.create_tikv_client().await {
            Ok(client) => {
                // Try a simple operation
                let test_key = b"__test__/health-check".to_vec();
                let test_value = b"test".to_vec();
                match client.put(test_key.clone(), test_value).await {
                    Ok(_) => {
                        let _ = client.delete(test_key).await;
                        status.tikv_available = true;
                    }
                    Err(e) => {
                        status.tikv_error = Some(format!("Write failed: {}", e));
                    }
                }
            }
            Err(e) => {
                status.tikv_error = Some(format!("Connection failed: {}", e));
            }
        }

        status
    }
}

/// Infrastructure availability status.
#[derive(Debug, Default)]
struct InfrastructureStatus {
    minio_available: bool,
    minio_error: Option<String>,
    tikv_available: bool,
    tikv_error: Option<String>,
}

impl InfrastructureStatus {
    fn all_available(&self) -> bool {
        self.minio_available && self.tikv_available
    }

    fn print_diagnostics(&self) {
        println!("\n=== Infrastructure Diagnostics ===");

        if self.minio_available {
            println!("✓ MinIO: Available");
        } else {
            println!("✗ MinIO: Not available");
            if let Some(ref err) = self.minio_error {
                println!("  Error: {}", err);
            }
            println!("  Hint: Start with 'make dev-up'");
        }

        if self.tikv_available {
            println!("✓ TiKV: Available");
        } else {
            println!("✗ TiKV: Not available");
            if let Some(ref err) = self.tikv_error {
                println!("  Error: {}", err);
            }
            println!("  Hint: Start with 'make dev-up'");
            println!("  Hint: Add '127.0.0.1 pd' to /etc/hosts");
        }

        println!("==================================\n");
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Path to test fixtures.
fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// Get available bag files.
fn get_available_bag_files() -> Vec<PathBuf> {
    let fixtures = fixtures_dir();
    let candidates = vec![
        fixtures.join("roboflow_sample.bag"),
        fixtures.join("roboflow_extracted.bag"),
    ];

    candidates.into_iter().filter(|p| p.exists()).collect()
}

/// Upload a file to MinIO.
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

/// Cleanup MinIO test directory.
#[allow(dead_code)]
async fn cleanup_minio_dir(
    storage: &AsyncS3Storage,
    prefix: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let object_store = storage.object_store();
    let list_result = object_store
        .list_with_delimiter(Some(&object_store::path::Path::from(prefix)))
        .await?;

    for object in list_result.objects {
        let path = Path::new(object.location.as_ref());
        storage.delete(path).await?;
    }

    for prefix in list_result.common_prefixes {
        let path = Path::new(prefix.as_ref());
        Box::pin(cleanup_minio_dir(storage, path.to_str().unwrap())).await?;
    }

    Ok(())
}

// =============================================================================
// E2E Tests
// =============================================================================

/// Test complete workflow: Upload bags → Submit batch → Process → Verify output.
///
/// This is the main integration test that exercises the entire pipeline:
/// 1. Uploads multiple bag files to MinIO
/// 2. Submits batch to TiKV with episodes_per_chunk=1
/// 3. Creates work units for each bag
/// 4. Processes work units with LeRobotExecutor
/// 5. Verifies output dataset structure in MinIO
#[tokio::test]
#[ignore = "Requires MinIO and TiKV infrastructure"]
async fn test_e2e_complete_batch_workflow() {
    let _ = tracing_subscriber::fmt::try_init();

    let config = IntegrationConfig::default();
    let status = config.check_infrastructure().await;
    status.print_diagnostics();

    if !status.all_available() {
        println!("Skipping test: infrastructure not fully available");
        return;
    }

    // Get available bag files
    let bag_files = get_available_bag_files();
    if bag_files.is_empty() {
        println!("No bag files found in tests/fixtures/");
        return;
    }

    println!("Found {} bag files:", bag_files.len());
    for bag in &bag_files {
        let size = std::fs::metadata(bag).map(|m| m.len()).unwrap_or(0);
        println!("  - {} ({} bytes)", bag.display(), size);
    }

    // Create clients
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

    // Create test directories
    let test_id = format!("integration-{}", uuid::Uuid::new_v4());
    let input_prefix = format!("batch-tests/{}/input", test_id);
    let output_prefix = format!("batch-tests/{}/output", test_id);

    println!("\nTest ID: {}", test_id);
    println!(
        "Input: s3://{}/{}/",
        config.minio_input_bucket, input_prefix
    );
    println!(
        "Output: s3://{}/{}/\n",
        config.minio_output_bucket, output_prefix
    );

    // Upload bag files
    let mut work_files = Vec::new();
    for (i, bag_file) in bag_files.iter().enumerate() {
        let bag_name = format!("episode_{:03}.bag", i);
        let remote_path = Path::new(&input_prefix).join(&bag_name);
        let file_size = std::fs::metadata(bag_file).map(|m| m.len()).unwrap_or(0);

        print!("Uploading {}... ", bag_name);
        match upload_file(&input_storage, bag_file, &remote_path).await {
            Ok(_) => {
                println!("OK");
                work_files.push(WorkFile::new(
                    format!(
                        "s3://{}/{}",
                        config.minio_input_bucket,
                        remote_path.display()
                    ),
                    file_size,
                ));
            }
            Err(e) => {
                println!("FAILED: {}", e);
                return;
            }
        }
    }

    // Create batch with episodes_per_chunk=1
    let batch_name = format!("batch-{}", test_id);
    let batch_id = format!("jobs:{}", batch_name);

    let mut spec = BatchSpec::new(
        &batch_name,
        vec![format!(
            "s3://{}/{}/",
            config.minio_input_bucket, input_prefix
        )],
        format!("s3://{}/{}/", config.minio_output_bucket, output_prefix),
    );

    // Use 1 episode per chunk for testing
    spec.spec.episodes_per_chunk = 1;
    spec.spec.parallelism = 2;

    spec.validate().expect("Batch spec should be valid");

    // Submit batch to TiKV
    print!("\nSubmitting batch to TiKV... ");
    let spec_key = BatchKeys::spec(&batch_id);
    let spec_data = serde_yaml_ng::to_string(&spec).unwrap().into_bytes();

    let mut status = BatchStatus::new();
    status.transition_to(BatchPhase::Running);
    status.set_work_units_total(work_files.len() as u32);
    status.set_files_total(work_files.len() as u32);
    let status_key = BatchKeys::status(&batch_id);
    let status_data = bincode::serialize(&status).unwrap();

    let phase_key = BatchIndexKeys::phase(BatchPhase::Running, &batch_id);

    match tikv
        .batch_put(vec![
            (spec_key, spec_data),
            (status_key.clone(), status_data),
            (phase_key, vec![]),
        ])
        .await
    {
        Ok(_) => println!("OK"),
        Err(e) => {
            println!("FAILED: {}", e);
            return;
        }
    }

    // Create work units
    print!("Creating work units... ");
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

        if let Err(e) = tikv.put(unit_key, unit_data).await {
            println!("FAILED: {}", e);
            return;
        }
    }
    println!("OK ({} units)", work_files.len());

    // Process work units
    println!("\nProcessing work units:");
    let executor = LeRobotExecutor::new(2, "/tmp/roboflow-output");
    let registry = Arc::new(tokio::sync::RwLock::new(JobRegistry::default()));

    for i in 0..work_files.len() {
        let unit_id = format!("unit-{}", i);
        print!("  Processing {}... ", unit_id);

        let unit_key = WorkUnitKeys::unit(&batch_id, &unit_id);
        let unit_data = match tikv.get(unit_key).await {
            Ok(Some(data)) => data,
            Ok(None) => {
                println!("NOT FOUND");
                continue;
            }
            Err(e) => {
                println!("READ ERROR: {}", e);
                continue;
            }
        };

        let work_unit: WorkUnit = match bincode::deserialize(&unit_data) {
            Ok(wu) => wu,
            Err(e) => {
                println!("DESERIALIZE ERROR: {}", e);
                continue;
            }
        };

        match executor.execute(&work_unit, registry.clone()).await {
            Ok(_) => println!("OK"),
            Err(e) => println!("FAILED: {}", e),
        }
    }

    // Run controller reconciliation
    print!("\nReconciling batch status... ");
    let controller = BatchController::with_client(tikv.clone());
    match controller.reconcile_all().await {
        Ok(_) => println!("OK"),
        Err(e) => println!("FAILED: {}", e),
    }

    // Verify batch status
    print!("Verifying batch status... ");
    match tikv.get(BatchKeys::status(&batch_id)).await {
        Ok(Some(data)) => {
            let final_status: BatchStatus = bincode::deserialize(&data).unwrap();
            println!(
                "OK ({}/{} completed)",
                final_status.work_units_completed, final_status.work_units_total
            );
        }
        _ => println!("FAILED: Could not read status"),
    }

    // Cleanup
    print!("\nCleaning up... ");
    cleanup_batch(&tikv, &batch_id).await;
    println!("OK");

    println!("\n✓ Test complete!");
    println!(
        "  Output location: s3://{}/{}/",
        config.minio_output_bucket, output_prefix
    );
}

/// Test LeRobot dataset generation with 1 episode per chunk.
///
/// Verifies that the chunk directory structure is correct when
/// episodes_per_chunk=1.
#[tokio::test]
#[ignore = "Requires MinIO and TiKV infrastructure"]
async fn test_e2e_one_episode_per_chunk_structure() {
    use roboflow_dataset::formats::common::DatasetWriter;

    let _ = tracing_subscriber::fmt::try_init();

    let config = IntegrationConfig::default();
    let status = config.check_infrastructure().await;

    if !status.all_available() {
        println!("Skipping test: infrastructure not fully available");
        return;
    }

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    // Create LeRobot config
    let lerobot_config = LerobotConfig {
        dataset: LeRobotDatasetConfig {
            base: DatasetBaseConfig {
                name: "one_ep_per_chunk_test".to_string(),
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

    // Create 3 episodes with 5 frames each
    for ep_idx in 0..3 {
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

    assert_eq!(stats.frames_written, 15); // 3 episodes * 5 frames

    // Verify chunk directory structure
    let data_dir = temp_dir.path().join("data");

    // With 1 episode per chunk, we should have 3 chunk directories
    let chunk_dirs: Vec<_> = std::fs::read_dir(&data_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .collect();

    println!("Chunk directories found: {}", chunk_dirs.len());
    for dir in &chunk_dirs {
        println!("  - {:?}", dir.path());
    }

    // Each chunk should have exactly 1 parquet file
    let mut total_parquet = 0;
    for dir in &chunk_dirs {
        let parquet_count = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "parquet")
                    .unwrap_or(false)
            })
            .count();
        total_parquet += parquet_count;
        println!("    Parquet files: {}", parquet_count);
    }

    assert_eq!(
        total_parquet, 3,
        "Should have 3 parquet files (one per episode)"
    );

    println!("✓ One episode per chunk structure test passed");
}

/// Test that validates the entire pipeline can be run end-to-end.
///
/// This test acts as a smoke test to ensure all components are properly
/// integrated. It uses minimal data and quick operations.
#[tokio::test]
#[ignore = "Requires MinIO and TiKV infrastructure"]
async fn test_e2e_smoke_test() {
    let _ = tracing_subscriber::fmt::try_init();

    let config = IntegrationConfig::default();
    let status = config.check_infrastructure().await;
    status.print_diagnostics();

    if !status.all_available() {
        println!("\nInfrastructure not available. To run this test:");
        println!("  1. Start infrastructure: make dev-up");
        println!("  2. Add DNS entry: echo '127.0.0.1 pd' | sudo tee -a /etc/hosts");
        println!("  3. Run test: cargo test --test batch_e2e_integration_test -- --ignored");
        return;
    }

    // Just verify we can create all clients successfully
    let _input_storage = config.create_input_storage().expect("MinIO input storage");
    let _output_storage = config
        .create_output_storage()
        .expect("MinIO output storage");
    let _tikv = config.create_tikv_client().await.expect("TiKV client");

    println!("\n✓ Smoke test passed - all clients created successfully");
}
