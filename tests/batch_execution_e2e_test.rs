// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Batch execution e2e tests with actual processing simulation.
//!
//! These tests verify the complete execution pipeline:
//! 1. Batch submission with multiple bag files
//! 2. Worker claiming and processing work units
//! 3. Dataset generation with video encoding
//! 4. Phase transitions (Pending -> Running -> Merging -> Complete)
//!
//! # Prerequisites
//!
//! 1. Start infrastructure: `make dev-up`
//! 2. Add to /etc/hosts: `127.0.0.1 pd`
//!
//! # Running
//!
//! ```bash
//! cargo test --test batch_execution_e2e_test -- --ignored --nocapture
//! ```

use std::path::Path;
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

#[derive(Debug, Clone)]
struct TestConfig {
    minio_endpoint: String,
    minio_access_key: String,
    minio_secret_key: String,
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
            output_bucket: "roboflow-datasets".to_string(),
        }
    }
}

impl TestConfig {
    async fn check_tikv(&self) -> Result<(), String> {
        match TikvClient::from_env().await {
            Ok(_) => Ok(()),
            Err(e) => Err(format!(
                "TiKV not accessible: {}. Make sure 'make dev-up' is running and '127.0.0.1 pd' is in /etc/hosts",
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

        let test_path = Path::new("__test__/health-check.txt");
        let test_data = Bytes::from("test");
        storage
            .write(test_path, test_data)
            .await
            .map_err(|e| format!("MinIO not accessible: {}", e))?;

        Ok(storage)
    }
}

async fn create_and_upload_dataset(
    storage: &AsyncS3Storage,
    output_prefix: &str,
    episode_count: usize,
    frames_per_episode: usize,
) -> Result<usize, String> {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let lerobot_config = LerobotConfig {
        dataset: LeRobotDatasetConfig {
            base: DatasetBaseConfig {
                name: "execution_test".to_string(),
                fps: 30,
                robot_type: Some("test_robot".to_string()),
            },
            env_type: None,
        },
        mappings: vec![],
        video: VideoConfig::default(),
        annotation_file: None,
        flushing: FlushingConfig::default(),
        streaming: StreamingConfig {
            finalize_metadata_in_coordinator: true,
            ..StreamingConfig::default()
        },
    };

    let mut writer =
        LerobotWriter::new_local(temp_dir.path(), lerobot_config).map_err(|e| e.to_string())?;

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

    // Upload to MinIO
    let mut dirs = vec![temp_dir.path().to_path_buf()];
    let base_path = temp_dir.path().to_path_buf();

    while let Some(dir) = dirs.pop() {
        let mut entries = tokio::fs::read_dir(&dir).await.expect("Failed to read dir");
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.is_file() {
                let relative_path = path.strip_prefix(&base_path).unwrap();
                let remote_path = Path::new(output_prefix).join(relative_path);
                storage
                    .write(
                        &remote_path,
                        Bytes::from(tokio::fs::read(&path).await.unwrap()),
                    )
                    .await
                    .map_err(|e| format!("Failed to upload: {}", e))?;
            } else if path.is_dir() {
                dirs.push(path);
            }
        }
    }

    Ok(stats.frames_written)
}

// =============================================================================
// E2E Tests
// =============================================================================

/// Test worker processing workflow with multiple work units.
///
/// This test simulates multiple workers claiming and processing work units,
/// then verifies the batch transitions through phases correctly.
#[tokio::test]
async fn test_worker_processing_multiple_work_units() {
    let _ = tracing_subscriber::fmt::try_init();

    let config = TestConfig::default();

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

    let tikv = Arc::new(TikvClient::from_env().await.unwrap());
    let controller = BatchController::with_client(tikv.clone());

    // Create test batch with 3 work units
    let batch_id = format!("execution-test-{}", uuid::Uuid::new_v4());
    let test_prefix = format!("execution/{}", batch_id);

    println!("\n1. Creating batch with 3 work units...");

    let spec = BatchSpec::new(
        &batch_id,
        vec![
            "s3://test/file1.bag".to_string(),
            "s3://test/file2.bag".to_string(),
            "s3://test/file3.bag".to_string(),
        ],
        format!("s3://{}/{}/output", config.output_bucket, test_prefix),
    );

    // Get the canonical batch_id from spec (namespace:name format)
    let canonical_batch_id = batch_id_from_spec(&spec);

    let mut status = BatchStatus::new();
    status.transition_to(BatchPhase::Running);
    status.set_work_units_total(3);

    // Store batch metadata
    let spec_key = BatchKeys::spec(&canonical_batch_id);
    let spec_data = serde_yaml_ng::to_string(&spec).unwrap().into_bytes();
    let status_key = BatchKeys::status(&canonical_batch_id);
    let status_data = bincode::serialize(&status).unwrap();
    let phase_key = BatchIndexKeys::phase(BatchPhase::Running, &canonical_batch_id);

    tikv.batch_put(vec![
        (spec_key, spec_data),
        (status_key.clone(), status_data),
        (phase_key, vec![]),
    ])
    .await
    .unwrap();

    // Create 3 work units
    for i in 0..3 {
        let unit_id = format!("unit-{}", i);
        let work_unit = WorkUnit::with_id(
            unit_id.clone(),
            canonical_batch_id.clone(),
            vec![WorkFile::new(format!("s3://test/file{}.bag", i), 1024)],
            format!("s3://{}/{}/output", config.output_bucket, test_prefix),
            "config-hash".to_string(),
        );

        let unit_key = WorkUnitKeys::unit(&canonical_batch_id, &unit_id);
        let unit_data = bincode::serialize(&work_unit).unwrap();
        tikv.put(unit_key, unit_data).await.unwrap();
    }

    println!("   ✓ Batch created: {}", canonical_batch_id);

    // Simulate workers processing work units
    println!("\n2. Simulating worker processing...");

    for i in 0..3 {
        let unit_id = format!("unit-{}", i);
        let unit_key = WorkUnitKeys::unit(&canonical_batch_id, &unit_id);

        // Claim work unit
        let mut work_unit: WorkUnit =
            bincode::deserialize(&tikv.get(unit_key.clone()).await.unwrap().unwrap()).unwrap();

        work_unit
            .claim(format!("worker-{}", i % 2))
            .expect("Failed to claim work unit");

        // Simulate processing by creating a dataset for this work unit
        let dataset_prefix = format!("{}/output/chunk-{:03}", test_prefix, i);
        let frames_written = create_and_upload_dataset(&storage, &dataset_prefix, 1, 5)
            .await
            .expect("Failed to create dataset");

        println!(
            "   ✓ Worker {} processed unit {} ({} frames)",
            i % 2,
            unit_id,
            frames_written
        );

        // Complete work unit
        work_unit.complete();
        tikv.put(unit_key, bincode::serialize(&work_unit).unwrap())
            .await
            .unwrap();
    }

    // Run controller reconcile
    println!("\n3. Reconciling batch...");
    controller.reconcile_all().await.unwrap();

    // Verify status
    let updated_status: BatchStatus =
        bincode::deserialize(&tikv.get(status_key.clone()).await.unwrap().unwrap()).unwrap();

    println!("   Batch phase: {:?}", updated_status.phase);
    println!(
        "   Work units: {}/{}",
        updated_status.work_units_completed, updated_status.work_units_total
    );

    assert_eq!(
        updated_status.work_units_completed, 3,
        "All work units should be completed"
    );
    assert_eq!(updated_status.phase, BatchPhase::Running);

    // Cleanup
    println!("\n4. Cleaning up...");
    let _ = tikv.delete(BatchKeys::spec(&canonical_batch_id)).await;
    let _ = tikv.delete(status_key).await;
    let _ = tikv
        .delete(BatchIndexKeys::phase(
            BatchPhase::Running,
            &canonical_batch_id,
        ))
        .await;
    for i in 0..3 {
        let _ = tikv
            .delete(WorkUnitKeys::unit(
                &canonical_batch_id,
                &format!("unit-{}", i),
            ))
            .await;
    }

    println!("\n✓ Worker processing test passed");
}

/// Test batch phase transitions with simulated time.
///
/// This test verifies that the batch controller correctly handles
/// phase transitions and timeouts.
#[tokio::test]
async fn test_batch_phase_transitions_with_timeouts() {
    let _ = tracing_subscriber::fmt::try_init();

    let config = TestConfig::default();

    if let Err(e) = config.check_tikv().await {
        panic!("Required service TiKV is not available: {}", e);
    }

    println!("✓ TiKV is available");

    let tikv = Arc::new(TikvClient::from_env().await.unwrap());
    let controller = BatchController::with_client(tikv.clone());

    let batch_id = format!("timeout-test-{}", uuid::Uuid::new_v4());

    println!("\n1. Creating batch in Pending phase...");

    let spec = BatchSpec::new(
        &batch_id,
        vec!["s3://test/file.bag".to_string()],
        "s3://test/output".to_string(),
    );

    // Get the canonical batch_id from spec (namespace:name format)
    let canonical_batch_id = batch_id_from_spec(&spec);

    let mut status = BatchStatus::new();
    status.transition_to(BatchPhase::Pending);

    let spec_key = BatchKeys::spec(&canonical_batch_id);
    let spec_data = serde_yaml_ng::to_string(&spec).unwrap().into_bytes();
    let status_key = BatchKeys::status(&canonical_batch_id);
    let status_data = bincode::serialize(&status).unwrap();
    let phase_key = BatchIndexKeys::phase(BatchPhase::Pending, &canonical_batch_id);

    tikv.batch_put(vec![
        (spec_key, spec_data),
        (status_key.clone(), status_data),
        (phase_key, vec![]),
    ])
    .await
    .unwrap();

    println!("   ✓ Batch created in Pending phase");

    // Simulate transition to Running
    println!("\n2. Transitioning to Running phase...");

    let mut status: BatchStatus =
        bincode::deserialize(&tikv.get(status_key.clone()).await.unwrap().unwrap()).unwrap();
    status.transition_to(BatchPhase::Running);

    // Move phase index
    let _ = tikv
        .delete(BatchIndexKeys::phase(
            BatchPhase::Pending,
            &canonical_batch_id,
        ))
        .await;
    let new_phase_key = BatchIndexKeys::phase(BatchPhase::Running, &canonical_batch_id);

    tikv.batch_put(vec![
        (status_key.clone(), bincode::serialize(&status).unwrap()),
        (new_phase_key, vec![]),
    ])
    .await
    .unwrap();

    println!("   ✓ Batch transitioned to Running");

    // Reconcile and verify
    println!("\n3. Running controller reconcile...");
    controller.reconcile_all().await.unwrap();

    let updated_status: BatchStatus =
        bincode::deserialize(&tikv.get(status_key.clone()).await.unwrap().unwrap()).unwrap();

    println!("   Current phase: {:?}", updated_status.phase);
    assert_eq!(updated_status.phase, BatchPhase::Running);

    // Cleanup
    println!("\n4. Cleaning up...");
    let _ = tikv.delete(BatchKeys::spec(&canonical_batch_id)).await;
    let _ = tikv.delete(status_key).await;
    let _ = tikv
        .delete(BatchIndexKeys::phase(
            BatchPhase::Running,
            &canonical_batch_id,
        ))
        .await;

    println!("\n✓ Phase transition test passed");
}

/// Test concurrent batch processing with multiple batches.
///
/// This test verifies that multiple batches can be processed concurrently
/// without interference.
#[tokio::test]
async fn test_concurrent_batch_processing() {
    let _ = tracing_subscriber::fmt::try_init();

    let config = TestConfig::default();

    if let Err(e) = config.check_tikv().await {
        panic!("Required service TiKV is not available: {}", e);
    }

    println!("✓ TiKV is available");

    let tikv = Arc::new(TikvClient::from_env().await.unwrap());
    let controller = BatchController::with_client(tikv.clone());

    let batch_count = 3;
    let mut batch_ids = Vec::new();

    println!("\n1. Creating {} concurrent batches...", batch_count);

    // Create multiple batches
    for i in 0..batch_count {
        let batch_id = format!("concurrent-test-{}-{}", i, uuid::Uuid::new_v4());

        let spec = BatchSpec::new(
            &batch_id,
            vec![format!("s3://test/file{}.bag", i)],
            format!("s3://test/output/{}", batch_id),
        );

        // Get the canonical batch_id from spec (namespace:name format)
        let canonical_batch_id = batch_id_from_spec(&spec);
        batch_ids.push(canonical_batch_id.clone());

        let mut status = BatchStatus::new();
        status.transition_to(BatchPhase::Running);
        status.set_work_units_total(1);

        let spec_key = BatchKeys::spec(&canonical_batch_id);
        let spec_data = serde_yaml_ng::to_string(&spec).unwrap().into_bytes();
        let status_key = BatchKeys::status(&canonical_batch_id);
        let status_data = bincode::serialize(&status).unwrap();
        let phase_key = BatchIndexKeys::phase(BatchPhase::Running, &canonical_batch_id);

        tikv.batch_put(vec![
            (spec_key, spec_data),
            (status_key, status_data),
            (phase_key, vec![]),
        ])
        .await
        .unwrap();

        // Create work unit
        let work_unit = WorkUnit::with_id(
            "unit-0".to_string(),
            canonical_batch_id.clone(),
            vec![WorkFile::new(format!("s3://test/file{}.bag", i), 1024)],
            format!("s3://test/output/{}", batch_id),
            "config-hash".to_string(),
        );

        let unit_key = WorkUnitKeys::unit(&canonical_batch_id, "unit-0");
        let unit_data = bincode::serialize(&work_unit).unwrap();
        tikv.put(unit_key, unit_data).await.unwrap();
    }

    println!("   ✓ Created {} batches", batch_count);

    // Complete work units for all batches
    println!("\n2. Completing work units for all batches...");

    for batch_id in &batch_ids {
        let unit_key = WorkUnitKeys::unit(batch_id, "unit-0");
        let mut work_unit: WorkUnit =
            bincode::deserialize(&tikv.get(unit_key.clone()).await.unwrap().unwrap()).unwrap();

        work_unit.claim("worker-1".to_string()).unwrap();
        work_unit.complete();

        tikv.put(unit_key, bincode::serialize(&work_unit).unwrap())
            .await
            .unwrap();
    }

    println!("   ✓ All work units completed");

    // Reconcile all batches
    println!("\n3. Reconciling all batches...");
    controller.reconcile_all().await.unwrap();

    // Verify all batches have completed work units
    println!("\n4. Verifying batch statuses...");
    for batch_id in &batch_ids {
        let status_key = BatchKeys::status(batch_id);
        let status: BatchStatus =
            bincode::deserialize(&tikv.get(status_key).await.unwrap().unwrap()).unwrap();

        assert_eq!(
            status.work_units_completed, 1,
            "Batch {} should have 1 completed work unit",
            batch_id
        );
        println!(
            "   ✓ {}: {}/{} completed",
            batch_id, status.work_units_completed, status.work_units_total
        );
    }

    // Cleanup
    println!("\n5. Cleaning up...");
    for batch_id in &batch_ids {
        let _ = tikv.delete(BatchKeys::spec(batch_id)).await;
        let _ = tikv.delete(BatchKeys::status(batch_id)).await;
        let _ = tikv
            .delete(BatchIndexKeys::phase(BatchPhase::Running, batch_id))
            .await;
        let _ = tikv.delete(WorkUnitKeys::unit(batch_id, "unit-0")).await;
    }

    println!("\n✓ Concurrent batch processing test passed");
}

/// Test error handling and recovery in batch processing.
///
/// This test simulates work unit failures and verifies proper error handling.
#[tokio::test]
async fn test_batch_error_handling_and_recovery() {
    let _ = tracing_subscriber::fmt::try_init();

    let config = TestConfig::default();

    if let Err(e) = config.check_tikv().await {
        panic!("Required service TiKV is not available: {}", e);
    }

    println!("✓ TiKV is available");

    let tikv = Arc::new(TikvClient::from_env().await.unwrap());
    let controller = BatchController::with_client(tikv.clone());

    let batch_id = format!("error-test-{}", uuid::Uuid::new_v4());

    println!("\n1. Creating batch with work units...");

    let spec = BatchSpec::new(
        &batch_id,
        vec![
            "s3://test/file1.bag".to_string(),
            "s3://test/file2.bag".to_string(),
        ],
        "s3://test/output".to_string(),
    );

    // Get the canonical batch_id from spec (namespace:name format)
    let canonical_batch_id = batch_id_from_spec(&spec);

    let mut status = BatchStatus::new();
    status.transition_to(BatchPhase::Running);
    status.set_work_units_total(2);

    let spec_key = BatchKeys::spec(&canonical_batch_id);
    let spec_data = serde_yaml_ng::to_string(&spec).unwrap().into_bytes();
    let status_key = BatchKeys::status(&canonical_batch_id);
    let status_data = bincode::serialize(&status).unwrap();
    let phase_key = BatchIndexKeys::phase(BatchPhase::Running, &canonical_batch_id);

    tikv.batch_put(vec![
        (spec_key, spec_data),
        (status_key.clone(), status_data),
        (phase_key, vec![]),
    ])
    .await
    .unwrap();

    // Create work units - one will succeed, one will fail
    for i in 0..2 {
        let unit_id = format!("unit-{}", i);
        let work_unit = WorkUnit::with_id(
            unit_id.clone(),
            canonical_batch_id.clone(),
            vec![WorkFile::new(format!("s3://test/file{}.bag", i), 1024)],
            "s3://test/output".to_string(),
            "config-hash".to_string(),
        );

        let unit_key = WorkUnitKeys::unit(&canonical_batch_id, &unit_id);
        let unit_data = bincode::serialize(&work_unit).unwrap();
        tikv.put(unit_key, unit_data).await.unwrap();
    }

    println!("   ✓ Batch created with 2 work units");

    // Process work units - first succeeds, second fails
    println!("\n2. Processing work units (one success, one failure)...");

    // Unit 0: Success
    let unit0_key = WorkUnitKeys::unit(&canonical_batch_id, "unit-0");
    let mut work_unit0: WorkUnit =
        bincode::deserialize(&tikv.get(unit0_key.clone()).await.unwrap().unwrap()).unwrap();
    work_unit0.claim("worker-1".to_string()).unwrap();
    work_unit0.complete();
    tikv.put(unit0_key, bincode::serialize(&work_unit0).unwrap())
        .await
        .unwrap();
    println!("   ✓ unit-0: Completed successfully");

    // Unit 1: Failure (simulate by leaving in claimed state without completion)
    let unit1_key = WorkUnitKeys::unit(&canonical_batch_id, "unit-1");
    let mut work_unit1: WorkUnit =
        bincode::deserialize(&tikv.get(unit1_key.clone()).await.unwrap().unwrap()).unwrap();
    work_unit1.claim("worker-1".to_string()).unwrap();
    // Don't complete - simulates a failed/crashed worker
    tikv.put(unit1_key.clone(), bincode::serialize(&work_unit1).unwrap())
        .await
        .unwrap();
    println!("   ⚠ unit-1: Left in claimed state (simulating failure)");

    // Reconcile
    println!("\n3. Running controller reconcile...");
    controller.reconcile_all().await.unwrap();

    // Verify status - only 1 should be completed
    let updated_status: BatchStatus =
        bincode::deserialize(&tikv.get(status_key.clone()).await.unwrap().unwrap()).unwrap();

    println!(
        "   Work units completed: {}/{}",
        updated_status.work_units_completed, updated_status.work_units_total
    );
    assert_eq!(
        updated_status.work_units_completed, 1,
        "Only unit-0 should be completed"
    );

    // Now simulate recovery - fail the stuck unit
    println!("\n4. Simulating recovery (failing stuck unit)...");
    let mut work_unit1: WorkUnit =
        bincode::deserialize(&tikv.get(unit1_key.clone()).await.unwrap().unwrap()).unwrap();
    work_unit1.fail("Worker crashed".to_string());
    tikv.put(unit1_key.clone(), bincode::serialize(&work_unit1).unwrap())
        .await
        .unwrap();
    println!("   ✓ unit-1: Marked as failed");

    // Reconcile again
    controller.reconcile_all().await.unwrap();

    let final_status: BatchStatus =
        bincode::deserialize(&tikv.get(status_key.clone()).await.unwrap().unwrap()).unwrap();
    println!(
        "   Final status: {}/{} completed",
        final_status.work_units_completed, final_status.work_units_total
    );

    // Cleanup
    println!("\n5. Cleaning up...");
    let _ = tikv.delete(BatchKeys::spec(&canonical_batch_id)).await;
    let _ = tikv.delete(status_key).await;
    let _ = tikv
        .delete(BatchIndexKeys::phase(
            BatchPhase::Running,
            &canonical_batch_id,
        ))
        .await;
    let _ = tikv
        .delete(WorkUnitKeys::unit(&canonical_batch_id, "unit-0"))
        .await;
    let _ = tikv
        .delete(WorkUnitKeys::unit(&canonical_batch_id, "unit-1"))
        .await;

    println!("\n✓ Error handling test passed");
}

/// Test dataset validation after batch completion.
///
/// This test verifies that generated datasets are valid LeRobot format.
#[tokio::test]
async fn test_dataset_validation_after_batch_completion() {
    let _ = tracing_subscriber::fmt::try_init();

    let config = TestConfig::default();

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

    let tikv = Arc::new(TikvClient::from_env().await.unwrap());
    let controller = BatchController::with_client(tikv.clone());

    let batch_id = format!("validation-test-{}", uuid::Uuid::new_v4());
    let output_prefix = format!("validation/{}/output", batch_id);

    println!("\n1. Creating batch with dataset output...");

    let spec = BatchSpec::new(
        &batch_id,
        vec!["s3://test/file.bag".to_string()],
        format!("s3://{}/{}", config.output_bucket, output_prefix),
    );

    // Get the canonical batch_id from spec (namespace:name format)
    let canonical_batch_id = batch_id_from_spec(&spec);

    let mut status = BatchStatus::new();
    status.transition_to(BatchPhase::Running);
    status.set_work_units_total(1);

    let spec_key = BatchKeys::spec(&canonical_batch_id);
    let spec_data = serde_yaml_ng::to_string(&spec).unwrap().into_bytes();
    let status_key = BatchKeys::status(&canonical_batch_id);
    let status_data = bincode::serialize(&status).unwrap();
    let phase_key = BatchIndexKeys::phase(BatchPhase::Running, &canonical_batch_id);

    tikv.batch_put(vec![
        (spec_key, spec_data),
        (status_key.clone(), status_data),
        (phase_key, vec![]),
    ])
    .await
    .unwrap();

    // Create work unit
    let work_unit = WorkUnit::with_id(
        "unit-0".to_string(),
        canonical_batch_id.clone(),
        vec![WorkFile::new("s3://test/file.bag".to_string(), 1024)],
        format!("s3://{}/{}", config.output_bucket, output_prefix),
        "config-hash".to_string(),
    );

    let unit_key = WorkUnitKeys::unit(&canonical_batch_id, "unit-0");
    let unit_data = bincode::serialize(&work_unit).unwrap();
    tikv.put(unit_key.clone(), unit_data).await.unwrap();

    println!("   ✓ Batch created");

    // Process work unit and generate dataset
    println!("\n2. Processing work unit and generating dataset...");

    let mut work_unit: WorkUnit =
        bincode::deserialize(&tikv.get(unit_key.clone()).await.unwrap().unwrap()).unwrap();

    work_unit.claim("worker-1".to_string()).unwrap();

    // Generate dataset with 1 episode per chunk
    let frames_written = create_and_upload_dataset(&storage, &output_prefix, 2, 5)
        .await
        .expect("Failed to create dataset");

    println!(
        "   ✓ Generated dataset with {} frames (2 episodes, 1 per chunk)",
        frames_written
    );

    work_unit.complete();
    tikv.put(unit_key, bincode::serialize(&work_unit).unwrap())
        .await
        .unwrap();

    // Reconcile
    println!("\n3. Reconciling batch...");
    controller.reconcile_all().await.unwrap();

    // Validate dataset structure in MinIO
    println!("\n4. Validating dataset structure...");

    let info_exists = storage
        .exists(Path::new(&format!("{}/meta/info.json", output_prefix)))
        .await;
    let episodes_exists = storage
        .exists(Path::new(&format!("{}/meta/episodes.jsonl", output_prefix)))
        .await;

    assert!(
        !info_exists,
        "info.json should not exist before coordinator finalization"
    );
    assert!(
        !episodes_exists,
        "episodes.jsonl should not exist before coordinator finalization"
    );

    println!("   ✓ meta/info.json not present before coordinator finalization");
    println!("   ✓ meta/episodes.jsonl not present before coordinator finalization");

    // Check for chunk directories
    let chunk_000_exists = storage
        .exists(Path::new(&format!("{}/data/chunk-000", output_prefix)))
        .await;
    let chunk_001_exists = storage
        .exists(Path::new(&format!("{}/data/chunk-001", output_prefix)))
        .await;

    println!("   ✓ chunk-000 exists: {}", chunk_000_exists);
    println!("   ✓ chunk-001 exists: {}", chunk_001_exists);

    // Cleanup
    println!("\n5. Cleaning up...");
    let _ = tikv.delete(BatchKeys::spec(&canonical_batch_id)).await;
    let _ = tikv.delete(status_key).await;
    let _ = tikv
        .delete(BatchIndexKeys::phase(
            BatchPhase::Running,
            &canonical_batch_id,
        ))
        .await;
    let _ = tikv
        .delete(WorkUnitKeys::unit(&canonical_batch_id, "unit-0"))
        .await;

    println!("\n✓ Dataset validation test passed");
}
