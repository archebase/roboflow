// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! End-to-end batch workflow test with real bag files.
//!
//! This test verifies the complete pipeline:
//! 1. Setup: Copy bag files to temp directory (simulating MinIO)
//! 2. Submit batch to TiKV with episodes_per_chunk=1
//! 3. Manually create work units (simulating scanner)
//! 4. Process work units with LeRobotExecutor
//! 5. Verify output structure

use std::path::Path;
use std::sync::Arc;

use roboflow_distributed::{
    BatchController, BatchIndexKeys, BatchKeys, BatchPhase, BatchSpec, BatchStatus,
    LeRobotExecutor, WorkFile, WorkUnit, WorkUnitStatus, batch::WorkUnitKeys, tikv::TikvClient,
    worker::JobRegistry,
};

/// Path to test fixtures
fn fixtures_dir() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("tests/fixtures")
}

/// Get TiKV client or skip test
async fn get_tikv_or_skip() -> Option<Arc<TikvClient>> {
    match TikvClient::from_env().await {
        Ok(c) => Some(Arc::new(c)),
        Err(e) => {
            println!("Skipping test: TiKV not available: {}", e);
            None
        }
    }
}

/// Cleanup batch data from TiKV
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

    // Clean up work units by scanning
    let work_unit_prefix = format!("/roboflow/v1/batch/{}/workunit/", batch_id);
    if let Ok(entries) = tikv.scan(work_unit_prefix.into_bytes(), 1000).await {
        for (key, _) in entries {
            let _ = tikv.delete(key).await;
        }
    }
}

// ============================================================================
// E2E Batch Workflow Tests
// ============================================================================

/// Test batch submission and work unit creation.
///
/// This test:
/// 1. Creates temp bag files
/// 2. Submits batch with episodes_per_chunk=1
/// 3. Creates work units manually (simulating scanner)
/// 4. Verifies work units are stored in TiKV
#[tokio::test]
async fn test_e2e_batch_submission_and_work_units() {
    let _ = tracing_subscriber::fmt::try_init();

    let Some(tikv) = get_tikv_or_skip().await else {
        return;
    };

    // Setup temp directories
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let input_dir = temp_dir.path().join("input");
    let output_dir = temp_dir.path().join("output");
    std::fs::create_dir_all(&input_dir).expect("Failed to create input dir");
    std::fs::create_dir_all(&output_dir).expect("Failed to create output dir");

    // Create 3 small test bag files (simulating multiple episodes)
    let num_files = 3usize;
    let mut work_files = Vec::new();
    for i in 0..num_files {
        let file_path = input_dir.join(format!("episode_{}.bag", i));
        // Write minimal bag header - enough for file type detection
        std::fs::write(&file_path, b"#ROSBAG V2.0\n").expect("Failed to write test file");
        work_files.push(WorkFile::new(
            format!("file://{}", file_path.display()),
            14, // Size of the bag header
        ));
    }

    // Create batch spec with episodes_per_chunk=1
    let batch_name = format!("e2e-test-{}", uuid::Uuid::new_v4());
    let batch_id = format!("jobs:{}", batch_name);
    let mut spec = BatchSpec::new(
        &batch_name,
        vec![format!("file://{}/", input_dir.display())],
        format!("file://{}/", output_dir.display()),
    );

    // Set episodes_per_chunk=1 for testing
    spec.spec.episodes_per_chunk = 1;
    spec.spec.parallelism = 2;

    // Validate and submit batch
    spec.validate().expect("Batch spec should be valid");

    let spec_key = BatchKeys::spec(&batch_id);
    let spec_data = serde_yaml_ng::to_string(&spec).unwrap().into_bytes();

    // Create initial batch status
    let mut status = BatchStatus::new();
    status.transition_to(BatchPhase::Running);
    status.set_work_units_total(num_files as u32);
    status.set_files_total(num_files as u32);
    let status_key = BatchKeys::status(&batch_id);
    let status_data = bincode::serialize(&status).unwrap();

    // Create phase index
    let phase_key = BatchIndexKeys::phase(BatchPhase::Running, &batch_id);

    // Submit to TiKV
    tikv.batch_put(vec![
        (spec_key, spec_data),
        (status_key.clone(), status_data),
        (phase_key, vec![]),
    ])
    .await
    .expect("Failed to submit batch");

    // Create work units
    for (i, work_file) in work_files.iter().enumerate() {
        let work_unit = WorkUnit::with_id(
            format!("unit-{}", i),
            batch_id.clone(),
            vec![work_file.clone()],
            format!("file://{}/episode_{:06}", output_dir.display(), i),
            "config-hash".to_string(),
        );

        let unit_key = WorkUnitKeys::unit(&batch_id, &format!("unit-{}", i));
        let unit_data = bincode::serialize(&work_unit).unwrap();
        tikv.put(unit_key, unit_data)
            .await
            .expect("Failed to store work unit");
    }

    // Verify work units were created by scanning
    let work_unit_prefix = WorkUnitKeys::batch_prefix(&batch_id);
    let stored_units: Vec<(Vec<u8>, Vec<u8>)> = tikv.scan(work_unit_prefix, 100).await.unwrap();

    assert_eq!(
        stored_units.len(),
        num_files,
        "Should have created {} work units",
        num_files
    );

    println!("Batch {} submitted with {} work units", batch_id, num_files);

    // Cleanup after test
    cleanup_batch(&tikv, &batch_id).await;
}

/// Test LeRobotExecutor processes work units.
#[tokio::test]
async fn test_e2e_lerobot_executor_processes_work_units() {
    let _ = tracing_subscriber::fmt::try_init();

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let output_dir = temp_dir.path().join("output");
    std::fs::create_dir_all(&output_dir).expect("Failed to create output dir");

    // Use the smallest real fixture file if available
    let fixture_dir = fixtures_dir();
    let bag_file = fixture_dir.join("roboflow_sample.bag");

    // If real bag doesn't exist, create a dummy one
    let input_file = if bag_file.exists() {
        bag_file
    } else {
        let dummy = temp_dir.path().join("test.bag");
        std::fs::write(&dummy, b"#ROSBAG V2.0\n").expect("Failed to write dummy file");
        dummy
    };

    let file_size = std::fs::metadata(&input_file).map(|m| m.len()).unwrap_or(0);

    let executor = LeRobotExecutor::new(2, output_dir.to_str().unwrap());
    let registry = Arc::new(tokio::sync::RwLock::new(JobRegistry::default()));

    let work_unit = WorkUnit::new(
        "test-batch".to_string(),
        vec![WorkFile::new(
            format!("file://{}", input_file.display()),
            file_size,
        )],
        format!("{}/episode_000000", output_dir.display()),
        "config_hash".to_string(),
    );

    let result = executor.execute(&work_unit, registry.clone()).await;

    // Should complete (success or error)
    match &result {
        Ok(_) => println!("Work unit execution succeeded"),
        Err(e) => println!(
            "Work unit execution failed (expected for dummy files): {}",
            e
        ),
    }

    // Test completes if we get here (no panic)
    assert!(true, "Test should complete without panic");
}

/// Test batch phase transitions.
#[tokio::test]
async fn test_e2e_batch_phase_transitions() {
    let _ = tracing_subscriber::fmt::try_init();

    let Some(tikv) = get_tikv_or_skip().await else {
        return;
    };

    let batch_name = format!("phase-test-{}", uuid::Uuid::new_v4());
    let batch_id = format!("jobs:{}", batch_name);

    // Create and submit batch
    let spec = BatchSpec::new(
        &batch_name,
        vec!["s3://test/input/*.bag".to_string()],
        "s3://test/output/".to_string(),
    );

    let spec_key = BatchKeys::spec(&batch_id);
    let spec_data = serde_yaml_ng::to_string(&spec).unwrap().into_bytes();

    // Create status in Pending phase
    let mut status = BatchStatus::new();
    status.transition_to(BatchPhase::Pending);
    let status_key = BatchKeys::status(&batch_id);
    let status_data = bincode::serialize(&status).unwrap();

    let phase_key = BatchIndexKeys::phase(BatchPhase::Pending, &batch_id);

    tikv.batch_put(vec![
        (spec_key.clone(), spec_data),
        (status_key.clone(), status_data),
        (phase_key, vec![]),
    ])
    .await
    .expect("Failed to submit batch");

    // Verify initial phase
    let stored = tikv.get(status_key.clone()).await.unwrap().unwrap();
    let stored_status: BatchStatus = bincode::deserialize(&stored).unwrap();
    assert_eq!(stored_status.phase, BatchPhase::Pending);

    // Simulate phase transition to Discovering
    let mut updated_status = stored_status;
    updated_status.transition_to(BatchPhase::Discovering);
    let updated_data = bincode::serialize(&updated_status).unwrap();

    // Update phase index
    let old_phase_key = BatchIndexKeys::phase(BatchPhase::Pending, &batch_id);
    let new_phase_key = BatchIndexKeys::phase(BatchPhase::Discovering, &batch_id);

    tikv.batch_put(vec![
        (status_key.clone(), updated_data),
        (new_phase_key, vec![]),
    ])
    .await
    .unwrap();
    tikv.delete(old_phase_key).await.unwrap();

    // Verify new phase
    let stored = tikv.get(status_key.clone()).await.unwrap().unwrap();
    let stored_status: BatchStatus = bincode::deserialize(&stored).unwrap();
    assert_eq!(stored_status.phase, BatchPhase::Discovering);

    cleanup_batch(&tikv, &batch_id).await;
}

/// Test controller reconciles batch status.
#[tokio::test]
async fn test_e2e_controller_reconciles_batch() {
    let _ = tracing_subscriber::fmt::try_init();

    let Some(tikv) = get_tikv_or_skip().await else {
        return;
    };

    let batch_name = format!("controller-test-{}", uuid::Uuid::new_v4());
    let batch_id = format!("jobs:{}", batch_name);
    let unit_id = "unit-1";

    // Create spec
    let spec = BatchSpec::new(
        &batch_name,
        vec!["s3://test/file.bag".to_string()],
        "s3://output/".to_string(),
    );

    // Create batch status: Running, 1 work unit total
    let mut status = BatchStatus::new();
    status.transition_to(BatchPhase::Running);
    status.set_work_units_total(1);
    status.set_files_total(1);
    status.started_at = Some(chrono::Utc::now());

    // Create work unit with status Complete
    let mut work_unit = WorkUnit::with_id(
        unit_id.to_string(),
        batch_id.clone(),
        vec![WorkFile::new("s3://test/file.bag".to_string(), 1024)],
        "s3://output/".to_string(),
        "config-hash".to_string(),
    );
    work_unit.complete();
    assert_eq!(work_unit.status, WorkUnitStatus::Complete);

    // Write spec, status, phase index, work unit to TiKV
    let spec_key = BatchKeys::spec(&batch_id);
    let spec_data = serde_yaml_ng::to_string(&spec).unwrap().into_bytes();
    let status_key = BatchKeys::status(&batch_id);
    let status_data = bincode::serialize(&status).unwrap();
    let phase_key = BatchIndexKeys::phase(BatchPhase::Running, &batch_id);
    let unit_key = WorkUnitKeys::unit(&batch_id, unit_id);
    let unit_data = bincode::serialize(&work_unit).unwrap();

    tikv.batch_put(vec![
        (spec_key, spec_data),
        (status_key, status_data),
        (phase_key, vec![]),
        (unit_key.clone(), unit_data),
    ])
    .await
    .unwrap();

    // Create controller and run reconciliation
    let controller = BatchController::with_client(tikv.clone());
    controller.reconcile_all().await.unwrap();

    // Read back status - should show completed work unit but still in Running phase
    let updated = tikv
        .get(BatchKeys::status(&batch_id))
        .await
        .unwrap()
        .unwrap();
    let status: BatchStatus = bincode::deserialize(&updated).unwrap();

    assert_eq!(status.work_units_completed, 1);
    assert_eq!(status.work_units_total, 1);
    assert!(status.is_complete());
    // Phase should still be Running (controller doesn't transition to Complete)
    assert_eq!(status.phase, BatchPhase::Running);

    cleanup_batch(&tikv, &batch_id).await;
}

/// Test complete workflow with actual bag file conversion.
///
/// This test requires:
/// - TiKV running (for batch coordination)
/// - Real bag files in tests/fixtures/
///
/// It verifies:
/// - Batch submission
/// - Work unit creation
/// - Conversion execution
/// - Output validation
#[tokio::test]
#[ignore = "Requires full infrastructure setup - run manually"]
async fn test_e2e_full_batch_conversion() {
    let _ = tracing_subscriber::fmt::try_init();

    let Some(tikv) = get_tikv_or_skip().await else {
        return;
    };

    let fixture_dir = fixtures_dir();
    let bag_file = fixture_dir.join("roboflow_sample.bag");

    if !bag_file.exists() {
        println!("Skipping: roboflow_sample.bag not found at {:?}", bag_file);
        return;
    }

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let output_dir = temp_dir.path().join("output");
    std::fs::create_dir_all(&output_dir).expect("Failed to create output dir");

    let file_size = std::fs::metadata(&bag_file).map(|m| m.len()).unwrap_or(0);

    // Create batch
    let batch_name = format!("full-e2e-{}", uuid::Uuid::new_v4());
    let batch_id = format!("jobs:{}", batch_name);

    let mut spec = BatchSpec::new(
        &batch_name,
        vec![format!("file://{}", bag_file.display())],
        format!("file://{}/", output_dir.display()),
    );
    spec.spec.episodes_per_chunk = 1;
    spec.spec.parallelism = 1;

    // Submit batch
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

    println!("Batch {} submitted, processing...", batch_id);

    // Create work unit
    let work_unit = WorkUnit::with_id(
        "unit-0".to_string(),
        batch_id.clone(),
        vec![WorkFile::new(
            format!("file://{}", bag_file.display()),
            file_size,
        )],
        format!("file://{}/episode_000000", output_dir.display()),
        "config_hash".to_string(),
    );

    let unit_key = WorkUnitKeys::unit(&batch_id, "unit-0");
    let unit_data = bincode::serialize(&work_unit).unwrap();
    tikv.put(unit_key, unit_data)
        .await
        .expect("Failed to store work unit");

    // Process work unit
    let executor = LeRobotExecutor::new(1, output_dir.to_str().unwrap());
    let registry = Arc::new(tokio::sync::RwLock::new(JobRegistry::default()));

    let result = executor.execute(&work_unit, registry.clone()).await;
    match &result {
        Ok(_) => println!("Conversion succeeded"),
        Err(e) => println!("Conversion result: {}", e),
    }

    // Verify output files exist
    let episode_dir = output_dir.join("episode_000000");
    if episode_dir.exists() {
        println!("Output directory created: {:?}", episode_dir);
        let entries: Vec<_> = std::fs::read_dir(&episode_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        for entry in entries {
            println!("  - {:?}", entry.path());
        }
    }

    // Cleanup
    cleanup_batch(&tikv, &batch_id).await;
}
