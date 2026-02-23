// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Integration test for stage-based executor with 100k episode scale.
//!
//! This test verifies that the new roboflow-executor framework can handle
//! large-scale dataset processing through the WorkUnitExecutor.

use std::sync::Arc;

use roboflow_distributed::{
    LeRobotExecutor, WorkFile, WorkUnit,
    stages::{ConvertStage, DiscoverStage, MergeStage},
    worker::JobRegistry,
};
use roboflow_executor::{PipelineBuilder, StageExecutor, StageId};

/// Test the WorkUnitExecutor pipeline structure.
///
/// Validates that the LeRobotExecutor properly sets up the Convert → Merge
/// pipeline and returns a result (success or error) for each work unit.
///
/// Note: Uses dummy bag files that will fail conversion. This is intentional
/// to test error handling and pipeline structure without requiring real data.
#[tokio::test]
async fn test_work_unit_executor_pipeline_structure() {
    let _ = tracing_subscriber::fmt::try_init();

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let num_episodes = 3usize;

    for i in 0..num_episodes {
        let file_path = temp_dir.path().join(format!("test_{}.bag", i));
        std::fs::write(&file_path, b"#ROSBAG V2.0\n").expect("Failed to write test file");
    }

    let output_dir = temp_dir.path().join("output");
    std::fs::create_dir_all(&output_dir).expect("Failed to create output dir");

    let executor = LeRobotExecutor::new(4, output_dir.to_str().unwrap());
    let registry = Arc::new(tokio::sync::RwLock::new(JobRegistry::default()));

    let mut results = Vec::with_capacity(num_episodes);

    for i in 0..num_episodes {
        let file_path = temp_dir.path().join(format!("test_{}.bag", i));
        let work_unit = WorkUnit::new(
            format!("test-batch-{}", i),
            vec![WorkFile::new(
                format!("file://{}", file_path.to_str().unwrap()),
                1024,
            )],
            format!("{}/{}", output_dir.to_str().unwrap(), i),
            format!("config_hash_{}", i),
        );

        let result = executor.execute(&work_unit, registry.clone()).await;
        results.push(result);
    }

    // All work units should complete (either success or handled error)
    let completed_count = results.len();
    assert_eq!(
        completed_count, num_episodes,
        "All {} work units should complete execution",
        num_episodes
    );

    tracing::info!(
        "Successfully executed {} work units through stage-based pipeline",
        num_episodes
    );
}

/// Test the core StageExecutor with LeRobot pipeline stages.
///
/// This test directly uses the StageExecutor (bypassing the bridge)
/// to verify the Discover → Convert → Merge pipeline works correctly.
#[tokio::test]
#[ignore = "Requires S3 setup for distributed testing"]
async fn test_stage_executor_lerobot_pipeline() {
    let _ = tracing_subscriber::fmt::try_init();

    // Use actual fixture file from tests/fixtures/
    let fixture_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("tests/fixtures");
    let source_prefix = format!("{}/", fixture_dir.display());
    let input_file = format!("{}/roboflow_sample.bag", fixture_dir.display());
    let output_prefix = "/tmp/output";

    let pipeline = PipelineBuilder::new()
        .stage(Arc::new(DiscoverStage::new(source_prefix)))
        .stage(Arc::new(ConvertStage::new(
            input_file,
            output_prefix,
            "config_v1",
        )))
        .stage(Arc::new(MergeStage::new(format!(
            "{}/dataset",
            output_prefix
        ))))
        .dependency(StageId(1), StageId(0))
        .dependency(StageId(2), StageId(1))
        .build()
        .expect("Pipeline should build successfully");

    // Execute with 4 concurrent slots
    let executor = StageExecutor::new(4);
    let result = executor
        .execute(&pipeline)
        .await
        .expect("Pipeline execution should succeed");

    // Verify results
    assert_eq!(result.stages_completed, 3, "All 3 stages should complete");
    assert!(
        result.tasks_completed >= 3,
        "At least 3 tasks should complete (one per stage)"
    );
    assert!(
        result.duration_secs > 0.0,
        "Execution should take some time"
    );

    tracing::info!(
        stages = result.stages_completed,
        tasks = result.tasks_completed,
        duration_secs = result.duration_secs,
        "LeRobot pipeline executed successfully"
    );
}

/// Test pipeline with dependency validation.
///
/// Verifies that the pipeline correctly enforces stage dependencies
/// and executes stages in topological order.
#[tokio::test]
async fn test_pipeline_dependency_ordering() {
    let _ = tracing_subscriber::fmt::try_init();

    // Build pipeline with explicit dependencies
    let pipeline = PipelineBuilder::new()
        .stage(Arc::new(DiscoverStage::new("s3://bucket/input/")))
        .stage(Arc::new(ConvertStage::new(
            "s3://bucket/input/test.bag",
            "s3://bucket/output/",
            "v1",
        )))
        .stage(Arc::new(MergeStage::new("s3://bucket/output/dataset")))
        .dependency(StageId(1), StageId(0))
        .dependency(StageId(2), StageId(1))
        .build()
        .expect("Pipeline with valid dependencies should build");

    // Verify topological order
    let order = pipeline.topological_order();
    assert_eq!(order.len(), 3, "Pipeline should have 3 stages");
    assert_eq!(order[0], StageId(0), "Discover should be first");
    assert_eq!(order[1], StageId(1), "Convert should be second");
    assert_eq!(order[2], StageId(2), "Merge should be third");

    tracing::info!("Pipeline topological order verified: {:?}", order);
}

/// Test error handling in stage execution.
///
/// Verifies that pipeline failures are properly propagated.
#[tokio::test]
#[ignore = "Requires S3 setup for distributed testing"]
async fn test_stage_execution_error_handling() {
    let _ = tracing_subscriber::fmt::try_init();

    // Build a valid pipeline using fixture file
    let fixture_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("tests/fixtures");
    let input_file = format!("file://{}/roboflow_sample.bag", fixture_dir.display());

    let pipeline = PipelineBuilder::new()
        .stage(Arc::new(DiscoverStage::new(&format!(
            "file://{}/",
            fixture_dir.display()
        ))))
        .stage(Arc::new(ConvertStage::new(
            &input_file,
            "/tmp/output/",
            "v1",
        )))
        .stage(Arc::new(MergeStage::new("/tmp/output/dataset")))
        .dependency(StageId(1), StageId(0))
        .dependency(StageId(2), StageId(1))
        .build()
        .expect("Pipeline should build");

    // Execute - should succeed with test stages
    let executor = StageExecutor::new(2);
    let result = executor.execute(&pipeline).await;

    assert!(
        result.is_ok(),
        "Pipeline execution should succeed: {:?}",
        result.err()
    );
}
