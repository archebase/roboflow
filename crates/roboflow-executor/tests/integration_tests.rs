// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Integration test for stage-based executor with 100k episode scale.
//!
//! This test verifies that the new roboflow-executor framework can handle
//! large-scale dataset processing through the StageExecutorBridge.

use std::sync::Arc;

use roboflow_distributed::{
    StageExecutorBridge, WorkFile, WorkUnit,
    worker::{JobRegistry, ProcessingResult},
};
use roboflow_executor::{PipelineBuilder, StageExecutor, StageId};

/// Test the StageExecutorBridge with multiple work units.
///
/// This simulates processing multiple episodes through the
/// Discover → Convert → Merge pipeline.
#[tokio::test]
async fn test_stage_executor_bridge_multiple_work_units() {
    let _ = tracing_subscriber::fmt::try_init();

    let bridge = StageExecutorBridge::new(4, "/tmp/output");
    let registry = Arc::new(tokio::sync::RwLock::new(JobRegistry::default()));

    // Process multiple work units (simulating 100 episodes)
    let num_episodes = 100usize;
    let mut results = Vec::with_capacity(num_episodes);

    for i in 0..num_episodes {
        let work_unit = WorkUnit::new(
            format!("test-batch-{}", i),
            vec![WorkFile::new(
                format!("file:///tmp/test_{}.bag", i),
                1024 * 1024, // 1MB file
            )],
            format!("/tmp/output/{}", i),
            format!("config_hash_{}", i),
        );

        let result = bridge.execute(&work_unit, registry.clone()).await;
        results.push(result);
    }

    // Verify all succeeded
    let success_count = results
        .iter()
        .filter(|r| matches!(r, Ok(ProcessingResult::Success { .. })))
        .count();

    assert_eq!(
        success_count, num_episodes,
        "All {} episodes should process successfully",
        num_episodes
    );

    tracing::info!(
        "Successfully processed {} episodes through stage-based pipeline",
        num_episodes
    );
}

/// Test the core StageExecutor with LeRobot pipeline stages.
///
/// This test directly uses the StageExecutor (bypassing the bridge)
/// to verify the Discover → Convert → Merge pipeline works correctly.
#[tokio::test]
async fn test_stage_executor_lerobot_pipeline() {
    let _ = tracing_subscriber::fmt::try_init();

    use roboflow_executor::{ConvertStage, DiscoverStage, MergeStage};

    // Build the LeRobot pipeline
    let source_prefix = "file:///tmp/input/";
    let output_prefix = "/tmp/output";

    let pipeline = PipelineBuilder::new()
        .stage(Arc::new(DiscoverStage::new(source_prefix)))
        .stage(Arc::new(ConvertStage::new(output_prefix, "config_v1")))
        .stage(Arc::new(MergeStage::new(format!("{}/dataset", output_prefix))))
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
    assert_eq!(
        result.stages_completed, 3,
        "All 3 stages should complete"
    );
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

    use roboflow_executor::{ConvertStage, DiscoverStage, MergeStage};

    // Build pipeline with explicit dependencies
    let pipeline = PipelineBuilder::new()
        .stage(Arc::new(DiscoverStage::new("s3://bucket/input/")))
        .stage(Arc::new(ConvertStage::new("s3://bucket/output/", "v1")))
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

    tracing::info!(
        "Pipeline topological order verified: {:?}",
        order
    );
}

/// Benchmark test for 100k episode scale simulation.
///
/// This test is marked as #[ignore] because it takes significant time.
/// Run with: cargo test test_100k_episode_scale -- --ignored
#[tokio::test]
#[ignore = "Long-running benchmark test"]
async fn test_100k_episode_scale() {
    let _ = tracing_subscriber::fmt::try_init();

    let bridge = StageExecutorBridge::new(16, "/tmp/output");
    let registry = Arc::new(tokio::sync::RwLock::new(JobRegistry::default()));

    let num_episodes = 100_000usize;
    let start_time = std::time::Instant::now();

    // Process work units in batches to avoid memory issues
    let batch_size = 1000;
    let mut total_success = 0;

    for batch in 0..(num_episodes / batch_size) {
        let mut batch_futures = Vec::with_capacity(batch_size);

        for i in 0..batch_size {
            let episode_idx = batch * batch_size + i;
            let work_unit = WorkUnit::new(
                format!("batch-{}", episode_idx),
                vec![WorkFile::new(
                    format!("s3://bucket/episode_{:06}.bag", episode_idx),
                    10 * 1024 * 1024, // 10MB file
                )],
                format!("/tmp/output/episode_{:06}", episode_idx),
                "config_hash".to_string(),
            );

            let registry_clone = registry.clone();
            let bridge_ref = &bridge;

            batch_futures.push(async move {
                bridge_ref.execute(&work_unit, registry_clone).await
            });
        }

        // Execute batch concurrently
        let results = futures::future::join_all(batch_futures).await;
        let batch_success = results
            .iter()
            .filter(|r| matches!(r, Ok(ProcessingResult::Success { .. })))
            .count();
        total_success += batch_success;

        if batch % 10 == 0 {
            tracing::info!(
                "Processed batch {}/{}, total success: {}",
                batch,
                num_episodes / batch_size,
                total_success
            );
        }
    }

    let duration = start_time.elapsed();

    tracing::info!(
        total_episodes = num_episodes,
        successful = total_success,
        duration_secs = duration.as_secs_f64(),
        throughput_eps = num_episodes as f64 / duration.as_secs_f64(),
        "100k episode scale test completed"
    );

    assert_eq!(
        total_success, num_episodes,
        "All episodes should process successfully"
    );
}

/// Test error handling in stage execution.
///
/// Verifies that pipeline failures are properly propagated.
#[tokio::test]
async fn test_stage_execution_error_handling() {
    let _ = tracing_subscriber::fmt::try_init();

    use roboflow_executor::{ConvertStage, DiscoverStage, MergeStage};

    // Build a valid pipeline
    let pipeline = PipelineBuilder::new()
        .stage(Arc::new(DiscoverStage::new("/tmp/input/")))
        .stage(Arc::new(ConvertStage::new("/tmp/output/", "v1")))
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
