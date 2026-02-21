// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Correctness test for LeRobotExecutor using real bag files.
//!
//! Verifies that the executor correctly processes a bag file through the
//! full pipeline including video encoding.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use chrono::Utc;
use roboflow_distributed::Executor;
use roboflow_distributed::batch::{WorkFile, WorkUnit, WorkUnitStatus};
use roboflow_distributed::lerobot_executor::LeRobotExecutor;
use roboflow_distributed::worker::{JobRegistry, ProcessingResult};

const TEST_BAG_PATH: &str =
    "tests/fixtures/A02-A01-37-45-77-factory_07-P4_210-leju_claw-20260104174020-v001.bag";
const CONFIG_HASH: &str = "test_config_v1";

fn create_work_unit(bag_path: &str, output_path: &str) -> WorkUnit {
    let metadata = fs::metadata(bag_path).expect("Failed to read bag metadata");
    let absolute_path = std::fs::canonicalize(bag_path)
        .expect("Failed to resolve absolute path")
        .to_string_lossy()
        .to_string();
    WorkUnit {
        id: "test-unit-001".to_string(),
        batch_id: "test-batch".to_string(),
        files: vec![WorkFile {
            url: absolute_path,
            size: metadata.len(),
            modified_at: None,
            checksum: None,
        }],
        output_path: output_path.to_string(),
        config_hash: CONFIG_HASH.to_string(),
        status: WorkUnitStatus::Pending,
        owner: None,
        attempts: 0,
        max_attempts: 3,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        error: None,
        priority: 0,
    }
}

/// Verify LeRobotExecutor correctly processes a bag file with video encoding.
#[tokio::test]
#[ignore = "Requires real bag file - run manually"]
async fn test_lerobot_executor_correctness() {
    if !Path::new(TEST_BAG_PATH).exists() {
        eprintln!("Skipping test: bag file not found at {}", TEST_BAG_PATH);
        return;
    }

    println!("\n=== LeRobotExecutor Correctness Test ===\n");
    println!("Input: {}", TEST_BAG_PATH);

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let output_path = temp_dir.path().to_string_lossy().to_string();

    // Create executor with the new architecture
    let executor: Box<dyn Executor> = Box::new(LeRobotExecutor::new(
        4, // max_concurrent
        output_path.clone(),
    ));

    let work_unit = create_work_unit(TEST_BAG_PATH, &output_path);
    let job_registry = Arc::new(tokio::sync::RwLock::new(JobRegistry::default()));

    println!("Executing work unit...");
    let start_time = std::time::Instant::now();
    let result = executor.execute(&work_unit, job_registry).await;
    let elapsed = start_time.elapsed();

    // Verify result
    match result {
        Ok(ProcessingResult::Success {
            episode_index,
            frame_count,
            ..
        }) => {
            println!("✅ SUCCESS");
            println!("   Episode index: {}", episode_index);
            println!("   Frames processed: {}", frame_count);
            println!("   Elapsed time: {:?}", elapsed);
            println!(
                "   Throughput: {:.2} fps",
                frame_count as f64 / elapsed.as_secs_f64()
            );

            // Verify output files exist (LeRobot format has nested directories)
            println!("\n   Output files:");
            let mut video_count = 0;
            let mut parquet_count = 0;

            fn scan_dir(dir: &Path, videos: &mut u32, parquets: &mut u32) {
                if let Ok(entries) = std::fs::read_dir(dir) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.is_dir() {
                            scan_dir(&path, videos, parquets);
                        } else if path.extension().map(|e| e == "mp4").unwrap_or(false) {
                            *videos += 1;
                            println!("     📹 {}", path.display());
                        } else if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                            *parquets += 1;
                            println!("     📊 {}", path.display());
                        }
                    }
                }
            }

            scan_dir(
                Path::new(&output_path),
                &mut video_count,
                &mut parquet_count,
            );

            // Assertions
            assert!(frame_count > 0, "Should have processed some frames");
            assert!(video_count > 0, "Should have created video files (MP4)");
            assert!(parquet_count > 0, "Should have created parquet files");
            println!("\n   ✅ All assertions passed!");
            println!("      - {} frames processed", frame_count);
            println!("      - {} video files created", video_count);
            println!("      - {} parquet files created", parquet_count);
            println!(
                "      - Throughput: {:.2} fps",
                frame_count as f64 / elapsed.as_secs_f64()
            );
        }
        Ok(ProcessingResult::Failed { error }) => {
            panic!("❌ Executor failed: {}", error);
        }
        Ok(ProcessingResult::Cancelled) => {
            panic!("❌ Executor was cancelled unexpectedly");
        }
        Err(e) => {
            panic!("❌ Executor error: {}", e);
        }
    }

    temp_dir.close().expect("Failed to clean up temp dir");
}

/// Benchmark test for LeRobotExecutor speed.
#[tokio::test]
#[ignore = "Requires real bag file - run manually"]
async fn test_lerobot_executor_speed() {
    if !Path::new(TEST_BAG_PATH).exists() {
        eprintln!("Skipping test: bag file not found at {}", TEST_BAG_PATH);
        return;
    }

    println!("\n=== LeRobotExecutor Speed Test ===\n");
    println!("Input: {}", TEST_BAG_PATH);

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let output_path = temp_dir.path().to_string_lossy().to_string();

    // Create executor
    let executor: Box<dyn Executor> = Box::new(LeRobotExecutor::new(
        4, // max_concurrent
        output_path.clone(),
    ));

    let work_unit = create_work_unit(TEST_BAG_PATH, &output_path);
    let job_registry = Arc::new(tokio::sync::RwLock::new(JobRegistry::default()));

    println!("Executing work unit...");
    let start_time = std::time::Instant::now();
    let result = executor.execute(&work_unit, job_registry).await;
    let elapsed = start_time.elapsed();

    match result {
        Ok(ProcessingResult::Success { frame_count, .. }) => {
            let fps = frame_count as f64 / elapsed.as_secs_f64();
            println!("\n✅ Speed Test Results:");
            println!("   Total frames: {}", frame_count);
            println!("   Total time: {:?}", elapsed);
            println!("   Throughput: {:.2} fps", fps);
            println!("   Processing time per frame: {:.2} ms", 1000.0 / fps);
        }
        _ => panic!("Test failed"),
    }

    temp_dir.close().expect("Failed to clean up temp dir");
}
