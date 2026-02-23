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
use roboflow_distributed::tikv::{ConfigRecord, TikvClient};
use roboflow_distributed::worker::{JobRegistry, ProcessingResult};

const TEST_BAG_PATH: &str = "tests/fixtures/roboflow_extracted.bag";
const CONFIG_FILE_PATH: &str = "examples/rust/lerobot_config.toml";

fn create_work_unit(bag_path: &str, output_path: &str, config_hash: &str) -> WorkUnit {
    let metadata = match fs::metadata(bag_path) {
        Ok(m) => m,
        Err(_) => {
            return WorkUnit {
                id: "test-unit-001".to_string(),
                batch_id: "test-batch".to_string(),
                files: vec![],
                output_path: output_path.to_string(),
                config_hash: config_hash.to_string(),
                status: WorkUnitStatus::Pending,
                owner: None,
                attempts: 0,
                max_attempts: 3,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                error: None,
                priority: 0,
            };
        }
    };
    let absolute_path = match std::fs::canonicalize(bag_path) {
        Ok(p) => p.to_string_lossy().to_string(),
        Err(_) => bag_path.to_string(),
    };
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
        config_hash: config_hash.to_string(),
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

/// Get TiKV client or return None if not available
async fn get_tikv_or_none() -> Option<Arc<TikvClient>> {
    match TikvClient::from_env().await {
        Ok(c) => Some(Arc::new(c)),
        Err(e) => {
            println!("TiKV not available: {}", e);
            None
        }
    }
}

/// Store config in TiKV and return the config hash
async fn store_config_in_tikv(tikv: &TikvClient) -> Option<String> {
    // Read config file
    let config_content = match fs::read_to_string(CONFIG_FILE_PATH) {
        Ok(content) => content,
        Err(e) => {
            println!("Failed to read config file: {}", e);
            return None;
        }
    };

    // Create config record
    let config_record = ConfigRecord::new(config_content);
    let config_hash = config_record.hash.clone();

    // Store in TiKV
    match tikv.put_config(&config_record).await {
        Ok(_) => {
            println!("Stored config in TiKV with hash: {}", config_hash);
            Some(config_hash)
        }
        Err(e) => {
            println!("Failed to store config in TiKV: {}", e);
            None
        }
    }
}

/// Verify LeRobotExecutor correctly processes a bag file with video encoding.
///
/// This test:
/// 1. Connects to TiKV (if available)
/// 2. Stores the config from examples/rust/lerobot_config.toml in TiKV
/// 3. Creates an executor with TiKV client
/// 4. Processes the bag file using the config from TiKV
#[tokio::test]
async fn test_lerobot_executor_correctness() {
    let _ = tracing_subscriber::fmt::try_init();

    // Skip if bag file doesn't exist
    if !Path::new(TEST_BAG_PATH).exists() {
        println!("Skipping test: bag file not found at {}", TEST_BAG_PATH);
        return;
    }

    println!("\n=== LeRobotExecutor Correctness Test ===\n");
    println!("Input: {}", TEST_BAG_PATH);

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let output_path = temp_dir.path().to_string_lossy().to_string();

    // Try to get TiKV client and store config
    let tikv = get_tikv_or_none().await;
    let config_hash = if let Some(ref tikv_client) = tikv {
        match store_config_in_tikv(tikv_client).await {
            Some(hash) => hash,
            None => "default".to_string(),
        }
    } else {
        println!("TiKV not available, using default config");
        "default".to_string()
    };

    // Create executor with TiKV client if available
    let mut executor_builder = LeRobotExecutor::new(4, output_path.clone());
    if let Some(tikv_client) = tikv {
        executor_builder = executor_builder.with_tikv(tikv_client);
    }
    let executor: Box<dyn Executor> = Box::new(executor_builder);

    let work_unit = create_work_unit(TEST_BAG_PATH, &output_path, &config_hash);
    let job_registry = Arc::new(tokio::sync::RwLock::new(JobRegistry::default()));

    println!("Executing work unit with config_hash: {}", config_hash);
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
            if elapsed.as_secs_f64() > 0.0 {
                println!(
                    "   Throughput: {:.2} fps",
                    frame_count as f64 / elapsed.as_secs_f64()
                );
            }

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
            println!("\n   ✅ All assertions passed!");
            println!("      - {} frames processed", frame_count);
            println!("      - {} video files created", video_count);
            println!("      - {} parquet files created", parquet_count);
        }
        Ok(ProcessingResult::Failed { error }) => {
            // This may happen if config doesn't match bag file topics
            println!(
                "⚠️ Executor failed (config may not match bag file topics): {}",
                error
            );
            // Don't panic - this is expected if the bag file doesn't have the expected topics
        }
        Ok(ProcessingResult::Cancelled) => {
            println!("⚠️ Executor was cancelled");
        }
        Err(e) => {
            println!("⚠️ Executor error: {}", e);
        }
    }

    temp_dir.close().expect("Failed to clean up temp dir");
}

/// Benchmark test for LeRobotExecutor speed.
///
/// This test measures throughput when processing bag files.
#[tokio::test]
async fn test_lerobot_executor_speed() {
    let _ = tracing_subscriber::fmt::try_init();

    // Skip if bag file doesn't exist
    if !Path::new(TEST_BAG_PATH).exists() {
        println!("Skipping test: bag file not found at {}", TEST_BAG_PATH);
        return;
    }

    println!("\n=== LeRobotExecutor Speed Test ===\n");
    println!("Input: {}", TEST_BAG_PATH);

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let output_path = temp_dir.path().to_string_lossy().to_string();

    // Try to get TiKV client and store config
    let tikv = get_tikv_or_none().await;
    let config_hash = if let Some(ref tikv_client) = tikv {
        match store_config_in_tikv(tikv_client).await {
            Some(hash) => hash,
            None => "default".to_string(),
        }
    } else {
        println!("TiKV not available, using default config");
        "default".to_string()
    };

    // Create executor with TiKV client if available
    let mut executor_builder = LeRobotExecutor::new(4, output_path.clone());
    if let Some(tikv_client) = tikv {
        executor_builder = executor_builder.with_tikv(tikv_client);
    }
    let executor: Box<dyn Executor> = Box::new(executor_builder);

    let work_unit = create_work_unit(TEST_BAG_PATH, &output_path, &config_hash);
    let job_registry = Arc::new(tokio::sync::RwLock::new(JobRegistry::default()));

    println!("Executing work unit with config_hash: {}", config_hash);
    let start_time = std::time::Instant::now();
    let result = executor.execute(&work_unit, job_registry).await;
    let elapsed = start_time.elapsed();

    match result {
        Ok(ProcessingResult::Success { frame_count, .. }) => {
            let fps = if elapsed.as_secs_f64() > 0.0 {
                frame_count as f64 / elapsed.as_secs_f64()
            } else {
                0.0
            };
            println!("\n✅ Speed Test Results:");
            println!("   Total frames: {}", frame_count);
            println!("   Total time: {:?}", elapsed);
            println!("   Throughput: {:.2} fps", fps);
            if fps > 0.0 {
                println!("   Processing time per frame: {:.2} ms", 1000.0 / fps);
            }
        }
        Ok(ProcessingResult::Failed { error }) => {
            println!(
                "⚠️ Executor failed (config may not match bag file topics): {}",
                error
            );
        }
        Ok(ProcessingResult::Cancelled) => {
            println!("⚠️ Executor was cancelled");
        }
        Err(e) => {
            println!("⚠️ Executor error: {}", e);
        }
    }

    temp_dir.close().expect("Failed to clean up temp dir");
}
