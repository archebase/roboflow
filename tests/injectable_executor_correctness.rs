// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Correctness test for InjectableTaskExecutor using real bag files.
//!
//! Verifies that the executor correctly processes a bag file through the
//! full pipeline including video encoding.

use std::fs;
use std::path::Path;
use std::time::Duration;

use chrono::Utc;
use roboflow::{DatasetBaseConfig, LerobotConfig, VideoConfig};
use roboflow_dataset::lerobot::{FlushingConfig, Mapping, MappingType, StreamingConfig};
use roboflow_distributed::batch::{WorkFile, WorkUnit, WorkUnitStatus};
use roboflow_distributed::providers::{InMemoryConfigProvider, ProductionSourceProvider};
use roboflow_distributed::worker::{InjectableTaskExecutor, NoOpJobRegistry, ProcessingResult};

const TEST_BAG_PATH: &str =
    "tests/fixtures/A02-A01-37-45-77-factory_07-P4_210-leju_claw-20260104174020-v001.bag";
const CONFIG_HASH: &str = "test_config_v1";

fn create_lerobot_config() -> LerobotConfig {
    LerobotConfig {
        dataset: roboflow::lerobot::DatasetConfig {
            base: DatasetBaseConfig {
                name: "test_dataset".to_string(),
                fps: 30,
                robot_type: Some("kuavo_p4".to_string()),
            },
            env_type: None,
        },
        mappings: vec![
            Mapping {
                topic: "/cam_h/color/image_raw/compressed".to_string(),
                feature: "observation.images.cam_high".to_string(),
                mapping_type: MappingType::Image,
                camera_key: Some("cam_high".to_string()),
            },
            Mapping {
                topic: "/cam_l/color/image_raw/compressed".to_string(),
                feature: "observation.images.cam_left".to_string(),
                mapping_type: MappingType::Image,
                camera_key: Some("cam_left".to_string()),
            },
            Mapping {
                topic: "/cam_r/color/image_raw/compressed".to_string(),
                feature: "observation.images.cam_right".to_string(),
                mapping_type: MappingType::Image,
                camera_key: Some("cam_right".to_string()),
            },
            Mapping {
                topic: "/kuavo_arm_traj".to_string(),
                feature: "observation.state".to_string(),
                mapping_type: MappingType::State,
                camera_key: None,
            },
            Mapping {
                topic: "/joint_cmd".to_string(),
                feature: "action".to_string(),
                mapping_type: MappingType::Action,
                camera_key: None,
            },
        ],
        video: VideoConfig {
            codec: "libx264".to_string(),
            crf: 18,
            preset: "fast".to_string(),
            profile: None,
        },
        annotation_file: None,
        flushing: FlushingConfig::default(),
        streaming: StreamingConfig::default(),
    }
}

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
            url: absolute_path, // Just use the path directly, not file:// URL
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

/// Verify InjectableTaskExecutor correctly processes a bag file with video encoding.
#[test]
#[ignore = "Requires real bag file - run manually"]
fn test_injectable_executor_correctness() {
    // Initialize source registry
    roboflow_sources::register_builtin_sources();

    if !Path::new(TEST_BAG_PATH).exists() {
        eprintln!("Skipping test: bag file not found at {}", TEST_BAG_PATH);
        return;
    }

    println!("\n=== InjectableTaskExecutor Correctness Test ===\n");
    println!("Input: {}", TEST_BAG_PATH);

    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().to_string_lossy().to_string();

        // Create executor with injectable dependencies
        let config = create_lerobot_config();
        let config_provider = InMemoryConfigProvider::new().with_config(CONFIG_HASH, config);

        let executor = InjectableTaskExecutor::new(
            ProductionSourceProvider::new(), // Real source provider
            config_provider,                 // In-memory config
            NoOpJobRegistry::new(),          // No-op job registry
            output_path.clone(),
            Duration::from_secs(3600),
        );

        let work_unit = create_work_unit(TEST_BAG_PATH, &output_path);

        println!("Executing work unit...");
        let result = executor.execute(&work_unit).await;

        // Verify result
        match result {
            ProcessingResult::Success {
                episode_index,
                frame_count,
                ..
            } => {
                println!("✅ SUCCESS");
                println!("   Episode index: {}", episode_index);
                println!("   Frames processed: {}", frame_count);

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
            }
            ProcessingResult::Failed { error } => {
                panic!("❌ Executor failed: {}", error);
            }
            ProcessingResult::Cancelled => {
                panic!("❌ Executor was cancelled unexpectedly");
            }
        }

        temp_dir.close().expect("Failed to clean up temp dir");
    });
}
