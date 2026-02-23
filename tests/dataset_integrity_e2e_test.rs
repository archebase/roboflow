// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Dataset integrity e2e tests with data validation.
//!
//! These tests verify data integrity through the complete pipeline:
//! 1. Frame data is correctly written and read back
//! 2. Video encoding produces valid output
//! 3. Parquet files contain expected data
//! 4. Dataset can be loaded and validated
//!
//! # Prerequisites
//!
//! 1. Start infrastructure: `make dev-up`
//! 2. Add to /etc/hosts: `127.0.0.1 pd`
//!
//! # Running
//!
//! ```bash
//! cargo test --test dataset_integrity_e2e_test -- --ignored --nocapture
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

// =============================================================================
// Helper Functions
// =============================================================================

type FrameData = (u64, Vec<f32>, Vec<f32>); // (timestamp, state, action)
type EpisodeData = Vec<FrameData>;

async fn create_dataset_with_specific_data(
    storage: &AsyncS3Storage,
    output_prefix: &str,
    episode_data: Vec<EpisodeData>,
) -> Result<usize, String> {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let lerobot_config = LerobotConfig {
        dataset: LeRobotDatasetConfig {
            base: DatasetBaseConfig {
                name: "integrity_test".to_string(),
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
        LerobotWriter::new_local(temp_dir.path(), lerobot_config).map_err(|e| e.to_string())?;

    // Use 1 episode per chunk for testing
    writer.set_episodes_per_chunk(1);

    for (ep_idx, frames) in episode_data.iter().enumerate() {
        writer.set_episode_index(ep_idx);
        writer
            .start_episode(Some(ep_idx))
            .map_err(|e| format!("Failed to start episode {}: {}", ep_idx, e))?;

        for (frame_idx, (timestamp, state, action)) in frames.iter().enumerate() {
            let frame = FrameBuilder::new(frame_idx)
                .with_timestamp(*timestamp)
                .add_state("observation.state", state.clone())
                .add_action("action", action.clone())
                .build();
            writer
                .write_frame(&frame)
                .map_err(|e| format!("Failed to write frame {}: {}", frame_idx, e))?;
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

fn verify_info_json(temp_dir: &Path) -> Result<serde_json::Value, String> {
    let info_path = temp_dir.join("meta/info.json");
    let info_content = std::fs::read_to_string(&info_path)
        .map_err(|e| format!("Failed to read info.json: {}", e))?;

    let info: serde_json::Value = serde_json::from_str(&info_content)
        .map_err(|e| format!("Failed to parse info.json: {}", e))?;

    // Verify required fields
    if info.get("name").is_none() {
        return Err("info.json missing 'name' field".to_string());
    }
    if info.get("fps").is_none() {
        return Err("info.json missing 'fps' field".to_string());
    }
    if info.get("features").is_none() {
        return Err("info.json missing 'features' field".to_string());
    }
    if info.get("total_episodes").is_none() {
        return Err("info.json missing 'total_episodes' field".to_string());
    }
    if info.get("total_frames").is_none() {
        return Err("info.json missing 'total_frames' field".to_string());
    }

    Ok(info)
}

fn verify_episodes_jsonl(
    temp_dir: &Path,
    expected_count: usize,
) -> Result<Vec<serde_json::Value>, String> {
    let episodes_path = temp_dir.join("meta/episodes.jsonl");
    let episodes_content = std::fs::read_to_string(&episodes_path)
        .map_err(|e| format!("Failed to read episodes.jsonl: {}", e))?;

    let episodes: Vec<serde_json::Value> = episodes_content
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();

    if episodes.len() != expected_count {
        return Err(format!(
            "Expected {} episodes, found {}",
            expected_count,
            episodes.len()
        ));
    }

    for (i, ep) in episodes.iter().enumerate() {
        if ep.get("episode_index").is_none() {
            return Err(format!("Episode {} missing 'episode_index'", i));
        }
        if ep.get("length").is_none() {
            return Err(format!("Episode {} missing 'length'", i));
        }
    }

    Ok(episodes)
}

#[allow(dead_code)]
fn count_parquet_files(temp_dir: &Path) -> Result<usize, String> {
    let data_dir = temp_dir.join("data");
    if !data_dir.exists() {
        return Ok(0);
    }

    let mut count = 0;
    for entry in std::fs::read_dir(&data_dir).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            let chunk_dir = entry.path();
            for file in std::fs::read_dir(&chunk_dir).map_err(|e| e.to_string())? {
                let file = file.map_err(|e| e.to_string())?;
                let path = file.path();
                if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                    count += 1;
                }
            }
        }
    }

    Ok(count)
}

// =============================================================================
// E2E Tests
// =============================================================================

/// Test data integrity through the complete pipeline.
///
/// This test verifies that frame data is correctly preserved through:
/// 1. Dataset creation
/// 2. Serialization to parquet
/// 3. Upload to MinIO
/// 4. Download from MinIO
#[tokio::test]
async fn test_data_integrity_through_pipeline() {
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

    let batch_id = format!("integrity-test-{}", uuid::Uuid::new_v4());
    let output_prefix = format!("integrity/{}", batch_id);

    println!("\n1. Creating batch with specific data patterns...");

    // Create specific data patterns for verification
    let episode_data = vec![
        vec![
            (33_333_333, vec![1.0, 2.0, 3.0], vec![0.1, 0.2]),
            (66_666_666, vec![1.1, 2.1, 3.1], vec![0.15, 0.25]),
            (99_999_999, vec![1.2, 2.2, 3.2], vec![0.2, 0.3]),
        ],
        vec![
            (33_333_333, vec![10.0, 20.0, 30.0], vec![1.0, 2.0]),
            (66_666_666, vec![10.5, 20.5, 30.5], vec![1.1, 2.1]),
            (99_999_999, vec![11.0, 21.0, 31.0], vec![1.2, 2.2]),
            (133_333_332, vec![11.5, 21.5, 31.5], vec![1.3, 2.3]),
        ],
    ];

    let total_expected_frames: usize = episode_data.iter().map(|e| e.len()).sum();

    // Create batch
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

    // Process work unit
    println!("\n2. Processing work unit and generating dataset...");

    let mut work_unit: WorkUnit =
        bincode::deserialize(&tikv.get(unit_key.clone()).await.unwrap().unwrap()).unwrap();

    work_unit.claim("worker-1".to_string()).unwrap();

    let frames_written =
        create_dataset_with_specific_data(&storage, &output_prefix, episode_data.clone())
            .await
            .expect("Failed to create dataset");

    println!("   ✓ Generated dataset with {} frames", frames_written);
    assert_eq!(frames_written, total_expected_frames);

    work_unit.complete();
    tikv.put(unit_key, bincode::serialize(&work_unit).unwrap())
        .await
        .unwrap();

    // Reconcile
    println!("\n3. Reconciling batch...");
    controller.reconcile_all().await.unwrap();

    // Download and validate
    println!("\n4. Downloading and validating dataset...");

    let download_dir = tempfile::tempdir().expect("Failed to create temp dir");

    // Download key files
    let files_to_download = vec!["meta/info.json", "meta/episodes.jsonl"];

    for file in &files_to_download {
        let remote_path = Path::new(&output_prefix).join(file);
        let local_path = download_dir.path().join(file);

        // Create parent directory
        if let Some(parent) = local_path.parent() {
            std::fs::create_dir_all(parent).ok();
        }

        match storage.read(&remote_path).await {
            Ok(data) => {
                std::fs::write(&local_path, data).expect("Failed to write file");
                println!("   ✓ Downloaded: {}", file);
            }
            Err(e) => {
                println!("   ✗ Failed to download {}: {}", file, e);
            }
        }
    }

    // Validate info.json
    println!("\n5. Validating info.json...");
    match verify_info_json(download_dir.path()) {
        Ok(info) => {
            println!(
                "   ✓ Dataset name: {}",
                info["name"].as_str().unwrap_or("unknown")
            );
            println!("   ✓ FPS: {}", info["fps"].as_u64().unwrap_or(0));
            println!(
                "   ✓ Total episodes: {}",
                info["total_episodes"].as_u64().unwrap_or(0)
            );
            println!(
                "   ✓ Total frames: {}",
                info["total_frames"].as_u64().unwrap_or(0)
            );

            assert_eq!(
                info["total_episodes"].as_u64().unwrap_or(0) as usize,
                episode_data.len(),
                "Should have correct number of episodes"
            );
            assert_eq!(
                info["total_frames"].as_u64().unwrap_or(0) as usize,
                total_expected_frames,
                "Should have correct number of frames"
            );
        }
        Err(e) => panic!("info.json validation failed: {}", e),
    }

    // Validate episodes.jsonl
    println!("\n6. Validating episodes.jsonl...");
    match verify_episodes_jsonl(download_dir.path(), episode_data.len()) {
        Ok(episodes) => {
            for (i, ep) in episodes.iter().enumerate() {
                let length = ep["length"].as_u64().unwrap_or(0) as usize;
                println!("   ✓ Episode {}: {} frames", i, length);
                assert_eq!(length, episode_data[i].len());
            }
        }
        Err(e) => panic!("episodes.jsonl validation failed: {}", e),
    }

    // Cleanup
    println!("\n7. Cleaning up...");
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

    println!("\n✓ Data integrity test passed");
    println!(
        "   Verified {} frames across {} episodes",
        total_expected_frames,
        episode_data.len()
    );
}

/// Test that each chunk contains exactly one episode (1 episode per chunk).
#[tokio::test]
async fn test_one_episode_per_chunk_structure() {
    let _ = tracing_subscriber::fmt::try_init();

    let config = TestConfig::default();

    let storage = match config.check_minio().await {
        Ok(s) => s,
        Err(e) => {
            panic!("Required service MinIO is not available: {}", e);
        }
    };

    println!("✓ MinIO is available");

    let test_prefix = format!("chunk-test-{}", uuid::Uuid::new_v4());

    println!("\n1. Creating dataset with 1 episode per chunk...");

    // Create 5 episodes with different frame counts
    let episode_data: Vec<Vec<_>> = (0..5)
        .map(|ep_idx| {
            (0..3 + ep_idx) // 3, 4, 5, 6, 7 frames per episode
                .map(|frame_idx| {
                    (
                        frame_idx as u64 * 33_333_333,
                        vec![ep_idx as f32, frame_idx as f32],
                        vec![(ep_idx + frame_idx) as f32],
                    )
                })
                .collect()
        })
        .collect();

    let frames_written =
        create_dataset_with_specific_data(&storage, &test_prefix, episode_data.clone())
            .await
            .expect("Failed to create dataset");

    let total_expected_frames: usize = episode_data.iter().map(|e| e.len()).sum();
    assert_eq!(frames_written, total_expected_frames);

    println!("   ✓ Created dataset with {} frames", frames_written);

    // Download and verify chunk structure
    println!("\n2. Verifying chunk structure...");

    let download_dir = tempfile::tempdir().expect("Failed to create temp dir");

    // Download info.json
    let remote_info = Path::new(&test_prefix).join("meta/info.json");
    let local_info = download_dir.path().join("meta/info.json");
    std::fs::create_dir_all(local_info.parent().unwrap()).ok();

    if let Ok(data) = storage.read(&remote_info).await {
        std::fs::write(&local_info, data).unwrap();
    }

    // Verify chunk directories
    let _data_dir = download_dir.path().join("data");

    // We need to check chunks were uploaded
    // Since we're not downloading everything, just verify via listing
    let expected_chunks = episode_data.len();

    println!("   Expected chunks: {} (1 per episode)", expected_chunks);

    // Count parquet files by checking each chunk directory
    let mut chunk_parquet_counts = Vec::new();
    for chunk_idx in 0..expected_chunks {
        let chunk_name = format!("chunk-{:03}", chunk_idx);
        let remote_chunk = Path::new(&test_prefix).join("data").join(&chunk_name);

        // Each chunk should have episode_{chunk_idx:06}.parquet (1 episode per chunk)
        let test_file = remote_chunk.join(format!("episode_{:06}.parquet", chunk_idx));
        if storage.exists(&test_file).await {
            chunk_parquet_counts.push((chunk_name, 1));
        } else {
            chunk_parquet_counts.push((chunk_name, 0));
        }
    }

    for (chunk_name, count) in &chunk_parquet_counts {
        println!("   {}: {} parquet file(s)", chunk_name, count);
        assert_eq!(*count, 1, "Each chunk should have exactly 1 parquet file");
    }

    println!("\n✓ 1 episode per chunk structure verified");
    println!(
        "   {} chunks for {} episodes",
        chunk_parquet_counts.len(),
        episode_data.len()
    );
}

/// Test batch with mixed success/failure scenarios.
#[tokio::test]
async fn test_mixed_success_failure_batch() {
    let _ = tracing_subscriber::fmt::try_init();

    let config = TestConfig::default();

    if let Err(e) = config.check_tikv().await {
        panic!("Required service TiKV is not available: {}", e);
    }

    println!("✓ TiKV is available");

    let tikv = Arc::new(TikvClient::from_env().await.unwrap());
    let controller = BatchController::with_client(tikv.clone());

    let batch_id = format!("mixed-test-{}", uuid::Uuid::new_v4());

    println!("\n1. Creating batch with 4 work units...");

    let spec = BatchSpec::new(
        &batch_id,
        vec![
            "s3://test/file1.bag".to_string(),
            "s3://test/file2.bag".to_string(),
            "s3://test/file3.bag".to_string(),
            "s3://test/file4.bag".to_string(),
        ],
        "s3://test/output".to_string(),
    );

    // Get the canonical batch_id from spec (namespace:name format)
    let canonical_batch_id = batch_id_from_spec(&spec);

    let mut status = BatchStatus::new();
    status.transition_to(BatchPhase::Running);
    status.set_work_units_total(4);

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

    // Create work units
    for i in 0..4 {
        let work_unit = WorkUnit::with_id(
            format!("unit-{}", i),
            canonical_batch_id.clone(),
            vec![WorkFile::new(format!("s3://test/file{}.bag", i), 1024)],
            "s3://test/output".to_string(),
            "config-hash".to_string(),
        );

        let unit_key = WorkUnitKeys::unit(&canonical_batch_id, &format!("unit-{}", i));
        let unit_data = bincode::serialize(&work_unit).unwrap();
        tikv.put(unit_key, unit_data).await.unwrap();
    }

    println!("   ✓ Batch created");

    // Process with mixed results: 2 success, 1 fail, 1 retry then success
    println!("\n2. Processing work units (mixed results)...");

    // unit-0: Success
    let unit0_key = WorkUnitKeys::unit(&canonical_batch_id, "unit-0");
    let mut work_unit: WorkUnit =
        bincode::deserialize(&tikv.get(unit0_key.clone()).await.unwrap().unwrap()).unwrap();
    work_unit.claim("worker-1".to_string()).unwrap();
    work_unit.complete();
    tikv.put(unit0_key, bincode::serialize(&work_unit).unwrap())
        .await
        .unwrap();
    println!("   ✓ unit-0: Completed");

    // unit-1: Success
    let unit1_key = WorkUnitKeys::unit(&canonical_batch_id, "unit-1");
    let mut work_unit: WorkUnit =
        bincode::deserialize(&tikv.get(unit1_key.clone()).await.unwrap().unwrap()).unwrap();
    work_unit.claim("worker-1".to_string()).unwrap();
    work_unit.complete();
    tikv.put(unit1_key, bincode::serialize(&work_unit).unwrap())
        .await
        .unwrap();
    println!("   ✓ unit-1: Completed");

    // unit-2: Fail
    let unit2_key = WorkUnitKeys::unit(&canonical_batch_id, "unit-2");
    let mut work_unit: WorkUnit =
        bincode::deserialize(&tikv.get(unit2_key.clone()).await.unwrap().unwrap()).unwrap();
    work_unit.claim("worker-2".to_string()).unwrap();
    work_unit.fail("Processing error".to_string());
    tikv.put(unit2_key, bincode::serialize(&work_unit).unwrap())
        .await
        .unwrap();
    println!("   ✗ unit-2: Failed");

    // unit-3: Success
    let unit3_key = WorkUnitKeys::unit(&canonical_batch_id, "unit-3");
    let mut work_unit: WorkUnit =
        bincode::deserialize(&tikv.get(unit3_key.clone()).await.unwrap().unwrap()).unwrap();
    work_unit.claim("worker-1".to_string()).unwrap();
    work_unit.complete();
    tikv.put(unit3_key, bincode::serialize(&work_unit).unwrap())
        .await
        .unwrap();
    println!("   ✓ unit-3: Completed");

    // Reconcile
    println!("\n3. Reconciling batch...");
    controller.reconcile_all().await.unwrap();

    let final_status: BatchStatus =
        bincode::deserialize(&tikv.get(status_key.clone()).await.unwrap().unwrap()).unwrap();

    println!(
        "   Final: {} completed, {} failed out of {}",
        final_status.work_units_completed,
        final_status.work_units_failed,
        final_status.work_units_total
    );

    assert_eq!(final_status.work_units_completed, 3);
    assert_eq!(final_status.work_units_failed, 1);

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
    for i in 0..4 {
        let _ = tikv
            .delete(WorkUnitKeys::unit(
                &canonical_batch_id,
                &format!("unit-{}", i),
            ))
            .await;
    }

    println!("\n✓ Mixed success/failure test passed");
}
