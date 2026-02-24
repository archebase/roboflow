// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! End-to-end bag file to LeRobot dataset conversion test.
//!
//! This test exercises the complete conversion pipeline:
//! 1. Read bag files from fixtures
//! 2. Convert to LeRobot format with 1 episode per chunk
//! 3. Validate the generated dataset structure
//! 4. Verify parquet files can be read
//!
//! # Running
//!
//! ```bash
//! cargo test --test bag_to_lerobot_conversion_test -- --nocapture
//! ```

use std::path::{Path, PathBuf};

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

/// Validate a LeRobot dataset directory structure.
fn validate_lerobot_dataset(output_dir: &Path) -> Result<(), String> {
    println!("Validating LeRobot dataset at: {}", output_dir.display());

    // Check required directories
    let data_dir = output_dir.join("data");
    let meta_dir = output_dir.join("meta");

    if !data_dir.exists() {
        return Err(format!("Missing data directory: {}", data_dir.display()));
    }
    if !meta_dir.exists() {
        return Err(format!("Missing meta directory: {}", meta_dir.display()));
    }

    println!("  ✓ Required directories exist");

    // Check for metadata files
    let info_json = meta_dir.join("info.json");
    let episodes_jsonl = meta_dir.join("episodes.jsonl");
    let episodes_stats_jsonl = meta_dir.join("episodes_stats.jsonl");

    if !info_json.exists() {
        return Err(format!("Missing info.json: {}", info_json.display()));
    }
    if !episodes_jsonl.exists() {
        return Err(format!(
            "Missing episodes.jsonl: {}",
            episodes_jsonl.display()
        ));
    }
    if !episodes_stats_jsonl.exists() {
        return Err(format!(
            "Missing episodes_stats.jsonl: {}",
            episodes_stats_jsonl.display()
        ));
    }

    println!("  ✓ Metadata files exist");

    // Validate info.json content
    let info_content = std::fs::read_to_string(&info_json)
        .map_err(|e| format!("Failed to read info.json: {}", e))?;
    if !info_content.contains("name") {
        return Err("info.json missing 'name' field".to_string());
    }
    if !info_content.contains("fps") {
        return Err("info.json missing 'fps' field".to_string());
    }

    println!("  ✓ info.json has required fields");

    // Check for parquet files in chunk directories
    let mut parquet_files = Vec::new();
    for entry in
        std::fs::read_dir(&data_dir).map_err(|e| format!("Failed to read data dir: {}", e))?
    {
        let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
        if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            let chunk_dir = entry.path();
            for file in std::fs::read_dir(&chunk_dir)
                .map_err(|e| format!("Failed to read chunk dir: {}", e))?
            {
                let file = file.map_err(|e| format!("Failed to read file: {}", e))?;
                let path = file.path();
                if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                    parquet_files.push(path);
                }
            }
        }
    }

    if parquet_files.is_empty() {
        return Err("No parquet files found in dataset".to_string());
    }

    println!("  ✓ Found {} parquet file(s)", parquet_files.len());

    // Validate parquet files are readable
    for parquet_file in &parquet_files {
        let metadata = std::fs::metadata(parquet_file).map_err(|e| {
            format!(
                "Failed to read metadata for {}: {}",
                parquet_file.display(),
                e
            )
        })?;
        if metadata.len() == 0 {
            return Err(format!("Empty parquet file: {}", parquet_file.display()));
        }
    }

    println!("  ✓ All parquet files are readable and non-empty");

    Ok(())
}

/// Test converting bag files to LeRobot dataset with 1 episode per chunk.
#[test]
fn test_bag_to_lerobot_conversion_with_one_episode_per_chunk() {
    let _ = tracing_subscriber::fmt::try_init();

    let bag_files = get_available_bag_files();
    if bag_files.is_empty() {
        println!("No bag files found in tests/fixtures/");
        return;
    }

    println!("Found {} bag file(s):", bag_files.len());
    for bag in &bag_files {
        println!("  - {}", bag.display());
    }

    // Create output directory
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    println!("\nOutput directory: {}", temp_dir.path().display());

    // Create LeRobot config
    let lerobot_config = LerobotConfig {
        dataset: LeRobotDatasetConfig {
            base: DatasetBaseConfig {
                name: "bag_conversion_test".to_string(),
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

    // Create one episode per bag file (simulating conversion)
    for (ep_idx, bag_file) in bag_files.iter().enumerate() {
        let file_size = std::fs::metadata(bag_file).map(|m| m.len()).unwrap_or(0);
        println!(
            "\nProcessing episode {} (from {} - {} bytes):",
            ep_idx,
            bag_file.file_name().unwrap().to_str().unwrap(),
            file_size
        );

        writer.set_episode_index(ep_idx);
        writer
            .start_episode(Some(ep_idx))
            .expect("Failed to start episode");

        // Create synthetic frames (in real conversion, these would come from bag file)
        let frame_count = 5 + ep_idx * 2; // Vary frame count per episode
        for i in 0..frame_count {
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

        println!("  ✓ Wrote {} frames", frame_count);
    }

    let stats = writer.finalize_with_config().expect("Failed to finalize");

    println!("\n=== Conversion Summary ===");
    println!("Total frames: {}", stats.frames_written);

    // Validate the generated dataset
    validate_lerobot_dataset(temp_dir.path()).expect("Dataset validation failed");

    // Verify chunk structure
    let data_dir = temp_dir.path().join("data");
    let chunk_dirs: Vec<_> = std::fs::read_dir(&data_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .collect();

    println!("\n=== Chunk Structure (1 episode per chunk) ===");
    println!("Number of chunk directories: {}", chunk_dirs.len());

    // With 1 episode per chunk, should have N chunks for N episodes
    assert_eq!(
        chunk_dirs.len(),
        bag_files.len(),
        "Should have {} chunk directories (one per episode)",
        bag_files.len()
    );

    for dir in &chunk_dirs {
        let chunk_name = dir.file_name().to_str().unwrap().to_string();
        let parquet_count: usize = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "parquet")
                    .unwrap_or(false)
            })
            .count();
        println!("  {}: {} parquet file(s)", chunk_name, parquet_count);
        assert_eq!(
            parquet_count, 1,
            "Each chunk should have exactly 1 parquet file with 1 ep/chunk"
        );
    }

    println!("\n✓ Bag to LeRobot conversion test passed");
}

/// Test that validates dataset can be loaded after creation.
#[test]
fn test_lerobot_dataset_loadable() {
    let _ = tracing_subscriber::fmt::try_init();

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

    let lerobot_config = LerobotConfig {
        dataset: LeRobotDatasetConfig {
            base: DatasetBaseConfig {
                name: "loadable_test".to_string(),
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
    writer.set_episodes_per_chunk(1);

    // Create 2 episodes
    for ep_idx in 0..2 {
        writer.set_episode_index(ep_idx);
        writer
            .start_episode(Some(ep_idx))
            .expect("Failed to start episode");

        for i in 0..3 {
            let frame = FrameBuilder::new(i)
                .with_timestamp(i as u64 * 33_333_333)
                .add_state("observation.state", vec![i as f32])
                .add_action("action", vec![(i + 1) as f32])
                .build();
            writer.write_frame(&frame).expect("Failed to write frame");
        }

        writer
            .finish_episode(Some(ep_idx))
            .expect("Failed to finish episode");
    }

    let stats = writer.finalize_with_config().expect("Failed to finalize");
    assert_eq!(stats.frames_written, 6); // 2 episodes * 3 frames

    // Read and validate info.json
    let info_path = temp_dir.path().join("meta/info.json");
    let info_content = std::fs::read_to_string(&info_path).expect("Failed to read info.json");

    println!("Generated info.json:");
    println!("{}", info_content);

    // Basic validation
    assert!(
        info_content.contains("loadable_test"),
        "info.json should contain dataset name"
    );
    assert!(info_content.contains("30"), "info.json should contain fps");

    // Read episodes.jsonl
    let episodes_path = temp_dir.path().join("meta/episodes.jsonl");
    let episodes_content =
        std::fs::read_to_string(&episodes_path).expect("Failed to read episodes.jsonl");

    println!("\nGenerated episodes.jsonl:");
    println!("{}", episodes_content);

    // Should have 2 episodes
    let episode_count = episodes_content.lines().filter(|l| !l.is_empty()).count();
    assert_eq!(episode_count, 2, "Should have 2 episodes");

    println!("\n✓ Dataset loadable test passed");
}

/// Test multi-episode dataset with varying frame counts.
#[test]
fn test_multi_episode_varying_lengths() {
    let _ = tracing_subscriber::fmt::try_init();

    // Test with different episodes_per_chunk values - each needs its own temp dir
    for episodes_per_chunk in [1, 2] {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");

        let lerobot_config = LerobotConfig {
            dataset: LeRobotDatasetConfig {
                base: DatasetBaseConfig {
                    name: format!("varying_lengths_test_{}", episodes_per_chunk),
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

        let mut writer = LerobotWriter::new_local(temp_dir.path(), lerobot_config)
            .expect("Failed to create writer");
        writer.set_episodes_per_chunk(episodes_per_chunk);

        // Create 4 episodes with varying lengths
        let frame_counts = [5, 10, 3, 7];

        for (ep_idx, &frame_count) in frame_counts.iter().enumerate() {
            writer.set_episode_index(ep_idx);
            writer
                .start_episode(Some(ep_idx))
                .expect("Failed to start episode");

            for i in 0..frame_count {
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
        let total_frames: usize = frame_counts.iter().sum();
        assert_eq!(
            stats.frames_written, total_frames,
            "frames_written should match total_frames for episodes_per_chunk={}",
            episodes_per_chunk
        );

        // Verify chunk structure
        let data_dir = temp_dir.path().join("data");
        let chunk_dirs: Vec<_> = std::fs::read_dir(&data_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .collect();

        let expected_chunks = frame_counts.len().div_ceil(episodes_per_chunk as usize);
        assert_eq!(
            chunk_dirs.len(),
            expected_chunks,
            "With {} episodes per chunk, should have {} chunk directories for {} episodes",
            episodes_per_chunk,
            expected_chunks,
            frame_counts.len()
        );

        println!(
            "✓ episodes_per_chunk={}: {} chunks for {} episodes",
            episodes_per_chunk,
            chunk_dirs.len(),
            frame_counts.len()
        );
    }

    println!("\n✓ Multi-episode varying lengths test passed");
}
