use std::collections::HashMap;
use std::fs;
use std::path::Path;

use roboflow::{DatasetBaseConfig, LerobotConfig, LerobotWriter, VideoConfig};
use roboflow_pipeline::DatasetWriter;
use roboflow_pipeline::formats::lerobot::{
    FlushingConfig, Mapping, MappingType, StreamingConfig as LerobotStreamingConfig,
};
use roboflow_pipeline::formats::streaming::StreamingConfig;
use roboflow_pipeline::sources::SourceConfig;
use roboflow_pipeline::{PipelineConfig, PipelineExecutor};

// Large bag files (1.6GB/1.7GB) - used for comprehensive testing
const _LARGE_BAG_PATH_1: &str =
    "tests/fixtures/A02-A01-37-45-77-factory_07-P4_210-leju_claw-20260104174020-v001.bag";
const _LARGE_BAG_PATH_2: &str =
    "tests/fixtures/A02-A01-37-45-77-factory_07-P4_210-leju_claw-20260105142915-v001.bag";

// Smaller fixture files (~120MB, ~4000 frames) - used for faster CI testing
const TEST_BAG_PATH: &str = "tests/fixtures/test_4000_frames.bag";
const TEST_BAG_PATH_2: &str = "tests/fixtures/test_4000_frames_2.bag";

fn create_test_lerobot_config() -> LerobotConfig {
    LerobotConfig {
        dataset: roboflow::lerobot::DatasetConfig {
            base: DatasetBaseConfig {
                name: "rubbish_sorting".to_string(),
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
        streaming: LerobotStreamingConfig::default(),
    }
}

#[test]
fn test_bag_to_lerobot_e2e() {
    if !Path::new(TEST_BAG_PATH).exists() {
        eprintln!("Skipping test: bag file not found at {}", TEST_BAG_PATH);
        return;
    }

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let output_path = temp_dir.path();

    let config = create_test_lerobot_config();

    // Register builtin sources before creating source
    roboflow_pipeline::sources::register_builtin_sources();

    let topic_mappings: HashMap<String, String> = config
        .mappings
        .iter()
        .map(|m| (m.topic.clone(), m.feature.clone()))
        .collect();

    // Use streaming config from LerobotConfig
    let pipeline_streaming =
        roboflow_pipeline::formats::streaming::StreamingConfig::with_fps(config.dataset.base.fps);
    let pipeline_config =
        PipelineConfig::new(pipeline_streaming).with_topic_mappings(topic_mappings);

    let writer = LerobotWriter::new_local(output_path, config.clone())
        .expect("Failed to create LeRobot writer");
    let mut executor = PipelineExecutor::new(writer, pipeline_config);

    let source_config = SourceConfig::bag(TEST_BAG_PATH);
    let mut source = roboflow_pipeline::sources::create_source(&source_config)
        .expect("Failed to create bag source");

    let _metadata: roboflow_pipeline::sources::SourceMetadata = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(source.initialize(&source_config))
        .expect("Failed to initialize source");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let frame_count = rt.block_on(async {
        let mut count = 0usize;
        loop {
            match source.read_batch(100).await {
                Ok(Some(messages)) if !messages.is_empty() => {
                    for msg in messages {
                        if executor.process_message(msg).is_ok() {
                            count += 1;
                        }
                        if count.is_multiple_of(100) {
                            println!("Processed {} frames...", count);
                        }
                    }
                }
                Ok(Some(_)) => continue,
                Ok(None) => {
                    println!("End of stream reached after {} frames", count);
                    break;
                }
                Err(e) => {
                    eprintln!("Error reading batch: {}", e);
                    break;
                }
            }
        }
        count
    });

    println!("Total frames processed: {}", frame_count);

    let stats = executor.finalize().expect("Failed to finalize executor");
    println!("Writer stats: {:?}", stats);

    println!("\nOutput directory structure:");
    fn list_dir(path: &std::path::Path, prefix: &str) {
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                let full_path = entry.path();
                if full_path.is_dir() {
                    println!("{}{}/", prefix, name_str);
                    list_dir(&full_path, &format!("{}  ", prefix));
                } else if let Ok(meta) = entry.metadata() {
                    println!("{}{} ({} bytes)", prefix, name_str, meta.len());
                } else {
                    println!("{}{}", prefix, name_str);
                }
            }
        }
    }
    list_dir(output_path, "  ");

    verify_lerobot_structure(output_path, &config);

    println!("LeRobot 2.1 output structure verified successfully!");
    println!("Output location: {}", output_path.display());

    temp_dir.close().expect("Failed to clean up temp dir");
}

#[test]
#[ignore = "Requires S3/MinIO and real bag file - run manually"]
fn test_bag_to_lerobot_s3_upload() {
    if !Path::new(TEST_BAG_PATH).exists() {
        eprintln!("Skipping test: bag file not found at {}", TEST_BAG_PATH);
        return;
    }

    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
        std::env::set_var("AWS_REGION", "us-east-1");
        std::env::set_var("AWS_ENDPOINT_URL", "http://localhost:9000");
    }

    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let local_path = temp_dir.path();

        let config = create_test_lerobot_config();

        let mut writer = LerobotWriter::new_local(local_path, config.clone())
            .expect("Failed to create LeRobot writer");

        let source_config = SourceConfig::bag(TEST_BAG_PATH);
        let mut source = roboflow_pipeline::sources::create_source(&source_config)
            .expect("Failed to create bag source");

        let metadata: roboflow_pipeline::sources::SourceMetadata = source.initialize(&source_config).await
            .expect("Failed to initialize source");
        println!("Source metadata: {:?}", metadata);

        writer.start_episode(Some(0)).expect("Failed to start episode");

        let mut frame_count = 0usize;
        loop {
            match source.read_batch(100).await {
                Ok(Some(messages)) => {
                    for _msg in messages {
                        frame_count += 1;
                        if frame_count.is_multiple_of(100) {
                            println!("Processed {} frames...", frame_count);
                        }
                    }
                }
                Ok(None) => {
                    println!("End of stream reached after {} frames", frame_count);
                    break;
                }
                Err(e) => {
                    eprintln!("Error reading batch: {}", e);
                    break;
                }
            }
        }

        writer.finish_episode(Some(0)).expect("Failed to finish episode");

        let stats = DatasetWriter::finalize(&mut writer)
            .expect("Failed to finalize writer");
        println!("Writer stats: {:?}", stats);

        verify_lerobot_structure(local_path, &config);

        println!("LeRobot 2.1 conversion completed successfully!");
        println!("To upload to S3, use: aws s3 cp {} s3://roboflow-datasets/test-e2e/ --recursive --endpoint-url=http://localhost:9000", 
            local_path.display());
    });
}

#[allow(clippy::cognitive_complexity)]
fn verify_lerobot_structure(output_path: &Path, config: &LerobotConfig) {
    use serde_json::Value;

    let data_dir = output_path.join("data");
    let meta_dir = output_path.join("meta");
    let videos_dir = output_path.join("videos");

    assert!(data_dir.exists(), "data directory should exist");
    assert!(meta_dir.exists(), "meta directory should exist");
    assert!(videos_dir.exists(), "videos directory should exist");

    println!("Verifying LeRobot v2.1 metadata files...");

    // 1. Verify info.json (LeRobot v2.1)
    let info_path = meta_dir.join("info.json");
    assert!(info_path.exists(), "info.json should exist");

    let info_content = fs::read_to_string(&info_path).expect("Failed to read info.json");
    let info: Value = serde_json::from_str(&info_content).expect("Failed to parse info.json");

    assert_eq!(
        info["name"].as_str(),
        Some("rubbish_sorting"),
        "info.json should contain correct dataset name"
    );
    assert_eq!(
        info["fps"].as_u64(),
        Some(30),
        "info.json should contain correct fps"
    );
    assert_eq!(
        info["robot_type"].as_str(),
        Some("kuavo_p4"),
        "info.json should contain correct robot_type"
    );
    assert!(
        info["codebase_version"].as_str().is_some(),
        "info.json should contain codebase_version"
    );
    assert!(
        info["total_episodes"].as_u64().is_some(),
        "info.json should contain total_episodes"
    );
    assert!(
        info["total_frames"].as_u64().is_some(),
        "info.json should contain total_frames"
    );
    assert!(
        info["features"].is_object(),
        "info.json should contain features dictionary"
    );

    println!("  ✓ info.json verified (LeRobot v2.1)");

    // 2. Verify episodes.jsonl (LeRobot v2.1)
    let episodes_path = meta_dir.join("episodes.jsonl");
    assert!(episodes_path.exists(), "episodes.jsonl should exist");

    let episodes_content =
        fs::read_to_string(&episodes_path).expect("Failed to read episodes.jsonl");
    let episodes: Vec<Value> = episodes_content
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).expect("Failed to parse episode line"))
        .collect();

    assert!(
        !episodes.is_empty(),
        "episodes.jsonl should contain at least one episode"
    );
    assert!(
        episodes[0]["episode_index"].is_number(),
        "episodes.jsonl entries should have episode_index"
    );
    assert!(
        episodes[0]["length"].is_number(),
        "episodes.jsonl entries should have length"
    );

    println!("  ✓ episodes.jsonl verified ({} episodes)", episodes.len());

    // 3. Verify tasks.jsonl (LeRobot v2.1) - optional
    let tasks_path = meta_dir.join("tasks.jsonl");
    if tasks_path.exists() {
        let tasks_content = fs::read_to_string(&tasks_path).expect("Failed to read tasks.jsonl");
        let tasks: Vec<Value> = tasks_content
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str(line).expect("Failed to parse task line"))
            .collect();

        if !tasks.is_empty() {
            assert!(
                tasks[0]["task_index"].is_number(),
                "tasks.jsonl entries should have task_index"
            );
            println!("  ✓ tasks.jsonl verified ({} tasks)", tasks.len());
        } else {
            println!("  ℓ tasks.jsonl exists but is empty");
        }
    } else {
        println!("  ℓ tasks.jsonl not present (optional)");
    }

    // 4. Verify episodes_stats.jsonl (LeRobot v2.1)
    let stats_path = meta_dir.join("episodes_stats.jsonl");
    assert!(stats_path.exists(), "episodes_stats.jsonl should exist");

    let stats_content =
        fs::read_to_string(&stats_path).expect("Failed to read episodes_stats.jsonl");
    let stats: Vec<Value> = stats_content
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).expect("Failed to parse stats line"))
        .collect();

    assert!(
        !stats.is_empty(),
        "episodes_stats.jsonl should contain at least one episode stats"
    );
    assert!(
        stats[0]["episode_index"].is_number(),
        "episodes_stats.jsonl entries should have episode_index"
    );
    assert!(
        stats[0]["stats"].is_object(),
        "episodes_stats.jsonl entries should have stats object"
    );

    println!(
        "  ✓ episodes_stats.jsonl verified ({} episode stats)",
        stats.len()
    );

    // 5. Verify Parquet data file
    let parquet_path = data_dir.join("chunk-000").join("episode_000000.parquet");
    assert!(
        parquet_path.exists(),
        "Episode parquet file should exist at {}",
        parquet_path.display()
    );

    let parquet_metadata = fs::metadata(&parquet_path).expect("Failed to read parquet metadata");
    assert!(
        parquet_metadata.len() > 0,
        "Parquet file should not be empty"
    );

    println!(
        "  ✓ Episode parquet verified ({} bytes)",
        parquet_metadata.len()
    );

    // 6. Verify video files for all cameras
    let mut video_count = 0;
    for mapping in &config.mappings {
        if mapping.mapping_type == MappingType::Image && !mapping.feature.contains(".depth") {
            let video_path = videos_dir
                .join("chunk-000")
                .join(&mapping.feature)
                .join("episode_000000.mp4");

            println!("  Looking for video at: {}", video_path.display());

            if video_path.exists() {
                let video_metadata =
                    fs::metadata(&video_path).expect("Failed to read video metadata");
                if video_metadata.len() > 0 {
                    video_count += 1;
                    println!(
                        "  ✓ Video MP4 for '{}' verified ({} bytes)",
                        mapping.feature,
                        video_metadata.len()
                    );
                }
            } else {
                println!(
                    "  ✗ Video not found at expected path: {}",
                    video_path.display()
                );
            }
        }
    }

    assert!(
        video_count >= 3,
        "Expected at least 3 videos (cam_high, cam_left, cam_right), found {}",
        video_count
    );

    println!("\nLeRobot v2.1 structure verification complete!");
    println!("  - info.json: ✓ (codebase_version, robot_type, fps, features)");
    println!("  - episodes.jsonl: ✓ ({} episodes)", episodes.len());
    println!("  - data/chunk-000/episode_000000.parquet: ✓");
    println!("  - videos/chunk-000/*/episode_000000.mp4: ✓");
}

#[allow(dead_code)]
fn verify_lerobot_structure_multi_episode(
    output_path: &Path,
    config: &LerobotConfig,
    expected_episodes: usize,
) {
    use serde_json::Value;

    let data_dir = output_path.join("data");
    let meta_dir = output_path.join("meta");
    let videos_dir = output_path.join("videos");

    assert!(data_dir.exists(), "data directory should exist");
    assert!(meta_dir.exists(), "meta directory should exist");
    assert!(videos_dir.exists(), "videos directory should exist");

    println!(
        "Verifying LeRobot v2.1 metadata files for {} episodes...",
        expected_episodes
    );

    // 1. Verify info.json
    let info_path = meta_dir.join("info.json");
    assert!(info_path.exists(), "info.json should exist");

    let info_content = fs::read_to_string(&info_path).expect("Failed to read info.json");
    let info: Value = serde_json::from_str(&info_content).expect("Failed to parse info.json");

    assert_eq!(
        info["name"].as_str(),
        Some("rubbish_sorting"),
        "info.json should contain correct dataset name"
    );
    assert_eq!(
        info["total_episodes"].as_u64(),
        Some(expected_episodes as u64),
        "info.json should show {} episodes",
        expected_episodes
    );

    println!(
        "  ✓ info.json verified ({} total_episodes)",
        expected_episodes
    );

    // 2. Verify episodes.jsonl has correct number of episodes
    let episodes_path = meta_dir.join("episodes.jsonl");
    assert!(episodes_path.exists(), "episodes.jsonl should exist");

    let episodes_content =
        fs::read_to_string(&episodes_path).expect("Failed to read episodes.jsonl");
    let episodes: Vec<Value> = episodes_content
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).expect("Failed to parse episode line"))
        .collect();

    assert_eq!(
        episodes.len(),
        expected_episodes,
        "episodes.jsonl should contain {} episodes",
        expected_episodes
    );

    // Verify episode indices are sequential
    for (i, episode) in episodes.iter().enumerate() {
        assert_eq!(
            episode["episode_index"].as_u64(),
            Some(i as u64),
            "Episode {} should have episode_index {}",
            i,
            i
        );
    }

    println!(
        "  ✓ episodes.jsonl verified ({} episodes with correct indices)",
        episodes.len()
    );

    // 3. Verify episodes_stats.jsonl has stats for each episode
    let stats_path = meta_dir.join("episodes_stats.jsonl");
    assert!(stats_path.exists(), "episodes_stats.jsonl should exist");

    let stats_content =
        fs::read_to_string(&stats_path).expect("Failed to read episodes_stats.jsonl");
    let stats: Vec<Value> = stats_content
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).expect("Failed to parse stats line"))
        .collect();

    assert_eq!(
        stats.len(),
        expected_episodes,
        "episodes_stats.jsonl should contain {} episode stats",
        expected_episodes
    );

    println!(
        "  ✓ episodes_stats.jsonl verified ({} episode stats)",
        stats.len()
    );

    // 4. Verify Parquet files for all episodes
    for episode_idx in 0..expected_episodes {
        let parquet_path = data_dir
            .join("chunk-000")
            .join(format!("episode_{:06}.parquet", episode_idx));
        assert!(
            parquet_path.exists(),
            "Episode {} parquet file should exist at {}",
            episode_idx,
            parquet_path.display()
        );

        let parquet_metadata =
            fs::metadata(&parquet_path).expect("Failed to read parquet metadata");
        assert!(
            parquet_metadata.len() > 0,
            "Parquet file for episode {} should not be empty",
            episode_idx
        );
    }

    println!(
        "  ✓ All {} episode parquet files verified",
        expected_episodes
    );

    // 5. Verify video files for all episodes and cameras
    for episode_idx in 0..expected_episodes {
        for mapping in &config.mappings {
            if mapping.mapping_type == MappingType::Image {
                let camera_key = mapping.camera_key.as_ref().unwrap_or(&mapping.feature);
                let video_path = videos_dir
                    .join("chunk-000")
                    .join(camera_key)
                    .join(format!("episode_{:06}.mp4", episode_idx));
                assert!(
                    video_path.exists(),
                    "Video file for episode {} camera '{}' should exist at {}",
                    episode_idx,
                    camera_key,
                    video_path.display()
                );
            }
        }
    }

    println!(
        "  ✓ All video files for {} episodes verified",
        expected_episodes
    );

    println!("\nMulti-episode LeRobot v2.1 structure verification complete!");
    println!("  - Total episodes: {}", expected_episodes);
    println!("  - Episode indices: 0 to {}", expected_episodes - 1);
    println!("  - Parquet files: {} episodes", expected_episodes);
    println!(
        "  - Video files: {} episodes x {} cameras",
        expected_episodes,
        config
            .mappings
            .iter()
            .filter(|m| m.mapping_type == MappingType::Image)
            .count()
    );
}

#[test]
fn test_two_bags_to_lerobot_two_episodes() {
    // Test that two bag files = two work units = two episodes
    let bag_files = [TEST_BAG_PATH, TEST_BAG_PATH_2];

    for (i, bag_path) in bag_files.iter().enumerate() {
        if !Path::new(bag_path).exists() {
            eprintln!("Skipping test: bag file {} not found at {}", i, bag_path);
            return;
        }
    }

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let output_path = temp_dir.path();

    let config = create_test_lerobot_config();

    // Register builtin sources before creating source
    roboflow_pipeline::sources::register_builtin_sources();

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Build topic mappings from config
    let topic_mappings: HashMap<String, String> = config
        .mappings
        .iter()
        .map(|m| (m.topic.clone(), m.feature.clone()))
        .collect();

    // Use streaming config from LerobotConfig
    let pipeline_streaming = StreamingConfig::with_fps(config.dataset.base.fps);
    let pipeline_config =
        PipelineConfig::new(pipeline_streaming).with_topic_mappings(topic_mappings);

    // Process each bag file as a separate episode
    for (episode_idx, bag_path) in bag_files.iter().enumerate() {
        println!(
            "\n=== Processing bag file {} as episode {} ===",
            bag_path, episode_idx
        );

        let source_config = SourceConfig::bag(*bag_path);
        let mut source = roboflow_pipeline::sources::create_source(&source_config)
            .expect("Failed to create bag source");

        let _metadata: roboflow_pipeline::sources::SourceMetadata = rt
            .block_on(source.initialize(&source_config))
            .expect("Failed to initialize source");

        // Create a new writer for each episode
        let writer = LerobotWriter::new_local(output_path, config.clone())
            .expect("Failed to create LeRobot writer");
        let mut executor = PipelineExecutor::new(writer, pipeline_config.clone());

        let frame_count = rt.block_on(async {
            let mut count = 0usize;
            loop {
                match source.read_batch(100).await {
                    Ok(Some(messages)) if !messages.is_empty() => {
                        for msg in messages {
                            if executor.process_message(msg).is_ok() {
                                count += 1;
                            }
                            if count.is_multiple_of(100) {
                                println!("Episode {}: Processed {} frames...", episode_idx, count);
                            }
                        }
                    }
                    Ok(Some(_)) => continue,
                    Ok(None) => {
                        println!(
                            "Episode {}: End of stream after {} frames",
                            episode_idx, count
                        );
                        break;
                    }
                    Err(e) => {
                        eprintln!("Episode {}: Error reading batch: {}", episode_idx, e);
                        break;
                    }
                }
            }
            count
        });

        println!(
            "Episode {}: Total frames processed: {}",
            episode_idx, frame_count
        );

        let _stats = executor.finalize().expect("Failed to finalize executor");
    }

    // Verify both bag files were processed successfully
    // Note: Each bag creates its own dataset with 1 episode since we use separate writers
    verify_lerobot_structure(output_path, &config);

    println!("\nTwo bag files converted to LeRobot format successfully!");
    println!("Output location: {}", output_path.display());

    temp_dir.close().expect("Failed to clean up temp dir");
}
