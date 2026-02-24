// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

use roboflow_dataset::{
    formats::common::DatasetBaseConfig,
    formats::lerobot::{LerobotConfig, LerobotWriterConfig, create_lerobot_writer},
};
use roboflow_distributed::{batch::BatchSpec, worker::ProcessingResult, worker::WorkerConfig};
use roboflow_pipeline::{DatasetPipelineConfig, DatasetPipelineExecutor, EpisodeStrategy};

fn test_output_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("failed to create temp dir")
}

fn test_lerobot_config() -> LerobotConfig {
    LerobotConfig {
        dataset: roboflow_dataset::formats::lerobot::config::DatasetConfig {
            base: DatasetBaseConfig {
                name: "test_pipeline_dataset".to_string(),
                fps: 30,
                robot_type: None,
            },
            env_type: None,
        },
        mappings: vec![],
        video: roboflow_dataset::formats::lerobot::config::VideoConfig::default(),
        annotation_file: None,
        flushing: Default::default(),
        streaming: Default::default(),
    }
}

#[test]
fn pipeline_executor_sequential_policy() {
    let output_dir = test_output_dir();
    let writer_config = LerobotWriterConfig::new(
        output_dir.path().to_string_lossy().to_string(),
        test_lerobot_config(),
    );
    let writer = create_lerobot_writer(&writer_config)
        .expect("writer create")
        .writer;

    let pipeline_config = DatasetPipelineConfig::with_fps(30);
    let executor = DatasetPipelineExecutor::sequential(writer, pipeline_config);
    assert_eq!(executor.policy_name(), "sequential");
}

#[test]
fn pipeline_executor_parallel_policy() {
    let output_dir = test_output_dir();
    let writer_config = LerobotWriterConfig::new(
        output_dir.path().to_string_lossy().to_string(),
        test_lerobot_config(),
    );
    let writer = create_lerobot_writer(&writer_config)
        .expect("writer create")
        .writer;

    let pipeline_config = DatasetPipelineConfig::with_fps(30);
    let executor = DatasetPipelineExecutor::parallel(writer, pipeline_config, 4);
    assert_eq!(executor.policy_name(), "parallel");
}

#[test]
fn pipeline_config_builders_work() {
    let config = DatasetPipelineConfig::with_fps(60)
        .with_max_frames(1000)
        .with_topic_mapping("/camera/image", "observation.images.camera");

    assert_eq!(config.streaming.fps, 60);
    assert_eq!(config.max_frames, Some(1000));
    assert_eq!(
        config.topic_mappings.get("/camera/image"),
        Some(&"observation.images.camera".to_string())
    );
}

#[test]
fn episode_strategy_variants_work() {
    assert!(matches!(EpisodeStrategy::Single, EpisodeStrategy::Single));
    assert!(matches!(
        EpisodeStrategy::GapBased { threshold_ns: 1 },
        EpisodeStrategy::GapBased { .. }
    ));
    assert!(matches!(
        EpisodeStrategy::FrameCount { max_frames: 10 },
        EpisodeStrategy::FrameCount { .. }
    ));
}

#[test]
fn processing_result_variants_work() {
    let ok = ProcessingResult::Success {
        episode_index: 1,
        frame_count: 10,
        episode_stats: None,
    };
    let failed = ProcessingResult::Failed {
        error: "boom".to_string(),
    };
    let cancelled = ProcessingResult::Cancelled;

    assert!(matches!(ok, ProcessingResult::Success { .. }));
    assert!(matches!(failed, ProcessingResult::Failed { .. }));
    assert!(matches!(cancelled, ProcessingResult::Cancelled));
}

#[test]
fn worker_config_builder_works() {
    let config = WorkerConfig::new()
        .with_max_concurrent_jobs(5)
        .with_poll_interval(std::time::Duration::from_secs(10))
        .with_heartbeat_interval(std::time::Duration::from_secs(60))
        .with_output_prefix("test_output/");

    assert_eq!(config.max_concurrent_jobs, 5);
    assert_eq!(config.poll_interval.as_secs(), 10);
    assert_eq!(config.heartbeat_interval.as_secs(), 60);
    assert_eq!(config.output_prefix, "test_output/");
}

#[test]
fn batch_spec_builder_works() {
    let spec = BatchSpec::new(
        "test-batch",
        vec!["file:///test/input.bag".to_string()],
        "file:///tmp/output".to_string(),
    );

    assert_eq!(spec.metadata.name, "test-batch");
    assert_eq!(spec.spec.sources.len(), 1);
    assert_eq!(spec.spec.output, "file:///tmp/output");
}
