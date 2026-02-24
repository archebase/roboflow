// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Integration tests for DatasetPipelineExecutor

use roboflow_core::Result;
use roboflow_dataset::core::traits::{AlignedFrame, FormatWriter, WriterStats};
use roboflow_pipeline::{DatasetPipelineConfig, DatasetPipelineExecutor, EpisodeStrategy};
use std::any::Any;

/// Mock writer for testing pipeline execution
struct MockWriter {
    frames: Vec<AlignedFrame>,
    episodes_started: usize,
    episodes_finished: usize,
}

impl MockWriter {
    fn new() -> Self {
        Self {
            frames: Vec::new(),
            episodes_started: 0,
            episodes_finished: 0,
        }
    }
}

impl FormatWriter for MockWriter {
    fn write_frame(&mut self, frame: &AlignedFrame) -> Result<()> {
        self.frames.push(frame.clone());
        Ok(())
    }

    fn finalize(&mut self) -> Result<WriterStats> {
        Ok(WriterStats {
            frames_written: self.frames.len(),
            images_encoded: 0,
            state_records: 0,
            output_bytes: 0,
            duration_sec: 0.0,
        })
    }

    fn frame_count(&self) -> usize {
        self.frames.len()
    }

    fn start_episode(&mut self, _task_index: Option<usize>) -> Result<usize> {
        self.episodes_started += 1;
        Ok(self.episodes_started - 1)
    }

    fn finish_episode(&mut self) -> Result<roboflow_dataset::core::stats::EpisodeStats> {
        self.episodes_finished += 1;
        Ok(roboflow_dataset::core::stats::EpisodeStats::default())
    }

    fn supports_episodes(&self) -> bool {
        true
    }

    fn format_name(&self) -> &'static str {
        "mock"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[test]
fn test_executor_with_sequential_policy() {
    let writer = MockWriter::new();
    let config = DatasetPipelineConfig::with_fps(30);
    let executor = DatasetPipelineExecutor::sequential(writer, config);

    assert_eq!(executor.policy_name(), "sequential");
}

#[test]
fn test_executor_with_parallel_policy() {
    let writer = MockWriter::new();
    let config = DatasetPipelineConfig::with_fps(30);
    let executor = DatasetPipelineExecutor::parallel(writer, config, 4);

    assert_eq!(executor.policy_name(), "parallel");
}

#[test]
fn test_config_with_fps() {
    let config = DatasetPipelineConfig::with_fps(60);
    assert_eq!(config.streaming.fps, 60);
}

#[test]
fn test_config_with_max_frames() {
    let config = DatasetPipelineConfig::with_fps(30).with_max_frames(1000);

    assert_eq!(config.max_frames, Some(1000));
}

#[test]
fn test_config_with_topic_mapping() {
    let config = DatasetPipelineConfig::with_fps(30)
        .with_topic_mapping("/camera/image", "observation.images.camera");

    assert_eq!(
        config.topic_mappings.get("/camera/image"),
        Some(&"observation.images.camera".to_string())
    );
}

#[test]
fn test_episode_strategy_single() {
    let strategy = EpisodeStrategy::Single;
    match strategy {
        EpisodeStrategy::Single => {}
        _ => panic!("Expected Single variant"),
    }
}

#[test]
fn test_episode_strategy_gap_based() {
    let strategy = EpisodeStrategy::GapBased {
        threshold_ns: 1_000_000_000,
    };
    match strategy {
        EpisodeStrategy::GapBased { threshold_ns } => {
            assert_eq!(threshold_ns, 1_000_000_000);
        }
        _ => panic!("Expected GapBased variant"),
    }
}

#[test]
fn test_episode_strategy_frame_count() {
    let strategy = EpisodeStrategy::FrameCount { max_frames: 100 };
    match strategy {
        EpisodeStrategy::FrameCount { max_frames } => {
            assert_eq!(max_frames, 100);
        }
        _ => panic!("Expected FrameCount variant"),
    }
}

#[test]
fn test_config_default() {
    let config: DatasetPipelineConfig = Default::default();
    assert_eq!(config.streaming.fps, 30);
    assert!(config.max_frames.is_none());
}

#[test]
fn test_get_feature_name_with_mapping() {
    let config = DatasetPipelineConfig::with_fps(30).with_topic_mapping("/topic", "mapped.feature");

    let feature = config.get_feature_name("/topic");
    assert_eq!(feature, "mapped.feature");
}

#[test]
fn test_get_feature_name_without_mapping() {
    let config = DatasetPipelineConfig::with_fps(30);

    let feature = config.get_feature_name("/camera/image_raw");
    assert_eq!(feature, "camera.image_raw");
}

#[test]
fn test_get_feature_name_with_leading_slash() {
    let config = DatasetPipelineConfig::with_fps(30);

    let feature = config.get_feature_name("/topic");
    assert_eq!(feature, "topic");
}
