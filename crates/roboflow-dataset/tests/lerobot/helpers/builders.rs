// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

use roboflow_dataset::formats::common::config::DatasetBaseConfig;
use roboflow_dataset::formats::common::{AlignedFrame, DatasetWriter, ImageData};
use roboflow_dataset::formats::lerobot::LerobotWriterTrait;
use roboflow_dataset::formats::lerobot::config::{
    DatasetConfig, FlushingConfig, LerobotConfig, StreamingConfig, VideoConfig,
};
use roboflow_dataset::formats::lerobot::metadata::MetadataCollector;
use roboflow_dataset::formats::lerobot::writer::LerobotWriter;
use roboflow_dataset::testing::{FrameBuilder, generate_test_jpeg};

pub fn default_lerobot_config() -> LerobotConfig {
    LerobotConfig {
        dataset: DatasetConfig {
            base: DatasetBaseConfig {
                name: "test_dataset".to_string(),
                fps: 30,
                robot_type: Some("test".to_string()),
            },
            env_type: None,
        },
        mappings: vec![],
        video: VideoConfig::default(),
        annotation_file: None,
        flushing: FlushingConfig::default(),
        streaming: StreamingConfig::default(),
    }
}

pub fn lerobot_config_with_fps(fps: u32) -> LerobotConfig {
    let mut config = default_lerobot_config();
    config.dataset.base.fps = fps;
    config
}

pub fn lerobot_config_with_name(name: &str) -> LerobotConfig {
    let mut config = default_lerobot_config();
    config.dataset.base.name = name.to_string();
    config
}

pub fn build_lerobot_dataset(episodes: usize, frames_per_episode: usize) -> TempDir {
    build_lerobot_dataset_with_config(default_lerobot_config(), episodes, frames_per_episode)
}

pub fn build_lerobot_dataset_with_config(
    config: LerobotConfig,
    episodes: usize,
    frames_per_episode: usize,
) -> TempDir {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let mut writer =
        LerobotWriter::new_local(temp_dir.path(), config).expect("Failed to create writer");

    for ep in 0..episodes {
        writer.start_episode(Some(ep));

        for i in 0..frames_per_episode {
            let frame = FrameBuilder::new(i)
                .with_timestamp((i as u64) * 33_333_333)
                .add_state("observation.state", vec![i as f32, (i + 1) as f32])
                .add_action("action", vec![(i + 2) as f32])
                .build();
            writer.write_frame(&frame).expect("Failed to write frame");
        }

        writer
            .finish_episode(Some(ep))
            .expect("Failed to finish episode");
    }

    writer.finalize_with_config().expect("Failed to finalize");
    temp_dir
}

pub fn build_lerobot_dataset_with_camera(camera_key: &str, frames: usize) -> TempDir {
    build_lerobot_dataset_with_camera_and_fps(camera_key, frames, 30.0)
}

pub fn build_lerobot_dataset_with_camera_and_fps(
    camera_key: &str,
    frames: usize,
    fps: f64,
) -> TempDir {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config = lerobot_config_with_fps(fps as u32);

    let mut writer =
        LerobotWriter::new_local(temp_dir.path(), config).expect("Failed to create writer");

    writer.start_episode(Some(0));

    let ns_per_frame = (1_000_000_000.0 / fps) as u64;
    for i in 0..frames {
        let img_data = generate_test_jpeg(320, 240, i as u8);
        let image = ImageData::encoded(320, 240, img_data);

        let mut images = HashMap::new();
        images.insert(camera_key.to_string(), Arc::new(image));

        let frame = AlignedFrame {
            frame_index: i,
            timestamp: (i as u64) * ns_per_frame,
            images,
            states: vec![("observation.state".to_string(), vec![i as f32])]
                .into_iter()
                .collect(),
            actions: vec![("action".to_string(), vec![(i + 1) as f32])]
                .into_iter()
                .collect(),
            timestamps: HashMap::new(),
            audio: HashMap::new(),
        };

        writer.write_frame(&frame).expect("Failed to write frame");
    }

    writer
        .finish_episode(Some(0))
        .expect("Failed to finish episode");
    writer.finalize_with_config().expect("Failed to finalize");
    temp_dir
}

pub fn build_lerobot_dataset_with_cameras(camera_keys: &[&str], frames: usize) -> TempDir {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config = default_lerobot_config();

    let mut writer =
        LerobotWriter::new_local(temp_dir.path(), config).expect("Failed to create writer");

    writer.start_episode(Some(0));

    for i in 0..frames {
        let mut images = HashMap::new();
        for camera_key in camera_keys {
            let img_data = generate_test_jpeg(320, 240, i as u8);
            let image = ImageData::encoded(320, 240, img_data);
            images.insert(camera_key.to_string(), Arc::new(image));
        }

        let frame = AlignedFrame {
            frame_index: i,
            timestamp: (i as u64) * 33_333_333,
            images,
            states: vec![("observation.state".to_string(), vec![i as f32])]
                .into_iter()
                .collect(),
            actions: vec![("action".to_string(), vec![(i + 1) as f32])]
                .into_iter()
                .collect(),
            timestamps: HashMap::new(),
            audio: HashMap::new(),
        };

        writer.write_frame(&frame).expect("Failed to write frame");
    }

    writer
        .finish_episode(Some(0))
        .expect("Failed to finish episode");
    writer.finalize_with_config().expect("Failed to finalize");
    temp_dir
}

pub fn build_lerobot_dataset_from_states(states: Vec<Vec<f32>>) -> TempDir {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config = default_lerobot_config();

    let mut writer =
        LerobotWriter::new_local(temp_dir.path(), config).expect("Failed to create writer");

    writer.start_episode(Some(0));

    for (i, state) in states.iter().enumerate() {
        let frame = FrameBuilder::new(i)
            .with_timestamp((i as u64) * 33_333_333)
            .add_state("observation.state", state.clone())
            .add_action("action", vec![(i + 1) as f32])
            .build();
        writer.write_frame(&frame).expect("Failed to write frame");
    }

    writer
        .finish_episode(Some(0))
        .expect("Failed to finish episode");
    writer.finalize_with_config().expect("Failed to finalize");
    temp_dir
}

pub fn build_lerobot_dataset_with_state_and_action() -> TempDir {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config = default_lerobot_config();

    let mut writer =
        LerobotWriter::new_local(temp_dir.path(), config).expect("Failed to create writer");

    writer.start_episode(Some(0));

    for i in 0..10 {
        let frame = FrameBuilder::new(i)
            .with_timestamp((i as u64) * 33_333_333)
            .add_state(
                "observation.state",
                vec![i as f32, (i + 1) as f32, (i + 2) as f32],
            )
            .add_action("action", vec![(i as f32) * 0.5, ((i + 1) as f32) * 0.5])
            .build();
        writer.write_frame(&frame).expect("Failed to write frame");
    }

    writer
        .finish_episode(Some(0))
        .expect("Failed to finish episode");
    writer.finalize_with_config().expect("Failed to finalize");
    temp_dir
}

pub fn build_lerobot_dataset_with_state_shape(dim: usize) -> TempDir {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config = default_lerobot_config();

    let mut writer =
        LerobotWriter::new_local(temp_dir.path(), config).expect("Failed to create writer");

    writer.start_episode(Some(0));

    for i in 0..10 {
        let state: Vec<f32> = (0..dim).map(|j| (i * dim + j) as f32).collect();
        let frame = FrameBuilder::new(i)
            .with_timestamp((i as u64) * 33_333_333)
            .add_state("observation.state", state)
            .add_action("action", vec![(i + 1) as f32])
            .build();
        writer.write_frame(&frame).expect("Failed to write frame");
    }

    writer
        .finish_episode(Some(0))
        .expect("Failed to finish episode");
    writer.finalize_with_config().expect("Failed to finalize");
    temp_dir
}

pub fn build_metadata_with_episodes(
    episodes: usize,
    frames_per_episode: usize,
) -> (TempDir, LerobotConfig) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config = default_lerobot_config();

    let mut collector = MetadataCollector::new();

    for ep in 0..episodes {
        collector.add_episode(ep, frames_per_episode, vec![0]);
        collector.update_state_dim("observation.state".to_string(), 3);
        collector.update_state_dim("action".to_string(), 1);

        let mut stats = std::collections::HashMap::new();
        stats.insert(
            "observation.state".to_string(),
            roboflow_dataset::formats::common::parquet_base::FeatureStats {
                min: vec![0.0, 0.0, 0.0],
                max: vec![1.0, 1.0, 1.0],
                mean: vec![0.5, 0.5, 0.5],
                std: vec![0.1, 0.1, 0.1],
            },
        );
        collector.add_episode_stats(ep, stats);
    }

    collector
        .write_all(temp_dir.path(), &config)
        .expect("Failed to write metadata");
    (temp_dir, config)
}
