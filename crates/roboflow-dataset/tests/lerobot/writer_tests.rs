// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

use roboflow_dataset::formats::common::DatasetWriter;
use roboflow_dataset::formats::lerobot::LerobotWriterTrait;
use roboflow_dataset::formats::lerobot::writer::LerobotWriter;
use roboflow_dataset::testing::FrameBuilder;
use tempfile::tempdir;

use crate::helpers::*;

#[test]
fn test_writer_creates_directory_structure() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let mut writer = LerobotWriter::new_local(temp_dir.path(), default_lerobot_config())
        .expect("Failed to create writer");

    writer
        .start_episode(Some(0))
        .expect("Failed to start episode");
    let frame = FrameBuilder::new(0)
        .add_state("observation.state", vec![0.0])
        .build();
    writer.write_frame(&frame).expect("Failed to write frame");

    assert!(temp_dir.path().join("data/chunk-000").exists());
    assert!(temp_dir.path().join("meta").exists());
}

#[test]
fn test_writer_start_episode() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let mut writer = LerobotWriter::new_local(temp_dir.path(), default_lerobot_config())
        .expect("Failed to create writer");

    writer
        .start_episode(Some(0))
        .expect("Failed to start episode");
    let frame = FrameBuilder::new(0)
        .add_state("observation.state", vec![0.0])
        .build();
    writer.write_frame(&frame).expect("Failed to write frame");
    writer
        .finish_episode(Some(0))
        .expect("Failed to finish episode");

    let stats = writer.finalize_with_config().expect("Failed to finalize");
    assert!(stats.frames_written > 0);
}

#[test]
fn test_writer_write_multiple_frames() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let mut writer = LerobotWriter::new_local(temp_dir.path(), default_lerobot_config())
        .expect("Failed to create writer");

    writer
        .start_episode(Some(0))
        .expect("Failed to start episode");

    for i in 0..10 {
        let frame = FrameBuilder::new(i)
            .add_state("observation.state", vec![i as f32])
            .build();
        writer.write_frame(&frame).expect("Failed to write frame");
    }

    writer
        .finish_episode(Some(0))
        .expect("Failed to finish episode");
    let stats = writer.finalize_with_config().expect("Failed to finalize");

    assert_eq!(stats.frames_written, 10);
}

#[test]
fn test_writer_multiple_episodes() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let mut writer = LerobotWriter::new_local(temp_dir.path(), default_lerobot_config())
        .expect("Failed to create writer");

    for ep in 0..3 {
        writer
            .start_episode(Some(ep))
            .expect("Failed to start episode");

        for i in 0..5 {
            let frame = FrameBuilder::new(i)
                .add_state("observation.state", vec![i as f32])
                .build();
            writer.write_frame(&frame).expect("Failed to write frame");
        }

        writer
            .finish_episode(Some(ep))
            .expect("Failed to finish episode");
    }

    let stats = writer.finalize_with_config().expect("Failed to finalize");
    assert_eq!(stats.frames_written, 15);
}

#[test]
fn test_writer_dataset_writer_trait() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let mut writer: Box<dyn DatasetWriter> = Box::new(
        LerobotWriter::new_local(temp_dir.path(), default_lerobot_config())
            .expect("Failed to create writer"),
    );

    let frame = FrameBuilder::new(0)
        .add_state("observation.state", vec![0.0])
        .build();

    writer.write_frame(&frame).expect("Failed to write frame");
    let stats = writer.finalize().expect("Failed to finalize");

    assert!(stats.frames_written > 0);
}

#[test]
fn test_writer_handles_episode_gap() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let mut writer = LerobotWriter::new_local(temp_dir.path(), default_lerobot_config())
        .expect("Failed to create writer");

    writer
        .start_episode(Some(0))
        .expect("Failed to start episode");
    let frame = FrameBuilder::new(0)
        .add_state("observation.state", vec![0.0])
        .build();
    writer.write_frame(&frame).expect("Failed to write frame");
    writer
        .finish_episode(Some(0))
        .expect("Failed to finish episode");

    writer
        .start_episode(Some(5))
        .expect("Failed to start episode");
    let frame = FrameBuilder::new(0)
        .add_state("observation.state", vec![1.0])
        .build();
    writer.write_frame(&frame).expect("Failed to write frame");
    writer
        .finish_episode(Some(5))
        .expect("Failed to finish episode");

    writer.finalize_with_config().expect("Failed to finalize");

    assert!(
        temp_dir
            .path()
            .join("data/chunk-000/episode_000000.parquet")
            .exists()
    );
}

#[test]
fn test_writer_register_task() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let mut writer = LerobotWriter::new_local(temp_dir.path(), default_lerobot_config())
        .expect("Failed to create writer");

    let task_idx0 = writer.register_task("pick_object".to_string());
    let task_idx1 = writer.register_task("place_object".to_string());
    let task_idx2 = writer.register_task("pick_object".to_string());

    assert_eq!(task_idx0, 0);
    assert_eq!(task_idx1, 1);
    assert_eq!(task_idx2, 0);
}

#[test]
fn test_writer_config_fps() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let config = lerobot_config_with_fps(60);

    let writer =
        LerobotWriter::new_local(temp_dir.path(), config).expect("Failed to create writer");

    drop(writer);
}

#[test]
fn test_writer_finalize_returns_stats() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let mut writer = LerobotWriter::new_local(temp_dir.path(), default_lerobot_config())
        .expect("Failed to create writer");

    writer
        .start_episode(Some(0))
        .expect("Failed to start episode");
    for i in 0..5 {
        let frame = FrameBuilder::new(i)
            .add_state("observation.state", vec![i as f32])
            .build();
        writer.write_frame(&frame).expect("Failed to write frame");
    }
    writer
        .finish_episode(Some(0))
        .expect("Failed to finish episode");

    let stats = writer.finalize_with_config().expect("Failed to finalize");

    assert_eq!(stats.frames_written, 5);
    assert!(stats.duration_sec > 0.0);
}
