// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

use roboflow_dataset::formats::lerobot::metadata::MetadataCollector;

use crate::helpers::*;

#[test]
fn test_metadata_collector_new() {
    let collector = MetadataCollector::new();

    assert!(collector.episodes.is_empty());
    assert!(collector.tasks.is_empty());
    assert!(collector.episode_stats.is_empty());
}

#[test]
fn test_metadata_collector_add_episode() {
    let mut collector = MetadataCollector::new();

    collector.add_episode(0, 100, vec![0]);
    collector.add_episode(1, 50, vec![1]);

    assert_eq!(collector.episodes.len(), 2);
    assert_eq!(collector.total_frames, 150);
}

#[test]
fn test_metadata_collector_register_task() {
    let mut collector = MetadataCollector::new();

    let idx0 = collector.register_task("task_a".to_string());
    let idx1 = collector.register_task("task_b".to_string());
    let idx2 = collector.register_task("task_a".to_string());

    assert_eq!(idx0, 0);
    assert_eq!(idx1, 1);
    assert_eq!(idx2, 0);
    assert_eq!(collector.tasks.len(), 2);
}

#[test]
fn test_metadata_collector_update_image_shape() {
    let mut collector = MetadataCollector::new();

    collector.update_image_shape("camera_0".to_string(), 640, 480);
    collector.update_image_shape("camera_1".to_string(), 1280, 720);

    assert_eq!(collector.image_shapes.len(), 2);
    assert_eq!(collector.image_shapes.get("camera_0"), Some(&(640, 480)));
}

#[test]
fn test_metadata_collector_update_state_dim() {
    let mut collector = MetadataCollector::new();

    collector.update_state_dim("observation.state".to_string(), 7);
    collector.update_state_dim("action".to_string(), 6);

    assert_eq!(collector.state_dims.len(), 2);
    assert_eq!(collector.state_dims.get("observation.state"), Some(&7));
}

#[test]
fn test_info_json_generation() {
    let (temp_dir, _config) = build_metadata_with_episodes(2, 50);

    let info = read_info_json(&temp_dir);

    assert_eq!(info["codebase_version"], "v2.1");
    assert_eq!(info["total_episodes"], 2);
    assert_eq!(info["total_frames"], 100);
}

#[test]
fn test_episodes_jsonl_generation() {
    let (temp_dir, _config) = build_metadata_with_episodes(2, 50);

    let episodes = read_episodes_jsonl(&temp_dir);

    assert_eq!(episodes.len(), 2);

    for ep in &episodes {
        assert!(ep.get("episode_index").is_some());
        assert!(ep.get("length").is_some());
        assert!(ep.get("tasks").is_some());
    }
}

#[test]
fn test_tasks_jsonl_deduplication() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config = default_lerobot_config();

    let mut collector = MetadataCollector::new();

    let task0 = collector.register_task("pick".to_string());
    let task1 = collector.register_task("place".to_string());
    let task2 = collector.register_task("pick".to_string());

    collector.add_episode(0, 10, vec![task0, task1]);
    collector.add_episode(1, 10, vec![task2]);

    collector
        .write_all(temp_dir.path(), &config)
        .expect("Failed to write");

    let tasks = read_tasks_jsonl(&temp_dir);
    assert_eq!(tasks.len(), 2);
}

#[test]
fn test_metadata_files_exist_after_write() {
    let (temp_dir, _config) = build_metadata_with_episodes(1, 10);

    assert!(temp_dir.path().join("meta/info.json").exists());
    assert!(temp_dir.path().join("meta/episodes.jsonl").exists());
}

#[test]
fn test_episodes_stats_jsonl_generated() {
    let (temp_dir, _config) = build_metadata_with_episodes(2, 10);

    let stats = read_episodes_stats_jsonl(&temp_dir);
    assert_eq!(stats.len(), 2);
}
