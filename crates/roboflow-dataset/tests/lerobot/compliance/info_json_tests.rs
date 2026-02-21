// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

use crate::helpers::*;

#[test]
fn test_info_json_required_fields() {
    let temp_dir = build_lerobot_dataset(2, 3);
    let info = read_info_json(&temp_dir);

    assert!(info.get("codebase_version").is_some());
    assert_eq!(info["codebase_version"], "v2.1");
    assert!(info.get("robot_type").is_some());
    assert!(info.get("fps").is_some());
    assert!(info.get("total_episodes").is_some());
    assert!(info.get("total_frames").is_some());
    assert!(info.get("features").is_some());
}

#[test]
fn test_info_json_total_episodes() {
    let temp_dir = build_lerobot_dataset(5, 3);
    let info = read_info_json(&temp_dir);

    assert_eq!(info["total_episodes"], 5);
}

#[test]
fn test_info_json_total_frames_count() {
    let episodes = 3;
    let frames_per = 10;
    let temp_dir = build_lerobot_dataset(episodes, frames_per);
    let info = read_info_json(&temp_dir);

    let total_frames = info["total_frames"]
        .as_u64()
        .expect("total_frames should be u64");
    assert_eq!(total_frames, (episodes * frames_per) as u64);
}

#[test]
fn test_info_json_fps_value() {
    let fps = 30;
    let temp_dir = build_lerobot_dataset_with_config(lerobot_config_with_fps(fps), 1, 3);
    let info = read_info_json(&temp_dir);

    assert_eq!(info["fps"], fps);
}

#[test]
fn test_info_json_robot_type() {
    let config = lerobot_config_with_name("test_robot_dataset");
    let temp_dir = build_lerobot_dataset_with_config(config, 1, 3);
    let info = read_info_json(&temp_dir);

    assert!(info.get("robot_type").is_some());
}

#[test]
fn test_info_json_features_present() {
    let temp_dir = build_lerobot_dataset_with_state_and_action();
    let info = read_info_json(&temp_dir);

    let features = info.get("features").expect("features should exist");
    assert!(features.is_object());
}

#[test]
fn test_info_json_has_video_info() {
    let temp_dir = build_lerobot_dataset(1, 3);
    let info = read_info_json(&temp_dir);

    if let Some(video) = info.get("video") {
        assert!(video.get("fps").is_some());
        assert!(video.get("codec").is_some());
    }
}
