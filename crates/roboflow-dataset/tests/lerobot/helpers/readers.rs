// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

pub fn read_info_json(dir: &TempDir) -> Value {
    let info_path = dir.path().join("meta/info.json");
    let content = fs::read_to_string(&info_path)
        .expect(&format!("Failed to read info.json at {:?}", info_path));
    serde_json::from_str(&content).expect("Failed to parse info.json")
}

pub fn read_info_json_from_path(path: &Path) -> Value {
    let info_path = path.join("meta/info.json");
    let content = fs::read_to_string(&info_path)
        .expect(&format!("Failed to read info.json at {:?}", info_path));
    serde_json::from_str(&content).expect("Failed to parse info.json")
}

pub fn read_episodes_jsonl(dir: &TempDir) -> Vec<Value> {
    let path = dir.path().join("meta/episodes.jsonl");
    if !path.exists() {
        return vec![];
    }

    let content = fs::read_to_string(&path).expect("Failed to read episodes.jsonl");
    content
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).expect("Failed to parse episode line"))
        .collect()
}

pub fn read_tasks_jsonl(dir: &TempDir) -> Vec<Value> {
    let path = dir.path().join("meta/tasks.jsonl");
    if !path.exists() {
        return vec![];
    }

    let content = fs::read_to_string(&path).expect("Failed to read tasks.jsonl");
    content
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).expect("Failed to parse task line"))
        .collect()
}

pub fn read_episodes_stats_jsonl(dir: &TempDir) -> Vec<Value> {
    let path = dir.path().join("meta/episodes_stats.jsonl");
    if !path.exists() {
        return vec![];
    }

    let content = fs::read_to_string(&path).expect("Failed to read episodes_stats.jsonl");
    content
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).expect("Failed to parse episode stats line"))
        .collect()
}

#[derive(Debug, Clone)]
pub struct EpisodeStatsData {
    pub episode_index: usize,
    pub stats: serde_json::Map<String, Value>,
}

pub fn read_episode_stats(dir: &TempDir, episode: usize) -> Option<EpisodeStatsData> {
    let stats = read_episodes_stats_jsonl(dir);

    for stat in stats {
        if let Some(idx) = stat.get("episode_index").and_then(|v| v.as_u64()) {
            if idx as usize == episode {
                return Some(EpisodeStatsData {
                    episode_index: idx as usize,
                    stats: stat
                        .get("stats")
                        .and_then(|v| v.as_object().cloned())
                        .unwrap_or_default(),
                });
            }
        }
    }

    None
}

pub fn count_total_frames(dir: &TempDir) -> usize {
    let info = read_info_json(dir);
    info.get("total_frames")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize
}

pub fn list_parquet_files(dir: &TempDir) -> Vec<PathBuf> {
    let mut files = vec![];
    let data_dir = dir.path().join("data");

    if let Ok(entries) = fs::read_dir(&data_dir) {
        for entry in entries.flatten() {
            if entry.path().is_dir() {
                if let Ok(sub_entries) = fs::read_dir(entry.path()) {
                    for sub_entry in sub_entries.flatten() {
                        let path = sub_entry.path();
                        if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                            files.push(path);
                        }
                    }
                }
            }
        }
    }

    files
}

pub fn list_video_files(dir: &TempDir) -> Vec<PathBuf> {
    let mut files = vec![];
    let videos_dir = dir.path().join("videos");

    if let Ok(entries) = fs::read_dir(&videos_dir) {
        for entry in entries.flatten() {
            if entry.path().is_dir() {
                if let Ok(chunk_entries) = fs::read_dir(entry.path()) {
                    for chunk_entry in chunk_entries.flatten() {
                        if chunk_entry.path().is_dir() {
                            if let Ok(camera_entries) = fs::read_dir(chunk_entry.path()) {
                                for camera_entry in camera_entries.flatten() {
                                    let path = camera_entry.path();
                                    if path.extension().map(|e| e == "mp4").unwrap_or(false) {
                                        files.push(path);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    files
}

pub fn find_first_video(dir: &TempDir) -> PathBuf {
    let videos = list_video_files(dir);
    videos.into_iter().next().expect("No video files found")
}

pub fn chunk_index_for_episode(episode: usize) -> usize {
    episode / 500
}

pub fn episode_parquet_path(dir: &TempDir, episode: usize) -> PathBuf {
    let chunk = chunk_index_for_episode(episode);
    dir.path().join(format!(
        "data/chunk-{:03}/episode_{:06}.parquet",
        chunk, episode
    ))
}
