// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

use serde_json::Value;
use std::fs;
use tempfile::TempDir;

pub fn read_info_json(dir: &TempDir) -> Value {
    let info_path = dir.path().join("meta/info.json");
    let content = fs::read_to_string(&info_path)
        .unwrap_or_else(|_| panic!("Failed to read info.json at {:?}", info_path));
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
