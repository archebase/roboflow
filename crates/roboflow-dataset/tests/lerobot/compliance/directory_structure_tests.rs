// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

use crate::helpers::*;

#[test]
fn test_lerobot_v2_1_directory_structure() {
    let temp_dir = build_lerobot_dataset(1, 3);

    assert!(
        temp_dir.path().join("data/chunk-000").exists(),
        "Missing data/chunk-000 directory"
    );
    assert!(
        temp_dir.path().join("meta").exists(),
        "Missing meta directory"
    );
}

#[test]
fn test_lerobot_v2_1_metadata_files_exist() {
    let temp_dir = build_lerobot_dataset(2, 3);

    assert!(temp_dir.path().join("meta/info.json").exists());
    assert!(temp_dir.path().join("meta/episodes.jsonl").exists());
    assert!(temp_dir.path().join("meta/episodes_stats.jsonl").exists());
}

#[test]
fn test_episode_parquet_file_exists() {
    let temp_dir = build_lerobot_dataset(1, 3);

    let parquet_path = temp_dir
        .path()
        .join("data/chunk-000/episode_000000.parquet");
    assert!(
        parquet_path.exists(),
        "Expected parquet at {:?}",
        parquet_path
    );
}

#[test]
fn test_data_chunk_directory_created() {
    let temp_dir = build_lerobot_dataset(1, 1);

    let chunk_dir = temp_dir.path().join("data/chunk-000");
    assert!(chunk_dir.exists(), "Chunk directory should exist");
    assert!(chunk_dir.is_dir(), "Chunk path should be a directory");
}
