# ADR-005: LeRobot v2.1 Test Coverage Requirements

**Author**: Sisyphus (AI Agent)  
**Date**: 2026-02-21  
**Status**: Proposed  
**Related**: [ADR-004](./adr-004-dataset-testing-strategy.md), [LeRobot v2.1 Spec](./lerobot-v2.1-specification.md)

## Context

Following the execution of `cargo llvm-cov --package roboflow-dataset`, we have identified critical coverage gaps in the LeRobot format implementation. Current test coverage for `roboflow-dataset` shows:

- **638 unit tests passing** (excellent baseline)
- **13 integration tests passing**
- **Overall line coverage: 55.66%** (needs improvement)

However, several **critical LeRobot v2.1 compliance modules have dangerously low coverage**, risking format incompatibility and silent data corruption.

## Current Coverage Analysis

### Critical Low-Coverage Modules (< 40%)

| Module | Line Coverage | Functions | Risk Level | Impact |
|--------|---------------|-----------|------------|---------|
| `formats/lerobot/format_writer_impl.rs` | **6.67%** | 12.50% | 🔴 CRITICAL | Format trait implementation untested |
| `sources/decode.rs` | **0.00%** | 0.00% | 🔴 CRITICAL | Message decoding untested |
| `formats/lerobot/trait_impl.rs` | **12.50%** | 20.00% | 🔴 CRITICAL | DatasetWriter trait untested |
| `formats/lerobot/upload.rs` | **17.66%** | 37.88% | 🔴 CRITICAL | Cloud upload logic untested |
| `media/video/composer.rs` | **16.08%** | 21.05% | 🔴 CRITICAL | Video segment merging untested |
| `media/video/pipeline/parallel.rs` | **3.60%** | 8.70% | 🔴 CRITICAL | Parallel video encoding untested |
| `formats/lerobot/metadata.rs` | **32.12%** | 30.56% | 🟡 HIGH | Metadata generation partially tested |
| `formats/lerobot/writer_impl.rs` | **34.37%** | 32.58% | 🟡 HIGH | Core writer implementation partially tested |
| `formats/alignment/buffer.rs` | **17.63%** | 32.35% | 🟡 HIGH | Frame alignment buffer untested |

### High-Coverage Modules (> 85%) ✅

| Module | Line Coverage | Functions | Notes |
|--------|---------------|-----------|-------|
| `formats/lerobot/writer/frame.rs` | **100.00%** | 100.00% | Frame structure fully tested |
| `formats/lerobot/writer/stats.rs` | **100.00%** | 100.00% | Statistics calculation fully tested |
| `formats/lerobot/writer/parquet.rs` | **88.86%** | 81.82% | Parquet writing well tested |
| `formats/lerobot/config.rs` | **97.15%** | 95.92% | Configuration parsing well tested |
| `formats/lerobot/episode.rs` | **91.85%** | 89.47% | Episode tracking well tested |

## Required Tests for LeRobot v2.1 Correctness

> **Note on helpers**: Use existing test utilities in
> `crates/roboflow-dataset/src/testing.rs` (e.g., `FrameBuilder`, `MockSource`,
> `InMemoryWriter`, `generate_test_jpeg`) and shared fixtures in
> `crates/roboflow-dataset/tests/fixtures/mod.rs`. The top-level workspace tests
> also contain LeRobot-specific examples (e.g., `tests/video_encoding_validation.rs`).

### 1. Format Compliance Tests (CRITICAL)

**Test helper functions to define in the test module** (using `FrameBuilder`, `MockSource`, and `LerobotWriter`):

```rust
// Helpers to implement once and reuse across tests
fn build_lerobot_dataset(episodes: usize, frames_per_episode: usize) -> TempDir { /* ... */ }
fn build_lerobot_dataset_with_camera(camera_key: &str, frames: usize) -> TempDir { /* ... */ }
fn build_lerobot_dataset_with_cameras(camera_keys: &[&str], frames: usize) -> TempDir { /* ... */ }
fn build_lerobot_dataset_with_camera_and_fps(camera_key: &str, frames: usize, fps: f64) -> TempDir { /* ... */ }
fn build_lerobot_dataset_from_states(states: Vec<Vec<f32>>) -> TempDir { /* ... */ }
fn build_lerobot_dataset_with_state_and_action() -> TempDir { /* ... */ }
fn build_lerobot_dataset_with_state_shape(dim: usize) -> TempDir { /* ... */ }
fn read_info_json(dir: &TempDir) -> serde_json::Value { /* ... */ }
fn read_episode_stats(dir: &TempDir, episode: usize) -> EpisodeStats { /* ... */ }
fn read_frame_indices(dir: &TempDir, episode: usize) -> Vec<i64> { /* ... */ }
fn read_global_indices(dir: &TempDir) -> Vec<i64> { /* ... */ }
fn read_video_references(dir: &TempDir, camera_key: &str) -> Vec<String> { /* ... */ }
fn find_first_video(dir: &TempDir) -> PathBuf { /* ... */ }
fn probe_video_properties(path: &Path) -> Option<VideoProperties> { /* reuse tests/video_encoding_validation.rs */ }
fn load_parquet_schema(path: &Path) -> Vec<String> { /* use polars or parquet crate */ }
fn create_frame_with_values(ts: f64, state: Vec<f32>, action: Vec<f32>) -> AlignedFrame { /* ... */ }
fn write_frames_to_parquet(frames: &[AlignedFrame]) -> TempDir { /* ... */ }
fn read_frames_from_parquet(dir: &TempDir) -> Vec<AlignedFrame> { /* ... */ }
fn create_writer_to_readonly_dir() -> LerobotWriter { /* reuse dataset_writer_error_tests.rs */ }
fn create_writer_with_limited_space() -> LerobotWriter { /* reuse dataset_writer_error_tests.rs */ }
```

#### 1.1 Directory Structure Validation
```rust
// crates/roboflow-dataset/tests/lerobot/compliance/directory_structure_tests.rs
#[test]
fn test_lerobot_v2_1_directory_structure() {
    let temp_dir = tempdir().unwrap();
    let mut writer = LerobotWriter::new_local(&temp_dir, LerobotConfig::default()).unwrap();
    
    // Write test data
    writer.start_episode(Some(0));
    writer.write_frame(
        &FrameBuilder::new(0)
            .add_state("observation.state", vec![0.0])
            .build(),
    )
    .unwrap();
    writer.finalize().unwrap();
    
    // Verify LeRobot v2.1 required directories exist
    assert!(temp_dir.path().join("data/chunk-000").exists(), 
            "Missing data/chunk-000 directory");
    assert!(temp_dir.path().join("videos/chunk-000").exists(), 
            "Missing videos/chunk-000 directory");
    assert!(temp_dir.path().join("meta").exists(), 
            "Missing meta directory");
}

#[test]
fn test_lerobot_v2_1_metadata_files_exist() {
    // Helper defined in the test module: build a tiny dataset on disk.
    let temp_dir = build_lerobot_dataset(2, 3);
    
    // All required metadata files per v2.1 spec
    assert!(temp_dir.path().join("meta/info.json").exists());
    assert!(temp_dir.path().join("meta/episodes.jsonl").exists());
    assert!(temp_dir.path().join("meta/episodes_stats.jsonl").exists());
    assert!(temp_dir.path().join("meta/tasks.jsonl").exists());
}

#[test]
fn test_episode_file_naming_convention() {
    // Episode 0 → episode_000000.parquet
    // Episode 500 → episode_000500.parquet (chunk-001)
    // Prefer path-level unit tests for chunking (episode_index -> chunk_index)
    // rather than generating 501 full episodes in an integration test.
    let temp_dir = build_lerobot_dataset(2, 3);
    
    assert!(temp_dir.path().join("data/chunk-000/episode_000000.parquet").exists());
    assert!(temp_dir.path().join("data/chunk-001/episode_000500.parquet").exists());
}
```

#### 1.2 info.json Schema Validation
```rust
// crates/roboflow-dataset/tests/lerobot/compliance/info_json_tests.rs
#[test]
fn test_info_json_required_fields() {
    let temp_dir = build_lerobot_dataset(2, 3);
    let info: serde_json::Value = read_info_json(&temp_dir);
    
    // Required per v2.1 spec
    assert!(info.get("codebase_version").is_some());
    assert_eq!(info["codebase_version"], "v2.1");
    assert!(info.get("robot_type").is_some());
    assert!(info.get("fps").is_some());
    assert!(info.get("total_episodes").is_some());
    assert!(info.get("total_frames").is_some());
    assert!(info.get("total_tasks").is_some());
    assert!(info.get("total_videos").is_some());
    assert!(info.get("splits").is_some());
    assert!(info.get("features").is_some());
}

#[test]
fn test_info_json_feature_schema() {
    let temp_dir = build_lerobot_dataset_with_camera("observation.images.cam_left", 3);
    let info = read_info_json(&temp_dir);
    let features = info["features"].as_object().unwrap();
    
    // Numeric feature schema
    let state = &features["observation.state"];
    assert_eq!(state["dtype"], "float32");
    assert!(state["shape"].as_array().unwrap().len() > 0);
    
    // Video feature schema
    let camera = &features["observation.images.cam_left"];
    assert_eq!(camera["dtype"], "video");
    let shape = camera["shape"].as_array().unwrap();
    assert_eq!(shape.len(), 3);
    assert!(shape.iter().all(|v| v.as_u64().unwrap_or(0) > 0));
    assert!(camera["info"]["video.fps"].is_number());
    assert!(camera["info"]["video.codec"].is_string());
}

#[test]
fn test_info_json_total_counts_are_accurate() {
    let temp_dir = build_lerobot_dataset(5, 3);
    let info = read_info_json(&temp_dir);
    
    assert_eq!(info["total_episodes"], 5);
    // Verify total_frames matches actual frame count
    let actual_frames = count_total_frames(&temp_dir);
    assert_eq!(info["total_frames"].as_i64().unwrap(), actual_frames as i64);
}
```

#### 1.3 Chunking Compliance (500 episodes per chunk)
```rust
// crates/roboflow-dataset/tests/lerobot/compliance/chunking_tests.rs
#[test]
fn test_chunk_boundary_at_500_episodes() {
    // Prefer unit-level path calculations to avoid heavy dataset creation.
    let temp_dir = build_lerobot_dataset(2, 3);
    
    // Episode 499 should be in chunk-000
    assert!(temp_dir.path().join("data/chunk-000/episode_000499.parquet").exists());
    
    // Episode 500 should be in chunk-001
    assert!(temp_dir.path().join("data/chunk-001/episode_000500.parquet").exists());
}

#[test]
fn test_video_chunking_matches_data_chunking() {
    let temp_dir = build_lerobot_dataset_with_cameras(&["cam_left", "cam_right"], 3);
    
    // Videos should follow same chunking as data
    assert!(temp_dir.path().join("videos/chunk-000/cam_left/episode_000499.mp4").exists());
    assert!(temp_dir.path().join("videos/chunk-001/cam_left/episode_000500.mp4").exists());
}
```

### 2. Parquet Format Compliance Tests

#### 2.1 Column Schema Validation
```rust
// crates/roboflow-dataset/tests/lerobot/compliance/parquet_schema_tests.rs
#[test]
fn test_parquet_has_required_columns() {
    let temp_dir = build_lerobot_dataset(1, 3);
    let parquet_path = temp_dir.path().join("data/chunk-000/episode_000000.parquet");
    
    // Use polars (already a dependency) or add a parquet dev-dependency
    // if direct schema inspection is required.
    let column_names = load_parquet_schema(&parquet_path);
    
    // Required columns per v2.1 spec
    assert!(column_names.iter().any(|c| c == "timestamp"));
    assert!(column_names.iter().any(|c| c == "episode_index"));
    assert!(column_names.iter().any(|c| c == "frame_index"));
    assert!(column_names.iter().any(|c| c == "index"));
    assert!(column_names.iter().any(|c| c == "task_index"));
    assert!(column_names.iter().any(|c| c == "observation.state"));
    assert!(column_names.iter().any(|c| c == "action"));
}

#[test]
fn test_parquet_video_reference_format() {
    let temp_dir = build_lerobot_dataset_with_camera("observation.images.cam_left", 3);
    let refs = read_video_references(&temp_dir, "observation.images.cam_left");
    
    // Format: "videos/chunk-{chunk_idx:03d}/{camera_key}/episode_{episode_idx:06d}.mp4:{timestamp}"
    for ref_str in refs {
        assert!(ref_str.starts_with("videos/chunk-"));
        assert!(ref_str.contains("/observation.images.cam_left/episode_"));
        assert!(ref_str.contains(".mp4:"));
        
        // Verify timestamp is present after colon
        let parts: Vec<_> = ref_str.split(':').collect();
        assert_eq!(parts.len(), 2);
        let timestamp: f64 = parts[1].parse().expect("Invalid timestamp format");
        assert!(timestamp >= 0.0);
    }
}

#[test]
fn test_frame_index_sequential_per_episode() {
    let temp_dir = build_lerobot_dataset(1, 10);
    let frame_indices = read_frame_indices(&temp_dir, 0);
    
    // Frame indices should be 0 to length-1
    assert_eq!(frame_indices, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
}

#[test]
fn test_global_index_monotonically_increasing() {
    let temp_dir = build_lerobot_dataset(2, 5);
    let global_indices = read_global_indices(&temp_dir);
    
    // Global index should increase across episodes
    for i in 1..global_indices.len() {
        assert!(global_indices[i] > global_indices[i - 1]);
    }
}
```

### 3. Video Format Compliance Tests

#### 3.1 MP4/H.264 Validation
```rust
// crates/roboflow-dataset/tests/lerobot/compliance/video_format_tests.rs
#[test]
fn test_video_is_valid_mp4() {
    let temp_dir = build_lerobot_dataset_with_camera("observation.images.cam_left", 3);
    let video_path = temp_dir.path().join("videos/chunk-000/observation.images.cam_left/episode_000000.mp4");
    
    // Use ffprobe binary if available (see tests/video_encoding_validation.rs)
    let info = match probe_video_properties(&video_path) {
        Some(info) => info,
        None => return, // Skip if ffprobe is unavailable
    };
    assert!(
        info.codec_name.contains("264")
            || info.codec_name.contains("hevc")
            || info.codec_name.contains("mpeg4")
    );
}

#[test]
fn test_video_codec_is_compatible() {
    let temp_dir = build_lerobot_dataset_with_camera("observation.images.cam_left", 30);
    let video_path = find_first_video(&temp_dir);
    let info = match probe_video_properties(&video_path) {
        Some(info) => info,
        None => return,
    };
    
    let codec = &info.streams[0].codec_name;
    // LeRobot v2.1 compatible codecs
    assert!(
        codec == "h264" || codec == "hevc" || codec == "mpeg4",
        "Codec {} is not LeRobot v2.1 compatible",
        codec
    );
}

#[test]
fn test_video_frame_count_matches_episode_length() {
    let fps = 30;
    let frame_count = 60;
    let temp_dir = build_lerobot_dataset_with_camera_and_fps("cam", frame_count as usize, fps as f64);
    
    let video_path = temp_dir.path().join("videos/chunk-000/cam/episode_000000.mp4");
    let info = match probe_video_properties(&video_path) {
        Some(info) => info,
        None => return,
    };
    
    let actual_frames = info.streams[0].nb_frames.unwrap_or(0);
    assert_eq!(actual_frames, frame_count as i64);
}

#[test]
fn test_video_fps_matches_dataset_fps() {
    let dataset_fps = 30.0;
    let temp_dir = build_lerobot_dataset_with_camera_and_fps("cam", 30, dataset_fps);
    
    let video_path = find_first_video(&temp_dir);
    let info = match probe_video_properties(&video_path) {
        Some(info) => info,
        None => return,
    };
    
    let video_fps = info.fps;
    if video_fps == 0.0 {
        return; // Skip if ffprobe did not report fps
    }
    assert!((video_fps - dataset_fps).abs() < 0.1);
}
```

### 4. Statistics Accuracy Tests

#### 4.1 episodes_stats.jsonl Validation
```rust
// crates/roboflow-dataset/tests/lerobot/compliance/statistics_tests.rs
#[test]
fn test_episode_stats_has_all_features() {
    let temp_dir = build_lerobot_dataset_with_state_and_action();
    let stats = read_episode_stats(&temp_dir, 0);
    
    assert!(stats.stats.contains_key("observation.state"));
    assert!(stats.stats.contains_key("action"));
}

#[test]
fn test_statistics_shape_matches_feature_shape() {
    let temp_dir = build_lerobot_dataset_with_state_shape(7); // 7-DOF robot
    let stats = read_episode_stats(&temp_dir, 0);
    
    let state_stats = &stats.stats["observation.state"];
    assert_eq!(state_stats.min.len(), 7);
    assert_eq!(state_stats.max.len(), 7);
    assert_eq!(state_stats.mean.len(), 7);
    assert_eq!(state_stats.std.len(), 7);
}

#[test]
fn test_statistics_values_are_correct() {
    // Create dataset with known values
    let states = vec![
        vec![0.0, 1.0, 2.0],
        vec![1.0, 2.0, 3.0],
        vec![2.0, 3.0, 4.0],
    ];
    let temp_dir = build_lerobot_dataset_from_states(states);
    let stats = read_episode_stats(&temp_dir, 0);
    
    let state_stats = &stats.stats["observation.state"];
    assert_eq!(state_stats.min, vec![0.0, 1.0, 2.0]);
    assert_eq!(state_stats.max, vec![2.0, 3.0, 4.0]);
    
    // Mean: [1.0, 2.0, 3.0]
    assert!((state_stats.mean[0] - 1.0).abs() < 0.001);
    assert!((state_stats.mean[1] - 2.0).abs() < 0.001);
    assert!((state_stats.mean[2] - 3.0).abs() < 0.001);
}

#[test]
fn test_min_leq_mean_leq_max() {
    let temp_dir = build_lerobot_dataset(1, 3);
    let stats = read_episode_stats(&temp_dir, 0);
    
    for (_, feature_stats) in &stats.stats {
        for i in 0..feature_stats.min.len() {
            assert!(
                feature_stats.min[i] <= feature_stats.mean[i] &&
                feature_stats.mean[i] <= feature_stats.max[i],
                "min <= mean <= max violated at dimension {}",
                i
            );
        }
    }
}

#[test]
fn test_std_is_non_negative() {
    let temp_dir = build_lerobot_dataset(1, 3);
    let stats = read_episode_stats(&temp_dir, 0);
    
    for (_, feature_stats) in &stats.stats {
        for val in &feature_stats.std {
            assert!(*val >= 0.0, "Standard deviation must be non-negative");
        }
    }
}
```

### 5. Writer Implementation Tests

#### 5.1 `format_writer_impl.rs` Coverage
```rust
// crates/roboflow-dataset/tests/lerobot/writer/format_writer_impl_tests.rs
#[test]
fn test_dataset_writer_trait_object_safety() {
    // Verify LerobotWriter implements DatasetWriter correctly
    let temp_dir = tempdir().unwrap();
    let writer: Box<dyn DatasetWriter> = Box::new(
        LerobotWriter::new_local(temp_dir.path(), LerobotConfig::default()).unwrap()
    );
    
    // Should be able to call trait methods
    let frame = FrameBuilder::new(0)
        .add_state("observation.state", vec![0.0])
        .build();
    writer.write_frame(&frame).unwrap();
    let stats = writer.finalize().unwrap();
    
    assert!(stats.frames_written > 0);
}

#[test]
fn test_format_writer_handles_episode_gaps() {
    // LeRobot should handle non-sequential episode indices
    let temp_dir = tempdir().unwrap();
    let mut writer = LerobotWriter::new_local(temp_dir.path(), LerobotConfig::default()).unwrap();
    
    writer.start_episode(Some(0));
    writer.write_frame(&FrameBuilder::new(0).add_state("observation.state", vec![0.0]).build()).unwrap();
    
    writer.start_episode(Some(5)); // Gap: episodes 1-4 skipped
    writer.write_frame(&FrameBuilder::new(0).add_state("observation.state", vec![1.0]).build()).unwrap();
    
    writer.finalize().unwrap();
    
    // Both episodes should exist
    assert!(temp_dir.path().join("data/chunk-000/episode_000000.parquet").exists());
    assert!(temp_dir.path().join("data/chunk-000/episode_000005.parquet").exists());
}

#[test]
fn test_format_writer_error_propagation() {
    // Test that errors from underlying writer are properly propagated
    // Reuse error harness patterns from tests/dataset_writer_error_tests.rs
    // (e.g., a read-only temp dir or failing MockStorage).
    let writer = create_writer_to_readonly_dir();
    
    let result = writer.write_frame(&FrameBuilder::new(0).add_state("observation.state", vec![0.0]).build());
    assert!(result.is_err());
}
```

#### 5.2 `writer_impl.rs` Coverage
```rust
// crates/roboflow-dataset/tests/lerobot/writer/writer_impl_tests.rs
#[test]
fn test_writer_creates_directory_structure() {
    let temp_dir = tempdir().unwrap();
    let mut writer = LerobotWriter::new_local(&temp_dir, LerobotConfig::default()).unwrap();
    
    // Directories should be created on writer initialization
    writer.start_episode(Some(0));
    writer.write_frame(&FrameBuilder::new(0).add_state("observation.state", vec![0.0]).build()).unwrap();
    
    assert!(temp_dir.path().join("data/chunk-000").exists());
    assert!(temp_dir.path().join("videos/chunk-000").exists());
}

#[test]
fn test_writer_handles_multiple_cameras() {
    let config = LerobotConfig {
        mappings: vec![
            Mapping { topic: "/cam1".to_string(), feature: "observation.images.cam1".to_string(), mapping_type: MappingType::Image, camera_key: None },
            Mapping { topic: "/cam2".to_string(), feature: "observation.images.cam2".to_string(), mapping_type: MappingType::Image, camera_key: None },
        ],
        ..Default::default()
    };
    
    let temp_dir = tempdir().unwrap();
    let mut writer = LerobotWriter::new_local(temp_dir.path(), config).unwrap();
    writer.start_episode(Some(0));
    
    let mut frame = FrameBuilder::new(0)
        .add_state("observation.state", vec![0.0])
        .add_image("observation.images.cam1", 640, 480)
        .add_image("observation.images.cam2", 640, 480)
        .build();
    
    writer.write_frame(&frame).unwrap();
    writer.finalize().unwrap();
    
    // Both camera directories should exist
    assert!(temp_dir.path().join("videos/chunk-000/observation.images.cam1").exists());
    assert!(temp_dir.path().join("videos/chunk-000/observation.images.cam2").exists());
}

#[test]
fn test_writer_video_segment_merge() {
    // Test video segments are properly merged on finalize
    let temp_dir = tempdir().unwrap();
    let mut writer = LerobotWriter::new_local(&temp_dir, LerobotConfig::default()).unwrap();
    writer.start_episode(Some(0));
    
    // Write frames that create multiple video segments
    for _ in 0..100 {
        writer
            .write_frame(
                &FrameBuilder::new(0)
                    .add_state("observation.state", vec![0.0])
                    .add_image("observation.images.cam", 640, 480)
                    .build(),
            )
            .unwrap();
    }
    
    writer.finalize().unwrap();
    
    // Verify single merged video exists (not multiple segments)
    let video_path = temp_dir.path()
        .join("videos/chunk-000/observation.images.cam/episode_000000.mp4");
    assert!(video_path.exists());
    
    // Verify no temporary segment files remain
    let temp_files: Vec<_> = fs::read_dir(&temp_dir)
        .unwrap()
        .filter(|e| e.unwrap().file_name().to_str().unwrap().starts_with("fragment_"))
        .collect();
    assert!(temp_files.is_empty(), "Temporary fragment files not cleaned up");
}

#[test]
fn test_writer_handles_missing_state_gracefully() {
    // Frame without observation.state should be handled
    let temp_dir = tempdir().unwrap();
    let mut writer = LerobotWriter::new_local(&temp_dir, LerobotConfig::default()).unwrap();
    writer.start_episode(Some(0));
    
    let frame = LerobotFrame::new(0.0); // No state
    let result = writer.write_frame(&frame);
    
    // Should either skip frame or use default values
    assert!(result.is_ok() || matches!(result, Err(DatasetError::MissingState)));
}

#[test]
fn test_writer_memory_flush_creates_single_episode() {
    // Test in-memory mode creates single episode correctly
    let mut writer = InMemoryWriter::new();
    writer.start_episode(None).unwrap();
    
    for i in 0..10 {
        writer
            .write_frame(
                &FrameBuilder::new(i)
                    .with_timestamp((i as u64) * 33_333_333)
                    .add_state("observation.state", vec![0.0])
                    .build(),
            )
            .unwrap();
    }
    
    writer.finish_episode().unwrap();
    let stats = writer.finalize().unwrap();
    
    assert_eq!(stats.frames_written, 10);
    assert_eq!(writer.episode_frames(0).unwrap().len(), 10);
}
```

### 6. Integration Tests

#### 6.1 End-to-End Conversion
```rust
// crates/roboflow-dataset/tests/lerobot/integration/e2e_tests.rs
#[tokio::test]
async fn test_bag_to_lerobot_conversion() {
    // Full pipeline test
    let input_bag = "tests/fixtures/sample.bag";
    let output_dir = tempdir().unwrap();
    
    let config = LerobotConfig::default();
    let writer = LerobotWriter::new_local(output_dir.path(), config).unwrap();
    
    let source = BagSource::new(input_bag).unwrap();
    let mut pipeline = PipelineExecutor::new(writer, PipelineConfig::default());
    let mut source = source;

    while let Some(batch) = source.read_batch(100).await.unwrap() {
        for msg in batch {
            pipeline.process_message(msg).unwrap();
        }
    }

    let stats = pipeline.finalize().unwrap();
    
    // Verify output
    assert!(stats.frames_written > 0);
    assert!(output_dir.path().join("meta/info.json").exists());
    
    // Load and validate info.json
    let info: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(output_dir.path().join("meta/info.json")).unwrap()
    ).unwrap();
    
    assert_eq!(info["codebase_version"], "v2.1");
    assert_eq!(info["total_frames"].as_i64().unwrap(), stats.frames_written as i64);
}

#[tokio::test]
async fn test_lerobot_dataset_passes_validation() {
    let output_dir = tempdir().unwrap();
    
    // Generate dataset
    generate_test_dataset(&output_dir, 10, 30).await;
    
    // Validation should reuse internal metadata checks or a dedicated validator when added.
}
```

#### 6.2 Round-Trip Tests
```rust
// crates/roboflow-dataset/tests/lerobot/integration/roundtrip_tests.rs
#[test]
fn test_parquet_roundtrip_preserves_data() {
    let original_frames = vec![
        create_frame_with_values(0.0, vec![1.0, 2.0, 3.0], vec![0.1, 0.2, 0.3]),
        create_frame_with_values(0.033, vec![1.1, 2.1, 3.1], vec![0.2, 0.3, 0.4]),
    ];
    
    let temp_dir = write_frames_to_parquet(&original_frames);
    let loaded_frames = read_frames_from_parquet(&temp_dir);
    
    assert_eq!(original_frames.len(), loaded_frames.len());
    for (orig, loaded) in original_frames.iter().zip(loaded_frames.iter()) {
        assert!((orig.timestamp - loaded.timestamp).abs() < 0.001);
        assert_eq!(orig.state, loaded.state);
        assert_eq!(orig.action, loaded.action);
    }
}
```

### 7. Error Handling Tests

```rust
// crates/roboflow-dataset/tests/lerobot/error_handling_tests.rs
#[test]
fn test_writer_rejects_invalid_fps() {
    let toml_str = r#"
[dataset]
name = "invalid_fps"
fps = 0
"#;
    let result = LerobotConfig::from_toml(toml_str);
    assert!(result.is_err());
}

#[test]
fn test_writer_handles_disk_full() {
    // Mock scenario where disk is full
    // Reuse error harness patterns from tests/dataset_writer_error_tests.rs
    let writer = create_writer_with_limited_space();
    
    let result = write_large_dataset(&writer);
    assert!(matches!(result, Err(DatasetError::StorageError(_))));
}

#[test]
fn test_corrupted_parquet_recovery() {
    // Test handling of corrupted intermediate files
    let temp_dir = tempdir().unwrap();
    let mut writer = LerobotWriter::new_local(temp_dir.path(), LerobotConfig::default()).unwrap();
    writer.start_episode(Some(0));
    
    write_some_frames(&mut writer);
    corrupt_parquet_file(&writer);
    
    let result = writer.finalize();
    // Should report error, not panic
    assert!(result.is_err());
}
```

### 8. Metadata Tests

#### 8.1 `metadata.rs` Coverage
```rust
// crates/roboflow-dataset/tests/lerobot/metadata_tests.rs
#[test]
fn test_info_json_generation() {
    let mut config = LerobotConfig::default();
    config.dataset.base.name = "test_dataset".to_string();
    config.dataset.base.fps = 30;
    config.dataset.base.robot_type = Some("stretch".to_string());

    let mut collector = MetadataCollector::new();
    collector.add_episode(0, 50, vec![0]);
    collector.total_frames = 50;
    let output_dir = tempdir().unwrap();
    collector.write_all(output_dir.path(), &config).unwrap();

    let info: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(output_dir.path().join("meta/info.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(info["codebase_version"], "v2.1");
    assert_eq!(info["robot_type"], "stretch");
    assert_eq!(info["fps"], 30);
}

#[test]
fn test_episodes_jsonl_generation() {
    let mut collector = MetadataCollector::new();
    collector.add_episode(0, 50, vec![0]);
    collector.add_episode(1, 45, vec![1]);

    let output_dir = tempdir().unwrap();
    collector.write_all(output_dir.path(), &LerobotConfig::default()).unwrap();
    let jsonl = std::fs::read_to_string(output_dir.path().join("meta/episodes.jsonl")).unwrap();
    let lines: Vec<_> = jsonl.lines().collect();
    
    assert_eq!(lines.len(), 2);
    
    // Verify each line is valid JSON with required fields
    for line in &lines {
        let ep: serde_json::Value = serde_json::from_str(line).unwrap();
        assert!(ep.get("episode_index").is_some());
        assert!(ep.get("tasks").is_some());
        assert!(ep.get("length").is_some());
    }
}

#[test]
fn test_tasks_jsonl_deduplication() {
    let mut collector = MetadataCollector::new();
    collector.add_episode(
        0,
        10,
        vec![
            collector.register_task("task1".to_string()),
            collector.register_task("task2".to_string()),
        ],
    );
    collector.add_episode(1, 10, vec![collector.register_task("task1".to_string())]);

    let output_dir = tempdir().unwrap();
    collector.write_all(output_dir.path(), &LerobotConfig::default()).unwrap();
    let jsonl = std::fs::read_to_string(output_dir.path().join("meta/tasks.jsonl")).unwrap();
    let lines: Vec<_> = jsonl.lines().collect();
    assert_eq!(lines.len(), 2);
}
```

## Test Coverage Targets

### Module-Specific Targets

| Module | Current Coverage | Target Coverage | Priority |
|--------|------------------|-----------------|----------|
| `format_writer_impl.rs` | 6.67% | **95%** | P0 - Critical |
| `trait_impl.rs` | 12.50% | **95%** | P0 - Critical |
| `writer_impl.rs` | 34.37% | **90%** | P0 - Critical |
| `metadata.rs` | 32.12% | **90%** | P1 - High |
| `upload.rs` | 17.66% | **85%** | P1 - High |
| `decode.rs` | 0.00% | **85%** | P1 - High |
| `composer.rs` | 16.08% | **80%** | P1 - High |
| `alignment/buffer.rs` | 17.63% | **80%** | P2 - Medium |

### Overall Targets

| Metric | Current | Target | Deadline |
|--------|---------|--------|----------|
| Line Coverage | 55.66% | **85%** | 2 weeks |
| Function Coverage | 54.69% | **80%** | 2 weeks |
| Critical Path Coverage | ~35% | **100%** | 1 week |

## Implementation Plan

### Phase 1: Critical Path (Week 1) - P0 Modules
1. `format_writer_impl.rs` - Format trait implementation tests
2. `trait_impl.rs` - DatasetWriter trait tests  
3. `writer_impl.rs` - Core writer tests
4. Add compliance validation helpers

**Goal**: 95% coverage on P0 modules, LeRobot v2.1 structure validation passing.

### Phase 2: Metadata & Upload (Week 1-2) - P1 Modules
1. `metadata.rs` - Info JSON, episodes JSONL generation tests
2. `upload.rs` - Cloud upload logic tests
3. `decode.rs` - Source decoding tests
4. Reuse ffprobe-based validation (see tests/video_encoding_validation.rs)

**Goal**: 85% coverage on P1 modules, end-to-end conversion tests passing.

### Phase 3: Integration & Compliance (Week 2)
1. Full e2e conversion tests (Bag → LeRobot)
2. LeRobot validation tool integration
3. Property-based tests for statistics
4. Performance regression tests

**Goal**: 85% overall coverage, all compliance tests passing.

## Testing Infrastructure

### Dependencies to Add

```toml
[dev-dependencies]
# Already present in crates/roboflow-dataset/Cargo.toml:
tempfile = "3.10"
proptest = "1.5"

# Optional addition if direct Parquet schema inspection is required:
# parquet = "53.0"
```

### Test Fixtures Required

```
crates/roboflow-dataset/tests/fixtures/ (module-based helpers)
tests/fixtures/ (workspace-level .bag fixtures)

If new LeRobot-specific fixtures are introduced, place them under:
crates/roboflow-dataset/tests/fixtures/lerobot/
```

### CI Integration

```yaml
# .github/workflows/coverage.yml
- name: Generate coverage report
  run: cargo llvm-cov --package roboflow-dataset --lcov --output-path lcov.info

- name: Check LeRobot compliance
  run: |
    cargo test -p roboflow-dataset --test lerobot_compliance -- --nocapture
    # Use ffprobe-based validation where available (see tests/video_encoding_validation.rs)
```

## Success Criteria

1. **All new tests pass**: `cargo test --package roboflow-dataset` exits with 0
2. **Coverage targets met**: 
   - P0 modules ≥ 95%
   - P1 modules ≥ 85%
   - Overall ≥ 85%
3. **Compliance verified**: Test datasets pass LeRobot official validation
4. **No regressions**: Existing 638 tests continue to pass
5. **Documentation**: All test functions have descriptive doc comments

## References

- [LeRobot Dataset Format Specification](./lerobot-v2.1-specification.md)
- [ADR-004: Comprehensive Testing Strategy](./adr-004-dataset-testing-strategy.md)
- [LeRobot GitHub](https://github.com/huggingface/lerobot)
- [Apache Parquet Format](https://parquet.apache.org/)
- [MP4 File Format](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Containers)
