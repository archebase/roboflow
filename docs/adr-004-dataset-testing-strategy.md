# ADR-004: Comprehensive Testing Strategy for roboflow-dataset

**Author**: Sisyphus (AI Agent)  
**Date**: 2026-02-20  
**Status**: Proposed  
**Related**: [ADR-003](./adr-003-testable-architecture.md)

## Context

Following ADR-003 which established testable interfaces, we need comprehensive tests for `roboflow-dataset` to ensure correctness. The crate contains:

- **Sources**: Bag/MCAP file reading, S3 prefix scanning
- **Formats**: LeRobot, HDF5, Zarr, RLDS writers
- **Media**: Video encoding/decoding, image processing
- **Pipeline**: Frame alignment, parallel processing
- **Core**: Traits, stats, registry

Current test coverage is partial. This ADR defines a complete testing strategy.

## Goals

1. **Unit Tests**: Every public function has unit tests
2. **Integration Tests**: Cross-module workflows tested
3. **Property Tests**: Data transformations verified
4. **Fixture Tests**: Real format compliance verified
5. **Mock Tests**: External dependencies isolated

## Testing Pyramid

```
                    ┌─────────────────┐
                    │   E2E Tests     │  (5%) - Full workflows
                    │  (fixtures)     │
                    └────────┬────────┘
                             │
              ┌──────────────┴──────────────┐
              │     Integration Tests       │  (15%) - Module interactions
              │    (mock sources/storage)   │
              └──────────────┬──────────────┘
                             │
        ┌────────────────────┴────────────────────┐
        │           Unit Tests                    │  (80%) - Individual functions
        │  (MockSource, InMemoryWriter, fixtures) │
        └─────────────────────────────────────────┘
```

## Detailed Testing Plan

### 1. Source Layer Tests (`src/sources/`)

#### 1.1 BagSource Tests
```rust
// tests/sources/bag_tests.rs
#[tokio::test]
async fn test_bag_source_reads_all_messages() {
    // Use test fixtures from tests/fixtures/
    let source = BagSource::new("tests/fixtures/sample.bag");
    let count = count_messages(&mut source).await;
    assert_eq!(count, EXPECTED_MESSAGE_COUNT);
}

#[tokio::test]
async fn test_bag_source_handles_large_files() {
    // Test memory efficiency with large bag
    let source = BagSource::new("tests/fixtures/large.bag");
    // Should not load entire file into memory
    let memory_before = get_memory_usage();
    process_in_batches(&mut source, 100).await;
    let memory_after = get_memory_usage();
    assert!(memory_after - memory_before < 100_000_000); // < 100MB overhead
}

#[tokio::test]
async fn test_bag_source_topic_filtering() {
    let source = BagSource::new_filtered("test.bag", &["/camera/image"]).await;
    let messages = collect_all(&mut source).await;
    assert!(messages.iter().all(|m| m.topic == "/camera/image"));
}
```

#### 1.2 Source Registry Tests
```rust
// tests/sources/registry_tests.rs
#[test]
fn test_all_source_types_registered() {
    register_builtin_sources();
    
    assert!(SourceType::Bag.can_create());
    assert!(SourceType::Mcap.can_create());
    assert!(SourceType::S3Prefix.can_create());
}

#[tokio::test]
async fn test_source_factory_creates_correct_type() {
    let config = SourceConfig::bag("test.bag");
    let source = create_source(&config).unwrap();
    
    assert!(source.as_any().is::<BagSource>());
}
```

### 2. Pipeline Layer Tests (`src/formats/`)

#### 2.1 Frame Alignment Tests
```rust
// tests/pipeline/alignment_tests.rs
#[tokio::test]
async fn test_frame_alignment_with_gaps() {
    let messages = vec![
        msg("/camera", 0.0),
        msg("/camera", 0.033),
        msg("/state", 0.015),  // Gap in timestamps
        msg("/camera", 0.066),
    ];
    
    let aligned = align_frames(messages, StreamingConfig::with_fps(30.0)).await;
    
    // Verify frames are aligned to 30fps grid
    assert_eq!(aligned[0].timestamp, 0.0);
    assert_eq!(aligned[1].timestamp, 0.033);
    assert_eq!(aligned[2].timestamp, 0.066);
}

#[tokio::test]
async fn test_frame_completion_criteria() {
    let criteria = CompletionCriteria::all_required(&["/camera", "/state"]);
    
    let partial = Frame::new(0.0)
        .with_data("/camera", image_data());
    
    assert!(!criteria.is_complete(&partial));
    
    let complete = partial.with_data("/state", state_data());
    assert!(criteria.is_complete(&complete));
}
```

#### 2.2 Parallel Pipeline Tests
```rust
// tests/pipeline/parallel_tests.rs
#[tokio::test]
async fn test_parallel_pipeline_processes_all_frames() {
    let messages = generate_test_messages(1000);
    let source = MockSource::with_messages(messages);
    let writer = InMemoryWriter::new();
    
    let pipeline = ParallelPipelineExecutor::new(writer, PipelineConfig::default());
    let stats = pipeline.process_messages_parallel(source).await.unwrap();
    
    assert_eq!(stats.frames_written, 1000);
    assert!(stats.fps > 0.0);
}

#[tokio::test]
async fn test_parallel_pipeline_handles_errors_gracefully() {
    let source = MockSource::with_error_at(50); // Error at message 50
    let writer = InMemoryWriter::new();
    
    let pipeline = ParallelPipelineExecutor::new(writer, PipelineConfig::default());
    let result = pipeline.process_messages_parallel(source).await;
    
    // Should return error but not panic
    assert!(result.is_err());
}
```

### 3. Format Writer Tests (`src/formats/lerobot/`)

#### 3.1 LeRobot Format Compliance Tests
```rust
// tests/formats/lerobot_compliance_tests.rs
#[test]
fn test_lerobot_directory_structure() {
    let writer = create_test_writer();
    writer.finalize().unwrap();
    
    // Verify LeRobot v2.1 structure
    assert!(Path::new("output/data/chunk-000/").exists());
    assert!(Path::new("output/videos/chunk-000/").exists());
    assert!(Path::new("output/meta/info.json").exists());
    assert!(Path::new("output/meta/episodes.jsonl").exists());
}

#[test]
fn test_episode_indexing() {
    let mut writer = LerobotWriter::new("/tmp/test");
    
    // Episode 0
    writer.set_episode_index(0);
    writer.write_frame(&frame(0)).unwrap();
    writer.write_frame(&frame(1)).unwrap();
    
    // Episode 5 (gap should be handled)
    writer.set_episode_index(5);
    writer.write_frame(&frame(0)).unwrap();
    
    writer.finalize().unwrap();
    
    // Verify files are named correctly
    assert!(Path::new("/tmp/test/data/chunk-000/episode_000000.parquet").exists());
    assert!(Path::new("/tmp/test/data/chunk-000/episode_000005.parquet").exists());
}

#[test]
fn test_video_chunking() {
    let config = LerobotConfig {
        episodes_per_chunk: 2,
        ..Default::default()
    };
    let mut writer = LerobotWriter::with_config("/tmp/test", config);
    
    // Write 5 episodes
    for ep_idx in 0..5 {
        writer.set_episode_index(ep_idx);
        writer.write_frame(&frame(0)).unwrap();
    }
    
    writer.finalize().unwrap();
    
    // Should create 3 chunks (0-1, 2-3, 4)
    assert!(Path::new("/tmp/test/videos/chunk-000/").exists());
    assert!(Path::new("/tmp/test/videos/chunk-001/").exists());
    assert!(Path::new("/tmp/test/videos/chunk-002/").exists());
}
```

#### 3.2 Parquet Tests
```rust
// tests/formats/lerobot_parquet_tests.rs
#[test]
fn test_parquet_schema_matches_lerobot_spec() {
    let writer = ParquetWriter::new("test.parquet");
    let schema = writer.schema();
    
    // Verify LeRobot schema fields
    assert!(schema.has_field("timestamp"));
    assert!(schema.has_field("action"));
    assert!(schema.has_field("observation.state"));
    assert!(schema.has_field("observation.images.*"));
}

#[test]
fn test_parquet_statistics_calculation() {
    let data = vec![
        vec![1.0, 2.0, 3.0],
        vec![2.0, 3.0, 4.0],
        vec![3.0, 4.0, 5.0],
    ];
    
    let stats = calculate_stats(&data);
    
    assert_eq!(stats.min, vec![1.0, 2.0, 3.0]);
    assert_eq!(stats.max, vec![3.0, 4.0, 5.0]);
    assert!(stats.mean[0] > 1.9 && stats.mean[0] < 2.1);
}
```

### 4. Media Layer Tests (`src/media/`)

#### 4.1 Video Encoder Tests
```rust
// tests/media/video_encoder_tests.rs
#[test]
fn test_video_encoder_produces_valid_mp4() {
    let frames = generate_test_frames(30, 640, 480); // 1 second @ 30fps
    
    let encoder = RsmpegMp4Encoder::new(VideoConfig::default());
    encoder.encode_frames(&frames, "output.mp4").unwrap();
    
    // Verify output is valid MP4
    let probe = ffprobe("output.mp4").unwrap();
    assert_eq!(probe.streams[0].codec_name, "h264");
    assert_eq!(probe.format.format_name, "mov,mp4,m4a");
}

#[test]
fn test_video_encoder_handles_various_resolutions() {
    let resolutions = vec![
        (320, 240),
        (640, 480),
        (1920, 1080),
        (2560, 1440),
    ];
    
    for (w, h) in resolutions {
        let frames = generate_test_frames(10, w, h);
        let encoder = RsmpegMp4Encoder::new(VideoConfig::default());
        
        assert!(
            encoder.encode_frames(&frames, &format!("{}x{}.mp4", w, h)).is_ok(),
            "Failed to encode {}x{}", w, h
        );
    }
}

#[test]
fn test_video_composer_merges_valid_mp4() {
    // Create two valid 1-second videos
    let video1 = create_test_video(30, "seg1.mp4");
    let video2 = create_test_video(30, "seg2.mp4");
    
    let composer = RsmpegVideoComposer::new();
    composer.compose(&[&seg1, &seg2], "merged.mp4").unwrap();
    
    // Verify merged video has correct frame count
    let probe = ffprobe("merged.mp4").unwrap();
    assert_eq!(probe.streams[0].nb_frames, 60); // 30 + 30
}
```

#### 4.2 Image Decoder Tests
```rust
// tests/media/image_decoder_tests.rs
#[test]
fn test_jpeg_decoding() {
    let jpeg_data = include_bytes!("fixtures/test.jpg");
    let decoder = ImageDecoder::new();
    
    let image = decoder.decode(jpeg_data).unwrap();
    
    assert_eq!(image.format, ImageFormat::Jpeg);
    assert!(image.width > 0);
    assert!(image.height > 0);
}

#[test]
fn test_nv12_conversion() {
    let rgb = generate_rgb_image(640, 480);
    let nv12 = rgb_to_nv12(&rgb, 640, 480);
    
    // Verify NV12 layout (Y plane + UV interleaved)
    let expected_size = 640 * 480 + (640 * 480 / 2);
    assert_eq!(nv12.len(), expected_size);
}
```

### 5. Core Layer Tests (`src/core/`)

#### 5.1 Trait Tests
```rust
// tests/core/trait_tests.rs
#[test]
fn test_dataset_writer_trait_object_safety() {
    // Verify trait is dyn-compatible
    let writers: Vec<Box<dyn DatasetWriter>> = vec![
        Box::new(LerobotWriter::new("/tmp/test1")),
        Box::new(Hdf5Writer::new("/tmp/test2")),
    ];
    
    for mut writer in writers {
        writer.write_frame(&test_frame()).unwrap();
        let stats = writer.finalize().unwrap();
        assert!(stats.frames_written > 0);
    }
}
```

#### 5.2 Stats Tests
```rust
// tests/core/stats_tests.rs
#[test]
fn test_episode_stats_aggregation() {
    let ep1 = EpisodeStats::new(0, 100)
        .with_feature("state", FeatureStats {
            min: vec![0.0],
            max: vec![10.0],
            mean: vec![5.0],
            std: vec![1.0],
        });
    
    let ep2 = EpisodeStats::new(1, 200)
        .with_feature("state", FeatureStats {
            min: vec![2.0],
            max: vec![8.0],
            mean: vec![6.0],
            std: vec![1.5],
        });
    
    let mut summary = BatchStatsSummary::new("test".to_string());
    summary.add_episode(ep1);
    summary.add_episode(ep2);
    summary.calculate_global_stats();
    
    let global = summary.global_stats.get("state").unwrap();
    assert_eq!(global.min, vec![0.0]);
    assert_eq!(global.max, vec![10.0]);
}
```

### 6. Integration Tests

#### 6.1 End-to-End Conversion Tests
```rust
// tests/integration/e2e_tests.rs
#[tokio::test]
async fn test_full_bag_to_lerobot_conversion() {
    // Setup
    let input = "tests/fixtures/sample.bag";
    let output = tempdir().path().join("output");
    
    // Configure
    let config = LerobotConfig::default();
    let writer = LerobotWriter::new(&output, config);
    
    // Convert
    let pipeline = ConversionPipeline::new(
        BagSource::new(input),
        writer,
        PipelineConfig::default(),
    );
    let result = pipeline.run().await.unwrap();
    
    // Verify
    assert!(result.frames_written > 0);
    assert!(output.join("meta/info.json").exists());
    
    // Verify LeRobot compliance
    let info: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(output.join("meta/info.json")).unwrap()
    ).unwrap();
    assert!(info.get("fps").is_some());
    assert!(info.get("total_episodes").is_some());
}

#[tokio::test]
async fn test_cloud_upload_integration() {
    // Use MockStorage to test upload logic without real cloud
    let storage = Arc::new(MockStorage::new());
    let sink = StorageSink::with_storage(storage.clone(), temp_dir());
    
    // Process some data
    sink.write_parquet("data.parquet", test_data()).await.unwrap();
    sink.upload_to_cloud("s3://bucket/prefix/").await.unwrap();
    
    // Verify upload operations were recorded
    let ops = storage.get_operations();
    assert!(ops.iter().any(|op| matches!(op, StorageOperation::Upload { .. })));
}
```

### 7. Property-Based Tests

```rust
// tests/property/property_tests.rs
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_frame_alignment_preserves_all_messages(
        messages in vec(any_message(), 1..1000)
    ) {
        let aligned = align_frames(messages.clone(), StreamingConfig::default());
        
        // All original messages should be in some frame
        let aligned_count: usize = aligned.iter().map(|f| f.message_count()).sum();
        prop_assert_eq!(aligned_count, messages.len());
    }
    
    #[test]
    fn test_stats_aggregation_is_associative(
        episodes in vec(any_episode_stats(), 2..10)
    ) {
        // Split episodes into two groups
        let mid = episodes.len() / 2;
        let group1 = &episodes[..mid];
        let group2 = &episodes[mid..];
        
        // Aggregate separately then combine
        let mut summary1 = BatchStatsSummary::new("test1".to_string());
        for ep in group1 { summary1.add_episode(ep.clone()); }
        summary1.calculate_global_stats();
        
        let mut summary2 = BatchStatsSummary::new("test2".to_string());
        for ep in group2 { summary2.add_episode(ep.clone()); }
        summary2.calculate_global_stats();
        
        // Aggregate all at once
        let mut summary_all = BatchStatsSummary::new("test_all".to_string());
        for ep in &episodes { summary_all.add_episode(ep.clone()); }
        summary_all.calculate_global_stats();
        
        // Results should be equivalent (within floating point tolerance)
        for (feature, stats1) in &summary1.global_stats {
            let stats2 = summary2.global_stats.get(feature).unwrap();
            let stats_all = summary_all.global_stats.get(feature).unwrap();
            
            // Combined stats should match individual aggregation
            prop_assert!((stats_all.mean[0] - 
                (stats1.mean[0] * summary1.total_frames as f32 + 
                 stats2.mean[0] * summary2.total_frames as f32) / 
                (summary1.total_frames + summary2.total_frames) as f32).abs() < 0.01);
        }
    }
}
```

## Test Fixtures

### Fixture Structure
```
tests/fixtures/
├── bags/
│   ├── minimal.bag           # 1 topic, 10 messages
│   ├── multi_topic.bag       # 5 topics, 100 messages each
│   ├── large.bag             # 10,000 messages (for perf tests)
│   └── corrupted.bag         # Intentionally corrupted for error tests
├── mcap/
│   └── sample.mcap
├── images/
│   ├── test_320x240.jpg
│   ├── test_640x480.png
│   └── test_1920x1080.jpg
└── expected/
    ├── minimal_lerobot/      # Expected output for minimal.bag
    └── minimal_stats.json    # Expected stats
```

### Fixture Generation
```rust
// tests/fixtures/generate.rs
pub fn generate_minimal_bag() -> Vec<u8> {
    // Programmatically generate minimal valid bag file
}

pub fn generate_test_frames(count: usize, width: u32, height: u32) -> Vec<VideoFrame> {
    (0..count)
        .map(|i| VideoFrame::new(width, height, test_pattern(i)))
        .collect()
}
```

## Continuous Integration

### Test Commands
```bash
# Run all tests
cargo test --lib -p roboflow-dataset

# Run with coverage
cargo tarpaulin --lib -p roboflow-dataset --out Html

# Run property tests only
cargo test --lib -p roboflow-dataset property

# Run integration tests only
cargo test --test integration

# Run with tracing for debugging
cargo test --lib -p roboflow-dataset -- --nocapture
```

### Coverage Targets
- Line coverage: > 85%
- Branch coverage: > 75%
- Critical paths: 100%

## Success Criteria

1. **All tests pass**: `cargo test` exits with code 0
2. **No flaky tests**: All tests pass consistently across 10 runs
3. **Fast feedback**: Unit tests complete in < 30 seconds
4. **Coverage report**: Generated on every PR
5. **Documentation**: Every test has a clear description

## Migration Plan

### Phase 1: Core Infrastructure (Week 1)
1. Set up test fixtures and utilities
2. Implement `MockSource`, `InMemoryWriter` improvements
3. Add test helper functions

### Phase 2: Unit Tests (Weeks 2-3)
1. Source layer tests
2. Pipeline layer tests  
3. Core layer tests

### Phase 3: Format Tests (Weeks 4-5)
1. LeRobot format compliance tests
2. Video encoding tests
3. Parquet tests

### Phase 4: Integration (Week 6)
1. End-to-end tests
2. Property-based tests
3. Performance benchmarks

## References

- [ADR-003: Testable Architecture](./adr-003-testable-architecture.md)
- [LeRobot Dataset Format](https://github.com/huggingface/lerobot/blob/main/lerobot/common/datasets/factory.py)
- [Rust Testing Best Practices](https://doc.rust-lang.org/book/ch11-00-testing.html)
- [Property-Based Testing with proptest](https://docs.rs/proptest/latest/proptest/)
