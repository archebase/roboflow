# ADR-001: Pipeline Architecture Refactoring - Writer/Storage Separation

**Author**: Sisyphus (AI Agent)  
**Date**: 2026-02-20  
**Status**: Accepted  
**Related**: [executor-architecture.md](./executor-architecture.md), [data-pipeline-design.md](./data-pipeline-design.md)

## Context

The `roboflow-pipeline` crate currently has tight coupling between data processing (writers) and storage operations. This creates several architectural issues:

- `LerobotWriter` owns `Arc<dyn Storage>`, making unit testing require real storage backends
- Video segmentation logic (for cloud memory limits) is applied even to local storage
- MP4 video segments are concatenated byte-by-byte, corrupting video files
- Pipeline parallelism is hardcoded into executor types (`PipelineExecutor` vs `ParallelPipelineExecutor`)

Meanwhile, `roboflow-storage` is a well-designed separate crate providing multipart upload, retry logic, and caching. The issue is not the storage crate itself, but how the pipeline crate uses it.

## Decision

Separate the concerns:
1. **Pipeline/Writers** produce complete local datasets in temp directories (pure data processing)
2. **Storage** handles upload from local temp to cloud (S3/OSS) via a `Sink` trait
3. **Stats/Metadata** flow back to executor/TiKV for distributed coordination

This follows a **staging pattern**: 
- Writer creates valid local dataset → temp directory
- Sink uploads staged data → cloud storage  
- Stats reported → TiKV for coordination

## Key Design Decisions

### 1. WriteOperation Enum

Central enum representing all possible write operations:

```rust
pub enum WriteOperation {
    WriteFile { path: PathBuf, data: Vec<u8> },
    WriteParquet { path: PathBuf, frames: Vec<u8> },
    EncodeAndWriteVideo { 
        camera: String, 
        frames: Vec<ImageData>, 
        output_path: PathBuf, 
        config: VideoConfig 
    },
    WriteMetadata { path: PathBuf, content: serde_json::Value },
}
```

### 2. DatasetWriter Trait (Refactored)

Writers are pure logic - they accumulate state and return operations:

```rust
pub trait DatasetWriter: Send + Sync {
    fn initialize(&mut self, config: &DatasetConfig) -> Result<()>;
    fn write_frame(&mut self, frame: &Frame) -> Result<()>;
    fn finalize(&mut self) -> Result<(WriterStats, Vec<WriteOperation>)>;
}
```

**Key change**: `finalize()` returns `Vec<WriteOperation>` instead of writing directly.

### 3. Sink Trait (in roboflow-storage)

```rust
pub trait Sink: Send + Sync {
    fn execute(&self, op: WriteOperation) -> Result<()>;
    fn execute_batch(&self, ops: Vec<WriteOperation>) -> Result<()>;
}

pub struct StorageSink {
    storage: Arc<dyn Storage>,
}

impl Sink for StorageSink {
    fn execute(&self, op: WriteOperation) -> Result<()> {
        match op {
            WriteOperation::EncodeAndWriteVideo { frames, output_path, config, .. } => {
                // For local storage: single-pass encode all frames
                // For cloud: may use multipart streaming
                let video_data = encode_video(&frames, &config)?;
                self.storage.write(&output_path, &video_data)?;
            }
            // ... other operations
        }
        Ok(())
    }
}
```

### 4. Pipeline Flow

```
Source → Aligner → Processor → Writer → [WriteOperations] → Sink → Storage
```

```rust
impl Pipeline {
    pub fn run(self, sink: &dyn Sink) -> Result<PipelineStats> {
        while let Some(messages) = self.source.read_batch()? {
            let aligned = self.aligner.align(messages)?;
            
            // Parallelism configured per-pipeline, not per-executor-type
            match self.execution {
                ExecutionStrategy::Sequential => { /* sequential processing */ }
                ExecutionStrategy::Parallel { workers } => { /* rayon parallel */ }
            }
        }
        
        let (stats, operations) = self.writer.finalize()?;
        sink.execute_batch(operations)?;  // Storage executes here
        Ok(stats)
    }
}
```

### 5. LeRobotWriter Simplification

**Before**: Owns storage, handles segmentation
```rust
pub struct LerobotWriter {
    storage: Arc<dyn Storage>,  // ❌ Wrong
    // ...
}
```

**After**: Pure logic, returns operations
```rust
pub struct LeRobotWriter {
    config: LerobotConfig,
    frame_buffer: Vec<LerobotFrame>,
    image_buffers: HashMap<String, Vec<ImageData>>,
    // ❌ REMOVED: storage, segmentation logic
}

impl DatasetWriter for LeRobotWriter {
    fn finalize(&mut self) -> Result<(WriterStats, Vec<WriteOperation>)> {
        let mut ops = Vec::new();
        
        // One operation per camera video
        for (camera, images) in &self.image_buffers {
            ops.push(WriteOperation::EncodeAndWriteVideo {
                camera: camera.clone(),
                frames: images.clone(),
                output_path: self.video_path(camera),
                config: self.config.video.clone(),
            });
        }
        
        ops.push(WriteOperation::WriteParquet { ... });
        ops.push(WriteOperation::WriteMetadata { ... });
        
        Ok((stats, ops))
    }
}
```

## Consequences

### Positive

| Aspect | Before | After |
|--------|--------|-------|
| **Coupling** | Writer owns storage | Writer produces ops, sink executes |
| **Testing** | Requires storage backend | Mock `VecSink`, test pure logic |
| **Video correctness** | Byte-concatenation corrupts MP4s | Single-pass encoding for local |
| **Parallelism** | Two executor types | One pipeline, configurable strategy |
| **Extensibility** | Low | High: new format = new writer |

#### Testability Improvements
- **Unit tests**: Test writer logic without mocking storage
- **Integration tests**: Full pipeline with `VecSink` to verify operations
- **Regression tests**: Capture operation streams and compare across versions
- **Fuzzing**: Generate random frames, verify writer produces valid operations

#### Adding New Formats
- **4-step process**: Config → Writer → Register → Test
- **No storage code**: Focus purely on format logic
- **Reusable components**: Video encoding, parquet writing handled by sink
- **Example formats**: See `Adding New Dataset Formats` section below

### Trade-offs

| Aspect | Consideration |
|--------|--------------|
| **Memory** | Writer must buffer all frames until `finalize()` |
| **Latency** | Operations executed at end vs incrementally |
| **Complexity** | Additional trait layer (Sink) |

### Mitigations

- For memory: Streaming operations can be added later as `WriteOperation::StreamingVideo { ... }`
- For latency: Pipeline can flush periodically to sink during processing (future enhancement)

## Implementation Plan

### Phase 1: Add New APIs (Backward Compatible)
1. Add `WriteOperation` enum to `roboflow-pipeline`
2. Add `Sink` trait to `roboflow-pipeline`
3. Extend `DatasetWriter` with `finalize_with_ops()` (default impl)
4. Add `StorageSink` to `roboflow-storage`

### Phase 2: Migrate Writers
1. Refactor `LeRobotWriter` to new API
2. Remove storage ownership from writer
3. Update tests to use `VecSink`

### Phase 3: Pipeline Integration
1. Refactor `Pipeline` to use `Sink` for output
2. Remove `PipelineExecutor` / `ParallelPipelineExecutor` duplication
3. Add `ExecutionStrategy` configuration

### Phase 4: Cleanup
1. Remove deprecated `finalize()` method
2. Remove old executor implementations
3. Update documentation

## Staging and Upload Architecture

### Design Rationale

For distributed processing, a **staging pattern** works better than per-operation streaming:

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐
│   Pipeline      │────▶│ Local Temp   │────▶│  Cloud Storage  │
│   (Writer)      │     │  (Staging)   │     │   (S3/OSS)      │
└─────────────────┘     └──────────────┘     └─────────────────┘
         │                       │                      │
         ▼                       ▼                      ▼
   Write frames            Dataset complete      Report to TiKV
   to temp dir             Upload dataset        (distributed
                                                     coord)
```

### Benefits

1. **Reliability**: Local dataset is complete before upload starts
2. **Simplicity**: Writer focuses on data, not cloud streaming
3. **Resumability**: Failed uploads can retry without re-encoding
4. **Stats Tracking**: Complete dataset stats available for TiKV

### Writer Responsibilities

- Create valid local dataset in temp directory
- Write all frames (parquet + videos + metadata)
- Return `WriterResult` with:
  - `temp_path`: Local dataset location
  - `stats`: Frame counts, video sizes, metadata
  - `checksums`: For verification during upload

### Sink Responsibilities

- Accept `WriterResult` with local path
- Upload to cloud storage (S3/OSS)
- Handle retries and multipart uploads
- Report completion status to TiKV
- Cleanup temp directory after successful upload

### Stats Flow

```
Writer ──▶ local stats ──▶ Sink ──▶ TiKV (distributed state)
             │                         │
             └──────▶ Executor ◀───────┘
             (progress reporting)
```

## Testing Strategy

The separation enables comprehensive testing at each layer:

### 1. Writer Testing (Pure Logic)

```rust
/// In-memory sink for testing
pub struct VecSink {
    operations: RefCell<Vec<WriteOperation>>,
}

impl Sink for VecSink {
    fn execute(&self, op: WriteOperation) -> Result<()> {
        self.operations.borrow_mut().push(op);
        Ok(())
    }
    
    fn operations(&self) -> Vec<WriteOperation> {
        self.operations.borrow().clone()
    }
}

#[test]
fn test_lerobot_writer_produces_correct_operations() {
    let mut writer = LeRobotWriter::new(config);
    
    // Add test frames
    writer.write_frame(&frame_with_camera("cam_0"))?;
    writer.write_frame(&frame_with_camera("cam_1"))?;
    
    let (stats, ops) = writer.finalize()?;
    
    // Assert on operations, not storage state
    assert_eq!(ops.len(), 4); // 2 videos + parquet + metadata
    assert!(matches!(ops[0], WriteOperation::EncodeAndWriteVideo { camera, .. } if camera == "cam_0"));
    
    // Execute with mock sink to verify operations are valid
    let mock_sink = VecSink::new();
    mock_sink.execute_batch(ops)?;
    
    // Verify sink received correct operations
    let stored = mock_sink.operations();
    assert_eq!(stored.len(), 4);
}

#[test]
fn test_writer_handles_missing_state() {
    let mut writer = LeRobotWriter::new(config);
    
    // Frame without state (only image)
    writer.write_frame(&frame_with_image_only())?;
    
    let (stats, ops) = writer.finalize()?;
    
    // Should still produce valid operations with forward-filled data
    assert!(stats.frames_written > 0);
    assert!(!ops.is_empty());
}
```

### 2. Sink Testing

```rust
#[test]
fn test_storage_sink_executes_operations() {
    let temp_dir = tempfile::tempdir()?;
    let storage = Arc::new(LocalStorage::new(&temp_dir));
    let sink = StorageSink::new(storage);
    
    // Test WriteFile operation
    sink.execute(WriteOperation::WriteFile {
        path: PathBuf::from("test.txt"),
        data: b"hello".to_vec(),
    })?;
    
    // Verify file was written
    assert!(temp_dir.path().join("test.txt").exists());
}
```

### 3. Integration Testing

```rust
#[test]
fn test_full_pipeline_with_mock_sink() {
    let pipeline = Pipeline::builder()
        .source(BagSource::new("test.bag"))
        .writer(LeRobotWriter::new(config))
        .build()?;
    
    let mock_sink = VecSink::new();
    let stats = pipeline.run(&mock_sink)?;
    
    assert!(stats.frames_written > 0);
    assert!(!mock_sink.operations().is_empty());
}
```

## Adding New Dataset Formats

The architecture makes adding new formats straightforward:

### Step 1: Define Format Configuration

```rust
// formats/myformat/config.rs
pub struct MyFormatConfig {
    pub dataset_name: String,
    pub fps: u32,
    pub compression: CompressionType,
}

impl FormatConfig for MyFormatConfig {
    fn validate(&self) -> Result<()> {
        // Validate configuration
        Ok(())
    }
}
```

### Step 2: Implement DatasetWriter

```rust
// formats/myformat/writer.rs
pub struct MyFormatWriter {
    config: MyFormatConfig,
    frame_buffer: Vec<MyFormatFrame>,
}

impl DatasetWriter for MyFormatWriter {
    fn initialize(&mut self, config: &DatasetConfig) -> Result<()> {
        self.config = config.as_myformat()?;
        Ok(())
    }
    
    fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        let my_frame = self.convert_frame(frame)?;
        self.frame_buffer.push(my_frame);
        Ok(())
    }
    
    fn finalize(&mut self) -> Result<(WriterStats, Vec<WriteOperation>)> {
        let mut ops = Vec::new();
        
        // Format-specific output
        ops.push(WriteOperation::WriteFile {
            path: self.output_path("data.bin"),
            data: self.serialize_frames()?,
        });
        
        ops.push(WriteOperation::WriteMetadata {
            path: self.output_path("manifest.json"),
            content: self.build_manifest()?,
        });
        
        let stats = WriterStats {
            frames_written: self.frame_buffer.len(),
        };
        
        Ok((stats, ops))
    }
}
```

### Step 3: Register Format

```rust
// formats/mod.rs
pub enum DatasetFormat {
    Lerobot,
    MyFormat,  // New format
}

pub fn create_writer(
    format: DatasetFormat,
    config: &DatasetConfig,
) -> Result<Box<dyn DatasetWriter>> {
    match format {
        DatasetFormat::Lerobot => {
            Ok(Box::new(LeRobotWriter::new(config.as_lerobot()?)))
        }
        DatasetFormat::MyFormat => {
            Ok(Box::new(MyFormatWriter::new(config.as_myformat()?)))
        }
    }
}
```

### Step 4: Test the New Format

```rust
#[test]
fn test_myformat_writer() {
    let config = MyFormatConfig {
        dataset_name: "test".to_string(),
        fps: 30,
        compression: CompressionType::Zstd,
    };
    
    let mut writer = MyFormatWriter::new(config);
    
    // Test with sample frames
    writer.write_frame(&test_frame())?;
    writer.write_frame(&test_frame())?;
    
    let (stats, ops) = writer.finalize()?;
    
    // Verify format-specific output
    assert_eq!(stats.frames_written, 2);
    assert!(matches!(ops[0], WriteOperation::WriteFile { path, .. } 
        if path.extension() == Some("bin")));
    
    // Test with mock sink
    let sink = VecSink::new();
    sink.execute_batch(ops)?;
    
    // Verify operations are valid for your format
    let stored = sink.operations();
    assert_eq!(stored.len(), 2);
}
```

### Benefits for Format Authors

- **No storage knowledge needed**: Focus purely on format logic
- **Testable in isolation**: Use `VecSink` to verify output operations
- **Reuses infrastructure**: Video encoding, parquet writing provided by sink
- **Type safety**: Format-specific configs and frames at compile time

## Open Questions

1. Should `WriteOperation` be an enum or trait objects?
   - **Enum**: Static dispatch, exhaustiveness checking
   - **Trait**: Extensible without modifying pipeline crate

2. How to handle streaming video encoding for cloud?
   - Option A: Pipeline buffers all frames, sink handles streaming
   - Option B: Add `WriteOperation::StreamingVideoChunk { ... }`

3. Should operations support batching at type level?
   - `Vec<WriteOperation>` vs `WriteBatch` struct with metadata

## References

- [executor-architecture.md](./executor-architecture.md) - Distributed execution design
- [data-pipeline-design.md](./data-pipeline-design.md) - Data flow design
- `robocodec/docs/adr-002-bag-s3-streaming.md` - ADR format reference
- `crates/roboflow-storage/` - Storage abstraction crate
