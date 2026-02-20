# ADR-003: Testable Architecture for Distributed Pipeline

**Author**: Sisyphus (AI Agent)  
**Date**: 2026-02-20  
**Status**: Proposed  
**Related**: [ADR-002](./adr-002-crate-architecture-refactoring.md)

## Context

The current architecture has tight coupling between crates that makes testing difficult:

1. **roboflow-dataset** requires real bag files and TiKV for testing
2. **roboflow-storage** requires S3/OSS credentials for testing  
3. **roboflow-distributed** requires TiKV and real worker coordination for testing
4. **MP4 video merging** uses byte concatenation which produces invalid files

This ADR proposes testable interfaces that allow each crate to be tested independently with mocks and in-memory implementations.

## Current Problems

### Problem 1: No Clear Boundary Between Distributed and Dataset

```
roboflow-distributed                    roboflow-dataset
┌─────────────────┐                     ┌─────────────────┐
│ LeRobotExecutor │────────────────────▶│ LerobotWriter   │
│ (tightly coupled)│  direct call       │ (concrete impl) │
└─────────────────┘                     └─────────────────┘
         │                                       ▲
         │           No trait abstraction        │
         └───────────────────────────────────────┘
```

### Problem 2: Storage Backend Requires Real Cloud Services

Testing cloud upload requires:
- S3/OSS credentials
- Network connectivity
- Real bucket setup

### Problem 3: Stats Collection Tied to TiKV

```rust
// Current: Stats stored in TiKV
let collector = TikvStatsCollector::new(client);
```

No way to test stats aggregation without TiKV.

### Problem 4: Invalid MP4 Merging (Bug)

Current code uses byte concatenation:

```rust
// crates/roboflow-storage/src/s3.rs:806-858
fn compose_objects(&self, sources: &[&Path], dest: &Path) -> Result<()> {
    // Download all sources
    for src in sources {
        self.download_file(src, &temp_path)?;
    }
    // Concatenate bytes - INVALID for regular MP4!
    for temp_file in &temp_files {
        std::io::copy(&mut reader, &mut merged_writer)?;
    }
}
```

This produces invalid MP4 files because:
- Regular MP4 has `moov` (metadata) and `mdat` (data) atoms
- Byte concatenation creates multiple `moov`/`mdat` pairs
- Result: Unplayable or corrupted video

The bug manifests when:
- `incremental_video_encoding = true` (default)
- **Multiple segments created per episode** (file size > memory limit)
- `finalize()` calls `compose_objects()` to merge segments

**Important**: Small files (< 2GB default memory limit) create only ONE segment per camera, so `compose_objects` just copies the file and produces valid MP4. The bug only appears with large files that trigger memory-based flushing.

Example:
```
1.6 GB bag file + 2 GB memory limit = Single segment = Valid MP4 (works by accident)
5.0 GB bag file + 2 GB memory limit = Multiple segments = INVALID MP4 (bug manifests)
```

## Decision

We will introduce **trait-based boundaries** between crates with:

1. **New traits** for testability: `TaskExecutor`, `StatsCollector`, `VideoComposer`
2. **Shared types** in `roboflow-core` for cross-crate communication
3. **Generic types** for compile-time testability
4. **Mock implementations** for each trait

## Proposed Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Testable Architecture                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    roboflow-core (Shared Types)                      │   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌────────────────┐ │   │
│  │  │ Conversion  │ │  Episode    │ │  Progress   │ │ VideoComposer  │ │   │
│  │  │   Task      │ │ Allocation  │ │  Reporter   │ │     Trait      │ │   │
│  │  └─────────────┘ └─────────────┘ └─────────────┘ └────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│  ┌─────────────────────────────────┼─────────────────────────────────────┐ │
│  │              roboflow-dataset (Pure Conversion)                      │ │
│  │                                │                                     │ │
│  │  ┌─────────────┐  ┌─────────────▼──────────────┐  ┌─────────────┐   │ │
│  │  │   Source    │  │   ConversionPipeline<S, W> │  │   Writer    │   │ │
│  │  │   Trait     │  │  ┌──────────────────────┐  │  │   Trait     │   │ │
│  │  │             │  │  │ Input: SourceStream  │  │  │             │   │ │
│  │  │ - read()    │  │  │ Output: Conversion   │  │  │ - write()   │   │ │
│  │  │ - seek()    │  │  │ Stats: EpisodeStats  │  │  │ - finalize()│   │ │
│  │  │ - metadata()│  │  └──────────────────────┘  │  │ - stats()   │   │ │
│  │  └─────────────┘  └────────────────────────────┘  └─────────────┘   │ │
│  │                                                                     │ │
│  │  Test helpers: MockSource, InMemoryWriter, LocalStorage            │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
│                                    │                                      │
│  ┌─────────────────────────────────┼─────────────────────────────────────┐│
│  │              roboflow-storage (I/O Abstraction)                      ││
│  │                                │                                     ││
│  │  ┌─────────────────────────────▼──────────────────────────────────┐ ││
│  │  │                       Storage Trait                              │ ││
│  │  │  - reader() / writer()                                          │ ││
│  │  │  - upload_file() / download_file()                              │ ││
│  │  │  - compose_objects() ◄── delegates to VideoComposer             │ ││
│  │  └─────────────────────────────────────────────────────────────────┘ ││
│  │                                                                     ││
│  │  Implementations: LocalStorage, S3Storage, OSSStorage, MockStorage  ││
│  └─────────────────────────────────────────────────────────────────────┘│
│                                    │                                      │
│  ┌─────────────────────────────────┼─────────────────────────────────────┐│
│  │              roboflow-distributed (Orchestration)                    ││
│  │                                │                                     ││
│  │  ┌─────────────┐  ┌─────────────▼──────────────┐  ┌─────────────┐   ││
│  │  │ WorkUnit    │  │      TaskExecutor Trait    │  │ Stats       │   ││
│  │  │   Queue     │  │  - execute(ConversionTask) │  │ Collector   │   ││
│  │  │             │  │  - returns ConversionResult│  │             │   ││
│  │  │ - claim()   │  │                            │  │ - aggregate │   ││
│  │  │ - complete()│  │ Uses: Source, Writer, Storage│  │ - merge     │   ││
│  │  └─────────────┘  └────────────────────────────┘  └─────────────┘   ││
│  │                                                                     ││
│  │  Test helpers: MockQueue, MockAllocator, MockStatsCollector        ││
│  └─────────────────────────────────────────────────────────────────────┘│
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

## Detailed Interface Designs

### 1. Shared Types (roboflow-core)

```rust
// crates/roboflow-core/src/task.rs
/// Self-contained conversion task - boundary object between distributed and dataset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversionTask {
    pub task_id: String,
    pub batch_id: String,
    pub input_source: InputSource,
    pub output_destination: OutputDestination,
    pub episode_allocation: EpisodeAllocation,
    pub config_hash: String,
    pub config: ConversionConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InputSource {
    Local { path: PathBuf },
    S3 { url: String },
    OSS { url: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OutputDestination {
    Local { path: PathBuf },
    Cloud { storage_url: String, local_buffer: PathBuf },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversionResult {
    pub task_id: String,
    pub episode_index: u64,
    pub chunk_index: u32,
    pub frames_processed: usize,
    pub frames_written: usize,
    pub stats: EpisodeStats,
    pub output_files: Vec<OutputFile>,
    pub duration_secs: f64,
}
```

### 2. VideoComposer Trait (Fixes MP4 Merging Bug)

**When is this needed?**
- Small files (< 2GB default memory limit): Single segment, no composition needed
- Large files (> 2GB): Multiple segments created due to memory flushing, **composition REQUIRED**
- Current `LocalStorage::compose_objects` uses byte concatenation which produces **invalid MP4** with multiple segments

```rust
// crates/roboflow-core/src/video/composer.rs
/// Composes multiple video segments into a single valid video file.
///
/// This trait fixes the current bug where byte concatenation produces
/// invalid MP4 files. Implementations must perform proper remuxing.
///
/// NOTE: This trait is SYNCHRONOUS because Storage::compose_objects is sync.
/// Video composition is CPU-bound and blocking anyway.
pub trait VideoComposer: Send + Sync {
    /// Compose multiple video segments into a single file.
    ///
    /// # Arguments
    /// * `sources` - Paths to segment files (must be same codec/resolution/fps)
    /// * `dest` - Output path for merged video
    ///
    /// # Implementation Notes
    /// - Uses rsmpeg (in-process FFmpeg) for composition via concat demuxer
    /// - Performs stream copy (no re-encode) for efficiency
    /// - Must handle timestamp continuity across segments
    fn compose(
        &self, sources: &[&Path], dest: &Path
    ) -> Result<()>;

    /// Check if sources can be composed (same format, codec, etc.)
    fn can_compose(
        &self, sources: &[&Path]
    ) -> Result<()>;
}

/// Implementation using rsmpeg (native FFmpeg bindings)
pub struct RsmpegVideoComposer;

impl RsmpegVideoComposer {
    pub fn new() -> Self {
        Self
    }
}

impl VideoComposer for RsmpegVideoComposer {
    fn compose(&self, sources: &[&Path], dest: &Path) -> Result<()> {
        use rsmpeg::avformat::{AVFormatContextInput, AVFormatContextOutput};
        use rsmpeg::avcodec::AVCodecContext;
        use rsmpeg::avutil::AVRational;
        use rsmpeg::ffi;
        use std::ffi::CString;

        if sources.is_empty() {
            return Err(ConversionError::NoSources);
        }

        // Open first source to get codec parameters
        let first_input = unsafe {
            AVFormatContextInput::open(&CString::new(sources[0].to_str().unwrap())?)
        }.map_err(|e| ConversionError::Rsmpeg(e.to_string()))?;

        // Create output context
        let mut output_ctx = unsafe {
            AVFormatContextOutput::create(&CString::new(dest.to_str().unwrap())?)
        }.map_err(|e| ConversionError::Rsmpeg(e.to_string()))?;

        // Copy streams from first input
        for (i, stream) in first_input.streams().iter().enumerate() {
            let mut out_stream = output_ctx.new_stream();
            let codecpar = stream.codecpar();
            out_stream.set_codecpar(codecpar);
            out_stream.set_time_base(stream.time_base());
        }

        // Write header
        output_ctx.write_header(None)
            .map_err(|e| ConversionError::Rsmpeg(e.to_string()))?;

        // Process each source file
        let mut pts_offset: i64 = 0;
        let mut last_dts: i64 = 0;

        for source_path in sources {
            // Open input file
            let mut input_ctx = unsafe {
                AVFormatContextInput::open(&CString::new(source_path.to_str().unwrap())?)
            }.map_err(|e| ConversionError::Rsmpeg(e.to_string()))?;

            // Read and write packets with adjusted timestamps
            while let Some((mut packet, stream_index)) = input_ctx.read_packet() {
                // Adjust timestamps for continuous playback
                packet.set_pts(packet.pts() + pts_offset);
                packet.set_dts(packet.dts() + pts_offset);

                // Write packet to output
                output_ctx.write_frame(&mut packet)
                    .map_err(|e| ConversionError::Rsmpeg(e.to_string()))?;

                last_dts = packet.dts();
            }

            // Update offset for next file
            pts_offset = last_dts + 1;
        }

        // Write trailer
        output_ctx.write_trailer()
            .map_err(|e| ConversionError::Rsmpeg(e.to_string()))?;

        tracing::info!(
            sources = sources.len(),
            dest = %dest.display(),
            "Video composition complete"
        );

        Ok(())
    }

    fn can_compose(&self, sources: &[&Path]) -> Result<()> {
        // Verify all sources have same codec, resolution, and fps
        // Implementation checks first frame of each source
        Ok(())
    }
}

#[async_trait::async_trait]
impl VideoComposer for FfmpegVideoComposer {
    async fn compose(&self, sources: &[&Path], dest: &Path) -> Result<()> {
        // Create concat demuxer file list
        let file_list = create_concat_list(sources)?;
        
        // Use FFmpeg concat demuxer with stream copy
        // ffmpeg -f concat -safe 0 -i filelist.txt -c copy output.mp4
        let output = Command::new(&self.ffmpeg_path)
            .args([
                "-f", "concat",
                "-safe", "0",
                "-i", &file_list,
                "-c", "copy",  // Stream copy, no re-encode
                "-movflags", "+faststart",
                "-y",
            ])
            .arg(dest)
            .output()?;
            
        if !output.status.success() {
            return Err(ConversionError::VideoCompose {
                sources: sources.iter().map(|p| p.to_path_buf()).collect(),
                error: String::from_utf8_lossy(&output.stderr).to_string(),
            });
        }
        
        Ok(())
    }
}

/// Mock implementation for testing
pub struct MockVideoComposer {
    operations: Arc<Mutex<Vec<ComposeOperation>>>,
}

impl VideoComposer for MockVideoComposer {
    fn compose(&self, sources: &[&Path], dest: &Path) -> Result<()> {
        self.operations.lock().unwrap().push(ComposeOperation {
            sources: sources.iter().map(|p| p.to_path_buf()).collect(),
            dest: dest.to_path_buf(),
        });
        Ok(())
    }
}
```

### 3. Storage Trait Update

```rust
// crates/roboflow-storage/src/traits.rs
pub trait Storage: Send + Sync {
    fn reader(&self, path: &Path) -> StorageResult<Box<dyn Read + Send>>;
    fn writer(&self, path: &Path) -> StorageResult<Box<dyn Write + Send>>;
    fn upload_file(&self, local: &Path, remote: &Path) -> StorageResult<u64>;
    
    /// Compose objects using VideoComposer for proper format handling.
    /// NOTE: This is SYNCHRONOUS - runs in blocking thread pool if needed.
    fn compose_objects(
        &self, 
        sources: &[&Path], 
        dest: &Path,
        composer: &dyn VideoComposer,  // NEW: Inject composer
    ) -> StorageResult<()>;
}

/// Mock storage with operation tracking
pub struct MockStorage {
    files: RwLock<HashMap<PathBuf, Vec<u8>>>,
    operations: RwLock<Vec<StorageOperation>>,
}

impl Storage for MockStorage {
    fn compose_objects(
        &self,
        sources: &[&Path],
        dest: &Path,
        composer: &dyn VideoComposer,
    ) -> StorageResult<()> {
        self.operations.write().unwrap().push(
            StorageOperation::Compose { 
                sources: sources.iter().map(|p| p.to_path_buf()).collect(),
                dest: dest.to_path_buf(),
            }
        );
        
        // Use the injected composer (synchronous)
        composer.compose(sources, dest)?;
        Ok(())
    }
}
```

### 4. TaskExecutor Trait

```rust
// crates/roboflow-dataset/src/executor.rs
#[async_trait::async_trait]
pub trait TaskExecutor: Send + Sync {
    /// Execute a conversion task end-to-end
    async fn execute(&self, task: ConversionTask) -> Result<ConversionResult>;
    
    /// Validate task before execution
    fn validate(&self, task: &ConversionTask) -> Result<()>;
}

/// Production implementation
pub struct PipelineExecutor<S: Storage> {
    storage: Arc<S>,
    video_composer: Arc<dyn VideoComposer>,
    progress_reporter: Option<Arc<dyn ProgressReporter>>,
}

#[async_trait::async_trait]
impl<S: Storage> TaskExecutor for PipelineExecutor<S> {
    async fn execute(&self, task: ConversionTask) -> Result<ConversionResult> {
        // 1. Download input if needed
        let local_input = self.ensure_local_input(&task.input_source).await?;
        
        // 2. Create source
        let source = SourceFactory::create(&local_input)?;
        
        // 3. Create writer with episode allocation
        let writer = self.create_writer(&task)?;
        
        // 4. Run conversion pipeline with generics
        let pipeline = ConversionPipeline::new(source, writer, task.config);
        let result = pipeline.run().await?;
        
        // 5. Upload outputs if needed
        self.upload_outputs(&task, &result).await?;
        
        Ok(result)
    }
}
```

### 5. Generic ConversionPipeline

```rust
// crates/roboflow-dataset/src/pipeline.rs
/// Testable conversion pipeline using generics
pub struct ConversionPipeline<S: Source, W: DatasetWriter> {
    source: S,
    writer: W,
    config: ConversionConfig,
    progress_reporter: Option<Arc<dyn ProgressReporter>>,
}

impl<S: Source, W: DatasetWriter> ConversionPipeline<S, W> {
    pub fn new(source: S, writer: W, config: ConversionConfig) -> Self {
        Self { source, writer, config, progress_reporter: None }
    }
    
    pub async fn run(mut self) -> Result<ConversionResult> {
        // Initialize source
        let metadata = self.source.initialize(&self.config.source_config).await?;
        
        // Configure writer
        self.writer.set_episode_index(self.config.episode_index);
        
        // Process messages
        let mut messages_read = 0;
        loop {
            match self.source.read_batch(100).await? {
                Some(messages) => {
                    messages_read += messages.len();
                    self.process_batch(&messages)?;
                }
                None => break,
            }
        }
        
        // Finalize and get stats
        let stats = self.writer.finalize()?;
        
        Ok(ConversionResult {
            task_id: self.config.task_id.clone(),
            episode_index: self.config.episode_index as u64,
            chunk_index: self.config.chunk_index,
            frames_processed: messages_read,
            frames_written: stats.frames_written,
            stats: stats.episode_stats.unwrap_or_default(),
            output_files: stats.output_files,
            duration_secs: start_time.elapsed().as_secs_f64(),
        })
    }
}

// ============== TESTING HELPERS ==============

/// Mock source for testing
pub struct MockSource {
    messages: Vec<TimestampedMessage>,
    index: usize,
}

#[async_trait::async_trait]
impl Source for MockSource {
    async fn read_batch(&mut self, size: usize) -> SourceResult<Option<Vec<TimestampedMessage>>> {
        if self.index >= self.messages.len() {
            return Ok(None);
        }
        let end = (self.index + size).min(self.messages.len());
        let batch = self.messages[self.index..end].to_vec();
        self.index = end;
        Ok(Some(batch))
    }
}

/// In-memory writer for testing
pub struct InMemoryWriter {
    frames: Vec<Frame>,
    episode_index: usize,
}

impl DatasetWriter for InMemoryWriter {
    fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        self.frames.push(frame.clone());
        Ok(())
    }
    
    fn finalize(&mut self) -> Result<WriterStats> {
        Ok(WriterStats {
            frames_written: self.frames.len(),
            episodes_written: 1,
            bytes_written: 0,
            episode_stats: None,
            output_files: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_pipeline_with_mocks() {
        let messages = create_test_messages(100);
        let source = MockSource::with_messages(messages);
        let writer = InMemoryWriter::new();
        
        let pipeline = ConversionPipeline::new(source, writer, ConversionConfig::default());
        let result = pipeline.run().await.unwrap();
        
        assert_eq!(result.frames_written, 100);
    }
}
```

### 6. StatsCollector Trait (Align with Existing)

**NOTE**: This trait ALREADY EXISTS in `crates/roboflow-distributed/src/stats/collector.rs`. 
The ADR proposes adding an in-memory implementation for testing, not changing the trait.

```rust
// crates/roboflow-distributed/src/stats/collector.rs (EXISTING - do not modify)
#[async_trait]
pub trait StatsCollector: Debug + Send + Sync {
    /// Record statistics for a single episode.
    async fn record_episode_stats(&self, batch_id: &str, stats: EpisodeStats) -> Result<()>;
    
    /// Retrieve aggregated statistics for a batch.
    async fn get_batch_stats(&self, batch_id: &str) -> Result<Option<BatchStatsSummary>>;
    
    /// Delete all statistics for a batch.
    async fn delete_batch_stats(&self, batch_id: &str) -> Result<()>;
    
    /// Check if stats collection is healthy.
    async fn is_healthy(&self) -> bool;
}
```

**NEW**: Add `InMemoryStatsCollector` for testing:

```rust
// crates/roboflow-distributed/src/stats/mock.rs (NEW)
pub struct InMemoryStatsCollector {
    stats: RwLock<HashMap<String, BatchStatsSummary>>,
}

#[async_trait]
impl StatsCollector for InMemoryStatsCollector {
    async fn record_episode_stats(&self, batch_id: &str, stats: EpisodeStats) -> Result<()> {
        let mut all_stats = self.stats.write().await;
        let batch = all_stats.entry(batch_id.to_string()).or_insert_with(|| {
            BatchStatsSummary::new(batch_id.to_string())
        });
        batch.add_episode(stats);
        Ok(())
    }
    
    async fn get_batch_stats(&self, batch_id: &str) -> Result<Option<BatchStatsSummary>> {
        Ok(self.stats.read().await.get(batch_id).cloned())
    }
    
    async fn delete_batch_stats(&self, batch_id: &str) -> Result<()> {
        self.stats.write().await.remove(batch_id);
        Ok(())
    }
    
    async fn is_healthy(&self) -> bool {
        true
    }
}
```

## Migration Path

### Phase 1: Add New Files (Non-Breaking)

```
crates/
├── roboflow-core/src/
│   ├── task.rs              # NEW: ConversionTask, ConversionResult
│   └── video/
│       └── composer.rs      # NEW: VideoComposer trait
│
├── roboflow-dataset/src/
│   ├── executor.rs          # NEW: TaskExecutor trait
│   ├── pipeline.rs          # NEW: ConversionPipeline<S, W>
│   └── testing.rs           # NEW: MockSource, InMemoryWriter
│
├── roboflow-storage/src/
│   └── mock.rs              # NEW: MockStorage
│
└── roboflow-distributed/src/
    └── stats/
        └── mock.rs          # NEW: InMemoryStatsCollector
```

### Phase 2: Fix MP4 Merging Bug

**⚠️ MIGRATION SCOPE**: This signature change affects ALL Storage implementations and call sites:
- `crates/roboflow-storage/src/local.rs:299` - LocalStorage::compose_objects
- `crates/roboflow-storage/src/s3.rs:806` - S3Storage::compose_objects  
- `crates/roboflow-storage/src/mock.rs` - MockStorage::compose_objects
- `crates/roboflow-dataset/src/formats/lerobot/writer/writer_impl.rs:1251` - merge_pending_segments caller
- All tests that use compose_objects

Update `Storage::compose_objects` signature:

```rust
// BEFORE (buggy)
fn compose_objects(&self, sources: &[&Path], dest: &Path) -> Result<()>;

// AFTER (fixed)
fn compose_objects(
    &self,
    sources: &[&Path],
    dest: &Path,
    composer: &dyn VideoComposer,
) -> Result<()>;
```

Implement `RsmpegVideoComposer` for production:

```rust
// crates/roboflow-dataset/src/media/video/composer.rs
impl LerobotWriter {
    fn merge_pending_segments(&mut self) -> Result<()> {
        // Use injected composer instead of byte concatenation
        let composer = RsmpegVideoComposer::new();
        self.storage.compose_objects(&sources, &dest, &composer)?;
    }
}
```

### Phase 3: Migrate Executors

Replace direct instantiation with trait-based injection:

```rust
// BEFORE
let executor = LeRobotExecutor::new(2, output_prefix);

// AFTER  
let executor = PipelineExecutor {
    storage: Arc::new(storage),
    video_composer: Arc::new(RsmpegVideoComposer::new()),
    progress_reporter: None,
};
```

## Testing Strategy

### Unit Tests per Crate

```rust
// roboflow-dataset: Test conversion without real bag files
#[tokio::test]
async fn test_conversion_pipeline() {
    let source = MockSource::with_messages(vec![/* synthetic data */]);
    let writer = InMemoryWriter::new();
    let pipeline = ConversionPipeline::new(source, writer, config);
    let result = pipeline.run().await.unwrap();
    assert_eq!(result.frames_written, 100);
}

// roboflow-storage: Test uploads without S3
#[test]
fn test_video_upload() {
    let storage = MockStorage::new();
    storage.upload_file(&local, &remote).unwrap();
    
    let ops = storage.get_operations();
    assert!(matches!(ops[0], StorageOperation::Upload { .. }));
}

// roboflow-distributed: Test stats without TiKV
#[tokio::test]
async fn test_stats_aggregation() {
    let collector = InMemoryStatsCollector::new();
    
    for i in 0..5 {
        let stats = EpisodeStats::new(i, 100);
        collector.record_episode_stats("batch-1", stats).await.unwrap();
    }
    
    let mut summary = collector.get_batch_stats("batch-1").await.unwrap().unwrap();
    summary.calculate_global_stats();
    assert_eq!(summary.total_episodes, 5);
}

// VideoComposer: Test without FFmpeg
#[test]
fn test_video_composition() {
    let composer = MockVideoComposer::new();
    let sources = vec![Path::new("seg0.mp4"), Path::new("seg1.mp4")];
    
    composer.compose(&sources, Path::new("out.mp4")).unwrap();
    
    let ops = composer.get_operations();
    assert_eq!(ops.len(), 1);
    assert_eq!(ops[0].sources.len(), 2);
}
```

### Integration Tests

```rust
// Cross-crate integration with mocks
#[tokio::test]
async fn test_end_to_end_with_mocks() {
    // Setup mocks
    let storage = Arc::new(MockStorage::new());
    let composer = Arc::new(MockVideoComposer::new());
    
    // Create executor
    let executor = PipelineExecutor {
        storage: storage.clone(),
        video_composer: composer.clone(),
        progress_reporter: None,
    };
    
    // Execute task
    let task = ConversionTask::default();
    let result = executor.execute(task).await.unwrap();
    
    // Verify results
    assert!(result.frames_written > 0);
    assert!(!storage.get_operations().is_empty());
}
```

## Trade-offs

### Pros

1. **Testability**: Each crate can be tested independently
2. **Bug Fix**: Proper MP4 merging instead of byte concatenation
3. **Flexibility**: Easy to swap implementations (S3 vs Local vs Mock)
4. **Type Safety**: Generics catch errors at compile time

### Cons

1. **Complexity**: More traits and abstractions to understand
2. **Performance**: Trait object dispatch has small overhead
3. **Migration Effort**: Need to update existing code

## Alternatives Considered

### Alternative 1: Keep Byte Concatenation

**Rejected**: Produces invalid MP4 files when `incremental_video_encoding=true`.

### Alternative 2: Always Use fMP4

**Rejected**: fMP4 has compatibility issues with some players. Regular MP4 is standard for LeRobot datasets.

### Alternative 3: Single Video Per Episode

**Rejected**: Would cause OOM on long recordings. Memory-bounded processing with segments is a requirement.

## Decision

We adopt the trait-based architecture with:

1. ✅ **VideoComposer trait** to fix MP4 merging bug
2. ✅ **TaskExecutor trait** for testable work execution  
3. ✅ **StatsCollector trait** for testable stats aggregation
4. ✅ **Generic ConversionPipeline<S, W>** for compile-time testability
5. ✅ **Shared types in roboflow-core** for clean interfaces
6. ✅ **Mock implementations** for each trait

## Next Steps

1. Create `roboflow-core/src/task.rs` with `ConversionTask`
2. Create `roboflow-core/src/video/composer.rs` with `VideoComposer`
3. Create `roboflow-dataset/src/executor.rs` with `TaskExecutor`
4. Create `roboflow-dataset/src/pipeline.rs` with generic `ConversionPipeline`
5. Create mock implementations in respective crates
6. Fix MP4 merging bug by using `FfmpegVideoComposer`
7. Migrate existing code to use new traits

## References

- [FFmpeg Concat Demuxer](https://ffmpeg.org/ffmpeg-formats.html#concat)
- [MP4 File Format](https://developer.apple.com/library/archive/documentation/QuickTime/QTFF/QTFFChap2/qtff2.html)
- [fMP4 vs MP4](https://www.wowza.com/blog/what-is-fragmented-mp4)
