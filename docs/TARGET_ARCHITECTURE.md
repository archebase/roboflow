# Target Architecture Design

**Date:** 2025-02-12
**Purpose:** Define the target architecture and migration strategy

---

## 1. Product Scope Reminder

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         ROBOFLOW                                         │
│                   "The Robotics ETL Tool"                               │
│                                                                          │
│  PURPOSE:                                                                │
│  Convert raw robotics recordings into query-ready training datasets     │
│                                                                          │
│  INPUT:                          OUTPUT:                                 │
│  • ROS bag files                 • LeRobot format                        │
│  • MCAP files                    • Parquet (state/action)               │
│  • S3 URIs                       • MP4 videos (observations)            │
│  • Raw sensor data               • Lance-compatible schema              │
│                                                                          │
│  VALUE PROPS:                                                            │
│  • Fast: GPU video encoding, zero-copy decoding                         │
│  • Scalable: Distributed processing with TiKV                           │
│  • Configurable: TOML mapping files                                     │
│  • Cloud-native: Direct S3/OSS output                                   │
│                                                                          │
│  NOT:                                                                    │
│  • Not a query engine → Use Lance                                       │
│  • Not an annotation tool → Use CVAT/Label Studio                       │
│  • Not a quality repair tool → Preprocess before roboflow               │
│  • Not a training framework → Use PyTorch/JAX                           │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Target Crate Structure

```
roboflow (workspace root)
│
├── roboflow-core        # Core types (UNCHANGED)
│   ├── error.rs
│   ├── registry.rs
│   └── value.rs
│
├── roboflow-storage     # Storage backends (UNCHANGED)
│   ├── local.rs
│   ├── s3.rs
│   ├── cached.rs
│   └── multipart.rs
│
├── roboflow-video       # NEW: Video encoding (extracted from dataset)
│   ├── encoder.rs       # VideoEncoder trait
│   ├── concurrent.rs    # ConcurrentVideoEncoder
│   ├── fragment.rs      # FragmentEncoder
│   ├── pool.rs          # EncoderPool
│   ├── gpu/
│   │   ├── nvenc.rs     # NVIDIA GPU encoding
│   │   └── videotoolbox.rs # Apple VideoToolbox
│   └── software/
│       └── ffmpeg.rs    # CPU fallback
│
├── roboflow-sources     # Input sources (IMPROVED)
│   ├── lib.rs           # Source trait
│   ├── bag.rs           # ROS bag
│   ├── mcap.rs          # MCAP
│   ├── rrd.rs           # Rerun
│   └── s3_prefix.rs     # NEW: S3 prefix input
│
├── roboflow-sinks       # Output sinks (IMPROVED)
│   ├── lib.rs           # Sink trait
│   └── lerobot.rs       # LeRobot format (CONSOLIDATED)
│
├── roboflow-dataset     # Dataset logic (SIMPLIFIED)
│   ├── lerobot/
│   │   ├── config.rs    # LerobotConfig
│   │   ├── writer.rs    # Core writer
│   │   ├── parquet.rs   # Parquet output
│   │   └── metadata.rs  # Info.json generation
│   ├── streaming/       # Frame alignment
│   └── pipeline.rs      # PipelineExecutor
│
├── roboflow-distributed # Distributed processing (SIMPLIFIED)
│   ├── coordinator.rs   # NEW: Job coordinator (simplified Worker)
│   ├── executor.rs      # NEW: Task executor (extracted from Worker)
│   ├── batch.rs         # Batch management
│   ├── merge.rs         # Result merging
│   ├── catalog.rs       # TiKV catalog
│   └── tikv/            # TiKV client
│
└── roboflow             # Main crate (NEW PUBLIC API)
    ├── lib.rs           # convert(), ConvertBuilder
    ├── config.rs        # Unified PipelineConfig
    └── report.rs        # ConversionReport
```

---

## 3. Dependency Graph (Target)

```
                         ┌─────────────────┐
                         │  roboflow-core  │
                         └────────┬────────┘
                                  │
         ┌────────────────────────┼────────────────────────┐
         │                        │                        │
         ▼                        ▼                        ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│roboflow-        │     │roboflow-        │     │roboflow-        │
│storage          │     │sources          │     │video            │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │    roboflow-sinks      │
                    │    roboflow-dataset    │
                    └────────────┬───────────┘
                                 │
                    ┌────────────┴───────────┐
                    │                        │
                    ▼                        ▼
         ┌─────────────────┐     ┌─────────────────┐
         │roboflow-        │     │roboflow         │
         │distributed      │     │(public API)     │
         │(optional)       │     │                 │
         └─────────────────┘     └─────────────────┘
```

**Key Change:** roboflow-distributed is now OPTIONAL
- Single-machine: Use roboflow directly
- Distributed: Add roboflow-distributed

---

## 4. Public API Design

### 4.1 Simple Convert Function

```rust
// roboflow/src/lib.rs

/// Convert a robotics data file to LeRobot format.
///
/// # Example
///
/// ```rust,no_run
/// use roboflow;
///
/// // Simple usage
/// roboflow::convert("input.mcap", "output/", "config.toml")?;
///
/// // Builder pattern
/// let report = roboflow::ConvertBuilder::new()
///     .input("input.mcap")
///     .output("s3://bucket/output/")
///     .config("config.toml")
///     .run()?;
///
/// println!("Converted {} frames", report.frames_total);
/// ```
pub fn convert(
    input: impl AsRef<str>,
    output: impl AsRef<str>,
    config: impl AsRef<str>,
) -> Result<ConversionReport> {
    ConvertBuilder::new()
        .input(input)
        .output(output)
        .config(config)
        .run()
}

/// Builder for conversion operations.
pub struct ConvertBuilder {
    input: Option<String>,
    output: Option<String>,
    config: Option<PipelineConfig>,
    distributed: bool,
    workers: usize,
}

impl ConvertBuilder {
    pub fn new() -> Self { ... }

    pub fn input(mut self, path: impl Into<String>) -> Self { ... }

    pub fn output(mut self, path: impl Into<String>) -> Self { ... }

    pub fn config(mut self, path: impl AsRef<str>) -> Result<Self> { ... }

    pub fn config_toml(mut self, toml: &str) -> Result<Self> { ... }

    pub fn distributed(mut self, enabled: bool) -> Self { ... }

    pub fn workers(mut self, count: usize) -> Self { ... }

    pub fn run(self) -> Result<ConversionReport> { ... }
}

/// Report from a conversion operation.
#[derive(Debug, Serialize)]
pub struct ConversionReport {
    pub input_files: usize,
    pub output_path: String,
    pub episodes_total: usize,
    pub frames_total: usize,
    pub videos_created: usize,
    pub duration_sec: f64,
    pub throughput_fps: f64,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}
```

### 4.2 Unified Pipeline Config

```rust
// roboflow/src/config.rs

/// Unified configuration for the conversion pipeline.
#[derive(Debug, Clone, Deserialize)]
pub struct PipelineConfig {
    /// Dataset configuration
    pub dataset: DatasetConfig,

    /// Topic to feature mappings
    pub mappings: Vec<TopicMapping>,

    /// Video encoding settings
    pub video: VideoConfig,

    /// Output settings
    pub output: OutputConfig,

    /// Processing settings (optional)
    #[serde(default)]
    pub processing: ProcessingConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TopicMapping {
    pub topic: String,
    pub feature: String,
    #[serde(rename = "type")]
    pub data_type: DataType,
    #[serde(default)]
    pub fields: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VideoConfig {
    #[serde(default = "default_codec")]
    pub codec: String,  // "h264", "h265"
    #[serde(default = "default_crf")]
    pub crf: u32,
    #[serde(default = "default_preset")]
    pub preset: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OutputConfig {
    pub format: OutputFormat,
    #[serde(default)]
    pub image_format: ImageOutputFormat,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProcessingConfig {
    #[serde(default = "default_workers")]
    pub workers: usize,
    #[serde(default)]
    pub distributed: bool,
}
```

### 4.3 Example Config File

```toml
# pipeline.toml - Unified configuration

[dataset]
name = "manipulation_dataset"
fps = 30
robot_type = "genie_s"

[[mappings]]
topic = "/camera/high"
feature = "observation.camera_0"
type = "image"

[[mappings]]
topic = "/joint_states"
feature = "observation.state"
type = "state"
fields = ["position"]

[[mappings]]
topic = "/joint_cmd"
feature = "action"
type = "action"

[video]
codec = "h264"
crf = 23
preset = "fast"

[output]
format = "lerobot"
image_format = "mp4"

[processing]
workers = 4
distributed = false
```

---

## 5. Worker Refactoring

### 5.1 Current Structure (God Class)

```rust
// worker/mod.rs - 1259 LOC
pub struct Worker {
    // Everything mixed together
    pod_id: String,
    tikv: TikvClient,
    config: WorkerConfig,
    metrics: WorkerMetrics,
    shutdown_handler: ShutdownHandler,
    cancellation_token: CancellationToken,
    job_registry: JobRegistry,
    config_cache: HashMap<String, LerobotConfig>,
    batch_controller: BatchController,
}

impl Worker {
    // 22 methods including:
    fn run(&mut self) { ... }
    fn find_and_claim_work_unit(&mut self) { ... }
    fn process_work_unit_with_pipeline(&mut self) { ... }
    fn complete_work_unit(&mut self) { ... }
    fn fail_work_unit(&mut self) { ... }
    fn create_lerobot_config(&mut self) { ... }
    fn send_heartbeat(&mut self) { ... }
    fn shutdown(&mut self) { ... }
    // ... more
}
```

### 5.2 Target Structure (Split)

```rust
// distributed/coordinator.rs - ~200 LOC
/// Coordinates distributed work, does NOT execute.
pub struct Coordinator {
    pod_id: String,
    tikv: TikvClient,
    heartbeat: HeartbeatManager,
    shutdown: ShutdownHandler,
}

impl Coordinator {
    /// Main loop: find work, delegate to executor, report results
    pub async fn run(&mut self, executor: &mut Executor) {
        loop {
            if self.shutdown.is_requested() {
                break;
            }

            if let Some(work) = self.claim_work().await? {
                match executor.execute(&work).await {
                    Ok(result) => self.complete_work(&work, result).await?,
                    Err(e) => self.fail_work(&work, e).await?,
                }
            }

            self.heartbeat.send().await?;
        }
    }

    async fn claim_work(&mut self) -> Result<Option<WorkUnit>> { ... }
    async fn complete_work(&mut self, work: &WorkUnit, result: ExecutionResult) { ... }
    async fn fail_work(&mut self, work: &WorkUnit, error: Error) { ... }
}

// distributed/executor.rs - ~300 LOC
/// Executes work units, does NOT coordinate.
pub struct Executor {
    config_cache: HashMap<String, LerobotConfig>,
    storage: Arc<dyn Storage>,
}

impl Executor {
    /// Execute a single work unit.
    pub async fn execute(&mut self, work: &WorkUnit) -> Result<ExecutionResult> {
        let config = self.load_config(&work.config_url).await?;
        let source = self.create_source(&work.source_url).await?;
        let sink = self.create_sink(&work.output_url, &config).await?;

        // Use the core pipeline
        let pipeline = Pipeline::new(source, sink, config);
        let stats = pipeline.run().await?;

        Ok(ExecutionResult {
            frames_processed: stats.frames_total,
            videos_created: stats.videos_total,
        })
    }
}

// distributed/worker.rs - ~100 LOC (thin wrapper)
/// Legacy Worker type for backward compatibility.
pub struct Worker {
    coordinator: Coordinator,
    executor: Executor,
}

impl Worker {
    pub fn new(config: WorkerConfig) -> Result<Self> {
        let coordinator = Coordinator::new(&config)?;
        let executor = Executor::new(&config)?;
        Ok(Self { coordinator, executor })
    }

    pub async fn run(&mut self) {
        self.coordinator.run(&mut self.executor).await
    }
}
```

---

## 6. Video Crate Design

```rust
// video/src/lib.rs

/// Video encoder trait.
pub trait VideoEncoder: Send + Sync {
    /// Encode frames to video file.
    fn encode(&mut self, frames: &[ImageFrame], output: &Path) -> Result<VideoMetadata>;

    /// Get supported codecs.
    fn codecs(&self) -> Vec<CodecInfo>;

    /// Check if hardware acceleration is available.
    fn is_hardware_accelerated(&self) -> bool;
}

/// Concurrent video encoder for multiple cameras.
pub struct ConcurrentVideoEncoder {
    pipelines: HashMap<CameraId, CameraPipeline>,
    runtime: Arc<Runtime>,
    storage: Arc<dyn Storage>,
    config: EncoderConfig,
}

/// Single camera pipeline.
pub struct CameraPipeline {
    camera_id: CameraId,
    encoder: Box<dyn VideoEncoder>,
    frame_buffer: RingBuffer<ImageFrame>,
}

// GPU backends (feature-gated)
#[cfg(feature = "nvenc")]
pub mod nvenc;

#[cfg(feature = "videotoolbox")]
pub mod videotoolbox;

// Software fallback (always available)
pub mod ffmpeg;
```

---

## 7. Migration Strategy

### Phase 1: Split Worker (Week 1)
1. Create `executor.rs` with execution logic
2. Create `coordinator.rs` with coordination logic
3. Keep `worker.rs` as thin wrapper
4. Add tests for new components
5. **No breaking changes** - Worker API unchanged

### Phase 2: Extract Video (Week 1-2)
1. Create `roboflow-video` crate
2. Move video encoding code from `roboflow-dataset`
3. Update imports in `roboflow-dataset`
4. Add feature flags for GPU backends
5. **Minimal breaking changes** - Internal refactor only

### Phase 3: Unify Input (Week 2)
1. Add `S3PrefixSource` to `roboflow-sources`
2. Ensure Source trait handles S3 URLs
3. Update Worker/Executor to use Source trait
4. **No breaking changes**

### Phase 4: Unify Output (Week 2-3)
1. Consolidate LeRobot logic in `roboflow-sinks`
2. Remove duplicate code from `roboflow-dataset`
3. Keep `roboflow-dataset` for pipeline logic only
4. **Minor breaking changes** - Internal API only

### Phase 5: Public API (Week 3)
1. Create `convert()` function
2. Create `ConvertBuilder`
3. Create `PipelineConfig`
4. Create `ConversionReport`
5. **New API, no breaking changes**

### Phase 6: Simplify Distributed (Week 3-4)
1. Make `roboflow-distributed` truly optional
2. Add local-only mode
3. Consider simpler queue-based alternative
4. **Breaking changes** - Requires migration guide

### Phase 7: Config Consolidation (Week 4)
1. Create unified `PipelineConfig`
2. Deprecate scattered configs
3. Add migration tool
4. **Breaking changes** - Requires config file updates

---

## 8. Success Criteria

| Metric | Current | Target |
|--------|---------|--------|
| Worker LOC | 1259 | <200 (wrapper) |
| Public API | None | `convert()` + builder |
| Config files | Multiple | Single TOML |
| Crates for video | 1 (buried) | 1 (first-class) |
| Local dev complexity | Requires TiKV | No dependencies |
| Time to first conversion | ~30 min setup | `cargo run -- convert ...` |

---

## 9. Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Breaking existing users | Keep Worker API as wrapper, add deprecation warnings |
| Video extraction breaks builds | Feature flags, gradual migration |
| Config changes break CI | Migration tool, backward-compatible parsing |
| Distributed simplification breaks prod | Keep TiKV path, add alternative later |

---

## Next Steps

1. ✅ Complete architecture audit
2. ✅ Define target architecture (this document)
3. Begin Phase 1: Split Worker God Class
