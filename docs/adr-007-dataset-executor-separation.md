# ADR-007: Dataset-Executor Separation and Pipeline Unification

**Author**: Claude Code
**Date**: 2026-02-22
**Status**: In Progress
**Related**: [ADR-001](./adr-001-pipeline-writer-storage-separation.md), [ADR-002](./adr-002-crate-architecture-refactoring.md), [executor-architecture.md](./executor-architecture.md)

## Context

The `roboflow-dataset` crate currently contains significant execution logic that overlaps with `roboflow-executor`, creating architectural confusion and code duplication. This ADR addresses the specific issues within `roboflow-dataset` to make it cleaner and more maintainable.

### Current Problems

#### Problem 1: Duplicate Pipeline Executors

`roboflow-dataset` contains two nearly identical executor implementations:

```
roboflow-dataset/src/formats/
├── pipeline.rs           # 1,120 lines - PipelineExecutor (single-threaded)
└── parallel_pipeline.rs  # 1,091 lines - ParallelPipelineExecutor (rayon-based)
```

**Duplication**: ~70% shared logic (frame alignment, episode management, message processing)
**Divergence**: Bug fixes in one often missing in the other
**Maintenance burden**: Changes require updating both files

#### Problem 2: Execution Logic in Dataset Crate

`roboflow-dataset` should focus on **data transformation** (formats, sources, alignment), but currently contains:

- Pipeline orchestration (should be in `roboflow-executor`)
- Thread pool management (should be in `roboflow-executor`)
- Task scheduling (should be in `roboflow-executor`)

This creates confusion about which crate owns execution concerns.

#### Problem 3: Frame Alignment is Tightly Coupled

The `FrameAlignmentBuffer` is buried inside `pipeline.rs` and cannot be:
- Tested in isolation
- Reused by other executors (e.g., `roboflow-executor` stages)
- Replaced with alternative alignment strategies

#### Problem 4: Storage Abstraction Confusion

Two storage traits exist:

```rust
// roboflow-dataset/src/storage_sink.rs
pub trait Sink {
    fn execute(&self, op: WriteOperation) -> Result<()>;
}

// roboflow-storage/src/traits.rs
pub trait Storage {
    async fn read(&self, path: &Path) -> Result<Bytes>;
    async fn write(&self, path: &Path, data: Bytes) -> Result<()>;
}
```

`Sink` is only used for testing (`VecSink`), while `Storage` is the real abstraction. This dual-trait pattern causes confusion.

#### Problem 5: Complex Configuration Hierarchy

```rust
// Current: 4-level nesting
LerobotConfig
├── dataset: DatasetConfig
├── mappings: Vec<Mapping>
├── video: VideoConfig
│   ├── encoder: VideoEncoderConfig
│   └── profiles: HashMap<String, Profile>
├── flushing: FlushingConfig
└── streaming: StreamingConfig
```

Excessive nesting makes configuration hard to understand and validate.

#### Problem 6: Media Logic in Dataset Crate

Video encoding and image decoding logic is scattered within `roboflow-dataset`:

```
roboflow-dataset/src/formats/lerobot/writer/
├── encoding.rs           # Video encoding logic (~556 lines)
└── camera.rs             # Camera parameter handling

roboflow-dataset/src/formats/common/base.rs
# ImageData processing, format conversion
```

The `roboflow-media` crate already exists but `roboflow-dataset` contains duplicate/concrete media logic that should be moved there:

- **Video encoding profiles** → Should be in `roboflow-media`
- **Image format conversion** → Should be in `roboflow-media`
- **Camera parameter handling** → Should be in `roboflow-media`

**Current issues**:
- `roboflow-dataset` depends on `roboflow-media` but duplicates some functionality
- Video encoding configuration is split between crates
- Image decoding happens in frame alignment (should be delegated to media crate)

## Decision

Separate concerns to make `roboflow-dataset` a pure **data transformation library**:

1. **Extract execution logic** → `roboflow-executor`
2. **Unify pipeline executors** → Single composable implementation
3. **Extract frame alignment** → Standalone, testable component
4. **Remove Sink trait** → Use `Storage` consistently
5. **Flatten configuration** → Simpler, more intuitive structure
6. **Move media logic** → `roboflow-media` (video encoding, image decoding)

### 1. Unified Pipeline Executor

Replace two separate executors with a single executor parameterized by execution policy:

```rust
// roboflow-executor (moved from dataset)
pub struct PipelineExecutor<P: ExecutionPolicy> {
    frame_aligner: FrameAligner,
    processor: Box<dyn FrameProcessor>,
    policy: P,
}

pub trait ExecutionPolicy: Send + Sync {
    fn execute_batch<F, R>(
        &self,
        items: Vec<F>,
        processor: impl Fn(F) -> R + Send,
    ) -> Vec<R>;
}

// Sequential implementation
pub struct SequentialPolicy;
impl ExecutionPolicy for SequentialPolicy {
    fn execute_batch<F, R>(&self, items: Vec<F>, processor: impl Fn(F) -> R) -> Vec<R> {
        items.into_iter().map(processor).collect()
    }
}

// Parallel implementation
pub struct ParallelPolicy {
    thread_pool: Arc<rayon::ThreadPool>,
}

impl ExecutionPolicy for ParallelPolicy {
    fn execute_batch<F, R>(
        &self,
        items: Vec<F>,
        processor: impl Fn(F) -> R + Send,
    ) -> Vec<R> {
        self.thread_pool.install(|| items.into_par_iter().map(processor).collect())
    }
}
```

**Benefits**:
- Single source of truth for pipeline logic
- Policy is swappable at runtime
- No code duplication
- Easier testing (inject mock policy)

### 2. Extract FrameAligner

Move frame alignment into a standalone component:

```rust
// roboflow-dataset/src/alignment/mod.rs
pub struct FrameAligner {
    buffer: FrameBuffer,
    completion: CompletionCriteria,
    stats: AlignmentStats,
}

impl FrameAligner {
    pub fn new(completion: CompletionCriteria) -> Self {
        Self {
            buffer: FrameBuffer::new(),
            completion,
            stats: AlignmentStats::default(),
        }
    }

    /// Add messages and return any completed frames
    pub fn add_messages(
        &mut self,
        messages: Vec<TimestampedMessage>,
    ) -> Result<Vec<AlignedFrame>> {
        for msg in messages {
            self.buffer.add_message(msg)?;
        }
        self.buffer.extract_completed(&self.completion)
    }

    /// Flush all remaining frames (end of stream)
    pub fn flush(&mut self) -> Result<Vec<AlignedFrame>> {
        self.buffer.extract_all()
    }

    pub fn stats(&self) -> &AlignmentStats {
        &self.stats
    }
}

// FrameProcessor trait for pluggable processing
pub trait FrameProcessor: Send + Sync {
    fn process(&mut self, frame: AlignedFrame) -> Result<ProcessedFrame>;
    fn finalize(&mut self) -> Result<Stats>;
}
```

**Benefits**:
- Testable in isolation
- Reusable by `roboflow-executor` stages
- Clear API boundary

### 3. Remove Sink Trait

Eliminate the `Sink` trait and use `Storage` consistently:

```rust
// BEFORE: Two traits
pub trait Sink { ... }
pub trait Storage { ... }

// AFTER: Storage only
// roboflow-storage/src/traits.rs (existing)
pub trait Storage: Send + Sync {
    async fn read(&self, path: &Path) -> Result<Bytes>;
    async fn write(&self, path: &Path, data: Bytes) -> Result<()>;
    async fn delete(&self, path: &Path) -> Result<()>;
}

// Testing: Use mock Storage implementation
#[cfg(test)]
pub struct MockStorage {
    files: Arc<Mutex<HashMap<PathBuf, Bytes>>>,
}

#[cfg(test)]
impl Storage for MockStorage {
    async fn write(&self, path: &Path, data: Bytes) -> Result<()> {
        self.files.lock().unwrap().insert(path.to_path_buf(), data);
        Ok(())
    }
    // ... other methods
}
```

**Migration**:
- Replace `VecSink` with `MockStorage`
- Update all tests to use `MockStorage`
- Remove `storage_sink.rs`

### 4. DatasetWriter Simplification

Clean up the writer trait hierarchy:

```rust
// roboflow-dataset/src/writer.rs
pub trait DatasetWriter: Send + Sync {
    /// Write a single aligned frame
    fn write_frame(&mut self, frame: &AlignedFrame) -> Result<()>;

    /// Write multiple frames (batch optimization)
    fn write_frames(&mut self, frames: &[AlignedFrame]) -> Result<()> {
        for frame in frames {
            self.write_frame(frame)?;
        }
        Ok(())
    }

    /// Start a new episode
    fn start_episode(&mut self, task_index: Option<usize>) -> Result<usize> {
        // Default: no-op for formats that don't support episodes
        Ok(0)
    }

    /// Finish current episode
    fn finish_episode(&mut self) -> Result<EpisodeStats> {
        // Default: no-op
        Ok(EpisodeStats::default())
    }

    /// Finalize and return write operations
    fn finalize(&mut self) -> Result<(WriterStats, Vec<WriteOperation>)>;
}
```

**Key changes**:
- Remove storage dependency from writers
- Default implementations for optional methods
- Consistent return type (`WriteOperation` from ADR-001)

### 6. Move Media Logic to roboflow-media

Move video encoding and image processing from `roboflow-dataset` to `roboflow-media`:

```rust
// roboflow-media (target location)
pub struct VideoEncoder {
    config: VideoConfig,
    // ... implementation
}

impl VideoEncoder {
    pub fn new(config: VideoConfig) -> Result<Self>;
    pub fn encode_frame(&mut self, frame: &ImageData) -> Result<()>;
    pub fn finalize(self) -> Result<Vec<u8>>;
}

pub struct ImageDecoder {
    // ... implementation
}

impl ImageDecoder {
    pub fn decode(data: &[u8], format: ImageFormat) -> Result<ImageData>;
    pub fn decode_to_rgb(data: &[u8]) -> Result<RgbImage>;
}
```

**Migration**:
- Move `encoding.rs` logic → `roboflow-media/src/video/encoder.rs`
- Move image decoding → `roboflow-media/src/image/decode.rs`
- Dataset crate calls media crate APIs
- Video profiles move to `roboflow-media/src/video/profiles.rs`

**Benefits**:
- Single source of truth for media processing
- `roboflow-media` can be tested independently
- Easier to add hardware acceleration (GPU) in one place
- Other crates can use media functionality without depending on dataset

### 5. Flatten Configuration

Simplify `LerobotConfig` structure:

```rust
// BEFORE: Deep nesting
pub struct LerobotConfig {
    pub dataset: DatasetConfig,
    pub mappings: Vec<Mapping>,
    pub video: VideoConfig,
    pub flushing: FlushingConfig,
    pub streaming: StreamingConfig,
}

// AFTER: Flat structure with sensible defaults
pub struct LerobotConfig {
    // Dataset metadata
    pub dataset_name: String,
    pub fps: u32,
    pub robot_type: Option<String>,

    // Topic mappings
    pub mappings: Vec<TopicMapping>,

    // Video encoding (optional, uses defaults)
    #[serde(default)]
    pub video: VideoOptions,

    // Memory management (optional, uses defaults)
    #[serde(default)]
    pub memory: MemoryOptions,
}

#[derive(Default)]
pub struct VideoOptions {
    pub encoder: VideoEncoder,
    pub quality: QualityPreset,
}

#[derive(Default)]
pub struct MemoryOptions {
    pub max_frame_buffer: usize,
    pub max_concurrent_encodes: usize,
}
```

**Benefits**:
- Easier to understand
- Simpler validation
- Better IDE support (flat structure)
- Sensible defaults reduce boilerplate

## Module Structure (Target)

```
roboflow-dataset/src/
├── lib.rs                    # Public exports
├── error.rs                  # Unified error types
├── frame.rs                  # AlignedFrame, ImageData
├── alignment/
│   ├── mod.rs                # FrameAligner
│   ├── buffer.rs             # FrameBuffer (private)
│   ├── completion.rs         # CompletionCriteria
│   └── stats.rs              # AlignmentStats
├── formats/
│   ├── mod.rs                # DatasetFormat enum, registry
│   ├── common/               # Shared format utilities
│   │   ├── config.rs         # Common configuration
│   │   ├── base.rs           # DatasetFrame, etc.
│   │   └── message_utils.rs  # Message extraction
│   └── lerobot/              # LeRobot format
│       ├── mod.rs            # Public exports
│       ├── config.rs         # LerobotConfig (flattened)
│       ├── writer.rs         # LeRobotWriter
│       ├── episode.rs        # Episode management
│       └── metadata.rs       # Metadata generation
├── sources/
│   ├── mod.rs                # Source trait
│   ├── registry.rs           # Source registration
│   ├── bag.rs                # ROS bag source
│   ├── mcap.rs               # MCAP source
│   └── s3_prefix.rs          # S3 batch source
├── writer.rs                 # DatasetWriter trait
└── testing.rs                # MockStorage, test utilities
```

**Deleted**:
- `storage_sink.rs` (use `MockStorage` instead)
- `parallel_pipeline.rs` (unified into executor)
- `pipeline.rs` (moved to executor)
- `formats/lerobot/writer/encoding.rs` (moved to roboflow-media)
- `formats/common/base.rs` image decoding (moved to roboflow-media)

## Dependency Graph (Target)

```
┌─────────────────────────────────────────────────────────────┐
│                   roboflow-distributed                      │
│              (distributed orchestration)                    │
│                                                             │
│  Depends on: roboflow-executor (execution)                  │
│              roboflow-dataset (FrameAligner, writers)       │
└─────────────────────────────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   roboflow-executor                         │
│              (execution engine)                             │
│                                                             │
│  - PipelineExecutor<Policy>                                 │
│  - Stage/Task traits                                        │
│  - ExecutionPolicy trait                                    │
│                                                             │
│  Depends on: roboflow-dataset (FrameAligner trait)          │
│              roboflow-core                                  │
└─────────────────────────────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   roboflow-dataset                          │
│              (data transformation library)                  │
│                                                             │
│  - FrameAligner                                             │
│  - Sources (bag, mcap)                                      │
│  - DatasetWriter implementations                            │
│  - Format configurations                                    │
│                                                             │
│  Depends on: roboflow-storage (Storage trait)               │
│              roboflow-media (video, image)                  │
│              roboflow-core                                  │
│              robocodec                                      │
└─────────────────────────────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  roboflow-storage │ roboflow-media │ roboflow-core │ robocodec
└─────────────────────────────────────────────────────────────┘
```

## Consequences

### Positive

| Aspect | Before | After |
|--------|--------|-------|
| **Code duplication** | 800+ lines duplicated between executors | Single `PipelineExecutor` with policy |
| **Testability** | Frame alignment tied to pipeline | `FrameAligner` tested in isolation |
| **Crate boundaries** | Dataset crate does execution | Clear separation: executor = execution, dataset = transformation |
| **Storage traits** | Confusing `Sink` + `Storage` | Single `Storage` trait |
| **Configuration** | 4-level nesting | Flat structure with defaults |
| **Lines of code** | ~2,200 in pipeline files | ~800 (unified) + extracted alignment |
| **Media logic** | Video/image in dataset crate | Centralized in roboflow-media |

### Trade-offs

| Aspect | Consideration |
|--------|--------------|
| **Refactoring effort** | Significant code movement between crates |
| **API changes** | Breaking changes to `DatasetWriter` trait |
| **Import complexity** | May need more imports (separate aligner) |

## Implementation Plan

### Status Legend
- 🔴 **Not Started** - Phase not yet begun
- 🟡 **In Progress** - Phase actively being worked on
- 🟢 **Completed** - Phase finished and verified
- ⏸️ **Blocked** - Phase blocked by dependency

---

### Phase 1: Create FrameAligner Component 🟢

**Completed**: 2026-02-22

**Goal**: Extract frame alignment into standalone, testable component

**Deliverables**:
| # | Task | Status | Files Changed |
|---|------|--------|---------------|
| 1.1 | Create `alignment/` module structure | 🟢 | `src/formats/alignment/mod.rs` |
| 1.2 | Move `FrameAlignmentBuffer` → `FrameBuffer` (internal) | 🟢 | `src/formats/alignment/buffer.rs` |
| 1.3 | Create `FrameAligner` public API | 🟢 | `src/formats/alignment/mod.rs` |
| 1.4 | Define `FrameProcessor` trait | 🟢 | `src/formats/alignment/processor.rs` (new) |
| 1.5 | Move `CompletionCriteria` to alignment module | 🟢 | `src/formats/alignment/completion.rs` |
| 1.6 | Move `AlignmentStats` to alignment module | 🟢 | `src/formats/alignment/stats.rs` |
| 1.7 | Update imports in existing code | 🟢 | Already using correct imports |
| 1.8 | Add comprehensive unit tests | 🟢 | Tests in each module file |
| 1.9 | Verify backward compatibility | 🟢 | All tests pass |

**Acceptance Criteria**:
- [x] `FrameAligner` can be instantiated and used independently
- [x] All existing tests pass with new structure
- [x] New unit tests achieve >90% coverage for alignment logic
- [x] No breaking changes to public API (temporarily)

**Notes**: The alignment module already existed in `formats/alignment/`. Added `FrameAligner` high-level API and `FrameProcessor` trait. Module stays at `formats/alignment/` for now (will move to `alignment/` in Phase 8).

**Estimated Effort**: 2-3 days (actual: 0.5 days)

---

### Phase 2: Unify Pipeline Executors 🟡

**Goal**: Replace dual executors with single policy-based executor

**Deliverables**:
| # | Task | Status | Files Changed |
|---|------|--------|---------------|
| 2.1 | Create `ExecutionPolicy` trait in `roboflow-executor` | 🟢 | `roboflow-executor/src/policy/mod.rs` (new) |
| 2.2 | Implement `SequentialPolicy` | 🟢 | `roboflow-executor/src/policy/sequential.rs` (new) |
| 2.3 | Implement `ParallelPolicy` using rayon | 🟢 | `roboflow-executor/src/policy/parallel.rs` (new) |
| 2.4 | Create unified `PipelineExecutor<P>` | 🟢 | `roboflow-executor/src/pipeline_executor.rs` (new) |
| 2.5 | Add deprecation warnings to old executors | 🟢 | `src/formats/pipeline.rs`, `src/formats/parallel_pipeline.rs` |
| 2.6 | Create `DatasetPipelineExecutor` for dataset use case | 🟢 | `src/formats/unified_executor.rs` (new) |
| 2.7 | Migrate internal usage to new executor | 🔴 | `src/formats/` |
| 2.8 | Delete `pipeline.rs` and `parallel_pipeline.rs` | 🔴 | Files removed |
| 2.9 | Update all tests to use new executor | 🔴 | Test files |

**Acceptance Criteria**:
- [x] `ExecutionPolicy` trait with sequential and parallel implementations
- [x] Old executors marked as deprecated with migration guidance
- [x] New `DatasetPipelineExecutor` works with `FormatWriter` and `AlignedFrame`
- [ ] All internal code migrated to new executor
- [ ] Old pipeline files removed
- [ ] All tests pass with new executor

**Notes**:
- Core infrastructure completed: ExecutionPolicy trait, SequentialPolicy, ParallelPolicy
- Created two executor types:
  1. `roboflow_executor::PipelineExecutor<P, T>` - Generic batch processor with policy
  2. `roboflow_dataset::DatasetPipelineExecutor<W, P>` - Dataset-specific executor working with FormatWriter
- The dataset-specific executor was needed because the generic one uses `FrameForProcessing<Vec<u8>>` while datasets work with `AlignedFrame`
- Old pipeline files deprecated but functional - migration is incremental
- Re-exports ExecutionPolicy types from roboflow_executor for convenience

**Estimated Effort**: 3-4 days (in progress: 2.5 days)
**Depends On**: Phase 1 (FrameAligner extraction)

---

### Phase 3: Remove Sink Trait 🔴

**Goal**: Eliminate dual storage abstraction, use `Storage` consistently

**Deliverables**:
| # | Task | Status | Files Changed |
|---|------|--------|---------------|
| 3.1 | Create `MockStorage` in `roboflow-storage` | 🔴 | `roboflow-storage/src/mock.rs` |
| 3.2 | Add `MockStorage` tests | 🔴 | `roboflow-storage/tests/mock_tests.rs` |
| 3.3 | Migrate dataset tests from `VecSink` to `MockStorage` | 🔴 | `tests/` files |
| 3.4 | Update `testing.rs` utilities | 🔴 | `src/testing.rs` |
| 3.5 | Remove `storage_sink.rs` | 🔴 | File deleted |
| 3.6 | Remove `Sink` trait references | 🔴 | All files |
| 3.7 | Update documentation | 🔴 | Doc comments |

**Acceptance Criteria**:
- [ ] `MockStorage` implements full `Storage` trait
- [ ] All tests pass without `VecSink`
- [ ] No references to `Sink` trait remain
- [ ] Test coverage maintained

**Estimated Effort**: 2 days

---

### Phase 4: Simplify DatasetWriter Trait 🔴

**Goal**: Clean up writer hierarchy with default implementations

**Deliverables**:
| # | Task | Status | Files Changed |
|---|------|--------|---------------|
| 4.1 | Add default implementations for optional methods | 🔴 | `src/writer.rs` |
| 4.2 | Remove storage dependency from writers | 🔴 | `src/formats/lerobot/writer/` |
| 4.3 | Update `LeRobotWriter` to use `WriteOperation` return | 🔴 | `src/formats/lerobot/writer/writer_impl.rs` |
| 4.4 | Update `FormatWriter` implementations | 🔴 | `src/formats/*/writer.rs` |
| 4.5 | Update tests for new trait signatures | 🔴 | Test files |
| 4.6 | Verify all writers compile | 🔴 | `cargo build` |

**Acceptance Criteria**:
- [ ] `DatasetWriter` has sensible defaults for all optional methods
- [ ] Writers no longer own storage
- [ ] `finalize()` returns `(WriterStats, Vec<WriteOperation>)`
- [ ] All writers implement simplified trait correctly

**Estimated Effort**: 2-3 days
**Depends On**: Phase 3 (Storage unification)

---

### Phase 5: Flatten Configuration 🔴

**Goal**: Simplify config structure with sensible defaults

**Deliverables**:
| # | Task | Status | Files Changed |
|---|------|--------|---------------|
| 5.1 | Design flattened `LerobotConfig` structure | 🔴 | Design doc |
| 5.2 | Create new config types (`VideoOptions`, `MemoryOptions`) | 🔴 | `src/formats/lerobot/config.rs` |
| 5.3 | Implement `Default` for all config types | 🔴 | Config files |
| 5.4 | Add serde default attributes | 🔴 | Config files |
| 5.5 | Update TOML parsing tests | 🔴 | `tests/lerobot/config_tests.rs` |
| 5.6 | Add validation logic | 🔴 | `src/formats/lerobot/config.rs` |
| 5.7 | Create migration guide for old configs | 🔴 | Documentation |
| 5.8 | Update example configs | 🔴 | `examples/` |

**Acceptance Criteria**:
- [ ] Flat config has maximum 2-level nesting
- [ ] All optional fields have sensible defaults
- [ ] Existing configs still parse (backward compatible)
- [ ] New validation catches misconfigurations early
- [ ] Documentation shows new structure

**Estimated Effort**: 2-3 days

---

### Phase 6: Move Video Encoding to roboflow-media 🔴

**Goal**: Centralize video encoding in media crate

**Deliverables**:
| # | Task | Status | Files Changed |
|---|------|--------|---------------|
| 6.1 | Analyze current video encoding in dataset | 🔴 | Review `encoding.rs` |
| 6.2 | Design media crate video encoder API | 🔴 | `roboflow-media/src/video/encoder.rs` |
| 6.3 | Move video encoding logic | 🔴 | From dataset to media |
| 6.4 | Move video profiles | 🔴 | `roboflow-media/src/video/profiles.rs` |
| 6.5 | Update `roboflow-dataset` to use media APIs | 🔴 | `src/formats/lerobot/writer/` |
| 6.6 | Add video encoder tests in media crate | 🔴 | `roboflow-media/tests/` |
| 6.7 | Verify video output unchanged | 🔴 | Integration tests |
| 6.8 | Remove old encoding.rs | 🔴 | File deleted |

**File Migrations**:
```
roboflow-dataset/src/formats/lerobot/writer/encoding.rs
  → roboflow-media/src/video/encoder.rs

roboflow-dataset/src/formats/lerobot/video_profiles.rs
  → roboflow-media/src/video/profiles.rs
```

**Acceptance Criteria**:
- [ ] Video encoding produces identical output
- [ ] `roboflow-media` has comprehensive tests
- [ ] `roboflow-dataset` depends on `roboflow-media` for encoding
- [ ] No video encoding logic remains in dataset crate

**Estimated Effort**: 3-4 days

---

### Phase 7: Move Image Decoding to roboflow-media 🔴

**Goal**: Centralize image processing in media crate

**Deliverables**:
| # | Task | Status | Files Changed |
|---|------|--------|---------------|
| 7.1 | Identify image decoding locations | 🔴 | `src/formats/common/base.rs`, etc. |
| 7.2 | Design media crate image decoder API | 🔴 | `roboflow-media/src/image/decode.rs` |
| 7.3 | Move image decoding logic | 🔴 | From dataset to media |
| 7.4 | Update `ImageData` to use media crate | 🔴 | `src/formats/common/base.rs` |
| 7.5 | Add image decoder tests | 🔴 | `roboflow-media/tests/` |
| 7.6 | Verify image processing unchanged | 🔴 | Integration tests |

**Acceptance Criteria**:
- [ ] Image decoding produces identical output
- [ ] All image formats supported previously still work
- [ ] `roboflow-dataset` delegates to `roboflow-media`

**Estimated Effort**: 2-3 days
**Depends On**: Phase 6 (Video encoding move)

---

### Phase 8: Update Module Structure 🔴

**Goal**: Clean up module hierarchy

**Deliverables**:
| # | Task | Status | Files Changed |
|---|------|--------|---------------|
| 8.1 | Reorganize `formats/` directory | 🔴 | Directory structure |
| 8.2 | Update `lib.rs` exports | 🔴 | `src/lib.rs` |
| 8.3 | Reorganize `sources/` directory | 🔴 | Directory structure |
| 8.4 | Verify public API is clean | 🔴 | `cargo doc` |
| 8.5 | Update module documentation | 🔴 | Module docs |

**Target Structure**:
```
roboflow-dataset/src/
├── lib.rs                    # Public exports
├── error.rs                  # Unified error types
├── frame.rs                  # AlignedFrame, ImageData
├── alignment/
│   ├── mod.rs                # FrameAligner
│   ├── buffer.rs             # FrameBuffer (private)
│   ├── completion.rs         # CompletionCriteria
│   └── stats.rs              # AlignmentStats
├── formats/
│   ├── mod.rs                # DatasetFormat enum
│   ├── common/               # Shared format utilities
│   └── lerobot/              # LeRobot format
├── sources/
│   ├── mod.rs                # Source trait
│   ├── registry.rs           # Source registration
│   ├── bag.rs                # ROS bag source
│   ├── mcap.rs               # MCAP source
│   └── s3_prefix.rs          # S3 batch source
├── writer.rs                 # DatasetWriter trait
└── testing.rs                # Test utilities
```

**Acceptance Criteria**:
- [ ] Module structure matches target
- [ ] All public items have documentation
- [ ] `cargo doc` generates without warnings
- [ ] No dead code or unused modules

**Estimated Effort**: 1-2 days
**Depends On**: Phases 1-7

---

### Phase 9: Final Verification 🔴

**Goal**: Ensure everything works together

**Deliverables**:
| # | Task | Status | Verification Method |
|---|------|--------|---------------------|
| 9.1 | Full test suite pass | 🔴 | `cargo test --all` |
| 9.2 | No compilation warnings | 🔴 | `cargo build --all-targets` |
| 9.3 | Clippy clean | 🔴 | `cargo clippy --all-targets -- -D warnings` |
| 9.4 | No circular dependencies | 🔴 | `cargo tree` |
| 9.5 | Code coverage check | 🔴 | `cargo llvm-cov` |
| 9.6 | API documentation complete | 🔴 | `cargo doc` review |
| 9.7 | Performance regression test | 🔴 | Benchmark comparison |
| 9.8 | Integration test pass | 🔴 | End-to-end conversion test |

**Acceptance Criteria**:
- [ ] All tests pass
- [ ] Coverage >= previous level
- [ ] No clippy warnings
- [ ] Performance within 5% of baseline
- [ ] Documentation complete

**Estimated Effort**: 2 days
**Depends On**: All previous phases

---

## Implementation Timeline

```
Week 1:  [====Phase 1====][==Phase 2==]
Week 2:  [==Phase 2 cont==][=Phase 3=][==Phase 4==]
Week 3:  [==Phase 4 cont==][===Phase 5===][==Phase 6==]
Week 4:  [==Phase 6 cont==][=Phase 7=][=Phase 8=][Phase 9]
```

**Total Estimated Time**: 4 weeks
**Risk Buffer**: Add 1 week for unexpected issues

---

## Progress Tracking Template

Use this template to update status in this ADR:

```markdown
### Phase X: Name [STATUS]

**Completed**: YYYY-MM-DD

| # | Task | Status |
|---|------|--------|
| X.1 | Task name | 🟢 |
| X.2 | Task name | 🟡 |
| X.3 | Task name | 🔴 |

**Blockers**: None / Description
**Notes**: Any relevant notes
```

## Testing Strategy

### 1. FrameAligner Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_aligner_returns_no_frames() {
        let aligner = FrameAligner::new(CompletionCriteria::any());
        assert!(aligner.flush().unwrap().is_empty());
    }

    #[test]
    fn test_aligner_groups_by_timestamp() {
        let mut aligner = FrameAligner::new(CompletionCriteria::any());

        let frames = aligner.add_messages(vec![
            message("cam", timestamp(100)),
            message("cam", timestamp(200)),
            message("state", timestamp(100)),
        ]).unwrap();

        // Should complete frame at t=100 (has both cam and state)
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].timestamp, 100);
    }

    #[test]
    fn test_aligner_respects_completion_criteria() {
        // Require specific features
        let criteria = CompletionCriteria::requires(&["cam", "state", "action"]);
        let mut aligner = FrameAligner::new(criteria);

        let frames = aligner.add_messages(vec![
            message("cam", timestamp(100)),
            message("state", timestamp(100)),
            // Missing "action"
        ]).unwrap();

        // Frame not complete
        assert!(frames.is_empty());

        // Add missing feature
        let frames = aligner.add_messages(vec![
            message("action", timestamp(100)),
        ]).unwrap();

        // Now complete
        assert_eq!(frames.len(), 1);
    }
}
```

### 2. Policy Tests

```rust
#[test]
fn test_policies_produce_same_results() {
    let frames = vec![frame(1), frame(2), frame(3)];

    let seq_policy = SequentialPolicy;
    let par_policy = ParallelPolicy::new(4);

    let seq_results = seq_policy.execute_batch(frames.clone(), process_frame);
    let par_results = par_policy.execute_batch(frames, process_frame);

    assert_eq!(seq_results, par_results);
}
```

### 3. Integration Tests

```rust
#[test]
fn test_full_pipeline_with_mock_storage() {
    let storage = Arc::new(MockStorage::new());
    let writer = LeRobotWriter::new(config);
    let aligner = FrameAligner::new(CompletionCriteria::any());

    let executor = PipelineExecutor::new(
        aligner,
        Box::new(writer),
        SequentialPolicy,
    );

    // Process messages
    executor.process_messages(messages).unwrap();

    // Finalize
    let (stats, ops) = executor.finalize().unwrap();

    // Execute operations
    for op in ops {
        storage.execute(op).await.unwrap();
    }

    // Verify storage contents
    assert!(storage.exists("data/chunk-000/observation.images.cam_000.mp4"));
}
```

## References

- [ADR-001](./adr-001-pipeline-writer-storage-separation.md) - Writer/Storage separation
- [ADR-002](./adr-002-crate-architecture-refactoring.md) - Crate architecture refactoring
- [executor-architecture.md](./executor-architecture.md) - Executor design
- [data-pipeline-design.md](./data-pipeline-design.md) - Data flow design

## Open Questions

1. **Should `FrameAligner` be in `roboflow-core` instead?**
   - Pro: Could be reused by other crates
   - Con: Currently dataset-specific logic

2. **Should we keep `PipelineExecutor` in dataset for backward compatibility?**
   - Option B: Clean break, update all callers

3. **How to handle streaming video encoding?**
   - Current: Buffer all frames, encode at finalize
   - Future: Stream chunks to encoder (requires API change)

4. **Should `ExecutionPolicy` support async?**
   - Current: Sync only (rayon for parallel)
   - Future: May need async policy for I/O-bound processing

5. **How should video encoding configuration be structured?**
   - Option A: Keep video profiles in `roboflow-media`, reference from dataset config
   - Option B: Move all video config to `roboflow-media`, dataset just references by name
   - Option C: Dataset owns video config structure, media crate just implements encoding

6. **Should image decoding happen in FrameAligner or be deferred?**
   - Current: Images decoded during alignment
   - Option A: Keep current (decode early)
   - Option B: Pass raw bytes, decode in writer (lazy decoding)
