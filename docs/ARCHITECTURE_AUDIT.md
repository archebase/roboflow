# Architecture Audit Report

**Date:** 2025-02-12
**Purpose:** Pre-refactoring audit to understand current state before migration

---

## 1. Current Crate Structure

```
roboflow (workspace root)
├── roboflow-core        # Error types, registry, values
├── roboflow-storage     # S3, OSS, Local, Cached storage
├── roboflow-distributed # TiKV, catalog, worker, batch, merge
├── roboflow-dataset     # LeRobot writer, video encoding, streaming
├── roboflow-sources     # Bag, MCAP, RRD readers
└── roboflow-sinks       # LeRobot sink wrapper
```

---

## 2. Dependency Graph

```
                    ┌─────────────────┐
                    │  roboflow-core  │
                    │  (no deps)      │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              ▼              ▼
     ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
     │roboflow-    │  │roboflow-    │  │roboflow-    │
     │storage      │  │sources      │  │dataset      │
     │             │  │(minimal)    │  │             │
     └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
            │                │                │
            │                │                │
            └────────────────┼────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  roboflow-      │
                    │  sinks          │
                    │  (thin wrapper) │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  roboflow-      │
                    │  distributed    │
                    │  (depends on    │
                    │   ALL crates)   │
                    └─────────────────┘
```

### Dependency Summary

| Crate | Dependencies | External Dependencies |
|-------|--------------|----------------------|
| roboflow-core | robocodec | thiserror, serde, tracing |
| roboflow-storage | roboflow-core | object_store, tokio, async-trait |
| roboflow-sources | robocodec | tokio, async-trait |
| roboflow-dataset | roboflow-core, roboflow-storage, roboflow-sources | polars, rsmpeg, image, rayon |
| roboflow-sinks | roboflow-dataset, roboflow-storage | chrono, async-trait |
| roboflow-distributed | ALL internal crates | tikv-client, polars, tokio |

---

## 3. Public API by Crate

### roboflow-core
```rust
// Types
pub use error::{ErrorCategory, Result, RoboflowError};
pub use logging::{LogFormat, LoggingConfig, init_logging, init_logging_with};
pub use registry::{Encoding, SchemaProvider, TypeAccessor, TypeRegistry};
pub use retry::{IsRetryableRef, RetryConfig, retry_with_backoff};
pub use value::{CodecValue, DecodedMessage};
```

### roboflow-storage
```rust
// Storage traits
pub use {Storage, SeekableStorage, StreamingRead};
pub use {StorageError, StorageResult, ObjectMetadata};

// Implementations
pub use {LocalStorage, S3Storage, AsyncS3Storage};
pub use {CachedStorage, CacheConfig, CacheStats};
pub use {StorageFactory, StorageConfig};

// Upload utilities
pub use {MultipartUploader, MultipartConfig, upload_multipart};
pub use {ParallelMultipartUploader, upload_multipart_parallel};
pub use {MultipartUpload, StorageStreamingExt};
```

### roboflow-sources
```rust
// Core trait
pub trait Source: Send + Sync + 'static {
    async fn initialize(&mut self, config: &SourceConfig) -> SourceResult<SourceMetadata>;
    async fn read_batch(&mut self, size: usize) -> SourceResult<Option<Vec<TimestampedMessage>>>;
    async fn seek(&mut self, timestamp: u64) -> SourceResult<()>;
    async fn metadata(&self) -> SourceResult<SourceMetadata>;
}

// Implementations
pub use {BagSource, McapSource, RrdSource};
pub use {SourceRegistry, create_source, register_source};
```

### roboflow-sinks
```rust
// Core trait
pub trait Sink: Send + Sync + 'static {
    async fn initialize(&mut self, config: &SinkConfig) -> SinkResult<()>;
    async fn write_frame(&mut self, frame: DatasetFrame) -> SinkResult<()>;
    async fn flush(&mut self) -> SinkResult<()>;
    async fn finalize(&mut self) -> SinkResult<SinkStats>;
    async fn checkpoint(&self) -> SinkResult<SinkCheckpoint>;
}

// Types
pub use {DatasetFrame, ImageData, ImageFormat, CameraInfo};
pub use {SinkStats, SinkCheckpoint};
pub use {SinkRegistry, create_sink, register_sink};
```

### roboflow-dataset
```rust
// LeRobot format
pub use lerobot::{LerobotConfig, LerobotWriter};
pub use common::{DatasetWriter, AlignedFrame, ImageData, WriterStats};
pub use pipeline::{PipelineConfig, PipelineExecutor, PipelineStats};

// Video encoding (BURIED HERE - should be separate)
pub use common::{
    ConcurrentVideoEncoder, FragmentEncoder, EncoderPool,
    CameraPipeline, StreamingUploader
};

// Image decoding
pub use image::{
    DecodedImage, ImageDecoderBackend, ImageDecoderFactory,
    decode_compressed_image
};
```

### roboflow-distributed
```rust
// Coordination
pub use tikv::{TikvClient, TikvConfig, LockManager, CircuitBreaker};
pub use catalog::{TiKVCatalog, EpisodeMetadata, SegmentMetaData};
pub use batch::{BatchController, WorkUnit, BatchSpec, BatchStatus};

// Worker (GOD CLASS - 1259 LOC)
pub use worker::{Worker, WorkerConfig, WorkerMetrics};

// Finalization
pub use finalizer::{Finalizer, FinalizerConfig};
pub use merge::{MergeCoordinator, MergeResult};

// Supporting
pub use scanner::{Scanner, ScannerConfig};
pub use heartbeat::{HeartbeatManager, HeartbeatConfig};
pub use reaper::{ZombieReaper, ReaperConfig};
pub use shutdown::{ShutdownHandler, ShutdownInterrupted};
```

---

## 4. Identified Problems

### Problem 1: Worker God Class (Critical)
**Location:** `crates/roboflow-distributed/src/worker/mod.rs`
**Size:** 1259 LOC, 22 methods

**Responsibilities (too many):**
1. Pipeline execution
2. Cloud I/O (S3 upload/download)
3. Heartbeat management
4. Config caching
5. Work unit claiming
6. Error handling
7. Shutdown coordination

**Impact:** Hard to test, modify, and reason about

### Problem 2: Video Encoding Buried in Dataset Crate
**Location:** `crates/roboflow-dataset/src/common/`

**Issues:**
- `ConcurrentVideoEncoder` is a core feature but lives in `common/`
- Video encoding should be a first-class crate
- GPU encoding (NVENC, VideoToolbox) mixed with software encoding

### Problem 3: Duplicate LeRobot Logic
**Locations:**
- `crates/roboflow-dataset/src/lerobot/` (main implementation)
- `crates/roboflow-sinks/src/lerobot.rs` (thin wrapper)

**Issue:** Unclear which is the "source of truth"

### Problem 4: Distributed Depends on Everything
**Dependency chain:**
```
roboflow-distributed
  ├── roboflow-core
  ├── roboflow-storage
  ├── roboflow-dataset
  ├── roboflow-sources
  └── roboflow-sinks
```

**Impact:** Can't use distributed crate independently, heavy compile times

### Problem 5: No Clean Public API
**Missing:** A simple `convert()` function that users can call

**Current state:** Users must understand Worker, BatchController, Source, Sink, etc.

### Problem 6: Configuration Scattered
**Locations:**
- `LerobotConfig` in roboflow-dataset
- `SourceConfig` in roboflow-sources
- `SinkConfig` in roboflow-sinks
- `WorkerConfig` in roboflow-distributed
- Various TOML files

**Impact:** No unified pipeline configuration

---

## 5. Source/Sink Abstraction Quality

### Source Trait (Good)
```rust
pub trait Source: Send + Sync + 'static {
    async fn initialize(&mut self, config: &SourceConfig) -> SourceResult<SourceMetadata>;
    async fn read_batch(&mut self, size: usize) -> SourceResult<Option<Vec<TimestampedMessage>>>;
    // ...
}
```
✅ Async, streaming, metadata support
✅ Implementations: BagSource, McapSource, RrdSource
⚠️ Missing: S3 prefix input (multiple files from S3)

### Sink Trait (Good)
```rust
pub trait Sink: Send + Sync + 'static {
    async fn initialize(&mut self, config: &SinkConfig) -> SinkResult<()>;
    async fn write_frame(&mut self, frame: DatasetFrame) -> SinkResult<()>;
    async fn finalize(&mut self) -> SinkResult<SinkStats>;
    // ...
}
```
✅ Async, checkpointing support
✅ Implementations: LeRobot sink
⚠️ Missing: Direct S3 output (uses storage abstraction)

---

## 6. Recommendations Summary

| Priority | Issue | Fix | Effort |
|----------|-------|-----|--------|
| **P0** | No clean API | Add `convert()` function | 2 days |
| **P0** | Worker god class | Split into Executor + Coordinator | 3 days |
| **P1** | Video buried in dataset | Create `roboflow-video` crate | 2 days |
| **P1** | Duplicate LeRobot logic | Consolidate in one place | 1 day |
| **P2** | Distributed depends on all | Invert dependency | 3 days |
| **P2** | Config scattered | Unified `PipelineConfig` | 1 day |
| **P3** | No S3 prefix input | Add to Source abstraction | 1 day |

---

## 7. Key Insights

1. **Source/Sink abstractions are good** - Keep them, just need minor improvements
2. **Worker is the main problem** - Splitting it unlocks most other improvements
3. **Video encoding deserves its own crate** - It's a core feature
4. **Distributed is too coupled** - Should be optional layer on top
5. **No user-facing API** - The biggest gap for adoption

---

## Next Steps

1. Complete Task #2: Define Target Architecture
2. Begin Phase 1: Split Worker God Class
3. Proceed with remaining phases in order
