# Roboflow Architecture

High-level architecture for the Roboflow distributed data transformation pipeline.

## Overview

Roboflow is a distributed data transformation pipeline that converts robotics bag/MCAP files to trainable datasets (LeRobot format). It supports horizontal scaling for large dataset processing with schema-driven message translation and cloud storage support.

## Data Flow

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  S3/OSS     │───▶│  Source     │───▶│  Decode     │───▶│  Transform  │───▶│  Encode     │
│  Input      │    │  Registry   │    │  (robocodec)│    │  & Align    │    │  (FFmpeg)   │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                                                                     │
                                                                     ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  S3/OSS     │◀───│  Upload     │◀───│  Parquet    │◀───│  Chunking   │◀───│  Flush      │
│  Output     │    │  Coordinator│    │  Writer     │    │  (Memory)   │    │  Control    │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

## Workspace Crates

| Crate | Purpose | Key Types |
|-------|---------|-----------|
| `roboflow-core` | Foundation types, error handling, registry | `RoboflowError`, `CodecValue`, `TypeRegistry` |
| `roboflow-storage` | Storage abstraction layer | `Storage`, `LocalStorage`, `S3Storage`, `StorageFactory` |
| `roboflow-executor` | Stage-based task executor | `StageExecutor`, `Pipeline`, `ExecutionPolicy`, `SlotPool` |
| `roboflow-media` | Image and video encoding/decoding | `ImageData`, `VideoEncoder`, `ConcurrentVideoEncoder` |
| `roboflow-dataset` | Dataset format writers and sources | `LerobotWriter`, `DatasetWriter`, `Source`, `BagSource`, `McapSource` |
| `roboflow-pipeline` | Pipeline execution and stages | `DatasetPipelineExecutor`, `DiscoverStage`, `ConvertStage`, `MergeStage` |
| `roboflow-distributed` | Distributed coordination via TiKV | `TiKVClient`, `BatchController`, `Worker`, `Scanner`, `Finalizer` |

## Core Abstractions

### Storage Layer

```rust
trait Storage: Send + Sync {
    fn reader(&self, path: &Path) -> StorageResult<Box<dyn Read + Send + 'static>>;
    fn writer(&self, path: &Path) -> StorageResult<Box<dyn Write + Send + 'static>>;
    fn exists(&self, path: &Path) -> bool;
    fn delete(&self, path: &Path) -> StorageResult<()>;
    fn list(&self, prefix: &Path) -> StorageResult<Vec<PathBuf>>;
}

trait SeekableStorage: Storage {
    fn seekable_reader(&self, path: &Path) -> StorageResult<Box<dyn SeekRead + Send + 'static>>;
}
```

**Supported backends:**
- **Local**: Filesystem storage with seek support
- **S3**: AWS S3-compatible storage
- **OSS**: Alibaba Cloud Object Storage

### Source/Sink Pattern

```rust
trait Source: Send + Sync {
    async fn initialize(&mut self, config: &SourceConfig) -> SourceResult<SourceMetadata>;
    async fn read_batch(&mut self, size: usize) -> SourceResult<Option<Vec<TimestampedMessage>>>;
    async fn finalize(&mut self) -> SourceResult<SourceStats>;
}
```

**Supported sources:**
- **MCAP**: Streaming and memory-mapped reads
- **ROS1 Bag**: Legacy bag format support
- **RRD**: Rerun data format

### Pipeline Stages

```rust
// Stage-based execution inspired by Spark
pub struct DiscoverStage;
pub struct ConvertStage;
pub struct MergeStage;

// Pipeline executor
pub struct DatasetPipelineExecutor {
    writer: Box<dyn DatasetWriter>,
    config: DatasetPipelineConfig,
}
```

### Data Types

```rust
/// Raw message from sources with topic, timestamp, and type-erased data
struct TimestampedMessage {
    pub topic: String,
    pub timestamp: i64,
    pub data: CodecValue,
    pub sequence: Option<u64>,
}

/// Unified frame structure for dataset output
struct DatasetFrame {
    pub frame_index: usize,
    pub episode_index: usize,
    pub timestamp: f64,
    pub task_index: Option<usize>,
    pub observation_state: Option<Vec<f32>>,
    pub action: Option<Vec<f32>>,
    pub images: HashMap<String, ImageData>,
    pub camera_info: HashMap<String, CameraInfo>,
}

/// Type-erased message container (CDR, Protobuf, JSON)
enum CodecValue {
    Cdr(Arc<Vec<u8>>),
    Json(Arc<String>),
    Protobuf(Arc<Vec<u8>>),
}
```

## Distributed Coordination

The distributed system uses a Kubernetes-inspired design with TiKV as the control plane:

### Components

| Kubernetes | Roboflow | Purpose |
|------------|----------|---------|
| Pod | Worker | Processing unit |
| etcd | TiKV | Distributed state store |
| kubelet heartbeat | HeartbeatManager | Worker liveness |
| Finalizers | Finalizer controller | Cleanup handling |
| Job/CronJob | BatchSpec, WorkUnit | Work scheduling |
| Scheduler | Scanner | File discovery and job creation |

### Batch State Machine

```
┌──────────┐    ┌─────────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│ Pending  │───▶│ Discovering │───▶│ Running  │───▶│ Merging  │───▶│ Complete │
└──────────┘    └─────────────┘    └──────────┘    └──────────┘    └──────────┘
                                       │
                                       ▼
                                    ┌──────────┐
                                    │ Failed   │
                                    └──────────┘
```

### TiKV Key Structure

```
roboflow/batch/{batch_id}          → BatchSpec
roboflow/batch/{batch_id}/phase    → BatchPhase
roboflow/batch/{batch_id}/units/*  → WorkUnit
roboflow/worker/{pod_id}/heartbeat → HeartbeatRecord
roboflow/worker/{pod_id}/lock      → LockRecord
roboflow/worker/{pod_id}/checkpoint→ CheckpointState
```

## CLI Commands

The unified `roboflow` binary provides all operations:

```bash
# Run unified service (default: all roles)
roboflow run

# Run specific roles
roboflow run --role worker
roboflow run --role finalizer

# Job management
roboflow submit s3://bucket/file.bag --output s3://bucket/out/
roboflow jobs list
roboflow batch list

# Health check
roboflow health
```

## Dataset Writing

### LeRobot Format

```rust
struct LerobotConfig {
    pub dataset: DatasetConfig,
    pub mappings: Vec<Mapping>,
    pub video: VideoConfig,
    pub flushing: FlushingConfig,
    pub streaming: StreamingConfig,
}

struct StreamingConfig {
    pub finalize_metadata_in_coordinator: bool,
}
```

### Video Encoding

```rust
// Concurrent video encoder for parallel chunk encoding
pub struct ConcurrentVideoEncoder {
    config: ConcurrentEncoderConfig,
}

pub struct ConcurrentEncoderConfig {
    pub storage: Arc<dyn Storage>,
    pub key_prefix: String,
    pub codec: VideoCodec,
    pub crf: u8,
}
```

### Upload Coordinator

```rust
struct EpisodeUploadCoordinator {
    pub storage: Arc<dyn Storage>,
    pub config: UploadConfig,
    pub progress: Option<UploadProgress>,
}

struct UploadConfig {
    pub concurrency: usize,       // Default: 4
    pub max_pending: usize,       // Default: 100
    pub max_retries: u32,         // Default: 3
    pub delete_after_upload: bool,
}
```

## Memory Management

### Zero-Copy Arena Allocation

Using `robocodec` for arena allocation (~22% memory savings):

```rust
use robocodec::arena::Arena;

let arena = Arena::new();
let data = arena.alloc_vec::<u8>(size);
// No explicit free - arena drops as a unit
```

### Streaming I/O

- **Read**: 10MB chunks from S3/OSS (not full file download)
- **Write**: 256KB chunks for uploads
- **Video**: FFmpeg stdin streaming for encoding

## Configuration

### Source Configuration

```toml
[source]
type = "mcap"  # or "bag", "rrd"
path = "s3://bucket/path/to/data.mcap"

# Optional: topic filtering
topics = ["/camera/image_raw", "/joint_states"]
```

### Dataset Configuration

```toml
[dataset]
name = "robot_dataset"
fps = 30
robot_type = "franka"

[[mappings]]
topic = "/camera/color/image_raw"
feature = "observation.images.camera_0"
mapping_type = "image"

[[mappings]]
topic = "/joint_states"
feature = "observation.state"
mapping_type = "state"

[video]
codec = "libx264"
crf = 18

[flushing]
max_frames_per_chunk = 1000
max_memory_bytes = 2147483648  # 2GB
```

### Storage Configuration (Environment)

```bash
# OSS (Alibaba Cloud)
export OSS_ACCESS_KEY_ID="..."
export OSS_ACCESS_KEY_SECRET="..."
export OSS_ENDPOINT="..."

# S3 (AWS)
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_ENDPOINT="..."  # Optional for S3-compatible
```

## Fault Tolerance

### Checkpointing

```rust
struct CheckpointState {
    pub last_frame_index: usize,
    pub last_episode_index: usize,
    pub checkpoint_time: i64,
    pub data: HashMap<String, serde_json::Value>,
}
```

Workers persist checkpoints to TiKV before processing each work unit.

### Heartbeats

```rust
struct HeartbeatRecord {
    pub pod_id: String,
    pub last_seen: i64,
    pub status: WorkerStatus,
}

// Zombie reaper reclaims stale pods after 30 seconds
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(30);
```

### Circuit Breakers

```rust
struct CircuitBreaker {
    pub failure_threshold: usize,
    pub success_threshold: usize,
    pub timeout: Duration,
    pub state: CircuitState,
}

enum CircuitState {
    Closed,    // Normal operation
    Open,      // Failing, requests blocked
    HalfOpen,  // Testing recovery
}
```

## Performance

### Throughput

- **Decoding**: ~1800 MB/s (MCAP streaming)
- **Encoding**: ~100 MB/s (FFmpeg H.264)
- **Upload**: ~50 MB/s (parallel uploads)

### Optimization Techniques

1. **CPU feature detection**: AVX2, AVX-512 when available
2. **Memory-mapped files**: For local bag/MCAP files
3. **Parallel encoding**: FFmpeg per-chunk processing
4. **Connection pooling**: Reuse S3/OSS connections

## Feature Flags

| Flag | Purpose |
|------|---------|
| `jemalloc` | jemalloc allocator (Linux only) |
| `cli` | CLI support for binaries |
| `profiling` | Profiling support for profiler binary |
| `cpuid` | CPU-aware WindowLog detection (x86_64 only) |
| `io-uring-io` | io_uring support for Linux 5.6+ |

## See Also

- `CLAUDE.md` - Developer guidelines and conventions
- `tests/` - Integration and E2E tests
- `crates/roboflow-dataset/src/` - Dataset writer and source implementations
- `crates/roboflow-distributed/src/` - Distributed coordination
