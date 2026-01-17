# Pipeline Architecture

This document describes the pipeline architectures used in Robocodec for high-performance robotics data processing.

## Overview

Robocodec provides **three pipeline implementations** optimized for different use cases:

| Pipeline | Stages | Target Throughput | Use Case |
|----------|--------|-------------------|----------|
| **Standard** | 4 | ~200 MB/s | Balanced performance, simplicity |
| **HyperPipeline** | 7 | ~1800+ MB/s | Maximum throughput, large-scale conversions |
| **KPS Pipeline** | 5+ | Varies | Robotics dataset conversion (experimental) |

```
Standard Pipeline:
┌────────┐ ┌──────────┐ ┌───────────┐ ┌────────┐
│ Reader │→│ Transform │→│ Compress  │→│ Writer │
│  (1)   │  │   (1)    │  │  (N)      │  │  (1)   │
└────────┘ └──────────┘ └───────────┘ └────────┘

HyperPipeline:
┌──────────┐ ┌─────────┐ ┌─────────┐ ┌──────────┐ ┌───────────┐ ┌─────┐ ┌────────┐
│ Prefetch │→│  Parse  │→│  Batch  │→│ Transform │→│ Compress  │→│ CRC │→│ Writer │
│   (1)    │  │  (1)    │  │  (1)    │  │   (1)     │  │   (N)     │  │(1)  │  │  (1)   │
└──────────┘ └─────────┘ └─────────┘ └──────────┘ └───────────┘ └─────┘ └────────┘

KPS Pipeline (experimental):
┌─────────┐ ┌───────────┐ ┌──────────────┐ ┌───────┐ ┌──────────┐
│ Decode  │→│TimeAlign  │→│CameraExtract │→│Encode │→│ Delivery │
│  (1)    │  │   (1)     │  │    (1)       │  │  (N)  │  │   (1)    │
└─────────┘ └───────────┘ └──────────────┘ └───────┘ └──────────┘
```

## Design Principles

1. **Zero-Copy**: Minimize data copying through arena allocation
2. **Backpressure**: Bounded channels prevent memory overload
3. **Parallelism**: CPU-bound stages use multiple workers
4. **Isolation**: Each stage runs independently with dedicated channels
5. **Platform-optimized**: Use platform-specific I/O optimizations

---

## Standard Pipeline

**Location**: `src/pipeline/`

### Architecture

```
Input File → Reader → [Transform] → Compression → Writer → Output File
   (1)         (1)       (optional)      (N)         (1)
```

### Stages

#### Reader Stage

**Location**: `src/pipeline/stages/reader.rs`

- Opens and detects file format (MCAP or ROS bag)
- Reads message data sequentially
- Batches messages into chunks (default 16MB)
- Sends chunks to the next stage

**Characteristics:**
- Single-threaded (sequential file I/O)
- Format-agnostic via `BagSource` trait
- Chunk-based batching for efficient compression

#### Transform Stage (Optional)

**Location**: `src/pipeline/stages/transform.rs`

- Topic renaming
- Message type normalization
- Channel ID remapping
- Metadata filtering

**Characteristics:**
- Optional (disabled when no transformations needed)
- Single-threaded
- Zero-copy (only remaps references)

#### Compression Stage

**Location**: `src/pipeline/stages/compression.rs`

- Multiple workers (one per CPU core)
- Thread-local compressors
- Buffer reuse via buffer pool
- Tuned ZSTD (WindowLog matches CPU cache)

**Characteristics:**
- Fully multi-threaded
- Ordering-aware (maintains chunk sequence)
- Zero-allocation compression

#### Writer Stage

**Location**: `src/pipeline/stages/writer.rs`

- Receives compressed chunks from workers
- Maintains output order via sequencing
- Writes to output file format
- Flushes data periodically

**Characteristics:**
- Single-threaded (sequential writes)
- Ordering buffer for reordering
- Format-agnostic via `BagWriter` trait

### Configuration

```rust
use robocodec::pipeline::Orchestrator;

let config = PipelineConfig {
    chunk_size: 16 * 1024 * 1024,  // 16MB
    channel_capacity: 16,
    compression_level: 3,
    num_workers: None,  // Auto-detect
    transform_pipeline: None,
};

let orchestrator = Orchestrator::new(config)?;
orchestrator.run("input.bag", "output.mcap")?;
```

---

## HyperPipeline

**Location**: `src/pipeline/hyper/`

### Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        HyperPipeline (7-stage)                       │
├──────────────────────────────────────────────────────────────────────┤
│  ┌──────────┐ ┌─────────┐ ┌─────────┐ ┌──────────┐ ┌───────────┐   │
│  │ Prefetch │→│  Parse  │→│  Batch  │→│ Transform │→│ Compress  │   │
│  │  Stage   │  │  Stage │  │  Stage │  │   Stage   │  │  Stage   │   │
│  └──────────┘ └─────────┘ └─────────┘ └──────────┘ └───────────┘   │
│       │           │          │             │            │           │
│       ▼           ▼          ▼             ▼            ▼           │
│   Platform    Arena     Sequence     Metadata    Parallel    Workers  │
│   I/O Opt     Alloc     Routing      Transform   Compress    (N)     │
│                                                                      │
│  ┌──────────┐ ┌─────────┘                                         │
│  │   CRC    │→│ Writer  │                                         │
│  │  Stage   │  │  Stage  │                                         │
│  └──────────┘ └─────────┘                                         │
└──────────────────────────────────────────────────────────────────────┘
```

### Stages

#### 1. Prefetch Stage

**Location**: `src/pipeline/hyper/stages/prefetch.rs`

Platform-optimized I/O prefetching:

| Platform | Implementation |
|----------|----------------|
| macOS | `madvise(MADV_SEQUENTIAL)` |
| Linux | `io_uring` (when available) |
| Generic | Buffered reads |

**Responsibilities:**
- Detect file format
- Platform-specific read-ahead optimization
- Pass raw data to parser

#### 2. Parse/Slicer Stage

**Location**: `src/pipeline/hyper/stages/parser.rs`

- Parse message boundaries (BAG records or MCAP chunks)
- Arena allocation for message data
- Zero-copy message construction

**Responsibilities:**
- Parse format-specific headers
- Extract message timestamps
- Allocate in arena for zero-copy

#### 3. Batcher/Router Stage

**Location**: `src/pipeline/hyper/stages/batcher.rs`

- Batch messages into optimal chunk sizes
- Assign sequence IDs for ordering
- Route to compression workers

**Responsibilities:**
- Target batch size configuration
- Sequence numbering
- Temporal metadata extraction

#### 4. Transform Stage

**Location**: `src/pipeline/hyper/stages/transform.rs`

- Pass-through for data (metadata transforms only)
- Topic/channel remapping
- Schema translation

**Characteristics:**
- Currently minimal processing
- Designed for future transformation capabilities

#### 5. Compressor Stage

**Location**: `src/pipeline/hyper/stages/compressor.rs`

Multi-threaded ZSTD compression:

```rust
// Per-worker configuration
struct CompressorWorker {
    compressor: zstd::bulk::Compressor,  // Thread-local
    buffer: PooledBuffer,                // Reused output buffer
    sequence: u64,                       // For ordering
}
```

**Characteristics:**
- Parallel compression (N workers)
- Lock-free buffer pool
- CPU cache-aware WindowLog tuning

#### 6. CRC/Packetizer Stage

**Location**: `src/pipeline/hyper/stages/crc.rs`

- CRC32 checksum computation
- MCAP message framing
- Reordering based on sequence IDs

**Responsibilities:**
- Ensure data integrity
- MCAP packet construction
- Order reconstruction

#### 7. Writer Stage

**Location**: `src/pipeline/hyper/stages/writer.rs`

- Sequential output file writes
- MCAP metadata generation
- Finalization and flush

**Characteristics:**
- Single-threaded (sequential writes optimal)
- Lock-free queue from CRC stage
- Efficient chunk merging

### Inter-Stage Communication

```rust
// Each stage has dedicated channels
struct HyperPipelineChannels {
    prefetch_to_parser: bounded_channel(8),
    parser_to_batcher: bounded_channel(8),
    batcher_to_transform: bounded_channel(16),
    transform_to_compressor: bounded_channel(16),
    compressor_to_crc: bounded_channel(16),
    crc_to_writer: bounded_channel(8),
}
```

**Benefits:**
- Isolated backpressure per stage
- No cross-stage contention
- Predictable memory usage

### Configuration

```rust
use robocodec::pipeline::hyper::{HyperPipeline, HyperPipelineConfig};

// Manual configuration
let config = HyperPipelineConfig::builder()
    .input_path("input.bag")
    .output_path("output.mcap")
    .compression_level(3)
    .batcher(BatcherConfig { target_size: 8_388_608, ..default() })
    .prefetcher(PrefetcherConfig { block_size: 2_097_152, ..default() })
    .compression_threads(8)
    .build()?;

// Auto-configuration (recommended)
let config = PipelineAutoConfig::auto(PerformanceMode::Throughput)
    .to_hyper_config("input.bag", "output.mcap")
    .build()?;

let pipeline = HyperPipeline::new(config)?;
let report = pipeline.run()?;
```

---

## KPS Pipeline (Experimental)

> **⚠️ Experimental Feature**: The KPS pipeline is currently experimental and under active development. APIs may change between versions.

**Location**: `src/pipeline/kps/`

### Overview

The KPS (Kupas) pipeline converts robotics data from MCAP format to KPS dataset format for robotics learning applications. It supports the v1.2 specification with compliant directory structures and statistics tracking.

### Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        KPS Pipeline (5+ stages)                      │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  MCAP Input                                                          │
│     │                                                                │
│     ▼                                                                │
│  ┌─────────┐   Decoded messages    ┌──────────────┐                 │
│  │ Decode  │──────────────────────→│ Time Aligner │                 │
│  │  Stage  │                       │              │                 │
│  └─────────┘                       └──────────────┘                 │
│                                           │                          │
│                                           ▼                          │
│                                    ┌──────────────┐                 │
│                                    │Camera Extract │                 │
│                                    │    (opt)      │                 │
│                                    └──────────────┘                 │
│                                           │                          │
│                                           ▼                          │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                    Encoders (parallel)                       │   │
│  │  ┌─────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────┐    │   │
│  │  │ HDF5    │ │ Parquet  │ │  Video   │ │    Audio     │    │   │
│  │  │ Writer  │ │  Writer  │ │ Encoder  │ │   Writer     │    │   │
│  │  └─────────┘ └──────────┘ └──────────┘ └──────────────┘    │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                           │                          │
│                                           ▼                          │
│  ┌─────────┐   Final dataset        ┌──────────┐                   │
│  │Delivery │───────────────────────→│   Output │                   │
│  │ Builder │                        │  Directory│                   │
│  └─────────┘                        └──────────┘                   │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### Stages

#### 1. Decode Stage

**Location**: `src/pipeline/kps/mod.rs`

- Opens and reads MCAP file
- Decodes CDR/Protobuf messages
- Groups messages by timestamp
- Extracts image data for video encoding

**Characteristics:**
- Schema-aware decoding using message definitions
- Topic-based message routing
- Timestamp ordering

#### 2. Time Alignment Stage

**Location**: `src/pipeline/kps/traits/time_alignment.rs`

Resamples data to target FPS:

| Strategy | Description |
|----------|-------------|
| LinearInterpolation | Linear interpolation between samples |
| NearestNeighbor | Use nearest sample without interpolation |
| HoldLastValue | Hold last value until next sample arrives |

**Configuration:**
```rust
pub struct TimeAlignerConfig {
    pub target_fps: u32,
    pub strategy: TimeAlignmentStrategyType,
    pub state_interpolation_max_gap_ns: u64,
    pub image_sync_tolerance_ns: u64,
}
```

#### 3. Camera Extraction Stage (Optional)

**Location**: `src/io/kps/delivery_v12.rs`

- Extracts camera parameters from TF messages
- Reads camera_info topics
- Generates calibration files

**Configuration:**
```rust
pub struct CameraExtractorConfig {
    pub enabled: bool,
    pub camera_topics: HashMap<String, String>,
    pub parent_frame: String,
    pub camera_info_suffix: String,
    pub tf_topic: String,
}
```

#### 4. Encode Stage

**Location**: `src/io/kps/writers/`

Multiple parallel encoders:

| Encoder | Output | Feature Flag |
|---------|--------|--------------|
| `V12Hdf5Writer` | HDF5 with v1.2 structure | `kps-hdf5` |
| `OriginalHdf5Writer` | Original unaligned data | `kps-hdf5` |
| `ParquetWriter` | Parquet data files | `kps-parquet` |
| `Mp4Encoder` | MP4 video files | default |
| `DepthEncoder` | Depth video | `kps-depth` |
| `AudioWriter` | WAV audio files | default |

#### 5. Delivery Stage

**Location**: `src/io/kps/delivery_v12.rs`

Creates v1.2 compliant directory structure:

```
{Robot}-{EndEffector}-{Scene}/
├── task_info/
│   └── {Scene}-{SubScene}-{Task}.json
├── {Scene}/
│   └── {SubScene}/
│       └── {Task}-{stats}/
│           ├── {UUID}/
│           │   ├── camera/
│           │   │   ├── video/
│           │   │   └── depth/
│           │   ├── parameters/
│           │   ├── proprio_stats/
│           │   └── audio/
└── URDF/
    └── {Robot}-{EndEffector}-{version}/
```

### Configuration

**TOML Configuration:**
```toml
[dataset]
name = "my_dataset"
fps = 30
robot_type = "Kuavo4Pro"

[[mappings]]
topic = "/camera/high"
feature = "observation.camera_0"
type = "image"

[[mappings]]
topic = "/joint_states"
feature = "observation.joint_position"
type = "state"

[output]
formats = ["parquet"]
image_format = "mp4"
```

**Fluent API:**
```rust
use robocodec::pipeline::kps::KpsConverter;

// Simple conversion
let report = KpsConverter::new("input.mcap", "output_dir")
    .config("config.toml")
    .run()?;

// V1.2 delivery with full configuration
let report = KpsConverter::new("input.mcap", "output_dir")
    .config("config.toml")
    .v12_delivery()
    .robot("Kuavo4Pro")
    .end_effector("Dexhand")
    .scene("Housekeeper")
    .sub_scene("Kitchen")
    .task("Dispose_of_takeout_containers")
    .calibration("robot_calibration.json")
    .urdf("robot.urdf")
    .with_statistics()
    .run()?;
```

### Statistics Tracking

The pipeline tracks statistics during conversion and updates the directory name:

```rust
pub struct TaskStatistics {
    pub total_bytes: u64,
    pub frame_count: usize,
    pub duration_hours: f64,
}

// Directory name format: {Task}-{size}GB_{counts}counts_{duration}h
// Example: Dispose_of_takeout_containers-53p21GB_2000counts_85p30h
```

### Feature Flags

| Flag | Description |
|------|-------------|
| `kps-hdf5` | HDF5 dataset support |
| `kps-parquet` | Parquet dataset support |
| `kps-depth` | Depth video support |
| `kps-all` | All KPS features |

---

## Auto-Configuration

**Location**: `src/pipeline/auto_config.rs`

Hardware-aware automatic pipeline tuning:

### Performance Modes

```rust
pub enum PerformanceMode {
    Throughput,        // Maximum throughput (aggressive)
    Balanced,          // Middle ground (default)
    MemoryEfficient,   // Conserve memory
}
```

### Auto-Detected Parameters

| Parameter | Detection Method |
|-----------|------------------|
| CPU cores | `num_cpus::get()` |
| Available memory | System memory query |
| L3 cache | CPUID (x86_64) or fixed values |
| Optimal batch size | Based on L3 cache |
| Channel capacities | Based on memory mode |

### Example Configuration by Mode

| Parameter | Throughput | Balanced | MemoryEfficient |
|-----------|------------|----------|-----------------|
| Batch size | 16MB | 8MB | 4MB |
| Channel capacity | 16 | 8 | 4 |
| Compression threads | All cores - 2 | All cores / 2 | 2-4 |

---

## Fluent API

**Location**: `src/pipeline/fluent/`

Type-safe builder API for both pipelines:

```rust
use robocodec::pipeline::fluent::{Robocodec, CompressionPreset};

// Standard pipeline
Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .with_compression(CompressionPreset::Balanced)
    .run()?;

// HyperPipeline with auto-configuration
Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .hyper()                                    // Use HyperPipeline
    .mode(PerformanceMode::Throughput)          // Auto-configure
    .run()?;

// Batch processing
Robocodec::open(vec!["file1.bag", "file2.bag"])?
    .write_to("/output/dir")
    .run()?;
```

---

## Data Structures

### MessageChunk

**Location**: `src/pipeline/types/chunk.rs`

```rust
pub struct MessageChunk<'arena> {
    arena: *mut MessageArena,           // Owning arena pointer
    pooled_arena: Option<PooledArena>,  // Pool management
    messages: Vec<ArenaMessage<'arena>>, // Zero-copy messages
    sequence: u64,                      // Ordering for writer
    message_start_time: u64,
    message_end_time: u64,
}
```

### Arena Allocation

**Location**: `src/pipeline/types/arena.rs`

```rust
pub struct MessageArena {
    blocks: Vec<ArenaBlock>,     // 64MB blocks
    current_block: AtomicUsize,  // Lock-free allocation
}
```

See [MEMORY.md](MEMORY.md) for detailed memory management documentation.

---

## Performance Characteristics

### Throughput Comparison

| Pipeline | Operation | Throughput |
|----------|-----------|------------|
| Standard | BAG → MCAP (ZSTD-3) | ~200 MB/s |
| HyperPipeline | BAG → MCAP (ZSTD-3) | ~1800 MB/s |
| **Speedup** | | **9x** |

### Latency

| Pipeline | Typical Latency |
|----------|-----------------|
| Standard | 100-200ms |
| HyperPipeline | 50-100ms |
| KPS Pipeline | Varies by encoding |

### Scalability

- **Standard**: Scales to ~8 cores (compression-bound)
- **HyperPipeline**: Scales to 16+ cores (better isolation)
- **KPS Pipeline**: Scales with encoder parallelism

---

## GPU Compression

**Location**: `src/pipeline/gpu/`

Experimental GPU acceleration:

| Platform | Backend | Feature Flag |
|----------|---------|--------------|
| NVIDIA (Linux) | nvCOMP | `gpu-nvcomp` |
| Apple Silicon | libcompression | `gpu-accelerate` |
| Fallback | CPU ZSTD | default |

```rust
let config = HyperPipelineConfig::builder()
    .compression_backend(CompressionBackend::Auto)
    .build()?;
```

---

## Usage Examples

### Standard Pipeline

```rust
use robocodec::pipeline::{Orchestrator, PipelineConfig};

let config = PipelineConfig {
    chunk_size: 16 * 1024 * 1024,
    compression_level: 3,
    ..Default::default()
};

let orchestrator = Orchestrator::new(config)?;
orchestrator.run("input.bag", "output.mcap")?;
```

### HyperPipeline (Manual Config)

```rust
use robocodec::pipeline::hyper::{HyperPipeline, HyperPipelineConfig};

let config = HyperPipelineConfig::builder()
    .input_path("input.bag")
    .output_path("output.mcap")
    .compression_level(3)
    .build()?;

let pipeline = HyperPipeline::new(config)?;
pipeline.run()?;
```

### HyperPipeline (Auto-Config)

```rust
use robocodec::pipeline::{PerformanceMode, PipelineAutoConfig};

let config = PipelineAutoConfig::auto(PerformanceMode::Throughput)
    .to_hyper_config("input.bag", "output.mcap")
    .build()?;

let pipeline = HyperPipeline::new(config)?;
pipeline.run()?;
```

### KPS Pipeline

```rust
use robocodec::pipeline::kps::KpsConverter;

let report = KpsConverter::new("input.mcap", "output_dir")
    .config("config.toml")
    .v12_delivery()
    .robot("Kuavo4Pro")
    .end_effector("Dexhand")
    .scene("Housekeeper")
    .sub_scene("Kitchen")
    .task("Dispose_of_takeout_containers")
    .with_statistics()
    .run()?;
```

### Fluent API

```rust
use robocodec::pipeline::fluent::{Robocodec, CompressionPreset};

Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .hyper()
    .mode(PerformanceMode::Throughput)
    .run()?;
```

---

## See Also

- [ARCHITECTURE.md](ARCHITECTURE.md) - High-level system architecture
- [MEMORY.md](MEMORY.md) - Memory management details
- [benches/README.md](../benches/README.md) - Benchmarking guide
