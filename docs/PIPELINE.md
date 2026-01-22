# Pipeline Architecture

This document describes the pipeline architectures used in Roboflow for high-performance robotics data processing.

## Overview

Roboflow provides **two pipeline implementations** optimized for different use cases:

| Pipeline | Stages | Target Throughput | Use Case |
|----------|--------|-------------------|----------|
| **Standard** | 4 | ~200 MB/s | Balanced performance, simplicity |
| **HyperPipeline** | 7 | ~1800+ MB/s | Maximum throughput, large-scale conversions |

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
```

## Design Principles

1. **Zero-Copy**: Minimize data copying through arena allocation (via `robocodec`)
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

- Opens and detects file format (MCAP or ROS bag) via `robocodec`
- Reads message data sequentially
- Batches messages into chunks (default 16MB)
- Sends chunks to the next stage

**Characteristics:**
- Single-threaded (sequential file I/O)
- Uses `robocodec` format readers
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
- Uses `robocodec` format writers

### Configuration

```rust
use roboflow::pipeline::{Orchestrator, PipelineConfig};

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
- Detect file format via `robocodec`
- Platform-specific read-ahead optimization
- Pass raw data to parser

#### 2. Parse/Slicer Stage

**Location**: `src/pipeline/hyper/stages/parser.rs`

- Parse message boundaries (via `robocodec` format parsers)
- Arena allocation for message data (from `robocodec`)
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
use roboflow::pipeline::hyper::{HyperPipeline, HyperPipelineConfig};

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
use roboflow::pipeline::fluent::Roboflow;

// Standard pipeline
Roboflow::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .run()?;

// HyperPipeline with auto-configuration
Roboflow::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .hyper_mode()                                // Use HyperPipeline
    .performance_mode(PerformanceMode::Throughput) // Auto-configure
    .run()?;

// Batch processing
Roboflow::open(vec!["file1.bag", "file2.bag"])?
    .write_to("/output/dir")
    .run()?;
```

---

## Data Structures

### MessageChunk

Provided by `robocodec`:

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

Provided by `robocodec`:

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

### Scalability

- **Standard**: Scales to ~8 cores (compression-bound)
- **HyperPipeline**: Scales to 16+ cores (better isolation)

---

## GPU Compression

**Location**: `src/pipeline/gpu/`

Experimental GPU acceleration:

| Platform | Backend | Feature Flag |
|----------|---------|--------------|
| NVIDIA (Linux) | nvCOMP | `gpu` (via robocodec) |
| Apple Silicon | libcompression | `gpu` (via robocodec) |
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
use roboflow::pipeline::{Orchestrator, PipelineConfig};

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
use roboflow::pipeline::hyper::{HyperPipeline, HyperPipelineConfig};

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
use roboflow::pipeline::{PerformanceMode, PipelineAutoConfig};

let config = PipelineAutoConfig::auto(PerformanceMode::Throughput)
    .to_hyper_config("input.bag", "output.mcap")
    .build()?;

let pipeline = HyperPipeline::new(config)?;
pipeline.run()?;
```

### Fluent API

```rust
use roboflow::pipeline::fluent::Roboflow;

Roboflow::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .hyper_mode()
    .performance_mode(PerformanceMode::Throughput)
    .run()?;
```

---

## See Also

- [ARCHITECTURE.md](ARCHITECTURE.md) - High-level system architecture
- [MEMORY.md](MEMORY.md) - Memory management details
- [README.md](../README.md) - Usage documentation
