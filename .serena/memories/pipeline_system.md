# Pipeline System in Robocodec

This document explains the pipeline architecture and how data flows through the system.

## Overview

Robocodec provides three pipeline implementations for different use cases:

1. **Standard Pipeline** - Simple 4-stage pipeline, ~200 MB/s
2. **HyperPipeline** - 7-stage pipeline for maximum throughput, ~1800 MB/s
3. **KPS Pipeline** - Dataset conversion pipeline (experimental)

## Standard Pipeline

### Architecture

```
Input File → [Reader] → [Transform] → [Compress] → [Writer] → Output File
```

### Stages

#### 1. Reader Stage (`src/pipeline/stages/reader.rs`)

**Purpose:** Read and parse input format (bag/MCAP)

**Responsibilities:**
- Detect file format automatically
- Parse file structure
- Extract messages and metadata
- Read schema definitions

**Key Types:**
```rust
pub struct ReaderStage {
    // BagSource for reading
    source: Box<dyn BagSource>,
}
```

#### 2. Transform Stage (`src/pipeline/stages/transform.rs`)

**Purpose:** Apply data transformations

**Transformations:**
- Topic renaming (wildcard patterns)
- Type renaming (wildcard patterns)
- Type normalization
- Message filtering (future)

**Key Types:**
```rust
pub struct TransformStage {
    pipeline: TransformPipeline,
}
```

#### 3. Compression Stage (`src/pipeline/stages/compression.rs`)

**Purpose:** Compress message data

**Compression Algorithms:**
- Zstandard (default)
- LZ4 (faster, lower compression)
- Bzip2 (slower, higher compression)

**Presets:**
- `Fast`: Speed priority
- `Balanced`: Balance between speed and size
- `Small`: Size priority

#### 4. Writer Stage (`src/pipeline/stages/writer.rs`)

**Purpose:** Write output format

**Responsibilities:**
- Format messages for output
- Write schema definitions
- Create output file structure

### Usage Example

```rust
use roboflow::pipeline::fluent::Robocodec;

// Standard pipeline usage
Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .run()?;
```

## HyperPipeline

### Architecture

```
Input → [Prefetch] → [Parse] → [Slice] → [Transform] → [Compress] → [Packetize] → [Write] → Output
```

### Additional Stages

#### 1. Prefetch Stage (`src/pipeline/hyper/stages/prefetcher.rs`)

**Purpose:** Read ahead from disk

**Features:**
- Async I/O with io_uring (Linux) or threads (macOS/Windows)
- Reads multiple chunks ahead
- Hides I/O latency

#### 2. Parse Stage (`src/pipeline/hyper/stages/parser_slicer.rs`)

**Purpose:** Parse message structure

**Responsibilities:**
- Extract message boundaries
- Parse message headers
- Prepare for slicing

#### 3. Slice Stage

**Purpose:** Split messages into processing units

**Responsibilities:**
- Group messages into chunks
- Optimize chunk size for cache
- Balance load across stages

#### 4. Transform Stage

Same as Standard Pipeline but optimized for batch processing.

#### 5. Compress Stage (`src/pipeline/hyper/stages/compression.rs`)

**Purpose:** Compress with parallelization

**Features:**
- Per-message compression
- Parallel compression across CPU cores
- Hardware-aware compression (AVX2/AVX-512)

#### 6. Packetize Stage (`src/pipeline/hyper/stages/crc_packetizer.rs`)

**Purpose:** Add CRC and packetize

**Responsibilities:**
- Calculate CRC checksums
- Create data packets
- Prepare for writing

#### 7. Write Stage

**Purpose:** Write to disk efficiently

**Features:**
- Batched writes
- Async I/O
- Minimize syscalls

### Performance Optimizations

1. **Lock-Free Queues**: Crossbeam channels between stages
2. **Hardware Detection**: CPU features (AVX2, AVX-512, SSE4.2)
3. **CPU-Aware Compression**: WindowLog optimization for Zstandard
4. **OS-Specific I/O**: io_uring on Linux, thread pool on macOS/Windows

### Usage Example

```rust
use roboflow::pipeline::fluent::Robocodec;

// HyperPipeline usage
Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .hyper()
    .mode(PerformanceMode::Throughput)
    .run()?;
```

## KPS Pipeline

### Overview

Converts robotics data to KPS (Knowledge Pretrained System) dataset format for machine learning.

**Status:** Experimental (APIs may change)

### Usage

```rust
use roboflow::pipeline::fluent::Robocodec;

// Convert to KPS format
Robocodec::open(vec!["input.mcap"])?
    .write_to_kps("output_dir")
    .config("kps_config.toml")
    .run()?;
```

### Configuration

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

## Fluent API

The fluent API provides a type-safe builder for pipeline configuration.

### Example Usage

```rust
use roboflow::Robocodec;
use roboflow::pipeline::fluent::{CompressionPreset, PerformanceMode};

// Basic conversion
let result = Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .run()?;

// With compression
let result = Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .compression(CompressionPreset::Balanced)
    .run()?;

// HyperPipeline with custom mode
let result = Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .hyper()
    .mode(PerformanceMode::Throughput)
    .run()?;

// With transforms
let transform = TransformBuilder::new()
    .with_topic_rename("/old_topic", "/new_topic")
    .with_type_rename("OldType", "NewType")
    .build();

let result = Robocodec::open(vec!["input.bag"])?
    .transform(transform)
    .write_to("output.mcap")
    .run()?;

// KPS conversion
let result = Robocodec::open(vec!["input.mcap"])?
    .write_to_kps("output_dir")
    .config("kps_config.toml")
    .run()?;
```

### API Chaining

All methods return `Result<Self>` for error handling:

```rust
Robocodec::open(vec!["input.bag"])?      // Returns Result<Robocodec>
    .write_to("output.mcap")?             // Returns Result<Robocodec>
    .hyper()?                             // Returns Result<Robocodec>
    .run()?;                              // Returns Result<Report>
```

## Performance Comparison

### Throughput (MB/s)

| Pipeline | Compression | Throughput | Use Case |
|----------|-------------|------------|----------|
| Standard | None | ~800 | Fast processing |
| Standard | Balanced | ~200 | Balanced speed/size |
| Hyper | Fast | ~1200 | High throughput |
| Hyper | Balanced | ~1800 | Maximum throughput |
| KPS | Variable | ~100 | Dataset generation |

### Latency (ms per message)

| Pipeline | Average | P95 | P99 |
|----------|---------|-----|-----|
| Standard | 0.5 | 1.0 | 2.0 |
| Hyper | 0.1 | 0.3 | 0.5 |

## Choosing a Pipeline

### Use Standard Pipeline when:
- Simplicity is important
- Memory is constrained
- Processing single files
- Developing/testing

### Use HyperPipeline when:
- Maximum throughput is needed
- Processing large datasets
- Batch conversion
- Production workloads

### Use KPS Pipeline when:
- Converting to ML dataset format
- Need Parquet/HDF5 output
- Generating training data

## Pipeline Configuration

### Auto-Configuration

The system automatically detects hardware:
- CPU core count
- CPU features (AVX2, AVX-512)
- Memory size
- Disk speed (approximately)

### Manual Override

```rust
use roboflow::pipeline::auto_config::PipelineConfig;

let config = PipelineConfig {
    compression_threads: 4,
    io_threads: 2,
    chunk_size: 1024 * 1024, // 1 MB chunks
};

let result = Robocodec::open_with_config(vec!["input.bag"], config)?
    .write_to("output.mcap")
    .run()?;
```

## Error Handling

All pipeline operations return `Result`:

```rust
use roboflow::Robocodec;
use roboflow::core::Error;

match Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .run()
{
    Ok(report) => {
        println!("Processed {} messages", report.message_count);
        println!("Throughput: {:.1} MB/s", report.throughput_mb_s);
    }
    Err(Error::Io(e)) => {
        eprintln!("I/O error: {}", e);
    }
    Err(Error::Format(e)) => {
        eprintln!("Format error: {}", e);
    }
    Err(e) => {
        eprintln!("Error: {}", e);
    }
}
```

## Related Documentation

- `PIPELINE.md` in docs/ for detailed pipeline architecture
- `src/pipeline/hyper/` for HyperPipeline implementation
- `src/pipeline/stages/` for individual stage implementations
- `src/pipeline/fluent/` for Fluent API implementation
