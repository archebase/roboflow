# Robocodec Architecture

This document provides a high-level overview of Robocodec's architecture and design decisions.

## Overview

Robocodec is a **schema-driven, universal robotics data codec** that enables efficient conversion between different robotics message formats and storage formats.

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Robocodec Core                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌───────┐ ┌──────────┐ │
│  │  CDR     │  │Protobuf  │  │  JSON    │  │Registry│ │   GPU    │ │
│  │  Codec   │  │  Codec   │  │  Codec   │  │       │ │Compression│ │
│  └──────────┘  └──────────┘  └──────────┘  └───────┘ └──────────┘ │
├─────────────────────────────────────────────────────────────────────┤
│                         Pipeline Layer                              │
│  ┌─────────────────────┐  ┌──────────────────────────────────────┐ │
│  │  Standard Pipeline  │  │      HyperPipeline (7-stage)          │ │
│  │  Reader→Transform→  │  │  Prefetch→Parse→Batch→Transform→     │ │
│  │  Compress→Write     │  │  Compress→CRC→Write                   │ │
│  └─────────────────────┘  └──────────────────────────────────────┘ │
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │           KPS Pipeline (experimental)                         │ │
│  │  Decode→TimeAlign→CameraExtract→Encode→Delivery              │ │
│  └──────────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────────────┤
│                         Fluent API Layer                            │
│  ┌────────────────────────────────────────────────────────────────┐│
│  │  Robocodec::open(input) → .write_to(output) → .run()          ││
│  │  KpsConverter::new(input, output) → .config() → .run()         ││
│  └────────────────────────────────────────────────────────────────┘│
├─────────────────────────────────────────────────────────────────────┤
│                       Auto-Configuration                           │
│  ┌────────────────────────────────────────────────────────────────┐│
│  │  Hardware Detection → Performance Mode → Pipeline Config       ││
│  └────────────────────────────────────────────────────────────────┘│
├─────────────────────────────────────────────────────────────────────┤
│                          I/O Layer                                   │
│  ┌──────────────────┐           ┌──────────────────┐                │
│  │  MCAP Format     │           │  ROS Bag Format  │                │
│  │  Reader/Writer   │           │  Reader/Writer   │                │
│  └──────────────────┘           └──────────────────┘                │
├─────────────────────────────────────────────────────────────────────┤
│                       Language Bindings                             │
│  ┌────────────────────┐          ┌────────────────────┐             │
│  │     Rust API       │          │    Python API      │             │
│  │  (native library)  │          │    (PyO3 bindings) │             │
│  └────────────────────┘          └────────────────────┘             │
└─────────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Codec Layer

**Location**: `src/encoding/`

The codec layer handles message encoding and decoding:

| Codec | Purpose | File |
|-------|---------|------|
| CDR | ROS1/ROS2 serialization | `src/encoding/cdr/` |
| Protobuf | Protocol Buffers | `src/encoding/protobuf/` |
| JSON | Human-readable format | `src/encoding/json/` |

**Design**: Each codec implements a common trait for encode/decode operations.

### 2. Schema Parser

**Location**: `src/schema/parser/`

Parses robotics interface definition languages:

- **ROS `.msg` files**: ROS1 message definitions
- **ROS2 IDL**: ROS2 interface definitions
- **OMG IDL**: Standard IDL format

**Implementation**: Uses Pest parser combinator for grammar definitions.

### 3. Pipeline System

**Location**: `src/pipeline/`

See [PIPELINE.md](PIPELINE.md) for detailed documentation.

**Three pipeline implementations:**

1. **Standard Pipeline** (`src/pipeline/`): 4-stage design for balanced performance
2. **HyperPipeline** (`src/pipeline/hyper/`): 7-stage design for maximum throughput (2000+ MB/s)
3. **KPS Pipeline** (`src/pipeline/kps/`): Converts robotics data to KPS dataset format (experimental)

### 4. Fluent API

**Location**: `src/pipeline/fluent/`

User-friendly, type-safe API for file processing:

```rust
use robocodec::pipeline::fluent::{Robocodec, CompressionPreset};

// Single file conversion
Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .with_compression(CompressionPreset::Balanced)
    .run()?;

// HyperPipeline with auto-configuration
Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .hyper()
    .mode(PerformanceMode::Throughput)
    .run()?;

// Batch processing
Robocodec::open(vec!["file1.bag", "file2.bag"])?
    .write_to("/output/dir")
    .run()?;
```

**KPS Pipeline Fluent API:**

```rust
use robocodec::pipeline::kps::KpsConverter;

// Simple conversion
let report = KpsConverter::new("input.mcap", "output_dir")
    .config("config.toml")
    .run()?;

// V1.2 delivery with statistics tracking
let report = KpsConverter::new("input.mcap", "output_dir")
    .config("config.toml")
    .v12_delivery()
    .robot("Kuavo4Pro")
    .end_effector("Dexhand")
    .scene("Housekeeper")
    .sub_scene("Kitchen")
    .task("Dispose_of_takeout_containers")
    .with_statistics()  // Auto-rename directory with actual stats
    .run()?;
```

**Features:**
- Type-state pattern ensures valid API usage
- Automatic output file naming
- Single file and batch processing modes
- Progress reporting for batch operations

### 5. Auto-Configuration

**Location**: `src/pipeline/auto_config/`

Hardware-aware automatic pipeline configuration:

```rust
pub enum PerformanceMode {
    Throughput,        // Maximum throughput on beefy machines
    Balanced,          // Middle ground (default)
    MemoryEfficient,   // Conserve memory
}

// Auto-detect hardware and configure
let config = PipelineAutoConfig::auto(PerformanceMode::Throughput)
    .to_hyper_config(input, output)
    .build();
```

**Auto-detected parameters:**
- CPU core count (with reservation for system)
- Available memory
- L3 cache size (for ZSTD WindowLog tuning)
- Optimal batch sizes
- Channel capacities

### 6. GPU Compression

**Location**: `src/pipeline/gpu/`

Experimental GPU-accelerated compression:

| Platform | Backend | Feature Flag |
|----------|---------|--------------|
| NVIDIA (Linux) | nvCOMP | `gpu-nvcomp` |
| Apple Silicon | libcompression | `gpu-accelerate` |
| Fallback | CPU ZSTD | default |

**Usage:**
```rust
// Automatically selected when feature enabled
let config = HyperPipelineConfig::builder()
    .compression_backend(CompressionBackend::Auto)
    .build()?;
```

### 7. I/O Layer

**Location**: `src/io/`

Unified interface for different storage formats:

```rust
pub trait BagSource {
    fn channels(&self) -> &[ChannelInfo];
    fn read_chunk(&mut self, chunk_size: usize) -> Result<MessageChunk>;
}

pub trait BagWriter {
    fn write(&mut self, chunk: MessageChunk) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
}
```

### 8. Type System

**Location**: `src/core/`

Core types used throughout the library:

- **`CodecValue`**: Unified representation for all message types
- **`MessageSchema`**: Schema definition with field descriptors
- **`Error`**: Comprehensive error types with context

## Module Structure

```
src/
├── bin/                   # Command-line tools
│   ├── convert.rs         # Unified convert command
│   ├── extract.rs         # Data extraction
│   ├── inspect.rs         # File inspection
│   ├── schema.rs          # Schema utilities
│   ├── search.rs          # Data search
│   └── extract_sample.rs  # Sample creation
│
├── core/                  # Core types and errors
│   ├── error.rs           # Error definitions
│   ├── registry.rs        # Codec registry
│   └── value.rs           # CodecValue type
│
├── encoding/              # Message codecs
│   ├── cdr/               # CDR (ROS1/ROS2)
│   ├── protobuf/          # Protobuf
│   └── json/              # JSON
│
├── io/                    # Unified I/O layer
│   ├── formats/           # Format implementations
│   │   ├── bag.rs         # ROS bag format
│   │   ├── bag_parallel.rs # Parallel bag reading
│   │   ├── mcap.rs        # MCAP format
│   │   └── mcap_two_pass.rs # Two-pass MCAP
│   └── kps/               # KPS dataset format (experimental)
│       ├── config.rs      # KPS configuration
│       ├── delivery_v12.rs # v1.2 delivery structure
│       ├── hdf5_schema.rs # HDF5 schema definitions
│       ├── video_encoder.rs # Video encoding
│       ├── writers/       # KPS writers
│       │   ├── base.rs    # Base writer traits
│       │   ├── v12_hdf5.rs # v1.2 HDF5 writer
│       │   ├── original_hdf5.rs # Original data writer
│       │   └── audio_writer.rs # Audio writer
│       └── robot_calibration.rs # Robot calibration
│
├── schema/                # Schema parsing
│   └── parser/            # IDL/MSG parsers
│
├── pipeline/              # Processing pipeline
│   ├── stages/            # Pipeline stages (standard)
│   ├── types/             # Chunk and arena types
│   ├── fluent/            # Builder API
│   ├── hyper/             # 7-stage HyperPipeline
│   ├── kps/               # KPS conversion pipeline
│   │   ├── fluent.rs      # KPS fluent API
│   │   ├── config.rs      # KPS pipeline config
│   │   └── traits/        # KPS traits
│   ├── gpu/               # GPU compression
│   ├── auto_config.rs     # Auto-configuration
│   └── orchestrator.rs    # Standard pipeline coordinator
│
├── transform/             # Data transformations
└── python/                # Python bindings
```

## Design Decisions

### Why Rust?

- **Memory safety**: No garbage collection pauses
- **Zero-cost abstractions**: High-level code, low-level performance
- **Cross-platform**: Linux, macOS, Windows
- **FFI friendly**: Easy Python bindings via PyO3

### Why Arena Allocation?

Robotics data processing involves millions of small messages:
- Traditional allocation: High overhead
- Arena allocation: Bulk allocation, bulk deallocation
- **Result**: ~22% CPU reduction

### Why Two Pipeline Designs?

**Standard Pipeline**: Simpler, easier to understand, good for most use cases

**HyperPipeline**: Maximum throughput for large-scale conversions
- More stages = better parallelization
- Isolated stages = no contention
- Platform-specific I/O optimizations
- **Result**: 3-5x higher throughput on multi-core systems

### Why Schema-Driven?

Robotics uses many message types:
- Hand-written codecs: Impractical for hundreds of types
- Schema-driven: Parse once, encode/decode many times
- **Result**: Support for any ROS message without code generation

## KPS Integration (Experimental)

> **⚠️ Experimental Feature**: The KPS integration is currently experimental and under active development. APIs may change between versions.

The KPS (Kupas) pipeline converts robotics data from MCAP format to KPS dataset format for robotics learning applications.

### Features

- **v1.2 Specification Support**: Compliant directory structure with Robot-EndEffector-Scene naming
- **Time Alignment**: Multiple strategies (Linear, Nearest Neighbor, Hold Last)
- **Camera Extraction**: Automatic camera parameter extraction from TF and camera_info
- **Statistics Tracking**: Automatic directory naming with actual statistics (size, counts, duration)
- **Multiple Output Formats**: HDF5 (legacy), Parquet + MP4 (v3.0)
- **Audio Support**: WAV file output for audio data
- **Original Data Storage**: `proprio_stats_original.hdf5` for unaligned data

### Fluent API

```rust
use robocodec::pipeline::kps::KpsConverter;

// Basic conversion
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

### Feature Flags

| Flag | Description |
|------|-------------|
| `kps-hdf5` | HDF5 dataset support |
| `kps-parquet` | Parquet dataset support |
| `kps-depth` | Depth video support |
| `kps-all` | All KPS features |

## New Features

### Enhanced CLI Tools

**Unified convert command:**
```bash
# BAG to MCAP
robocodec convert bag-to-mcap -i input.bag -o output.mcap

# MCAP to BAG
robocodec convert mcap-to-bag -i input.mcap -o output.bag

# Normalize with transformations
robocodec convert normalize -c transform.toml -i input.bag -o output.mcap
```

## Performance Characteristics

### Throughput

| Pipeline Mode | Operation | Typical Throughput |
|---------------|-----------|-------------------|
| Standard | BAG → MCAP (no compression) | ~500 MB/s |
| Standard | BAG → MCAP (ZSTD-3) | ~200 MB/s |
| HyperPipeline | BAG → MCAP (ZSTD-3) | ~1800 MB/s |
| HyperPipeline | MCAP → BAG (decompress) | ~2500 MB/s |

### Memory

Per-operation memory usage:
- **Arena pool**: ~100MB (depends on CPU count)
- **Buffer pool**: ~50MB (depends on worker count)
- **In-flight data**: ~256MB (16 chunks × 16MB)

## Language Support

### Rust API

Native library with full feature access:
```rust
use robocodec::pipeline::fluent::{Robocodec, CompressionPreset};

Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .with_compression(CompressionPreset::Balanced)
    .run()?;
```

### Python API

PyO3 bindings with feature parity:
```python
from robocodec import Reader, Writer

with Reader("data.bag") as reader:
    with Writer("output.mcap") as writer:
        for topic, message in reader:
            writer.write(topic, message)
```

## Extensibility

### Adding a New Codec

1. Implement `MessageCodec` trait
2. Register in `src/core/registry.rs`
3. Add schema parser if needed

### Adding a New File Format

1. Implement `BagSource` for reading
2. Implement `BagWriter` for writing
3. Register in `src/io/mod.rs`

### Adding a New Transform

1. Implement `Transform` trait
2. Add to `TransformPipeline`
3. Wire into orchestrator

## Trade-offs

### Simplicity vs Performance

We offer two pipeline modes:
- **Standard**: Simpler code, easier debugging, good performance
- **HyperPipeline**: More complex, but 3-5x faster

### Memory vs Throughput

HyperPipeline uses more memory for better throughput:
- Large batching: Better compression and parallelism
- More stages: Higher in-flight data
- **Result**: ~1-2GB typical usage for maximum throughput

### Compatibility vs Features

ROS has many edge cases:
- We focus on common cases
- Handle ROS1 and ROS2
- **Result**: Works with 99% of real-world data

## See Also

- [PIPELINE.md](PIPELINE.md) - Detailed pipeline architecture
- [MEMORY.md](MEMORY.md) - Memory management details
- [CONTRIBUTING.md](../CONTRIBUTING.md) - Contribution guidelines
- [benches/README.md](../benches/README.md) - Benchmarking guide
