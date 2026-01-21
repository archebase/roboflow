# Robocodec Architecture

This document provides a high-level overview of Robocodec's architecture and design decisions.

## Overview

Robocodec is a **schema-driven, universal robotics data codec** that enables efficient conversion between different robotics message formats and storage formats. The project is organized as a Cargo workspace with two crates:

- **`robofmt`** - Low-level format library for robotics data
- **`robocodec`** - High-level pipeline and conversion tool

```
 Workspace: robocodec
 ----------------------------------------------------------------------
| Crate: robofmt                                                      |
|  --------       --------       --------       --------------       |
| |  CDR   |     |Protobuf|     |  JSON  |     |   Schema     |      |
| | Codec  |     | Codec  |     | Codec  |     |   Parser     |      |
|  --------       --------       --------       --------------       |
|  ----------------------        ----------------------               |
| |   Format Readers    |      |   Format Writers     |             |
| |   (MCAP, BAG)       |      |   (MCAP, BAG)        |             |
|  ----------------------        ----------------------               |
 ----------------------------------------------------------------------
 ----------------------------------------------------------------------
| Crate: robocodec                                                     |
|  ---------------------       -----------------------------------     |
| | Standard Pipeline  |     |      HyperPipeline (7-stage)  |      |
| | Reader->Transform->|     |  Prefetch->Parse->Batch->      |     |
| | Compress->Write    |     |  Transform->Compress->Write   |     |
|  ---------------------       -----------------------------------     |
|  ----------------------------------------                          |
| |     Fluent API: Robocodec::open()->run() |                     |
|  ----------------------------------------                          |
|  ----------------------------------------                          |
| |     Transform: Topic/Type Renaming      |                      |
|  ----------------------------------------                          |
|  ----------------------------------------                          |
| |     KPS Format Writer (experimental)    |                      |
|  ----------------------------------------                          |
|  ----------------------------------------                          |
| |     Python Bindings (PyO3)              |                      |
|  ----------------------------------------                          |
 ----------------------------------------------------------------------
 ----------------------------------------------------------------------
|                       Language Bindings                             |
|  ------------------          ------------------                      |
| |    Rust API       |        |    Python API     |                  |
| |  (native library) |        |  (PyO3 bindings)  |                  |
|  ------------------          ------------------                      |
 ----------------------------------------------------------------------
```

## Workspace Structure

### Crate: robofmt

**Purpose**: Low-level robotics data format library

**Location**: `robofmt/src/`

**Modules**:

| Module | Description |
|--------|-------------|
| `core/` | Core types (CodecValue, errors, Encoding) |
| `encoding/` | Message codecs (CDR, Protobuf, JSON) |
| `schema/` | Schema parser (ROS .msg, ROS2 IDL, OMG IDL) |
| `io/` | I/O types (arena, metadata, traits) |
| `mcap/` | MCAP format reader |
| `bag/` | ROS1 bag format reader |

**Design**: This crate provides the foundational types and format-specific logic that `robocodec` builds upon. It can be used independently for low-level robotics data access.

### Crate: robocodec

**Purpose**: High-level pipeline and conversion tool

**Location**: `src/`

**Modules**:

| Module | Description |
|--------|-------------|
| `core/` | Core types and errors |
| `encoding/` | Re-exports from robofmt |
| `schema/` | Re-exports from robofmt |
| `io/` | Unified I/O layer (readers, writers) |
| `formats/` | Format-specific handlers (MCAP, BAG) |
| `format/` | High-level format APIs |
| `transform/` | Data transformations |
| `pipeline/` | Processing pipelines |
| `python/` | Python bindings |

**Design**: This crate depends on `robofmt` and provides the user-facing APIs including the fluent API, transformations, and Python bindings.

## Core Components

### 1. Codec Layer (robofmt)

**Location**: `robofmt/src/encoding/`

The codec layer handles message encoding and decoding:

| Codec | Purpose | File |
|-------|---------|------|
| CDR | ROS1/ROS2 serialization | `robofmt/src/encoding/cdr.rs` |
| Protobuf | Protocol Buffers | `robofmt/src/encoding/protobuf.rs` |
| JSON | Human-readable format | `robofmt/src/encoding/json.rs` |

**Design**: Each codec implements a common trait for encode/decode operations.

### 2. Schema Parser (robofmt)

**Location**: `robofmt/src/schema/`

Parses robotics interface definition languages:

- **ROS `.msg` files**: ROS1 message definitions
- **ROS2 IDL**: ROS2 interface definitions
- **OMG IDL**: Standard IDL format

**Implementation**: Uses Pest parser combinator for grammar definitions.

### 3. Format Readers (robofmt)

**Location**: `robofmt/src/mcap/` and `robofmt/src/bag/`

Format-specific readers for robotics data files:

| Format | Reader | Features |
|--------|--------|----------|
| MCAP | `McapFormat` | Index-based, parallel access |
| ROS1 Bag | `BagFormat` | Chunk-based parsing |

### 4. Pipeline System (robocodec)

**Location**: `src/pipeline/`

See [PIPELINE.md](PIPELINE.md) for detailed documentation.

**Two pipeline implementations:**

1. **Standard Pipeline** (`src/pipeline/`): 4-stage design for balanced performance
2. **HyperPipeline** (`src/pipeline/hyper/`): 7-stage design for maximum throughput

### 5. Fluent API (robocodec)

**Location**: `src/pipeline/fluent/`

User-friendly, type-safe API for file processing:

```rust
use robocodec::pipeline::fluent::{Robocodec, CompressionPreset};

// Single file conversion
Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .compression(CompressionPreset::Balanced)
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

**Features:**
- Type-state pattern ensures valid API usage
- Automatic output file naming
- Single file and batch processing modes
- Progress reporting for batch operations

### 6. Auto-Configuration (robocodec)

**Location**: `src/pipeline/auto_config.rs`

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

### 7. GPU Compression (robocodec)

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

### 8. Unified I/O Layer (robocodec)

**Location**: `src/io/`

Unified interface for different storage formats:

```rust
pub trait FormatReader {
    fn channels(&self) -> &[ChannelInfo];
    fn read_chunk(&mut self, chunk_size: usize) -> Result<MessageChunk>;
}

pub trait FormatWriter {
    fn write(&mut self, chunk: MessageChunk) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
}
```

The I/O layer includes:
- **Reader builder**: Automatic format detection and strategy selection
- **Writer builder**: Format-specific writers
- **Strategy pattern**: Sequential vs parallel reading based on file capabilities

### 9. Transform Layer (robocodec)

**Location**: `src/transform/`

Data transformation capabilities:

- **Topic renaming**: Rename topics during conversion
- **Type renaming**: Rename message types
- **Type normalization**: Normalize ROS1/ROS2 type differences

```rust
let transform = TransformBuilder::new()
    .with_topic_rename("/old", "/new")
    .with_type_rename("std_msgs/String", "std_msgs/msg/String")
    .build();
```

### 10. KPS Format Writer (robocodec, experimental)

**Location**: `src/io/formats/kps/`

Converts robotics data to KPS dataset format:

- **v1.2 Specification Support**: Compliant directory structure
- **Multiple Output Formats**: HDF5, Parquet + MP4
- **Configuration**: TOML-based configuration

```rust
use robocodec::io::formats::kps::{Hdf5KpsWriter, KpsConfig};

let config = KpsConfig::from_file("config.toml")?;
let writer = Hdf5KpsWriter::new("output_dir", config)?;
```

## Design Decisions

### Why Split into Two Crates?

The workspace structure separates concerns:

1. **`robofmt`** - Low-level format handling
   - Can be used independently
   - Stable API for format access
   - Minimal dependencies

2. **`robocodec`** - High-level processing
   - Depends on `robofmt`
   - Fluent API and transformations
   - Python bindings

This allows other projects to use `robofmt` for format access without pulling in the full pipeline infrastructure.

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

The KPS (Kupas) format writer converts robotics data from MCAP format to KPS dataset format for robotics learning applications.

### Features

- **v1.2 Specification Support**: Compliant directory structure
- **Multiple Output Formats**: HDF5 (legacy), Parquet + MP4
- **Configuration**: TOML-based configuration

### Feature Flags

| Flag | Description |
|------|-------------|
| `kps-hdf5` | HDF5 dataset support |
| `kps-parquet` | Parquet dataset support |
| `kps-depth` | Depth video support |
| `kps-all` | All KPS features |

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
    .compression(CompressionPreset::Balanced)
    .run()?;
```

### Python API

PyO3 bindings with feature parity:
```python
from robocodec import RoboReader, RoboWriter

with RoboReader("data.bag") as reader:
    with RoboWriter("output.mcap") as writer:
        for topic, message in reader:
            writer.write(topic, message)
```

## Extensibility

### Adding a New Codec

1. Implement codec trait in `robofmt/src/encoding/`
2. Register in `robofmt/src/core/registry.rs`
3. Add schema parser if needed

### Adding a New File Format

1. Implement `FormatReader` in `robofmt/src/`
2. Implement `FormatWriter` in `robofmt/src/`
3. Add format detection in `robofmt/src/io/detection.rs`

### Adding a New Transform

1. Implement transform trait in `src/transform/`
2. Add to `TransformBuilder`
3. Wire into pipeline

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
- [CONTRIBUTING.md](CONTRIBUTING.md) - Contribution guidelines
- [benches/README.md](../benches/README.md) - Benchmarking guide
