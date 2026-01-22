# Robocodec Architecture

This document provides a high-level overview of Robocodec's architecture and design decisions.

## Overview

Robocodec is a **schema-driven, universal robotics data codec** that enables efficient conversion between different robotics message formats and storage formats. The project is organized as a Cargo workspace with two crates:

- **`robocodec`** - Low-level format library for robotics data
- **`robocodec`** - High-level pipeline and conversion tool

```
 Workspace: robocodec
 ----------------------------------------------------------------------
| Crate: robocodec                                                      |
|  --------       --------       --------                            |
| |  CDR   |     |Protobuf|     |  JSON  |                           |
| | Codec  |     | Codec  |     | Codec  |                           |
|  --------       --------       --------                            |
|  ----------------------                                         |
| |   Schema Parser    |                                         |
| | (ROS .msg, IDL)    |                                         |
|  ----------------------                                         |
|  ----------------------                                         |
| |   Arena Types     |                                         |
| | (Allocation, I/O) |                                         |
|  ----------------------                                         |
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
| |     Format I/O: MCAP, ROS bag readers/writers  |               |
|  ----------------------------------------                          |
|  ----------------------------------------                          |
| |     Transform: Topic/Type Renaming      |                      |
|  ----------------------------------------                          |
|  ----------------------------------------                          |
| |     KPS Writer (experimental)           |                      |
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

### Crate: robocodec

**Purpose**: Low-level robotics data format library

**Location**: `robocodec/src/`

**Modules**:

| Module | Description |
|--------|-------------|
| `core/` | Core types, errors, encoding registry |
| `encoding/` | Message codecs (CDR, Protobuf, JSON) |
| `schema/` | Schema parser (ROS .msg, ROS2 IDL, OMG IDL) |
| `io/` | Unified I/O layer with format implementations |
| `io/formats/bag/` | ROS bag format support |
| `io/formats/mcap/` | MCAP format support |

**Design**: This crate provides the foundational types and codec logic that `robocodec` builds upon. It can be used independently for low-level robotics data access.

### Crate: robocodec

**Purpose**: High-level pipeline and conversion tool

**Location**: `src/`

**Modules**:

| Module | Description |
|--------|-------------|
| `core/` | Core types and configuration |
| `pipeline/` | Processing pipelines (Standard, HyperPipeline) |
| `io/` | Unified I/O layer (readers, writers, format detection) |
| `io/formats/` | Format-specific handlers (MCAP, ROS bag, KPS) |
| `transform/` | Data transformations (topic/type renaming) |
| `python/` | Python bindings via PyO3 |

**Design**: This crate depends on `robocodec` and provides the user-facing APIs including the fluent API, format I/O, transformations, and Python bindings.

## Core Components

### 1. Codec Layer (robocodec)

**Location**: `robocodec/src/encoding/`

The codec layer handles message encoding and decoding:

| Codec | Purpose | File |
|-------|---------|------|
| CDR | ROS1/ROS2 serialization | `robocodec/src/encoding/cdr.rs` |
| Protobuf | Protocol Buffers | `robocodec/src/encoding/protobuf.rs` |
| JSON | Human-readable format | `robocodec/src/encoding/json.rs` |

**Design**: Each codec implements a common trait for encode/decode operations.

### 2. Schema Parser (robocodec)

**Location**: `robocodec/src/schema/`

Parses robotics interface definition languages:

- **ROS `.msg` files**: ROS1 message definitions
- **ROS2 IDL**: ROS2 interface definitions
- **OMG IDL**: Standard IDL format

**Implementation**: Uses Pest parser combinator for grammar definitions.

### 3. Format I/O Layer (robocodec)

**Location**: `src/io/`

Unified interface for robotics data file formats:

| Module | Description |
|--------|-------------|
| `reader/` | Unified reader interface with format auto-detection |
| `writer/` | Unified writer interface |
| `formats/` | Format-specific implementations (MCAP, ROS bag, KPS) |
| `detection.rs` | File format detection |
| `traits.rs` | Core I/O traits |
| `arena.rs` | Arena allocation types |

**Format Handlers** (in `src/io/formats/`):

| Format | Reader | Writer |
|--------|--------|--------|
| MCAP | `mcap.rs`, `mcap_sequential.rs`, `mcap_two_pass.rs` | Via writer interface |
| ROS Bag | `bag.rs`, `bag_parser.rs`, `bag_parallel.rs` | Via writer interface |
| KPS | — | `kps/` directory (experimental) |

### 4. Pipeline System (robocodec)

**Location**: `src/pipeline/`

See [PIPELINE.md](PIPELINE.md) for detailed documentation.

**Two pipeline implementations:**

1. **Standard Pipeline** (`src/pipeline/stages/`): 4-stage design for balanced performance
2. **HyperPipeline** (`src/pipeline/hyper/`): 7-stage design for maximum throughput

### 5. Fluent API (robocodec)

**Location**: `src/pipeline/fluent/`

User-friendly, type-safe API for file processing:

```rust
use robocodec::pipeline::fluent::Robocodec;

// Simple conversion with auto-detection
Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
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
| NVIDIA (Linux) | nvCOMP | `gpu` |
| Apple Silicon | libcompression | `gpu` |
| Fallback | CPU ZSTD | default |

**Usage:**
```rust
// Automatically selected when feature enabled
let config = HyperPipelineConfig::builder()
    .compression_backend(CompressionBackend::Auto)
    .build()?;
```

### 8. Transform Layer (robocodec)

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

### 9. KPS Format Writer (robocodec, experimental)

**Location**: `src/io/formats/kps/`

Converts robotics data to KPS dataset format for robotics learning:

- **v1.2 Specification Support**: Compliant directory structure
- **Multiple Output Formats**: HDF5, Parquet + MP4
- **Configuration**: TOML-based configuration

```rust
use robocodec::pipeline::fluent::Robocodec;

// Convert to KPS format
Robocodec::open(vec!["input.mcap"])?
    .write_to_kps("output_dir")
    .config("kps_config.toml")
    .run()?;
```

## Design Decisions

### Why Split into Two Crates?

The workspace structure separates concerns:

1. **`robocodec`** - Low-level format handling
   - Can be used independently
   - Stable API for format access
   - Minimal dependencies

2. **`robocodec`** - High-level processing
   - Depends on `robocodec`
   - Fluent API and transformations
   - Python bindings

This allows other projects to use `robocodec` for format access without pulling in the full pipeline infrastructure.

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
use robocodec::pipeline::fluent::Robocodec;

Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
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

1. Implement codec trait in `robocodec/src/encoding/`
2. Register in `robocodec/src/core/registry.rs`
3. Add schema parser if needed

### Adding a New File Format

1. Implement format reader/writer in `src/io/formats/`
2. Add format detection in `src/io/detection.rs`
3. Implement the I/O traits from `src/io/traits.rs`

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
- [README.md](../README.md) - Usage documentation
