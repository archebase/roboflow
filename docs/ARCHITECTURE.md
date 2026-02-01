# Roboflow Architecture

This document provides a high-level overview of Roboflow's architecture and design decisions.

## Overview

Roboflow is a **high-performance robotics data processing pipeline** built on top of the `robocodec` library. It provides schema-driven conversion between different robotics message formats (CDR, Protobuf, JSON) and storage formats (MCAP, ROS1 bag).

```
┌─────────────────────────────────────────────────────────────────┐
│                         Roboflow                                │
│  ┌────────────────────────────────────────────────────────┐    │
│  │                 Fluent API                            │    │
│  │            Roboflow::open()->run()                    │    │
│  └────────────────────────────────────────────────────────┘    │
│  ┌────────────────────────────────────────────────────────┐    │
│  │              Pipeline System                          │    │
│  │  ┌──────────────┐  ┌──────────────────────────┐       │    │
│  │  │   Standard   │  │    HyperPipeline (7)     │       │    │
│  │  │  (4-stage)   │  │   Maximum throughput     │       │    │
│  │  └──────────────┘  └──────────────────────────┘       │    │
│  └────────────────────────────────────────────────────────┘    │
│  ┌────────────────────────────────────────────────────────┐    │
│  │            Python Bindings (PyO3)                     │    │
│  └────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                            │ depends on
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                        robocodec                                │
│                    (external crate)                            │
│  ┌────────────────────────────────────────────────────────┐    │
│  │              Format I/O Layer                          │    │
│  │  ┌─────────┐  ┌─────────┐  ┌──────────────────────┐   │    │
│  │  │  MCAP   │  │ ROS Bag │  │   KPS (experimental) │   │    │
│  │  └─────────┘  └─────────┘  └──────────────────────┘   │    │
│  └────────────────────────────────────────────────────────┘    │
│  ┌────────────────────────────────────────────────────────┐    │
│  │              Codec Layer                              │    │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐               │    │
│  │  │   CDR   │  │Protobuf │  │  JSON   │               │    │
│  │  └─────────┘  └─────────┘  └─────────┘               │    │
│  └────────────────────────────────────────────────────────┘    │
│  ┌────────────────────────────────────────────────────────┐    │
│  │           Schema Parser & Types                       │    │
│  │     ROS .msg │ ROS2 IDL │ OMG IDL │ Arena Types      │    │
│  └────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

## Project Structure

### Roboflow Crate

**Location**: `src/`

**Purpose**: High-level pipeline orchestration and user-facing APIs

**Modules**:

| Module | Description |
|--------|-------------|
| `pipeline/` | Processing pipelines (Standard, HyperPipeline) |
| `pipeline/fluent/` | Type-safe builder API |
| `pipeline/hyper/` | 7-stage HyperPipeline implementation |
| `pipeline/auto_config.rs` | Hardware-aware auto-configuration |
| `pipeline/gpu/` | GPU compression support (experimental) |
| `python/` | PyO3 bindings for Python API |
| `bin/` | CLI tools (convert, extract, inspect, schema, search) |

**Design**: Roboflow depends on the external `robocodec` crate for all low-level format handling, codecs, and schema parsing.

### Robocodec (External Dependency)

**Source**: `https://github.com/archebase/robocodec`

**Purpose**: Low-level robotics data format library

**Capabilities**:
- **Codec Layer**: CDR, Protobuf, JSON encoding/decoding
- **Schema Parser**: ROS `.msg`, ROS2 IDL, OMG IDL
- **Format I/O**: MCAP, ROS bag readers/writers
- **Transform**: Topic/type renaming, normalization
- **Types**: Arena allocation, zero-copy message types

**Why External?**
- **Separation of concerns**: Format handling vs. pipeline orchestration
- **Reusability**: `robocodec` can be used independently
- **Focused development**: Each crate has a clear responsibility

## Core Components

### 1. Pipeline System

**Location**: `src/pipeline/`

Two pipeline implementations for different use cases:

| Pipeline | Stages | Target Throughput | Use Case |
|----------|--------|-------------------|----------|
| **Standard** | 4 | ~200 MB/s | Balanced performance, simplicity |
| **HyperPipeline** | 7 | ~1800+ MB/s | Maximum throughput, large-scale conversions |

### 2. Fluent API

**Location**: `src/pipeline/fluent/`

User-friendly, type-safe API:

```rust
use roboflow::pipeline::fluent::Roboflow;

// Simple conversion
Roboflow::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .run()?;

// HyperPipeline with auto-configuration
Roboflow::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .hyper_mode()
    .run()?;
```

### 3. Auto-Configuration

**Location**: `src/pipeline/auto_config.rs`

Hardware-aware automatic pipeline tuning:

```rust
pub enum PerformanceMode {
    Throughput,        // Maximum throughput
    Balanced,          // Middle ground (default)
    MemoryEfficient,   // Conserve memory
}

let config = PipelineAutoConfig::auto(PerformanceMode::Throughput)
    .to_hyper_config(input, output)
    .build();
```

### 4. Python Bindings

**Location**: `src/python/`

PyO3 bindings with feature parity:

```python
from roboflow import Roboflow

# Use via fluent API
Roboflow.open(["input.bag"]).write_to("output.mcap").run()
```

## CLI Tools

| Tool | Location | Purpose |
|------|----------|---------|
| `convert` | `src/bin/convert.rs` | Unified format conversion |
| `extract` | `src/bin/extract.rs` | Extract data from files |
| `inspect` | `src/bin/inspect.rs` | Inspect file metadata |
| `schema` | `src/bin/schema.rs` | Work with schema definitions |
| `search` | `src/bin/search.rs` | Search through data files |

## Design Decisions

### Why Separate Crates?

| Roboflow | Robocodec |
|----------|-----------|
| Pipeline orchestration | Format handling |
| Fluent API | Codecs (CDR/Protobuf/JSON) |
| Auto-configuration | Schema parsing |
| Python bindings | MCAP/ROS bag I/O |
| GPU compression | Arena types |

This separation allows:
1. **Independent development**: Format handling evolves separately from pipeline logic
2. **Reusability**: `robocodec` can be used in other projects
3. **Clear boundaries**: Each crate has a focused responsibility

### Why Rust?

- **Memory safety**: No garbage collection pauses
- **Zero-cost abstractions**: High-level code, low-level performance
- **Cross-platform**: Linux, macOS, Windows
- **FFI friendly**: Easy Python bindings via PyO3

### Why Two Pipeline Designs?

| Standard | HyperPipeline |
|----------|---------------|
| Simpler, easier to understand | Maximum throughput |
| Good for most use cases | Large-scale conversions |
| ~200 MB/s | ~1800+ MB/s (9x faster) |

## Performance Characteristics

### Throughput

| Pipeline Mode | Operation | Throughput |
|---------------|-----------|------------|
| Standard | BAG → MCAP (ZSTD-3) | ~200 MB/s |
| HyperPipeline | BAG → MCAP (ZSTD-3) | ~1800 MB/s |

### Memory

| Component | Typical Usage |
|-----------|---------------|
| Arena pool | ~100MB (depends on CPU count) |
| Buffer pool | ~50MB (depends on worker count) |
| In-flight data | ~256MB (16 chunks × 16MB) |
| **Total** | ~600MB (8-core system) |

## Language Support

### Rust API (Native)

```rust
use roboflow::pipeline::fluent::Roboflow;

Roboflow::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .run()?;
```

### Python API (PyO3)

```python
from roboflow import Roboflow

Roboflow.open(["input.bag"]).write_to("output.mcap").run()
```

## Feature Flags

| Flag | Description |
|------|-------------|
| `python` | Python bindings via PyO3 |
| `dataset-hdf5` | HDF5 dataset support |
| `dataset-parquet` | Parquet dataset support |
| `dataset-depth` | Depth video support |
| `dataset-all` | All KPS features |
| `cli` | CLI tools |
| `jemalloc` | Use jemalloc allocator (Linux) |
| `gpu` | GPU compression support |

## See Also

- [DISTRIBUTED_DESIGN.md](DISTRIBUTED_DESIGN.md) - Distributed system design for 10 Gbps throughput
- [PIPELINE.md](PIPELINE.md) - Detailed pipeline architecture
- [MEMORY.md](MEMORY.md) - Memory management details
- [README.md](../README.md) - Usage documentation
