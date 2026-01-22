# Robocodec Architecture Overview

This document provides a high-level overview of the Robocodec architecture and component organization.

## System Overview

Robocodec is a universal, schema-driven runtime decoding engine for robotics data. It provides high-performance conversion between different robotics message formats (CDR, Protobuf, JSON) and storage formats (MCAP, ROS1 bag).

## Workspace Architecture

### Two-Crate Design

The project is organized as a Cargo workspace with two crates:

```
roboflow (workspace root)
├── roboflow/          # Top-level pipeline crate
│   └── depends on → robocodec
└── robocodec/            # Bottom-level format library crate
```

### Dependency Direction

```
┌─────────────────────────────────────┐
│      roboflow (pipeline)           │
│  ┌──────────────────────────────┐   │
│  │ • Pipeline orchestration     │   │
│  │ • Fluent API                │   │
│  │ • Python bindings           │   │
│  │ • CLI tools                 │   │
│  └──────────┬───────────────────┘   │
└─────────────┼───────────────────────┘
              │ depends on
              ↓
┌─────────────────────────────────────┐
│       robocodec (I/O layer)           │
│  ┌──────────────────────────────┐   │
│  │ • Format readers/writers     │   │
│  │ • Message codecs             │   │
│  │ • Schema parsers             │   │
│  │ • Arena allocation           │   │
│  │ • Core types                 │   │
│  └──────────────────────────────┘   │
└─────────────────────────────────────┘
```

**Key Principle:** `roboflow` depends on `robocodec`, not vice versa. This separation allows:
- Reuse of `robocodec` as a standalone library
- Clear separation of concerns (I/O vs. orchestration)
- Independent testing and evolution

## Component Organization

### robocodec Crate (Bottom Layer)

Located at `robocodec/src/`:

```
robocodec/src/
├── encoding/          # Message codec implementations
│   ├── protobuf/      # Protobuf codec
│   ├── json/          # JSON codec
│   └── registry.rs    # Codec registry
├── schema/            # Schema parsing
│   └── parser/        # PEG-based parsers
│       ├── msg_parser/       # ROS .msg
│       ├── idl_parser/       # ROS2 IDL
│       └── unified.rs        # Unified parser
├── io/                # Unified I/O layer
│   ├── formats/
│   │   ├── mcap/       # MCAP format
│   │   └── bag/        # ROS bag format
│   └── kps/           # KPS dataset format
├── transform/         # Data transformations
│   ├── topic_rename.rs
│   ├── type_rename.rs
│   └── normalization.rs
├── types/             # Core memory management
│   ├── arena.rs       # Arena allocation
│   ├── arena_pool.rs  # Arena pooling
│   ├── buffer_pool.rs # Compression buffers
│   └── chunk.rs       # Message chunks
├── core/              # Core types and errors
└── surface/           # High-level surface API
```

### roboflow Crate (Top Layer)

Located at `src/`:

```
src/
├── pipeline/          # Pipeline implementations
│   ├── stages/        # Standard pipeline stages
│   │   ├── reader.rs
│   │   ├── transform.rs
│   │   ├── compression.rs
│   │   └── writer.rs
│   ├── hyper/         # 7-stage HyperPipeline
│   │   ├── stages/
│   │   ├── orchestrator.rs
│   │   └── config.rs
│   └── fluent/        # Builder API
│       └── builder.rs
├── python/            # PyO3 bindings
├── bin/               # CLI tools
│   ├── convert.rs
│   ├── inspect.rs
│   ├── extract.rs
│   ├── schema.rs
│   └── search.rs
├── dataset/           # KPS dataset support
├── core/              # Core configuration
└── config.rs          # Type normalization config
```

## Pipeline Architecture

### Standard Pipeline (4-stage)

```
Input File → [Reader] → [Transform] → [Compress] → [Writer] → Output File
```

Stages:
1. **Reader**: Reads and parses input format (bag/MCAP)
2. **Transform**: Applies transformations (topic/type rename)
3. **Compress**: Compresses message data
4. **Writer**: Writes output format

Performance: ~200 MB/s

### HyperPipeline (7-stage)

For maximum throughput, the pipeline is split into 7 stages:

```
Input → [Prefetch] → [Parse] → [Slice] → [Transform] → [Compress] → [Packetize] → [Write] → Output
```

Additional optimizations:
- io_uring for async I/O (Linux)
- Hardware-aware compression
- Lock-free queues between stages
- CPU-aware WindowLog for Zstandard

Performance: ~1800 MB/s

## Data Flow

### Format Conversion Flow

```
┌──────────────┐
│  Input File  │ (bag/MCAP)
└──────┬───────┘
       │
       ↓
┌─────────────────────────────┐
│  Format Detection            │
│  (BagSource trait)           │
└──────┬──────────────────────┘
       │
       ↓
┌─────────────────────────────┐
│  Schema Parsing              │
│  (msg/IDL parsers)           │
└──────┬──────────────────────┘
       │
       ↓
┌─────────────────────────────┐
│  Message Decoding            │
│  (CDR/Protobuf/JSON codecs)  │
└──────┬──────────────────────┘
       │
       ↓
┌─────────────────────────────┐
│  Transform (optional)        │
│  (topic/type rename)         │
└──────┬──────────────────────┘
       │
       ↓
┌─────────────────────────────┐
│  Compression (optional)      │
│  (Zstd/LZ4/Bzip2)            │
└──────┬──────────────────────┘
       │
       ↓
┌─────────────────────────────┐
│  Message Encoding            │
│  (CDR/Protobuf/JSON codecs)  │
└──────┬──────────────────────┘
       │
       ↓
┌──────────────┐
│ Output File  │ (bag/MCAP)
└──────────────┘
```

### Memory Flow

```
┌──────────────────────────────────────────────┐
│  File I/O                                    │
└────────┬─────────────────────────────────────┘
         │
         ↓
┌──────────────────────────────────────────────┐
│  Arena Allocation (zero-copy)                │
│  • Message data in arena                     │
│  • Schema metadata in arena                  │
└────────┬─────────────────────────────────────┘
         │
         ↓
┌──────────────────────────────────────────────┐
│  Buffer Pools                                │
│  • Compression buffers (reused)              │
│  • Arena pools (reused)                      │
└────────┬─────────────────────────────────────┘
         │
         ↓
┌──────────────────────────────────────────────┐
│  File Output                                 │
└──────────────────────────────────────────────┘
```

## Key Design Principles

### 1. Schema-Driven Decoding
- No code generation required
- Runtime schema parsing using Pest PEG parser
- Supports ROS .msg, ROS2 IDL, OMG IDL formats

### 2. Zero-Copy Design
- Arena allocation for message data
- Borrowing instead of copying where possible
- Reduces CPU overhead by ~22%

### 3. Parallel Processing
- Data parallelism with Rayon
- Pipeline parallelism with crossbeam channels
- Lock-free queues in HyperPipeline

### 4. Hardware-Aware Optimization
- CPU feature detection (AVX2, AVX-512)
- Hardware-specific compression presets
- OS-specific optimizations (io_uring on Linux)

### 5. Type Safety
- Rust's type system for memory safety
- Compile-time schema validation
- Strong typing across FFI boundary (PyO3)

## Extension Points

### Adding a New Codec
1. Implement codec trait in `robocodec/src/encoding/`
2. Register in `robocodec/src/core/registry.rs`
3. Add schema parser if needed

### Adding a New Format
1. Implement `BagSource` for reading
2. Implement `BagWriter` for writing
3. Add format detection logic

### Adding a New Transform
1. Implement in `robocodec/src/transform/`
2. Integrate with `TransformBuilder`
3. Add tests for edge cases

### Adding Python Bindings
1. Add `#[pyfunction]` or `#[pymodule]` in `roboflow/src/python/`
2. Rebuild with `maturin develop --features python`
3. Export from `python/roboflow/__init__.py`

## Related Documentation

- `PIPELINE.md` - Detailed pipeline architecture
- `MEMORY.md` - Memory management details
- `ARCHITECTURE.md` - High-level system design
