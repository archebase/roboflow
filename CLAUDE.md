# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/claude-code) when working with code in this repository.

## Project Overview

Robocodec is a universal, schema-driven runtime decoding engine for robotics data. It provides high-performance conversion between different robotics message formats (CDR, Protobuf, JSON) and storage formats (MCAP, ROS1 bag). The project is written in Rust with Python bindings via PyO3.

**Key characteristics:**
- Workspace with two crates: `robocodec` (pipeline) and `robofmt` (I/O formats)
- Multi-format support: MCAP, ROS bag, CDR, Protobuf, JSON
- Three pipeline implementations: Standard (4-stage), HyperPipeline (7-stage), KPS (experimental)
- Zero-copy design using arena allocation for performance

## Build Commands

```bash
# Build Rust library (debug)
make build
cargo build

# Build Rust library (release)
make build-release
cargo build --release

# Build Python package in development mode (required before running Python tests)
make build-python-dev
maturin develop --features python

# Build Python wheel for release
make build-python
maturin build --release --features python
```

## Testing

**Important:** Rust and Python tests must be run separately because PyO3's `extension-module` feature prevents linking in standalone test binaries.

```bash
# Run all tests (Rust + Python)
make test

# Rust tests only
make test-rust
cargo test

# Rust tests with KPS features (requires HDF5 installed)
make test-rust-kps
cargo test --features kps-all

# Python tests (builds extension first)
make test-python
pytest python/

# Single Python test file
pytest python/tests/test_file.py

# Run specific Rust test
cargo test test_name
```

## Code Quality

```bash
# Format all code (Rust + Python)
make fmt
cargo fmt
ruff format python/

# Lint checks
make check
cargo clippy --all-targets -- -D warnings
ruff check python/

# Type check Python (requires built extension)
make lint-python
mypy python/robocodec
```

## Workspace Structure

The project uses Cargo workspace with two main crates:

```
robocodec/                    # Workspace root
├── robocodec/                # Main pipeline crate
│   ├── src/
│   │   ├── pipeline/         # Pipeline implementations
│   │   │   ├── stages/       # Standard pipeline stages
│   │   │   ├── hyper/        # 7-stage HyperPipeline
│   │   │   ├── kps/          # KPS dataset conversion (experimental)
│   │   │   ├── fluent/       # Builder API
│   │   │   ├── gpu/          # GPU compression support
│   │   │   └── auto_config.rs # Hardware-aware configuration
│   │   ├── python/           # PyO3 bindings
│   │   └── lib.rs
│   └── bin/convert.rs        # Unified convert CLI
│
└── robofmt/                  # I/O format handling crate
    ├── src/
    │   ├── encoding/         # CDR, Protobuf, JSON codecs
    │   ├── schema/           # ROS .msg, ROS2 IDL, OMG IDL parsers
    │   ├── io/               # Unified I/O layer
    │   │   ├── formats/      # MCAP, ROS bag readers/writers
    │   │   └── kps/          # KPS dataset format (experimental)
    │   ├── transform/        # Topic/type renaming, normalization
    │   └── lib.rs
    └── bin/                  # CLI tools: inspect, extract, schema, search
```

**Key separation:** `robocodec` depends on `robofmt`. All I/O operations and format handling go through `robofmt`, while `robocodec` provides the pipeline orchestration and processing logic.

## Architecture

### Pipeline System

The codebase provides three pipeline implementations for different use cases:

1. **Standard Pipeline** (`robocodec/src/pipeline/`): 4-stage (Reader→Transform→Compress→Write), ~200 MB/s
2. **HyperPipeline** (`robocodec/src/pipeline/hyper/`): 7-stage for maximum throughput, ~1800 MB/s
3. **KPS Pipeline** (`robocodec/src/pipeline/kps/`): Converts to robotics learning dataset format (experimental)

### Codec Layer

Located in `robofmt/src/encoding/`:
- **CDR**: ROS1/ROS2 serialization with cursor-based encoding
- **Protobuf**: Protocol Buffers support
- **JSON**: Human-readable serialization

### Schema Parser

Located in `robofmt/src/schema/parser/`:
- Uses Pest parser combinator
- Parses ROS `.msg`, ROS2 IDL, and OMG IDL formats
- Schema-driven runtime decoding (no code generation)

### Fluent API

Type-safe builder pattern in `robocodec/src/pipeline/fluent/`:
```rust
Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .hyper()
    .mode(PerformanceMode::Throughput)
    .run()?;
```

## Memory Management

The codebase uses **arena allocation** extensively for zero-copy operations:
- Message data allocated in arenas (`robofmt/src/types/arena.rs`)
- Arena pooling for reuse (`robofmt/src/types/arena_pool.rs`)
- Buffer pools for compression (`robofmt/src/types/buffer_pool.rs`)
- Chunk-based processing (`robofmt/src/types/chunk.rs`)

**When working with memory code:** Always use arena allocation for message data to avoid the ~22% CPU overhead of individual allocations.

## Feature Flags

Key feature flags in both crates:

| Flag | Description |
|------|-------------|
| `python` | Python bindings via PyO3 |
| `kps-hdf5` | HDF5 dataset support |
| `kps-parquet` | Parquet dataset support |
| `kps-depth` | Depth video support |
| `kps-all` | All KPS features |
| `cli` | CLI tools |
| `jemalloc` | Use jemalloc allocator |

## KPS Integration (Experimental)

The KPS pipeline converts robotics data to KPS dataset format. Key files:
- Configuration: `robofmt/src/io/kps/config.rs`
- Fluent API: `robocodec/src/pipeline/kps/fluent.rs`
- Delivery v1.2: `robofmt/src/io/kps/delivery_v12.rs`
- Writers: `robofmt/src/io/kps/writers/`

## Common Patterns

### Adding a new codec
1. Implement in `robofmt/src/encoding/`
2. Register in `robofmt/src/core/registry.rs`
3. Add schema parser if needed

### Adding a new file format
1. Implement `BagSource` for reading in `robofmt/src/io/formats/`
2. Implement `BagWriter` for writing in `robofmt/src/io/writer/`
3. Add format detection in `robofmt/src/io/detection.rs`

### Adding Python bindings
1. Add `#[pyfunction]` or `#[pymethods]` in `robocodec/src/python/`
2. Rebuild with `maturin develop --features python`

## Testing Notes

- Rust tests: Use `cargo test` (no `--features python` - PyO3 linking conflicts)
- Python tests: Always run `maturin develop` first to build the extension
- Test fixtures: Located in `tests/fixtures/`
- Round-trip tests: Verify encode/decode consistency

## Documentation

- [ARCHITECTURE.md](docs/ARCHITECTURE.md) - High-level system design
- [PIPELINE.md](docs/PIPELINE.md) - Detailed pipeline architecture
- [MEMORY.md](docs/MEMORY.md) - Memory management details
