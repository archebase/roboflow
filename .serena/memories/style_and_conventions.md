# Code Style and Conventions for Roboflow

This document describes the coding style, conventions, and design patterns used in the Roboflow project.

## Project Structure

### Workspace Layout
Roboflow is organized as a Cargo workspace with two crates:

1. **`robocodec`** (bottom crate) - Low-level format handling library
   - Message codecs (CDR, Protobuf, JSON)
   - Schema parser (ROS .msg, ROS2 IDL, OMG IDL)
   - Core types and error handling
   - Arena allocation primitives
   - MCAP and ROS bag format implementations

2. **`roboflow`** (top crate) - High-level pipeline and conversion tool
   - Pipeline orchestration (Standard, HyperPipeline)
   - Fluent API for batch operations
   - Data transformations
   - Python bindings via PyO3
   - CLI tools (convert, inspect, extract, schema, search)

**Key Principle:** `roboflow` depends on `robocodec`. All I/O operations and format handling go through `robocodec`, while `roboflow` provides pipeline orchestration.

### Directory Structure

```
roboflow/
├── src/
│   ├── pipeline/         # Pipeline implementations
│   │   ├── stages/       # Standard pipeline stages
│   │   ├── hyper/        # 7-stage HyperPipeline
│   │   └── fluent/       # Builder API
│   ├── python/           # PyO3 bindings
│   ├── bin/              # CLI tools
│   └── dataset/          # KPS dataset support
│
└── robocodec/
    └── src/
        ├── encoding/     # CDR, Protobuf, JSON codecs
        ├── schema/       # Schema parsers
        ├── io/           # Unified I/O layer
        ├── transform/    # Topic/type renaming, normalization
        └── types/        # Arena allocation, chunking
```

## Rust Code Style

### Naming Conventions
- **Modules**: `snake_case` (e.g., `mod.rs`, `config.rs`)
- **Types/Structs/Enums**: `PascalCase` (e.g., `NormalizeConfig`, `TopicMapping`)
- **Functions/Methods**: `snake_case` (e.g., `load_from_file`, `write_to`)
- **Constants**: `SCREAMING_SNAKE_CASE` (e.g., `GLOBAL`)
- **Generic Parameters**: Short `T`, `E` etc. or descriptive `TMessage`

### Documentation
- Module-level: Use `//!` for crate/module documentation
- Item-level: Use `///` for public APIs
- Include examples in doc comments for complex APIs
- Document all public structs, fields, and functions

Example:
```rust
//! Configuration file parser for type normalization.
//!
//! Loads type mappings from TOML config files.

/// Topic-aware type mapping.
#[derive(Debug, Deserialize, Clone)]
pub struct TopicTypeMapping {
    /// Topic pattern
    pub topic: String,
}
```

### Error Handling
- Use `thiserror` for defining error types
- Use `anyhow` for application-level error propagation
- Provide context with `.context()` for errors
- Implement `From` conversions for error types

### Dependencies
- **Serialization**: `serde` with `derive` feature
- **Parsing**: `pest` for PEG parsers
- **Parallel Processing**: `rayon` for data parallelism
- **Async/Channels**: `crossbeam-channel` for MPSC communication
- **Compression**: `zstd`, `lz4_flex`, `bzip2`
- **Arena Allocation**: `bumpalo` for zero-copy operations

## Memory Management

### Arena Allocation
The project extensively uses arena allocation for zero-copy performance:
- Message data allocated in arenas (`robocodec/src/types/arena.rs`)
- Arena pooling for reuse (`robocodec/src/types/arena_pool.rs`)
- Buffer pools for compression (`robocodec/src/types/buffer_pool.rs`)

**When to use:** Always use arena allocation for message data to avoid ~22% CPU overhead from individual allocations.

### Zero-Copy Design
- Prefer borrowing over copying when possible
- Use `&[u8]` slices instead of `Vec<u8>` for read-only data
- Leverage arena lifetimes for temporary allocations

## Design Patterns

### Builder Pattern
The Fluent API uses a builder pattern for type-safe, composable operations:

```rust
Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .hyper()
    .compression(CompressionPreset::Balanced)
    .run()?;
```

### Pipeline Architecture
Three pipeline implementations for different use cases:
1. **Standard Pipeline** - 4-stage, ~200 MB/s
2. **HyperPipeline** - 7-stage, ~1800 MB/s  
3. **KPS Pipeline** - Dataset conversion (experimental)

### Codec Layer
Located in `robocodec/src/encoding/`:
- CDR: ROS1/ROS2 serialization with cursor-based encoding
- Protobuf: Protocol Buffers support
- JSON: Human-readable serialization

## Python Code Style

### Module Organization
- Main package: `python/roboflow/`
- Tests: `python/tests/`
- Use `__all__` to explicitly define public API

### Docstrings
- Use Google-style docstrings for Python functions
- Include usage examples in docstrings
- Document all public functions and classes

Example:
```python
"""
Robocodec - High-performance robotics data conversion.

Fluent API for converting between MCAP and ROS bag formats.

Example:
    >>> import roboflow
    >>> result = roboflow.Roboflow.open(["input.bag"]).write_to("output.mcap").run()
"""
```

### Type Hints
- Use type hints for all function signatures
- Python 3.11+ target (checked by mypy)
- Use `mypy` for type checking

## Testing Conventions

### Rust Tests
- Located in `tests/` directory (integration tests)
- Use `pretty_assertions` for better diff output
- Test fixtures in `tests/fixtures/`
- Round-trip tests verify encode/decode consistency

### Python Tests
- Located in `python/tests/`
- Use `pytest` framework
- Always build extension with `maturin develop` before testing
- Test both fluent API and direct function calls

## Feature Flags

### Key Features
- `python` - Python bindings (default: off)
- `kps-hdf5` - HDF5 dataset support
- `kps-parquet` - Parquet dataset support
- `kps-depth` - Depth video support
- `kps-all` - All KPS features
- `jemalloc` - Use jemalloc allocator (Linux only)
- `cli` - CLI tools
- `profiling` - Profiling support

**Important:** Never enable `python` feature for Rust tests (PyO3 linking issues).

## Common Patterns

### Adding a New Codec
1. Implement in `robocodec/src/encoding/`
2. Register in `robocodec/src/core/registry.rs`
3. Add schema parser if needed

### Adding a New File Format
1. Implement reader in `robocodec/src/io/`
2. Implement writer in `robocodec/src/io/writer/`
3. Add format detection

### Adding Python Bindings
1. Add `#[pyfunction]` or `#[pymodule]` in `roboflow/src/python/`
2. Rebuild with `maturin develop --features python`
3. Export from `python/roboflow/__init__.py`

## Performance Considerations

- Use arena allocation for message data
- Prefer parallel processing with `rayon`
- Leverage SIMD operations where possible
- Profile before optimizing (use `--features profiling`)
- Consider HyperPipeline for maximum throughput

## Code Quality Tools

### Rust
- **Formatting**: `cargo fmt` (default rustfmt config)
- **Linting**: `cargo clippy` with `-D warnings`
- **Testing**: `cargo test`

### Python
- **Formatting**: `ruff format python/`
- **Linting**: `ruff check python/`
- **Type Checking**: `mypy python/roboflow`
- **Testing**: `pytest python/`
