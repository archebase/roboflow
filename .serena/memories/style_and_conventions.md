# Code Style and Conventions for Roboflow

Coding style, conventions, and design patterns used in the Roboflow project.

## Project Structure

### Workspace Layout
Roboflow is organized as a single-crate workspace with an external I/O library dependency:

1. **`robocodec`** (external crate) - Low-level format handling library
   - Location: https://github.com/archebase/robocodec
   - Message codecs (CDR, Protobuf, JSON)
   - Schema parser (ROS .msg, ROS2 IDL, OMG IDL)
   - Core types and error handling
   - Arena allocation primitives
   - MCAP and ROS bag format implementations

2. **`roboflow`** (main crate) - High-level pipeline and conversion tool
   - Pipeline orchestration (Standard, HyperPipeline)
   - KPS dataset conversion (`src/dataset/kps/`)
   - Fluent API for batch operations
   - Python bindings via PyO3
   - CLI tools (convert, inspect, extract, schema, search)

**Key Principle:** `roboflow` depends on external `robocodec` for all I/O operations. All format handling, codecs, and schema parsing happen in `robocodec`.

### Directory Structure

```
roboflow/
├── src/
│   ├── bin/              # CLI tools
│   ├── core/             # Core types and registry
│   ├── dataset/          # Dataset conversion
│   │   └── kps/         # KPS dataset format
│   ├── pipeline/         # Pipeline implementations
│   │   ├── stages/       # Standard pipeline stages
│   │   ├── hyper/        # 7-stage HyperPipeline
│   │   └── fluent/       # Builder API
│   ├── python/           # PyO3 bindings
│   └── config.rs         # Global configuration
├── tests/                # Integration tests
│   └── fixtures/         # Test data files
└── examples/            # Example code
    ├── python/           # Python examples
    └── rust/             # Rust examples
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

### Error Handling
- Use `thiserror` for defining error types
- Use `anyhow` for application-level error propagation
- Provide context with `.context()` for errors
- Implement `From` conversions for error types

### Dependencies
- **Serialization**: `serde` with `derive` feature
- **Parsing**: `pest` (external robocodec) for PEG parsers
- **Parallel Processing**: `rayon` for data parallelism
- **Async/Channels**: `crossbeam-channel` for MPSC communication
- **Compression**: `zstd`, `lz4_flex`, `bzip2`
- **Arena Allocation**: `bumpalo` for zero-copy operations

## Memory Management

### Arena Allocation
The project uses arena allocation extensively for zero-copy performance:
- Arena allocation handled by external `robocodec`
- Buffer pools for compression

**When to use:** Always use arena allocation for message data to avoid ~22% CPU overhead from individual allocations.

### Zero-Copy Design
- Prefer borrowing over copying when possible
- Use `&[u8]` slices instead of `Vec<u8>` for read-only data
- Leverage arena lifetimes for temporary allocations

## Design Patterns

### Builder Pattern (Fluent API)

```rust
Roboflow::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .hyper_mode()
    .run()?;
```

### Pipeline Architecture
Three pipeline implementations for different use cases:
1. **Standard Pipeline** - 4-stage, ~200 MB/s
2. **HyperPipeline** - 7-stage, ~1800 MB/s
3. **KPS Dataset** - Dataset conversion (`src/dataset/kps/`)

## Python Code Style

### Module Organization
- Main package: `python/roboflow/`
- Tests: `python/tests/`
- Use `__all__` to explicitly define public API

### Docstrings
- Use Google-style docstrings for Python functions
- Include usage examples in docstrings
- Document all public functions and classes

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
- KPS v1.2 specification tests in `tests/kps_v12_tests.rs`

### Python Tests
- Located in `python/tests/`
- Use `pytest` framework
- Always build extension with `maturin develop` before testing
- Test both fluent API and direct function calls

## Feature Flags

### Key Features
- `python` - Python bindings
- `kps-hdf5` - HDF5 dataset support
- `kps-parquet` - Parquet dataset support
- `kps-depth` - Depth video support
- `kps-all` - All KPS features
- `jemalloc` - Use jemalloc allocator (Linux only)
- `cli` - CLI tools
- `profiling` - Profiling support

**Important:** Never enable `python` feature for Rust tests (PyO3 linking issues).

## Common Patterns

### Adding Python Bindings
1. Add `#[pyfunction]` or `#[pymodule]` in `src/python/`
2. Rebuild with `maturin develop --features python`
3. Export from `python/roboflow/__init__.py`

### Adding KPS Features
1. Implement in `src/dataset/kps/`
2. Add feature flag to `Cargo.toml` if needed
3. Add tests in `tests/kps_v12_tests.rs` for v1.2 compliance

### Running Tools

```bash
# Convert files
cargo run --bin convert -- input.bag output.mcap

# Convert to KPS dataset
cargo run --bin convert -- to-kps input.mcap ./output config.toml

# Inspect file
cargo run --bin inspect -- data.mcap

# Extract topics
cargo run --bin extract -- data.bag --topics /camera --output out/
```

## Performance Considerations

- Use arena allocation for message data (external robocodec handles this)
- Prefer parallel processing with `rayon`
- Leverage SIMD operations where possible
- Profile before optimizing (use `--features profiling`)
- Consider HyperPipeline for maximum throughput
