# Contributing to Roboflow

Thank you for your interest in contributing to Roboflow! This document provides guidelines and instructions for contributors.

## Code of Conduct

Please be respectful and constructive in all interactions. See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for details.

## Development Setup

### Prerequisites

- Rust 1.92 or later
- Python 3.11+ (for Python bindings)
- maturin (for building Python package)

### Building from Source

1. Fork the repository and clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/roboflow.git
   cd roboflow
   git remote add upstream https://github.com/archebase/roboflow.git
   ```

2. Build the Rust library:
   ```bash
   cargo build --release
   ```

3. Build and install the Python package:
   ```bash
   # Install maturin if not already installed
   pip install maturin

   # Build and install in development mode
   maturin develop --features python

   # Or build a release wheel
   maturin build --release --features python
   ```

4. Run tests to verify your setup:
   ```bash
   # Rust tests
   cargo test

   # Python tests (requires extension built first)
   pytest
   ```

### Project Structure

Roboflow is organized as a Cargo workspace with two crates:

```
roboflow/                    # Workspace root
├── roboflow/                # Main pipeline crate
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
│   └── bin/                  # CLI tools
│
└── robocodec/                # I/O format handling crate
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

**Key separation**: `roboflow` depends on `robocodec`. All I/O operations and format handling go through `robocodec`, while `roboflow` provides pipeline orchestration and processing logic.

### Optional Features

| Feature | Description |
|---------|-------------|
| `python` | Python bindings via PyO3 |
| `dataset-hdf5` | KPS HDF5 dataset support |
| `dataset-parquet` | KPS Parquet dataset support |
| `dataset-depth` | KPS depth video support |
| `dataset-all` | All KPS features |
| `jemalloc` | Use jemalloc allocator (Linux only) |
| `cli` | CLI tools |
| `profiling` | Profiling support |

Enable features when building:
```bash
cargo build --features "python,dataset-all"
maturin develop --features python
```

## Development Workflow

### Creating a Branch

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/your-bug-fix
```

### Making Changes

1. **Follow the existing code style**: The project uses standard Rust formatting
2. **Write tests**: Add tests for new functionality or bug fixes
3. **Update documentation**: Update relevant documentation and comments
4. **Commit messages**: Use clear, descriptive commit messages:
   ```
   feat: add support for XYZ format
   fix: handle edge case in CDR decoder
   docs: update installation instructions
   ```

### Testing

```bash
# Run Rust tests (without Python feature due to PyO3 linking)
cargo test

# Run Rust tests with KPS features (requires HDF5 installed)
cargo test --features dataset-all

# Run Python tests (build extension first)
maturin develop --features python
pytest python/

# Run specific test
cargo test test_name
pytest python/tests/test_file.py
```

### Code Quality

```bash
# Format all code
cargo fmt
ruff format python/

# Lint checks
cargo clippy --all-targets -- -D warnings
ruff check python/

# Type check Python
mypy python/roboflow
```

## Adding Features

### New Codec Support

1. Implement codec in `robocodec/src/encoding/`
2. Register in `robocodec/src/core/registry.rs`
3. Add schema parser if needed
4. Add tests for encode/decode consistency

### New File Format

1. Implement `BagSource` for reading in `robocodec/src/io/formats/`
2. Implement `BagWriter` for writing in `robocodec/src/io/writer/`
3. Add format detection in `robocodec/src/io/detection.rs`
4. Add integration tests

### CLI Tool

1. Add binary to `roboflow/bin/` or `robocodec/bin/`
2. Update `Cargo.toml` with the binary name
3. Add help documentation and examples

### Python Bindings

When adding Rust APIs that should be exposed to Python:

1. Add `#[pyfunction]` or `#[pymethods]` attributes in `roboflow/src/python/`
2. Register in `roboflow/src/python/mod.rs`
3. Add type stubs to `python/roboflow/` if needed
4. Rebuild with `maturin develop --features python`

## Testing Guidelines

- **Unit tests**: Test individual functions and modules
- **Integration tests**: Test end-to-end functionality
- **Round-trip tests**: Verify encode/decode consistency
- **Cross-language tests**: Verify Rust and Python API parity

## Architecture Deep Dives

For detailed architecture information, see:

- [ARCHITECTURE.md](docs/ARCHITECTURE.md) - High-level system design
- [PIPELINE.md](docs/PIPELINE.md) - Pipeline architecture details
- [MEMORY.md](docs/MEMORY.md) - Memory management and arena allocation

## Reporting Bugs

Before creating bug reports, please check existing issues. When creating a bug report, include:

- **Clear title and description**: Summarize the issue
- **Steps to reproduce**: Detailed steps to reproduce the bug
- **Expected behavior**: What you expected to happen
- **Actual behavior**: What actually happened
- **Environment**: OS, Rust version, Python version
- **Logs/error messages**: Any relevant error messages or stack traces
- **Test files**: Sample data files that reproduce the issue (if applicable)

## Submitting Pull Requests

1. Ensure all tests pass and code is formatted
2. Push your branch to your fork
3. Create a pull request to the `main` branch
4. Fill out the pull request template
5. Wait for review and address feedback

## Release Process

Maintainers follow this process for releases:

1. Update version in `Cargo.toml`
2. Update `CHANGELOG.md`
3. Create git tag
4. Publish to crates.io
5. Build and publish Python package to PyPI

## Questions?

Feel free to open an issue for questions or discussion about contributions.
