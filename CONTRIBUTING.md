# Contributing to Roboflow

Thank you for your interest in contributing to Roboflow! This document provides guidelines and instructions for contributors.

## Code of Conduct

Please be respectful and constructive in all interactions. See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for details.

## Development Setup

### Prerequisites

- Rust 1.92 or later

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

3. Run tests to verify your setup:
   ```bash
   cargo test
   ```

### Project Structure

Roboflow is organized as a Cargo workspace with multiple crates:

```
roboflow/                              # Workspace root
├── crates/
│   ├── roboflow-core/                # Core types and error handling
│   ├── roboflow-storage/             # Storage abstraction (S3, OSS, local)
│   ├── roboflow-dataset/             # Dataset writers (KPS, LeRobot)
│   ├── roboflow-distributed/         # TiKV distributed coordination
│   ├── roboflow-hdf5/                # Optional HDF5 support
│   ├── roboflow-pipeline/            # Pipeline implementations
│   └── roboflow/                     # Main crate with CLI tools
│       ├── src/
│       │   ├── pipeline/             # Pipeline implementations
│       │   │   ├── stages/           # Standard pipeline stages
│       │   │   ├── hyper/            # 7-stage HyperPipeline
│       │   │   ├── fluent/           # Builder API
│       │   │   ├── gpu/              # GPU compression support
│       │   │   └── auto_config.rs    # Hardware-aware configuration
│       │   └── bin/                  # CLI tools
│       └── Cargo.toml
```

**Key**: The project uses the external `robocodec` library for I/O operations and format handling.

### Optional Features

| Feature | Description |
|---------|-------------|
| `dataset-hdf5` | KPS HDF5 dataset support |
| `dataset-parquet` | KPS Parquet dataset support |
| `dataset-depth` | KPS depth video support |
| `dataset-all` | All KPS features |
| `cloud-storage` | S3/OSS cloud storage support |
| `jemalloc` | Use jemalloc allocator (Linux only) |
| `cli` | CLI tools |
| `profiling` | Profiling support |

Enable features when building:
```bash
cargo build --features "dataset-all"
cargo test --features dataset-all
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
# Run all tests
cargo test

# Run tests with KPS features (requires HDF5 installed)
cargo test --features dataset-all

# Run specific test
cargo test test_name

# Run clippy to check for issues
cargo clippy --all-targets --all-features -- -D warnings
```

### Code Quality

```bash
# Format all code
cargo fmt

# Lint checks
cargo clippy --all-targets -- -D warnings
```

## Adding Features

### New Codec Support

1. Implement codec in the `robocodec` library
2. Register in the core registry
3. Add schema parser if needed
4. Add tests for encode/decode consistency

### New File Format

1. Implement reader for the format
2. Implement writer for the format
3. Add format detection
4. Add integration tests

### CLI Tool

1. Add binary to `crates/roboflow/src/bin/`
2. Update root `Cargo.toml` with the binary name
3. Add help documentation and examples

## Testing Guidelines

- **Unit tests**: Test individual functions and modules
- **Integration tests**: Test end-to-end functionality
- **Round-trip tests**: Verify encode/decode consistency

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
- **Environment**: OS, Rust version
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

## Questions?

Feel free to open an issue for questions or discussion about contributions.
