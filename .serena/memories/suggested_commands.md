# Suggested Commands for Robocodec Development

This file contains essential commands for developing, testing, and building the Robocodec project on Darwin (macOS).

## Build Commands

### Rust Library
```bash
# Build debug version
make build
cargo build

# Build release version
make build-release
cargo build --release
```

### Python Package
```bash
# Build Python wheel (development mode - required before running Python tests)
make build-python-dev
maturin develop --features python

# Build Python wheel for distribution (debug)
make build-python
maturin build

# Build Python wheel for distribution (release)
make build-python-release
maturin build --release --strip
```

## Testing Commands

**IMPORTANT:** Rust and Python tests must be run separately due to PyO3's `extension-module` feature preventing linking in standalone test binaries.

### All Tests
```bash
# Run both Rust and Python tests
make test
```

### Rust Tests
```bash
# Run Rust tests only
make test-rust
cargo test

# Run Rust tests with KPS features (requires HDF5 installed)
make test-all
cargo test --features kps-all

# Run specific Rust test
cargo test test_name
```

### Python Tests
**IMPORTANT:** Always build the extension first before running Python tests.
```bash
# Run Python tests (builds extension first)
make test-python
pytest python/

# Run specific Python test file
pytest python/tests/test_file.py

# Run with verbose output
pytest python/ -v
```

## Code Quality Commands

### Format Code
```bash
# Format all code (Rust + Python)
make fmt
cargo fmt                    # Format Rust
ruff format python/          # Format Python (if ruff is installed)
```

### Lint/Check Code
```bash
# Run format and lint checks
make check

# Lint Rust code
make lint
cargo clippy --all-targets -- -D warnings

# Lint with all features including KPS
make lint-all
cargo clippy --all-targets --all-features -- -D warnings
```

### Python Type Checking
```bash
# Type check Python (requires built extension)
make lint-python
mypy python/roboflow
```

## Coverage Commands

```bash
# Run all coverage reports
make coverage

# Rust coverage with llvm-cov
make coverage-rust
cargo llvm-cov --workspace --html --output-dir target/llvm-cov/html

# Python coverage
make coverage-python
pytest python/ --cov=roboflow --cov-report=term-missing --cov-report=html:coverage-html
```

## Running CLI Tools

```bash
# Convert between formats
cargo run --bin convert -- input.bag output.mcap

# Inspect file contents
cargo run --bin inspect -- data.mcap

# Extract specific topics
cargo run --bin extract -- data.bag --topics /camera/image_raw --output extracted/

# Work with schemas
cargo run --bin schema -- [options]

# Search through data
cargo run --bin search -- pattern
```

## Clean Build Artifacts

```bash
make clean
```

## Darwin-Specific Notes

- On macOS, the default system allocator is already excellent, so `jemalloc` is not used
- Most Linux-specific features (like `io_uring`) are not available on Darwin
- Python tests require virtual environment setup before running
