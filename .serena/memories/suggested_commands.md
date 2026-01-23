# Suggested Commands for Roboflow Development

Essential commands for developing, testing, and building the Roboflow project.

## Build Commands

### Rust Library
```bash
# Build debug version
cargo build

# Build release version
cargo build --release
```

### Python Package
```bash
# Build Python wheel (development mode - required before running Python tests)
maturin develop --features python

# Build Python wheel for distribution
maturin build --features python
```

## Testing Commands

**IMPORTANT:** Rust and Python tests must be run separately due to PyO3's `extension-module` feature preventing linking in standalone test binaries.

### Rust Tests
```bash
# Run Rust tests only
cargo test

# Run Rust tests with KPS features (requires HDF5 installed)
cargo test --features kps-all

# Run specific Rust test
cargo test test_name

# Run KPS v1.2 specification tests
cargo test --test kps_v12_tests --features kps-all
```

### Python Tests
**IMPORTANT:** Always build the extension first before running Python tests.
```bash
# Build extension first
maturin develop --features python

# Run Python tests
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
cargo fmt
ruff format python/
```

### Lint/Check Code
```bash
# Lint Rust code
cargo clippy --all-targets -- -D warnings

# Lint Python code
ruff check python/
```

### Type Checking
```bash
# Type check Python (requires built extension)
mypy python/roboflow
```

## Running CLI Tools

```bash
# Convert between formats
cargo run --bin convert -- input.bag output.mcap

# Convert to KPS dataset format
cargo run --bin convert -- to-kps input.mcap ./output config.toml

# Inspect file contents
cargo run --bin inspect -- data.mcap

# Extract specific topics
cargo run --bin extract -- data.bag --topics /camera/image_raw --output extracted/

# Work with schemas
cargo run --bin schema -- data.mcap

# Search through data
cargo run --bin search -- pattern
```

## Clean Build Artifacts

```bash
cargo clean
```

## Platform-Specific Notes

- On macOS, `jemalloc` is not used (system allocator is already excellent)
- Linux-specific features (like `io_uring`) are not available on macOS
- Python tests require `maturin develop` before running
