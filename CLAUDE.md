# CLAUDE.md

Guidance for Claude Code (claude.ai/claude-code) when working with this repository.

## Project Overview

Roboflow: Schema-driven robotics data codec (CDR, Protobuf, JSON) converting between MCAP and ROS1 bag formats. Rust with Python bindings.

**Key:** Single-crate workspace, external `robocodec` for I/O, zero-copy arena allocation.

## Build & Test

```bash
# Build
cargo build
cargo build --release

# Python bindings (requires maturin develop before tests)
maturin develop --features python

# Tests (Rust only - Python uses separate pytest)
cargo test                              # All Rust tests
cargo test --features kps-all          # With KPS features
cargo test --test kps_v12_tests       # KPS v1.2 spec tests
```

## Code Quality

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
ruff format python/ && ruff check python/
```

## Structure

```
roboflow/
├── src/
│   ├── bin/              # CLI: convert, extract, inspect, schema, search
│   ├── core/             # Core types, registry
│   ├── dataset/kps/      # KPS dataset format (HDF5, Parquet, v1.2)
│   ├── pipeline/         # Standard (4-stage), Hyper (7-stage)
│   ├── python/           # PyO3 bindings
│   └── config.rs
├── tests/                # Integration tests, fixtures/
├── examples/
│   ├── python/           # Python examples + KPS package
│   └── rust/             # Rust examples + KPS templates
└── docs/                 # ARCHITECTURE.md, PIPELINE.md, MEMORY.md
```

**External:** `robocodec` (https://github.com/archebase/robocodec) handles all I/O formats and codecs.

## Key Modules

- `src/pipeline/`: Standard pipeline, HyperPipeline, fluent builder API
- `src/dataset/kps/`: KPS dataset conversion with v1.2 spec support
  - `config.rs`: TOML-based topic mapping configuration
  - `delivery_v12.rs`: v1.2 series delivery structure
  - `task_info.rs`: Task metadata JSON generation
  - `hdf5_schema.rs`: HDF5 dataset specifications
  - `writers/`: Streaming HDF5/Parquet writers

## Feature Flags

| Flag | Description |
|------|-------------|
| `python` | PyO3 bindings |
| `kps-all` | All KPS features (HDF5, Parquet, depth) |
| `gpu` | GPU compression (Linux) |
| `jemalloc` | jemalloc allocator (Linux) |
| `cli` / `profiling` | CLI tools |

## Important Notes

- **Testing:** Rust and Python tests must run separately (PyO3 `extension-module` conflicts)
- **Memory:** Use arena allocation for message data (~22% overhead otherwise)
- **KPS v1.2:** Comprehensive spec tests in `tests/kps_v12_tests.rs`

## Common Tasks

**Add Python bindings:** `#[pymethods]` in `src/python/` → `maturin develop`

**Add KPS features:** Implement in `src/dataset/kps/` → add to `Cargo.toml` → test in `tests/kps_v12_tests.rs`
