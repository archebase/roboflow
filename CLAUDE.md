# CLAUDE.md

Guidance for Claude Code working on the roboflow repository.

## Project

Roboflow: Schema-driven robotics data codec (CDR, Protobuf, JSON) converting between MCAP and ROS1 bag formats.

**Key characteristics:**
- Single-crate workspace
- External `robocodec` crate handles all I/O formats and codecs
- Zero-copy arena allocation critical for performance
- Python bindings via PyO3 (must use `extension-module` mode)

## Build & Test

```bash
cargo build                              # Standard build
cargo test --features cloud-storage    # With storage layer
cargo test --test kps_v12_tests         # KPS v1.2 spec tests
```

**Important:** Run Python tests separately via pytest (PyO3 extension-module conflict).

## Code Quality

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
```

## Feature Flags

| Flag | Purpose |
|------|---------|
| `python` | PyO3 bindings |
| `dataset-all` | All KPS features (HDF5, Parquet, depth) |
| `cloud-storage` | Storage abstraction (OSS/S3, object_store) |
| `gpu` | GPU compression (Linux only) |
| `jemalloc` | jemalloc allocator (Linux only) |

## Key Conventions

### Storage Layer (`cloud-storage` feature)
- `Storage` trait uses `&Path` (not `impl AsRef<Path>`) for dyn-compatibility
- `LocalStorage` implements `SeekableStorage` for seekable reads
- `StorageFactory` creates backends from URL schemes (file://, s3://, oss://)
- Environment variables for OSS: `OSS_ACCESS_KEY_ID`, `OSS_ACCESS_KEY_SECRET`, `OSS_ENDPOINT`

### KPS Dataset
- TOML config at `src/dataset/kps/config.rs` for topic mappings
- v1.2 spec tests in `tests/kps_v12_tests.rs` are authoritative
- Writers in `src/dataset/kps/writers/` use streaming patterns

### Memory
- **Always use arena allocation** for message data (~22% overhead if skipped)
- Arena types are in `robocodec`, imported via `use robocodec::arena::Arena`

### Python Bindings
- Use `#[pymethods]` on structs in `src/python/`
- Must rebuild with `maturin develop` after changes
- Cannot run Rust and Python tests in same invocation

## External Dependencies

- `robocodec`: https://github.com/archebase/robocodec (I/O, codecs, arena)
