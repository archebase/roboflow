# CLAUDE.md

Guidance for Claude Code working on the roboflow repository.

## Project

Roboflow: Distributed data transformation pipeline converting robotics bag/MCAP files to trainingable datasets (LeRobot format).

**Key characteristics:**
- Horizontal scaling for large dataset processing
- Schema-driven message translation (CDR, Protobuf, JSON)
- Zero-copy arena allocation for memory efficiency
- Cloud storage support (OSS, S3) for distributed workloads
- Python bindings via PyO3 (must use `extension-module` mode)

## Workspace Structure

The project uses a Cargo workspace with 6 crates:

| Crate | Purpose |
|-------|---------|
| `roboflow-core` | Error types, registry, values |
| `roboflow-storage` | S3, OSS, Local storage (always available) |
| `roboflow-dataset` | KPS, LeRobot, streaming converters |
| `roboflow-distributed` | TiKV client, catalog, circuit breaker |
| `roboflow-hdf5` | Optional HDF5 format support |
| `roboflow-pipeline` | Hyper pipeline, compression stages |

**Import patterns:**
- Use facade re-exports from `roboflow`: `use roboflow::{Robocodec, DatasetWriter, ...}`
- Or direct crate imports: `use roboflow_core::Result;`

## Build & Test

```bash
cargo build                              # Standard build
cargo test --features distributed       # With distributed coordination
cargo test --test kps_v12_tests         # KPS v1.2 spec tests
```

**Important:** Run Python tests separately via pytest (PyO3 extension-module conflict).

## Code Quality

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
```

## Commit Message Convention

Follow [Conventional Commits](https://www.conventionalcommits.org/) format:

```
<type>: <description>

[optional body]

[optional footer]
```

**Types:**
| Type | Usage |
|------|-------|
| `feat:` | New feature |
| `fix:` | Bug fix |
| `refactor:` | Code restructuring (no functional change) |
| `style:` | Code style changes (formatting, etc.) |
| `docs:` | Documentation only |
| `test:` | Adding or updating tests |
| `chore:` | Maintenance tasks (deps, config, etc.) |
| `perf:` | Performance improvements |

**Examples:**
```
feat: add distributed catalog for TiKV backend
fix: correct frame alignment in streaming converter
refactor: extract storage layer into separate crate
style: apply code formatting fixes
```

## Pull Request Titles

PR titles should follow the same Conventional Commits format as commit messages:

**Do:**
- `feat: add graceful shutdown handling for distributed workers`
- `fix: correct frame alignment in streaming converter`
- `docs: update storage configuration examples`

**Don't:**
- `[Phase 7.2] Add graceful shutdown` ← No internal project tags
- `Adding graceful shutdown` ← Use imperative mood
- `Added graceful shutdown` ← Use imperative mood

PR titles are public-facing and should be clean, descriptive, and free of internal project management artifacts (phase numbers, sprint tags, etc.).

## Git Workflow

1. Create feature branch from `main`
2. Make commits following the convention above
3. Push to remote
4. Create PR with clear description and test checklist
5. Ensure CI passes (`make lint && cargo test`)

## Feature Flags

| Flag | Purpose |
|------|---------|
| `distributed` | TiKV distributed coordination |
| `python` | PyO3 bindings |
| `gpu` | GPU compression (Linux only) |
| `jemalloc` | jemalloc allocator (Linux only) |

**Note:** Storage (S3/OSS) and dataset formats (Parquet, LeRobot) are always available.

## Key Conventions

### Storage Layer
- `Storage` trait uses `&Path` (not `impl AsRef<Path>`) for dyn-compatibility
- `LocalStorage` implements `SeekableStorage` for seekable reads
- `StorageFactory` creates backends from URL schemes (file://, s3://, oss://)
- Environment variables for OSS: `OSS_ACCESS_KEY_ID`, `OSS_ACCESS_KEY_SECRET`, `OSS_ENDPOINT`

### KPS Dataset
- TOML config at `crates/roboflow-dataset/src/kps/config.rs` for topic mappings
- v1.2 spec tests in `tests/kps_v12_tests.rs` are authoritative
- Writers use streaming patterns

### Memory
- **Always use arena allocation** for message data (~22% overhead if skipped)
- Arena types are in `robocodec`, imported via `use robocodec::arena::Arena`

### Python Bindings
- Use `#[pymethods]` on structs in `src/python/`
- Must rebuild with `maturin develop` after changes
- Cannot run Rust and Python tests in same invocation

## External Dependencies

- `robocodec`: https://github.com/archebase/robocodec (I/O, codecs, arena)
