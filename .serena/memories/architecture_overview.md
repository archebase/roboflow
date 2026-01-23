# Roboflow Architecture Overview

High-level overview of the Roboflow architecture and component organization.

## System Overview

Roboflow is a universal, schema-driven runtime decoding engine for robotics data. It provides high-performance conversion between different robotics message formats (CDR, Protobuf, JSON) and storage formats (MCAP, ROS1 bag).

## Workspace Architecture

### Single-Crate Design

The project is a Cargo workspace with one main crate and an external dependency:

```
roboflow (workspace root)
└── roboflow (main crate)
    └── depends on → robocodec (external: github.com/archebase/robocodec)
```

### Dependency Direction

```
┌─────────────────────────────────────┐
│           roboflow                      │
│  ┌──────────────────────────────┐   │
│  │ • Pipeline orchestration     │   │
│  │ • Fluent API                │   │
│  │ • Python bindings           │   │
│  │ • KPS dataset conversion   │   │
│  │ • CLI tools                 │   │
│  └──────────┬───────────────────┘   │
└─────────────┼───────────────────────┘
              │ depends on (git)
              ↓
┌─────────────────────────────────────┐
│       robocodec (external crate)      │
│  ┌──────────────────────────────┐   │
│  │ • Format readers/writers     │   │
│  │ • Message codecs             │   │
│  │ • Schema parsers             │   │
│  │ • Arena allocation           │   │
│  │ • Core types                 │   │
│  └──────────────────────────────┘   │
└─────────────────────────────────────┘
```

**Key Principle:** `roboflow` depends on external `robocodec` for all I/O operations. This separation allows:
- Reuse of `robocodec` as a standalone library
- Clear separation of concerns (I/O vs. orchestration)
- Independent development and versioning

## Component Organization

### roboflow Crate (Main Layer)

Located at `src/`:

```
src/
├── bin/               # CLI tools
│   ├── convert.rs    # Unified convert CLI
│   ├── extract.rs     # Extract messages from files
│   ├── inspect.rs     # Inspect MCAP/bag files
│   ├── schema.rs      # Display ROS message schemas
│   └── search.rs      # Search topics in files
├── core/              # Core types and registry
├── dataset/           # Dataset conversion
│   └── kps/          # KPS dataset format implementation
│       ├── config.rs
│       ├── delivery_v12.rs
│       ├── task_info.rs
│       ├── hdf5_schema.rs
│       ├── camera_params.rs
│       ├── robot_calibration.rs
│       └── writers/
├── pipeline/          # Pipeline implementations
│   ├── stages/       # Standard pipeline stages
│   ├── hyper/        # 7-stage HyperPipeline
│   └── fluent/       # Builder API
├── python/           # PyO3 bindings
└── config.rs         # Global configuration
```

### robocodec Crate (External)

Located at https://github.com/archebase/robocodec:

```
robocodec/src/
├── encoding/          # Message codec implementations
├── schema/            # Schema parsing (ROS msg, IDL)
├── io/                # Unified I/O layer
│   ├── formats/       # MCAP, ROS bag readers/writers
│   └── kps/           # KPS I/O utilities
├── transform/         # Data transformations
└── types/             # Core memory management (arenas)
```

## Pipeline Architecture

### Standard Pipeline (4-stage)

```
Input File → [Reader] → [Transform] → [Compress] → [Writer] → Output File
```

Performance: ~200 MB/s

### HyperPipeline (7-stage)

```
Input → [Prefetch] → [Parse] → [Slice] → [Transform] → [Compress] → [Packetize] → [Write] → Output
```

Additional optimizations:
- io_uring for async I/O (Linux)
- Hardware-aware compression
- Lock-free queues between stages
- CPU-aware WindowLog for Zstandard

Performance: ~1800 MB/s

### KPS Dataset Pipeline

Located in `src/dataset/kps/`:

- **HDF5 format**: Legacy HDF5-based datasets
- **Parquet format**: Parquet + MP4 video format
- **v1.2 specification**: Latest KPS specification with series delivery structure

## Key Design Principles

### 1. Schema-Driven Decoding
- Runtime schema parsing using Pest (external robocodec)
- No code generation required
- Supports ROS .msg, ROS2 IDL, OMG IDL formats

### 2. Zero-Copy Design
- Arena allocation for message data (external robocodec)
- Borrowing instead of copying where possible
- Reduces CPU overhead by ~22%

### 3. Parallel Processing
- Data parallelism with Rayon
- Pipeline parallelism with crossbeam channels
- Lock-free queues in HyperPipeline

### 4. Hardware-Aware Optimization
- CPU feature detection (AVX2, AVX-512)
- Hardware-specific compression presets
- OS-specific optimizations (io_uring on Linux)

## Extension Points

### Adding Python Bindings
1. Add `#[pyfunction]` or `#[pymethods]` in `src/python/`
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

# Inspect file
cargo run --bin inspect -- data.mcap

# Extract topics
cargo run --bin extract -- data.bag --topics /camera --output out/
```

## Related Documentation

- `CLAUDE.md` - Project overview for Claude Code
- `docs/PIPELINE.md` - Detailed pipeline architecture
- `docs/MEMORY.md` - Memory management details
- `docs/ARCHITECTURE.md` - High-level system design
