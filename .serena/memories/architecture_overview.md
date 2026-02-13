# Roboflow Architecture Overview

High-level overview of the Roboflow architecture and component organization.

## System Overview

Roboflow is a distributed data transformation pipeline that converts robotics bag/MCAP files to trainable datasets (LeRobot format). Key features:
- Horizontal scaling for large dataset processing
- Schema-driven message translation (CDR, Protobuf, JSON)
- Zero-copy arena allocation for memory efficiency
- Cloud storage support (OSS, S3) for distributed workloads

## Workspace Architecture

### Multi-Crate Design (8 Crates)

```
roboflow (workspace root)
├── roboflow/                    # Main crate - public API facade
└── crates/
    ├── roboflow-core/           # Error types, registry, values
    ├── roboflow-storage/        # S3, OSS, Local storage
    ├── roboflow-dataset/        # KPS, LeRobot, streaming converters
    ├── roboflow-distributed/    # TiKV client, catalog, circuit breaker
    ├── roboflow-sources/        # Data source implementations
    ├── roboflow-sinks/          # Data sink implementations
    ├── roboflow-video/          # Video encoding/decoding
    └── roflow-dataset/          # (legacy/deprecated)
```

### Dependency Graph

```
┌─────────────────────────────────────────────────────────────┐
│                      roboflow (facade)                      │
│  Public API re-exports from sub-crates                      │
└─────────────────────────────────────────────────────────────┘
                              │
       ┌──────────────────────┼──────────────────────┐
       ▼                      ▼                      ▼
┌─────────────┐    ┌──────────────────┐    ┌────────────────┐
│roboflow-core│    │roboflow-dataset  │    │roboflow-distributed│
│• Error types│    │• PipelineExecutor│    │• TiKV client      │
│• Registry   │    │• KPS converters  │    │• Catalog          │
│• Values     │    │• LeRobot format  │    │• Circuit breaker  │
└─────────────┘    └──────────────────┘    └──────────────────┘
       │                      │                      │
       └──────────────────────┼──────────────────────┘
                              ▼
                    ┌──────────────────┐
                    │  robocodec (git) │
                    │ External crate:  │
                    │ github.com/      │
                    │ archebase/       │
                    │ robocodec        │
                    │ • Format I/O     │
                    │ • Codecs         │
                    │ • Arena alloc    │
                    └──────────────────┘
```

**Key Principle:** `roboflow` depends on external `robocodec` for all I/O operations. This separation allows:
- Reuse of `robocodec` as a standalone library
- Clear separation of concerns (I/O vs. orchestration)
- Independent development and versioning

## Component Organization

### Main Crate (src/)

```
src/
├── bin/                    # CLI tools
│   └── roboflow.rs        # Main CLI entry point
├── core/                   # Core types
├── catalog/                # Catalog management
├── config.rs              # Global configuration
├── pipeline_config.rs     # Pipeline configuration structs
├── convert.rs             # Conversion utilities
└── lib.rs                 # Crate root with re-exports
```

### Key Sub-Crates

#### roboflow-dataset (`crates/roboflow-dataset/src/`)

```
├── pipeline.rs            # PipelineExecutor, PipelineStats
├── zarr.rs                # Zarr format support
├── common/
│   ├── camera_pipeline.rs          # Camera streaming
│   ├── camera_streaming_pipeline.rs
│   ├── concurrent_video_encoder.rs
│   └── streaming_uploader.rs
├── hardware/              # Hardware-aware optimization
│   ├── strategy.rs
│   └── mod.rs
└── image/                 # Image processing
    ├── config.rs
    ├── backend.rs
    └── factory.rs
```

#### roboflow-distributed (`crates/roboflow-distributed/src/`)

```
├── worker/
│   ├── mod.rs
│   └── executor.rs        # Distributed worker execution
└── (TiKV client, catalog, circuit breaker)
```

#### roboflow-storage (`crates/roboflow-storage/src/`)

- S3 storage backend (AWS SDK)
- OSS storage backend (Alibaba Cloud)
- Local filesystem storage
- `StorageFactory` for URL-based backend selection

## Pipeline Architecture

### Dataset Pipeline (roboflow-dataset)

Located in `crates/roboflow-dataset/src/pipeline.rs`:

- **PipelineExecutor**: Main pipeline execution engine
- **PipelineStats**: Performance metrics collection
- **PipelineConfig**: Configuration management

### Camera Streaming Pipeline

Located in `crates/roboflow-dataset/src/common/`:

- **camera_pipeline.rs**: Individual camera processing
- **camera_streaming_pipeline.rs**: Multi-camera streaming
- **concurrent_video_encoder.rs**: Parallel video encoding
- **fragment_uploader.rs**: Chunked upload to cloud storage

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
- Concurrent video encoding

### 4. Hardware-Aware Optimization
- CPU feature detection (AVX2, AVX-512)
- Hardware-specific compression presets
- OS-specific optimizations (io_uring on Linux)

## Distributed Features (roboflow-distributed)

- **TiKV Integration**: Distributed KV storage for coordination
- **Catalog**: Dataset metadata management
- **Circuit Breaker**: Fault tolerance for distributed operations
- **Worker Executor**: Distributed task execution

## Infrastructure (Docker Compose)

| Service | Purpose | Ports |
|---------|---------|-------|
| MinIO | S3-compatible storage | 9000 (API), 9001 (Console) |
| TiKV | Distributed KV storage | 20160 |
| PD | TiKV placement driver | 2379, 2380 |

Pre-created buckets: `roboflow-datasets`, `roboflow-raw`, `roboflow-temp`

## Related Documentation

- `CLAUDE.md` - Project overview for Claude Code
- `memory://pipeline_system` - Pipeline system details
- `memory://style_and_conventions` - Coding conventions
