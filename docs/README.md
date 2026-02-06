# Roboflow Documentation

This directory contains detailed architecture and design documentation for Roboflow.

## Documents

| Document | Description |
|----------|-------------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | High-level system architecture, module organization, and design decisions |
| [PIPELINE.md](PIPELINE.md) | Pipeline architectures including Standard (4-stage) and HyperPipeline (7-stage) |
| [MEMORY.md](MEMORY.md) | Memory management strategies, arena allocation, and zero-copy optimizations |

## Quick Reference

### For Users

- See the main [README.md](../README.md) for installation and usage
- See [CONTRIBUTING.md](../CONTRIBUTING.md) for contribution guidelines

### For Contributors

- Start with [ARCHITECTURE.md](ARCHITECTURE.md) for system overview
- Read [PIPELINE.md](PIPELINE.md) to understand both pipeline implementations:
  - **Standard Pipeline**: 4-stage design (Reader → Transform → Compress → Write)
  - **HyperPipeline**: 7-stage design for maximum throughput
- Review [MEMORY.md](MEMORY.md) for optimization strategies

### For Performance Analysis

- [PIPELINE.md - Performance Characteristics](PIPELINE.md#performance-characteristics)
- [PIPELINE.md - Auto-Configuration](PIPELINE.md#auto-configuration)
- [MEMORY.md - Performance Impact](MEMORY.md#performance-impact)

## Project Structure

Roboflow is a single-crate project that depends on the external `robocodec` library:

```
roboflow/
├── src/                    # Main source code
│   ├── pipeline/           # Pipeline implementations
│   │   ├── stages/         # Standard pipeline stages
│   │   ├── hyper/          # 7-stage HyperPipeline
│   │   ├── fluent/         # Builder API
│   │   ├── auto_config.rs  # Hardware-aware configuration
│   │   └── gpu/            # GPU compression support
│   └── bin/                # CLI tools
└── depends on → robocodec   # External library
                            # https://github.com/archebase/robocodec
```

### Robocodec (External Dependency)

The `robocodec` library provides:

| Component | Description |
|-----------|-------------|
| **Codec Layer** | CDR, Protobuf, JSON encoding/decoding |
| **Schema Parser** | ROS `.msg`, ROS2 IDL, OMG IDL parsing |
| **Format I/O** | MCAP, ROS bag readers/writers |
| **Transform** | Topic/type renaming, normalization |
| **Types** | Arena allocation, zero-copy message types |

## Key Features

### Pipeline Modes

| Feature | Standard Pipeline | HyperPipeline |
|---------|-------------------|---------------|
| Stages | 4 | 7 |
| Throughput | ~200 MB/s | ~1800+ MB/s |
| Complexity | Simple | Advanced |
| Use Case | General purpose | Large-scale conversions |

### Auto-Configuration

Hardware-aware automatic tuning with three performance modes:
- **Throughput**: Maximum throughput on beefy machines
- **Balanced**: Middle ground (default)
- **MemoryEfficient**: Conserve memory

### Fluent API

Type-safe builder API for easy file processing:

```rust
use roboflow::pipeline::fluent::Roboflow;

// Standard pipeline
Roboflow::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .run()?;

// HyperPipeline with auto-configuration
Roboflow::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .hyper_mode()
    .performance_mode(PerformanceMode::Throughput)
    .run()?;
```

## Related Resources

### Source Code

**Roboflow (this repository)**:
- Pipeline: `src/pipeline/`
  - Standard: `src/pipeline/stages/`
  - HyperPipeline: `src/pipeline/hyper/`
  - Fluent API: `src/pipeline/fluent/`
  - Auto-configuration: `src/pipeline/auto_config.rs`
  - GPU: `src/pipeline/gpu/`
- CLI Tools: `src/bin/`

**Robocodec (external library)**:
- Repository: https://github.com/archebase/robocodec
- Encoding: `robocodec/src/encoding/`
- Schema parsing: `robocodec/src/schema/`
- Format I/O: `robocodec/src/io/`
- Arena types: `robocodec/src/types/arena/`

### Tools

| Tool | Location | Purpose |
|------|----------|---------|
| `convert` | `src/bin/convert.rs` | Unified convert command |
| `extract` | `src/bin/extract.rs` | Extract data from files |
| `inspect` | `src/bin/inspect.rs` | Inspect file metadata |
| `schema` | `src/bin/schema.rs` | Work with schema definitions |
| `search` | `src/bin/search.rs` | Search through data files |

### Configuration

- Transformation configs: TOML-based topic and type mapping
- Performance modes: Auto-detected hardware parameters
