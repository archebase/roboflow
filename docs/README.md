# Robocodec Documentation

This directory contains detailed architecture and design documentation for Robocodec.

## Documents

| Document | Description |
|----------|-------------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | High-level system architecture, module organization, and design decisions |
| [PIPELINE.md](PIPELINE.md) | Pipeline architectures including Standard (4-stage) and HyperPipeline (7-stage) |
| [MEMORY.md](MEMORY.md) | Memory management strategies, arena allocation, and zero-copy optimizations |

## Quick Reference

### For Users

- See the main [README.md](../README.md) for installation and usage
- See [benches/README.md](../benches/README.md) for benchmarking guide
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
- [benches/README.md](../benches/README.md) - Benchmarking and profiling

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
use robocodec::pipeline::fluent::{Robocodec, CompressionPreset};

// Standard pipeline
Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .with_compression(CompressionPreset::Balanced)
    .run()?;

// HyperPipeline with auto-configuration
Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .hyper()
    .mode(PerformanceMode::Throughput)
    .run()?;
```

### GPU Compression

Experimental GPU acceleration (feature flags required):
- `gpu-nvcomp`: NVIDIA GPUs via nvCOMP (Linux)
- `gpu-accelerate`: Apple Silicon via libcompression

## Related Resources

### Source Code

- Pipeline: `src/pipeline/`
  - Standard: `src/pipeline/stages/`
  - HyperPipeline: `src/pipeline/hyper/`
  - Fluent API: `src/pipeline/fluent/`
  - Auto-configuration: `src/pipeline/auto_config.rs`
  - GPU: `src/pipeline/gpu/`
- Types: `src/pipeline/types/`
- Format handlers: `src/format/`
- Encoding: `src/encoding/`

### Tools

- Convert: `src/bin/convert.rs` - Unified convert command
- Benchmark: `benches/profiler.rs` - Profiling and benchmarking tool

### Configuration

- Transformation configs: TOML-based topic and type mapping
- LeRobot configs: Dataset conversion settings
- Performance modes: Auto-detected hardware parameters
