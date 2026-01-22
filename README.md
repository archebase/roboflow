# Roboflow

[![License: MulanPSL-2.0](https://img.shields.io/badge/License-MulanPSL--2.0-blue.svg)](http://license.coscl.org.cn/MulanPSL2)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)

[English](README.md) | [简体中文](README_zh.md)

**Roboflow** is a universal, schema-driven runtime decoding engine for robotics data. It provides a unified interface for decoding, encoding, and converting between different robotics message formats and data storage formats.

## Workspace Structure

Roboflow is organized as a Cargo workspace with two crates:

- **`robocodec`** - Low-level robotics data format library
  - Message codecs (CDR, Protobuf, JSON)
  - Schema parser (ROS .msg, ROS2 IDL, OMG IDL)
  - Core types and error handling
  - Arena allocation primitives

- **`roboflow`** - High-level pipeline and conversion tool
  - Format-specific readers and writers (MCAP, ROS1 bag)
  - Parallel processing pipelines (Standard, HyperPipeline)
  - Fluent API for batch operations
  - Data transformations (topic renaming, type normalization)
  - Python bindings via PyO3
  - KPS dataset format writer (experimental)

## Features

- **Multi-Format Support**: Decode and encode CDR (ROS1/ROS2), Protobuf, and JSON messages
- **File Format Support**: Read and write MCAP and ROS1 bag files
- **Schema Parsing**: Parse ROS `.msg` files, ROS2 IDL, and OMG IDL formats
- **Cross-Language**: Rust and Python APIs with full feature parity
- **High-Performance Pipeline**: Parallel processing with Standard and HyperPipeline modes
- **Data Transformation**: Built-in tools for format conversion, topic renaming, and type normalization
- **KPS Integration**: Convert robotics datasets to KPS format for robotics learning (experimental)

## Installation

> **⚠️ Experimental Feature**: The KPS integration is currently experimental and under active development. APIs may change between versions.

> **Note**: PyPI and Crate packages are currently being prepared and will be available soon. For now, please clone the project and run a local build.

### Prerequisites

- Rust 1.92 or later
- Python 3.11+ (for Python bindings)
- maturin (for building Python package)

### Building from Source

1. Clone the repository:

```bash
git clone https://github.com/archebase/roboflow.git
cd roboflow
```

2. Build Rust library:

```bash
cargo build --release
```

3. Build Python package:

```bash
# Install maturin if not already installed
pip install maturin

# Build and install Python package
maturin develop
# Or for production build:
maturin build
```

### Using as Rust Dependency

To use `robocodec` in your Rust project, add the following to your `Cargo.toml`:

```toml
[dependencies]
roboflow = "0.1"
```

For the format library only:

```toml
[dependencies]
robocodec = "0.1"
```

Enable optional features as needed:

```toml
roboflow = { version = "0.1", features = ["python", "kps-all"] }
```

### Using Python Package

After building with `maturin develop`, you can use the Python package:

```python
import roboflow
```

## Quick Start

### Rust API

```rust
use roboflow::Robocodec;

// Convert between formats (single file)
Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .run()?;

// Batch processing multiple files
Robocodec::open(vec!["a.bag", "b.bag"])?
    .write_to("/output/dir")
    .run()?;
```

### Python API

```python
import roboflow

# Convert between formats (single file)
result = roboflow.Roboflow.open(["input.bag"]) \
    .write_to("output.mcap") \
    .run()

# With compression preset
result = roboflow.Roboflow.open(["input.bag"]) \
    .write_to("output.mcap") \
    .with_compression(roboflow.CompressionPreset.Fast) \
    .run()
```

### Command Line Tools

Convert between formats:

```bash
# Convert ROS bag to MCAP
convert input.bag output.mcap

# Inspect file contents
inspect data.mcap

# Extract specific topics
extract data.bag --topics /camera/image_raw --output extracted/
```

### Fluent API for Batch Processing

```rust
use roboflow::{Robocodec, pipeline::fluent::CompressionPreset};

// Simple conversion with auto-detection
Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .run()?;

// HyperPipeline with custom compression
Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .hyper_mode()
    .with_compression(CompressionPreset::Balanced)
    .run()?;

// Batch processing with compression
Robocodec::open(vec!["a.bag", "b.bag"])?
    .write_to("/output/dir")
    .with_compression(CompressionPreset::Fast)
    .run()?;

// With transformations
use robocodec::TransformBuilder;

let transform = TransformBuilder::new()
    .with_topic_rename("/old_topic", "/new_topic")
    .build();

Robocodec::open(vec!["input.bag"])?
    .transform(transform)
    .write_to("output.mcap")
    .run()?;
```

### KPS Dataset Conversion (Experimental)

> **⚠️ Experimental**: The KPS conversion API is experimental and may change between versions.

Convert robotics data to KPS dataset format for robotics learning:

```rust
use roboflow::dataset::kps::{KpsConfig, ParquetKpsWriter};

// Load configuration from TOML file
let config = KpsConfig::from_file("kps_config.toml")?;

// Create and run KPS writer
let writer = ParquetKpsWriter::new("output_dir", config)?;
writer.process("input.mcap")?;
```

Or use the CLI:

```bash
# Convert MCAP to KPS format
roboflow convert to-kps input.mcap ./output/ config.toml
```

KPS configuration format (TOML):

```toml
[dataset]
name = "my_dataset"
fps = 30
robot_type = "Kuavo4Pro"

[[mappings]]
topic = "/camera/high"
feature = "observation.camera_0"
type = "image"

[[mappings]]
topic = "/joint_states"
feature = "observation.joint_position"
type = "state"

[output]
formats = ["parquet"]
image_format = "mp4"
```

## Supported Formats

| Format | Read | Write | Notes |
|--------|------|-------|-------|
| MCAP | ✅ | ✅ | Common data format optimized for appending |
| ROS1 Bag | ✅ | ✅ | ROS1 rosbag format |
| CDR | ✅ | ✅ | Common Data Representation (ROS1/ROS2) |
| Protobuf | ✅ | ✅ | Protocol Buffers |
| JSON | ✅ | ✅ | JSON serialization |

## Schema Support

- ROS `.msg` files (ROS1)
- ROS2 IDL (Interface Definition Language)
- OMG IDL (Object Management Group)

## Python Bindings

Python bindings provide full access to the Rust core:

```python
import roboflow

# Fluent API for conversions
result = roboflow.Roboflow.open(["input.bag"]) \
    .write_to("output.mcap") \
    .run()

# With transformations
builder = roboflow.TransformBuilder()
transform_id = builder.with_topic_rename("/old", "/new").build()

result = roboflow.Roboflow.open(["input.bag"]) \
    .transform(transform_id) \
    .write_to("output.mcap") \
    .run()

# Hyper mode for maximum throughput
result = roboflow.Roboflow.open(["input.bag"]) \
    .write_to("output.mcap") \
    .hyper_mode() \
    .run()
```

## Optional Features

| Feature | Description |
|---------|-------------|
| `python` | Python bindings via PyO3 |
| `kps-hdf5` | KPS HDF5 dataset support |
| `kps-parquet` | KPS Parquet dataset support |
| `kps-depth` | KPS depth video support |
| `kps-all` | All KPS features |
| `jemalloc` | Use jemalloc allocator (Linux only) |
| `cli` | CLI tools |
| `profiling` | Profiling support |

> **Note**: KPS features (`kps-*`) are experimental and APIs may change.

## Command Line Tools

| Tool | Description |
|------|-------------|
| `convert` | Convert between bag/MCAP formats |
| `extract` | Extract data from files |
| `inspect` | Inspect file metadata |
| `schema` | Work with schema definitions |
| `search` | Search through data files |

## Development

### Building

```bash
# Build Rust library
cargo build --release

# Build Python package
maturin develop

# Run tests
cargo test

# Run Python tests
pytest
```

### Running Examples

```bash
cargo run --bin convert -- input.bag output.mcap
cargo run --bin inspect -- data.mcap
```

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the MulanPSL v2 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

Robocodec was originally developed as part of the [Strata](https://github.com/archebase/strata) robotics platform.

## Related Projects

- [MCAP](https://mcap.dev/) - Common data format optimized for appending in robotics community
- [ROS](https://www.ros.org/) - Robot Operating System

## Documentation

- [Architecture](docs/ARCHITECTURE.md) - High-level system design
- [Pipeline](docs/PIPELINE.md) - Async pipeline architecture
- [Memory Management](docs/MEMORY.md) - Zero-copy and arena allocation

## Links

- [Issue Tracker](https://github.com/archebase/roboflow/issues)
- [Changelog](CHANGELOG.md)
