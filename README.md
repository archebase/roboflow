# Roboflow

[![License: MulanPSL-2.0](https://img.shields.io/badge/License-MulanPSL--2.0-blue.svg)](http://license.coscl.org.cn/MulanPSL2)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)
[![codecov](https://codecov.io/gh/archebase/roboflow/branch/main/graph/badge.svg)](https://codecov.io/gh/archebase/roboflow)

[English](README.md) | [简体中文](README_zh.md)

**Roboflow** is a universal, schema-driven runtime decoding engine for robotics data. Convert between ROS bag files, MCAP, and different message formats with a simple fluent API.

## Quick Start

### Rust

```rust
use roboflow::Robocodec;

// Convert ROS bag to MCAP
Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .run()?;
```

### Python

```python
import roboflow

# Convert ROS bag to MCAP
result = roboflow.Roboflow.open(["input.bag"]) \
    .write_to("output.mcap") \
    .run()
```

### Command Line

```bash
# Convert between formats
convert input.bag output.mcap

# Inspect file contents
inspect data.mcap

# Extract specific topics
extract data.bag --topics /camera/image_raw --output extracted/
```

## Installation

> **Note**: PyPI and crates.io packages are being prepared. For now, build from source (see [CONTRIBUTING.md](CONTRIBUTING.md)).

### Rust Dependency

Add to `Cargo.toml`:

```toml
[dependencies]
roboflow = "0.1"
```

For the format library only:

```toml
[dependencies]
robocodec = "0.1"
```

### Python Package

```bash
pip install roboflow
```

## Features

- **Multi-Format Support**: CDR (ROS1/ROS2), Protobuf, JSON
- **File Format Support**: MCAP, ROS1 bag (read/write)
- **Schema Parsing**: ROS `.msg`, ROS2 IDL, OMG IDL
- **Cross-Language**: Rust and Python APIs with full feature parity
- **High-Performance**: Parallel processing pipelines up to ~1800 MB/s
- **Data Transformation**: Topic renaming, type normalization, format conversion

## Supported Formats

| Format | Read | Write | Notes |
|--------|------|-------|-------|
| MCAP | ✅ | ✅ | Robotics data format optimized for appending |
| ROS1 Bag | ✅ | ✅ | ROS1 rosbag format |
| CDR | ✅ | ✅ | Common Data Representation (ROS1/ROS2) |
| Protobuf | ✅ | ✅ | Protocol Buffers |
| JSON | ✅ | ✅ | JSON serialization |

## Schema Support

- ROS `.msg` files (ROS1)
- ROS2 IDL (Interface Definition Language)
- OMG IDL (Object Management Group)

## Usage Examples

### Batch Processing

```rust
use roboflow::{Robocodec, pipeline::fluent::CompressionPreset};

// Process multiple files
Robocodec::open(vec!["a.bag", "b.bag"])?
    .write_to("/output/dir")
    .with_compression(CompressionPreset::Fast)
    .run()?;
```

### With Transformations

```rust
use robocodec::TransformBuilder;

let transform = TransformBuilder::new()
    .with_topic_rename("/old_topic", "/new_topic")
    .build();

Robocodec::open(vec!["input.bag"])?
    .transform(transform)
    .write_to("output.mcap")
    .run()?;
```

### Hyper Mode (Maximum Throughput)

```rust
Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .hyper_mode()
    .run()?;
```

### KPS Dataset Conversion (Experimental)

> **⚠️ Experimental**: APIs may change between versions.

Convert robotics data to KPS dataset format:

```bash
roboflow convert to-kps input.mcap ./output/ config.toml
```

## Command Line Tools

| Tool | Description |
|------|-------------|
| `convert` | Convert between bag/MCAP formats |
| `extract` | Extract data from files |
| `inspect` | Inspect file metadata |
| `schema` | Work with schema definitions |
| `search` | Search through data files |

## Documentation

- [Architecture](docs/ARCHITECTURE.md) - High-level system design
- [Pipeline](docs/PIPELINE.md) - Async pipeline architecture
- [Memory Management](docs/MEMORY.md) - Zero-copy and arena allocation
- [Contributing](CONTRIBUTING.md) - Development setup and guidelines

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.

## License

This project is licensed under the MulanPSL v2 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

Robocodec was originally developed as part of the [Strata](https://github.com/archebase/strata) robotics platform.

## Related Projects

- [MCAP](https://mcap.dev/) - Common data format optimized for appending in robotics community
- [ROS](https://www.ros.org/) - Robot Operating System

## Links

- [Issue Tracker](https://github.com/archebase/roboflow/issues)
- [Changelog](CHANGELOG.md)
