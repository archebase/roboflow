# Robocodec

[![Crates.io](https://img.shields.io/crates/v/robocodec)](https://crates.io/crates/robocodec)
[![PyPI](https://img.shields.io/pypi/v/robocodec)](https://pypi.org/project/robocodec/)
[![License: MulanPSL-2.0](https://img.shields.io/badge/License-MulanPSL--2.0-blue.svg)](http://mulan.cosine.org.cn/license/MulanPSL2)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)

[English](README.md) | [简体中文](README_zh.md)

**Robocodec** is a universal, schema-driven runtime decoding engine for robotics data. It provides a unified interface for decoding, encoding, and converting between different robotics message formats and data storage formats.

## Workspace Structure

Robocodec is organized as a Cargo workspace with two crates:

- **`robofmt`** - Low-level robotics data format library
  - Format-specific readers and writers (MCAP, ROS1 bag)
  - Message codecs (CDR, Protobuf, JSON)
  - Schema parser (ROS .msg, ROS2 IDL, OMG IDL)
  - Core types and error handling

- **`robocodec`** - High-level pipeline and conversion tool
  - Parallel processing pipelines (Standard, HyperPipeline)
  - Fluent API for batch operations
  - Data transformations (topic renaming, type normalization)
  - Python bindings via PyO3
  - KPS dataset format support (experimental)

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

- Rust 1.70 or later
- Python 3.11+ (for Python bindings)
- maturin (for building Python package)

### Building from Source

1. Clone the repository:

```bash
git clone https://github.com/archebase/robocodec.git
cd robocodec
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
robocodec = "0.1"
```

For the format library only:

```toml
[dependencies]
robofmt = "0.1"
```

Enable optional features as needed:

```toml
robocodec = { version = "0.1", features = ["python", "kps-all"] }
```

### Using Python Package

After building with `maturin develop`, you can use the Python package:

```python
from robocodec import Reader, Writer, decode, encode
```

## Quick Start

### Rust API

```rust
use robocodec::RoboReader;

// Open a robotics data file (auto-detects format)
let reader = RoboReader::open("data.bag")?;

// Iterate through messages
for result in reader.iter_messages() {
    let (topic, message) = result?;
    println!("Topic: {}, Data: {}", topic, message);
}
```

### Python API

```python
from robocodec import RoboReader

# Open a robotics data file (auto-detects format)
reader = RoboReader("data.bag")

# Iterate through messages
for topic, message in reader:
    print(f"Topic: {topic}, Data: {message}")
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
use robocodec::Robocodec;

// Simple conversion with auto-detection
Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .run()?;

// HyperPipeline with custom compression
Robocodec::open(vec!["input.bag"])?
    .write_to("output.mcap")
    .hyper()
    .compression(CompressionPreset::Balanced)
    .run()?;
```

### KPS Dataset Conversion (Experimental)

> **⚠️ Experimental**: The KPS conversion API is experimental and may change between versions.

Convert robotics data to KPS dataset format with v1.2 specification support:

```rust
use robocodec::io::formats::kps::{
    Hdf5KpsWriter, KpsConfig, V12DeliveryBuilder
};

// Create KPS writer with configuration
let config = KpsConfig::from_file("config.toml")?;
let writer = Hdf5KpsWriter::new("output_dir", config)?;

// Or use the v1.2 delivery builder
let delivery = V12DeliveryBuilder::new()
    .robot("Kuavo4Pro")
    .end_effector("Dexhand")
    .scene("Housekeeper")
    .task("Dispose_of_takeout_containers")
    .build()?;
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
| MCAP | ✅ | ✅ | Mission Data Capture format |
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
from robocodec import RoboReader, RoboWriter, decode, encode

# Read from file
reader = RoboReader("data.mcap")

# Write to file
writer = RoboWriter("output.bag")

# Decode binary messages
data = decode(b"<binary data>", schema)

# Encode to binary
binary = encode(data, schema)
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

- [MCAP](https://mcap.dev/) - Mission Data Capture format
- [Kupas](https://github.com/unitaryrobotics/mit/) - Dataset format for robotics learning
- [ROS](https://www.ros.org/) - Robot Operating System

## Documentation

- [Architecture](docs/ARCHITECTURE.md) - High-level system design
- [Pipeline](docs/PIPELINE.md) - Async pipeline architecture
- [Memory Management](docs/MEMORY.md) - Zero-copy and arena allocation

## Links

- [Issue Tracker](https://github.com/archebase/robocodec/issues)
- [Changelog](CHANGELOG.md)
