# roboflow-sources

[![License: MulanPSL-2.0](https://img.shields.io/badge/License-MulanPSL--2.0-blue.svg)](http://license.coscl.org.cn/MulanPSL2)

Input source abstraction for reading robotics data files.

## Supported Formats

| Format | Description |
|--------|-------------|
| **MCAP** | Modern container format for robotics data |
| **ROS Bag** | ROS1 and ROS2 bag files |
| **RRD** | Rerun data format |

## Features

- **Plugin Registry**: Dynamic source registration
- **Async Reading**: Non-blocking batch reads
- **Seekable**: Random access to message timestamps
- **Schema Extraction**: Automatic schema discovery from files

## Usage

### Basic Usage

```rust
use roboflow_sources::{Source, SourceConfig, create_source, register_builtin_sources};

// Register built-in sources
register_builtin_sources();

// Create source from config
let config = SourceConfig::mcap("/path/to/file.mcap");
let mut source = create_source(&config)?;

// Initialize and read
source.initialize(&config).await?;

// Read messages in batches
while let Some(messages) = source.read_batch(100).await? {
    for msg in messages {
        println!("Topic: {}, Timestamp: {}", msg.topic, msg.timestamp);
    }
}
```

### S3 Streaming

```rust
use roboflow_sources::SourceType;

let config = SourceConfig {
    source_type: SourceType::S3Prefix,
    path: "s3://bucket/rosbags/".to_string(),
    ..Default::default()
};
```

## Source Types

| Type | Config Key | Description |
|------|------------|-------------|
| `Mcap` | `"mcap"` | MCAP container files |
| `Bag` | `"bag"` | ROS bag files |
| `Rrd` | `"rrd"` | Rerun data files |
| `S3Prefix` | `"s3_prefix"` | S3 prefix scanning |

## Source Trait

```rust
pub trait Source: Send + Sync {
    async fn initialize(&mut self, config: &SourceConfig) -> SourceResult<()>;
    async fn read_batch(&mut self, batch_size: usize) -> SourceResult<Option<Vec<TimestampedMessage>>>;
    async fn seek(&mut self, timestamp_ns: i64) -> SourceResult<()>;
    async fn metadata(&self) -> SourceResult<SourceMetadata>;
}
```

## Message Format

```rust
pub struct TimestampedMessage {
    pub topic: String,
    pub timestamp: i64,
    pub data: Vec<u8>,
    pub encoding: String,
    pub schema_name: Option<String>,
}
```

## License

MulanPSL-2.0
