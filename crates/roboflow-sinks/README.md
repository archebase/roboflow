# roboflow-sinks

[![License: MulanPSL-2.0](https://img.shields.io/badge/License-MulanPSL--2.0-blue.svg)](http://license.coscl.org.cn/MulanPSL2)

Output sink abstraction for writing processed data.

## Supported Formats

| Format | Description |
|--------|-------------|
| **LeRobot** | HuggingFace LeRobot dataset format |
| **Parquet** | Apache Parquet columnar format |

## Features

- **Plugin Registry**: Dynamic sink registration
- **Async Writing**: Non-blocking batch writes
- **Checkpointing**: Resume from last written position
- **Statistics**: Track frames, episodes, and bytes written

## Usage

### Basic Usage

```rust
use roboflow_sinks::{Sink, SinkConfig, create_sink, DatasetFrame};

// Create sink from config
let config = SinkConfig::lerobot("/output/dataset/");
let mut sink = create_sink(&config)?;

// Initialize
sink.initialize(&config).await?;

// Write frames
let frame = DatasetFrame {
    timestamp: 1234567890,
    image_data: Some(image_bytes),
    action: Some(action_data),
    ..Default::default()
};
sink.write_frame(frame).await?;

// Finalize and get stats
let stats = sink.finalize().await?;
println!("Wrote {} frames", stats.frames_written);
```

### S3 Output

```rust
let config = SinkConfig {
    sink_type: SinkType::Lerobot,
    path: "s3://bucket/datasets/my_dataset/".to_string(),
    ..Default::default()
};
```

## Sink Trait

```rust
pub trait Sink: Send + Sync {
    async fn initialize(&mut self, config: &SinkConfig) -> SinkResult<()>;
    async fn write_frame(&mut self, frame: DatasetFrame) -> SinkResult<()>;
    async fn flush(&mut self) -> SinkResult<SinkCheckpoint>;
    async fn finalize(&mut self) -> SinkResult<SinkStats>;
}
```

## Dataset Frame

```rust
pub struct DatasetFrame {
    pub timestamp: i64,
    pub episode_id: u64,
    pub frame_id: u64,
    pub image_data: Option<ImageData>,
    pub action: Option<Vec<f32>>,
    pub observation: Option<HashMap<String, Vec<f32>>>,
}
```

## Sink Statistics

```rust
pub struct SinkStats {
    pub frames_written: u64,
    pub episodes_written: u64,
    pub bytes_written: u64,
    pub duration_secs: f64,
}
```

## License

MulanPSL-2.0
