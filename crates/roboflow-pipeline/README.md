# roboflow-pipeline

[![License: MulanPSL-2.0](https://img.shields.io/badge/License-MulanPSL--2.0-blue.svg)](http://license.coscl.org.cn/MulanPSL2)

High-performance data pipeline stages for streaming compression and transformation.

## Features

- **Hyper Pipeline**: Multi-stage parallel processing
- **Compression Stages**: ZSTD, LZ4, and Snappy support
- **Backpressure**: Automatic flow control between stages
- **Batch Processing**: Configurable batch sizes for throughput

## Usage

### Pipeline Builder

```rust
use roboflow_pipeline::{Pipeline, PipelineBuilder, Stage};

let pipeline = PipelineBuilder::new()
    .add_stage(Stage::decode())
    .add_stage(Stage::transform())
    .add_stage(Stage::encode())
    .batch_size(100)
    .buffer_size(1000)
    .build()?;

pipeline.process(source, sink).await?;
```

### Compression Stage

```rust
use roboflow_pipeline::{CompressionStage, CompressionConfig};

let stage = CompressionStage::new(CompressionConfig {
    algorithm: "zstd".to_string(),
    level: 3,
    ..Default::default()
});
```

## Pipeline Stages

| Stage | Purpose |
|-------|---------|
| `Decode` | Parse raw message data |
| `Transform` | Apply schema transformations |
| `Compress` | Compress data payloads |
| `Encode` | Encode video/image data |
| `Write` | Write to output sink |

## Performance

The pipeline achieves high throughput through:
- **Parallel stages**: Each stage runs in its own task
- **Bounded channels**: Backpressure prevents memory overflow
- **Zero-copy**: Minimize data cloning where possible

Typical throughput: ~1800 MB/s on modern hardware.

## License

MulanPSL-2.0
