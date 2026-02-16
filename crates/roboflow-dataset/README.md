# roboflow-dataset

[![License: MulanPSL-2.0](https://img.shields.io/badge/License-MulanPSL--2.0-blue.svg)](http://license.coscl.org.cn/MulanPSL2)

Dataset writers and converters for robotics training data formats.

## Architecture

This crate builds on top of `roboflow-video` (low-level video primitives):

```
roboflow-dataset (this crate)
├── LerobotWriter (dataset format)
├── ConcurrentVideoEncoder (multi-camera orchestration)
└── CameraStreamingPipeline (encoding pipelines)

roboflow-video (dependency)
├── RsmpegMp4Encoder (FFmpeg encoding)
├── StreamingMp4Encoder (streaming output)
├── FragmentEncoder (fragment-based encoding)
└── SIMD color conversion (RGB→NV12/YUV420P)
```

**Dependency**: `roboflow-dataset` → `roboflow-video` (one-way)

## Supported Formats

| Format | Description |
|--------|-------------|
| **LeRobot** | HuggingFace LeRobot v2.1 format with MP4 video |
| **KPS** | Key Point Sequence format |

## Features

- **LeRobot v2.1**: Video encoding with H.264/H.265, Parquet metadata
- **Streaming**: Incremental writing with configurable flushing
- **Multi-Camera**: Concurrent encoding for multiple camera streams
- **S3 Upload**: Direct upload to S3 without local temp files

## Usage

### LeRobot Format

```rust
use roboflow_dataset::{
    LerobotConfig, LerobotWriter, DatasetConfig,
    VideoConfig, Mapping,
};

let config = LerobotConfig {
    dataset: DatasetConfig {
        base: DatasetBaseConfig {
            name: "my_dataset".to_string(),
            fps: 30,
            robot_type: Some("ur5".to_string()),
        },
        env_type: None,
    },
    mappings: vec![
        Mapping {
            topic: "/camera/image".to_string(),
            feature: "observation.image".to_string(),
        },
    ],
    video: VideoConfig {
        codec: "h264".to_string(),
        crf: 23,
        ..Default::default()
    },
    ..Default::default()
};

let writer = LerobotWriter::new_local(&output_dir, config)?;
writer.write_frame(&frame).await?;
writer.finalize()?;
```

### Video Encoding

```rust
use roboflow_dataset::ConcurrentVideoEncoder;

let encoder = ConcurrentVideoEncoder::new(config)?;
encoder.encode_frame(&frame).await?;
let video_data = encoder.finalize().await?;
```

## Directory Structure

LeRobot v2.1 output:
```
dataset/
├── meta/
│   ├── info.json
│   └── stats.json
├── data/
│   └── chunk-000/
│       └── episode_000000.parquet
└── videos/
    └── chunk-000/
        └── observation.images.cam/
            └── episode_000000.mp4
```

## License

MulanPSL-2.0
