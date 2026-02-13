# roboflow-video

[![License: MulanPSL-2.0](https://img.shields.io/badge/License-MulanPSL--2.0-blue.svg)](http://license.coscl.org.cn/MulanPSL2)

Video encoding and processing for robotics datasets.

## Features

- **FFmpeg Integration**: H.264/H.265 encoding via rsmpeg
- **Concurrent Encoding**: Multi-camera parallel processing
- **Fragment-Based**: Memory-efficient fragment encoding
- **Hardware Acceleration**: GPU encoding support (optional)

## Usage

### Video Frame

```rust
use roboflow_video::{VideoFrame, VideoFrameConfig};

let frame = VideoFrame::new(
    width, height,
    &rgb_data,
    timestamp_ns,
    VideoFrameConfig::default()
)?;
```

### Fragment Encoder

```rust
use roboflow_video::{FragmentEncoder, FragmentEncoderConfig};

let encoder = FragmentEncoder::new(FragmentEncoderConfig {
    width: 640,
    height: 480,
    fps: 30,
    codec: "h264".to_string(),
    crf: 23,
    ..Default::default()
})?;

// Encode frames
for frame in frames {
    encoder.encode_frame(&frame)?;
}

// Finalize and get MP4 data
let mp4_data = encoder.finalize()?;
```

### Concurrent Encoder

```rust
use roboflow_video::{ConcurrentVideoEncoder, ConcurrentEncoderConfig};

let encoder = ConcurrentVideoEncoder::new(ConcurrentEncoderConfig {
    cameras: vec!["cam_left", "cam_right"],
    video_config: VideoEncoderConfig {
        codec: "h264".to_string(),
        crf: 23,
        ..Default::default()
    },
    ..Default::default()
})?;

// Encode frames for different cameras
encoder.encode_frame("cam_left", &frame1).await?;
encoder.encode_frame("cam_right", &frame2).await?;

// Get encoded videos
let videos = encoder.finalize().await?;
```

## Configuration

```rust
pub struct VideoEncoderConfig {
    pub codec: String,        // "h264" or "h265"
    pub crf: u8,              // 0-51, lower = better quality
    pub preset: String,       // "fast", "medium", "slow"
    pub pixel_format: String, // "yuv420p"
    pub gop_size: u32,        // Keyframe interval
}
```

## Feature Flags

| Flag | Description |
|------|-------------|
| `gpu` | NVIDIA GPU encoding (Linux only) |

## Memory Model

The encoder uses a fragment-based approach:
1. Accumulate frames until fragment size threshold
2. Encode fragment to MP4
3. Upload/clear fragment memory
4. Repeat for next fragment

This keeps memory bounded regardless of total video length.

## License

MulanPSL-2.0
