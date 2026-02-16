# roboflow-video

[![License: MulanPSL-2.0](https://img.shields.io/badge/License-MulanPSL--2.0-blue.svg)](http://license.coscl.org.cn/MulanPSL2)

Low-level video encoding primitives for robotics datasets.

## Crate Separation

This crate provides **low-level video primitives**. For high-level dataset encoding
with multi-camera support, see `roboflow-dataset`.

| Crate | Responsibility |
|-------|---------------|
| `roboflow-video` (this crate) | Hardware encoders, SIMD color conversion, streaming MP4 |
| `roboflow-dataset` | Multi-camera orchestration, dataset format writers (LeRobot) |

**Dependency direction**: `roboflow-dataset` → `roboflow-video` (one-way only)

## Features

- **FFmpeg Integration**: H.264/H.265 encoding via rsmpeg
- **SIMD Color Conversion**: RGB→NV12/YUV420P (8-12x faster than FFmpeg)
- **Fragment-Based**: Memory-efficient fragment encoding
- **Streaming MP4**: Zero-temp-file encoding with chunked output
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

### Streaming MP4 Encoder

For cloud uploads with streaming output (used by `roboflow-dataset`):

```rust
use roboflow_video::{StreamingMp4Encoder, StreamingEncoderConfig};
use std::sync::mpsc::channel;

let (chunk_tx, chunk_rx) = channel();

let mut encoder = StreamingMp4Encoder::with_dimensions(
    StreamingEncoderConfig::default(),
    chunk_tx,
    1920, 1080
)?;

// Add frames
encoder.add_frame(&rgb_data)?;

// Receive encoded chunks
while let Ok(chunk) = chunk_rx.recv() {
    // Upload chunk to S3, etc.
}

encoder.finalize()?;
```

For multi-camera orchestration, see `roboflow-dataset::ConcurrentVideoEncoder`.

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
