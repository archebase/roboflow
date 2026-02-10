# Max-Performance Streaming Architecture for 1200 MB/s Throughput

## Executive Summary

This document proposes a high-performance video streaming architecture using **rsmpeg** (native FFmpeg bindings) to achieve **1200 MB/s** sustained throughput - a **12x improvement** over the current ~100 MB/s encode bottleneck.

**Key Innovation**: True frame-by-frame streaming encoding with concurrent S3/OSS upload, eliminating intermediate buffering and leveraging zero-copy patterns.

---

## Current State Analysis

### Bottleneck Identification

| Component | Current Speed | Limiting Factor |
|-----------|---------------|-----------------|
| S3 Download | ~1800 MB/s | Network bandwidth |
| Decode | ~1800 MB/s | Arena allocation efficient |
| **Encode** | **~100 MB/s** | **FFmpeg CLI spawn, PPM conversion** |
| S3 Upload | ~500 MB/s | Multipart chunking |

### Root Causes

1. **FFmpeg CLI Overhead** (`std::process::Command`):
   - Process spawn: 50-100ms per camera
   - IPC through stdin/stdout pipes
   - Context switching between processes

2. **PPM Format Overhead**:
   - ASCII header per frame (`P6\n640 480\n255\n`)
   - Extra string formatting
   - Parser overhead in FFmpeg

3. **Batch Mode Operation**:
   - All frames buffered before encoding starts
   - Peak memory: ~27 GB for 10K frames
   - No pipeline parallelism

4. **Multiple Memory Copies**:
   - Arena → ImageData → VideoFrame → PPM → FFmpeg stdin
   - 4× memory amplification

---

## Proposed Architecture: rsmpeg Native Streaming

### Core Principle: In-Process Encoding with Custom AVIO

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MAX-PERFORMANCE STREAMING PIPELINE                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                    CAPTURE THREAD (Main)                             │   │
│  │  ┌──────────┐    ┌──────────┐    ┌─────────────┐    ┌──────────┐  │   │
│  │  │ S3 Chunk │───▶│  Decode  │───▶│ Zero-Copy   │───▶│  Push   │  │   │
│  │  │ Download │    │(robocodec│    │  Arc<Image> │    │ Channel │  │   │
│  │  └──────────┘    └──────────┘    └─────────────┘    └────┬─────┘  │   │
│  │                                                     │             │   │
│  └─────────────────────────────────────────────────────┼───────────────┘   │
│                                                        │                   │
│                                                        ▼                   │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                    ENCODER THREAD POOL (per camera)                  │   │
│  │  ┌────────────────────────────────────────────────────────────────┐  │   │
│  │  │  rsmpeg Native Encoder (in-process)                           │  │   │
│  │  │  ┌─────────────┐    ┌──────────────┐    ┌──────────────────┐   │  │   │
│  │  │  │   AVCodec   │───▶│  SwsContext  │───▶│  AVIOContext     │   │  │   │
│  │  │  │  (H.264/NVENC)│   │  (RGB→NV12)  │    │  (Custom Buffer)│   │  │   │
│  │  │  └─────────────┘    └──────────────┘    └──────┬───────────┘   │  │   │
│  │  │                                                    │            │  │   │
│  │  │                                       fMP4 fragments │            │  │   │
│  │  │                                                    ▼            │  │   │
│  │  │  ┌──────────────────────────────────────────────────────────┐  │  │   │
│  │  │  │                 UPLOAD CHANNEL                          │  │  │   │
│  │  │  └──────────────────────────────────────────────────────────┘  │  │   │
│  │  └────────────────────────────────────────────────────────────────┘  │   │
│  │                                                                       │   │
│  │  Thread 1: Camera 0 │ Thread 2: Camera 1 │ Thread 3: Camera 2       │   │
│  └───────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                    UPLOAD THREAD POOL                                │   │
│  │  ┌────────────────────────────────────────────────────────────────┐  │   │
│  │  │  S3 Multipart Uploader (streaming)                             │  │   │
│  │  │  ┌──────────┐    ┌──────────────┐    ┌──────────────────┐     │  │   │
│  │  │  │ Fragment │───▶│  Buffer      │───▶│ S3 Put Part      │     │  │   │
│  │  │  │ Queue    │    │  Accumulator │    │ (16MB chunks)     │     │  │   │
│  │  │  └──────────┘    └──────────────┘    └──────────────────┘     │  │   │
│  │  └────────────────────────────────────────────────────────────────┘  │   │
│  └───────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Innovations

#### 1. rsmpeg In-Process Encoding

**Instead of**: `Command::new("ffmpeg").spawn()`

**Use**: Direct FFmpeg library calls via rsmpeg

```rust
use rsmpeg::avcodec::*;
use rsmpeg::avformat::*;
use rsmpeg::swscale::*;
use rsmpeg::util::avio::*;

// Native encoder structure
pub struct RsmpegEncoder {
    codec_context: AVCodecContext,
    sws_context: SwsContext,
    format_context: AVFormatContext,
    avio_buffer: AVIOContextCustom,  // Custom I/O for in-memory output
    frame_count: u64,
}

impl RsmpegEncoder {
    pub fn new(width: u32, height: u32, fps: u32, bitrate: u64) -> Result<Self> {
        // 1. Find H.264 encoder
        let codec = AVCodec::find_encoderByName(c"h264_nvenc")
            .or_else(|_| AVCodec::find_encoder_by_id(c"AV_CODEC_ID_H264"))?;

        // 2. Allocate codec context
        let mut codec_context = AVCodecContext::new(&codec)?;

        // 3. Configure encoding parameters
        codec_context.set_width(width);
        codec_context.set_height(height);
        codec_context.set_time_base(AVRational { num: 1, den: fps as i32 });
        codec_context.set_framerate(AVRational { num: fps as i32, den: 1 });
        codec_context.set_bit_rate(bitrate);
        codec_context.set_gop_size(30);

        // NVENC-specific settings for speed
        if codec.name() == "h264_nvenc" {
            codec_context.set_pix_format(c"nv12");
            // Use faster preset
            unsafe { codec_context.as_mut_ptr().rc_max_rate = 0; }  // CBR/VBR
        }

        // 4. Open codec
        codec_context.open(&codec, None)?;

        // 5. Create SWScale context for RGB→NV12 conversion
        let sws_context = SwsContext::get_context(
            width, height, c"rgb24",
            width, height, c"nv12",
            SWS_BILINEAR,
        )?;

        // 6. Custom AVIO for in-memory output
        let write_buffer = AVMem::new(4 * 1024 * 1024)?;  // 4MB write buffer
        let avio_buffer = AVIOContextCustom::alloc_context(
            write_buffer,
            true,  // write_flag
            vec![],
            None,   // read_packet
            Some(write_callback),
            None,   // seek
        );

        // 7. Create format context with custom AVIO
        let mut format_context = unsafe {
            AVFormatContext::wrap_pointer(ffi::avformat_alloc_context2(
                std::ptr::null_mut(),
                std::ptr::null(),
                c"mp4".as_ptr(),
                b"output.mp4\0".as_ptr() as *const i8,
            ))
        };

        // Set up fragmented MP4
        format_context.set_max_interleave_delta(0);
        format_context.set_oformat(AVOutputFormat::muxer_by_name("mp4")?);

        // 8. Create video stream
        let stream = format_context.new_stream()?;
        stream.set_codecpar(codec_context.extract_codecpar());

        // 9. Write header with movflags
        let mut opts = [
            (c"movflags", c"frag_keyframe+empty_moov+default_base_moof"),
        ];
        format_context.write_header(&mut opts)?;

        Ok(Self {
            codec_context,
            sws_context,
            format_context,
            avio_buffer,
            frame_count: 0,
        })
    }

    pub fn add_frame(&mut self, rgb_data: &[u8]) -> Result<Vec<u8>> {
        // 1. Allocate frame
        let mut frame = AVFrame::new();
        frame.set_width(self.codec_context.width());
        frame.set_height(self.codec_context.height());
        frame.set_format(self.codec_context.pix_fmt());

        frame.get_buffer()?;

        // 2. Convert RGB24 → NV12 (GPU-accelerated if available)
        self.sws_context.scale(
            rgb_data,
            self.codec_context.width() as usize * 3,
            &mut frame,
        )?;

        // 3. Set timestamp
        frame.set_pts(self.frame_count as i64);
        self.frame_count += 1;

        // 4. Encode frame
        let mut pkt = AVPacket::new();
        self.codec_context.send_frame(&frame)?;
        self.codec_context.receive_packet(&mut pkt)?;

        // 5. Write packet to format context
        self.format_context.write_frame(&mut pkt)?;

        // 6. Return encoded data from AVIO buffer
        Ok(self.avio_buffer.get_data())
    }
}
```

#### 2. Custom AVIO Write Callback for Streaming Upload

```rust
use std::sync::mpsc::{Sender, channel};
use std::os::raw::{c_void, c_char};

// Write callback that sends encoded data directly to upload channel
extern "C" fn write_callback(
    opaque: *mut c_void,
    buf: *mut u8,
    buf_size: i32,
) -> i32 {
    unsafe {
        let sender = &*(opaque as *const Sender<Vec<u8>>);
        let data = std::slice::from_raw_parts(buf, buf_size as usize);
        let _ = sender.send(data.to_vec());  // Non-blocking send
    }
    buf_size  // Return bytes written
}

// In the encoder setup:
let (encoded_tx, encoded_rx): (Sender<Vec<u8>>, Receiver<Vec<u8>>) = channel();

let avio = AVIOContextCustom::alloc_context(
    buffer,
    true,
    Box::new(encoded_tx),  // Pass channel through opaque
    None,
    Some(write_callback),
    None,
);
```

#### 3. Streaming S3 Upload via Multipart

```rust
pub struct StreamingUploader {
    store: Arc<dyn ObjectStore>,
    multipart: WriteMultipart,
    buffer: Vec<u8>,
    part_size: usize,
    part_number: u16,
}

impl StreamingUploader {
    pub fn new(store: Arc<dyn ObjectStore>, key: &ObjectPath, part_size: usize) -> Self {
        let multipart = tokio::block_on(async {
            store.put_multipart(key).await.unwrap()
        });

        Self {
            store,
            multipart: WriteMultipart::new_with_chunk_size(multipart, part_size),
            buffer: Vec::with_capacity(part_size),
            part_size,
            part_number: 0,
        }
    }

    pub fn add_fragment(&mut self, data: Vec<u8>) -> Result<()> {
        self.buffer.extend_from_slice(&data);

        // Upload full parts immediately
        while self.buffer.len() >= self.part_size {
            let part: Vec<u8> = self.buffer.drain(..self.part_size).collect();

            tokio::block_on(async {
                self.multipart.put_part(part).await
            })?;

            self.part_number += 1;
        }

        Ok(())
    }

    pub fn finalize(mut self) -> Result<()> {
        // Upload remaining partial buffer
        if !self.buffer.is_empty() {
            tokio::block_on(async {
                self.multipart.put_part(self.buffer).await
            })?;
        }

        // Complete multipart upload
        tokio::block_on(async {
            self.multipart.finish().await
        })?;

        Ok(())
    }
}
```

---

## Thread Architecture

### 1. Capture Thread (Main)

```rust
pub struct CaptureCoordinator {
    encoder_tx: mpsc::SyncSender<FrameCommand>,
    encoders: HashMap<String, EncoderHandle>,
}

pub enum FrameCommand {
    AddFrame {
        camera: String,
        image: Arc<ImageData>,
    },
    Flush {
        camera: String,
    },
    Shutdown,
}

impl CaptureCoordinator {
    pub fn add_frame(&mut self, camera: String, image: ImageData) -> Result<()> {
        let image = Arc::new(image);  // Zero-copy sharing
        self.encoder_tx.try_send(FrameCommand::AddFrame { camera, image })?;
        Ok(())
    }
}
```

### 2. Per-Camera Encoder Thread

```rust
pub struct EncoderThread {
    receiver: mpsc::Receiver<FrameCommand>,
    encoder: Option<RsmpegEncoder>,
    uploader: StreamingUploader,
}

impl EncoderThread {
    pub fn run(mut self) -> Result<()> {
        for cmd in self.receiver {
            match cmd {
                FrameCommand::AddFrame { camera: _, image } => {
                    // Initialize encoder on first frame
                    if self.encoder.is_none() {
                        self.encoder = Some(RsmpegEncoder::new(
                            image.width,
                            image.height,
                            30,  // fps
                            5_000_000,  // 5Mbps bitrate
                        )?);
                    }

                    // Encode and stream
                    let encoded = self.encoder.as_mut().unwrap()
                        .add_frame(&image.data)?;

                    // Upload immediately
                    self.uploader.add_fragment(encoded)?;
                }
                FrameCommand::Flush { camera: _ } => {
                    if let Some(encoder) = self.encoder.take() {
                        encoder.finalize()?;
                        self.uploader.finalize()?;
                    }
                }
                FrameCommand::Shutdown => break,
            }
        }
        Ok(())
    }
}
```

---

## Performance Projections

### Theoretical Maximum Throughput

Assuming **NVENC** hardware acceleration:

| Component | Speed | Notes |
|-----------|-------|-------|
| RGB→NV12 conversion | ~3000 MB/s | CUDA-accelerated |
| H.264 encoding (NVENC) | ~2000 MB/s | Real-time 4K @ 60fps |
| S3 multipart upload | ~600 MB/s | Network limited |
| **Total Pipeline** | **~1200 MB/s** | **Sustained** |

### Memory Usage

| Component | Current | Optimized | Reduction |
|-----------|---------|-----------|-----------|
| Frame buffering | 27 GB | 500 MB | 54× |
| Encoder overhead | 200 MB | 50 MB | 4× |
| Total | ~27.2 GB | ~550 MB | **49×** |

### Latency Breakdown

| Stage | Current | Optimized |
|-------|---------|-----------|
| FFmpeg spawn | 50-100ms | 0ms (in-process) |
| Frame encoding | 270s | 30s |
| Upload | 45s | 45s (parallel) |
| **Total** | **~315s** | **~75s** |
| **Improvement** | - | **4.2× faster** |

---

## Implementation Plan

### Phase 1: rsmpeg Foundation (Week 1-2)

**Tasks**:
1. Add rsmpeg as non-optional dependency (currently `optional = true`)
2. Create `crates/roboflow-dataset/src/common/rsmpeg_encoder.rs`
3. Implement basic `RsmpegEncoder` with:
   - `AVCodecContext` setup
   - `SwsContext` for pixel format conversion
   - Custom `AVIOContext` with write callback
4. Add unit tests for encoding single frame

**Acceptance Criteria**:
- [ ] rsmpeg dependency is always available
- [ ] `RsmpegEncoder::new()` creates valid encoder
- [ ] `add_frame()` returns encoded fMP4 fragment
- [ ] Single frame encoding produces valid H.264 packet

### Phase 2: Custom AVIO + Streaming (Week 2-3)

**Tasks**:
1. Implement `AVIOContextCustom` with channel-based write callback
2. Create `StreamingUploader` for concurrent S3 upload
3. Wire encoder → uploader via channel
4. Add backpressure handling (channel capacity limit)

**Acceptance Criteria**:
- [ ] Encoded fragments are sent through channel
- [ ] Uploader receives fragments during encoding
- [ ] S3 parts are uploaded as they accumulate
- [ ] Backpressure prevents memory explosion

### Phase 3: Thread Pool Architecture (Week 3-4)

**Tasks**:
1. Create `CaptureCoordinator` with frame distribution
2. Implement per-camera `EncoderThread` workers
3. Add graceful shutdown handling
4. Implement thread-safe statistics collection

**Acceptance Criteria**:
- [ ] Multiple cameras encode in parallel
- [ ] Each camera has dedicated encoder thread
- [ ] Shutdown completes all in-flight uploads
- [ ] Statistics report encoded frames per camera

### Phase 4: NVENC Integration (Week 4-5)

**Tasks**:
1. Detect NVENC availability at runtime
2. Create CUDA context for zero-copy GPU upload
3. Implement NVENC-specific codec configuration
4. Add CPU fallback (libx264) for systems without GPU

**Acceptance Criteria**:
- [ ] NVENC encoder created when GPU available
- [ ] Falls back to CPU encoding gracefully
- [ ] NVENC path achieves >1500 MB/s encode
- [ ] CPU path still improves over FFmpeg CLI

### Phase 5: Integration & Testing (Week 5-6)

**Tasks**:
1. Integrate with `LerobotWriter`
2. Add integration tests with real S3/OSS
3. Performance benchmarking
4. Memory profiling

**Acceptance Criteria**:
- [ ] `encode_videos_streaming()` uses rsmpeg path
- [ ] End-to-end test produces valid fMP4 videos
- [ ] Benchmark shows >1000 MB/s sustained
- [ ] Memory profiler shows <1GB peak

---

## Code Structure

### New Files

```
crates/roboflow-dataset/src/common/
├── rsmpeg_encoder.rs      # rsmpeg native encoder
│   ├── RsmpegEncoder      # Main encoder struct
│   ├── AVIOCallback       # Custom write callback
│   ├── PixelFormatConv    # RGB→NV12 conversion
│   └── FragmentBuffer     # fMP4 fragment handling
│
├── streaming_coordinator.rs  # Multi-thread coordinator
│   ├── CaptureCoordinator  # Main entry point
│   ├── FrameCommand       # Command enum
│   └── EncoderHandle      # Per-camera thread handle
│
└── streaming_uploader.rs  # S3 streaming upload
    ├── StreamingUploader  # Multipart uploader
    ├── FragmentQueue      # Fragment buffer queue
    └── PartAccumulator    # Chunk assembly
```

### Modified Files

```
crates/roboflow-dataset/
├── Cargo.toml              # Make rsmpeg non-optional
├── src/lerobot/writer/
│   ├── mod.rs              # Add streaming mode selection
│   └── streaming.rs        # Use rsmpeg when available
└── src/common/
    └── mod.rs              # Re-export rsmpeg_encoder
```

---

## Configuration

### Video Config Enhancement

```rust
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    /// Enable rsmpeg native encoding
    pub use_rsmpeg: bool,

    /// Force NVENC (auto-detect if false)
    pub force_nvenc: bool,

    /// Number of encoder threads (0 = num_cpus)
    pub encoder_threads: usize,

    /// Fragment size for fMP4 (bytes)
    pub fragment_size: usize,

    /// Upload part size (bytes)
    pub upload_part_size: usize,

    /// Channel capacity for frame queue
    pub frame_channel_capacity: usize,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            use_rsmpeg: true,
            force_nvenc: false,
            encoder_threads: 0,  // Auto-detect
            fragment_size: 1024 * 1024,      // 1MB fragments
            upload_part_size: 16 * 1024 * 1024,  // 16MB parts
            frame_channel_capacity: 64,  // 64 frames backpressure
        }
    }
}
```

---

## Risk Analysis

| Risk | Impact | Mitigation |
|------|--------|------------|
| **rsmpeg compilation fails** | High | Keep FFmpeg CLI fallback |
| **NVENC unavailable** | Medium | Auto-fallback to CPU libx264 |
| **Thread deadlock** | High | Timeout + watchdog monitoring |
| **Memory leak in AVIO** | Medium | RAII wrappers + valgrind testing |
| **S3 upload stalls** | Medium | Async timeout + retry logic |

---

## Success Criteria

1. **Throughput**: Sustained **>1000 MB/s** on 3-camera 1080p @ 30fps
2. **Memory**: Peak **<1 GB** for 10K frame episode
3. **Latency**: End-to-end **<90s** for 10K frames
4. **Reliability**: 99.9% frames successfully encoded and uploaded
5. **Compatibility**: Works with both S3 and OSS storage backends

---

## References

- rsmpeg documentation: https://docs.rs/rsmpeg/
- FFmpeg fragmented MP4: https://developer.apple.com/documentation/quicktime-file-format/fragmented-mp4-file-format
- S3 multipart upload: https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
- NVENC programming guide: https://developer.nvidia.com/nvidia-video-codec-sdk/
