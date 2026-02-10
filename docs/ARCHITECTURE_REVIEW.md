# Architecture Review & Optimization Proposal

## Executive Summary

This document analyzes the current Roboflow architecture from the perspective of image/video processing and high-performance system programming, identifying bottlenecks and proposing concrete optimizations.

**Current State**: ~1800 MB/s decode throughput, ~100 MB/s encode throughput
**Target**: 3-5x improvement in encode throughput, reduced memory pressure, better GPU utilization

---

## Current Architecture Analysis

### Data Flow Path

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           CURRENT PIPELINE                                           │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  ┌─────────┐    ┌──────────┐    ┌───────────┐    ┌─────────┐    ┌────────┐    │
│  │ S3/OSS  │───▶│  Source  │───▶│  Decode  │───▶│  Align  │───▶│ Encode │───▶│ Upload │
│  │  Input  │    │ Registry│    │(robocodec│    │ & Buffer│    │(FFmpeg)│    │Coordinator│
│  └─────────┘    └──────────┘    └───────────┘    └─────────┘    └────────┘    └────────┘
│       │                │                │               │           │            │
│       │                │                │               │           │            │
│       ▼                ▼                ▼               ▼           ▼            ▼
│  [10MB chunks]   [Threaded       [Arena         [In-memory  [Batch      [Parallel    │
│   streaming]     decoder]       allocation]    buffering]  encoding]  workers]   │
│                                    │               │           │            │
│                                    │               ▼           ▼            │
│                                    │          [MEMORY PRESSURE POINT]            │
│                                    │        * All frames buffered                │
│                                    │        * All images in memory              │
│                                    │        * Then encode all at once           │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Critical Bottlenecks Identified

#### 1. **Encode Bottleneck** (~100 MB/s)

**Location**: `crates/roboflow-dataset/src/lerobot/writer/encoding.rs:100-294`

**Problem**: Video encoding happens **after** all frames are buffered. For a 10K frame episode:
- Memory: ~27GB (3 cameras × 640×480×3 × 10000 frames)
- Encode time: ~270 seconds at 100 MB/s for 27GB of raw data

**Current Flow**:
```rust
// 1. Buffer all frames first (line 44-50 in encoding.rs)
let camera_data: Vec<(String, Vec<ImageData>)> = image_buffers
    .iter()
    .map(|(camera, images)| (camera.clone(), images.clone()))  // FULL CLONE
    .collect();

// 2. Then encode all at once (line 72-78)
encode_videos_sequential(camera_data, ...)
```

**Issues**:
- `images.clone()` creates full copy of all image data
- Sequential encoding per camera (no parallelism without hardware acceleration)
- PPM format adds overhead (header per frame)

#### 2. **Memory Copy Chain**

```
S3/OSS → decode to arena → clone to ImageData → buffer in HashMap
                                      │
                                      ▼
                                   PPM conversion (another copy)
                                      │
                                      ▼
                                   FFmpeg stdin (yet another copy)
```

**Each 640×480 RGB frame**: 921,600 bytes
- Arena allocation: 1×
- HashMap storage: 2×
- VideoFrameBuffer: 3×
- PPM encoding: 4× (with headers)
- **Total: ~4× memory amplification**

#### 3. **FFmpeg Process Spawning Overhead**

**Location**: `crates/roboflow-dataset/src/common/video.rs:267-510`

**Current**: Spawn new FFmpeg process per camera per chunk

```rust
let mut child = Command::new(ffmpeg_path)
    .arg("-f").arg("image2pipe")
    .arg("-vcodec").arg("ppm")
    // ... 20+ arguments
    .spawn()
    .map_err(|_| VideoEncoderError::FfmpegNotFound)?;
```

**Overhead**: ~50-100ms per spawn × 3 cameras × 10 chunks = 15-30 seconds overhead

#### 4. **Suboptimal Pixel Format Pipeline**

**Current**: RGB → PPM → FFmpeg → H.264/yuv420p

```
ImageData (RGB8) → PPM header + RGB → FFmpeg stdin → libx264 → yuv420p → MP4
     │                      │                           │
     ▼                      ▼                           ▼
   3 bytes/pixel      3+ bytes/pixel          RGB→YUV conversion (CPU intensive)
```

**YUV420p conversion**: 70-80% of encoding time on CPU

#### 5. **Hardware Acceleration Underutilized**

**Current**:
- NVENC available: `crates/roboflow-dataset/src/common/video.rs:612-801`
- VideoToolbox available: `crates/roboflow-dataset/src/common/video.rs:803-969`
- **But**: Only used in specific profiles, not by default

**Check**: `crates/roboflow-dataset/src/lerobot/video_profiles.rs`

---

## Optimization Proposal

### Phase 1: Zero-Copy Pipeline (Immediate Win)

#### 1.1 Direct NV12/NV21 Encoding (Eliminate RGB→YUV conversion)

**Approach**: Keep images in compressed format (JPEG) or decode directly to NV12

```rust
// New ImageData variant supporting zero-copy
pub enum ImageData {
   Rgb8(Vec<u8>),                          // Current: RGB8 raw
    Jpeg(Arc<Vec<u8>>),                      // NEW: JPEG passthrough
    Nv12(Arc<Vec<u8>>),                     // NEW: Direct YUV
    Compressed {                              // NEW: Codec-aware storage
        codec: ImageCodec,
        data: Arc<Vec<u8>>,
        width: u32,
        height: u32,
    },
}
```

**Benefit**:
- Skip RGB→YUV conversion in FFmpeg
- Use `-c:v h264_nvenc -rc -b:v 0` (lossless/pass-through)
- **3-5x faster encoding**

#### 1.2 Shared Ownership (Eliminate Cloning)

**Current**:
```rust
.map(|(camera, images)| (camera.clone(), images.clone()))  // FULL COPY
```

**Proposed**:
```rust
pub struct FrameBuffer {
    images: HashMap<String, Arc<ImageData>>,  // Arc instead of owned
}

// No clone needed when encoding
encoder.encode_buffer(&image_data, path)  // Pass Arc directly
```

**Benefit**: 2× memory reduction

#### 1.3 Persistent FFmpeg Process (Eliminate Spawn Overhead)

**Current**: Spawn per camera per chunk

**Proposed**: Spawn once per camera, stream frames

```rust
struct PersistentEncoder {
    ffmpeg_process: Child,
    stdin: BufWriter<ChildStdin>,
    camera: String,
    episode_index: usize,
}

impl PersistentEncoder {
    fn add_frame(&mut self, frame: &VideoFrame) -> Result<()> {
        // Write directly to running process
        write_ppm_frame(&mut self.stdin, frame)?;
        self.stdin.flush()?;
        Ok(())
    }

    fn finish(mut self) -> Result<PathBuf> {
        drop(self.stdin);  // Send EOF
        self.ffmpeg_process.wait()?;
        Ok(self.output_path)
    }
}
```

**Benefit**: 15-30 seconds saved per episode

---

### Phase 2: Streaming Video Encoding (Architecture Change)

#### 2.1 Frame-by-Frame Encoding During Capture

**Current**: Buffer all → encode all at flush

**Proposed**: Encode-as-you-go with bounded lookahead

```
┌────────────────────────────────────────────────────────────────────┐
│                     STREAMING ENCODE ARCHITECTURE                     │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  add_frame()                                                      │
│     │                                                             │
│     ├─▶ [Add to circular buffer]                                 │
│     │                                                             │
│     └─▶ [If buffer threshold: encode N frames]                     │
│              │                                                    │
│              ▼                                                    │
│         [Write to persistent FFmpeg]                              │
│              │                                                    │
│              ├─▶ [Clear buffer slot]                             │
│              │                                                    │
│              └─▶ [Continue capturing]                              │
│                                                                    │
│  finish_episode()                                                 │
│     │                                                             │
│     └─▶ [Flush remaining frames]                                │
│              └─▶ [Signal EOF to FFmpeg]                           │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

**Key insight**: Encoding can happen **parallel** to capture!

#### 2.2 Parallel Capture + Encode Pipeline

```
Thread 1 (Capture)              Thread 2 (Encode)
     │                               │
     ▼                               ▼
  [Incoming Frame]              [FFmpeg Process]
     │                               │
     ├──────────────────────────────▶│
     │                               │
     ▼                               ▼
  [Ring Buffer: 64 frames]      [Encode frame]
     │                               │
     │                               ▼
     │                            [Write MP4]
     │                               │
     └───────────────────────────────┘
```

**Implementation**:
```rust
struct PipelineEncoder {
    capture_tx: mpsc::Sender<VideoFrame>,
    encoder_rx: mpsc::Receiver<VideoFrame>,
    buffer: Vec<VideoFrame>,  // Bounded
    ffmpeg: Option<FfmpegWriter>,
}

impl PipelineEncoder {
    fn add_frame(&mut self, frame: VideoFrame) -> Result<()> {
        self.capture_tx.send(frame)?;

        // Background encoder handles it
        Ok(())
    }
}
```

**Benefit**:
- Overlapping I/O and computation
- Constant memory usage (64 frames instead of 10,000)
- No pause in capture during encoding

---

### Phase 3: GPU Acceleration (Performance Boost)

#### 3.1 NVENC with Zero-Copy

**Current**: CPU RGB → YUV → NVENC

**Proposed**: JPEG → NVENC passthrough or CUDA direct

```rust
// For JPEG input (already compressed)
ffmpeg -f mjpeg -i - -c:v h264_nvenc -rc -b:v 0 ...

// For raw input with GPU upload
ffmpeg -hwaccel cuda -hwaccel_output_format cuda -i - -c:v h264_nvenc ...
```

**Implementation**:
```rust
struct GpuEncoder {
    cuda_context: CudaContext,
    encoder: NvencEncoder,
}

impl GpuEncoder {
    fn encode_from_device(&mut self, cuda_ptr: *mut u8, width: u32, height: u32) {
        // Zero-copy from GPU memory
        self.encoder.encode_cuda_frame(cuda_ptr, width, height)?;
    }
}
```

**Benefit**: 5-10x encode speedup

#### 3.2 Multiple GPU Support

```toml
[video]
gpu_device = 0  # Which GPU to use
parallel_encoders = 3  # 3 parallel encoding sessions
```

---

### Phase 4: Upload Pipeline Optimization

#### 4.1 Upload-During-Encode (Pipeline Parallelism)

**Current**: Encode all → Upload all

```
┌─────────────────────────────────────────────────────────┐
│ CURRENT: Sequential                                    │
├─────────────────────────────────────────────────────────┤
│  Encode Camera 1 ████████████████████████████████████   │
│  Encode Camera 2 ████████████████████████████████████   │
│  Encode Camera 3 ████████████████████████████████████   │
│                                                         │
│  Upload All ████████████████████████████████████████████   │
└─────────────────────────────────────────────────────────┘
```

**Proposed**: Upload-as-you-go

```
┌─────────────────────────────────────────────────────────┐
│ PROPOSED: Pipelined                                     │
├─────────────────────────────────────────────────────────┤
│  Encode C1 ████░░░░░░░░░░░Upload C1 ░░░░░░░░░░░░░░░░░░░░░░░░░░  │
│  Encode C2     ░███░░░░░░░░░Upload C2 ░░░░░░░░░░░░░░░░░░░░░░░░░  │
│  Encode C3       ░███░░░░░░░Upload C3 ░░░░░░░░░░░░░░░░░░░░░░░░░░  │
└─────────────────────────────────────────────────────────┘
│  █ = Encoding, ░ = Uploading (happening in parallel)   │
└─────────────────────────────────────────────────────────┘
```

**Implementation**:
```rust
struct PipelinedUpload {
    encode_tx: mpsc::Sender<(PathBuf, String)>,  // (video_path, camera)
    upload_worker: UploadWorker,
}

impl PipelinedUpload {
    async fn process_video(&mut self, video_path: PathBuf) {
        // Start upload immediately after video is written
        self.upload_worker.queue_upload(video_path.clone()).await?;
    }
}
```

---

## Implementation Priority

### Sprint 1: Quick Wins (1-2 weeks)

| Change | Effort | Impact | Risk |
|--------|--------|--------|------|
| Shared ownership (Arc) | Low | 2× memory reduction | Low |
| JPEG passthrough detection | Low | 2× encode speed | Low |
| Persistent FFmpeg | Medium | 15-30s saved | Medium |

### Sprint 2: Architecture (3-4 weeks)

| Change | Effort | Impact | Risk |
|--------|--------|--------|------|
| Ring buffer pipeline | High | 3× overall throughput | High |
| Upload-during-encode | Medium | 2× end-to-end | Medium |

### Sprint 3: GPU (2-3 weeks)

| Change | Effort | Impact | Risk |
|--------|--------|--------|------|
| CUDA integration | High | 5-10× encode speed | High |
| Multi-GPU support | Medium | Linear scaling | Medium |

---

## Proposed New Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        OPTIMIZED PIPELINE                                              │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  ┌─────────┐    ┌──────────┐    ┌───────────┐    ┌─────────┐                   │
│  │ S3/OSS  │───▶│  Source  │───▶│  Arena    │───▶│ Capture │                   │
│  │  Input  │    │ Registry│    │ Allocator│    │ Thread │                   │
│  └─────────┘    └──────────┘    └─────┬─────┘    └────┬────┘                   │
│                                       │                   │                            │
│                                       │                   ▼                            │
│                                       │            ┌────────────────┐                      │
│                                       │            │  Ring Buffer    │                      │
│                                       │            │  (64 frames)    │                      │
│                                       │            └────┬──────────┘                      │
│                                       │                 │                               │
│                                       │                 ▼                               │
│  ┌────────────────────────────────────────────────────┴─────────┐                      │
│  │                    Encoder Thread Pool                       │                      │
│  │  ┌────────┐  ┌────────┐  ┌────────┐                       │                      │
│  │  │NVENC C1 │  │NVENC C2 │  │NVENC C3 │  (per camera)        │                      │
│  │  └────────┘  └────────┘  └────────┘                       │                      │
│  │                                                             │                      │
│  │  Output: MP4 files (streaming)                               │                      │
│  └─────────────────────────────────────────────────────────────┘                      │
│                                       │                                                          │
│                                       ▼                                                          │
│  ┌────────────────────────────────────────────────────────────────┐                      │
│  │              Upload Thread Pool                              │                      │
│  │  ┌────────┐  ┌────────┐  ┌────────┐                       │                      │
│  │  │Upload  │  │Upload  │  │Upload  │  (as videos complete)   │                      │
│  │  │ C1     │  │C2     │  │C3     │                            │                      │
│  │  └────────┘  └────────┘  └────────┘                       │                      │
│  └────────────────────────────────────────────────────────────────┘                      │
│                                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐                      │
│  │                    Parquet Writer (separate thread)            │                      │
│  │  ┌────────┐  ┌────────┐  ┌────────┐                       │                      │
│  │  │Chunk 1 │  │Chunk 2 │  │Chunk 3 │  (streaming writes)       │                      │
│  │  └────────┘  └────────┘  └────────┘                       │                      │
│  └────────────────────────────────────────────────────────────────┘                      │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Key Data Structures

```rust
// Zero-copy image storage
pub struct ImageFrame {
    pub data: Arc<ImageData>,  // Shared ownership
    pub timestamp: u64,
    pub camera: String,
}

// Bounded ring buffer for capture→encode handoff
struct FrameRingBuffer {
    buffer: Vec<Option<ImageFrame>>,
    write_pos: AtomicUsize,
    read_pos: AtomicUsize,
    capacity: usize,  // e.g., 64 frames
}

// Per-camera persistent encoder
struct PerCameraEncoder {
    camera: String,
    ffmpeg: Option<FfmpegInstance>,
    gpu: Option<GpuEncoder>,
    state: EncoderState,
}

enum EncoderState {
    Idle,
    Encoding {
        frames_encoded: usize,
        output_path: PathBuf,
    },
    Finished(PathBuf),
}
```

---

## Performance Projections

### Current vs Optimized (10,000 frames, 3 cameras @ 640×480)

| Metric | Current | Optimized | Improvement |
|--------|---------|-----------|-------------|
| **Memory Peak** | ~27 GB | ~500 MB | 54× |
| **Encode Time** | ~270s | ~30s | 9× |
| **End-to-End** | ~300s | ~50s | 6× |
| **CPU Usage** | 100% (1 core) | 30% (spread) | Better utilization |
| **GPU Usage** | 0% | 80% | New capability |

---

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| **Ring buffer overflow** | Frame loss | Dynamic sizing + backpressure |
| **FFmpeg crash** | Lost data | Process monitoring + restart |
| **GPU memory** | OOM | Batch size limits + fallback to CPU |
| **Upload ordering** | Data inconsistency | Sequence tracking in metadata |

---

## Success Criteria

1. **Memory**: <1GB for 10K frame episode (vs 27GB today)
2. **Throughput**: >500 MB/s sustained encode (vs 100 MB/s today)
3. **Latency**: <60s end-to-end for 10K frames (vs 300s today)
4. **GPU**: >70% GPU utilization during encode
5. **Reliability**: 99.9% frames successfully processed
