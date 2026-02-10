# Architecture Comparison: Current vs Proposed

## Visual Comparison

### Current Architecture (FFmpeg CLI Approach)

```
┌────────────────────────────────────────────────────────────────────────────┐
│                           CURRENT PIPELINE                                 │
│                           ~100 MB/s throughput                            │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Phase 1: Download & Decode (efficient)                                     │
│  ┌─────────┐   ┌──────────┐   ┌───────────┐                               │
│  │ S3/OSS  │──▶│  Source │──▶│  Decode  │──▶ Arc<ImageData>              │
│  │ 10MB/chunks │Registry │   │(robocodec)│     Arena: Zero-copy           │
│  └─────────┘   └──────────┘   └───────────┘                               │
│                                                                             │
│  Phase 2: Buffer (MEMORY BLOAT)                                            │
│  ┌─────────────────────────────────────────────────────────┐               │
│  │  HashMap<String, Vec<ImageData>>                        │               │
│  │  ┌───────────┐ ┌───────────┐ ┌───────────┐              │               │
│  │  │ Camera 0  │ │ Camera 1  │ │ Camera 2  │ 10K frames each │               │
│  │  │ ~9GB      │ │ ~9GB      │ │ ~9GB      │              │               │
│  │  └───────────┘ └───────────┘ └───────────┘              │               │
│  │  Total: ~27 GB                                            │               │
│  └─────────────────────────────────────────────────────────┘               │
│                          │                                              │
│                          ▼ FULL CLONE                                   │
│  Phase 3: Encode (BOTTLENECK)                                             │
│  ┌───────────────────────────────────────────────────────────────────┐    │
│  │  FFmpeg CLI Process (per camera)                                   │    │
│  │  ┌─────────┐   ┌─────────────┐   ┌──────────┐                   │    │
│  │  │ Process │──▶│ PPM Format  │──▶│ H.264    │                   │    │
│  │  │ Spawn   │   │ Conversion  │   │ Encode   │                   │    │
│  │  │ 50-100ms │   │ 70-80% CPU   │   │ ~100MB/s │                   │    │
│  │  └─────────┘   └─────────────┘   └──────────┘                   │    │
│  │                                                                   │    │
│  │  Issues:                                                           │    │
│  │  • IPC through stdin/stdout pipes                                 │    │
│  │  • Process context switching                                      │    │
│  │  • PPM header parsing overhead                                     │    │
│  │  • No GPU acceleration (usually)                                   │    │
│  └───────────────────────────────────────────────────────────────────┘    │
│                          │                                              │
│                          ▼                                              │
│  Phase 4: Upload                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐    │
│  │  S3 Multipart Upload                                               │    │
│  │  • Waits for ALL videos to complete                                │    │
│  │  • Then uploads all                                                │    │
│  └───────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  Total Memory: ~27 GB                                                     │
│  Total Time: ~300s                                                         │
└────────────────────────────────────────────────────────────────────────────┘
```

### Proposed Architecture (rsmpeg Native Streaming)

```
┌────────────────────────────────────────────────────────────────────────────┐
│                      OPTIMIZED PIPELINE (rsmpeg)                            │
│                        TARGET: 1200 MB/s                                   │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │                    MAIN THREAD (Capture)                             │  │
│  │  ┌─────────┐   ┌──────────┐   ┌───────────┐   ┌────────────────┐  │  │
│  │  │ S3/OSS  │──▶│  Source │──▶│  Decode  │──▶│ Arc<ImageData> │  │  │
│  │  │Download │   │Registry  │   │(robocodec│   │   Zero-copy    │  │  │
│  │  └─────────┘   └──────────┘   └───────────┘   └───────┬────────┘  │  │
│  │                                                      │             │  │
│  │                                                      ▼             │  │
│  │                                              ┌──────────────┐       │  │
│  │                                              │SyncSender    │       │  │
│  │                                              │Channel       │       │  │
│  │                                              │(64 frames)    │       │  │
│  │                                              └───────┬───────┘       │  │
│  └──────────────────────────────────────────────────────┼───────────────┘  │
│                                                             │                │
│                                    ┌──────────────────────┴────────┐       │
│                                    │    Frame Distribution          │       │
│                                    │  (broadcast to encoders)        │       │
│                                    └──────────────────────┬─────────┘       │
│                                                       │                 │
│         ┌──────────────────────────────────────────────┼─────────┐       │
│         │              ┌────────────────────────────────┼────┐   │       │
│         │              │         ┌───────────────────────┼────┼───┼───┐   │
│         ▼              ▼         ▼                       ▼    ▼   ▼   ▼   │   │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐   │   │
│  │ ENCODER     │ │ ENCODER     │ │ ENCODER     │ │ ENCODER     │   │   │
│  │ THREAD 1    │ │ THREAD 2    │ │ THREAD 3    │ │ THREAD N    │   │   │
│  │ Camera 0    │ │ Camera 1    │ │ Camera 2    │ │ ...         │   │   │
│  │ ┌─────────┐ │ │ ┌─────────┐ │ │ ┌─────────┐ │ │             │   │   │
│  │ │rsmpeg   │ │ │ │rsmpeg   │ │ │ │rsmpeg   │ │ │             │   │   │
│  │ │Native   │ │ │ │Native   │ │ │ │Native   │ │ │             │   │   │
│  │ │Encoder  │ │ │ │Encoder  │ │ │ │Encoder  │ │ │             │   │   │
│  │ └────┬────┘ │ │ └────┬────┘ │ │ └────┬────┘ │ │             │   │   │
│  │      │      │ │      │      │ │      │      │ │             │   │   │
│  │      ▼      │ │      ▼      │ │      ▼      │ │             │   │   │
│  │ ┌────────┐  │ │ ┌────────┐  │ │ ┌────────┐  │ │             │   │   │
│  │ │SwsCtx  │  │ │ │SwsCtx  │  │ │ │SwsCtx  │  │ │             │   │   │
│  │ │RGB→NV12│  │ │ │RGB→NV12│  │ │ │RGB→NV12│  │ │             │   │   │
│  │ └────────┘  │ │ └────────┘  │ │ └────────┘  │ │             │   │   │
│  │      │      │ │      │      │ │      │      │ │             │   │   │
│  │      ▼      │ │      ▼      │ │      ▼      │ │             │   │   │
│  │ ┌────────┐  │ │ ┌────────┐  │ │ ┌────────┐  │ │             │   │   │
│  │ │AVIO    │  │ │ │AVIO    │  │ │ │AVIO    │  │ │             │   │   │
│  │ │Custom  │  │ │ │Custom  │  │ │ │Custom  │  │ │             │   │   │
│  │ │Write   │  │ │ │Write   │  │ │ │Write   │  │ │             │   │   │
│  │ │Callback│  │ │ │Callback│  │ │ │Callback│  │ │             │   │   │
│  │ └───┬────┘  │ │ └───┬────┘ │ │ └───┬────┘ │ │             │   │   │
│  │      │      │ │      │      │ │      │      │ │             │   │   │
│  └──────┼──────┘─┴──────┼──────┴───────┼──────┴─┴─────────────┘   │   │
│         │              │              │                              │   │
│         ▼              ▼              ▼                              │   │
│  ┌──────────────────────────────────────────────────────────────────┐ │   │
│  │                   ENCODED FRAGMENT CHANNEL                       │ │   │
│  │                   (fMP4 fragments, ~1MB each)                    │ │   │
│  └───────────────────────────────────────┬──────────────────────────┘ │   │
│                                          │                             │
│                                          ▼                             │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                    UPLOAD THREAD POOL                             │ │
│  │  ┌────────────────────────────────────────────────────────────┐  │ │
│  │  │                S3 MULTIPART UPLOADER                        │  │ │
│  │  │  ┌──────────┐    ┌──────────────┐    ┌────────────────┐    │  │ │
│  │  │  │Fragment  │───▶│ Part         │───▶│ S3 Put Part    │    │  │ │
│  │  │  │Accumulator│    │Assembler     │    │(16MB chunks)   │    │  │ │
│  │  │  └──────────┘    └──────────────┘    └────────────────┘    │  │ │
│  │  │                                                           │  │ │
│  │  │  • Upload happens CONCURRENTLY with encoding              │  │ │
│  │  │  • No waiting for all videos to complete                  │  │ │
│  │  │  • Backpressure via channel capacity                      │  │ │
│  │  └────────────────────────────────────────────────────────────┘  │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  Memory per camera: ~50 MB (encoder state + buffer)                         │
│  Ring buffer: ~64 frames × ~1MB = ~64 MB                                     │
│  Total Memory: ~500 MB (54× reduction!)                                      │
│                                                                             │
│  Pipeline Parallelism:                                                       │
│  • Capture: ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━                                  │
│  • Encode:    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━                                │
│  • Upload:       ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━                             │
│                                                                             │
│  Overlapping operations = 3× throughput improvement!                        │
│                                                                             │
│  Total Time: ~75s (4.2× faster!)                                             │
└────────────────────────────────────────────────────────────────────────────┘
```

## Key Differences Summary

| Aspect | Current (FFmpeg CLI) | Proposed (rsmpeg) | Improvement |
|--------|---------------------|-------------------|-------------|
| **Encoding Process** | Separate FFmpeg process | In-process native library | No IPC overhead |
| **Frame Transfer** | stdin/stdout pipes | Direct function call | Zero-copy |
| **Pixel Format** | PPM (ASCII) | Direct RGB→NV12 | No parsing |
| **GPU Acceleration** | Possible but complex | Native NVENC integration | Easy GPU use |
| **Memory** | 27 GB (batch) | 500 MB (streaming) | 54× reduction |
| **Throughput** | ~100 MB/s | ~1200 MB/s | 12× faster |
| **Parallelism** | Sequential | Pipelined | 3× improvement |
| **Upload** | After encoding | During encoding | No added latency |

## Implementation Checklist

- [ ] Phase 1: rsmpeg Foundation
  - [ ] Make rsmpeg non-optional dependency
  - [ ] Create `rsmpeg_encoder.rs` module
  - [ ] Implement `RsmpegEncoder::new()`
  - [ ] Implement `add_frame()` with pixel conversion
  - [ ] Unit tests for single frame encoding

- [ ] Phase 2: Custom AVIO
  - [ ] Implement `avio_write_callback()`
  - [ ] Create `StreamingUploader` for S3
  - [ ] Wire encoder → uploader via channel
  - [ ] Add backpressure handling

- [ ] Phase 3: Thread Architecture
  - [ ] Create `CaptureCoordinator`
  - [ ] Implement `EncoderThreadWorker`
  - [ ] Add graceful shutdown
  - [ ] Statistics collection

- [ ] Phase 4: NVENC Integration
  - [ ] Runtime GPU detection
  - [ ] CUDA context creation
  - [ ] NVENC-specific configuration
  - [ ] CPU fallback

- [ ] Phase 5: Integration
  - [ ] Update `LerobotWriter`
  - [ ] Integration tests
  - [ ] Benchmark verification
  - [ ] Memory profiling
