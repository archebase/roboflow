# Video Encoding Optimization Implementation Plan

## Executive Summary

This document provides a comprehensive, actionable implementation plan for optimizing the video encoding pipeline in the Roboflow codebase. The plan is organized into 3 phases as identified in `docs/ARCHITECTURE_REVIEW.md`, with specific tasks, file changes, dependencies, effort estimates, and rollback procedures.

**Current State Analysis:**
- **Bottleneck Location**: `/Users/zhexuany/repo/archebase/roboflow/crates/roboflow-dataset/src/lerobot/writer/encoding.rs:44-294`
- **Memory Issue**: Line 744 in `mod.rs` - full cloning of image buffers before encoding
- **FFmpeg Overhead**: Lines 267-510 in `video.rs` - process spawning per camera per chunk
- **Pixel Format**: Current RGB→PPM→YUV420p conversion path (lines 416-510 in `video.rs`)

**Target Improvements:**
- 3-5x encode throughput increase (100 MB/s → 300-500 MB/s)
- 54x memory reduction (27GB → <500MB for 10K frames)
- 15-30 seconds savings per episode from eliminating spawn overhead

---

## Phase 1: Zero-Copy Pipeline (Quick Wins - 1-2 weeks)

### Overview
Eliminate unnecessary memory copies and FFmpeg process spawning overhead through shared ownership and persistent encoder processes.

### Task 1.1: Implement Shared Ownership for ImageData (Arc Wrapper)

**Objective**: Eliminate the full clone at line 744 in `mod.rs`

**Files to Modify:**

1. **`crates/roboflow-dataset/src/common/base.rs`**
   - **Change**: Modify `ImageData` struct to use `Arc<Vec<u8>>` for data field
   - **Lines**: ~333-351
   - **Implementation**:
     ```rust
     pub struct ImageData {
         pub width: u32,
         pub height: u32,
         pub data: Arc<Vec<u8>>,  // Changed from Vec<u8>
         pub original_timestamp: u64,
         pub is_encoded: bool,
         pub is_depth: bool,
     }
     ```
   - **Update constructors**: `new_rgb()`, `encoded()`, etc. to wrap data in `Arc::new()`
   - **Effort**: 2 hours
   - **Risk**: Low
   - **Testing**: Run existing unit tests, verify no regression in `ImageData` creation

2. **`crates/roboflow-dataset/src/lerobot/writer/encoding.rs`**
   - **Change**: Remove `.clone()` calls on image data
   - **Lines**: 44-50 (camera_data collection)
   - **Implementation**:
     ```rust
     // BEFORE (line 744):
     let camera_data: Vec<(String, Vec<ImageData>)> = self.image_buffers
         .iter()
         .map(|(camera, images)| (camera.clone(), images.clone()))  // FULL COPY
         .collect();

     // AFTER:
     let camera_data: Vec<(String, Vec<ImageData>)> = self.image_buffers
         .iter()
         .map(|(camera, images)| {
             // Only clone the camera name string, images are Arc-wrapped
             (camera.clone(), images.iter().map(|img| {
                 // Arc::clone() is cheap (just increments reference count)
                 ImageData {
                     width: img.width,
                     height: img.height,
                     data: Arc::clone(&img.data),
                     original_timestamp: img.original_timestamp,
                     is_encoded: img.is_encoded,
                     is_depth: img.is_depth,
                 }
             }).collect())
         })
         .collect();
     ```
   - **Effort**: 3 hours
   - **Risk**: Low
   - **Testing**: Verify memory usage reduction with heap profiling

3. **`crates/roboflow-dataset/src/common/video.rs`**
   - **Change**: Update `VideoFrame` to accept `Arc<Vec<u8>>`
   - **Lines**: ~85-151
   - **Implementation**:
     ```rust
     pub struct VideoFrame {
         pub width: u32,
         pub height: u32,
         pub data: Arc<Vec<u8>>,  // Changed from Vec<u8>
         pub is_jpeg: bool,
     }

     impl VideoFrame {
         pub fn new(width: u32, height: u32, data: Arc<Vec<u8>>) -> Self {
             Self { width, height, data, is_jpeg: false }
         }

         pub fn from_jpeg(width: u32, height: u32, jpeg_data: Arc<Vec<u8>>) -> Self {
             Self { width, height, data: jpeg_data, is_jpeg: true }
         }
     }
     ```
   - **Effort**: 2 hours
   - **Risk**: Low
   - **Testing**: Update unit tests in `video.rs` to use `Arc`

**Dependencies**: None (can start immediately)

**Expected Impact**: 2× memory reduction (from 4× amplification to 2×)

**Rollback Plan**: Revert `ImageData` and `VideoFrame` to use `Vec<u8>`, restore `.clone()` calls

---

### Task 1.2: JPEG Passthrough Detection and Optimization

**Objective**: Use `-f mjpeg` input for JPEG-encoded images to skip RGB→YUV conversion

**Files to Modify:**

1. **`crates/roboflow-dataset/src/lerobot/writer/encoding.rs`**
   - **Change**: Detect JPEG format in `build_frame_buffer_static()`
   - **Lines**: ~426-496
   - **Implementation**:
     ```rust
     fn is_jpeg_data(data: &[u8]) -> bool {
         data.len() >= 3 && data[0] == 0xFF && data[1] == 0xD8 && data[2] == 0xFF
     }
     ```
   - **Effort**: 4 hours
   - **Risk**: Low
   - **Testing**: Verify JPEG videos encode correctly with existing tests

2. **`crates/roboflow-dataset/src/common/video.rs`**
   - **Change**: Leverage existing `encode_jpeg_passthrough()` (already implemented at lines 286-392)
   - **Modification**: Ensure `Mp4Encoder::encode_buffer()` correctly routes to this path
   - **Effort**: 1 hour (verification only)
   - **Risk**: Low

**Dependencies**: Task 1.1 (Arc wrapper)

**Expected Impact**: 2-3× encode speedup for JPEG sources (eliminates decode + RGB→YUV)

**Rollback Plan**: Remove JPEG detection logic, always decode to RGB

---

### Task 1.3: Persistent FFmpeg Process Per Camera

**Objective**: Eliminate 50-100ms spawn overhead per camera per chunk

**Files to Create/Modify:**

1. **NEW FILE**: `crates/roboflow-dataset/src/common/persistent_encoder.rs`
   - **Purpose**: Manage persistent FFmpeg process for streaming frame encoding
   - **Effort**: 6 hours
   - **Risk**: Medium (process management complexity)

2. **MODIFY**: `crates/roboflow-dataset/src/lerobot/writer/encoding.rs`
   - **Change**: Add streaming encoding function using `PersistentEncoder`
   - **Effort**: 4 hours
   - **Risk**: Medium

3. **MODIFY**: `crates/roboflow-dataset/src/lerobot/writer/mod.rs`
   - **Change**: Add config flag to enable streaming mode
   - **Effort**: 2 hours
   - **Risk**: Low

4. **MODIFY**: `crates/roboflow-dataset/src/lerobot/config.rs`
   - **Change**: Add `streaming_encode` option to `VideoConfig`
   - **Effort**: 1 hour
   - **Risk**: Low

**Dependencies**: Task 1.1 (Arc wrapper), Task 1.2 (JPEG detection)

**Expected Impact**: 15-30 seconds saved per episode (eliminated spawn overhead)

**Rollback Plan**:
1. Set `streaming_encode` config to `false`
2. Delete `persistent_encoder.rs`
3. Revert changes to `encoding.rs` and `mod.rs`

---

## Phase 2: Streaming Video Encoding (Architecture Change - 3-4 weeks)

### Overview
Implement frame-by-frame encoding during capture with ring buffer to eliminate memory pressure from buffering all frames before encoding.

### Task 2.1: Design Ring Buffer Architecture

**Objective**: Create bounded buffer for capture→encode handoff

**Files to Create:**

1. **NEW FILE**: `crates/roboflow-dataset/src/common/ring_buffer.rs`
   - **Purpose**: Lock-free ring buffer for frame passing between capture and encode threads
   - **Effort**: 6 hours
   - **Risk**: High (concurrency bugs)
   - **Testing**: Extensive concurrent testing with multiple producers/consumers

**Dependencies**: Phase 1 complete

---

### Task 2.2: Implement Per-Camera Streaming Encoder

**Objective**: Create encoder that writes frames as they arrive, not all at once

**Files to Create/Modify:**

1. **NEW FILE**: `crates/roboflow-dataset/src/lerobot/writer/streaming.rs`
   - **Purpose**: Manage per-camera encoder state during episode capture
   - **Effort**: 12 hours
   - **Risk**: High (thread management, synchronization)

2. **MODIFY**: `crates/roboflow-dataset/src/lerobot/writer/mod.rs`
   - **Change**: Integrate `StreamingEncoderManager` into `LerobotWriter`
   - **Effort**: 8 hours
   - **Risk**: High (changes to core writer lifecycle)

**Dependencies**: Task 2.1 (ring buffer)

**Expected Impact**:
- Constant memory usage (64 frames instead of 10,000)
- Overlapping I/O and computation
- No pause in capture during encoding

**Rollback Plan**:
1. Set `streaming_encode` config to `false`
2. Delete `ring_buffer.rs` and `streaming.rs`
3. Revert `LerobotWriter` changes

---

### Task 2.3: Upload-During-Encode Pipeline

**Objective**: Start uploads as soon as each camera's video completes, don't wait for all cameras

**Files to Modify:**

1. **`crates/roboflow-dataset/src/lerobot/writer/streaming.rs`**
   - **Change**: Trigger upload immediately when encoder finishes

2. **MODIFY**: `crates/roboflow-dataset/src/lerobot/upload.rs`
   - **Change**: Add `queue_video_upload()` method for per-video upload
   - **Effort**: 4 hours
   - **Risk**: Medium

**Dependencies**: Task 2.2 (streaming encoder)

**Expected Impact**: 2× end-to-end speedup (overlapping upload with encode)

**Rollback Plan**: Remove per-video upload logic, use batch upload at end

---

## Phase 3: GPU Acceleration (Performance Boost - 2-3 weeks)

### Overview
Leverage existing NVENC/VideoToolbox infrastructure with zero-copy memory transfers.

### Task 3.1: CUDA Zero-Copy Pipeline

**Objective**: Eliminate CPU→GPU memory copies for NVENC encoding

**Files to Create/Modify:**

1. **NEW FILE**: `crates/roboflow-dataset/src/common/cuda_encoder.rs`
   - **Purpose**: Direct CUDA memory encoding using Nvidia libraries
   - **Dependencies**: Add `cudarc` crate to `Cargo.toml`
   - **Effort**: 16 hours
   - **Risk**: High (CUDA API complexity, driver compatibility)

2. **MODIFY**: `crates/roboflow-dataset/src/common/video.rs`
   - **Change**: Use `GpuEncoder` when NVENC available
   - **Effort**: 6 hours
   - **Risk**: Medium

3. **MODIFY**: `crates/roboflow-dataset/Cargo.toml`
   - **Change**: Add CUDA dependencies
   - **Effort**: 1 hour
   - **Risk**: Low

**Dependencies**: Phase 2 complete

**Expected Impact**: 5-10× encode speedup with NVENC

**Rollback Plan**:
1. Disable `gpu` feature flag
2. Delete `cuda_encoder.rs`
3. Revert `NvencEncoder` changes

---

### Task 3.2: Multi-GPU Support

**Objective**: Distribute encoding across multiple GPUs for linear scaling

**Files to Modify:**

1. **`crates/roboflow-dataset/src/lerobot/config.rs`**
   - **Change**: Add GPU device selection
   - **Effort**: 2 hours
   - **Risk**: Low

2. **`crates/roboflow-dataset/src/lerobot/writer/streaming.rs`**
   - **Change**: Assign different cameras to different GPUs
   - **Effort**: 6 hours
   - **Risk**: Medium

**Dependencies**: Task 3.1 (CUDA encoder)

**Expected Impact**: Linear scaling with GPU count (2 GPUs = 2× speedup)

**Rollback Plan**: Set `parallel_encoders = 1` to use single GPU

---

## Implementation Roadmap

### Sprint 1 (Week 1-2): Phase 1 Zero-Copy Pipeline
| Day | Task | Status |
|-----|------|--------|
| 1-2 | Task 1.1: Arc wrapper for ImageData | |
| 3-4 | Task 1.2: JPEG passthrough detection | |
| 5-7 | Task 1.3: Persistent FFmpeg process | |
| 8-10 | Testing, benchmarking, bug fixes | |

**Success Criteria**:
- 2× memory reduction verified
- JPEG sources encode 2× faster
- FFmpeg spawn overhead eliminated

### Sprint 2 (Week 3-6): Phase 2 Streaming Architecture
| Day | Task | Status |
|-----|------|--------|
| 1-3 | Task 2.1: Ring buffer implementation | |
| 4-10 | Task 2.2: Per-camera streaming encoder | |
| 11-14 | Task 2.3: Upload-during-encode | |
| 15-21 | Testing, integration, bug fixes | |

**Success Criteria**:
- Memory usage constant (<500MB for 10K frames)
- No frame drops under normal load
- Uploads start before all encoding finishes

### Sprint 3 (Week 7-9): Phase 3 GPU Acceleration
| Day | Task | Status |
|-----|------|--------|
| 1-8 | Task 3.1: CUDA zero-copy encoder | |
| 9-11 | Task 3.2: Multi-GPU support | |
| 12-14 | Testing, optimization, bug fixes | |

**Success Criteria**:
- >70% GPU utilization during encode
- 5× encode speedup with NVENC
- Linear scaling with multiple GPUs

---

## Risk Assessment & Mitigation

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| **Ring buffer overflow** | Frame loss | Medium | Dynamic sizing + backpressure + monitoring |
| **FFmpeg crash** | Lost data | Medium | Process monitoring + restart + fallback |
| **GPU memory OOM** | Process killed | Low | Batch size limits + CPU fallback |
| **Upload ordering** | Data inconsistency | Low | Sequence tracking in metadata |
| **Thread deadlocks** | Hang | Low | Timeout detection + graceful degradation |
| **Arc reference cycles** | Memory leak | Low | Weak references + cycle detection |
| **CUDA driver issues** | GPU unavailable | Medium | CPU fallback + graceful degradation |

---

## Testing Strategy

### Unit Tests
- **ImageData Arc wrapper**: Verify reference counting works correctly
- **Ring buffer**: Concurrent push/pop with multiple threads
- **PersistentEncoder**: Mock FFmpeg process, verify frame ordering

### Integration Tests
- **10K frame episode**: Memory stays constant, no leaks
- **Multi-camera**: 3 cameras encode independently
- **Crash recovery**: Encoder dies, capture continues

### Performance Tests
- **Baseline**: Measure current 100 MB/s throughput
- **Phase 1**: Verify 200-300 MB/s after zero-copy
- **Phase 2**: Verify constant memory usage
- **Phase 3**: Verify 500+ MB/s with GPU

### Regression Tests
- **Existing tests**: All current tests must pass
- **Output comparison**: Video files identical bit-for-bit
- **Metadata validation**: Parquet files contain correct references

---

## Rollback Procedures

### Phase 1 Rollback
```bash
# Revert Arc wrapper
git revert <commit-hash>

# Restore old clone behavior
git checkout main -- crates/roboflow-dataset/src/lerobot/writer/encoding.rs

# Delete persistent encoder
rm crates/roboflow-dataset/src/common/persistent_encoder.rs
```

### Phase 2 Rollback
```bash
# Disable streaming in config
# config.toml:
[video]
streaming_encode = false

# Delete new files
rm crates/roboflow-dataset/src/common/ring_buffer.rs
rm crates/roboflow-dataset/src/lerobot/writer/streaming.rs
```

### Phase 3 Rollback
```bash
# Disable GPU feature
cargo build --no-default-features --features "distributed dataset-all cloud-storage"

# Delete CUDA encoder
rm crates/roboflow-dataset/src/common/cuda_encoder.rs
```

---

## Monitoring & Observability

### Metrics to Track
```rust
// Add to EncodeStats
pub struct EncodeStats {
    pub images_encoded: usize,
    pub memory_peak_mb: usize,  // NEW
    pub encode_throughput_mbps: f64,  // NEW
    pub frame_drops: usize,  // NEW
    pub gpu_utilization_percent: f64,  // NEW
}
```

### Logging
```rust
tracing::info!(
    memory_mb = get_memory_usage(),
    buffer_len = ring_buffer.len(),
    encode_fps = calculate_encode_fps(),
    gpu_util = get_gpu_utilization(),
    "Encoding progress"
);
```

### Health Checks
- Ring buffer fullness < 80%
- FFmpeg process alive
- GPU memory < 90%
- No frame drops in last 1000 frames

---

## Success Metrics

### Phase 1
- [ ] Memory usage reduced by 50% (13.5GB → <7GB for 10K frames)
- [ ] Encode throughput 200-300 MB/s (2-3× improvement)
- [ ] FFmpeg spawn overhead eliminated (15-30s saved per episode)

### Phase 2
- [ ] Memory usage constant at <500MB (vs 27GB baseline)
- [ ] Zero frame drops under normal load
- [ ] Uploads start before encoding completes

### Phase 3
- [ ] GPU utilization >70% during encode
- [ ] Encode throughput 500+ MB/s (5× improvement)
- [ ] Linear scaling with multiple GPUs

### Overall
- [ ] End-to-end time <60s for 10K frames (vs 300s baseline)
- [ ] 99.9% frame success rate
- [ ] All existing tests pass
- [ ] No regression in output quality

---

## Appendix: File Change Summary

### New Files
1. `crates/roboflow-dataset/src/common/persistent_encoder.rs` (300 lines)
2. `crates/roboflow-dataset/src/common/ring_buffer.rs` (150 lines)
3. `crates/roboflow-dataset/src/lerobot/writer/streaming.rs` (400 lines)
4. `crates/roboflow-dataset/src/common/cuda_encoder.rs` (250 lines)

### Modified Files
1. `crates/roboflow-dataset/src/common/base.rs` (ImageData Arc wrapper)
2. `crates/roboflow-dataset/src/common/video.rs` (VideoFrame Arc, GpuEncoder integration)
3. `crates/roboflow-dataset/src/lerobot/writer/encoding.rs` (JPEG detection, streaming mode)
4. `crates/roboflow-dataset/src/lerobot/writer/mod.rs` (StreamingEncoderManager integration)
5. `crates/roboflow-dataset/src/lerobot/config.rs` (streaming_encode, gpu_device options)
6. `crates/roboflow-dataset/src/lerobot/upload.rs` (Per-video upload)

### Estimated Total Effort
- **Phase 1**: 40 hours (1 week)
- **Phase 2**: 80 hours (2 weeks)
- **Phase 3**: 60 hours (1.5 weeks)
- **Testing**: 40 hours (1 week)
- **Total**: 220 hours (~6 weeks for one developer)
