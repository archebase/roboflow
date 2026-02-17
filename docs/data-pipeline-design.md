# Data Pipeline Design

## Overview

This document describes the architecture for converting robotics bag/MCAP files to LeRobot v2.1 format datasets. The system is designed for horizontal scalability, supporting processing of 100k+ source files.

## Core Design Principle

**One bag/mcap file = One episode**

Each input file is processed independently as a single episode, enabling parallel processing across distributed workers.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│  S3/OSS Storage (Source)                                                │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐                                 │
│  │ bag_001 │  │ bag_002 │  │ bag_003 │  ...  (100k+ files)             │
│  └────┬────┘  └────┬────┘  └────┬────┘                                 │
└───────┼────────────┼────────────┼──────────────────────────────────────┘
        │            │            │
        ▼            ▼            ▼
┌───────────────────────────────────────────────────────────────────────┐
│  Distributed Worker Pool (TiKV-coordinated)                            │
│                                                                        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                    │
│  │   Worker 1  │  │   Worker 2  │  │   Worker 3  │  ...               │
│  │  (bag_001)  │  │  (bag_002)  │  │  (bag_003)  │                    │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                    │
└─────────┼────────────────┼────────────────┼───────────────────────────┘
          │                │                │
          ▼                ▼                ▼
┌───────────────────────────────────────────────────────────────────────┐
│  Output: LeRobot v2.1 Dataset                                          │
│                                                                        │
│  episode_000000/        episode_000001/        episode_000002/         │
│  ├── data/              ├── data/              ├── data/               │
│  │   └── episode.parquet│  └── episode.parquet│  └── ...              │
│  ├── videos/            ├── videos/            ├── videos/             │
│  │   └── chunk-000/     │   └── chunk-000/     │   └── ...            │
│  │       └── cam/*.mp4  │       └── cam/*.mp4  │                       │
│  └── meta/              └── meta/              └── meta/               │
└───────────────────────────────────────────────────────────────────────┘
```

## Worker Processing Flow

### Phase 1: Streaming Ingestion

```
┌──────────────────┐
│  S3/OSS Source   │
│  (bag/mcap file) │
└────────┬─────────┘
         │ Streaming read
         ▼
┌──────────────────┐     ┌──────────────────┐
│  Message Buffer  │────►│  Frame Alignment │
│  (by timestamp)  │     │  (by FPS config) │
└──────────────────┘     └────────┬─────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │  AlignedFrame    │
                         │  - states        │
                         │  - actions       │
                         │  - images        │
                         └────────┬─────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │  LerobotWriter   │
                         └────────┬─────────┘
```

### Phase 2: Memory-Bounded Encoding

The key challenge is handling long recordings (hours of data) without running out of memory.

```
┌─────────────────────────────────────────────────────────────────────────┐
│  LerobotWriter Memory Management                                        │
│                                                                        │
│  frame_data[]          image_buffers{}                                 │
│  ┌─────────────┐       ┌───────────────────────────────┐               │
│  │ frame_0     │       │ cam_left:  [img0, img1, ...]  │               │
│  │ frame_1     │       │ cam_right: [img0, img1, ...]  │               │
│  │ ...         │       │ cam_wrist: [img0, img1, ...]  │               │
│  │ frame_N     │       └───────────────────────────────┘               │
│  └─────────────┘                    │                                  │
│        │                            │                                  │
│        │ Never cleared               │ Cleared after flush             │
│        │ (kept for parquet)          │ (frees memory)                  │
│        ▼                            ▼                                  │
│  ┌─────────────┐       ┌───────────────────────────────┐               │
│  │ Accumulates │       │ Temp Video Segments           │               │
│  │ ALL frames  │       │ temp/{session}/               │               │
│  │ for final   │       │   episode_{ep}/{cam}/         │               │
│  │ parquet     │       │     segment_0000.mp4          │               │
│  └─────────────┘       │     segment_0001.mp4          │               │
│                        │     ...                        │               │
│                        └───────────────────────────────┘               │
└─────────────────────────────────────────────────────────────────────────┘
```

**Flush Trigger**: Based on image count since last flush (NOT cumulative frame count).

```rust
// Correct approach
if image_count_since_flush >= max_images_per_chunk {
    flush_video_segment();  // Encode to temp segment, clear image_buffers
    image_count_since_flush = 0;
}
```

### Phase 3: Finalization

When all messages from the bag file are processed:

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Finalize Process                                                       │
│                                                                        │
│  1. Flush remaining image buffers                                      │
│     └─► Encode any remaining images to final segment                   │
│                                                                        │
│  2. Write Parquet file                                                 │
│     └─► All accumulated frame_data → episode_XXXXXX.parquet            │
│                                                                        │
│  3. Merge video segments                                               │
│     └─► segment_0000.mp4 + segment_0001.mp4 + ...                      │
│     └─► → episode_XXXXXX.mp4 (single file per camera)                  │
│                                                                        │
│  4. Write metadata                                                     │
│     └─► info.json, episodes.jsonl, episodes_stats.jsonl, etc.          │
│                                                                        │
│  5. Upload to cloud storage (if configured)                            │
│     └─► Move from temp/ to final location                              │
└─────────────────────────────────────────────────────────────────────────┘
```

## Output Structure (LeRobot v2.1)

```
dataset/
├── data/
│   └── chunk-000/
│       ├── episode_000000.parquet
│       ├── episode_000001.parquet
│       └── ...
├── videos/
│   └── chunk-000/
│       ├── observation.images.cam_left/
│       │   ├── episode_000000.mp4
│       │   ├── episode_000001.mp4
│       │   └── ...
│       ├── observation.images.cam_right/
│       │   └── ...
│       └── observation.images.cam_wrist/
│           └── ...
├── meta/
│   ├── info.json
│   ├── episodes.jsonl
│   ├── episodes_stats.jsonl
│   ├── tasks.jsonl
│   └── stats.json
└── cameras/
    ├── observation.images.cam_left_intrinsics.json
    ├── observation.images.cam_left_extrinsics.json
    └── ...
```

## Key Components

### 1. PipelineExecutor

Location: `crates/roboflow-dataset/src/pipeline.rs`

Responsibilities:
- Buffer incoming messages by timestamp
- Align messages to frame boundaries based on FPS
- Create `AlignedFrame` from grouped messages
- Delegate to `DatasetWriter` for actual writing

### 2. LerobotWriter

Location: `crates/roboflow-dataset/src/lerobot/writer/writer_impl.rs`

Responsibilities:
- Accumulate frame data for parquet
- Buffer images for video encoding
- Trigger segment encoding when memory threshold reached
- Merge segments on finalize
- Write metadata files

### 3. ConcurrentVideoEncoder

Location: `crates/roboflow-video/src/concurrent.rs`

Responsibilities:
- Multi-camera parallel encoding
- Streaming upload to S3/OSS
- Memory-efficient frame handling

### 4. TaskExecutor

Location: `crates/roboflow-distributed/src/worker/executor.rs`

Responsibilities:
- Execute processing tasks for assigned bag files
- Coordinate with TiKV for episode allocation
- Report results to coordinator

## Configuration

### FlushingConfig

```toml
[flushing]
max_frames_per_chunk = 1000      # Images per video segment
max_memory_bytes = 2147483648    # 2GB memory limit
incremental_video_encoding = true
```

### StreamingConfig

```toml
[streaming]
fps = 30                          # Output framerate
completion_window_ns = 99999999   # 3 frames worth of time
ring_buffer_size = 128            # Frame channel capacity
```

## Common Issues and Solutions

### Issue: Too Many Small Segments

**Symptom**: Thousands of 1-frame segment files created.

**Cause**: Flush triggered by cumulative frame count instead of images-since-last-flush.

**Solution**: Track `image_count_since_flush` separately from `frame_data.len()`.

### Issue: Empty Segments Created

**Symptom**: Segments with 0 frames being encoded.

**Cause**: Frames without images triggering flush when image buffers are empty.

**Solution**: Check `image_buffers.values().all(|v| v.is_empty())` before encoding.

### Issue: Memory Not Released

**Symptom**: OOM on long recordings.

**Cause**: `frame_data` accumulating all frames without bound.

**Solution**: This is expected behavior - `frame_data` is kept for the final parquet write. Memory is bounded by flushing `image_buffers` (which typically dominate memory usage).

## Scaling to 100k Files

1. **Episode Allocation**: TiKV distributes episode indices across workers
2. **Independent Processing**: Each worker processes files independently
3. **Output Isolation**: Each episode writes to its own directory
4. **Finalizer Aggregation**: Collects stats and merges metadata after all workers complete

## Future Improvements

1. **Chunked Parquet**: For very long episodes, write parquet in chunks
2. **Streaming Upload**: Upload segments as they're encoded (already implemented)
3. **GPU Encoding**: Hardware-accelerated video encoding
4. **Adaptive Flushing**: Dynamic thresholds based on available memory
