# Robocodec Roadmap

This document outlines planned features and improvements for robocodec.

---

## Feature 1: Multiple File Parallel Processing

**Status:** Planned
**Priority:** High

### Current State
Batch mode processes files sequentially in a loop within `fluent/builder.rs`.

### Goal
Process multiple input files in parallel for significantly improved throughput.

```rust
Robocodec::open(vec!["a.bag", "b.bag", "c.bag"])?
    .write_to("/output")
    .parallel()  // Enable parallel batch processing
    .with_batch_threads(4)  // Optional: limit concurrent files
    .run()?;
```

### Tasks

| Task | Description | Effort |
|------|-------------|--------|
| 1.1 | Add `.parallel()` method to enable parallel batch mode | Small |
| 1.2 | Add `.with_batch_threads(n)` for concurrency control | Small |
| 1.3 | Implement parallel execution using `rayon` or `std::thread::scope` | Medium |
| 1.4 | Thread-safe result aggregation with ordering preservation | Small |
| 1.5 | Resource management to prevent memory exhaustion | Medium |
| 1.6 | Shared thread pool for compression across files | Medium |

---

## Feature 2: LeRobot Pipeline Integration

**Status:** Planned
**Priority:** High

### Current State
LeRobot writers (`Hdf5LeRobotWriter`, `ParquetLeRobotWriter`) exist as standalone classes in `src/io/lerobot/`. They are not integrated with the fluent pipeline API.

### Goal
Seamless conversion from bag/mcap to LeRobot dataset format via fluent API.

```rust
// Single file → single episode
Robocodec::open(vec!["recording.bag"])?
    .write_to("/dataset")
    .with_lerobot_config("config.toml")
    .run()?;

// Multiple files → multiple episodes in same dataset
Robocodec::open(vec!["ep1.bag", "ep2.bag", "ep3.bag"])?
    .write_to("/dataset")
    .with_lerobot_config("config.toml")
    .run()?;
```

### Output Structure
```
/dataset/
  meta/
    info.json           # Unified metadata
    episodes.jsonl      # Episode index
  data/
    episode_000/
      data.hdf5         # HDF5 format
      # OR
      data.parquet      # Parquet format
      videos/           # MP4 videos for images
    episode_001/
      ...
```

### Tasks

| Task | Description | Effort |
|------|-------------|--------|
| 2.1 | Add `lerobot_config: Option<LeRobotConfig>` to builder state | Small |
| 2.2 | Implement `.with_lerobot_config(path)` method | Small |
| 2.3 | Implement `.with_lerobot_config_inline(config)` method | Small |
| 2.4 | Modify output path resolution for LeRobot mode | Small |
| 2.5 | Create `LeRobotPipeline` orchestrator | Medium |
| 2.6 | Implement single-file bag/mcap → LeRobot episode | Medium |
| 2.7 | Implement episode subdirectory structure | Small |
| 2.8 | Implement multi-file → multi-episode aggregation | Medium |
| 2.9 | Aggregate metadata (info.json) across all episodes | Small |
| 2.10 | Add `RunOutput::LeRobot(LeRobotReport)` variant | Small |

### LeRobot Config Format
```toml
[dataset]
name = "my_robot_dataset"
fps = 30
robot_type = "so100"

[output]
formats = ["hdf5"]  # or ["parquet"], or both

[[mappings]]
topic = "/camera/image_raw"
feature = "observation.image"
type = "image"

[[mappings]]
topic = "/joint_states"
feature = "observation.state"
type = "state"
fields = ["position"]

[[mappings]]
topic = "/cmd_vel"
feature = "action"
type = "action"
```

---

## Feature 3: GPU Acceleration

**Status:** Planned
**Priority:** Medium

### Current State
Basic `GpuCompressionConfig` exists but GPU acceleration is not fully implemented.

### Goals
1. GPU-accelerated compression/decompression
2. GPU-accelerated image/video encoding for LeRobot
3. GPU-accelerated data transforms

### 3.1 GPU Compression

```rust
Robocodec::open(vec!["large.bag"])?
    .write_to("output.mcap")
    .with_gpu_compression(GpuConfig::default())
    .run()?;
```

| Task | Description | Effort |
|------|-------------|--------|
| 3.1.1 | Integrate nvCOMP for ZSTD/LZ4 GPU compression | Large |
| 3.1.2 | Implement GPU memory pool for zero-copy transfers | Medium |
| 3.1.3 | Automatic CPU fallback when GPU unavailable | Small |
| 3.1.4 | Batch compression kernel for small chunks | Medium |
| 3.1.5 | Benchmark and tune chunk sizes for GPU | Medium |

### 3.2 GPU Video Encoding (LeRobot)

```rust
Robocodec::open(vec!["recording.bag"])?
    .write_to("/dataset")
    .with_lerobot_config("config.toml")
    .with_gpu_encoding(true)  // Use NVENC/VideoToolbox
    .run()?;
```

| Task | Description | Effort |
|------|-------------|--------|
| 3.2.1 | Integrate NVENC (NVIDIA) for H.264/H.265 encoding | Large |
| 3.2.2 | Integrate VideoToolbox (macOS) for hardware encoding | Medium |
| 3.2.3 | Integrate VAAPI (Linux) for Intel/AMD hardware encoding | Medium |
| 3.2.4 | Implement encoder abstraction layer | Medium |
| 3.2.5 | Zero-copy image pipeline (GPU decode → encode) | Large |

### 3.3 GPU Transforms

| Task | Description | Effort |
|------|-------------|--------|
| 3.3.1 | GPU image resize/crop for LeRobot preprocessing | Medium |
| 3.3.2 | GPU color space conversion (BGR→RGB, etc.) | Small |
| 3.3.3 | GPU point cloud transforms (future) | Large |

---

## Feature 4: Additional Format Support

**Status:** Planned
**Priority:** Low

### 4.1 HDF5 Direct Output

```rust
Robocodec::open(vec!["recording.bag"])?
    .write_to("output.h5")  // Detected from extension
    .run()?;
```

### 4.2 Parquet Direct Output

```rust
Robocodec::open(vec!["recording.bag"])?
    .write_to("output.parquet")
    .run()?;
```

### Tasks

| Task | Description | Effort |
|------|-------------|--------|
| 4.1 | Implement `Hdf5Writer` with `FormatWriter` trait | Medium |
| 4.2 | Implement `ParquetWriter` with `FormatWriter` trait | Medium |
| 4.3 | Add format detection for `.h5`/`.hdf5` extensions | Small |
| 4.4 | Add format detection for `.parquet` extension | Small |
| 4.5 | Integrate writers into `RoboWriter` builder | Small |

---

## Implementation Phases

### Phase 1: Foundation (Current)
- [x] Unified `RoboReader` / `RoboWriter` architecture
- [x] LeRobot modules in `src/io/lerobot/`
- [x] Fluent API with type-state pattern
- [x] Hyper pipeline for maximum throughput

### Phase 2: LeRobot Integration
- [ ] `.with_lerobot_config()` in fluent API
- [ ] Single file → LeRobot episode
- [ ] Multi-file → multi-episode dataset
- [ ] Metadata aggregation

### Phase 3: Parallel & Performance
- [ ] Parallel batch file processing
- [ ] Resource management and limits
- [ ] Shared compression thread pool

### Phase 4: GPU Acceleration
- [ ] GPU compression (nvCOMP)
- [ ] GPU video encoding (NVENC/VideoToolbox)
- [ ] GPU image transforms

### Phase 5: Extended Formats
- [ ] Direct HDF5 output
- [ ] Direct Parquet output
- [ ] Additional robotics formats

---

## Contributing

Contributions are welcome! Please see the issue tracker for tasks labeled with `good-first-issue` or `help-wanted`.

For major features, please open an issue first to discuss the approach.
