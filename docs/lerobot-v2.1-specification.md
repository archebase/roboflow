# LeRobot v2.1 Dataset Format Specification

## Overview

This document specifies the LeRobot v2.1 dataset format requirements for the roboflow pipeline. LeRobot is a robotics learning framework that standardizes how robot demonstration data is stored, enabling interoperability between different robot platforms and learning algorithms.

**Version**: v2.1  
**Reference**: [HuggingFace LeRobot](https://github.com/huggingface/lerobot)

---

## Directory Structure

```
dataset/
├── data/
│   └── chunk-{chunk_idx:03d}/              # Chunk directories (500 episodes each)
│       └── episode_{episode_idx:06d}.parquet
├── videos/
│   └── chunk-{chunk_idx:03d}/
│       └── {camera_key}/                   # e.g., observation.images.cam_left
│           └── episode_{episode_idx:06d}.mp4
├── meta/
│   ├── info.json                           # Dataset-level metadata
│   ├── episodes.jsonl                      # Per-episode metadata
│   ├── episodes_stats.jsonl               # Per-episode statistics
│   └── tasks.jsonl                         # Task definitions
└── cameras/                               # Optional: Camera calibration
    ├── {camera_key}_intrinsics.json
    └── {camera_key}_extrinsics.json
```

**Key Constraints**:
- **Episodes per chunk**: 500 (hard limit for v2.1)
- **Chunk index**: `episode_index / 500`
- **Video format**: MP4 (H.264 or compatible)
- **Data format**: Apache Parquet

---

## Metadata Files

### 1. info.json

Dataset-level information and schema definition.

```json
{
  "codebase_version": "v2.1",
  "robot_type": "stretch",
  "fps": 30,
  "total_episodes": 100000,
  "total_frames": 5000000,
  "total_tasks": 50,
  "total_videos": 300000,
  "splits": {
    "train": "0:90000",
    "eval": "90000:100000"
  },
  "features": {
    "observation.state": {
      "dtype": "float32",
      "shape": [7],
      "names": ["joint1", "joint2", "joint3", "joint4", "joint5", "joint6", "gripper"]
    },
    "action": {
      "dtype": "float32", 
      "shape": [7],
      "names": ["joint1", "joint2", "joint3", "joint4", "joint5", "joint6", "gripper"]
    },
    "observation.images.cam_left": {
      "dtype": "video",
      "shape": [224, 224, 3],
      "names": ["height", "width", "channel"],
      "info": {
        "video.fps": 30,
        "video.codec": "mp4v",
        "video.pix_fmt": "yuv420p"
      }
    },
    "timestamp": {
      "dtype": "float32",
      "shape": [1],
      "names": ["seconds"]
    },
    "episode_index": {
      "dtype": "int64",
      "shape": [1]
    },
    "frame_index": {
      "dtype": "int64", 
      "shape": [1]
    },
    "index": {
      "dtype": "int64",
      "shape": [1]
    },
    "task_index": {
      "dtype": "int64",
      "shape": [1]
    }
  }
}
```

**Required Fields**:

| Field | Type | Description |
|-------|------|-------------|
| `codebase_version` | string | Always "v2.1" |
| `robot_type` | string | Robot platform identifier |
| `fps` | number | Frames per second (integer or float) |
| `total_episodes` | integer | Total number of episodes |
| `total_frames` | integer | Total frames across all episodes |
| `total_tasks` | integer | Number of unique tasks |
| `total_videos` | integer | Total video files (episodes × cameras) |
| `splits` | object | Train/eval splits as episode ranges |
| `features` | object | Schema definition for all features |

**Feature Schema**:

| Feature Type | Required Fields |
|--------------|----------------|
| **Numeric** | `dtype` (float32/int64), `shape`, `names` (optional) |
| **Video** | `dtype: "video"`, `shape` [H,W,C], `info.video.fps`, `info.video.codec` |
| **Index** | `dtype: "int64"`, `shape: [1]` |

---

### 2. episodes.jsonl

One JSON line per episode (JSON Lines format).

```jsonl
{"episode_index": 0, "tasks": ["pick up the red block"], "length": 50}
{"episode_index": 1, "tasks": ["place in blue bin"], "length": 45}
{"episode_index": 2, "tasks": ["push the box"], "length": 60}
```

**Schema**:

| Field | Type | Description |
|-------|------|-------------|
| `episode_index` | integer | Zero-based episode identifier |
| `tasks` | array[string] | Task descriptions for this episode |
| `length` | integer | Number of frames in episode |

**Constraints**:
- Lines must be sorted by `episode_index`
- `length` must match actual frames in parquet file
- Multiple tasks per episode allowed

---

### 3. episodes_stats.jsonl

Per-episode statistics for each feature (v2.1 format).

```jsonl
{"episode_index": 0, "stats": {"observation.state": {"max": [1.5, ...], "min": [-1.5, ...], "mean": [0.1, ...], "std": [0.5, ...]}, "action": {...}}}
{"episode_index": 1, "stats": {"observation.state": {...}, "action": {...}}}
```

**Schema**:

| Field | Type | Description |
|-------|------|-------------|
| `episode_index` | integer | Episode identifier |
| `stats` | object | Feature statistics keyed by feature name |

**Per-Feature Statistics**:

| Statistic | Shape | Description |
|-----------|-------|-------------|
| `max` | Same as feature | Per-dimension maximum |
| `min` | Same as feature | Per-dimension minimum |
| `mean` | Same as feature | Per-dimension mean |
| `std` | Same as feature | Per-dimension standard deviation |

**For Video Features**:
- Statistics computed per-channel (R, G, B)
- Shape: `[3]` for mean, std across channels

---

### 4. tasks.jsonl

Task definitions with unique indices.

```jsonl
{"task_index": 0, "task": "pick up the red block"}
{"task_index": 1, "task": "place in blue bin"}
{"task_index": 2, "task": "push the box"}
```

**Schema**:

| Field | Type | Description |
|-------|------|-------------|
| `task_index` | integer | Unique task identifier |
| `task` | string | Human-readable task description |

**Constraints**:
- `task_index` must be unique and sequential
- Referenced by `task_index` field in parquet files

---

## Parquet File Format

### Location

```
data/chunk-{chunk_idx:03d}/episode_{episode_idx:06d}.parquet
```

**Examples**:
- `data/chunk-000/episode_000000.parquet` (episodes 0-499)
- `data/chunk-000/episode_000499.parquet`
- `data/chunk-001/episode_000500.parquet` (episodes 500-999)

### Column Schema

| Column | Type | Description |
|--------|------|-------------|
| `observation.state` | List[float32] | Robot state (joints, pose, etc.) |
| `action` | List[float32] | Action taken (targets, commands) |
| `timestamp` | float32 | Time in seconds from episode start |
| `episode_index` | int64 | Episode identifier (repeated per row) |
| `frame_index` | int64 | Frame index within episode (0 to length-1) |
| `index` | int64 | Global frame index across dataset |
| `task_index` | int64 | Reference to tasks.jsonl |
| `observation.images.{camera_key}` | string | Video reference: `"path/to/video.mp4:{timestamp}"` |

**Index Calculations**:

```python
chunk_idx = episode_index // 500
chunk_offset = episode_index % 500
global_index = sum(episodes[i].length for i in range(episode_index)) + frame_index
```

### Video Reference Format

Camera observations in parquet store video references, not pixel data:

```python
# Stored as string in parquet
video_ref = f"videos/chunk-{chunk_idx:03d}/{camera_key}/episode_{episode_idx:06d}.mp4:{timestamp}"

# Example:
# "videos/chunk-000/observation.images.cam_left/episode_000042.mp4:1.433"
```

**Format**: `{relative_path}:{timestamp_in_seconds}`

---

## Video File Format

### Location

```
videos/chunk-{chunk_idx:03d}/{camera_key}/episode_{episode_idx:06d}.mp4
```

**Examples**:
- `videos/chunk-000/observation.images.cam_left/episode_000000.mp4`
- `videos/chunk-000/observation.images.cam_right/episode_000000.mp4`
- `videos/chunk-001/observation.images.cam_left/episode_000500.mp4`

### Video Requirements

| Property | Requirement |
|----------|-------------|
| **Container** | MP4 (.mp4) |
| **Codecs** | H.264 (avc1), H.265 (hevc), or MPEG-4 (mp4v) |
| **Pixel Format** | YUV420P (recommended for compatibility) |
| **FPS** | Must match `info.json` fps field |
| **Resolution** | Defined in `info.json` features (e.g., [224, 224, 3]) |
| **Duration** | Must match episode length / fps |

**Frame Timing**:
- Frame N timestamp = N / fps seconds
- Used to seek to correct frame in video

---

## Chunking Strategy

### Episode to Chunk Mapping

```python
def get_chunk_info(episode_index: int) -> tuple[int, int]:
    """Returns (chunk_index, chunk_offset)"""
    chunk_index = episode_index // 500
    chunk_offset = episode_index % 500
    return chunk_index, chunk_offset

# Examples:
# Episode 0 → chunk 0, offset 0
# Episode 499 → chunk 0, offset 499
# Episode 500 → chunk 1, offset 0
# Episode 99999 → chunk 199, offset 499
```

### Why 500 Episodes per Chunk?

1. **Parquet efficiency**: Optimal row group sizing
2. **Video management**: Reasonable file sizes (1-5GB per chunk)
3. **Memory efficiency**: Can load full chunk in memory for training
4. **Network transfers**: Efficient S3/OSS listing and downloading

---

## Validation Checklist

### Structure Validation

- [ ] All required directories exist (`data/`, `videos/`, `meta/`)
- [ ] Chunk directories properly named (`chunk-000`, `chunk-001`, etc.)
- [ ] Episode files follow naming convention (`episode_{index:06d}`)
- [ ] No missing episodes in sequence

### Metadata Validation

- [ ] `info.json` has all required fields
- [ ] `codebase_version` is exactly "v2.1"
- [ ] `total_episodes` matches actual episode count
- [ ] `features` schema matches parquet columns
- [ ] `episodes.jsonl` has correct line count
- [ ] All `episode_index` values are unique and sequential

### Data Validation

- [ ] Each parquet file has required columns
- [ ] `episode_index` in parquet matches filename
- [ ] `frame_index` ranges from 0 to length-1
- [ ] Video files exist for all camera observations
- [ ] Video frame count matches `length` field
- [ ] Global index is monotonically increasing

### Statistics Validation

- [ ] `episodes_stats.jsonl` has entry for every episode
- [ ] Statistics shape matches feature shape
- [ ] Min ≤ Mean ≤ Max for all dimensions
- [ ] Std ≥ 0 for all dimensions

---

## Example: Minimal Valid Dataset

```
dataset/
├── data/
│   └── chunk-000/
│       ├── episode_000000.parquet
│       └── episode_000001.parquet
├── videos/
│   └── chunk-000/
│       └── observation.images.cam_left/
│           ├── episode_000000.mp4
│           └── episode_000001.mp4
└── meta/
    ├── info.json
    ├── episodes.jsonl
    ├── episodes_stats.jsonl
    └── tasks.jsonl
```

**info.json**:
```json
{
  "codebase_version": "v2.1",
  "robot_type": "test_robot",
  "fps": 30,
  "total_episodes": 2,
  "total_frames": 100,
  "total_tasks": 1,
  "total_videos": 2,
  "splits": {"train": "0:2"},
  "features": {
    "observation.state": {"dtype": "float32", "shape": [7]},
    "action": {"dtype": "float32", "shape": [7]},
    "observation.images.cam_left": {
      "dtype": "video",
      "shape": [224, 224, 3],
      "info": {"video.fps": 30, "video.codec": "mp4v"}
    },
    "timestamp": {"dtype": "float32", "shape": [1]},
    "episode_index": {"dtype": "int64", "shape": [1]},
    "frame_index": {"dtype": "int64", "shape": [1]},
    "index": {"dtype": "int64", "shape": [1]},
    "task_index": {"dtype": "int64", "shape": [1]}
  }
}
```

**episodes.jsonl**:
```jsonl
{"episode_index": 0, "tasks": ["test task"], "length": 50}
{"episode_index": 1, "tasks": ["test task"], "length": 50}
```

**tasks.jsonl**:
```jsonl
{"task_index": 0, "task": "test task"}
```

---

## Integration with Roboflow Pipeline

### Stage Outputs

| Stage | Output Location | Format |
|-------|----------------|--------|
| Discover | ObjectStore | File list metadata |
| Transform | `data/chunk-*/episode_*.parquet` | Direct write |
| Transform | `videos/chunk-*/camera/*.mp4` | Direct write |
| Metadata | `meta/*.json` / `*.jsonl` | JSON/JSONL |

### Episode Index Allocation

```rust
// Distributed episode index allocation via TiKV
let episode_index = tikv.allocate_episode_index(batch_id).await?;
let chunk_index = episode_index / 500;
let chunk_offset = episode_index % 500;

// Output paths:
// data/chunk-{chunk_index:03d}/episode_{episode_index:06d}.parquet
// videos/chunk-{chunk_index:03d}/{camera}/episode_{episode_index:06d}.mp4
```

### 100k Episode Handling

For 100k episodes:
- **Chunks**: 200 (0-199)
- **Files per chunk**: 500 parquet + 500 × cameras videos
- **Metadata size**: ~100MB total (info.json + episodes.jsonl + stats)
- **Final stage**: Only aggregates metadata, not video data

---

## References

1. [LeRobot GitHub](https://github.com/huggingface/lerobot)
2. [LeRobot Dataset Format](https://github.com/huggingface/lerobot/blob/main/lerobot/common/datasets/factory.py)
3. [Parquet Format](https://parquet.apache.org/)
4. [JSON Lines](https://jsonlines.org/)
