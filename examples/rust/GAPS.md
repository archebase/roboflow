# Kps Format Specification Gaps (Updated)

This document identifies the gaps between the provided Kps data format specification (v1.2) and the current robocodec implementation.

## Recent Updates (2025-01)

### ✅ Implemented

1. **HDF5 Schema Module** (`src/format/kps/hdf5_schema.rs`)
   - Full schema definition for HDF5 structure
   - Default joint names for all groups (arm, leg, head, waist, effector)
   - `KpsHdf5Schema` type for creating and customizing schemas
   - Support for custom URDF joint names via `with_urdf_joint_names()`

2. **HDF5 Writer Update** (`src/format/kps/hdf5_writer.rs`)
   - Creates full hierarchical structure: `/action/` and `/state/` groups
   - Creates all subgroups: effector, end, head, joint, leg, robot, waist
   - Writes `names` datasets for each joint group (URDF correspondence)
   - Creates per-sensor timestamp datasets at root level
   - Support for original data HDF5 (`proprio_stats_original.hdf5`)
   - `write_task_info()` method for writing task_info JSON

3. **Enhanced Configuration** (`src/format/kps/config.rs`)
   - Added `hdf5_path` field for direct HDF5 path specification
   - Added `field` field for extracting specific message fields
   - `Mapping::hdf5_dataset_path()` method for automatic path resolution

4. **Task Info JSON** (`src/format/kps/task_info.rs`)
   - `TaskInfo` struct with all required fields per v1.2 spec
   - `TaskInfoBuilder` for fluent construction
   - `ActionSegmentBuilder` for building action segments
   - `write_task_info()` function for JSON generation
   - Support for skill types: Pick, Place, Drop, Grasp, Release, Move, Push, Pull, Twist, Pour

### 🟡 Partially Implemented

1. **HDF5 Structure + Data Writing**
   - Group hierarchy is created correctly ✅
   - Names datasets are written with default URDF names ✅
   - Per-sensor timestamp datasets are created ✅
   - Data writing to HDF5 datasets is implemented ✅
   - Pipeline integration via KpsHdf5WriterStage ✅

### ❌ Remaining Gaps

---

## High Priority (for basic compliance)

### 1. Message Decoding Integration

**Issue**: The KpsHdf5WriterStage has simplified message extraction that needs proper codec integration.

**Required**:
- Integrate with the codec registry for proper message decoding
- Support CDR, Protobuf, and JSON message encodings
- Extract data based on schema field names

**Current Status**: Simplified float array extraction (needs proper decoding).

---

## Medium Priority (for full compliance)

### 2. Camera Parameters

### Spec Requirements
For each camera:
- `<camera>_intrinsic_params.json`: fx, fy, cx, cy, width, height, distortion coefficients
- `<camera>_extrinsic_params.json`: frame_id, child_frame_id, position {x,y,z}, orientation {x,y,z,w}

### Current Status
- **✅ Implemented** (2025-01) - Via `CameraParamCollector` in `src/io/kps/camera_params.rs`
- Extracts intrinsics from CameraInfo messages
- Extracts extrinsics from TF messages
- Integrated into `KpsPipeline`

---

### 3. Time Alignment

### Spec Requirements
- All sensor data must be aligned to a unified timestamp
- Original timestamps preserved in per-sensor datasets
- Resampling to target FPS

### Current Status
- **✅ Implemented** (2025-01) - Via `TimeAlignmentStrategy` in `src/pipeline/kps/traits/time_alignment.rs`
- Three strategies: LinearInterpolation, HoldLastValue, NearestNeighbor
- Configurable max gaps and tolerances
- Integrated into `KpsPipeline`

---

### 3.1. MP4 Video Encoding

### Spec Requirements
- Color: `.mp4` with H.264 codec
- Stored in `videos/` directory

### Current Status
- **✅ Implemented** (2025-01) - Via `Mp4Encoder` in `src/io/kps/video_encoder.rs`
- ffmpeg-based encoding with graceful fallback to PPM files
- Configurable codec, FPS, quality

---

## Low Priority (optional features)

### 5. Robot Calibration

### Spec Requirements
`robot_calibration.json` with joint calibration:
```json
{
  "<joint_name>": {
    "id": 0,
    "drive_mode": 0,
    "homing_offset": 0.0,
    "range_min": -3.14,
    "range_max": 3.14
  }
}
```

### Current Status
- **✅ Implemented** (2025-01) - Via `RobotCalibrationGenerator` in `src/io/kps/robot_calibration.rs`
- Parses URDF files to extract joint limits
- Generates `robot_calibration.json` in required format
- Fallback to joint names list when URDF unavailable

---

### 5. Delivery Disk Structure

### Spec Requirements
```
F盘/
    ├── <Robot>-<EndEffector>-<Scene>1/
    ├── URDF/
    │   └── <Robot>-<EndEffector>-v1.0/
    │       └── robot_calibration.json
    └── README.md
```

### Current Status
- **✅ Implemented** (2025-01) - Via `DeliveryBuilder` in `src/io/kps/delivery.rs`
- Creates full directory structure
- Copies episode data, meta, videos
- Copies URDF files
- Generates README.md

---

### 6. Video Format

### Spec Requirements
- Color: `.mp4` with H.264 codec
- Depth: `.mkv` with FFV1 lossless (16-bit)

### Current Status
- **✅ Implemented** (2025-01) - MP4 encoding via `Mp4Encoder`
- **✅ Implemented** (2025-01) - Depth MKV via `DepthMkvEncoder` in `src/io/kps/video_encoder.rs`
- Uses FFV1 codec with 16-bit grayscale input
- Per-camera MKV files (depth_camera_0.mkv, etc.)
- PNG fallback when `dataset-depth` feature enabled

---

### 7. URDF Validation

### Spec Requirements
- All joint `names` must match URDF joint names exactly
- Consistency across HDF5, `robot_calibration.json`, and URDF

### Current Status
- **Not Implemented**: Default names provided but not validated

---

## Summary Table

| Feature | Status | Notes |
|---------|--------|-------|
| HDF5 schema definition | ✅ Implemented | Full schema with defaults |
| HDF5 structure creation | ✅ Implemented | All groups and datasets created |
| Joint names arrays | ✅ Implemented | Written from schema |
| Per-sensor timestamps | ✅ Implemented | Datasets created and written |
| Task info JSON | ✅ Implemented | Builder + writer functions |
| Data writing to HDF5 | ✅ Implemented | Buffered 2D array writing |
| Pipeline integration | ✅ Implemented | KpsHdf5WriterStage |
| Message decoding | ✅ Implemented | `SchemaAwareExtractor` for auto-organization |
| Original data HDF5 | 🟡 Partial | File created, needs data population |
| Camera parameters | ✅ Implemented | `CameraParamCollector` + pipeline |
| Time alignment | ✅ Implemented | `TimeAlignmentStrategy` + pipeline |
| MP4 video encoding | ✅ Implemented | `Mp4Encoder` with ffmpeg fallback |
| Depth video (MKV) | ✅ Implemented | `DepthMkvEncoder` with FFV1 + PNG fallback |
| Robot calibration | ✅ Implemented | `RobotCalibrationGenerator` from URDF |
| Delivery structure | ✅ Implemented | `DeliveryBuilder` + README |

Legend:
- ✅ Implemented
- 🟡 Partially Implemented
- ❌ Not Implemented

---

## Usage Examples

### Creating Task Info JSON

```rust
use robocodec::format::kps::{
    ActionSegmentBuilder, TaskInfoBuilder, write_task_info
};

let task_info = TaskInfoBuilder::new()
    .episode_id("uuid-123")
    .scene_name("Housekeeper")
    .sub_scene_name("Kitchen")
    .init_scene_text("外卖袋放置在桌面左侧")
    .english_init_scene_text("Takeout bag on the left")
    .task_name("收拾外卖盒")
    .english_task_name("Dispose of takeout containers")
    .sn_code("A2D0001AB00029")
    .sn_name("宇树-H1-Dexhand")
    .add_action_segment(
        ActionSegmentBuilder::new(0, 100, "Pick")
            .action_text("左臂拿起桌面上的外卖袋")
            .english_action_text("Pick up the bag with left arm")
            .timestamp("2025-06-16T02:22:48.391668+00:00")
            .build()?,
    )
    .build()?;

write_task_info(&output_dir, &task_info)?;
```

### Writing Task Info from HDF5 Writer

```rust
let mut writer = Hdf5KpsWriter::create(output_dir, episode_id)?;
writer.write_from_mcap(mcap_path, config)?;
writer.write_task_info(&task_info)?;
writer.finish(config)?;
```

---

## Files Created/Modified

1. `src/format/kps/hdf5_schema.rs` - **NEW** - Schema definitions
2. `src/format/kps/hdf5_writer.rs` - **UPDATED** - Full hierarchical structure + data writing
3. `src/format/kps/config.rs` - **UPDATED** - Enhanced mapping support
4. `src/format/kps/mod.rs` - **UPDATED** - Export new types
5. `src/format/kps/task_info.rs` - **NEW** - Task info JSON generation
6. `src/pipeline/stages/kps_hdf5_writer.rs` - **NEW** - Pipeline integration stage
7. `src/pipeline/stages/mod.rs` - **UPDATED** - Export Kps writer stage
8. `examples/kps/kps_config.toml` - **UPDATED** - Comprehensive example
9. `examples/kps/task_info_example.rs` - **NEW** - Usage example
10. `examples/kps/GAPS.md` - **UPDATED** - This document
