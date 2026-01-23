# KPS Dataset Conversion (Python)

Full Python implementation for converting robotics data (MCAP/BAG) to KPS dataset format with annotation sidecar files.

## Overview

KPS (Keyframe-and-Propagation-based-Sampler) is a dataset format used for robotics learning. See: https://github.com/huggingface/kps

This package provides:
- **Config module** - Load and manage KPS conversion configuration
- **Reader module** - Read MCAP/BAG files
- **Writer module** - Write KPS HDF5 files and directory structure
- **Converter module** - High-level conversion interface
- **CLI module** - Command-line interface

## Installation

```bash
# Build roboflow with Python bindings
cd /path/to/roboflow
maturin develop --features python

# Install additional dependencies
pip install h5py tomli tomli-w
```

## Quick Start

### Command Line

```bash
# Convert a single episode
python examples/python/kps/kps_conversion.py episode_001.mcap ./output

# Convert a dataset directory
python examples/python/kps/kps_conversion.py ./data ./kps_output config.toml

# Generate templates
python examples/python/kps/kps_conversion.py --generate-config ./kps_config.toml
python examples/python/kps/kps_conversion.py --generate-task-info ./task_info.json
```

### Python API

```python
from examples.python.kps import (
    load_config,
    KpsConverter
)

# Load configuration
config = load_config("kps_config.toml")

# Create converter
converter = KpsConverter(config, use_cli=True)

# Convert episode
result = converter.convert_episode(
    mcap_path="episode_001.mcap",
    output_dir="./output",
    task_info={"episode_id": "001", "english_task_name": "Pick and Place", ...}
)
```

## Directory Structure

### Input Structure

The converter expects robotics data with annotation sidecar files:

```
data/
├── episode_001/
│   ├── episode_001.mcap          # Robotics data
│   ├── episode_001.json          # Annotations
│   └── episode_001_config.toml   # Optional per-episode config
├── episode_002/
│   ├── episode_002.mcap
│   └── episode_002.json
└── ...
```

### Output Structure

The converter creates KPS v1.2 compliant output:

```
output/
└── <Scene>/
    └── <SubScene>/
        └── <Task>-<size>_<counts>_<duration>/
            └── <UUID>/
                ├── camera/
                │   ├── video/
                │   └── depth/
                │   ├── parameters/
                │   │   ├── hand_right_intrinsic_params.json
                │   │   └── hand_right_extrinsic_params.json
                ├── proprio_stats/
                │   ├── proprio_stats.hdf5
                │   └── proprio_stats_original.hdf5
                ├── audio/
                └── task_info.json
```

## Configuration

The `kps_config.toml` file defines topic mappings:

```toml
[dataset]
name = "robot_dataset"
fps = 30
robot_type = "custom_robot"

[output]
formats = ["hdf5"]
image_format = "raw"

# Camera topics
[[mappings]]
topic = "/camera/hand/right/color"
feature = "observation.camera_hand_right"
type = "image"

# Joint states
[[mappings]]
topic = "/joint_states"
feature = "observation.joint_position"
type = "state"

[[mappings]]
topic = "/joint_states"
feature = "observation.joint_velocity"
type = "state"
field = "velocity"

# Actions
[[mappings]]
topic = "/command/joint_states"
feature = "action.joint_position"
type = "action"
```

## Task Info Format

Annotation files (`task_info.json`) should contain:

```json
{
  "episode_id": "001",
  "scene_name": "Kitchen",
  "sub_scene_name": "Counter",
  "english_task_name": "Pick and Place",
  "english_task_description": "Pick up an object and place it elsewhere.",
  "language": "en",
  "label_info": {
    "action_config": [
      {
        "start_frame": 0,
        "end_frame": 100,
        "action_id": "pick_up",
        "action_name": "Pick Up Object"
      }
    ]
  }
}
```

## Modules

### `config.py`

```python
from kps import load_config, KpsConfig

config = load_config("kps_config.toml")
print(f"Dataset: {config.dataset.name}")
print(f"FPS: {config.dataset.fps}")
print(f"Mappings: {len(config.mappings)}")
```

### `reader.py`

```python
from kps import McapReader

reader = McapReader("episode_001.mcap")
reader.open()

print(f"Channels: {list(reader.channels.keys())}")
print(f"Messages: {reader.message_count}")

for msg in reader.iter_messages(topics=["/joint_states"]):
    print(f"Topic: {msg.topic}, Timestamp: {msg.timestamp_ns}")
```

### `writer.py`

```python
from kps import KpsWriter, create_kps_structure, write_task_info

# Create directory structure
episode_dir = create_kps_structure(
    output_dir=Path("./output"),
    scene_name="Kitchen",
    sub_scene_name="Counter",
    task_name="Pick and Place",
    episode_id="001"
)

# Write HDF5 data
writer = KpsWriter(episode_dir)
writer.add_timestamp(1234567890)
writer.add_data("state/joint/position", [1.0, 2.0, 3.0])
writer.finalize()
```

### `converter.py`

```python
from kps import KpsConverter, convert_single, convert_dataset

# High-level conversion
result = convert_single(
    mcap_path="episode_001.mcap",
    output_dir="./output",
    config_path="kps_config.toml"
)

# Or use the converter class
converter = KpsConverter(config)
result = converter.convert_episode(...)
```

## See Also

- [Rust Examples](../../rust/) - Rust implementation and config templates
- [Python Examples](../) - Other roboflow Python examples
