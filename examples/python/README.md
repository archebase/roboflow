# Python Examples

Examples demonstrating how to use the roboflow Python bindings for robotics data conversion.

## Prerequisites

```bash
# Build the Python extension
cd /path/to/roboflow
maturin develop --features python

# Or use make
make build-python-dev
```

## Examples

| File | Description |
|------|-------------|
| `basic_conversion.py` | Convert a single file between formats |
| `batch_conversion.py` | Process multiple files at once |
| `transforms.py` | Rename topics and types during conversion |
| `complete_workflow.py` | End-to-end dataset processing workflow |
| `roboflow_utils.py` | Utility functions for common operations |
| `kps/` | **KPS dataset conversion package** |

## Quick Start

### Single File Conversion

```bash
python examples/python/basic_conversion.py input.bag output.mcap
```

### Batch Conversion

```bash
python examples/python/batch_conversion.py ./bags ./mcaps --hyper
```

### Using Transforms

```bash
python examples/python/transforms.py input.mcap output.mcap multiple
```

### Complete Workflow

```bash
# Full workflow: discover → convert → export annotations → create splits
python examples/python/complete_workflow.py ./raw_data ./processed
```

## KPS Dataset Conversion

The `kps/` subdirectory contains a full Python implementation for converting robotics data to KPS dataset format with annotation sidecar files.

### KPS Quick Start

```bash
# Convert a single episode
python examples/python/kps/kps_conversion.py episode_001.mcap ./output

# Convert a dataset directory
python examples/python/kps/kps_conversion.py ./data ./kps_output config.toml

# Generate templates
python examples/python/kps/kps_conversion.py --generate-config ./kps_config.toml
python examples/python/kps/kps_conversion.py --generate-task-info ./task_info.json
```

### KPS Directory Structure

```
data/
├── episode_001/
│   ├── episode_001.mcap          # Robotics data
│   └── episode_001.json          # Annotations
├── episode_002/
│   ├── episode_002.mcap
│   └── episode_002.json
```

See [kps/README.md](kps/README.md) for full KPS documentation.

## Common Patterns

### Basic Conversion

```python
import roboflow

result = (
    roboflow.Roboflow.open(["input.bag"])
    .write_to("output.mcap")
    .run()
)
```

### With Transforms

```python
builder = roboflow.TransformBuilder()
builder = builder.with_topic_rename("/old", "/new")
transform_id = builder.build()

result = (
    roboflow.Roboflow.open(["input.bag"])
    .transform(transform_id)
    .write_to("output.mcap")
    .run()
)
```

### Hyper Mode (Maximum Throughput)

```python
result = (
    roboflow.Roboflow.open(["input.mcap"])
    .write_to("output.bag")
    .hyper_mode()
    .run()
)
```

### Batch Processing

```python
# Multiple files are processed in parallel
result = (
    roboflow.Roboflow.open(["ep1.mcap", "ep2.mcap", "ep3.mcap"])
    .write_to("./output")
    .run()
)
```

## See Also

- [Rust Examples](../rust/) - Rust examples and KPS config templates
- [CLAUDE.md](../../CLAUDE.md) - Project documentation
