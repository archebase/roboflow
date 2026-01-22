# Robocodec Kps Examples

This directory contains examples demonstrating how to use robocodec to convert robotics data to the Kps dataset format as specified in the data format documentation (v1.2).

## Files

| File | Description |
|------|-------------|
| `kps_config.toml` | Example configuration for MCAP → Kps conversion |
| `task_info_example.json` | Example task_info metadata file |
| `convert_to_kps.py` | Python example using robocodec Python API |
| `convert_to_kps.rs` | Rust example using robocodec Rust API |
| `GAPS.md` | Document identifying gaps between spec and implementation |

## Quick Start

### Using the Python API

```bash
# Install robocodec with Python bindings
cd /path/to/robocodec
pip install -e .

# Run the example
python examples/kps/convert_to_kps.py \
    input.mcap \
    ./output \
    examples/kps/kps_config.toml \
    examples/kps/task_info_example.json
```

### Using the Rust API

```bash
# Run with HDF5 support
cargo run --example kps_convert --features kps-hdf5 -- \
    input.mcap \
    ./output \
    examples/kps/kps_config.toml
```

### Using the Binary (Recommended for production)

```bash
# Convert MCAP to Kps format
cargo run --bin convert --features kps-hdf5 -- \
    to-kps input.mcap ./output examples/kps/kps_config.toml
```

## Configuration

The `kps_config.toml` file defines:

1. **Dataset metadata** - name, FPS, robot type
2. **Topic mappings** - which MCAP topics map to which Kps features
3. **Output format** - HDF5 (legacy) or Parquet (v3.0)

### Example Mapping

```toml
[[mappings]]
topic = "/camera/hand/right/color"
feature = "observation.camera_hand_right"
type = "image"

[[mappings]]
topic = "/joint_states"
feature = "observation.joint_position"
type = "state"
```

## Output Structure

The converter creates the following directory structure:

```
./output/
└── <Scene>/
    └── <SubScene>/
        └── <Task>-<size>_<counts>_<duration>/
            └── <UUID>/
                ├── camera/
                │   ├── video/
                │   │   ├── hand_right_color.mp4
                │   │   └── hand_left_color.mp4
                │   └── depth/
                │       ├── hand_right_depth.mkv
                │       └── hand_left_depth.mkv
                ├── parameters/
                │   ├── hand_right_intrinsic_params.json
                │   ├── hand_right_extrinsic_params.json
                │   └── ...
                ├── proprio_stats/
                │   ├── proprio_stats.hdf5
                │   └── proprio_stats_original.hdf5
                └── audio/
                    └── microphone.wav
```

## HDF5 Structure

The `proprio_stats.hdf5` file contains:

```
/ (root)
├── timestamps                          # (N,) int64 - aligned timestamps
├── hand_right_color_mp4_timestamps     # (N,) int64 - original camera timestamps
├── hand_left_color_mp4_timestamps      # (N,) int64
├── action/
│   ├── effector/
│   │   ├── position                    # (N, P1) float32
│   │   └── names                       # (P1,) str
│   ├── joint/
│   │   ├── position                    # (N, 14) float32
│   │   ├── velocity                    # (N, 14) float32
│   │   └── names                       # (14,) str
│   └── ...
└── state/
    ├── joint/
    │   ├── position                    # (N, 14) float32
    │   ├── velocity                    # (N, 14) float32
    │   ├── effort                      # (N, 14) float32
    │   └── names                       # (14,) str
    └── ...
```

## Known Limitations

See `GAPS.md` for a detailed list of features that are not yet implemented:

1. **HDF5 structure** - Not all subgroups are implemented
2. **Task info JSON** - Not automatically generated
3. **Camera parameters** - Not extracted from MCAP
4. **Time alignment** - No temporal resampling
5. **Original data HDF5** - Only aligned data is saved

## Contributing

To add support for missing features:

1. Review the specification in the main data format document
2. Check `GAPS.md` for implementation priorities
3. Modify `src/format/kps/` modules
4. Add tests in `tests/`

## References

- Kps: https://github.com/huggingface/kps
- MCAP format: https://mcap.dev/spec
- ROS bag format: http://wiki.ros.org/Bags/Format
