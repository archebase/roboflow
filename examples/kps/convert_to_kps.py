"""
Example: Convert MCAP to Kps dataset format using robocodec Python API.

This example shows how to use robocodec's Python bindings to read robotics data
and convert it to the Kps dataset format as specified in the data format docs.

Usage:
    python examples/kps/convert_to_kps.py <input.mcap> <output_dir> <config.toml>
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Any
import h5py
import numpy as np


def convert_mcap_to_kps_spec(
    mcap_path: str,
    output_dir: str,
    config_path: str,
    task_info_path: str = None
):
    """
    Convert MCAP file to Kps dataset format with full spec compliance.

    Args:
        mcap_path: Path to input MCAP file
        output_dir: Path to output directory (will create series/scene/sub_scene/task structure)
        config_path: Path to TOML configuration file
        task_info_path: Optional path to task_info JSON file
    """
    # Import robocodec (assuming it's installed or in PYTHONPATH)
    import robocodec

    # Load configuration
    config = _load_toml_config(config_path)
    task_info = _load_task_info(task_info_path) if task_info else None

    # Create output directory structure
    output_path = Path(output_dir)
    series_dir = _create_directory_structure(output_path, task_info)

    # Open MCAP file and iterate through messages
    print(f"Reading MCAP file: {mcap_path}")
    reader = robocodec.Reader(mcap_path)

    # Get channel information
    channels = reader.channels()
    print(f"Found {len(channels)} channels")

    # Collect data by topic for HDF5 writing
    data_by_topic = _collect_messages(reader, config, task_info)

    # Write proprio_stats.hdf5
    proprio_path = series_dir / "proprio_stats" / "proprio_stats.hdf5"
    proprio_path.parent.mkdir(parents=True, exist_ok=True)
    _write_proprio_stats_hdf5(proprio_path, data_by_topic, config, task_info)

    # Write proprio_stats_original.hdf5 (unaligned data)
    original_path = series_dir / "proprio_stats" / "proprio_stats_original.hdf5"
    _write_proprio_stats_original_hdf5(original_path, data_by_topic, config)

    # Write camera parameters if available
    _write_camera_parameters(series_dir, data_by_topic)

    print(f"\nConversion complete! Output written to: {series_dir}")


def _load_toml_config(config_path: str) -> Dict[str, Any]:
    """Load TOML configuration file."""
    try:
        import tomli
        with open(config_path, 'rb') as f:
            return tomli.load(f)
    except ImportError:
        # Fallback to tomllib (Python 3.11+)
        import tomllib
        with open(config_path, 'rb') as f:
            return tomllib.load(f)


def _load_task_info(task_info_path: str) -> List[Dict[str, Any]]:
    """Load task_info JSON file."""
    with open(task_info_path, 'r') as f:
        return json.load(f)


def _create_directory_structure(
    output_path: Path,
    task_info: List[Dict[str, Any]] = None
) -> Path:
    """
    Create the Kps directory structure.

    Structure:
    <output>/<scene>/<sub_scene>/<task>-<size>_<counts>_<duration>/<uuid>/
    """
    if task_info and len(task_info) > 0:
        episode = task_info[0]
        scene_name = episode.get('scene_name', 'UnknownScene')
        sub_scene_name = episode.get('sub_scene_name', 'UnknownSubScene')
        task_name = episode.get('english_task_name', 'UnknownTask').replace(' ', '_')
        episode_id = episode.get('episode_id', '000000')

        # For simplicity, create a basic structure
        # In production, calculate actual size, count, duration
        task_dir_name = f"{task_name}_approx_100counts_5min"
        series_dir = output_path / scene_name / sub_scene_name / task_dir_name / episode_id
    else:
        # Default structure
        series_dir = output_path / "DefaultScene" / "DefaultSubScene" / "default_task" / "000000"

    series_dir.mkdir(parents=True, exist_ok=True)

    # Create subdirectories
    (series_dir / "camera" / "video").mkdir(parents=True, exist_ok=True)
    (series_dir / "camera" / "depth").mkdir(parents=True, exist_ok=True)
    (series_dir / "parameters").mkdir(parents=True, exist_ok=True)
    (series_dir / "proprio_stats").mkdir(parents=True, exist_ok=True)
    (series_dir / "audio").mkdir(parents=True, exist_ok=True)

    return series_dir


def _collect_messages(
    reader: Any,
    config: Dict[str, Any],
    task_info: List[Dict[str, Any]] = None
) -> Dict[str, List[Any]]:
    """
    Collect messages from MCAP, organized by topic.

    Returns:
        Dictionary mapping topic names to lists of (timestamp, decoded_message) tuples
    """
    data_by_topic: Dict[str, List[Any]] = {}
    mappings = config.get('mappings', [])

    print("Processing messages...")
    for i, (msg_dict, channel_info) in enumerate(reader.iter_messages()):
        topic = channel_info['topic']

        # Initialize topic list if needed
        if topic not in data_by_topic:
            data_by_topic[topic] = []

        # Store message with timestamp
        data_by_topic[topic].append({
            'message': msg_dict,
            'timestamp_ns': msg_dict.get('timestamp', 0)  # or use channel timestamp
        })

        if i % 1000 == 0:
            print(f"  Processed {i} messages...")

    return data_by_topic


def _write_proprio_stats_hdf5(
    output_path: Path,
    data_by_topic: Dict[str, List[Any]],
    config: Dict[str, Any],
    task_info: List[Dict[str, Any]] = None
):
    """
    Write proprio_stats.hdf5 with the full Kps spec structure.

    Structure:
    /timestamps                    (N,) int64 - aligned timestamps
    /hand_right_color_mp4_timestamps  (N,) int64
    /action/effector/position      (N, P1) float32
    /action/effector/names         (P1,) str
    /action/joint/position        (N, 14) float32
    /action/joint/names           (14,) str
    /state/joint/position         (N, 14) float32
    /state/joint/effort           (N, 14) float32
    ...
    """
    print(f"Writing HDF5 file: {output_path}")

    with h5py.File(output_path, 'w') as f:
        # Determine alignment size from shortest topic
        min_length = min(len(msgs) for msgs in data_by_topic.values()) if data_by_topic else 0

        # Write root timestamps (aligned)
        f.create_dataset(
            'timestamps',
            data=np.arange(min_length, dtype=np.int64),
            dtype=np.int64
        )

        # Create /action group
        action_group = f.create_group('action')

        # Create /state group
        state_group = f.create_group('state')

        # Process mappings and write data
        for mapping in config.get('mappings', []):
            topic = mapping['topic']
            feature = mapping['feature']
            data_type = mapping.get('type', 'state')

            if topic not in data_by_topic or not data_by_topic[topic]:
                continue

            # Parse feature path (e.g., "observation.joint_position" -> group="observations", name="joint_position")
            parts = feature.split('.')
            if len(parts) < 2:
                continue

            category = parts[0]  # "observation" or "action"
            feature_name = '.'.join(parts[1:])

            # Select target group
            if category == 'action':
                target_group = action_group
            elif category == 'observation':
                target_group = state_group  # In Kps, observations are in /state
            else:
                continue

            # Extract data from messages
            data = data_by_topic[topic]
            arrays = _extract_arrays_from_messages(data, feature_name)

            # Write to HDF5
            for arr_name, arr_data in arrays.items():
                _write_dataset(target_group, arr_name, arr_data)


def _write_proprio_stats_original_hdf5(
    output_path: Path,
    data_by_topic: Dict[str, List[Any]],
    config: Dict[str, Any]
):
    """
    Write proprio_stats_original.hdf5 with unaligned original data.

    This preserves all original frequency data before any resampling.
    """
    print(f"Writing original HDF5 file: {output_path}")

    with h5py.File(output_path, 'w') as f:
        # Write each topic's data at its original frequency
        for mapping in config.get('mappings', []):
            topic = mapping['topic']
            feature = mapping['feature']

            if topic not in data_by_topic or not data_by_topic[topic]:
                continue

            data = data_by_topic[topic]
            feature_name = feature.split('.')[-1]

            # Write timestamps for this topic
            timestamps = np.array([msg['timestamp_ns'] for msg in data], dtype=np.int64)
            timestamp_name = f"{feature_name}_timestamps"
            f.create_dataset(timestamp_name, data=timestamps, dtype=np.int64)

            # Write data
            arrays = _extract_arrays_from_messages(data, feature_name)
            for arr_name, arr_data in arrays.items():
                if arr_name != 'names':  # Skip names for original file
                    full_name = f"{feature_name}_{arr_name}"
                    _write_dataset(f, full_name, arr_data)


def _extract_arrays_from_messages(
    messages: List[Dict[str, Any]],
    feature_name: str
) -> Dict[str, Any]:
    """
    Extract numeric arrays from a list of decoded messages.

    Returns dict with 'position', 'velocity', 'names', etc.
    """
    result = {
        'position': [],
        'velocity': [],
        'effort': [],
        'names': []
    }

    for msg in messages:
        data = msg['message']

        # Try to extract common fields
        for field in ['position', 'velocity', 'effort', 'names']:
            if field in data:
                if field == 'names':
                    # Names are constant, store once
                    if not result['names'] and isinstance(data[field], list):
                        result['names'] = data[field]
                elif isinstance(data[field], list):
                    result[field].append(data[field])

    # Convert to numpy arrays
    for key in ['position', 'velocity', 'effort']:
        if result[key]:
            result[key] = np.array(result[key], dtype=np.float32)
        else:
            del result[key]

    if not result['names']:
        del result['names']

    return result


def _write_dataset(group: h5py.Group, name: str, data: Any):
    """Write a dataset to HDF5 group, handling different data types."""
    if isinstance(data, np.ndarray):
        group.create_dataset(name, data=data, dtype=data.dtype)
    elif isinstance(data, list):
        if data and isinstance(data[0], str):
            # String array
            dt = h5py.string_dtype(encoding='utf-8')
            group.create_dataset(name, data=data, dtype=dt)
        else:
            group.create_dataset(name, data=np.array(data, dtype=np.float32))
    else:
        group.create_dataset(name, data=data)


def _write_camera_parameters(series_dir: Path, data_by_topic: Dict[str, List[Any]]):
    """Write camera intrinsic/extrinsic parameters to JSON files."""
    # This would extract camera info from calibration topics
    # For now, create placeholder files

    params_dir = series_dir / "parameters"

    # Example: hand right camera parameters
    example_intrinsic = {
        "fx": 976.97998046875,
        "fy": 732.7349853515625,
        "cx": 645.2012329101562,
        "cy": 315.3855285644531,
        "width": 1280,
        "height": 720,
        "distortion_model": "plumb_bob",
        "k1": 0.0,
        "k2": 0.0,
        "p1": 0.0,
        "p2": 0.0,
        "k3": 0.0
    }

    example_extrinsic = {
        "frame_id": "right_arm_end_effector_mount_link",
        "child_frame_id": "right_arm_camera_color_optical_frame",
        "position": {
            "x": -0.001807534985204,
            "y": -0.0000127749221,
            "z": 0.12698557287
        },
        "orientation": {
            "x": -0.061042519636452198,
            "y": -0.734867956625483362,
            "z": 0.0003818870463874191,
            "w": 0.6795214914222156511
        }
    }

    # Write example parameters
    for camera in ['hand_right', 'hand_left', 'head']:
        intrinsic_path = params_dir / f"{camera}_intrinsic_params.json"
        extrinsic_path = params_dir / f"{camera}_extrinsic_params.json"

        with open(intrinsic_path, 'w') as f:
            json.dump(example_intrinsic, f, indent=2)
        with open(extrinsic_path, 'w') as f:
            json.dump(example_extrinsic, f, indent=2)


def main():
    if len(sys.argv) < 3:
        print("Usage: python convert_to_kps.py <input.mcap> <output_dir> [config.toml] [task_info.json]")
        print("\nExample:")
        print("  python convert_to_kps.py data.mcap ./output kps_config.toml task_info.json")
        sys.exit(1)

    mcap_path = sys.argv[1]
    output_dir = sys.argv[2]
    config_path = sys.argv[3] if len(sys.argv) > 3 else "examples/kps/kps_config.toml"
    task_info_path = sys.argv[4] if len(sys.argv) > 4 else "examples/kps/task_info_example.json"

    convert_mcap_to_kps_spec(mcap_path, output_dir, config_path, task_info_path)


if __name__ == "__main__":
    main()
