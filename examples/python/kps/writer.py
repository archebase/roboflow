# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""
Writer for KPS dataset format.

Handles creating KPS directory structure and writing HDF5 files.
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional
import numpy as np


def create_kps_structure(
    output_dir: Path,
    scene_name: str,
    sub_scene_name: str,
    task_name: str,
    episode_id: str
) -> Path:
    """
    Create the KPS directory structure.

    Structure: <output>/<scene>/<sub_scene>/<task>-<size>_<counts>_<duration>/<uuid>/

    Args:
        output_dir: Root output directory
        scene_name: Scene name
        sub_scene_name: Sub-scene name
        task_name: Task name
        episode_id: Episode identifier

    Returns:
        Path to the episode directory
    """
    task_dir_name = task_name.replace(" ", "_").lower()
    episode_dir = (
        output_dir / scene_name / sub_scene_name /
        f"{task_dir_name}_approx_100counts_5min" / episode_id
    )
    episode_dir.mkdir(parents=True, exist_ok=True)

    # Create subdirectories
    (episode_dir / "camera" / "video").mkdir(parents=True, exist_ok=True)
    (episode_dir / "camera" / "depth").mkdir(parents=True, exist_ok=True)
    (episode_dir / "parameters").mkdir(parents=True, exist_ok=True)
    (episode_dir / "proprio_stats").mkdir(parents=True, exist_ok=True)
    (episode_dir / "audio").mkdir(parents=True, exist_ok=True)

    return episode_dir


def write_task_info(
    episode_dir: Path,
    task_info: Dict[str, Any]
) -> Path:
    """
    Write task_info.json file.

    Args:
        episode_dir: Episode directory
        task_info: Task information dictionary

    Returns:
        Path to written file
    """
    output_path = episode_dir / "task_info.json"
    with open(output_path, "w") as f:
        json.dump(task_info, f, indent=2, ensure_ascii=False)
    return output_path


class KpsWriter:
    """
    Write KPS dataset files.

    Handles writing proprio_stats HDF5 files with the KPS v1.2 structure.
    """

    def __init__(self, episode_dir: Path):
        """
        Initialize writer for an episode.

        Args:
            episode_dir: Episode directory path
        """
        self._episode_dir = Path(episode_dir)
        self._timestamps: List[int] = []
        self._data: Dict[str, List[Any]] = {}

    def add_timestamp(self, timestamp_ns: int) -> None:
        """Add a timestamp to the sequence."""
        self._timestamps.append(timestamp_ns)

    def add_data(self, hdf5_path: str, data: Any) -> None:
        """
        Add data for an HDF5 dataset.

        Args:
            hdf5_path: HDF5 path (e.g., "state/joint/position")
            data: Data to add (will be accumulated into array)
        """
        if hdf5_path not in self._data:
            self._data[hdf5_path] = []
        self._data[hdf5_path].append(data)

    def write_proprio_stats(self, output_path: Optional[Path] = None) -> Path:
        """
        Write proprio_stats.hdf5 file.

        Args:
            output_path: Optional custom output path

        Returns:
            Path to written file
        """
        if output_path is None:
            output_path = self._episode_dir / "proprio_stats" / "proprio_stats.hdf5"

        output_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            import h5py
        except ImportError:
            raise ImportError(
                "h5py is required for HDF5 output. Install: pip install h5py"
            )

        with h5py.File(output_path, "w") as f:
            # Write timestamps
            if self._timestamps:
                ts_array = np.array(self._timestamps, dtype=np.int64)
                f.create_dataset("timestamps", data=ts_array)

            # Write all data arrays
            for hdf5_path, data_list in self._data.items():
                if not data_list:
                    continue

                # Convert to numpy array
                arr = np.array(data_list)

                # Create nested groups if needed
                parts = hdf5_path.split("/")
                current = f
                for part in parts[:-1]:
                    if part not in current:
                        current = current.create_group(part)
                    else:
                        current = current[part]

                # Create dataset
                dataset_name = parts[-1]
                current.create_dataset(dataset_name, data=arr)

        return output_path

    def write_proprio_stats_original(self, output_path: Optional[Path] = None) -> Path:
        """
        Write proprio_stats_original.hdf5 with unaligned data.

        Args:
            output_path: Optional custom output path

        Returns:
            Path to written file
        """
        if output_path is None:
            output_path = self._episode_dir / "proprio_stats" / "proprio_stats_original.hdf5"

        output_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            import h5py
        except ImportError:
            raise ImportError(
                "h5py is required for HDF5 output. Install: pip install h5py"
            )

        with h5py.File(output_path, "w") as f:
            # Write timestamps
            if self._timestamps:
                ts_array = np.array(self._timestamps, dtype=np.int64)
                f.create_dataset("timestamps", data=ts_array)

            # Write data with original topic names
            for hdf5_path, data_list in self._data.items():
                if not data_list:
                    continue

                arr = np.array(data_list)
                # Use original path as dataset name (flat structure)
                safe_name = hdf5_path.replace("/", "_")
                f.create_dataset(safe_name, data=arr)

        return output_path

    def write_camera_parameters(
        self,
        camera_name: str,
        intrinsic: Dict[str, Any],
        extrinsic: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Write camera parameter files.

        Args:
            camera_name: Name of the camera (e.g., "hand_right")
            intrinsic: Intrinsic parameters dictionary
            extrinsic: Optional extrinsic parameters dictionary
        """
        params_dir = self._episode_dir / "parameters"

        # Write intrinsic params
        intrinsic_path = params_dir / f"{camera_name}_intrinsic_params.json"
        with open(intrinsic_path, "w") as f:
            json.dump(intrinsic, f, indent=2)

        # Write extrinsic params if provided
        if extrinsic:
            extrinsic_path = params_dir / f"{camera_name}_extrinsic_params.json"
            with open(extrinsic_path, "w") as f:
                json.dump(extrinsic, f, indent=2)

    def finalize(self) -> Dict[str, Path]:
        """
        Finalize writing and return all created file paths.

        Returns:
            Dictionary mapping file type to path
        """
        result = {}

        # Write HDF5 files
        try:
            proprio_path = self.write_proprio_stats()
            result["proprio_stats"] = proprio_path
            original_path = self.write_proprio_stats_original()
            result["proprio_stats_original"] = original_path
        except ImportError:
            # h5py not available, skip HDF5 writing
            pass

        return result


def extract_joint_data(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract joint data from a sensor_msgs/JointState message.

    Args:
        message: Decoded message dictionary

    Returns:
        Dictionary with position, velocity, effort arrays
    """
    result = {
        "position": message.get("position", []),
        "velocity": message.get("velocity", []),
        "effort": message.get("effort", []),
        "names": message.get("name", [])
    }
    return result


def extract_image_info(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract image metadata from a sensor_msgs/Image message.

    Args:
        message: Decoded message dictionary

    Returns:
        Dictionary with image metadata
    """
    return {
        "width": message.get("width", 0),
        "height": message.get("height", 0),
        "encoding": message.get("encoding", ""),
        "step": message.get("step", 0),
    }
