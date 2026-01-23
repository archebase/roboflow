# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""
KPS dataset converter.

Main converter that orchestrates reading MCAP/BAG files and writing KPS format.
"""

import json
import subprocess
from pathlib import Path
from typing import Dict, List, Any, Optional

from .config import KpsConfig, TopicMapping, load_config
from .reader import McapReader, Message
from .writer import KpsWriter, create_kps_structure, write_task_info


class KpsConverter:
    """
    Convert robotics data to KPS dataset format.

    This converter can work in two modes:
    1. CLI mode: Uses the roboflow `convert` binary for KPS conversion
    2. Pure Python mode: Uses Python reader/writer (limited features)
    """

    def __init__(
        self,
        config: KpsConfig,
        use_cli: bool = True,
        convert_binary: Optional[str] = None
    ):
        """
        Initialize converter.

        Args:
            config: KPS configuration
            use_cli: Whether to use CLI binary (recommended)
            convert_binary: Path to convert binary (auto-detected if None)
        """
        self.config = config
        self.use_cli = use_cli
        self.convert_binary = convert_binary

    def convert_episode(
        self,
        mcap_path: Path,
        output_dir: Path,
        task_info: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Convert a single episode to KPS format.

        Args:
            mcap_path: Path to MCAP/BAG file
            output_dir: Output directory
            task_info: Optional task information dictionary

        Returns:
            Conversion result dictionary
        """
        if self.use_cli:
            return self._convert_with_cli(mcap_path, output_dir, task_info)
        else:
            return self._convert_with_python(mcap_path, output_dir, task_info)

    def _convert_with_cli(
        self,
        mcap_path: Path,
        output_dir: Path,
        task_info: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Convert using the roboflow CLI binary."""
        # Resolve config path
        config_path = self._get_config_path()

        # Build command
        cmd = [
            "convert", "to-kps",
            str(mcap_path),
            str(output_dir),
            str(config_path)
        ]

        # Run conversion
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600
            )

            if result.returncode != 0:
                return {
                    "success": False,
                    "error": result.stderr or "Unknown error"
                }

            # Write task_info.json if provided
            if task_info:
                # Find the created episode directory
                episode_dirs = list(output_dir.rglob("proprio_stats"))
                if episode_dirs:
                    episode_dir = episode_dirs[0].parent.parent
                    write_task_info(episode_dir, task_info)

            return {
                "success": True,
                "output_dir": str(output_dir)
            }

        except subprocess.TimeoutExpired:
            return {"success": False, "error": "Conversion timed out"}
        except FileNotFoundError:
            return {
                "success": False,
                "error": "convert binary not found. Build with: cargo build --bin convert --features kps-hdf5"
            }

    def _convert_with_python(
        self,
        mcap_path: Path,
        output_dir: Path,
        task_info: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Convert using pure Python implementation."""
        # Load task info for metadata
        episode_id = task_info.get("episode_id", mcap_path.stem) if task_info else mcap_path.stem
        scene_name = task_info.get("scene_name", "DefaultScene") if task_info else "DefaultScene"
        sub_scene_name = task_info.get("sub_scene_name", "DefaultSubScene") if task_info else "DefaultSubScene"
        task_name = task_info.get("english_task_name", "task") if task_info else "task"

        # Create directory structure
        episode_dir = create_kps_structure(
            output_dir,
            scene_name,
            sub_scene_name,
            task_name,
            episode_id
        )

        # Read MCAP file
        reader = McapReader(mcap_path)
        reader.open()

        # Initialize writer
        writer = KpsWriter(episode_dir)

        # Process messages according to config
        for mapping in self.config.mappings:
            messages = reader.get_messages_for_topic(mapping.topic)
            self._process_messages(writer, messages, mapping)

        # Write output files
        try:
            writer.finalize()
        except ImportError as e:
            # h5py not available
            return {
                "success": False,
                "error": str(e),
                "episode_dir": str(episode_dir)
            }

        # Write task_info.json
        if task_info:
            write_task_info(episode_dir, task_info)
        else:
            # Create minimal task_info
            default_task_info = {
                "episode_id": episode_id,
                "scene_name": scene_name,
                "sub_scene_name": sub_scene_name,
                "english_task_name": task_name,
                "language": "en"
            }
            write_task_info(episode_dir, default_task_info)

        return {
            "success": True,
            "episode_dir": str(episode_dir),
            "messages_processed": reader.message_count
        }

    def _process_messages(
        self,
        writer: KpsWriter,
        messages: List[Message],
        mapping: TopicMapping
    ) -> None:
        """Process messages for a topic mapping."""
        if mapping.type == "timestamp":
            for msg in messages:
                writer.add_timestamp(msg.timestamp_ns)

        elif mapping.type in ("state", "action"):
            # Extract joint data
            for msg in messages:
                writer.add_timestamp(msg.timestamp_ns)
                data = msg.data

                # Determine HDF5 path
                if mapping.hdf5_path:
                    hdf5_path = mapping.hdf5_path
                else:
                    # Generate from feature name
                    parts = mapping.feature.split(".")
                    if len(parts) >= 2:
                        category = parts[0]  # "observation" or "action"
                        feature = parts[1]  # e.g., "joint_position"
                        hdf5_path = f"{category}/{feature}"
                    else:
                        hdf5_path = mapping.feature

                # Extract field if specified
                if mapping.field and mapping.field in data:
                    field_data = data[mapping.field]
                else:
                    field_data = data.get("position", data.get("data", []))

                writer.add_data(hdf5_path, field_data)

    def _get_config_path(self) -> Path:
        """Get path to config file for CLI."""
        # For CLI mode, we need to write the config to a temp file
        # or use the existing config path
        import tempfile
        f = tempfile.NamedTemporaryFile(mode="wb", suffix=".toml", delete=False)
        temp_path = Path(f.name)
        f.close()

        try:
            from .config import save_config
            save_config(self.config, temp_path)
            return temp_path
        except ImportError:
            # Can't save config, return default path
            return Path("kps_config.toml")


def convert_single(
    mcap_path: Path,
    output_dir: Path,
    config_path: Optional[Path] = None,
    task_info: Optional[Dict[str, Any]] = None,
    use_cli: bool = True
) -> Dict[str, Any]:
    """
    Convenience function to convert a single file.

    Args:
        mcap_path: Path to MCAP/BAG file
        output_dir: Output directory
        config_path: Optional path to KPS config
        task_info: Optional task information
        use_cli: Whether to use CLI binary

    Returns:
        Conversion result
    """
    # Load or create config
    if config_path and config_path.exists():
        config = load_config(config_path)
    else:
        from .config import create_default_config
        config = create_default_config()

    # Create converter
    converter = KpsConverter(config, use_cli=use_cli)

    # Convert
    return converter.convert_episode(mcap_path, output_dir, task_info)


def convert_dataset(
    data_dir: Path,
    output_dir: Path,
    config_path: Optional[Path] = None,
    use_cli: bool = True
) -> List[Dict[str, Any]]:
    """
    Convert an entire dataset directory.

    Expected structure:
        data_dir/
        ├── episode_001/
        │   ├── episode_001.mcap
        │   └── episode_001.json
        ├── episode_002/
        │   ├── episode_002.mcap
        │   └── episode_002.json
        └── ...

    Args:
        data_dir: Directory containing episodes
        output_dir: Output directory
        config_path: Optional path to KPS config
        use_cli: Whether to use CLI binary

    Returns:
        List of conversion results for each episode
    """
    # Load or create config
    if config_path and config_path.exists():
        config = load_config(config_path)
    else:
        from .config import create_default_config
        config = create_default_config()

    # Create converter
    converter = KpsConverter(config, use_cli=use_cli)

    # Find episodes
    results = []
    for entry in sorted(data_dir.iterdir()):
        if not entry.is_dir():
            continue

        # Find data file
        mcap_files = list(entry.glob("*.mcap"))
        bag_files = list(entry.glob("*.bag"))
        data_file = mcap_files[0] if mcap_files else (bag_files[0] if bag_files else None)

        if not data_file:
            continue

        # Load annotation if present
        annotation_files = list(entry.glob("*.json"))
        task_info = None
        if annotation_files:
            for ann_file in annotation_files:
                try:
                    with open(ann_file, "r") as f:
                        task_info = json.load(f)
                    break
                except:
                    pass

        # Convert episode
        episode_output = output_dir / entry.name
        result = converter.convert_episode(data_file, episode_output, task_info)
        result["episode_id"] = entry.name
        results.append(result)

    return results
