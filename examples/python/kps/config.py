# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""
KPS configuration management.

Handles loading and validating KPS conversion configuration from TOML files.
"""

from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field


@dataclass
class TopicMapping:
    """Mapping from MCAP topic to KPS feature."""
    topic: str
    feature: str
    type: str  # "image", "state", "action", "timestamp"
    field: Optional[str] = None
    hdf5_path: Optional[str] = None


@dataclass
class DatasetConfig:
    """Dataset metadata configuration."""
    name: str = "robot_dataset"
    fps: int = 30
    robot_type: str = "custom_robot"


@dataclass
class OutputConfig:
    """Output format configuration."""
    formats: List[str] = field(default_factory=lambda: ["hdf5"])
    image_format: str = "raw"
    max_frames: Optional[int] = None


@dataclass
class KpsConfig:
    """Complete KPS conversion configuration."""

    dataset: DatasetConfig = field(default_factory=DatasetConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    mappings: List[TopicMapping] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "KpsConfig":
        """Create config from dictionary (loaded from TOML)."""
        config = cls()

        # Parse dataset config
        if "dataset" in data:
            ds = data["dataset"]
            config.dataset = DatasetConfig(
                name=ds.get("name", "robot_dataset"),
                fps=ds.get("fps", 30),
                robot_type=ds.get("robot_type", "custom_robot")
            )

        # Parse output config
        if "output" in data:
            out = data["output"]
            config.output = OutputConfig(
                formats=out.get("formats", ["hdf5"]),
                image_format=out.get("image_format", "raw"),
                max_frames=out.get("max_frames")
            )

        # Parse topic mappings
        if "mappings" in data:
            for mapping in data["mappings"]:
                config.mappings.append(TopicMapping(
                    topic=mapping["topic"],
                    feature=mapping["feature"],
                    type=mapping["type"],
                    field=mapping.get("field"),
                    hdf5_path=mapping.get("hdf5_path")
                ))

        return config

    def get_mappings_for_type(self, type_name: str) -> List[TopicMapping]:
        """Get all mappings for a specific type."""
        return [m for m in self.mappings if m.type == type_name]

    def get_mapping_for_topic(self, topic: str) -> Optional[TopicMapping]:
        """Get mapping for a specific topic."""
        for m in self.mappings:
            if m.topic == topic:
                return m
        return None

    def get_image_topics(self) -> List[str]:
        """Get all image topic names."""
        return [m.topic for m in self.mappings if m.type == "image"]

    def get_state_topics(self) -> List[str]:
        """Get all state topic names."""
        return [m.topic for m in self.mappings if m.type == "state"]

    def get_action_topics(self) -> List[str]:
        """Get all action topic names."""
        return [m.topic for m in self.mappings if m.type == "action"]


def load_config(path: Path) -> KpsConfig:
    """
    Load KPS configuration from TOML file.

    Args:
        path: Path to TOML configuration file

    Returns:
        KpsConfig object
    """
    # Try tomli (Python < 3.11) or tomllib (Python 3.11+)
    try:
        import tomli
        with open(path, "rb") as f:
            data = tomli.load(f)
    except ImportError:
        try:
            import tomllib
            with open(path, "rb") as f:
                data = tomllib.load(f)
        except ImportError:
            raise ImportError(
                "No TOML library found. Install tomli: pip install tomli"
            )

    return KpsConfig.from_dict(data)


def create_default_config() -> KpsConfig:
    """Create a default KPS configuration."""
    config = KpsConfig()

    # Add common default mappings
    default_mappings = [
        # Camera topics
        TopicMapping("/camera/hand/right/color", "observation.camera_hand_right", "image"),
        TopicMapping("/camera/hand/left/color", "observation.camera_hand_left", "image"),
        TopicMapping("/camera/head/color", "observation.camera_head", "image"),

        # Joint states
        TopicMapping("/joint_states", "observation.joint_position", "state"),
        TopicMapping("/joint_states", "observation.joint_velocity", "state", "velocity"),
        TopicMapping("/joint_states", "observation.joint_effort", "state", "effort"),

        # Actions
        TopicMapping("/command/joint_states", "action.joint_position", "action"),
        TopicMapping("/command/joint_states", "action.joint_velocity", "action", "velocity"),
    ]

    config.mappings.extend(default_mappings)
    return config


def save_config(config: KpsConfig, path: Path) -> None:
    """
    Save configuration to TOML file.

    Args:
        config: KpsConfig to save
        path: Output path
    """
    try:
        import tomli_w
    except ImportError:
        raise ImportError(
            "tomli_w is required to save configs. Install: pip install tomli-w"
        )

    data = {
        "dataset": {
            "name": config.dataset.name,
            "fps": config.dataset.fps,
            "robot_type": config.dataset.robot_type,
        },
        "output": {
            "formats": config.output.formats,
            "image_format": config.output.image_format,
        }
    }

    if config.output.max_frames is not None:
        data["output"]["max_frames"] = config.output.max_frames

    data["mappings"] = []
    for m in config.mappings:
        mapping = {
            "topic": m.topic,
            "feature": m.feature,
            "type": m.type,
        }
        if m.field:
            mapping["field"] = m.field
        if m.hdf5_path:
            mapping["hdf5_path"] = m.hdf5_path
        data["mappings"].append(mapping)

    with open(path, "wb") as f:
        tomli_w.dump(data, f)
