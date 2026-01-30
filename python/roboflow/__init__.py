# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""
Roboflow - High-performance robotics data conversion.

Fluent API for converting between MCAP and ROS bag formats.
Dataset API for creating ML training datasets (KPS, LeRobot).

Example:
    >>> import roboflow
    >>> result = (
    ...     roboflow.Roboflow.open(["input.bag"])
    ...     .write_to("output.mcap")
    ...     .run()
    ... )
    >>> print(result)

    # Dataset conversion:
    >>> config = roboflow.DatasetConfig.from_file("config.toml", format="kps")
    >>> converter = roboflow.DatasetConverter.create("/output", config)
    >>> stats = converter.convert("input.mcap")
    >>> print(f"Converted {stats.frames_written} frames")
"""

from roboflow._roboflow import (
    __version__,
    # Main API
    Roboflow,
    TransformBuilder,
    CompressionPreset,
    PipelineReport,
    HyperPipelineReport,
    FileResult,
    BatchReport,
    # Dataset API
    DatasetConverter,
    DatasetConfig,
    ConversionJob,
    DatasetStats,
    ProgressUpdate,
    convert,
)

__all__ = [
    "__version__",
    # Main API
    "Roboflow",
    "TransformBuilder",
    "CompressionPreset",
    "PipelineReport",
    "HyperPipelineReport",
    "FileResult",
    "BatchReport",
    # Dataset API
    "DatasetConverter",
    "DatasetConfig",
    "ConversionJob",
    "DatasetStats",
    "ProgressUpdate",
    "convert",
    # Dataset submodule
    "dataset",
]

# Dataset submodule alias for convenience
import sys
import types

# Create the dataset submodule
_dataset_module = types.ModuleType("roboflow.dataset", "Dataset submodule")
_dataset_module.DatasetConverter = DatasetConverter
_dataset_module.DatasetConfig = DatasetConfig
_dataset_module.ConversionJob = ConversionJob
_dataset_module.DatasetStats = DatasetStats
_dataset_module.ProgressUpdate = ProgressUpdate

# Register in sys.modules so 'from roboflow.dataset import X' works
sys.modules["roboflow.dataset"] = _dataset_module

# Also expose as attribute so 'roboflow.dataset' works
dataset = _dataset_module
