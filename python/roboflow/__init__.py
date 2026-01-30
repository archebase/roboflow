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
    >>> config = roboflow.dataset.LerobotConfig.from_file("config.toml")
    >>> converter = roboflow.dataset.DatasetConverter.create("/output", config)
    >>> stats = converter.convert("input.bag")
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
    LerobotConfig,
    KpsConfig,
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
    "LerobotConfig",
    "KpsConfig",
    "DatasetConfig",
    "ConversionJob",
    "DatasetStats",
    "ProgressUpdate",
    "convert",
]

# Dataset submodule alias for convenience
from roboflow._roboflow import (
    DatasetConverter as _DatasetConverter,
    LerobotConfig as _LerobotConfig,
    KpsConfig as _KpsConfig,
    DatasetConfig as _DatasetConfig,
    ConversionJob as _ConversionJob,
    DatasetStats as _DatasetStats,
    ProgressUpdate as _ProgressUpdate,
)

import sys
if sys.version_info >= (3, 9):
    import types

    _dataset_module = types.ModuleType("dataset", "Dataset submodule")
    _dataset_module.DatasetConverter = _DatasetConverter
    _dataset_module.LerobotConfig = _LerobotConfig
    _dataset_module.KpsConfig = _KpsConfig
    _dataset_module.DatasetConfig = _DatasetConfig
    _dataset_module.ConversionJob = _ConversionJob
    _dataset_module.DatasetStats = _DatasetStats
    _dataset_module.ProgressUpdate = _ProgressUpdate
    sys.modules["roboflow.dataset"] = _dataset_module
