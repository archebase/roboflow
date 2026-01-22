# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""
Roboflow - High-performance robotics data conversion.

Fluent API for converting between MCAP and ROS bag formats.

Example:
    >>> import roboflow
    >>> result = (
    ...     roboflow.Roboflow.open(["input.bag"])
    ...     .write_to("output.mcap")
    ...     .run()
    ... )
    >>> print(result)

    # With transforms:
    >>> builder = roboflow.TransformBuilder()
    >>> builder = builder.with_topic_rename("/old", "/new")
    >>> transform_id = builder.build()
    >>> result = (
    ...     roboflow.Roboflow.open(["input.bag"])
    ...     .transform(transform_id)
    ...     .write_to("output.mcap")
    ...     .run()
    ... )
"""

from roboflow._roboflow import (
    __version__,
    Roboflow,
    TransformBuilder,
    CompressionPreset,
    PipelineReport,
    HyperPipelineReport,
    FileResult,
    BatchReport,
)

__all__ = [
    "__version__",
    "Roboflow",
    "TransformBuilder",
    "CompressionPreset",
    "PipelineReport",
    "HyperPipelineReport",
    "FileResult",
    "BatchReport",
]
