# Copyright (c) 2026 ArcheBase
# Roboflow is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#     http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

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
