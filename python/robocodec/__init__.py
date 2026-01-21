"""
Robocodec - High-performance robotics data conversion.

Fluent API for converting between MCAP and ROS bag formats.

Example:
    >>> import robocodec
    >>> result = robocodec.open(["input.bag"]).write_to("output.mcap").run()
    >>> print(f"Converted at {result.average_throughput_mb_s:.1f} MB/s")

    # With hyper mode for maximum throughput:
    >>> result = robocodec.open(["input.bag"]).write_to("output.mcap").hyper_mode().run()

    # With transforms:
    >>> transform = robocodec.TransformBuilder().with_topic_rename("/old", "/new").build()
    >>> result = robocodec.open(["input.bag"]).transform(transform).write_to("output.mcap").run()
"""

from robocodec._robocodec import (
    __version__,
    # Main entry point
    open,
    # Fluent API builder (also accessible via open())
    Robocodec,
    # Configuration
    CompressionPreset,
    _TransformBuilder,
    # Report types (returned by run())
    PipelineReport,
    HyperPipelineReport,
    FileResult,
    BatchReport,
)

__all__ = [
    "__version__",
    # Main entry point
    "open",
    # Fluent API
    "Robocodec",
    "CompressionPreset",
    "TransformBuilder",
    # Reports
    "PipelineReport",
    "HyperPipelineReport",
    "FileResult",
    "BatchReport",
]

# Re-export with friendly name
TransformBuilder = _TransformBuilder
