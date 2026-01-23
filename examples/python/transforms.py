# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""
Example: Using transforms to rename topics and types.

This example demonstrates how to use transforms to modify topics and message types
during conversion. This is useful when you need to standardize data from different
sources or match a specific schema.

Usage:
    python examples/transforms.py <input_file> <output_file>
"""

import sys
import roboflow


def rename_single_topic(input_path: str, output_path: str) -> roboflow.PipelineReport:
    """
    Rename a single topic during conversion.

    Common use case: Standardizing topic names across different datasets.
    """
    builder = roboflow.TransformBuilder()
    builder = builder.with_topic_rename("/old_camera/image_raw", "/camera/color/image_raw")
    transform_id = builder.build()

    result = (
        roboflow.Roboflow.open([input_path])
        .transform(transform_id)
        .write_to(output_path)
        .run()
    )
    return result


def rename_multiple_topics(input_path: str, output_path: str) -> roboflow.PipelineReport:
    """
    Rename multiple topics using a chained builder.
    """
    builder = roboflow.TransformBuilder()
    builder = (builder
        .with_topic_rename("/camera/left/image_raw", "/camera/hand/left/color")
        .with_topic_rename("/camera/right/image_raw", "/camera/hand/right/color")
        .with_topic_rename("/camera/head/image_raw", "/camera/head/color")
        .with_topic_rename("/joint_states", "/robot/joint_states")
    )
    transform_id = builder.build()

    result = (
        roboflow.Roboflow.open([input_path])
        .transform(transform_id)
        .write_to(output_path)
        .run()
    )
    return result


def rename_with_wildcards(input_path: str, output_path: str) -> roboflow.PipelineReport:
    """
    Rename topics using wildcard patterns.

    The wildcard `*` matches any topic suffix. This is useful for renaming
    entire groups of topics at once.
    """
    builder = roboflow.TransformBuilder()
    # Rename all topics under /old_ns/ to /new_ns/
    builder = builder.with_topic_rename_wildcard("/old_ns/*", "/new_ns/*")
    transform_id = builder.build()

    result = (
        roboflow.Roboflow.open([input_path])
        .transform(transform_id)
        .write_to(output_path)
        .run()
    )
    return result


def rename_message_types(input_path: str, output_path: str) -> roboflow.PipelineReport:
    """
    Rename message types during conversion.

    This is useful when message types have been renamed between
    ROS versions or when working with custom schemas.
    """
    builder = roboflow.TransformBuilder()
    builder = (builder
        .with_type_rename("sensor_msgs/JointState", "my_robot_msgs/JointState")
        .with_type_rename("sensor_msgs/Image", "my_robot_msgs/Image")
    )
    transform_id = builder.build()

    result = (
        roboflow.Roboflow.open([input_path])
        .transform(transform_id)
        .write_to(output_path)
        .run()
    )
    return result


def rename_topic_type_pair(input_path: str, output_path: str) -> roboflow.PipelineReport:
    """
    Rename the message type for a specific topic.

    This allows per-topic type renaming when the same type name
    has different meanings on different topics.
    """
    builder = roboflow.TransformBuilder()
    # Only rename type for this specific topic
    builder = builder.with_topic_type_rename(
        "/custom/data",
        "old_msgs/CustomData",
        "new_msgs/CustomData"
    )
    transform_id = builder.build()

    result = (
        roboflow.Roboflow.open([input_path])
        .transform(transform_id)
        .write_to(output_path)
        .run()
    )
    return result


def main():
    if len(sys.argv) < 3:
        print("Usage: python transforms.py <input_file> <output_file> [mode]")
        print("\nModes:")
        print("  single      Rename a single topic (default)")
        print("  multiple    Rename multiple topics")
        print("  wildcard    Use wildcard patterns")
        print("  type        Rename message types")
        print("  topic-type  Rename type for specific topic")
        print("\nExample:")
        print("  python transforms.py input.bag output.mcap multiple")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    mode = sys.argv[3] if len(sys.argv) > 3 else "single"

    print(f"Converting {input_path} to {output_path}...")
    print(f"Mode: {mode}\n")

    if mode == "single":
        report = rename_single_topic(input_path, output_path)
    elif mode == "multiple":
        report = rename_multiple_topics(input_path, output_path)
    elif mode == "wildcard":
        report = rename_with_wildcards(input_path, output_path)
    elif mode == "type":
        report = rename_message_types(input_path, output_path)
    elif mode == "topic-type":
        report = rename_topic_type_pair(input_path, output_path)
    else:
        print(f"Unknown mode: {mode}")
        sys.exit(1)

    print(f"\nConversion complete!")
    print(f"  Throughput:  {report.average_throughput_mb_s:.2f} MB/s")
    print(f"  Messages:    {report.message_count:,}")
    print(f"  Compression: {report.compression_ratio:.2%} of original")


if __name__ == "__main__":
    main()
