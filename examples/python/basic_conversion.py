# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""
Example: Basic file conversion with roboflow.

This example demonstrates the simplest way to convert between robotics data formats
using the roboflow Python bindings.

Usage:
    python examples/basic_conversion.py <input_file> <output_file>
"""

import sys
import roboflow


def convert_single_file(input_path: str, output_path: str) -> roboflow.PipelineReport:
    """
    Convert a single robotics data file to another format.

    Args:
        input_path: Path to input file (.bag or .mcap)
        output_path: Path to output file

    Returns:
        PipelineReport with conversion statistics
    """
    result = (
        roboflow.Roboflow.open([input_path])
        .write_to(output_path)
        .run()
    )
    return result


def convert_with_compression(input_path: str, output_path: str) -> roboflow.PipelineReport:
    """
    Convert with custom compression settings.

    Args:
        input_path: Path to input file
        output_path: Path to output file

    Returns:
        PipelineReport with conversion statistics
    """
    result = (
        roboflow.Roboflow.open([input_path])
        .write_to(output_path)
        .with_compression(roboflow.CompressionPreset.slow())
        .run()
    )
    return result


def main():
    if len(sys.argv) < 3:
        print("Usage: python basic_conversion.py <input_file> <output_file>")
        print("\nExample:")
        print("  python basic_conversion.py input.bag output.mcap")
        print("\nCompression options:")
        print("  - CompressionPreset.fast()    # Fastest, larger files")
        print("  - CompressionPreset.balanced() # Balanced compression")
        print("  - CompressionPreset.slow()    # Best compression, slower")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    # Basic conversion
    print(f"Converting {input_path} to {output_path}...")
    report = convert_single_file(input_path, output_path)

    print(f"\nConversion complete!")
    print(f"  Input size:      {report.input_size_bytes / 1024 / 1024:.2f} MB")
    print(f"  Output size:     {report.output_size_bytes / 1024 / 1024:.2f} MB")
    print(f"  Compression:     {report.compression_ratio:.2%} of original")
    print(f"  Throughput:      {report.average_throughput_mb_s:.2f} MB/s")
    print(f"  Messages:        {report.message_count:,}")
    print(f"  Duration:        {report.duration_seconds:.2f} seconds")


if __name__ == "__main__":
    main()
