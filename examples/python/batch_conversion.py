# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""
Example: Batch conversion of multiple robotics data files.

This example demonstrates how to convert multiple files at once,
which is useful when processing entire datasets.

Usage:
    python examples/batch_conversion.py <input_dir> <output_dir>
"""

import sys
from pathlib import Path
from typing import List
import roboflow


def find_data_files(directory: Path, extensions: List[str] = None) -> List[Path]:
    """
    Find all robotics data files in a directory.

    Args:
        directory: Directory to search
        extensions: File extensions to include (default: .bag, .mcap)

    Returns:
        List of file paths
    """
    if extensions is None:
        extensions = [".bag", ".mcap"]

    files = []
    for ext in extensions:
        files.extend(directory.rglob(f"*{ext}"))
    return sorted(files)


def convert_batch(input_files: List[str], output_dir: str) -> roboflow.BatchReport:
    """
    Convert multiple files at once.

    When multiple input files are provided, roboflow processes them
    in parallel and outputs to the specified directory.

    Args:
        input_files: List of input file paths
        output_dir: Output directory path

    Returns:
        BatchReport with statistics for all conversions
    """
    result = (
        roboflow.Roboflow.open(input_files)
        .write_to(output_dir)
        .run()
    )
    return result


def convert_with_hyper_mode(input_files: List[str], output_dir: str) -> roboflow.HyperPipelineReport:
    """
    Convert multiple files using the hyper pipeline for maximum throughput.

    The hyper pipeline is a 7-stage pipeline optimized for high performance,
    achieving ~1800 MB/s on modern hardware.

    Args:
        input_files: List of input file paths
        output_dir: Output directory path

    Returns:
        HyperPipelineReport with conversion statistics
    """
    result = (
        roboflow.Roboflow.open(input_files)
        .write_to(output_dir)
        .hyper_mode()
        .run()
    )
    return result


def main():
    if len(sys.argv) < 3:
        print("Usage: python batch_conversion.py <input_dir> <output_dir> [--hyper]")
        print("\nExample:")
        print("  python batch_conversion.py ./bags ./mcaps")
        print("\nOptions:")
        print("  --hyper    Use hyper pipeline for maximum throughput")
        sys.exit(1)

    input_dir = Path(sys.argv[1])
    output_dir = sys.argv[2]
    use_hyper = "--hyper" in sys.argv

    if not input_dir.exists():
        print(f"Error: Input directory not found: {input_dir}")
        sys.exit(1)

    # Find all data files
    print(f"Searching for data files in {input_dir}...")
    input_files = find_data_files(input_dir)

    if not input_files:
        print("No .bag or .mcap files found in input directory.")
        sys.exit(1)

    print(f"Found {len(input_files)} file(s):")
    for f in input_files:
        print(f"  - {f}")

    # Convert
    input_paths = [str(f) for f in input_files]
    print(f"\nConverting to {output_dir}...")

    if use_hyper:
        report = convert_with_hyper_mode(input_paths, output_dir)
        print(f"\nHyper Pipeline complete!")
        print(f"  Throughput:  {report.throughput_mb_s:.2f} MB/s")
        print(f"  Messages:    {report.message_count:,}")
        print(f"  Compression: {report.compression_ratio:.2%} of original")
    else:
        report = convert_batch(input_paths, output_dir)
        print(f"\nBatch conversion complete!")
        print(f"  Successful:  {report.success_count}/{len(input_files)}")
        print(f"  Failed:      {report.failure_count}/{len(input_files)}")
        print(f"  Duration:    {report.total_duration_seconds:.2f} seconds")

        # Show per-file results
        print("\nPer-file results:")
        for file_report in report.file_reports:
            status = "✓" if file_report.success else "✗"
            print(f"  {status} {file_report.input_path}")
            if file_report.error:
                print(f"      Error: {file_report.error}")


if __name__ == "__main__":
    main()
