# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""
Utility functions for working with roboflow Python bindings.

This module provides helper functions for common robotics data processing tasks.
"""

import sys
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
import roboflow


# =============================================================================
# File discovery utilities
# =============================================================================

def find_robotics_files(
    directory: Path,
    recursive: bool = True,
    extensions: Optional[List[str]] = None
) -> List[Path]:
    """
    Find all robotics data files in a directory.

    Args:
        directory: Directory to search
        recursive: Whether to search recursively (default: True)
        extensions: File extensions to include (default: .bag, .mcap)

    Returns:
        Sorted list of file paths
    """
    if extensions is None:
        extensions = [".bag", ".mcap"]

    files = []
    if recursive:
        for ext in extensions:
            files.extend(directory.rglob(f"*{ext}"))
    else:
        for ext in extensions:
            files.extend(directory.glob(f"*{ext}"))
    return sorted(files)


def find_sidecar_files(data_file: Path, extensions: List[str] = None) -> Dict[str, Optional[Path]]:
    """
    Find sidecar files for a robotics data file.

    Sidecar files are additional files that accompany the main data file,
    such as annotations, configurations, or metadata.

    Args:
        data_file: Path to the main data file
        extensions: List of sidecar extensions to look for

    Returns:
        Dictionary mapping extension to Path (or None if not found)
    """
    if extensions is None:
        extensions = [".json", ".yaml", ".yml", ".toml", ".txt"]

    result = {}
    for ext in extensions:
        sidecar = data_file.with_suffix(ext)
        result[ext] = sidecar if sidecar.exists() else None

    return result


# =============================================================================
# Conversion utilities
# =============================================================================

def convert_with_options(
    input_paths: List[str],
    output_path: str,
    compression: str = "balanced",
    hyper_mode: bool = False,
    chunk_size: Optional[int] = None,
    threads: Optional[int] = None,
    transform_id: Optional[int] = None
):
    """
    Convert files with various options.

    Args:
        input_paths: List of input file paths
        output_path: Output file or directory path
        compression: Compression preset ("fast", "balanced", or "slow")
        hyper_mode: Whether to use hyper pipeline
        chunk_size: Optional chunk size in bytes
        threads: Optional number of threads
        transform_id: Optional transform ID from TransformBuilder

    Returns:
        PipelineReport, HyperPipelineReport, or BatchReport
    """
    # Get compression preset
    presets = {
        "fast": roboflow.CompressionPreset.fast(),
        "balanced": roboflow.CompressionPreset.balanced(),
        "slow": roboflow.CompressionPreset.slow(),
    }
    preset = presets.get(compression, roboflow.CompressionPreset.balanced())

    # Build pipeline
    builder = roboflow.Roboflow.open(input_paths)

    if transform_id is not None:
        builder = builder.transform(transform_id)

    builder = builder.write_to(output_path)

    if hyper_mode:
        builder = builder.hyper_mode()

    builder = builder.with_compression(preset)

    if chunk_size is not None:
        builder = builder.with_chunk_size(chunk_size)

    if threads is not None:
        builder = builder.with_threads(threads)

    return builder.run()


# =============================================================================
# Transform builders
# =============================================================================

def build_standard_transforms(rename_map: Dict[str, str]) -> int:
    """
    Build a transform pipeline with topic renames.

    Args:
        rename_map: Dictionary mapping old topic names to new names

    Returns:
        Transform ID
    """
    builder = roboflow.TransformBuilder()
    for old, new in rename_map.items():
        builder = builder.with_topic_rename(old, new)
    return builder.build()


def build_prefix_transforms(old_prefix: str, new_prefix: str) -> int:
    """
    Build a transform that renames all topics with a given prefix.

    Args:
        old_prefix: Original topic prefix (e.g., "/old_ns/")
        new_prefix: New topic prefix (e.g., "/new_ns/")

    Returns:
        Transform ID
    """
    builder = roboflow.TransformBuilder()
    pattern = f"{old_prefix}*"
    target = f"{new_prefix}*"
    builder = builder.with_topic_rename_wildcard(pattern, target)
    return builder.build()


# =============================================================================
# Batch processing utilities
# =============================================================================

class BatchProcessor:
    """
    Process multiple files in batches with progress tracking.

    Example:
        >>> processor = BatchProcessor(output_dir="./output")
        >>> processor.add_files(["file1.bag", "file2.bag"])
        >>> results = processor.run()
    """

    def __init__(
        self,
        output_dir: Path,
        batch_size: int = 10,
        compression: str = "balanced",
        hyper_mode: bool = False
    ):
        self.output_dir = Path(output_dir)
        self.batch_size = batch_size
        self.compression = compression
        self.hyper_mode = hyper_mode
        self.files: List[Path] = []

    def add_files(self, files: List[Path]) -> None:
        """Add files to process."""
        self.files.extend(files)

    def add_directory(self, directory: Path) -> None:
        """Add all robotics files from a directory."""
        self.files.extend(find_robotics_files(directory))

    def run(self) -> List[Dict[str, Any]]:
        """
        Process all files in batches.

        Returns:
            List of result dictionaries
        """
        results = []
        total = len(self.files)

        for i in range(0, total, self.batch_size):
            batch = self.files[i:i + self.batch_size]
            batch_paths = [str(f) for f in batch]

            print(f"Processing batch {i // self.batch_size + 1}/{(total + self.batch_size - 1) // self.batch_size}")
            print(f"  Files: {len(batch_paths)}")

            try:
                report = convert_with_options(
                    batch_paths,
                    str(self.output_dir),
                    compression=self.compression,
                    hyper_mode=self.hyper_mode
                )

                results.append({
                    "batch_start": i,
                    "batch_end": i + len(batch),
                    "success": True,
                    "report": report
                })

            except Exception as e:
                results.append({
                    "batch_start": i,
                    "batch_end": i + len(batch),
                    "success": False,
                    "error": str(e)
                })

        return results


# =============================================================================
# Annotation utilities
# =============================================================================

class AnnotationLoader:
    """
    Load and manage annotation sidecar files.

    Common annotation formats:
    - JSON: task_info.json, annotations.json
    - YAML: config.yaml
    - TOML: settings.toml
    """

    @staticmethod
    def load_json(annotation_path: Path) -> Dict[str, Any]:
        """Load JSON annotation file."""
        import json
        with open(annotation_path, "r") as f:
            return json.load(f)

    @staticmethod
    def load_yaml(annotation_path: Path) -> Dict[str, Any]:
        """Load YAML annotation file."""
        try:
            import yaml
            with open(annotation_path, "r") as f:
                return yaml.safe_load(f)
        except ImportError:
            raise ImportError("PyYAML is required for YAML files: pip install pyyaml")

    @staticmethod
    def load_toml(annotation_path: Path) -> Dict[str, Any]:
        """Load TOML annotation file."""
        try:
            import tomli
            with open(annotation_path, "rb") as f:
                return tomli.load(f)
        except ImportError:
            try:
                import tomllib
                with open(annotation_path, "rb") as f:
                    return tomllib.load(f)
            except ImportError:
                raise ImportError("Python 3.11+ or tomli is required for TOML files")


def find_annotation_files(data_file: Path) -> Dict[str, Optional[Path]]:
    """
    Find annotation files for a robotics data file.

    Looks for files with the same base name but different extensions.

    Args:
        data_file: Path to the main data file

    Returns:
        Dictionary mapping type to Path
    """
    base = data_file.stem
    parent = data_file.parent

    return {
        "json": parent / f"{base}.json" if (parent / f"{base}.json").exists() else None,
        "yaml": parent / f"{base}.yaml" if (parent / f"{base}.yaml").exists() else None,
        "yml": parent / f"{base}.yml" if (parent / f"{base}.yml").exists() else None,
        "toml": parent / f"{base}.toml" if (parent / f"{base}.toml").exists() else None,
    }


# =============================================================================
# Validation utilities
# =============================================================================

def validate_files(files: List[Path]) -> Tuple[List[Path], List[Path]]:
    """
    Validate that files exist and are readable.

    Args:
        files: List of file paths to validate

    Returns:
        Tuple of (valid_files, invalid_files)
    """
    valid = []
    invalid = []

    for f in files:
        if f.exists() and f.is_file():
            valid.append(f)
        else:
            invalid.append(f)

    return valid, invalid


def get_file_info(file_path: Path) -> Dict[str, Any]:
    """
    Get information about a robotics data file.

    Args:
        file_path: Path to the file

    Returns:
        Dictionary with file information
    """
    stat = file_path.stat()

    return {
        "path": str(file_path),
        "name": file_path.name,
        "stem": file_path.stem,
        "extension": file_path.suffix,
        "size_bytes": stat.st_size,
        "size_mb": stat.st_size / 1024 / 1024,
        "exists": file_path.exists(),
    }


# =============================================================================
# CLI helpers
# =============================================================================

def print_report(report, detailed: bool = False) -> None:
    """
    Print a conversion report in a formatted way.

    Args:
        report: PipelineReport, HyperPipelineReport, or BatchReport
        detailed: Whether to print detailed information
    """
    if hasattr(report, "file_reports"):  # BatchReport
        print("\nBatch Report:")
        print(f"  Total files: {len(report.file_reports)}")
        print(f"  Successful: {report.success_count}")
        print(f"  Failed: {report.failure_count}")
        print(f"  Duration: {report.total_duration_seconds:.2f}s")

        if detailed:
            for fr in report.file_reports:
                status = "✓" if fr.success else "✗"
                print(f"    {status} {fr.input_path}")

    elif hasattr(report, "crc_enabled"):  # HyperPipelineReport
        print("\nHyper Pipeline Report:")
        print(f"  Input: {report.input_file}")
        print(f"  Output: {report.output_file}")
        print(f"  Throughput: {report.throughput_mb_s:.2f} MB/s")
        print(f"  Messages: {report.message_count:,}")
        print(f"  Compression: {report.compression_ratio:.2%}")

    else:  # PipelineReport
        print("\nPipeline Report:")
        print(f"  Input: {report.input_file}")
        print(f"  Output: {report.output_file}")
        print(f"  Throughput: {report.average_throughput_mb_s:.2f} MB/s")
        print(f"  Messages: {report.message_count:,}")
        print(f"  Compression: {report.compression_ratio:.2%}")
        print(f"  Threads: {report.threads_used}")


# =============================================================================
# Progress tracking
# =============================================================================

class ProgressTracker:
    """Simple progress tracker for long-running operations."""

    def __init__(self, total: int, description: str = "Processing"):
        self.total = total
        self.current = 0
        self.description = description

    def update(self, n: int = 1) -> None:
        """Update progress by n items."""
        self.current += n
        self._print()

    def _print(self) -> None:
        percent = self.current / self.total * 100
        bar_length = 40
        filled = int(bar_length * self.current / self.total)
        bar = "█" * filled + "░" * (bar_length - filled)
        print(f"\r{self.description}: [{bar}] {self.current}/{self.total} ({percent:.1f}%)", end="")
        if self.current >= self.total:
            print()  # New line when complete


if __name__ == "__main__":
    # Run a simple demo
    print("Roboflow Utils")
    print(f"Python: {sys.version}")
    print(f"Roboflow: {roboflow.__version__}")
