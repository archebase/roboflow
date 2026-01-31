# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""
Example: Complete workflow for converting annotated robotics datasets.

This example demonstrates a complete workflow for processing robotics data
with annotation sidecar files. It covers:

1. Discovering data files and their annotations
2. Organizing data by episodes/tasks
3. Converting to standardized formats
4. Merging annotations with converted data
5. Preparing output for KPS dataset format

This is useful when you have:
- Multiple MCAP/BAG files from robot teleoperation
- Corresponding annotation files (task descriptions, labels, language)
- Need to convert everything to a standardized format for ML training

Usage:
    python examples/complete_workflow.py <input_dir> <output_dir>
"""

import sys
import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
import roboflow

# Add parent directory to path for importing roboflow_utils
examples_dir = Path(__file__).parent
if str(examples_dir) not in sys.path:
    sys.path.insert(0, str(examples_dir))

try:
    from roboflow_utils import (
        find_robotics_files,
        find_sidecar_files,
        AnnotationLoader,
        validate_files,
        get_file_info,
        print_report
    )
except ImportError:
    # Fallback implementations if roboflow_utils is not available
    def find_robotics_files(directory, recursive=True, extensions=None):
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

    def find_sidecar_files(data_file, extensions=None):
        if extensions is None:
            extensions = [".json", ".yaml", ".yml", ".toml", ".txt"]
        result = {}
        for ext in extensions:
            sidecar = data_file.with_suffix(ext)
            result[ext] = sidecar if sidecar.exists() else None
        return result

    class AnnotationLoader:
        @staticmethod
        def load_json(path):
            with open(path, "r") as f:
                return json.load(f)

    def validate_files(files):
        valid, invalid = [], []
        for f in files:
            if f.exists() and f.is_file():
                valid.append(f)
            else:
                invalid.append(f)
        return valid, invalid

    def get_file_info(file_path):
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

    def print_report(report, detailed=False):
        if hasattr(report, "file_reports"):
            print("\nBatch Report:")
            print(f"  Total files: {len(report.file_reports)}")
            print(f"  Successful: {report.success_count}")
            print(f"  Failed: {report.failure_count}")
        elif hasattr(report, "crc_enabled"):
            print("\nHyper Pipeline Report:")
            print(f"  Input: {report.input_file}")
            print(f"  Output: {report.output_file}")
            print(f"  Throughput: {report.throughput_mb_s:.2f} MB/s")
        else:
            print("\nPipeline Report:")
            print(f"  Input: {report.input_file}")
            print(f"  Output: {report.output_file}")
            print(f"  Throughput: {report.average_throughput_mb_s:.2f} MB/s")


# =============================================================================
# Data structures
# =============================================================================

@dataclass
class AnnotationData:
    """Container for annotation data from sidecar files."""
    task_name: str = ""
    task_description: str = ""
    scene_name: str = "DefaultScene"
    sub_scene_name: str = "DefaultSubScene"
    language: str = "en"
    labels: List[Dict] = field(default_factory=list)
    metadata: Dict = field(default_factory=dict)

    @classmethod
    def from_json(cls, path: Path) -> "AnnotationData":
        """Load annotation data from JSON file."""
        loader = AnnotationLoader()
        data = loader.load_json(path)

        return cls(
            task_name=data.get("english_task_name", ""),
            task_description=data.get("english_task_description", ""),
            scene_name=data.get("scene_name", "DefaultScene"),
            sub_scene_name=data.get("sub_scene_name", "DefaultSubScene"),
            language=data.get("language", "en"),
            labels=data.get("label_info", {}).get("action_config", []),
            metadata=data
        )

    def to_kps_format(self) -> Dict[str, Any]:
        """Convert annotation to KPS task_info format."""
        return {
            "english_task_name": self.task_name,
            "english_task_description": self.task_description,
            "scene_name": self.scene_name,
            "sub_scene_name": self.sub_scene_name,
            "language": self.language,
            "label_info": {"action_config": self.labels}
        }


@dataclass
class Episode:
    """A single episode with data file and annotations."""
    data_file: Path
    annotation: Optional[AnnotationData] = None
    config: Optional[Dict] = None
    episode_id: str = ""

    @property
    def has_annotation(self) -> bool:
        return self.annotation is not None

    @property
    def file_size_mb(self) -> float:
        return get_file_info(self.data_file)["size_mb"]


@dataclass
class Dataset:
    """A collection of episodes."""
    episodes: List[Episode] = field(default_factory=list)
    metadata: Dict = field(default_factory=dict)

    def add_episode(self, episode: Episode) -> None:
        self.episodes.append(episode)

    def filter_by_scene(self, scene: str) -> "Dataset":
        """Return a new dataset with only episodes from the given scene."""
        filtered = [e for e in self.episodes if e.annotation and e.annotation.scene_name == scene]
        return Dataset(episodes=filtered, metadata=self.metadata.copy())

    def filter_by_task(self, task: str) -> "Dataset":
        """Return a new dataset with only episodes matching the task name."""
        filtered = [e for e in self.episodes if e.annotation and task.lower() in e.annotation.task_name.lower()]
        return Dataset(episodes=filtered, metadata=self.metadata.copy())

    def get_unique_scenes(self) -> List[str]:
        """Get list of unique scene names."""
        scenes = set()
        for ep in self.episodes:
            if ep.annotation:
                scenes.add(ep.annotation.scene_name)
        return sorted(scenes)

    def get_unique_tasks(self) -> List[str]:
        """Get list of unique task names."""
        tasks = set()
        for ep in self.episodes:
            if ep.annotation:
                tasks.add(ep.annotation.task_name)
        return sorted(tasks)

    def summary(self) -> Dict[str, Any]:
        """Get dataset summary."""
        total_size = sum(ep.file_size_mb for ep in self.episodes)
        annotated = sum(1 for ep in self.episodes if ep.has_annotation)

        return {
            "total_episodes": len(self.episodes),
            "annotated_episodes": annotated,
            "total_size_mb": total_size,
            "unique_scenes": self.get_unique_scenes(),
            "unique_tasks": self.get_unique_tasks()
        }


# =============================================================================
# Dataset discovery
# =============================================================================

def discover_dataset(
    data_dir: Path,
    annotation_extensions: List[str] = None
) -> Dataset:
    """
    Discover a dataset from a directory.

    Handles multiple directory structures:
    1. Flat: data_file.mcap + data_file.json
    2. Episode folders: episode_001/episode_001.mcap + episode_001/episode_001.json
    3. Task folders: pick_place/ep1.mcap, pick_place/ep1.json

    Args:
        data_dir: Root directory containing data
        annotation_extensions: List of annotation file extensions to look for

    Returns:
        Dataset object with discovered episodes
    """
    if annotation_extensions is None:
        annotation_extensions = [".json", ".yaml", ".yml"]

    dataset = Dataset()
    data_files = find_robotics_files(data_dir, recursive=True)

    print(f"Found {len(data_files)} data file(s)")

    for data_file in data_files:
        # Look for annotation files
        sidecars = find_sidecar_files(data_file, annotation_extensions)

        annotation = None
        for ext, path in sidecars.items():
            if path is not None:
                try:
                    if ext == ".json":
                        annotation = AnnotationData.from_json(path)
                        break
                    elif ext in [".yaml", ".yml"]:
                        # Could add YAML support
                        pass
                except Exception as e:
                    print(f"  Warning: Could not load {path}: {e}")

        # Generate episode ID from file or directory name
        episode_id = data_file.stem
        if data_file.parent.name != data_dir.name:
            # Use parent folder name as part of ID
            episode_id = f"{data_file.parent.name}_{data_file.stem}"

        dataset.add_episode(Episode(
            data_file=data_file,
            annotation=annotation,
            episode_id=episode_id
        ))

    return dataset


# =============================================================================
# Dataset processing
# =============================================================================

class DatasetProcessor:
    """Process a dataset through various conversion pipelines."""

    def __init__(self, dataset: Dataset, output_dir: Path):
        self.dataset = dataset
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def convert_to_mcap(
        self,
        compression: str = "balanced",
        hyper_mode: bool = False
    ) -> Dict[str, Any]:
        """
        Convert all data files to MCAP format.

        Args:
            compression: Compression preset
            hyper_mode: Use hyper pipeline

        Returns:
            Summary of conversion results
        """
        mcap_dir = self.output_dir / "mcap"
        mcap_dir.mkdir(exist_ok=True)

        results = {
            "successful": 0,
            "failed": 0,
            "episodes": []
        }

        for episode in self.dataset.episodes:
            output_path = mcap_dir / f"{episode.episode_id}.mcap"

            try:
                report = (
                    roboflow.Roboflow.open([str(episode.data_file)])
                    .write_to(str(output_path))
                    .with_compression(getattr(roboflow.CompressionPreset, compression)())
                    .run()
                )

                results["successful"] += 1
                results["episodes"].append({
                    "episode_id": episode.episode_id,
                    "success": True,
                    "output": str(output_path),
                    "compression": report.compression_ratio
                })

            except Exception as e:
                results["failed"] += 1
                results["episodes"].append({
                    "episode_id": episode.episode_id,
                    "success": False,
                    "error": str(e)
                })

        return results

    def export_annotations(self, format: str = "json") -> Path:
        """
        Export all annotations to a single file.

        Args:
            format: Export format ("json" or "csv")

        Returns:
            Path to exported annotations file
        """
        annotated = [ep for ep in self.dataset.episodes if ep.has_annotation]

        if format == "json":
            output_path = self.output_dir / "annotations.json"
            data = {
                "episodes": [
                    {
                        "episode_id": ep.episode_id,
                        "data_file": str(ep.data_file),
                        "annotation": ep.annotation.to_kps_format() if ep.annotation else None
                    }
                    for ep in annotated
                ],
                "metadata": {
                    "total_episodes": len(self.dataset.episodes),
                    "annotated_episodes": len(annotated),
                    "scenes": self.dataset.get_unique_scenes(),
                    "tasks": self.dataset.get_unique_tasks()
                }
            }

            with open(output_path, "w") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

        elif format == "csv":
            import csv
            output_path = self.output_dir / "annotations.csv"

            with open(output_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["episode_id", "scene", "task", "language", "num_labels"])

                for ep in annotated:
                    writer.writerow([
                        ep.episode_id,
                        ep.annotation.scene_name,
                        ep.annotation.task_name,
                        ep.annotation.language,
                        len(ep.annotation.labels)
                    ])

        return output_path

    def create_split(self, train_ratio: float = 0.8, val_ratio: float = 0.1) -> Dict[str, List[str]]:
        """
        Create train/val/test split based on scene or task stratification.

        Args:
            train_ratio: Ratio of training data
            val_ratio: Ratio of validation data

        Returns:
            Dictionary with train/val/test episode IDs
        """
        episodes = self.dataset.episodes
        n = len(episodes)

        n_train = int(n * train_ratio)
        n_val = int(n * val_ratio)

        # Simple sequential split (could be enhanced for stratified splitting)
        train_eps = episodes[:n_train]
        val_eps = episodes[n_train:n_train + n_val]
        test_eps = episodes[n_train + n_val:]

        split = {
            "train": [ep.episode_id for ep in train_eps],
            "val": [ep.episode_id for ep in val_eps],
            "test": [ep.episode_id for ep in test_eps]
        }

        # Save split to file
        split_path = self.output_dir / "split.json"
        with open(split_path, "w") as f:
            json.dump(split, f, indent=2)

        return split


# =============================================================================
# KPS preparation
# =============================================================================

def prepare_kps_structure(dataset: Dataset, output_dir: Path) -> None:
    """
    Prepare the KPS directory structure from a dataset.

    Creates the KPS v1.2 directory structure:
    <output>/<scene>/<sub_scene>/<task>-<size>_<counts>_<duration>/<uuid>/

    Args:
        dataset: Dataset to prepare
        output_dir: Output directory
    """
    for episode in dataset.episodes:
        if not episode.has_annotation:
            print(f"Skipping {episode.episode_id}: no annotation")
            continue

        ann = episode.annotation

        # Create KPS directory structure
        task_dir_name = ann.task_name.replace(" ", "_").lower()
        episode_dir = (
            output_dir / ann.scene_name / ann.sub_scene_name /
            f"{task_dir_name}_approx_100counts_5min" / episode.episode_id
        )
        episode_dir.mkdir(parents=True, exist_ok=True)

        # Create subdirectories
        (episode_dir / "camera" / "video").mkdir(parents=True, exist_ok=True)
        (episode_dir / "camera" / "depth").mkdir(parents=True, exist_ok=True)
        (episode_dir / "parameters").mkdir(parents=True, exist_ok=True)
        (episode_dir / "proprio_stats").mkdir(parents=True, exist_ok=True)
        (episode_dir / "audio").mkdir(parents=True, exist_ok=True)

        # Write task_info.json
        task_info_path = episode_dir / "task_info.json"
        with open(task_info_path, "w") as f:
            json.dump(episode.annotation.to_kps_format(), f, indent=2, ensure_ascii=False)

        # Copy or link data file (would need actual conversion for KPS)
        # For now, just create a placeholder
        data_link = episode_dir / "data_source.txt"
        data_link.write_text(f"Source: {episode.data_file}\n")

        print(f"Prepared: {episode_dir}")


# =============================================================================
# Main workflow
# =============================================================================

def run_complete_workflow(input_dir: Path, output_dir: Path) -> None:
    """
    Run the complete dataset processing workflow.

    Args:
        input_dir: Directory containing raw data and annotations
        output_dir: Directory for processed output
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Complete Dataset Processing Workflow")
    print("=" * 60)

    # Step 1: Discover dataset
    print("\n[Step 1] Discovering dataset...")
    dataset = discover_dataset(input_dir)
    summary = dataset.summary()

    print(f"  Total episodes: {summary['total_episodes']}")
    print(f"  Annotated: {summary['annotated_episodes']}")
    print(f"  Total size: {summary['total_size_mb']:.2f} MB")
    print(f"  Scenes: {', '.join(summary['unique_scenes']) or 'None'}")
    print(f"  Tasks: {', '.join(summary['unique_tasks']) or 'None'}")

    # Step 2: Export annotations
    print("\n[Step 2] Exporting annotations...")
    processor = DatasetProcessor(dataset, output_dir)
    annotations_path = processor.export_annotations(format="json")
    print(f"  Exported to: {annotations_path}")

    # Step 3: Create train/val/test split
    print("\n[Step 3] Creating train/val/test split...")
    split = processor.create_split()
    print(f"  Train: {len(split['train'])} episodes")
    print(f"  Val: {len(split['val'])} episodes")
    print(f"  Test: {len(split['test'])} episodes")

    # Step 4: Convert to MCAP (optional)
    print("\n[Step 4] Converting to MCAP format...")
    conversion_results = processor.convert_to_mcap(compression="balanced")
    print(f"  Successful: {conversion_results['successful']}")
    print(f"  Failed: {conversion_results['failed']}")

    # Step 5: Prepare KPS structure
    print("\n[Step 5] Preparing KPS directory structure...")
    kps_dir = output_dir / "kps_dataset"
    prepare_kps_structure(dataset, kps_dir)

    print("\n" + "=" * 60)
    print("Workflow complete!")
    print(f"Output directory: {output_dir}")
    print("=" * 60)


def main():
    if len(sys.argv) < 3:
        print("Usage: python complete_workflow.py <input_dir> <output_dir>")
        print("\nExample:")
        print("  python complete_workflow.py ./raw_data ./processed")
        print()
        print("Expected input structure:")
        print("  raw_data/")
        print("  ├── episode_001/")
        print("  │   ├── episode_001.mcap")
        print("  │   └── episode_001.json")
        print("  ├── episode_002/")
        print("  │   ├── episode_002.mcap")
        print("  │   └── episode_002.json")
        print()
        print("Output structure:")
        print("  processed/")
        print("  ├── mcap/              # Converted MCAP files")
        print("  ├── annotations.json   # All annotations")
        print("  ├── split.json         # Train/val/test split")
        print("  └── kps_dataset/       # KPS directory structure")
        sys.exit(1)

    input_dir = Path(sys.argv[1])
    output_dir = Path(sys.argv[2])

    if not input_dir.exists():
        print(f"Error: Input directory not found: {input_dir}")
        sys.exit(1)

    run_complete_workflow(input_dir, output_dir)


if __name__ == "__main__":
    main()
