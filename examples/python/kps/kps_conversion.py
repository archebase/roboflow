#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""
Example: Convert MCAP/BAG files to KPS dataset format with annotation sidecar files.

This script demonstrates using the roboflow KPS Python package to convert
robotics data with annotation sidecar files to the KPS dataset format.

Usage:
    python examples/python/kps/kps_conversion.py <data_dir> <output_dir> [config.toml]

Or use the CLI directly:
    python -m examples.python.kps.cli convert <input> <output>
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from kps import (
    load_config,
    create_default_config,
    save_config,
    KpsConverter
)


def print_usage():
    """Print usage information."""
    print("KPS Dataset Conversion with Annotation Sidecar Files")
    print()
    print("Usage:")
    print("  python kps_conversion.py <input> <output> [config.toml]")
    print()
    print("Commands:")
    print("  kps_conversion.py <data_dir> <output_dir> [config]  # Convert dataset")
    print("  kps_conversion.py --generate-config [path]           # Generate config template")
    print("  kps_conversion.py --generate-task-info [path]       # Generate task_info template")
    print()
    print("Examples:")
    print("  # Convert single file with annotation")
    print("  python kps_conversion.py episode_001.mcap ./output")
    print()
    print("  # Convert dataset directory")
    print("  python kps_conversion.py ./data ./kps_output config.toml")
    print()
    print("  # Generate templates")
    print("  python kps_conversion.py --generate-config ./kps_config.toml")
    print("  python kps_conversion.py --generate-task-info ./task_info.json")
    print()
    print("Expected data structure:")
    print("  data/")
    print("  ├── episode_001/")
    print("  │   ├── episode_001.mcap")
    print("  │   └── episode_001.json")
    print("  ├── episode_002/")
    print("  │   ├── episode_002.mcap")
    print("  │   └── episode_002.json")


def generate_config(output_path: Path = None) -> int:
    """Generate a default KPS config file."""
    if output_path is None:
        output_path = Path("kps_config.toml")

    config = create_default_config()
    save_config(config, output_path)
    print(f"Generated config: {output_path}")
    return 0


def generate_task_info(output_path: Path = None) -> int:
    """Generate a default task_info.json file."""
    if output_path is None:
        output_path = Path("task_info.json")

    import json
    template = {
        "episode_id": "000000",
        "scene_name": "DefaultScene",
        "sub_scene_name": "DefaultSubScene",
        "english_task_name": "Pick and Place",
        "english_task_description": "Pick up an object and place it in a target location.",
        "language": "en",
        "label_info": {
            "action_config": []
        }
    }

    with open(output_path, "w") as f:
        json.dump(template, f, indent=2)

    print(f"Generated task_info template: {output_path}")
    return 0


def convert_dataset(
    data_dir: Path,
    output_dir: Path,
    config_path: Path = None
) -> int:
    """Convert a dataset directory to KPS format."""
    import json

    # Load config
    if config_path and config_path.exists():
        print(f"Loading config: {config_path}")
        config = load_config(config_path)
    else:
        print("Using default config")
        config = create_default_config()

    # Create converter
    converter = KpsConverter(config, use_cli=True)

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Find and convert episodes
    results = {"total": 0, "successful": 0, "failed": 0, "episodes": []}

    for entry in sorted(data_dir.iterdir()):
        if not entry.is_dir():
            # Check for direct file
            if entry.suffix in [".mcap", ".bag"]:
                episodes = [_create_episode_from_file(entry)]
            else:
                continue
        else:
            episodes = _find_episodes_in_dir(entry)

        if not episodes:
            continue

        for ep_info in episodes:
            results["total"] += 1
            episode_id = ep_info["episode_id"]
            print(f"\n[{results['total']}] Converting: {episode_id}")
            print(f"  Data:   {ep_info['data_file']}")

            # Load annotation if present
            task_info = None
            if ep_info["annotation"]:
                print(f"  Annot:  {ep_info['annotation']}")
                with open(ep_info["annotation"], "r") as f:
                    task_info = json.load(f)

            # Create episode output directory
            episode_output = output_dir / episode_id

            # Convert
            result = converter.convert_episode(
                ep_info["data_file"],
                episode_output,
                task_info
            )

            result["episode_id"] = episode_id
            results["episodes"].append(result)

            if result.get("success"):
                results["successful"] += 1
                print(f"  Success")
            else:
                results["failed"] += 1
                print(f"  Failed: {result.get('error', 'Unknown error')}")

    # Print summary
    print("\n" + "=" * 60)
    print("Conversion Summary")
    print("=" * 60)
    print(f"Total:      {results['total']}")
    print(f"Successful: {results['successful']}")
    print(f"Failed:     {results['failed']}")

    return 0 if results["failed"] == 0 else 1


def _find_episodes_in_dir(episode_dir: Path) -> list:
    """Find episodes in a directory."""
    mcap_files = list(episode_dir.glob("*.mcap"))
    bag_files = list(episode_dir.glob("*.bag"))

    if not mcap_files and not bag_files:
        return []

    data_file = mcap_files[0] if mcap_files else bag_files[0]
    return [_create_episode_from_file(data_file)]


def _create_episode_from_file(data_file: Path) -> dict:
    """Create episode info from a data file."""
    # Look for annotation file (same name, .json extension)
    annotation = None
    json_file = data_file.with_suffix(".json")
    if json_file.exists():
        annotation = json_file

    # Generate episode ID
    episode_id = data_file.stem

    return {
        "data_file": data_file,
        "annotation": annotation,
        "episode_id": episode_id
    }


def main(argv: list = None) -> int:
    """Main entry point."""
    if argv is None:
        argv = sys.argv[1:]

    if len(argv) < 1:
        print_usage()
        return 1

    # Handle special commands
    if argv[0] == "--generate-config":
        output_path = Path(argv[1]) if len(argv) > 1 else None
        return generate_config(output_path)

    if argv[0] == "--generate-task-info":
        output_path = Path(argv[1]) if len(argv) > 1 else None
        return generate_task_info(output_path)

    # Normal conversion
    if len(argv) < 2:
        print_usage()
        return 1

    input_path = Path(argv[0])
    output_dir = Path(argv[1])
    config_path = Path(argv[2]) if len(argv) > 2 else None

    if not input_path.exists():
        print(f"Error: Input not found: {input_path}")
        return 1

    return convert_dataset(input_path, output_dir, config_path)


if __name__ == "__main__":
    sys.exit(main())
