# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""
CLI for KPS dataset conversion.

Command-line interface for converting robotics data to KPS format.
"""

import sys
import json
from pathlib import Path
from typing import Optional

from .config import load_config, create_default_config, save_config, KpsConfig
from .converter import convert_single, convert_dataset, KpsConverter


def cmd_convert(args: list) -> int:
    """Convert MCAP/BAG to KPS format."""
    if len(args) < 2:
        print("Usage: kps convert <input> <output> [config.toml]")
        print()
        print("Examples:")
        print("  kps convert episode.mcap ./output")
        print("  kps convert episode.mcap ./output config.toml")
        print("  kps convert ./data_dir ./output  # For dataset directories")
        return 1

    input_path = Path(args[0])
    output_dir = Path(args[1])
    config_path = Path(args[2]) if len(args) > 2 else None

    if not input_path.exists():
        print(f"Error: Input not found: {input_path}")
        return 1

    # Load config
    if config_path and config_path.exists():
        config = load_config(config_path)
    else:
        config = create_default_config()
        if config_path:
            print(f"Config not found, using defaults")

    # Check if input is a file or directory
    if input_path.is_file():
        # Single file conversion
        print(f"Converting {input_path}...")

        # Look for annotation file
        annotation_path = input_path.with_suffix(".json")
        task_info = None
        if annotation_path.exists():
            with open(annotation_path, "r") as f:
                task_info = json.load(f)
            print(f"Found annotation: {annotation_path}")

        result = convert_single(
            input_path,
            output_dir,
            config_path,
            task_info
        )

        if result.get("success"):
            print(f"Success! Output: {result.get('episode_dir', result.get('output_dir', output_dir))}")
            return 0
        else:
            print(f"Conversion failed: {result.get('error', 'Unknown error')}")
            return 1

    else:
        # Dataset directory conversion
        print(f"Converting dataset in {input_path}...")

        results = convert_dataset(input_path, output_dir, config_path)

        successful = sum(1 for r in results if r.get("success"))
        failed = len(results) - successful

        print(f"\nConversion complete:")
        print(f"  Successful: {successful}")
        print(f"  Failed: {failed}")

        if failed > 0:
            print("\nFailed episodes:")
            for r in results:
                if not r.get("success"):
                    print(f"  - {r.get('episode_id', '?')}: {r.get('error', 'Unknown')}")
            return 1

        return 0


def cmd_config(args: list) -> int:
    """Generate or validate KPS configuration."""
    if len(args) < 1:
        print("Usage: kps config <generate|validate> [file]")
        return 1

    command = args[0]
    file_path = Path(args[1]) if len(args) > 1 else None

    if command == "generate":
        output_path = file_path or Path("kps_config.toml")

        config = create_default_config()

        try:
            save_config(config, output_path)
            print(f"Generated config: {output_path}")
            return 0
        except ImportError as e:
            print(f"Error: {e}")
            return 1

    elif command == "validate":
        if not file_path or not file_path.exists():
            print("Usage: kps config validate <config.toml>")
            return 1

        try:
            config = load_config(file_path)
            print(f"Config loaded successfully:")
            print(f"  Dataset: {config.dataset.name}")
            print(f"  FPS: {config.dataset.fps}")
            print(f"  Robot type: {config.dataset.robot_type}")
            print(f"  Mappings: {len(config.mappings)}")
            return 0
        except Exception as e:
            print(f"Validation failed: {e}")
            return 1

    else:
        print(f"Unknown config command: {command}")
        return 1


def cmd_task_info(args: list) -> int:
    """Generate or validate task_info.json."""
    if len(args) < 1:
        print("Usage: kps task-info <generate|validate> [file]")
        return 1

    command = args[0]
    file_path = Path(args[1]) if len(args) > 1 else Path("task_info.json")

    if command == "generate":
        # Generate template
        template = {
            "episode_id": "000000",
            "scene_name": "DefaultScene",
            "sub_scene_name": "DefaultSubScene",
            "english_task_name": "Pick and Place",
            "english_task_description": "Pick up an object and place it in a target location.",
            "language": "en",
            "label_info": {
                "action_config": [
                    {
                        "start_frame": 0,
                        "end_frame": 100,
                        "action_id": "pick_up",
                        "action_name": "Pick Up Object"
                    }
                ]
            }
        }

        with open(file_path, "w") as f:
            json.dump(template, f, indent=2)

        print(f"Generated task_info template: {file_path}")
        return 0

    elif command == "validate":
        if not file_path.exists():
            print(f"File not found: {file_path}")
            return 1

        with open(file_path, "r") as f:
            data = json.load(f)

        # Validate required fields
        required = ["episode_id", "scene_name", "sub_scene_name", "english_task_name"]
        missing = [f for f in required if f not in data]

        if missing:
            print(f"Missing required fields: {', '.join(missing)}")
            return 1

        print(f"task_info.json is valid!")
        print(f"  Episode: {data['episode_id']}")
        print(f"  Scene: {data['scene_name']} / {data['sub_scene_name']}")
        print(f"  Task: {data['english_task_name']}")
        print(f"  Actions: {len(data.get('label_info', {}).get('action_config', []))}")
        return 0

    else:
        print(f"Unknown task-info command: {command}")
        return 1


def main(argv: Optional[list] = None) -> int:
    """Main CLI entry point."""
    if argv is None:
        argv = sys.argv[1:]

    if len(argv) < 1:
        print("KPS Dataset Conversion Tool")
        print()
        print("Usage: kps <command> [args...]")
        print()
        print("Commands:")
        print("  convert <input> <output> [config]  Convert MCAP/BAG to KPS format")
        print("  config generate [file]           Generate config template")
        print("  config validate <file>            Validate config file")
        print("  task-info generate [file]         Generate task_info template")
        print("  task-info validate <file>         Validate task_info file")
        print()
        print("Examples:")
        print("  kps convert episode.mcap ./output")
        print("  kps convert ./data ./kps_output config.toml")
        print("  kps config generate ./my_config.toml")
        print("  kps task-info generate ./task_info.json")
        return 1

    command = argv[0]
    args = argv[1:]

    if command == "convert":
        return cmd_convert(args)
    elif command == "config":
        return cmd_config(args)
    elif command in ("task-info", "task_info"):
        return cmd_task_info(args)
    else:
        print(f"Unknown command: {command}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
