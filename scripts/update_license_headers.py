#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

# REUSE-IgnoreStart

"""
Update license headers from verbose format to REUSE SPDX format.

Old format (9 lines):
// Copyright (c) 2026 ArcheBase
// Roboflow is licensed under Mulan PSL v2.
...

New format (3 lines):
// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0
"""

import re
from pathlib import Path

# Old license header pattern (for .rs files)
OLD_HEADER_RUST = r"""// Copyright \(c\) 2026 ArcheBase
// Roboflow is licensed under Mulan PSL v2\.
// You can use this software according to the terms and conditions of the Mulan PSL v2\.
// You may obtain a copy of Mulan PSL v2 at:
//     http://license\.coscl\.org\.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE\."""

# New SPDX header for Rust
NEW_HEADER_RUST = """// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0"""

# Old license header pattern (for .py files)
OLD_HEADER_PYTHON = r"""# Copyright \(c\) 2026 ArcheBase
# Roboflow is licensed under Mulan PSL v2\.
# You can use this software according to the terms and conditions of the Mulan PSL v2\.
# You may obtain a copy of Mulan PSL v2 at:
#     http://license\.coscl\.org\.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE\."""

# New SPDX header for Python
NEW_HEADER_PYTHON = """# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0"""


def update_rust_file(filepath: Path) -> bool:
    """Update a Rust file's license header."""
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    # Check if file already has SPDX header
    if "SPDX-License-Identifier" in content:
        print(f"  ✓ {filepath}: Already has SPDX header")
        return False

    # Replace old header with new SPDX header
    new_content = re.sub(OLD_HEADER_RUST, NEW_HEADER_RUST, content, count=1)

    if new_content != content:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(new_content)
        print(f"  ✓ {filepath}: Updated")
        return True
    else:
        print(f"  - {filepath}: No changes needed")
        return False


def update_python_file(filepath: Path) -> bool:
    """Update a Python file's license header."""
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    # Check if file already has SPDX header
    if "SPDX-License-Identifier" in content:
        print(f"  ✓ {filepath}: Already has SPDX header")
        return False

    # Replace old header with new SPDX header
    new_content = re.sub(OLD_HEADER_PYTHON, NEW_HEADER_PYTHON, content, count=1)

    if new_content != content:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(new_content)
        print(f"  ✓ {filepath}: Updated")
        return True
    else:
        print(f"  - {filepath}: No changes needed")
        return False


def main():
    """Update all source files with REUSE SPDX headers."""
    root = Path(__file__).parent.parent

    # Count updated files
    updated_count = 0

    # Process Rust files
    print("\nProcessing Rust files:")
    rust_dirs = ["src", "robocodec/src", "tests", "benches", "examples"]
    for dir_name in rust_dirs:
        dir_path = root / dir_name
        if not dir_path.exists():
            continue

        for rs_file in dir_path.rglob("*.rs"):
            if update_rust_file(rs_file):
                updated_count += 1

    # Process Python files
    print("\nProcessing Python files:")
    python_dirs = ["python"]
    for dir_name in python_dirs:
        dir_path = root / dir_name
        if not dir_path.exists():
            continue

        for py_file in dir_path.rglob("*.py"):
            if update_python_file(py_file):
                updated_count += 1

    print(f"\n✓ Updated {updated_count} files with SPDX headers")


if __name__ == "__main__":
    main()

# REUSE-IgnoreEnd
