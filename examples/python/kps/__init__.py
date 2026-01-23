# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""
KPS dataset conversion using roboflow Python bindings.

This package provides tools for converting robotics data (MCAP/BAG files)
to the KPS dataset format with annotation sidecar files.
"""

from .config import KpsConfig, load_config
from .reader import McapReader, MessageIterator
from .writer import KpsWriter, create_kps_structure

__all__ = [
    "KpsConfig",
    "load_config",
    "McapReader",
    "MessageIterator",
    "KpsWriter",
    "create_kps_structure",
]
