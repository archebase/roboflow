# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2025-01-18

### Added
- Initial open source release
- Multi-format message codec supporting CDR (ROS1/ROS2), Protobuf, and JSON
- MCAP and ROS1 bag file format support (read/write)
- Schema parsers for ROS `.msg`, ROS2 IDL, and OMG IDL
- Python bindings via PyO3
- Command-line tools: convert, extract, inspect, schema, search, extract_sample
- LeRobot dataset format support (HDF5 and Parquet)
- Cross-language API with Rust and Python
- Data transformation capabilities (topic renaming, type normalization)

### License
- Changed license to MulanPSL v2 for open source release

[Unreleased]: https://github.com/archebase/robocodec/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/archebase/robocodec/releases/tag/v0.1.0
