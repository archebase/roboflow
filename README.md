# Roboflow

[![License: MulanPSL-2.0](https://img.shields.io/badge/License-MulanPSL--2.0-blue.svg)](http://license.coscl.org.cn/MulanPSL2)
[![Rust](https://img.shields.io/badge/rust-1.80%2B-orange.svg)](https://www.rust-lang.org)
[![codecov](https://codecov.io/gh/archebase/roboflow/branch/main/graph/badge.svg)](https://codecov.io/gh/archebase/roboflow)

[English](README.md) | [简体中文](README_zh.md)

**Roboflow** is a distributed data transformation pipeline for converting robotics bag/MCAP files to trainable datasets (LeRobot format).

## Features

- **Horizontal Scaling**: Distributed processing with TiKV coordination
- **Schema-Driven Translation**: CDR (ROS1/ROS2), Protobuf, JSON message formats
- **Zero-Copy Allocation**: Arena-based memory efficiency (~22% overhead reduction)
- **Cloud Storage**: Native S3 and Alibaba OSS support for distributed workloads
- **High Throughput**: Parallel chunk processing up to ~1800 MB/s
- **LeRobot Export**: Convert to LeRobot dataset format for robotics learning

## Architecture

Roboflow uses a **Kubernetes-inspired distributed control plane** for fault-tolerant batch processing.

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Control Plane                               │
├─────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │   Scanner    │  │   Reaper     │  │  Finalizer   │              │
│  │  Controller  │  │  Controller  │  │  Controller  │              │
│  │              │  │              │  │              │              │
│  │ • Discover   │  │ • Detect     │  │ • Monitor    │              │
│  │   files      │  │   stale pods │  │   batches    │              │
│  │ • Create     │  │ • Reclaim    │  │ • Trigger    │              │
│  │   jobs       │  │   orphaned   │  │   merge      │              │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘              │
│         │                 │                 │                       │
│         └─────────────────┼─────────────────┘                       │
│                           │                                         │
│                           ▼                                         │
│                    ┌─────────────┐                                  │
│                    │    TiKV     │                                  │
│                    │  (etcd-like │                                  │
│                    │   state)    │                                  │
│                    └─────────────┘                                  │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                          Data Plane                                 │
├─────────────────────────────────────────────────────────────────────┤
│  Worker (pod-abc)    Worker (pod-def)    Worker (pod-xyz)           │
│  • Claim jobs        • Claim jobs        • Claim jobs               │
│  • Send heartbeat    • Send heartbeat    • Send heartbeat           │
│  • Process data      • Process data      • Process data             │
│  • Save checkpoint   • Save checkpoint   • Save checkpoint          │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Patterns

| Kubernetes Concept | Roboflow Equivalent |
|-------------------|---------------------|
| Pod | `Worker` with `pod_id` |
| etcd | TiKV distributed store |
| kubelet heartbeat | `HeartbeatManager` |
| node-controller | `ZombieReaper` |
| Finalizers | `Finalizer` controller |
| Job/CronJob | `JobRecord`, `BatchSpec` |
| State machine | `BatchPhase` (Pending → Discovering → Running → Merging → Complete/Failed) |

## Workspace Structure

| Crate | Purpose |
|-------|---------|
| `roboflow-core` | Error types, registry, values |
| `roboflow-storage` | S3, OSS, Local storage (always available) |
| `roboflow-executor` | Stage-based task executor for distributed pipelines |
| `roboflow-media` | Image and video encoding/decoding for robotics datasets |
| `roboflow-dataset` | KPS, LeRobot, streaming converters, data sources |
| `roboflow-pipeline` | Pipeline execution and stages for dataset processing |
| `roboflow-distributed` | TiKV client, catalog, circuit breaker, worker coordination |

## Quick Start

### Run the Unified Service

```bash
# Set environment variables
export TIKV_PD_ENDPOINTS="127.0.0.1:2379"
export AWS_ACCESS_KEY_ID="your-key"
export AWS_SECRET_ACCESS_KEY="your-secret"

# Run unified service (scanner + worker + finalizer + reaper)
roboflow run
```

### Run Specific Roles

```bash
# Worker only - processes work units
roboflow run --role worker

# Finalizer only - merges completed batches
roboflow run --role finalizer

# With custom pod ID
roboflow run --pod-id my-pod-1
```

### Submit a Conversion Job

```bash
roboflow submit s3://bucket/input.bag --output s3://bucket/output/
```

### Manage Jobs

```bash
roboflow jobs list
roboflow jobs get <job-id>
roboflow batch list
roboflow batch get <batch-id>
```

## Installation

### From Source

```bash
git clone https://github.com/archebase/roboflow.git
cd roboflow
cargo build --release
```

### Requirements

- Rust 1.80+
- TiKV 4.0+ (for distributed coordination)
- ffmpeg (for video encoding in LeRobot datasets)

## Configuration

### LeRobot Dataset Config (`lerobot_config.toml`)

```toml
[dataset]
name = "my_dataset"
fps = 30
robot_type = "stretch"

[[mapping]]
topic = "/camera/image_raw"
name = "observation.images.camera_0"
encoding = "ros1msg"

[[mapping]]
topic = "/joint_states"
name = "observation.joint_state"
encoding = "cdr"
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `TIKV_PD_ENDPOINTS` | TiKV PD endpoints | `127.0.0.1:2379` |
| `AWS_ACCESS_KEY_ID` | AWS access key | - |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | - |
| `AWS_REGION` | AWS region | - |
| `OSS_ACCESS_KEY_ID` | Alibaba OSS key | - |
| `OSS_ACCESS_KEY_SECRET` | Alibaba OSS secret | - |
| `OSS_ENDPOINT` | Alibaba OSS endpoint | - |
| `WORKER_POLL_INTERVAL_SECS` | Job poll interval | `5` |
| `WORKER_MAX_CONCURRENT_JOBS` | Max concurrent jobs | `1` |
| `SCANNER_SCAN_INTERVAL_SECS` | Scan interval | `60` |
| `FINALIZER_POLL_INTERVAL_SECS` | Finalizer poll interval | `30` |

## Development

### Build

```bash
cargo build
```

### Test

```bash
cargo test
```

### Format & Lint

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
```

### Development Infrastructure

Start required services with Docker Compose:

```bash
docker compose up -d       # Start all services (MinIO, TiKV, PD)
docker compose down        # Stop all services
```

**Services:**
| Service | Purpose | Ports |
|---------|---------|-------|
| MinIO | S3-compatible object storage | 9000 (API), 9001 (Console) |
| TiKV | Distributed KV storage | 20160 |
| PD | TiKV placement driver | 2379, 2380 |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.

## License

This project is licensed under the MulanPSL v2 - see the [LICENSE](LICENSE) file for details.

## Related Projects

- [robocodec](https://github.com/archebase/robocodec) - I/O, codecs, arena allocation
- [LeRobot](https://github.com/huggingface/lerobot) - Robotics learning datasets
- [TiKV](https://github.com/tikv/tikv) - Distributed transaction KV store

## Links

- [Issue Tracker](https://github.com/archebase/roboflow/issues)
- [Changelog](CHANGELOG.md)
