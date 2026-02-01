# Distributed Data Transformation System Design

This document describes the high-level design for Roboflow's distributed data transformation system, targeting **10 Gbps throughput** for converting robotics bag/MCAP files to training datasets (LeRobot v2.1).

## Table of Contents

- [Overview](#overview)
- [Requirements](#requirements)
- [Architecture](#architecture)
- [Component Design](#component-design)
- [Data Flow](#data-flow)
- [Scaling Strategy](#scaling-strategy)
- [Failure Handling](#failure-handling)
- [Implementation Roadmap](#implementation-roadmap)

## Overview

### Problem Statement

Robotics teams generate large volumes of recording data (bag/MCAP files) that need to be converted to ML-ready dataset formats for training. Manual conversion is:
- **Slow**: Sequential processing cannot keep up with data generation
- **Error-prone**: No coordination means duplicate work or missed files
- **Resource-intensive**: Video encoding is CPU/GPU heavy

### Solution

A distributed pipeline that:
1. **Discovers** new files in S3/OSS automatically
2. **Distributes** work across GPU-enabled workers
3. **Converts** to LeRobot v2.1 (and other formats) with GPU acceleration
4. **Tracks** progress with exactly-once semantics

### Key Metrics

| Metric | Target | Notes |
|--------|--------|-------|
| Throughput | 10 Gbps (1.25 GB/s) | ~1125 files/hour at 4GB each |
| File size | ~4 GB | One episode per file |
| Latency | < 2 min/file | End-to-end processing |
| Recovery | < 5 min | From worker failure |

## Requirements

### Functional Requirements

1. **Input Support**
   - ROS bag files (ROS1 format)
   - MCAP files (ROS2/generic)
   - S3 and OSS storage backends

2. **Output Support**
   - LeRobot v2.1 (initial target)
   - Extensible to KPS, custom formats

3. **Operations**
   - Automatic file discovery
   - Distributed job coordination
   - Progress tracking and resume
   - Duplicate detection

### Non-Functional Requirements

1. **Throughput**: 10 Gbps sustained
2. **Availability**: 99.9% (worker failures handled automatically)
3. **Consistency**: Exactly-once processing semantics
4. **Scalability**: Linear scaling with worker count

## Architecture

### System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           Control Plane (TiKV Cluster)                          │
│                                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌───────────────────┐   │
│  │   Job Queue  │  │  Checkpoints │  │   Catalog    │  │ Worker Registry   │   │
│  │  (Pending/   │  │  (Episode-   │  │  (Episodes/  │  │ (Heartbeats/      │   │
│  │  Processing/ │  │   level)     │  │   Metadata)  │  │  Leader Election) │   │
│  │  Complete)   │  │              │  │              │  │                   │   │
│  └──────────────┘  └──────────────┘  └──────────────┘  └───────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                    ┌───────────────────┼───────────────────┐
                    │                   │                   │
┌───────────────────▼───┐   ┌──────────▼───────────┐   ┌──▼────────────────────┐
│     Scanner Pod       │   │     Worker Pod 1     │   │     Worker Pod N      │
│  ┌─────────────────┐  │   │  ┌───────────────┐   │   │  ┌───────────────┐    │
│  │ Leader Election │  │   │  │ Prefetch Queue│   │   │  │ Prefetch Queue│    │
│  │ File Discovery  │  │   │  │ (2 slots)     │   │   │  │ (2 slots)     │    │
│  │ Job Creation    │  │   │  └───────┬───────┘   │   │  └───────┬───────┘    │
│  └─────────────────┘  │   │          │           │   │          │            │
└───────────────────────┘   │  ┌───────▼───────┐   │   │  ┌───────▼───────┐    │
                            │  │   Pipeline    │   │   │  │   Pipeline    │    │
                            │  │   Executor    │   │   │  │   Executor    │    │
                            │  │  ┌─────────┐  │   │   │  │  ┌─────────┐  │    │
                            │  │  │ Decode  │  │   │   │  │  │ Decode  │  │    │
                            │  │  │ Align   │  │   │   │  │  │ Align   │  │    │
                            │  │  │ NVENC   │  │   │   │  │  │ NVENC   │  │    │
                            │  │  │ Upload  │  │   │   │  │  │ Upload  │  │    │
                            │  │  └─────────┘  │   │   │  │  └─────────┘  │    │
                            │  └───────────────┘   │   │  └───────────────┘    │
                            └──────────────────────┘   └───────────────────────┘
                                        │                           │
                    ┌───────────────────┴───────────────────────────┘
                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Object Storage (S3/OSS)                            │
│  ┌───────────────────────┐                    ┌─────────────────────────────┐  │
│  │    Input Bucket       │                    │      Output Bucket          │  │
│  │  *.bag / *.mcap       │  ═══════════════▶  │  LeRobot v2.1 Dataset       │  │
│  └───────────────────────┘                    └─────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Component Overview

| Component | Purpose | Scaling |
|-----------|---------|---------|
| **Scanner** | File discovery, job creation | Single leader (HA standby) |
| **Worker** | Job execution, data transformation | Horizontal (20-24 for 10 Gbps) |
| **TiKV** | Coordination, metadata storage | 3-5 node cluster |
| **S3/OSS** | Input/output storage | Managed service |

## Component Design

### Scanner

The Scanner discovers new files and creates jobs for processing.

```
┌─────────────────────────────────────────────────────────────────┐
│                         Scanner Flow                             │
│                                                                  │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐  │
│  │ Acquire  │───▶│  List    │───▶│  Filter  │───▶│  Create  │  │
│  │ Leader   │    │  Files   │    │  Dupes   │    │  Jobs    │  │
│  │ Lock     │    │  (S3)    │    │  (TiKV)  │    │  (TiKV)  │  │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘  │
│       │                                                │        │
│       │                                                │        │
│       └────────────────── Sleep ◀──────────────────────┘        │
│                         (60 sec)                                │
└─────────────────────────────────────────────────────────────────┘
```

**Key Design Decisions:**

1. **Leader Election**: Only one scanner runs at a time (via TiKV lock)
2. **Deduplication**: Hash(path + size + config) prevents duplicate jobs
3. **Batch Operations**: Jobs created in batches of 100 for efficiency

**Configuration:**

```rust
pub struct ScannerConfig {
    /// S3/OSS prefix to scan
    pub input_prefix: String,
    
    /// Scan interval
    pub scan_interval: Duration,  // 60s default
    
    /// File pattern filter
    pub file_pattern: Option<glob::Pattern>,  // "*.mcap"
    
    /// Configuration hash for versioning
    pub config_hash: String,
}
```

### Worker

Workers claim and process jobs with GPU acceleration.

```
┌─────────────────────────────────────────────────────────────────┐
│                    Worker Internal Architecture                  │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                   Prefetch Pipeline                      │    │
│  │  ┌─────────────┐     ┌─────────────┐                    │    │
│  │  │  Slot 1     │     │  Slot 2     │                    │    │
│  │  │ (downloading│     │ (queued)    │                    │    │
│  │  │  next job)  │     │             │                    │    │
│  │  └──────┬──────┘     └─────────────┘                    │    │
│  └─────────┼───────────────────────────────────────────────┘    │
│            │                                                     │
│  ┌─────────▼───────────────────────────────────────────────┐    │
│  │              Active Job Processing                       │    │
│  │                                                          │    │
│  │  ┌──────────┐   ┌──────────┐   ┌──────────┐            │    │
│  │  │ Decode   │──▶│  Align   │──▶│ NVENC    │            │    │
│  │  │ (rayon)  │   │ (frames) │   │ Encode   │            │    │
│  │  └──────────┘   └──────────┘   └────┬─────┘            │    │
│  │                                      │                  │    │
│  │  ┌──────────┐                       │                  │    │
│  │  │ Parquet  │◀──────────────────────┘                  │    │
│  │  │ Writer   │                                          │    │
│  │  └────┬─────┘                                          │    │
│  │       │                                                │    │
│  │  ┌────▼─────┐                                          │    │
│  │  │ Multipart│──▶ S3/OSS                                │    │
│  │  │ Upload   │                                          │    │
│  │  └──────────┘                                          │    │
│  └──────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

**Key Design Decisions:**

1. **Prefetch Pipeline**: Download next job while processing current (hides I/O latency)
2. **GPU Encoding**: NVENC hardware encoder for 10x faster video encoding
3. **Episode-Level Checkpoints**: 4GB files process in ~60s; per-frame checkpoints add overhead
4. **Multipart Upload**: Async upload with 8 parallel parts

**Configuration:**

```rust
pub struct WorkerConfig {
    /// Prefetch slots (download ahead)
    pub prefetch_slots: usize,  // 2
    
    /// Parallel download connections
    pub download_connections: usize,  // 16
    
    /// NVENC sessions per GPU
    pub nvenc_sessions: usize,  // 2
    
    /// Upload parallelism
    pub upload_parts: usize,  // 8
    
    /// Heartbeat interval
    pub heartbeat_interval: Duration,  // 30s
}
```

### Pipeline Executor

The pipeline processes a single file through all transformation stages.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Pipeline Stages                                     │
│                                                                              │
│  Input: episode.bag (4GB)                                                    │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ Stage 1: DECODE (CPU, parallel)                                       │   │
│  │ - Parse bag/MCAP format                                               │   │
│  │ - Deserialize messages (CDR/Protobuf)                                 │   │
│  │ - Output: Raw message stream                                          │   │
│  │ - Time: ~30s                                                          │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                              │                                               │
│                              ▼                                               │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ Stage 2: ALIGN (CPU)                                                  │   │
│  │ - Timestamp alignment across topics                                   │   │
│  │ - Frame assembly (state + action + images)                            │   │
│  │ - Output: AlignedFrame stream                                         │   │
│  │ - Time: ~10s                                                          │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                              │                                               │
│                              ▼                                               │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ Stage 3: ENCODE (GPU, NVENC)                                          │   │
│  │ - RGB frames → H.264/H.265 video                                      │   │
│  │ - Parallel cameras (2 NVENC sessions)                                 │   │
│  │ - Output: MP4 files per camera                                        │   │
│  │ - Time: ~15s                                                          │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                              │                                               │
│                              ▼                                               │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ Stage 4: WRITE (CPU)                                                  │   │
│  │ - Parquet file with frame data                                        │   │
│  │ - Metadata JSON files                                                 │   │
│  │ - Time: ~5s                                                           │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                              │                                               │
│                              ▼                                               │
│  Output: LeRobot v2.1 dataset                                               │
│  ├── data/chunk-000/episode_000000.parquet                                  │
│  ├── videos/chunk-000/observation.images.*/episode_000000.mp4               │
│  └── meta/{info,episodes,tasks,stats}.json                                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### TiKV Schema

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           TiKV Key-Value Schema                              │
│                                                                              │
│  Namespace: roboflow/                                                        │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ Jobs                                                                 │    │
│  │ Key:   roboflow/jobs/{job_id}                                       │    │
│  │ Value: JobRecord { status, source_key, pod_id, attempts, ... }      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ Checkpoints                                                          │    │
│  │ Key:   roboflow/checkpoints/{job_id}                                │    │
│  │ Value: CheckpointState { stage, parquet_uploaded, videos_uploaded } │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ Heartbeats                                                           │    │
│  │ Key:   roboflow/heartbeats/{pod_id}                                 │    │
│  │ Value: HeartbeatRecord { status, active_jobs, last_beat, ... }      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ Locks                                                                │    │
│  │ Key:   roboflow/locks/{resource}                                    │    │
│  │ Value: LockRecord { owner, expires_at, ... }                        │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ Catalog (Episodes)                                                   │    │
│  │ Key:   roboflow/catalog/episodes/{episode_id}                       │    │
│  │ Value: EpisodeMetadata { frames, duration, cameras, ... }           │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow

### Job Lifecycle

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            Job State Machine                                 │
│                                                                              │
│                              ┌──────────┐                                   │
│                              │ Pending  │                                   │
│                              └────┬─────┘                                   │
│                                   │ Worker claims (CAS)                     │
│                                   ▼                                         │
│                              ┌──────────┐                                   │
│                         ┌───▶│Processing│◀───┐                              │
│                         │    └────┬─────┘    │                              │
│                         │         │          │ Retry (< max_attempts)       │
│                         │         │          │                              │
│              Zombie     │    ┌────┴────┐     │                              │
│              Reaper     │    ▼         ▼     │                              │
│                         │ Success   Failure ─┘                              │
│                         │    │         │                                    │
│                         │    ▼         │ Retry exhausted                    │
│                         │ ┌──────┐     ▼                                    │
│                         └─│Failed│  ┌──────┐                                │
│                           └──────┘  │ Dead │                                │
│                                     └──────┘                                │
│                              ┌──────────┐                                   │
│                              │Complete  │                                   │
│                              └──────────┘                                   │
│                                                                              │
│  States:                                                                     │
│  - Pending: Waiting for worker                                              │
│  - Processing: Worker actively processing                                   │
│  - Complete: Successfully processed and uploaded                            │
│  - Failed: Temporary failure, will retry                                    │
│  - Dead: Permanent failure (max retries exceeded)                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Exactly-Once Semantics

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Exactly-Once Processing Guarantees                      │
│                                                                              │
│  1. Job Deduplication (Scanner)                                             │
│     └─▶ Hash(path + size + config_hash) → unique job ID                     │
│     └─▶ Same file + same config = same job ID (idempotent)                  │
│                                                                              │
│  2. Atomic Job Claiming (Worker)                                            │
│     └─▶ TiKV CAS: status Pending → Processing only if unchanged             │
│     └─▶ Only one worker can claim a job                                     │
│                                                                              │
│  3. Idempotent Output Paths                                                 │
│     └─▶ s3://output/{config_hash}/{job_id}/episode_*.parquet                │
│     └─▶ Re-processing overwrites same location                              │
│                                                                              │
│  4. Atomic Completion (Worker)                                              │
│     └─▶ TiKV transaction: checkpoint delete + job complete + catalog update │
│     └─▶ All-or-nothing commit                                               │
│                                                                              │
│  Result: Each input file is processed exactly once per configuration        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Output Structure (LeRobot v2.1)

```
s3://output-bucket/lerobot-dataset/
├── data/
│   └── chunk-000/
│       ├── episode_000000.parquet    # Frame data (state, action, timestamps)
│       ├── episode_000001.parquet
│       └── ...
├── videos/
│   └── chunk-000/
│       ├── observation.images.cam0/
│       │   ├── episode_000000.mp4    # H.264 encoded video
│       │   └── ...
│       └── observation.images.cam1/
│           ├── episode_000000.mp4
│           └── ...
└── meta/
    ├── info.json                      # Dataset info (fps, features, etc.)
    ├── episodes.json                  # Episode index
    ├── tasks.json                     # Task definitions
    └── stats.json                     # Feature statistics

Parquet Schema:
┌────────────────────┬──────────┬─────────────────────────────────┐
│ Column             │ Type     │ Description                     │
├────────────────────┼──────────┼─────────────────────────────────┤
│ episode_index      │ int64    │ Episode number                  │
│ frame_index        │ int64    │ Frame within episode            │
│ index              │ int64    │ Global frame index              │
│ timestamp          │ float64  │ Timestamp in seconds            │
│ observation.state.N│ float32  │ Joint positions (per dimension) │
│ action.N           │ float32  │ Actions (per dimension)         │
│ task_index         │ int64    │ Task identifier                 │
└────────────────────┴──────────┴─────────────────────────────────┘
```

## Scaling Strategy

### Throughput Analysis

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Pipeline Stage Throughput Analysis                       │
│                                                                              │
│  Target: 10 Gbps = 1.25 GB/s = 4.5 TB/hour                                  │
│  File size: 4 GB                                                             │
│  Files/hour: ~1125                                                           │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ Stage           │ Time/File │ Throughput │ Bottleneck               │    │
│  ├─────────────────┼───────────┼────────────┼──────────────────────────┤    │
│  │ S3 Download     │ 3-8 sec   │ 5-10 Gbps  │ Network, parallel conns  │    │
│  │ Decode          │ 30-60 sec │ 2-4 GB/s   │ CPU cores                │    │
│  │ Align           │ 5-10 sec  │ 10+ GB/s   │ Memory bandwidth         │    │
│  │ Video Encode    │ 15-30 sec │ 100-200MB/s│ GPU NVENC sessions       │    │
│  │ Parquet Write   │ 3-5 sec   │ 500+ MB/s  │ CPU (Polars)             │    │
│  │ S3 Upload       │ 3-8 sec   │ 5-10 Gbps  │ Network, multipart       │    │
│  ├─────────────────┼───────────┼────────────┼──────────────────────────┤    │
│  │ TOTAL           │ 60-90 sec │            │ Video encoding (GPU)     │    │
│  │ With prefetch   │ 45-60 sec │            │ I/O hidden by overlap    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Per-Worker Throughput:                                                      │
│  - 4 GB / 60 sec = 67 MB/s = 536 Mbps                                       │
│                                                                              │
│  Workers for 10 Gbps:                                                        │
│  - 10000 Mbps / 536 Mbps ≈ 19 workers                                       │
│  - Recommendation: 20-24 workers (headroom for variance)                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Horizontal Scaling

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Scaling Dimensions                                    │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ Dimension          │ Mechanism           │ Limit                    │    │
│  ├────────────────────┼─────────────────────┼──────────────────────────┤    │
│  │ Worker count       │ K8s HPA             │ TiKV coordination (~100) │    │
│  │ Internal parallel  │ rayon thread pool   │ CPU cores per node       │    │
│  │ Video encoding     │ NVENC sessions      │ 2-3 per GPU              │    │
│  │ Download speed     │ Parallel connections│ S3 throttling (~100)     │    │
│  │ Upload speed       │ Multipart parts     │ 10000 parts per upload   │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Scaling Formula:                                                            │
│  - Throughput (Gbps) ≈ Workers × 0.5 Gbps                                   │
│  - 10 Gbps → 20 workers                                                      │
│  - 50 Gbps → 100 workers (requires TiKV tuning)                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Resource Requirements

```yaml
# Worker Pod Specification (for 10 Gbps cluster)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: roboflow-worker
spec:
  replicas: 24
  template:
    spec:
      containers:
      - name: worker
        image: roboflow-worker:latest
        resources:
          requests:
            cpu: "8"
            memory: "32Gi"
            nvidia.com/gpu: "1"
          limits:
            cpu: "16"
            memory: "64Gi"
            nvidia.com/gpu: "1"
        env:
        - name: PREFETCH_SLOTS
          value: "2"
        - name: DOWNLOAD_CONNECTIONS
          value: "16"
        - name: NVENC_SESSIONS
          value: "2"
        - name: UPLOAD_PARTS
          value: "8"
      nodeSelector:
        cloud.google.com/gke-accelerator: nvidia-tesla-t4
```

## Failure Handling

### Failure Modes and Recovery

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Failure Recovery Matrix                                 │
│                                                                              │
│  ┌─────────────────────┬───────────────────────────────────────────────┐    │
│  │ Failure Mode        │ Recovery Strategy                              │    │
│  ├─────────────────────┼───────────────────────────────────────────────┤    │
│  │ Worker crash        │ ZombieReaper detects stale heartbeat (>60s)   │    │
│  │                     │ Job marked Failed, another worker claims       │    │
│  │                     │ Resume from checkpoint if exists               │    │
│  ├─────────────────────┼───────────────────────────────────────────────┤    │
│  │ Worker OOM          │ Job fails, retry on different worker           │    │
│  │                     │ Reduce parallel cameras if persistent          │    │
│  ├─────────────────────┼───────────────────────────────────────────────┤    │
│  │ TiKV unavailable    │ Circuit breaker opens after 3 failures         │    │
│  │                     │ Workers pause, local state preserved           │    │
│  │                     │ Auto-retry when TiKV recovers                  │    │
│  ├─────────────────────┼───────────────────────────────────────────────┤    │
│  │ S3 download failure │ Exponential backoff retry (3 attempts)        │    │
│  │                     │ Job fails if persistent                        │    │
│  ├─────────────────────┼───────────────────────────────────────────────┤    │
│  │ S3 upload failure   │ Retry with multipart resume                    │    │
│  │                     │ Checkpoint preserves encoding progress         │    │
│  ├─────────────────────┼───────────────────────────────────────────────┤    │
│  │ Corrupt input file  │ Job marked Dead after max_attempts (3)        │    │
│  │                     │ Alert for manual review                        │    │
│  ├─────────────────────┼───────────────────────────────────────────────┤    │
│  │ Scanner crash       │ Another scanner acquires leadership            │    │
│  │                     │ No jobs lost (TiKV is source of truth)        │    │
│  └─────────────────────┴───────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Checkpoint Strategy

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Episode-Level Checkpoint Design                          │
│                                                                              │
│  Rationale:                                                                  │
│  - 4GB file processes in ~60 seconds                                         │
│  - Frame-level checkpoints add overhead with minimal benefit                │
│  - Episode-level checkpoints are sufficient for recovery                    │
│                                                                              │
│  Checkpoint Stages:                                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Downloaded → Decoded → Aligned → Encoded → ParquetUploaded →       │    │
│  │  VideosUploading(progress) → Complete                                │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Checkpoint Schema:                                                          │
│  ```rust                                                                     │
│  pub struct EpisodeCheckpoint {                                              │
│      pub job_id: String,                                                     │
│      pub stage: ProcessingStage,                                             │
│      pub parquet_uploaded: bool,                                             │
│      pub videos_uploaded: Vec<String>,  // Camera names                      │
│      pub multipart_ids: HashMap<String, String>,  // For resume             │
│      pub updated_at: i64,                                                    │
│  }                                                                           │
│  ```                                                                         │
│                                                                              │
│  Recovery Behavior:                                                          │
│  - Stage < Encoded: Restart from beginning                                  │
│  - Stage = Encoded: Resume upload only                                      │
│  - Stage = VideosUploading: Resume multipart uploads                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Circuit Breaker

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Circuit Breaker Pattern                               │
│                                                                              │
│  Purpose: Prevent cascade failures when TiKV is overloaded                  │
│                                                                              │
│  States:                                                                     │
│  ┌──────────┐   3 failures    ┌──────────┐   timeout    ┌──────────┐       │
│  │  Closed  │ ───────────────▶│   Open   │ ────────────▶│Half-Open │       │
│  │(normal)  │                 │(blocking)│              │(testing) │       │
│  └────┬─────┘                 └──────────┘              └────┬─────┘       │
│       │                             ▲                        │             │
│       │ success                     │ failure                │ success    │
│       └─────────────────────────────┴────────────────────────┘             │
│                                                                              │
│  Configuration:                                                              │
│  ```rust                                                                     │
│  pub struct CircuitConfig {                                                  │
│      pub failure_threshold: u32,    // 3                                    │
│      pub success_threshold: u32,    // 2                                    │
│      pub timeout: Duration,         // 30s                                  │
│  }                                                                           │
│  ```                                                                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Implementation Roadmap

### Phase 1: Pipeline Integration (Current)

**Goal**: Complete Worker.process_job() with existing components

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Tasks:                                                                      │
│  □ Integrate LerobotWriter with Worker                                       │
│  □ Add streaming download from S3                                            │
│  □ Wire up checkpoint save/restore                                           │
│  □ Add multipart upload for outputs                                          │
│                                                                              │
│  Deliverable: End-to-end single-worker processing                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Phase 2: Prefetch Pipeline

**Goal**: Hide I/O latency with prefetching

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Tasks:                                                                      │
│  □ Implement PrefetchQueue with 2 slots                                      │
│  □ Add parallel range-request downloader (16 connections)                    │
│  □ Background download while processing                                      │
│  □ Memory-mapped file handling for large downloads                           │
│                                                                              │
│  Deliverable: 40% throughput improvement from I/O overlap                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Phase 3: GPU Acceleration (NVENC)

**Goal**: Hardware-accelerated video encoding

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Tasks:                                                                      │
│  □ NVENC encoder integration (h264_nvenc)                                    │
│  □ Parallel camera encoding (2 sessions/GPU)                                 │
│  □ Quality/speed preset tuning                                               │
│  □ Fallback to CPU encoding when GPU unavailable                             │
│                                                                              │
│  Deliverable: 10x video encoding speedup                                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Phase 4: Production Hardening

**Goal**: Reliability and observability

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Tasks:                                                                      │
│  □ Prometheus metrics export                                                 │
│  □ Grafana dashboard                                                         │
│  □ Alert rules for failures and throughput                                   │
│  □ Load testing at 10 Gbps                                                   │
│  □ Chaos testing (worker/TiKV failures)                                      │
│                                                                              │
│  Deliverable: Production-ready system                                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Phase 5: Multi-Format Support

**Goal**: Extensible dataset format system

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Tasks:                                                                      │
│  □ DatasetFormat trait for pluggable writers                                 │
│  □ KPS v1.2 format support                                                   │
│  □ Custom format registration API                                            │
│  □ Per-job format configuration                                              │
│                                                                              │
│  Deliverable: Support for multiple output formats                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Monitoring

### Key Metrics

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Observability Metrics                               │
│                                                                              │
│  Throughput Metrics:                                                         │
│  - roboflow_throughput_bytes_total (Counter)                                │
│  - roboflow_throughput_gbps (Gauge)                                          │
│  - roboflow_files_processed_total (Counter)                                  │
│                                                                              │
│  Latency Metrics:                                                            │
│  - roboflow_job_duration_seconds (Histogram)                                │
│  - roboflow_stage_duration_seconds{stage} (Histogram)                       │
│  - roboflow_download_duration_seconds (Histogram)                           │
│  - roboflow_upload_duration_seconds (Histogram)                             │
│                                                                              │
│  Queue Metrics:                                                              │
│  - roboflow_jobs_pending (Gauge)                                             │
│  - roboflow_jobs_processing (Gauge)                                          │
│  - roboflow_jobs_failed_total (Counter)                                     │
│  - roboflow_jobs_dead_total (Counter)                                       │
│                                                                              │
│  Resource Metrics:                                                           │
│  - roboflow_worker_cpu_usage (Gauge)                                         │
│  - roboflow_worker_memory_bytes (Gauge)                                     │
│  - roboflow_gpu_utilization (Gauge)                                          │
│  - roboflow_nvenc_sessions_active (Gauge)                                   │
│                                                                              │
│  Health Metrics:                                                             │
│  - roboflow_workers_active (Gauge)                                           │
│  - roboflow_tikv_rpc_duration_seconds (Histogram)                           │
│  - roboflow_circuit_breaker_state (Gauge)                                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Dashboard Layout

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Roboflow Distributed Dashboard                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────────────┐  ┌──────────────────────────┐                 │
│  │  Cluster Throughput      │  │  Job Queue               │                 │
│  │  ━━━━━━━━━━━━━━━━━━━━    │  │  ━━━━━━━━━━━━━━━━━━━━    │                 │
│  │  Current: 9.7 Gbps       │  │  Pending:    2,341       │                 │
│  │  Target:  10.0 Gbps      │  │  Processing: 23          │                 │
│  │  [█████████░] 97%        │  │  Failed:     12          │                 │
│  └──────────────────────────┘  └──────────────────────────┘                 │
│                                                                              │
│  ┌──────────────────────────┐  ┌──────────────────────────┐                 │
│  │  Workers                 │  │  Processing Latency      │                 │
│  │  ━━━━━━━━━━━━━━━━━━━━    │  │  ━━━━━━━━━━━━━━━━━━━━    │                 │
│  │  Active: 23/24           │  │  p50: 52s                │                 │
│  │  Prefetching: 46         │  │  p95: 68s                │                 │
│  │  GPU Util: 78%           │  │  p99: 85s                │                 │
│  └──────────────────────────┘  └──────────────────────────┘                 │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Throughput Over Time (24h)                                          │    │
│  │  ▲                                                                   │    │
│  │  │    ╭──────╮      ╭─────────────────────────╮                      │    │
│  │  │   ╱        ╲    ╱                           ╲                     │    │
│  │  │  ╱          ╲──╱                             ╲                    │    │
│  │  │ ╱                                                                 │    │
│  │  └────────────────────────────────────────────────────────────▶      │    │
│  │    00:00    06:00    12:00    18:00    24:00                         │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Appendix

### A. Related Documents

- [ARCHITECTURE.md](ARCHITECTURE.md) - Core architecture overview
- [PIPELINE.md](PIPELINE.md) - Pipeline implementation details
- [MEMORY.md](MEMORY.md) - Memory management
- [ROADMAP_ALIGNMENT.md](ROADMAP_ALIGNMENT.md) - GitHub issue alignment with roadmap

### B. External Dependencies

| Component | Version | Purpose |
|-----------|---------|---------|
| TiKV | 7.x | Distributed coordination |
| FFmpeg | 6.x | Video encoding (with NVENC) |
| Polars | 0.41 | Parquet writing |
| tokio | 1.x | Async runtime |

### C. Glossary

| Term | Definition |
|------|------------|
| **Episode** | A single recording session (one bag/MCAP file) |
| **Chunk** | LeRobot's grouping of episodes (chunk-000, chunk-001, ...) |
| **NVENC** | NVIDIA's hardware video encoder |
| **CAS** | Compare-And-Swap (atomic operation for job claiming) |
| **Prefetch** | Downloading next job while processing current |
