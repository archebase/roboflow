# Roadmap Alignment Analysis

This document aligns GitHub issues with the implementation roadmap defined in [DISTRIBUTED_DESIGN.md](DISTRIBUTED_DESIGN.md).

## Executive Summary

The GitHub issues use a legacy phase numbering (Phases 1-10) from earlier planning. The new design document defines 5 phases optimized for 10 Gbps throughput. This document maps existing issues to the new roadmap and identifies gaps.

### Key Findings

| Status | Count | Notes |
|--------|-------|-------|
| **Aligned & Complete** | 22 | Foundation work (storage, TiKV, LeRobot) |
| **Aligned & Open** | 8 | Match new roadmap phases |
| **Phase Mismatch** | 3 | Need renumbering |
| **Missing Issues** | 5 | Need to be created |
| **Future Scope** | 2 | Beyond current roadmap |

## Phase Mapping

### New Roadmap vs Legacy Issue Phases

| New Phase | Description | Legacy Issue Phases |
|-----------|-------------|---------------------|
| **Phase 1** | Pipeline Integration | Phases 7.1, 7.2, 9.1 |
| **Phase 2** | Prefetch Pipeline | (No existing issues) |
| **Phase 3** | GPU Acceleration | Phase 8 |
| **Phase 4** | Production Hardening | Phases 6.2, 7.1, 7.2 |
| **Phase 5** | Multi-Format Support | (No existing issues) |

---

## Completed Work (Closed Issues)

These issues are complete and form the foundation for the new roadmap.

### Storage Layer (Foundation) ✅

| Issue | Title | Status |
|-------|-------|--------|
| #10 | [Phase 1.1] Add core dependencies for storage abstraction | ✅ Closed |
| #11 | [Phase 1.2] Define Storage trait and error types | ✅ Closed |
| #23 | [Phase 1.3] Implement LocalStorage backend | ✅ Closed |
| #24 | [Phase 1.4] Implement URL/path parsing for storage backends | ✅ Closed |
| #25 | [Phase 1.5] Create StorageFactory for backend instantiation | ✅ Closed |

### Cloud Storage (Foundation) ✅

| Issue | Title | Status |
|-------|-------|--------|
| #12 | [Phase 2.2] Implement multipart upload for large files | ✅ Closed |
| #13 | [Phase 2.1] Implement OSS/S3 backend using object_store | ✅ Closed |
| #14 | [Phase 2.3] Add retry logic and error handling | ✅ Closed |
| #15 | [Phase 2.4] Implement cached storage backend | ✅ Closed |
| #45 | [Phase 6.1] Add streaming S3 reader with range requests | ✅ Closed |
| #46 | [Phase 6.2] Add parallel multipart uploads | ✅ Closed |

### LeRobot Integration (Foundation) ✅

| Issue | Title | Status |
|-------|-------|--------|
| #16 | [Phase 3.1] Refactor LeRobotWriter to accept Storage backend | ✅ Closed |
| #17 | [Phase 3.2] Implement parallel episode upload | ✅ Closed |
| #19 | [Phase 5] Frame-level checkpoint with TiKV | ✅ Closed |
| #26 | [Phase 5.1] Add storage support to StreamingDatasetConverter | ✅ Closed |
| #27 | [Phase 5.2] Update CLI to accept cloud URLs | ✅ Closed |

### Distributed Coordination (Foundation) ✅

| Issue | Title | Status |
|-------|-------|--------|
| #40 | [Phase 4.1] Add TiKV client and define distributed schema | ✅ Closed |
| #41 | [Phase 4.2] Implement distributed lock manager with TTL | ✅ Closed |
| #42 | [Phase 4.3] Implement Scanner actor with leader election | ✅ Closed |
| #43 | [Phase 4.4] Implement Worker loop with job claiming | ✅ Closed |
| #44 | [Phase 4.5] Implement heartbeat and zombie detection | ✅ Closed |

---

## Open Issues Alignment

### Phase 1: Pipeline Integration (Current Priority)

**Goal**: Complete Worker.process_job() with existing components

| Issue | Title | Alignment | Action |
|-------|-------|-----------|--------|
| #47 | [Phase 7.1] Integrate pipeline with checkpoint hooks | ✅ **Direct match** | Rename to Phase 1.1 |
| #48 | [Phase 7.2] Add graceful shutdown handling | ✅ **Direct match** | Rename to Phase 1.2 |
| #18 | [Phase 9.1] Implement long-running Worker Deployment | ⚠️ **Partial match** | Split: pipeline logic → Phase 1, K8s → Phase 4 |
| — | Integrate LerobotWriter with Worker | ❌ **Missing** | Create new issue |
| — | Wire up checkpoint save/restore in pipeline | ❌ **Missing** | Create new issue |

**Codebase Verification**:
- `Worker.process_job()` is a placeholder (TODO: issue #35 referenced)
- Checkpoint infrastructure exists in `roboflow-distributed`
- LerobotWriter exists in `roboflow-dataset`
- Storage layer is complete

### Phase 2: Prefetch Pipeline

**Goal**: Hide I/O latency with prefetching

| Issue | Title | Alignment | Action |
|-------|-------|-----------|--------|
| — | Implement PrefetchQueue with 2 slots | ❌ **Missing** | Create new issue |
| — | Add parallel range-request downloader | ❌ **Missing** | Create new issue |
| — | Background download while processing | ❌ **Missing** | Create new issue |

**Codebase Verification**:
- Streaming reader exists (`StreamingOssReader`)
- Prefetch not implemented (TODO noted in streaming.rs)
- Range requests supported in OSS backend

### Phase 3: GPU Acceleration (NVENC)

**Goal**: Hardware-accelerated video encoding

| Issue | Title | Alignment | Action |
|-------|-------|-----------|--------|
| #49 | [Phase 8] Add NVENC GPU video encoding support | ✅ **Direct match** | Rename to Phase 3 |

**Codebase Verification**:
- NVENC detection exists in `roboflow-dataset/src/lerobot/hardware.rs`
- `check_encoder_available("h264_nvenc")` implemented
- Hardware backend enum includes `Nvenc`
- Video encoding uses FFmpeg (h264_nvenc codec supported)
- GPU compression in pipeline crate is **stub only** (nvCOMP not linked)

### Phase 4: Production Hardening

**Goal**: Reliability and observability

| Issue | Title | Alignment | Action |
|-------|-------|-----------|--------|
| #20 | [Phase 6.2] Create worker container image and Helm chart | ✅ **Match** | Rename to Phase 4.1 |
| #21 | [Phase 7.1] Add Prometheus metrics for monitoring | ✅ **Match** | Rename to Phase 4.2 |
| #22 | [Phase 7.2] Add structured logging with SLS integration | ✅ **Match** | Rename to Phase 4.3 |
| — | Load testing at 10 Gbps | ❌ **Missing** | Create new issue |
| — | Chaos testing (worker/TiKV failures) | ❌ **Missing** | Create new issue |

**Codebase Verification**:
- Helm chart exists at `helm/roboflow/`
- Dockerfile.worker exists
- Basic tracing implemented via `tracing` crate
- No Prometheus metrics integration yet

### Phase 5: Multi-Format Support

**Goal**: Extensible dataset format system

| Issue | Title | Alignment | Action |
|-------|-------|-----------|--------|
| — | DatasetFormat trait for pluggable writers | ❌ **Missing** | Create new issue (future) |
| — | KPS v1.2 format support | ⚠️ **Exists** | KPS already implemented in codebase |
| — | Custom format registration API | ❌ **Missing** | Create new issue (future) |

**Codebase Verification**:
- `DatasetWriter` trait exists in `roboflow-dataset/src/common/base.rs`
- KPS writer exists at `roboflow-dataset/src/kps/`
- LeRobot writer exists at `roboflow-dataset/src/lerobot/`
- No unified format registry yet

### Future Scope (Beyond Current Roadmap)

| Issue | Title | Status | Notes |
|-------|-------|--------|-------|
| #50 | [Phase 10.1] Add CLI for job submission | 🔮 Future | Not in current 5-phase roadmap |
| #51 | [Phase 10.2] Add web UI for job monitoring | 🔮 Future | Not in current 5-phase roadmap |
| #9 | [Epic] Distributed Roboflow | 📋 Epic | Parent tracking issue |
| #55 | [Cleanup] Remove deprecated code | 🧹 Cleanup | Can be done anytime |

---

## Recommended Actions

### High Priority: Create Missing Issues

1. **[Phase 1.3] Integrate LerobotWriter with Worker**
   ```
   Integrate the LerobotWriter from roboflow-dataset with the Worker's
   process_job() method. Wire up:
   - Storage backend for input/output
   - LerobotConfig from job parameters
   - Episode finalization and upload
   ```

2. **[Phase 1.4] Wire up checkpoint save/restore in pipeline**
   ```
   Complete the checkpoint integration:
   - Save checkpoints periodically during processing
   - Restore from checkpoint on job resume
   - Delete checkpoint on successful completion
   ```

3. **[Phase 2.1] Implement PrefetchQueue with 2 slots**
   ```
   Create a prefetch pipeline that downloads the next job while
   the current job is being processed:
   - PrefetchQueue with configurable slot count
   - Background download task
   - Memory-mapped file handling for large downloads
   ```

4. **[Phase 4.4] Load testing at 10 Gbps**
   ```
   Create load testing infrastructure:
   - Synthetic workload generator
   - Throughput measurement tooling
   - Bottleneck identification
   ```

### Medium Priority: Rename Existing Issues

| Issue | Current Title | New Title |
|-------|---------------|-----------|
| #47 | [Phase 7.1] Integrate pipeline with checkpoint hooks | [Phase 1.1] Integrate pipeline with checkpoint hooks |
| #48 | [Phase 7.2] Add graceful shutdown handling | [Phase 1.2] Add graceful shutdown handling |
| #49 | [Phase 8] Add NVENC GPU video encoding support | [Phase 3.1] Add NVENC GPU video encoding support |
| #20 | [Phase 6.2] Create worker container image and Helm chart | [Phase 4.1] Create worker container image and Helm chart |
| #21 | [Phase 7.1] Add Prometheus metrics for monitoring | [Phase 4.2] Add Prometheus metrics for monitoring |
| #22 | [Phase 7.2] Add structured logging with SLS integration | [Phase 4.3] Add structured logging with SLS integration |

### Low Priority: Update Epic

Update #9 [Epic] to reference the new phase structure and link to DISTRIBUTED_DESIGN.md.

---

## Implementation Status Summary

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Implementation Progress by Phase                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Phase 1: Pipeline Integration                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ ████████████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  50%     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│  ✅ Worker infrastructure (claim, heartbeat, checkpoint schema)             │
│  ✅ LerobotWriter with storage support                                      │
│  ✅ Streaming converter                                                      │
│  ❌ Worker.process_job() integration (placeholder)                          │
│  ❌ Checkpoint save during processing                                        │
│                                                                              │
│  Phase 2: Prefetch Pipeline                                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ ████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  20%     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│  ✅ Streaming reader (range requests)                                        │
│  ❌ PrefetchQueue                                                            │
│  ❌ Parallel range-request downloader                                        │
│  ❌ Background download pipeline                                             │
│                                                                              │
│  Phase 3: GPU Acceleration                                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ ████████████████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  60%     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│  ✅ NVENC detection in hardware.rs                                          │
│  ✅ Hardware backend enum (Nvenc, VideoToolbox, Vaapi, Cpu)                 │
│  ✅ FFmpeg integration for video encoding                                    │
│  ❌ NVENC preset tuning for throughput                                       │
│  ❌ Parallel camera encoding (2 sessions)                                    │
│                                                                              │
│  Phase 4: Production Hardening                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ ████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  30%     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│  ✅ Helm chart skeleton                                                      │
│  ✅ Dockerfile.worker                                                        │
│  ✅ Basic tracing                                                            │
│  ❌ Prometheus metrics                                                       │
│  ❌ Grafana dashboard                                                        │
│  ❌ Load testing                                                             │
│                                                                              │
│  Phase 5: Multi-Format Support                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ ████████████████████████████████████░░░░░░░░░░░░░░░░░░░░░  80%     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│  ✅ DatasetWriter trait                                                      │
│  ✅ LeRobot v2.1 writer                                                      │
│  ✅ KPS v1.2 writer                                                          │
│  ❌ Unified format registry                                                  │
│  ❌ Per-job format configuration                                             │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Appendix: Issue Reference

### Open Issues (11)

| # | Title | Phase (New) | Priority |
|---|-------|-------------|----------|
| 9 | [Epic] Distributed Roboflow | - | Epic |
| 18 | Long-running Worker Deployment | 1/4 | High |
| 20 | Worker container image and Helm chart | 4.1 | High |
| 21 | Prometheus metrics | 4.2 | Medium |
| 22 | Structured logging | 4.3 | Medium |
| 47 | Pipeline with checkpoint hooks | 1.1 | High |
| 48 | Graceful shutdown | 1.2 | High |
| 49 | NVENC GPU encoding | 3.1 | Medium |
| 50 | CLI for job submission | Future | Low |
| 51 | Web UI for monitoring | Future | Low |
| 55 | Cleanup deprecated code | - | Low |

### Closed Issues (22)

All foundation issues (Phases 1-6 in legacy numbering) are complete.
