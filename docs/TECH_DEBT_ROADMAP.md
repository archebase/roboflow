# Technical Debt Elimination Roadmap

**Created**: 2026-02-13
**Total Duration**: 15 weeks
**Total Effort**: 148 hours
**Expected Velocity Gain**: +29%

---

## Task Dependencies

```
Phase 1 (#55)
    ├── Phase 2A (#56) ─┐
    ├── Phase 2B (#57) ─┼── Phase 3A (#59) ─┐
    └── Phase 2C (#58) ─┤   Phase 3B (#60) ─┼── Phase 4A (#62)
                        │   Phase 3C (#61) ─┤   Phase 4B (#63)
                        │                    └── Phase 4C (#64)
                        └──────────────────────────────────────┘
```

---

## Phase Overview

| # | Phase | Duration | Effort | Status |
|---|-------|----------|--------|--------|
| 55 | Phase 1: Quick Wins | Week 1 | 8h | ⏳ Pending |
| 56 | Phase 2A: Test Expansion (video) | Week 2 | 8h | ⏳ Blocked by #55 |
| 57 | Phase 2B: Test Expansion (dataset) | Week 3 | 16h | ⏳ Blocked by #55 |
| 58 | Phase 2C: Test Expansion (distributed) | Week 4 | 16h | ⏳ Blocked by #55 |
| 59 | Phase 3A: Refactor cached.rs | Week 5-6 | 16h | ⏳ Blocked by #56,#57,#58 |
| 60 | Phase 3B: Refactor rsmpeg_encoder.rs | Week 7-8 | 16h | ⏳ Blocked by #56,#57,#58 |
| 61 | Phase 3C: Refactor s3.rs, scanner.rs | Week 9-10 | 24h | ⏳ Blocked by #56,#57,#58 |
| 62 | Phase 4A: Unified Upload Coordinator | Week 11-12 | 20h | ⏳ Blocked by #59,#60,#61 |
| 63 | Phase 4B: Unwrap Audit | Week 13-14 | 10h | ⏳ Blocked by #59,#60,#61 |
| 64 | Phase 4C: Deep Nesting Refactor | Week 15 | 10h | ⏳ Blocked by #59,#60,#61 |

---

## Phase 1: Quick Wins (8h)

**Goal**: Immediate improvements with high ROI

### Deliverables:
- [ ] cargo-deny in CI pipeline
- [ ] Replace serde_yaml 0.9.34 (deprecated)
- [ ] SAFETY comments on 21 unsafe blocks

### Commands to Start:
```bash
# 1. Add cargo-deny step to CI
# Edit .github/workflows/ci.yml

# 2. Update serde_yaml
cargo update serde_yaml

# 3. Add SAFETY comments
# Edit files in roboflow-video/src/rsmpeg.rs
```

---

## Phase 2: Test Expansion (40h)

**Goal**: Increase test ratio from 1.5% to 5%

### 2A: roboflow-video (+40 tests)
| File | Current Tests | Target | Add |
|------|---------------|--------|-----|
| rsmpeg.rs | - | 15 | 15 |
| hardware.rs | - | 10 | 10 |
| concurrent.rs | - | 10 | 10 |
| simd.rs | - | 5 | 5 |

### 2B: roboflow-dataset (+80 tests)
| File | Current Tests | Target | Add |
|------|---------------|--------|-----|
| writer_impl.rs | 0 | 20 | 20 |
| rsmpeg_encoder.rs | - | 15 | 15 |
| streaming_encoder.rs | - | 15 | 15 |
| pipeline.rs | - | 15 | 15 |
| alignment.rs | - | 10 | 10 |
| upload.rs | - | 5 | 5 |

### 2C: roboflow-distributed (+60 tests)
| File | Current Tests | Target | Add |
|------|---------------|--------|-----|
| scanner.rs | - | 15 | 15 |
| batch/controller.rs | - | 12 | 12 |
| tikv/client.rs | - | 10 | 10 |
| merge/coordinator.rs | - | 10 | 10 |
| tikv/locks.rs | - | 8 | 8 |
| reaper.rs | - | 5 | 5 |

**Special**: Rewrite zombie_reaper_test.rs for WorkUnit-based reaping

---

## Phase 3: Complexity Reduction (56h)

**Goal**: Reduce large files from 20 to <10

### 3A: cached.rs (1,435 → <800 lines)
Extract:
- `CachePolicy` (~200 lines)
- `CacheStorage` (~300 lines)
- `UploadCoordinator` (~200 lines)

### 3B: rsmpeg_encoder.rs (1,295 → <800 lines)
Extract:
- `RsmpegConfig` (~150 lines)
- `RsmpegFrameBuffer` (~200 lines)
- `RsmpegWriter` (~200 lines)

### 3C: s3.rs + scanner.rs (2,464 → <1,400 lines)
s3.rs:
- Extract `AsyncS3Storage`
- Extract `S3Storage` (sync)

scanner.rs:
- Extract `BatchScanner`
- Extract `SingleScanner`
- Extract `ScanCoordinator`

---

## Phase 4: Architecture Consolidation (40h)

**Goal**: Eliminate structural debt

### 4A: Unified Upload Coordinator (20h)
Create `UploadCoordinator` trait to consolidate 10+ upload functions

```rust
pub trait UploadCoordinator: Send + Sync {
    async fn upload(&self, path: &Path, remote: &Path) -> Result<()>;
    async fn upload_parallel(&self, items: &[(PathBuf, PathBuf)]) -> Result<()>;
    fn progress(&self) -> UploadProgress;
}
```

### 4B: Unwrap Audit (10h)
| Crate | Current | Target | Reduction |
|-------|---------|--------|-----------|
| roboflow-dataset | 162 | 40 | -122 |
| roboflow-storage | 156 | 40 | -116 |
| roboflow-distributed | 42 | 15 | -27 |
| Others | 76 | 5 | -71 |
| **Total** | **436** | **100** | **-336** |

### 4C: Deep Nesting Refactor (10h)
Reduce deep nesting from 30 to <10 hotspots using:
- Early returns
- Guard clauses
- Extract methods

---

## Success Metrics

### Before
| Metric | Value |
|--------|-------|
| Test ratio | 1.5% |
| Large files | 20 |
| Deep nesting | 30 |
| Unwrap calls | 436 |
| Deprecated deps | 1 |

### After (Target)
| Metric | Value |
|--------|-------|
| Test ratio | 5% |
| Large files | <10 |
| Deep nesting | <10 |
| Unwrap calls | <100 |
| Deprecated deps | 0 |

---

## Getting Started

```bash
# View all tasks
/task-list

# Start Phase 1
/task-get 55

# Mark as in-progress when starting
/task-update 55 --status in_progress
```

---

## Notes

- Phases 2A, 2B, 2C can run in parallel after Phase 1
- Phases 3A, 3B, 3C can run in parallel after Phase 2
- Phases 4A, 4B, 4C can run in parallel after Phase 3
- Each phase should be committed separately with tests passing
