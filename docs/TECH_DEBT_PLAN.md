# Technical Debt Remediation Plan

## Overview

This plan addresses all technical debt identified in the comprehensive analysis.
Total estimated effort: ~320 hours over 6 months.

---

## Phase 1: Quick Wins (Week 1-2) - 40 hours

### 1.1 Consolidate Duplicate Configurations (8h)
- [ ] Consolidate `VideoEncoderConfig` to `roboflow-video`
- [ ] Consolidate `DepthEncoderConfig` to `roboflow-video`
- [ ] Consolidate `FragmentEncoderConfig` to `roboflow-video`
- [ ] Update all usages across crates

### 1.2 Create Video Encoder Trait (8h)
- [ ] Define `VideoEncoder` trait in `roboflow-video`
- [ ] Implement trait for existing encoders
- [ ] Update consumers to use trait

### 1.3 Add Core Unit Tests (12h)
- [ ] Add tests for `roboflow-core/src/error.rs`
- [ ] Add tests for `roboflow-core/src/retry.rs`
- [ ] Add tests for `roboflow-core/src/value.rs`

### 1.4 Dependency Cleanup (4h)
- [ ] Pin git dependencies to specific commits
- [ ] Remove unused dependencies
- [ ] Update outdated dependencies

### 1.5 Remove Debug Code (4h)
- [ ] Audit and remove `// DEBUG:` comments
- [ ] Clean up TODO/FIXME comments

**Expected ROI**: 250%+ in first month

---

## Phase 2: Testing Foundation (Week 3-6) - 60 hours

### 2.1 Test Infrastructure Setup (16h)
- [ ] Create test utilities module
- [ ] Set up test fixtures for common scenarios
- [ ] Create mock implementations for Storage trait

### 2.2 roboflow-dataset Tests (24h)
- [ ] Add tests for AlignedFrame
- [ ] Add tests for ImageData validation
- [ ] Add tests for ring buffer synchronization
- [ ] Add tests for streaming uploader

### 2.3 roboflow-video Tests (20h)
- [ ] Add tests for FragmentEncoder
- [ ] Add tests for video configuration validation
- [ ] Add round-trip encoding tests

---

## Phase 3: Architecture Refactoring (Week 7-12) - 100 hours

### 3.1 Split LerobotWriter (40h)
- [ ] Extract `VideoCoordinator` component
- [ ] Extract `ParquetBatchWriter` component
- [ ] Extract `UploadCoordinator` component
- [ ] Create `LerobotWriter` facade
- [ ] Add integration tests

### 3.2 Decouple Dataset from Distributed (24h)
- [ ] Move distributed logic to separate module
- [ ] Create abstraction layer for coordination
- [ ] Update dependency structure

### 3.3 Consolidate Storage Usage (20h)
- [ ] Audit all direct storage usage
- [ ] Convert to `dyn Storage` trait
- [ ] Add factory pattern for storage creation

### 3.4 Simplify Pipeline (16h)
- [ ] Reduce nesting in state machine
- [ ] Extract state handlers to separate functions
- [ ] Add comprehensive tests

---

## Phase 4: Large File Refactoring (Week 13-16) - 60 hours

### 4.1 Refactor video.rs (24h)
- [ ] Split into encoder modules
- [ ] Create clear interfaces
- [ ] Add documentation

### 4.2 Refactor s3.rs (20h)
- [ ] Separate async/sync concerns
- [ ] Improve error types
- [ ] Add retry utilities

### 4.3 Refactor pipeline.rs (16h)
- [ ] Extract frame assembly logic
- [ ] Simplify timestamp management
- [ ] Add state pattern for message processing

---

## Phase 5: Complete Implementations (Week 17-20) - 40 hours

### 5.1 SIMD Implementations (24h)
- [ ] Add SSE2 implementations
- [ ] Add AVX2 implementations
- [ ] Add NEON implementations
- [ ] Add benchmark tests

### 5.2 Zarr Writer (16h)
- [ ] Complete stub implementation
- [ ] Add tests
- [ ] Add documentation

---

## Phase 6: Documentation & Prevention (Week 21-24) - 20 hours

### 6.1 API Documentation (12h)
- [ ] Document all public APIs
- [ ] Add usage examples
- [ ] Create architecture diagrams

### 6.2 Prevention Mechanisms (8h)
- [ ] Set up complexity limits in CI
- [ ] Add test coverage gates
- [ ] Create debt tracking dashboard

---

## Progress Tracking

| Phase | Status | Start | End | Hours |
|-------|--------|-------|-----|-------|
| 1. Quick Wins | 🔲 Not Started | - | - | 40 |
| 2. Testing Foundation | 🔲 Not Started | - | - | 60 |
| 3. Architecture Refactoring | 🔲 Not Started | - | - | 100 |
| 4. Large File Refactoring | 🔲 Not Started | - | - | 60 |
| 5. Complete Implementations | 🔲 Not Started | - | - | 40 |
| 6. Documentation & Prevention | 🔲 Not Started | - | - | 20 |

---

## Debt Metrics Dashboard

| Metric | Current | Phase 2 Target | Final Target |
|--------|---------|----------------|--------------|
| Code Duplication | 15% | 10% | 5% |
| Test Coverage | 20% | 40% | 60% |
| Avg File Size | 450 lines | 400 lines | 300 lines |
| Untested Crates | 3 | 1 | 0 |
| God Classes | 2 | 1 | 0 |
