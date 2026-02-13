# Technical Debt Remediation Plan - Roboflow

**Created**: 2026-02-13
**Target Completion**: Q2 2026
**Total Estimated Effort**: 80 hours across 6 phases

---

## Phase Overview

```
Phase 1: Quick Wins ───────────────────► Phase 2: Code Consolidation
    (8h)                                      (16h)
                                               │
                                               ▼
Phase 3: Architecture Cleanup ─────────► Phase 4: Testing Expansion
    (20h)                                     (16h)
                                               │
                                               ▼
Phase 5: Documentation ─────────────────► Phase 6: Prevention Systems
    (12h)                                     (8h)
```

---

## Phase 1: Quick Wins (8 hours)

**Goal**: Low-effort, high-impact fixes that clean up obvious debt

### TD-1.1: Remove Duplicate Dependencies
- **File**: `Cargo.toml` (root)
- **Issue**: chrono listed twice (lines 27 and 96)
- **Effort**: 0.5h
- **Action**: Remove duplicate entry
- **Status**: ✅ Done (partial - need to verify)

### TD-1.2: Remove Unused Feature
- **File**: `Cargo.toml` (root)
- **Issue**: `cloud-storage` feature does nothing
- **Effort**: 0.5h
- **Action**: Remove feature flag
- **Status**: Pending

### TD-1.3: Consolidate SIMD TODOs
- **Files**:
  - `crates/roboflow-video/src/simd.rs:153`
  - `crates/roboflow-dataset/src/common/simd_convert.rs:154`
- **Issue**: Identical TODO comments
- **Effort**: 0.5h
- **Action**: Create single tracking issue, remove duplicate
- **Status**: Pending

### TD-1.4: Add Missing Error Tests
- **File**: `crates/roboflow-core/src/error.rs`
- **Effort**: 2h
- **Action**: Add tests for all error variants
- **Status**: ✅ Done (14 new tests added)

### TD-1.5: Fix Compiler Warnings
- **Scope**: All crates
- **Effort**: 1h
- **Action**: Run `cargo clippy --fix`, resolve all warnings
- **Status**: ✅ Done

### TD-1.6: Add Critical Path Tests
- **Files**:
  - `crates/roboflow-dataset/src/lerobot/writer/encoding.rs`
  - `crates/roboflow-distributed/src/worker/executor.rs`
- **Effort**: 3.5h
- **Action**: Add unit tests for encoding logic and worker execution
- **Status**: Pending

---

## Phase 2: Code Consolidation (16 hours)

**Goal**: Eliminate duplicate code through shared abstractions

### TD-2.1: Create Unified Registry Trait
- **Files**:
  - `crates/roboflow-core/src/registry.rs` (base trait)
  - `crates/roboflow-sources/src/registry.rs` (impl)
  - `crates/roboflow-sinks/src/registry.rs` (impl)
  - `crates/roboflow-distributed/src/catalog/pool.rs` (impl)
- **Current State**: 4 implementations, 95% similar
- **Effort**: 4h
- **Action**:
  1. Create `Registry<K, V>` trait in roboflow-core
  2. Create `GlobalRegistry<K, V>` with OnceLock pattern
  3. Refactor all registries to use base implementation
- **Savings**: ~400 lines removed
- **Status**: Pending

### TD-2.2: Consolidate Video Encoder Configurations
- **Files**:
  - `crates/roboflow-video/src/config.rs`
  - `crates/roboflow-dataset/src/common/streaming_encoder.rs`
  - `crates/roboflow-dataset/src/common/concurrent_video_encoder.rs`
  - `crates/roboflow-video/src/fragment.rs`
- **Current State**: Multiple config structs with overlapping fields
- **Effort**: 3h
- **Action**:
  1. Define base `VideoEncoderConfig` in roboflow-video
  2. Create builder pattern for common fields
  3. Use composition for specialized configs
- **Savings**: ~200 lines removed
- **Status**: ✅ Partially Done (VideoEncoderConfig, FragmentEncoderConfig)

### TD-2.3: Create Shared Configuration Validation
- **Files**:
  - `crates/roboflow-dataset/src/lerobot/config.rs`
  - `crates/roboflow-dataset/src/kps/config.rs`
  - `crates/roboflow-distributed/src/worker/config.rs`
  - `crates/roboflow-distributed/src/tikv/config.rs`
- **Effort**: 3h
- **Action**:
  1. Create `ConfigValidate` trait in roboflow-core
  2. Create validation helpers (range, format, required)
  3. Refactor configs to use shared validation
- **Savings**: ~150 lines removed
- **Status**: Pending

### TD-2.4: Unify Error Handling Patterns
- **Files**: Multiple across crates
- **Issue**: `RoboflowError::parse` pattern repeated 35+ times
- **Effort**: 3h
- **Action**:
  1. Create specialized error constructors
  2. Add context helpers for common patterns
  3. Document error handling best practices
- **Status**: Pending

### TD-2.5: Break Down lerobot/writer/mod.rs
- **File**: `crates/roboflow-dataset/src/lerobot/writer/mod.rs`
- **Current**: 1,646 lines (god module)
- **Effort**: 3h
- **Action**: Split into:
  - `writer/mod.rs` - Public API only
  - `writer/video.rs` - Video encoding logic
  - `writer/parquet.rs` - Parquet writing logic
  - `writer/encoding.rs` - Frame encoding
  - `writer/stats.rs` - Statistics tracking
- **Status**: Pending

---

## Phase 3: Architecture Cleanup (20 hours)

**Goal**: Fix layering violations and improve modularity

### TD-3.1: Fix Dataset→Sources Layering Violation
- **File**: `crates/roboflow-dataset/Cargo.toml`
- **Issue**: Dataset depends on Sources crate (line 14)
- **Effort**: 4h
- **Action**:
  1. Move shared types to roboflow-core
  2. Use dependency injection for source creation
  3. Remove direct dependency
- **Status**: Pending

### TD-3.2: Create MessageDecoder Trait
- **File**: `crates/roboflow-sources/src/decode.rs`
- **Issue**: Format-specific decode functions instead of trait
- **Effort**: 4h
- **Action**:
  1. Define `MessageDecoder` trait
  2. Implement for Bag, MCAP, RRD formats
  3. Create factory pattern for decoder selection
- **Status**: Pending

### TD-3.3: Split Storage Module
- **File**: `crates/roboflow-storage/src/lib.rs`
- **Current**: 590 lines with multiple concerns
- **Effort**: 4h
- **Action**:
  1. `storage/traits.rs` - Storage trait definitions
  2. `storage/errors.rs` - Storage-specific errors
  3. `storage/local.rs` - LocalStorage impl
  4. `storage/s3.rs` - S3Storage impl
  5. `storage/cached.rs` - CachedStorage impl
- **Status**: Pending

### TD-3.4: Remove Global Registry State
- **Files**: Multiple registries using `OnceLock`
- **Effort**: 4h
- **Action**:
  1. Create `RegistryContainer` for dependency injection
  2. Pass registries explicitly to components
  3. Remove global static registries
- **Status**: Pending

### TD-3.5: Fix TiKVCatalog Leaky Abstraction
- **File**: `crates/roboflow-distributed/src/catalog/catalog_impl.rs`
- **Issue**: `pool()` method exposes internal state
- **Effort**: 2h
- **Action**:
  1. Remove `pool()` from public API
  2. Add necessary operations as trait methods
  3. Update callers
- **Status**: Pending

### TD-3.6: Reduce Trait Object Usage
- **File**: `crates/roboflow-storage/src/lib.rs`
- **Issue**: `Box<dyn Read/Write>` forces dynamic dispatch
- **Effort**: 2h
- **Action**:
  1. Use generic bounds where possible
  2. Add associated types to Storage trait
  3. Profile and optimize hot paths
- **Status**: Pending

---

## Phase 4: Testing Expansion (16 hours)

**Goal**: Increase test coverage from ~45% to 70%+

### TD-4.1: Add Writer Module Tests
- **Files**:
  - `lerobot/writer/mod.rs`
  - `lerobot/writer/encoding.rs`
  - `lerobot/writer/parquet.rs`
  - `lerobot/writer/stats.rs`
- **Effort**: 4h
- **Action**: Add comprehensive unit tests for each module
- **Target Coverage**: 80%
- **Status**: Pending

### TD-4.2: Add Distributed Worker Tests
- **Files**:
  - `worker/executor.rs`
  - `worker/coordinator.rs`
  - `worker/metrics.rs`
- **Effort**: 4h
- **Action**: Add unit tests with mocked TiKV client
- **Status**: Pending

### TD-4.3: Add Integration Tests
- **Scope**: End-to-end workflows
- **Effort**: 4h
- **Action**:
  1. MCAP → LeRobot conversion test
  2. Distributed batch processing test
  3. S3 upload/download test (MinIO)
- **Status**: Pending

### TD-4.4: Fix Ignored Tests
- **Files**:
  - `zombie_reaper_test.rs`
  - Various doc tests
- **Effort**: 2h
- **Action**: Fix or remove ignored tests
- **Status**: Pending

### TD-4.5: Add Property-Based Tests
- **Scope**: Core data transformations
- **Effort**: 2h
- **Action**:
  1. Add proptest dependency
  2. Add property tests for encoding/decoding
  3. Add property tests for config validation
- **Status**: Pending

---

## Phase 5: Documentation (12 hours)

**Goal**: Document all public APIs and architecture

### TD-5.1: Document Public Traits
- **Files**: All crates
- **Scope**: ~20 public traits
- **Effort**: 4h
- **Action**:
  1. Add `//!` module-level docs
  2. Add `///` doc comments to all trait methods
  3. Add usage examples
- **Status**: Pending

### TD-5.2: Document Public Structs/Enums
- **Scope**: ~80 structs, ~30 enums
- **Effort**: 4h
- **Action**: Add doc comments with examples
- **Status**: Pending

### TD-5.3: Create Architecture Documentation
- **Deliverables**:
  1. Crate dependency diagram
  2. Data flow diagram
  3. Component overview
- **Effort**: 2h
- **Status**: Pending

### TD-5.4: Update CLAUDE.md
- **Issue**: May reference obsolete patterns
- **Effort**: 1h
- **Action**: Review and update all sections
- **Status**: Pending

### TD-5.5: Add README for Each Crate
- **Scope**: All 8 crates
- **Effort**: 1h
- **Action**: Add purpose, usage examples, feature flags
- **Status**: Pending

---

## Phase 6: Prevention Systems (8 hours)

**Goal**: Automate quality gates to prevent new debt

### TD-6.1: Add Pre-commit Hooks
- **File**: `.pre-commit-config.yaml`
- **Checks**:
  - `cargo fmt --check`
  - `cargo clippy -- -D warnings`
  - Test coverage threshold
- **Effort**: 2h
- **Status**: Pending

### TD-6.2: Add CI Quality Gates
- **File**: `.github/workflows/ci.yml`
- **Checks**:
  - Complexity check (max 15)
  - File size check (max 800 lines)
  - Duplication check (max 5%)
- **Effort**: 2h
- **Status**: Pending

### TD-6.3: Add Coverage Reporting
- **Tool**: cargo-tarpaulin or cargo-llvm-cov
- **Target**: 60% minimum for new code
- **Effort**: 2h
- **Status**: Pending

### TD-6.4: Add Dependency Auditing
- **Tools**: cargo-outdated, cargo-audit
- **Schedule**: Weekly
- **Effort**: 1h
- **Status**: Pending

### TD-6.5: Add Complexity Tracking
- **Tool**: cargo-clippy + custom lint
- **Action**: Track complexity over time
- **Effort**: 1h
- **Status**: Pending

---

## Tracking Tasks

### Create Tasks for Each Phase

```bash
# Phase 1 Tasks
- [ ] TD-1.2: Remove unused cloud-storage feature
- [ ] TD-1.3: Consolidate SIMD TODOs
- [ ] TD-1.6: Add critical path tests

# Phase 2 Tasks
- [ ] TD-2.1: Create unified Registry trait
- [ ] TD-2.3: Create shared config validation
- [ ] TD-2.4: Unify error handling patterns
- [ ] TD-2.5: Break down lerobot/writer/mod.rs

# Phase 3 Tasks
- [ ] TD-3.1: Fix dataset→sources layering
- [ ] TD-3.2: Create MessageDecoder trait
- [ ] TD-3.3: Split storage module
- [ ] TD-3.4: Remove global registry state
- [ ] TD-3.5: Fix TiKVCatalog leaky abstraction
- [ ] TD-3.6: Reduce trait object usage

# Phase 4 Tasks
- [ ] TD-4.1: Add writer module tests
- [ ] TD-4.2: Add distributed worker tests
- [ ] TD-4.3: Add integration tests
- [ ] TD-4.4: Fix ignored tests
- [ ] TD-4.5: Add property-based tests

# Phase 5 Tasks
- [ ] TD-5.1: Document public traits
- [ ] TD-5.2: Document public structs/enums
- [ ] TD-5.3: Create architecture documentation
- [ ] TD-5.4: Update CLAUDE.md
- [ ] TD-5.5: Add README for each crate

# Phase 6 Tasks
- [ ] TD-6.1: Add pre-commit hooks
- [ ] TD-6.2: Add CI quality gates
- [ ] TD-6.3: Add coverage reporting
- [ ] TD-6.4: Add dependency auditing
- [ ] TD-6.5: Add complexity tracking
```

---

## Progress Summary

| Phase | Total Tasks | Completed | In Progress | Pending |
|-------|-------------|-----------|-------------|---------|
| Phase 1 | 6 | 3 | 0 | 3 |
| Phase 2 | 5 | 1 | 0 | 4 |
| Phase 3 | 6 | 0 | 0 | 6 |
| Phase 4 | 5 | 0 | 0 | 5 |
| Phase 5 | 5 | 0 | 0 | 5 |
| Phase 6 | 5 | 0 | 0 | 5 |
| **Total** | **32** | **4** | **0** | **28** |

---

## Next Actions

1. **This Week**: Complete Phase 1 remaining tasks (TD-1.2, TD-1.3, TD-1.6)
2. **Next Week**: Start Phase 2 with TD-2.1 (Registry trait)
3. **Ongoing**: Track progress and update this document
