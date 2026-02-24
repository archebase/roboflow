# Technical Debt Tracking

Last Updated: 2026-02-24

## Summary

This document tracks technical debt remediation progress for the Roboflow codebase.

## Metrics Dashboard

| Metric | Before | After | Target | Status |
|--------|--------|-------|--------|--------|
| Undocumented unsafe blocks | 15 | 0 | 0 | ✅ Fixed |
| Unused dependencies | ~40 | ~30 | 0 | 🔄 In Progress |
| roboflow-executor test coverage | 0% | ~80% | 80% | ✅ Fixed |
| Duplicate dependencies | 30+ | 30+ | 0 | 📋 Transitive |
| Feature flags for heavy deps | 0 | 1 | 3 | 🔄 In Progress |

## Completed Remediation

### Phase 1: Quick Wins

1. **Safety Comments for Unsafe Code** ✅
   - `encoder.rs`: Added 6 safety comments
   - `arena.rs`: Added 10 safety comments
   - `codec.rs`: Added 1 safety comment
   - `composer.rs`: Added 1 safety comment
   - `ring_buffer.rs`: Already documented

2. **Unused Dependencies Removed** ✅
   - `roboflow-pipeline`: Removed 8 unused deps
   - `roboflow-dataset`: Removed 16 unused deps
   - `roboflow-distributed`: Removed 2 unused deps
   - `roboflow-media`: Removed 1 unused dep

3. **Unit Tests for roboflow-executor** ✅
   - Already had good test coverage:
     - `executor.rs`: test_executor_simple
     - `resource.rs`: 4 tests for SlotPool
     - `policy/mod.rs`: 3 tests
     - `policy/parallel.rs`: 7 tests
     - `pipeline.rs`: 5 tests

### Phase 2: Medium-Term

4. **execute_merge Refactoring** ✅
   - Already refactored into smaller helper methods:
     - `get_or_create_merge_state`
     - `ensure_merge_ready`
     - `transition_to_merging`
     - `verify_cas_won`
     - `execute_merge`
     - `save_merge_state`
     - `fail_merge_with_status`
     - `complete_merge_with_status`

5. **Feature Flags** 🔄
   - Added to `roboflow-dataset`:
     - `lerobot` (default): LeRobot format support
     - `mcap-source` (default): MCAP file reading
     - `video`: Video encoding support

## Remaining Work

### High Priority

1. **Reduce unwrap/expect usage** (1,530 occurrences)
   - Strategy: Replace with `?` operator and proper error types
   - Target: 50% reduction (765 occurrences)

2. **God Module Refactoring**
   - `writer_impl.rs` (1,598 lines): Split into writer/merge/validation
   - `scanner.rs` (1,381 lines): Split into discovery/pattern/metadata

### Medium Priority

3. **Duplicate Dependencies**
   - Most duplicates are transitive (from tikv-client, polars, etc.)
   - Requires upstream updates or feature flag tuning

4. **Feature Flags for roboflow-media**
   - Add `ffmpeg` feature flag for video encoding

## Prevention Strategy

### Quality Gates (Recommended)

```yaml
# .github/workflows/quality.yml
pre_commit:
  - cognitive_complexity: "max 15"
  - unsafe_documentation: "required"
  - test_coverage: "min 80% for new code"
```

### Debt Budget

- Allowed monthly increase:
  - Complexity: 1%
  - Dependencies: 2 new max
  - unwrap: 0 (must decrease)

- Mandatory reduction:
  - Complexity: 5% per quarter
  - unwrap: 20% per quarter

## Tracking Commands

```bash
# Check for unused dependencies
cargo machete

# Check for duplicate dependencies
cargo tree --duplicates

# Run clippy with complexity warnings
cargo clippy -- -W clippy::cognitive_complexity

# Count unwrap/expect
rg "\.unwrap\(\)|\.expect\(" --type rust --stats
```

## Historical Changes

### 2026-02-24
- Added safety comments to all undocumented unsafe blocks
- Removed ~27 unused dependencies across 4 crates
- Added feature flags to roboflow-dataset
- Verified existing test coverage for roboflow-executor
