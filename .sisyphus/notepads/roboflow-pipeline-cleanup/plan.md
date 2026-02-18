# Roboflow-Pipeline Cleanup Plan

## Current State Analysis
- **93 source files**, ~36,000 lines of code
- Consolidated from 4 crates (sources, sinks, video, dataset)
- Only 1 clippy warning currently
- Structure: sources/, sinks/, video/, formats/

## Issues Identified

### 1. **Duplicate Type Names** (High Priority)
- `ImageData` - defined in both `video/mod.rs` and `formats/common/base.rs`
  - **Action**: Keep `video::ImageData` (has `is_encoded` field), remove `formats::ImageData`
  - **Status**: Partially done - unified to video::ImageData
  
- `StreamingConfig` - defined in `formats/lerobot/config.rs` and `formats/streaming/config.rs`
  - **Action**: Consolidate to one location
  
- `UploadStats`, `UploadProgress`, `UploadConfig` - duplicates in lerobot/upload.rs and common/streaming_uploader.rs
  - **Action**: Merge or rename to be distinct

### 2. **Unused/Stub Modules** (Medium Priority)
- `formats/zarr.rs` - Complete stub with TODO comments, no implementation
  - **Action**: Remove file and references
  - **Status**: Done

### 3. **Test Failures** (High Priority)
Multiple test compilation errors:
- `crate::register_builtin_sources()` not found in tests
- `crate::common::video::VideoEncoderConfig` path issues
- Test imports using old crate paths
  - **Action**: Fix all test imports to use `crate::` paths correctly

### 4. **#[allow(dead_code)] Attributes** (Low Priority)
Found in 4 locations:
- `video/pipeline/three_stage.rs` - camera field (intentional for debugging)
- `video/pipeline/two_stage.rs` - camera field (intentional)
- `formats/lerobot/writer/encoding.rs` - decode_failures field
- `formats/lerobot/writer/parquet.rs` - write_episode_parquet function
  - **Action**: Review each - either remove code or add proper usage

### 5. **Large Files** (Medium Priority)
Files over 1000 lines that could benefit from splitting:
- `video/rsmpeg.rs` (1,830 lines)
- `formats/lerobot/writer/writer_impl.rs` (1,855 lines)
- `formats/pipeline.rs` (1,350 lines)
- `video/hardware.rs` (1,066 lines)
  - **Action**: Consider extracting sub-modules

### 6. **Import Cleanup** (Medium Priority)
- Many internal imports still using old patterns
- Some unused imports (ImageData in sinks/mod.rs is actually used but flagged)
  - **Action**: Audit all imports, remove unused ones

## Cleanup Phases

### Phase 1: Critical Fixes (Tests Must Pass)
1. Fix all test compilation errors
2. Fix import paths in test modules
3. Verify `cargo test -p roboflow-pipeline` passes

### Phase 2: Type Consolidation
1. Remove duplicate ImageData from formats/common/base.rs
2. Update all references to use video::ImageData
3. Consolidate or rename duplicate Upload* types
4. Consolidate StreamingConfig types

### Phase 3: Dead Code Removal
1. Review and remove #[allow(dead_code)] items or make them used
2. Remove commented-out code blocks
3. Delete unused helper functions

### Phase 4: Module Simplification
1. Split oversized files
2. Simplify module hierarchy where possible
3. Improve public API surface in lib.rs

### Phase 5: Final Verification
1. Run full test suite
2. Run clippy with all features
3. Check documentation completeness
4. Verify no regressions

## Expected Outcomes
- 35,000 → ~30,000 lines of code (15% reduction)
- Single ImageData type throughout
- All tests passing
- No #[allow(dead_code)] attributes
- Cleaner module hierarchy
