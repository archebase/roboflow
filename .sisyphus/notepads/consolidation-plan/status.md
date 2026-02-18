# Crate Consolidation Status

## Current State: 8 Crates (Unchanged)

```
crates/
├── roboflow-core          # 500 lines - Error types, core types
├── roboflow-storage       # 3,000 lines - S3, OSS, Local storage
├── roboflow-executor      # 1,500 lines - Stage framework (traits only)
├── roboflow-distributed   # 18,000 lines - TiKV + Stage implementations
├── roboflow-video         # 11,000 lines - Video encoding
├── roboflow-dataset       # 20,000 lines - LeRobot, KPS formats
├── roboflow-sources       # 3,000 lines - Bag, MCAP readers
└── roboflow-sinks         # 1,500 lines - LeRobot output
```

## What Was Accomplished

### Phase 1: Real Stage Implementations
- **DiscoverStage**: Real file discovery using StorageFactory
- **ConvertStage**: Real bag→LeRobot conversion using PipelineExecutor  
- **MergeStage**: Parquet + info.json creation
- All stages moved to `roboflow-distributed/src/stages/`

### Phase 2: Integration
- LeRobotExecutor uses Convert → Merge pipeline
- 265/266 tests passing (1 ignored - needs real bag file)

### Phase 3: Crate Consolidation
Created but removed incomplete `roboflow-pipeline` crate.

## Target Architecture: 4 Crates

```
roboflow-core (~500 lines)
    ↓
roboflow-executor (~2,000 lines) - Pure framework
    ↓
roboflow-pipeline (~25,000 lines) - All conversion logic
    ↓
roboflow-distributed (~15,000 lines) - Distributed coordination
```

## Remaining Work

To achieve the 4-crate consolidation:

1. **Create roboflow-pipeline** with proper module structure:
   - `src/sources/` (from roboflow-sources)
   - `src/sinks/` (from roboflow-sinks)
   - `src/video/` (from roboflow-video)
   - `src/formats/` (from roboflow-dataset)

2. **Create all mod.rs files** with proper exports

3. **Fix imports in all 93 source files**:
   - `roboflow_video::` → `crate::video::`
   - `roboflow_sources::` → `crate::sources::`
   - etc.

4. **Update roboflow-distributed** to use `roboflow_pipeline::` instead of individual crates

5. **Remove old crates** and update workspace

6. **Verify all tests pass**

## Recommendation

This is a large refactoring (estimated 4-6 hours of focused work).

**Current Status**: We have working stage implementations in roboflow-distributed.
The consolidation to 4 crates is incomplete.

**Next Steps Options**:
- **A**: Continue with full consolidation (large effort)
- **B**: Keep current 8-crate structure (working, tested)  
- **C**: Do gradual migration (one crate at a time)
