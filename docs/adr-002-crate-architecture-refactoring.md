# ADR-002: Crate Architecture Refactoring - Clean Separation of Concerns

**Author**: Sisyphus (AI Agent)  
**Date**: 2026-02-20  
**Status**: Proposed  
**Related**: [executor-architecture.md](./executor-architecture.md), [ADR-001](./adr-001-pipeline-writer-storage-separation.md)

## Context

The current crate structure has significant architectural violations that create coupling, duplication, and circular dependencies:

### Problem 1: Circular Dependency
```
roboflow-executor (dev) → roboflow-distributed → roboflow-executor
```

The executor crate has dev-dependencies on distributed, while distributed depends on executor for production code.

### Problem 2: Duplicate Stage Definitions

**roboflow-executor/src/stages/**:
- `convert.rs` - Converts bag/mcap to format
- `discover.rs` - Discovers input files
- `merge.rs` - Merges parquet files
- `transform.rs` - Transforms data

**roboflow-distributed/src/stages/**:
- `convert.rs` - Same purpose, different implementation
- `discover.rs` - Same purpose, different implementation
- `merge.rs` - Same purpose, different implementation

These are dataset-format-specific stages that belong in the pipeline domain, not the executor runtime.

### Problem 3: Mixed Concerns in roboflow-distributed

The distributed crate currently contains:
- TiKV infrastructure (low-level coordination)
- Worker runtime (should be separate)
- LeRobotExecutor (concrete pipeline integration)
- Batch controllers (business logic)

This violates the principle that the control plane should depend on abstractions, not concrete implementations.

### Problem 4: roboflow-pipeline Monolith

The pipeline crate contains:
- Dataset formats (LeRobot, HDF5 stubs)
- Media processing (video encoding, image decoding)
- Data sources (bag, MCAP readers)
- Frame alignment

These are distinct domains that should be separable.

## Decision

Establish clear architectural boundaries between three layers:

### 1. roboflow-executor: Runtime Kernel Only

**Owns**: DAG/Stage/Task/scheduler/resources/object refs
**Does NOT own**: Dataset formats, concrete conversion stages

```rust
// Core abstractions
pub trait Stage: Send + Sync {
    fn stage_id(&self) -> StageId;
    fn execute(&self, ctx: &TaskContext) -> Result<TaskResult>;
}

pub trait Task: Send + Sync {
    fn task_id(&self) -> TaskId;
    fn execute(&self) -> Result<TaskResult>;
}

pub struct StageExecutor {
    slot_pool: SlotPool,
    scheduler: StageScheduler,
    // ... runtime only
}
```

**Key change**: Remove `stages/` module entirely. The executor provides the framework; others implement it.

### 2. roboflow-pipeline: Data Conversion Domain

**Owns**: Sources, decoding, format configs, writers, concrete conversion stages
**Implements**: executor Stage/Task traits via adapters

```rust
// Pipeline implements executor traits
impl roboflow_executor::Stage for ConvertStage {
    fn execute(&self, ctx: &TaskContext) -> Result<TaskResult> {
        // Concrete LeRobot conversion logic
    }
}
```

**Move into pipeline**:
- `crates/roboflow-executor/src/stages/` → `crates/roboflow-pipeline/src/stages/`
- Stages become implementations of executor traits

### 3. roboflow-distributed: Control Plane Only

**Owns**: Batch lifecycle, worker coordination, TiKV state, heartbeats, retries, merge coordination state
**Depends on**: Abstract `WorkProcessor` trait, not pipeline internals

```rust
// Abstract work processor - no dependency on LeRobot
pub trait WorkProcessor: Send + Sync {
    type Input;
    type Output;
    type Error;
    
    async fn process(&self, input: Self::Input) -> Result<Self::Output, Self::Error>;
}

// LeRobotExecutor becomes generic
pub struct DistributedExecutor<P: WorkProcessor> {
    processor: P,
    coordinator: Arc<dyn Coordinator>,
    // ... control plane only
}
```

**Remove from distributed**:
- `lerobot_executor.rs` → Move to pipeline or make generic
- `stages/` → Delete (duplicate)
- `converter/` → Move to pipeline

## Dependency Graph (Target State)

```
┌─────────────────────────────────────────────────────────────────┐
│                     roboflow-distributed                        │
│                    (Control Plane - Orchestration)              │
│  - Scanner, Reaper, Finalizer controllers                       │
│  - Batch lifecycle management                                   │
│  - Worker coordination, heartbeats                              │
│  - TiKV state management                                        │
│  - Merge coordination                                           │
│                                                                 │
│  Depends on: roboflow-executor (traits)                         │
│              roboflow-pipeline (WorkProcessor impl, optional)   │
└─────────────────────────────────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      roboflow-pipeline                          │
│              (Data Conversion Domain - Domain Logic)            │
│  - Sources (bag, mcap, rrd)                                     │
│  - Media processing (video encode, image decode)                │
│  - Dataset formats (LeRobot, HDF5, RLDS)                        │
│  - Writers and conversion stages                                │
│  - Concrete WorkProcessor implementations                       │
│                                                                 │
│  Implements: roboflow_executor::Stage/Task                      │
│  Depends on: roboflow-executor (traits)                         │
│              roboflow-storage (Storage trait)                   │
│              roboflow-core (types)                              │
└─────────────────────────────────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      roboflow-executor                          │
│              (Runtime Kernel - Execution Framework)             │
│  - DAG/Pipeline/Stage/Task abstractions                         │
│  - Slot-based resource management                               │
│  - Stage scheduler                                              │
│  - Object store for lineage                                     │
│  - Lineage tracking                                             │
│                                                                 │
│  Depends on: roboflow-core (types only)                         │
│  No dev-dependencies on distributed                             │
└─────────────────────────────────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  roboflow-storage │ roboflow-core │ robocodec                   │
│  (Infrastructure Layer - No Internal Dependencies)              │
└─────────────────────────────────────────────────────────────────┘
```

## Key Design Decisions

### 1. Executor Stays Pure Framework

**Before** (wrong):
```rust
// roboflow-executor/src/stages/convert.rs
pub struct ConvertStage {
    input_path: String,
    output_path: String,
}

impl Stage for ConvertStage {
    fn execute(&self, _ctx: &TaskContext) -> Result<TaskResult> {
        // LeRobot-specific conversion logic here
        roboflow_pipeline::sources::register_builtin_sources();
        // ... concrete pipeline logic
    }
}
```

**After** (correct):
```rust
// roboflow-executor/src/stage.rs
pub trait Stage: Send + Sync {
    fn stage_id(&self) -> StageId;
    fn execute(&self, ctx: &TaskContext) -> Result<TaskResult>;
}

// roboflow-pipeline/src/stages/convert.rs
pub struct ConvertStage {
    input_path: String,
    output_path: String,
}

impl roboflow_executor::Stage for ConvertStage {
    fn execute(&self, _ctx: &TaskContext) -> Result<TaskResult> {
        // LeRobot-specific logic here
        // Depends on pipeline's sources, media, etc.
    }
}
```

### 2. WorkProcessor Trait for Distributed

**Before** (wrong):
```rust
// roboflow-distributed/src/lerobot_executor.rs
pub struct LeRobotExecutor {
    stage_executor: StageExecutor,
}

impl LeRobotExecutor {
    pub async fn execute(&self, unit: &WorkUnit) -> Result<ProcessingResult> {
        roboflow_pipeline::sources::register_builtin_sources();  // Direct dependency
        
        let pipeline = PipelineBuilder::new()
            .stage(Arc::new(ConvertStage::new(...)))  // Concrete stage
            .stage(Arc::new(MergeStage::new(...)))    // Concrete stage
            .build()?;
        
        self.stage_executor.execute(&pipeline).await
    }
}
```

**After** (correct):
```rust
// roboflow-executor/src/processor.rs
pub trait WorkProcessor: Send + Sync {
    type Input;
    type Output;
    type Error;
    
    async fn process(&self, input: Self::Input) -> Result<Self::Output, Self::Error>;
}

// roboflow-distributed/src/executor.rs
pub struct DistributedExecutor<P: WorkProcessor> {
    processor: P,
    coordinator: Arc<dyn Coordinator>,
}

impl<P: WorkProcessor> DistributedExecutor<P> {
    pub async fn execute(&self, work: WorkUnit) -> Result<ProcessingResult> {
        // Control plane logic only
        self.claim_work().await?;
        self.send_heartbeat().await?;
        
        // Delegate to processor
        let result = self.processor.process(work.input).await;
        
        // Report completion
        self.complete_work(&result).await?;
        Ok(result)
    }
}

// roboflow-pipeline/src/processors/lerobot.rs
pub struct LeRobotProcessor;

impl WorkProcessor for LeRobotProcessor {
    type Input = WorkUnit;
    type Output = EpisodeResult;
    type Error = PipelineError;
    
    async fn process(&self, input: Self::Input) -> Result<Self::Output, Self::Error> {
        // All pipeline-specific logic here
        roboflow_pipeline::sources::register_builtin_sources();
        // ... convert, merge, etc.
    }
}
```

### 3. Remove Duplicate Stages

Delete `roboflow-distributed/src/stages/` entirely. Stages live in one place:
- `roboflow-pipeline/src/stages/` - Dataset-specific implementations

Delete `roboflow-executor/src/stages/` entirely. Executor provides only traits.

## File Migrations

### Phase 1: Clean roboflow-executor

**Move out**:
```
crates/roboflow-executor/src/stages/convert.rs     → crates/roboflow-pipeline/src/stages/convert.rs
crates/roboflow-executor/src/stages/discover.rs    → crates/roboflow-pipeline/src/stages/discover.rs
crates/roboflow-executor/src/stages/merge.rs       → crates/roboflow-pipeline/src/stages/merge.rs
crates/roboflow-executor/src/stages/transform.rs   → crates/roboflow-pipeline/src/stages/transform.rs
crates/roboflow-executor/src/stages/mod.rs         → DELETE (moved to pipeline)
```

**Update**:
- `crates/roboflow-executor/src/lib.rs` - Remove `pub mod stages` and stage re-exports
- `crates/roboflow-executor/Cargo.toml` - Remove dev-dependency on distributed

### Phase 2: Clean roboflow-distributed

**Delete** (duplicates):
```
crates/roboflow-distributed/src/stages/convert.rs
crates/roboflow-distributed/src/stages/discover.rs
crates/roboflow-distributed/src/stages/merge.rs
crates/roboflow-distributed/src/stages/mod.rs
```

**Move to pipeline**:
```
crates/roboflow-distributed/src/lerobot_executor.rs    → crates/roboflow-pipeline/src/executors/distributed.rs
crates/roboflow-distributed/src/converter/            → crates/roboflow-pipeline/src/converter/
```

**Update**:
- `crates/roboflow-distributed/src/lib.rs` - Remove concrete stage/executor exports
- Add `WorkProcessor` trait usage

### Phase 3: Update roboflow-pipeline

**Create**:
```
crates/roboflow-pipeline/src/stages/mod.rs
crates/roboflow-pipeline/src/processors/mod.rs
crates/roboflow-pipeline/src/executors/mod.rs
```

**Update**:
- `crates/roboflow-pipeline/Cargo.toml` - Add dependency on `roboflow-executor`

## Consequences

### Positive

| Aspect | Before | After |
|--------|--------|-------|
| **Circular deps** | executor (dev) → distributed → executor | Clean DAG |
| **Stage duplication** | Two sets of Convert/Merge/Discover | One set in pipeline |
| **Testability** | Hard to test stages in isolation | Pure executor framework, mockable |
| **Extensibility** | Adding format requires touching multiple crates | Add stages in pipeline only |
| **Control plane** | Knows about LeRobot | Generic over WorkProcessor |

### Trade-offs

| Aspect | Consideration |
|--------|--------------|
| **Complexity** | Additional trait layer (WorkProcessor) |
| **Refactoring** | Large-scale code movement |
| **API stability** | Public API changes for stages |

## Implementation Plan

### Phase 1: roboflow-executor Cleanup

1. Remove `stages/` module from executor
2. Remove stage re-exports from `lib.rs`
3. Remove dev-dependency on distributed
4. Verify executor tests pass

### Phase 2: roboflow-pipeline Enhancement

1. Create `stages/` module
2. Move stages from executor to pipeline
3. Implement executor Stage trait for pipeline stages
4. Create `processors/` module with WorkProcessor implementations
5. Update Cargo.toml to depend on executor

### Phase 3: roboflow-distributed Cleanup

1. Remove duplicate `stages/` directory
2. Replace concrete LeRobotExecutor with generic WorkProcessor
3. Move LeRobotExecutor to pipeline
4. Update all references

### Phase 4: Verification

1. Run full test suite
2. Verify no circular dependencies: `cargo tree`
3. Build all targets: `cargo build --all-targets`
4. Check clippy: `cargo clippy --all-targets -- -D warnings`

## Testing Strategy

### Executor Tests (Pure Framework)

```rust
// Test executor with mock stages
struct MockStage;

impl Stage for MockStage {
    fn execute(&self, ctx: &TaskContext) -> Result<TaskResult> {
        Ok(TaskResult::success())
    }
}

#[test]
fn test_executor_schedules_stages() {
    let executor = StageExecutor::new(4);
    let pipeline = PipelineBuilder::new()
        .stage(Arc::new(MockStage))
        .build()
        .unwrap();
    
    let result = executor.execute(&pipeline).await.unwrap();
    assert_eq!(result.stages_completed, 1);
}
```

### Pipeline Tests (Domain Logic)

```rust
// Test ConvertStage with real data
#[test]
fn test_convert_stage_produces_valid_frames() {
    let stage = ConvertStage::new("test.bag", "/output", "config_hash");
    let ctx = TaskContext::default();
    
    let result = stage.execute(&ctx).unwrap();
    
    assert!(result.frames_written > 0);
    // Verify output format
}
```

### Distributed Tests (Control Plane)

```rust
// Test with mock WorkProcessor
struct MockProcessor;

impl WorkProcessor for MockProcessor {
    type Input = WorkUnit;
    type Output = ();
    type Error = ();
    
    async fn process(&self, _input: Self::Input) -> Result<Self::Output, Self::Error> {
        Ok(())
    }
}

#[test]
fn test_distributed_executor_claims_work() {
    let processor = MockProcessor;
    let executor = DistributedExecutor::new(processor, mock_coordinator());
    
    // Test work claiming, heartbeats, completion
}
```

## References

- [executor-architecture.md](./executor-architecture.md) - Current executor design
- [ADR-001](./adr-001-pipeline-writer-storage-separation.md) - Related writer refactoring
- [data-pipeline-design.md](./data-pipeline-design.md) - Data flow design

## Open Questions

1. **Should WorkProcessor be in executor or core?**
   - Option A: `roboflow-executor` - It's an execution abstraction
   - Option B: `roboflow-core` - It's a shared trait used by multiple crates

2. **What happens to converter/ module?**
   - Option A: Move to pipeline as `converter/`
   - Option B: Merge into stages/ as orchestration logic
   - Option C: Keep in distributed as generic orchestration

3. **Should sources be a separate crate?**
   - Current: In pipeline
   - Alternative: `roboflow-sources` crate

4. **Should media be a separate crate?**
   - Current: In pipeline
   - Alternative: `roboflow-media` crate
