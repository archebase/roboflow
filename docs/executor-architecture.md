# Stage-Based Executor Architecture

## Overview

This document describes the architecture for a distributed task execution system inspired by Apache Spark, Trino, and Ray. The design introduces **stage-based execution** with **lineage-based fault tolerance** to achieve clean, testable, and scalable data pipeline processing.

**IMPORTANT**: No backward compatibility is required. All crates can be restructured, removed, or fully rewritten. This is a clean-sheet design that prioritizes correctness, observability, and maintainability.

## Design Principles

1. **Stage-based execution**: Break execution into well-defined stages separated by shuffle boundaries (from Spark)
2. **Lineage-based recovery**: Track task dependencies for automatic recomputation on failure (from Ray)
3. **Resource-aware scheduling**: Slots-based resource management with explicit resource requirements (from Spark/Ray)
4. **Explicit data flow**: Stage outputs are immutable objects stored in a distributed object store
5. **Testability**: Every component testable in isolation with injectable dependencies
6. **Observability**: Clear visibility into each execution stage with timing, lineage, and metrics
7. **Format as Stage Implementation**: Dataset format defines stage types, enabling compile-time type safety

## Motivation

### Problems with Current Architecture

1. **Tight Coupling**: `TaskExecutor` is tightly coupled to TiKV, making unit testing require real infrastructure
2. **Monolithic Execution**: All pipeline stages run in a single opaque function, hard to debug and optimize
3. **Resource Blindness**: No concept of resource management - tasks compete for CPU/memory without limits
4. **Poor Observability**: Execution is a black box; difficult to identify bottlenecks or stage failures
5. **Testing Complexity**: Tests either mock too much (losing confidence) or require real infrastructure

### Design Goals

1. **Stage-based Execution**: Break complex pipelines into well-defined stages with explicit boundaries (from Spark's DAG stages)
2. **Lineage-based Fault Tolerance**: Track task dependencies for automatic recovery on failure (from Ray's object lineage)
3. **Testability**: Every component testable in isolation with injectable dependencies
4. **Observability**: Clear visibility into each execution stage with timing, metrics, and lineage
5. **Resource Awareness**: Slot-based resource management with explicit resource requirements (from Spark's slot model)
6. **Shuffle/Exchange Support**: Handle data redistribution between stages (from Spark's shuffle and Trino's exchange)
7. **Immutable Objects**: Stage outputs are immutable, content-addressed objects (from Ray's object store)
8. **Composability**: Stages can be chained, branched, or parallelized without code changes

## Architecture

The architecture consists of three layers:
1. **Control Plane**: Coordination and state management (TiKV-based)
2. **Stage Scheduler**: Pipeline composition and DAG scheduling
3. **Task Executor**: Stage execution with resource management and lineage tracking

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CONTROL PLANE                                      │
│  (Coordination, State Management, Scheduling)                               │
│                                                                              │
│  ┌─────────┐  ┌─────────┐  ┌───────────┐  ┌───────────────┐  ┌───────────┐ │
│  │ Scanner │  │ Reaper  │  │ Finalizer │  │BatchController│  │   TiKV    │ │
│  │         │  │         │  │           │  │               │  │           │ │
│  │ Discover│  │ Timeout │  │ Aggregate │  │  Batches      │  │  State    │ │
│  │  Files  │  │  Retry  │  │  Results  │  │  Work Units   │  │  Store    │ │
│  └────┬────┘  └────┬────┘  └─────┬─────┘  └───────┬───────┘  └─────┬─────┘ │
│       │            │             │                │                │       │
│       └────────────┴─────────────┴────────────────┴────────────────┘       │
│                                    │                                         │
└────────────────────────────────────│─────────────────────────────────────────┘
                                     │ Assigns Work Units
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         STAGE SCHEDULER                                      │
│  (DAG Construction, Stage Orchestration, Shuffle Coordination)               │
│                                                                              │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                     Pipeline DAG                                     │   │
│   │                                                                      │   │
│   │   Discover ──► Convert ──► Shuffle ──► Merge                       │   │
│   │   Stage        Stage      (Exchange)   Stage                       │   │
│   │      │            │            │          │                        │   │
│   │      ▼            ▼            ▼          ▼                        │   │
│   │   [Task 1]    [Task 1-N]    [Objects]  [Task 1]                    │   │
│   │   [Output]    [Outputs]                 [Final]                    │   │
│   │       \            |           |          /                        │   │
│   │        \           |           |         /                         │   │
│   │         \          |           |        /                          │   │
│   │          ▼         ▼           ▼       ▼                           │   │
│   │          ┌──────────────────────────────────┐                      │   │
│   │          │      Object Store (Lineage)      │                      │   │
│   │          │  - Content-addressed objects     │                      │   │
│   │          │  - Reference counting            │                      │   │
│   │          │  - Automatic GC                  │                      │   │
│   │          └──────────────────────────────────┘                      │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                     │                                        │
│                                     ▼                                        │
┌─────────────────────────────────────────────────────────────────────────────┐
│                            TASK EXECUTOR                                     │
│  (Resource Management, Lineage Tracking, Task Processing)                    │
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  Slot Pool               Task Registry          Lineage Graph         │  │
│  │  ┌────┬────┬────┐       ┌──────────────┐       ┌────────────────┐    │  │
│  │  │ S1 │ S2 │ S3 │       │ Task-001     │       │  Task 1        │    │  │
│  │  ├────┼────┼────┤       │ Task-002     │◄─────│    ▲  Obj-1    │    │  │
│  │  │ S4 │ S5 │    │       │ Task-003     │       │    │  +-------│----│──┤
│  │  └────┴────┴────┘       └──────────────┘       │    |  |       │    │  │
│  │                                                 │    ▼  ▼       │    │  │
│  │  Resource-aware scheduling                     │  Task 2        │    │  │
│  │  (from Spark slots)                            │    ▲  Obj-2    │    │  │
│  │                                                │    |  +───────┼────│──┤
│  │                                                │    ▼  ▼       │    │  │
│  │                                                │  Task 3        │    │  │
│  │                                                └────────────────┘    │  │
│  │                                                 (from Ray lineage)   │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                     │                                        │
│                                     ▼                                        │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  Injectable Dependencies                                             │  │
│  │  ┌───────────────┐  ┌───────────────┐  ┌───────────────────────────┐ │  │
│  │  │ SourceProvider│  │ ConfigProvider│  │ ObjectStore              │ │  │
│  │  │ (file/S3/mock)│  │ (TiKV/memory) │  │ (memory/distributed)     │ │  │
│  │  └───────────────┘  └───────────────┘  └───────────────────────────┘ │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                     │                                        │
│                                     ▼                                        │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                           Workers                                      │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌──────────────┐  │  │
│  │  │ Worker-1    │  │ Worker-2    │  │ Worker-3    │  │  Worker-N    │  │  │
│  │  │ 4 slots     │  │ 4 slots     │  │ 4 slots     │  │  4 slots     │  │  │
│  │  │ ObjectStore │  │ ObjectStore │  │ ObjectStore │  │ ObjectStore  │  │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └──────────────┘  │  │
│  │                                                                        │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CONTROL PLANE                                      │
│  (Coordination, State Management, Scheduling)                               │
│                                                                              │
│  ┌─────────┐  ┌─────────┐  ┌───────────┐  ┌───────────────┐  ┌───────────┐ │
│  │ Scanner │  │ Reaper  │  │ Finalizer │  │BatchController│  │   TiKV    │ │
│  │         │  │         │  │           │  │               │  │           │ │
│  │ Discover│  │ Timeout │  │ Aggregate │  │  Batches      │  │  State    │ │
│  │  Files  │  │  Retry  │  │  Results  │  │  Work Units   │  │  Store    │ │
│  └────┬────┘  └────┬────┘  └─────┬─────┘  └───────┬───────┘  └─────┬─────┘ │
│       │            │             │                │                │       │
│       └────────────┴─────────────┴────────────────┴────────────────┘       │
│                                    │                                         │
└────────────────────────────────────│─────────────────────────────────────────┘
                                     │ Assigns Work Units
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            DATA PLANE                                        │
│  (Stage Execution, Resource Management, Task Processing)                     │
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                        Stage Scheduler                                 │  │
│  │                                                                        │  │
│  │   ┌─────────┐        ┌──────────┐        ┌─────────┐                  │  │
│  │   │ Discover│  ───►  │ Convert  │  ───►  │  Merge  │                  │  │
│  │   │ Stage   │        │  Stage   │        │  Stage  │                  │  │
│  │   │         │        │          │        │         │                  │  │
│  │   │ Scan    │        │ Process  │        │ Combine │                  │  │
│  │   │ Validate│        │ Encode   │        │ Upload  │                  │  │
│  │   └─────────┘        └──────────┘        └─────────┘                  │  │
│  │                                                                        │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                     │                                        │
│                                     ▼                                        │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                         TaskExecutor                                   │  │
│  │                                                                        │  │
│  │   ┌─────────────┐   ┌──────────────┐   ┌─────────────────┐            │  │
│  │   │  Slot Pool  │   │Task Registry │   │ Pipeline Runner │            │  │
│  │   │             │   │              │   │                 │            │  │
│  │   │ [Slot 1]    │   │ Task-001     │   │ Initialize      │            │  │
│  │   │ [Slot 2]    │   │ Task-002     │   │ Process         │            │  │
│  │   │ [Slot 3]    │   │ Task-003     │   │ Finalize        │            │  │
│  │   │ [Slot 4]    │   │ ...          │   │ Collect Stats   │            │  │
│  │   └─────────────┘   └──────────────┘   └─────────────────┘            │  │
│  │                                                                        │  │
│  │   ┌─────────────────────────────────────────────────────────────┐     │  │
│  │   │              Injectable Dependencies                         │     │  │
│  │   │                                                              │     │  │
│  │   │  SourceProvider  │  ConfigProvider  │  JobRegistry          │     │  │
│  │   │  (file/S3/mock)  │  (TiKV/memory)   │  (cancel/monitor)     │     │  │
│  │   └─────────────────────────────────────────────────────────────┘     │  │
│  │                                                                        │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                     │                                        │
│                                     ▼                                        │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                           Workers                                      │  │
│  │                                                                        │  │
│  │   ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐              │  │
│  │   │ Worker1 │   │ Worker2 │   │ Worker3 │   │ WorkerN │              │  │
│  │   │ 2 slots │   │ 2 slots │   │ 4 slots │   │ 2 slots │              │  │
│  │   └─────────┘   └─────────┘   └─────────┘   └─────────┘              │  │
│  │                                                                        │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Core Abstractions

### 0. DatasetFormat — Type-Safe Format Definition

A **DatasetFormat** defines the complete type hierarchy for a specific dataset format. It acts as a compile-time contract ensuring all stages in a pipeline produce compatible outputs.

```rust
/// Dataset format definition with associated types.
/// Implement this trait for each supported format (LeRobot, RLDS, HDF5, etc.)
pub trait DatasetFormat: Send + Sync + 'static {
    /// Format name (e.g., "lerobot", "rlds")
    const NAME: &'static str;
    
    /// Format version (e.g., "v2.1")
    const VERSION: &'static str;
    
    /// Writer type for this format
    type Writer: EpisodeWriter;
    
    /// Metadata generator type
    type MetadataGenerator: MetadataGenerator;
    
    /// Format-specific configuration
    type Config: FormatConfig;
    
    /// Episode metadata type
    type EpisodeMetadata: Serialize + DeserializeOwned;
    
    /// Dataset metadata type  
    type DatasetMetadata: Serialize + DeserializeOwned;
}

/// Example: LeRobot v2.1 format definition
pub struct LeRobotV21;

impl DatasetFormat for LeRobotV21 {
    const NAME: &'static str = "lerobot";
    const VERSION: &'static str = "v2.1";
    
    type Writer = LeRobotWriter;
    type MetadataGenerator = LeRobotMetadataGenerator;
    type Config = LeRobotConfig;
    type EpisodeMetadata = LeRobotEpisodeMeta;
    type DatasetMetadata = LeRobotDatasetMeta;
}

/// Example: RLDS format definition
pub struct RLDS;

impl DatasetFormat for RLDS {
    const NAME: &'static str = "rlds";
    const VERSION: &'static str = "0.1.0";
    
    type Writer = RLDSWriter;
    type MetadataGenerator = RLDSMetadataGenerator;
    type Config = RLDSConfig;
    type EpisodeMetadata = RLDSEpisodeMeta;
    type DatasetMetadata = RLDSDatasetMeta;
}
```

### 1. Stage — Execution Boundary (from Spark/Trino)

A **Stage** is a logical grouping of tasks that can execute in parallel without cross-task data exchange. Stages are separated by **shuffles** (data redistribution, from Spark) or **exchanges** (from Trino).

**Key Design**: Stages are **generic over DatasetFormat**, providing compile-time type safety.

```rust
/// Stage represents a phase in the execution pipeline.
/// Generic over DatasetFormat for type-safe format-specific implementations.
pub trait Stage<F: DatasetFormat>: Send + Sync {
    /// Stage identifier (unique within pipeline).
    fn id(&self) -> StageId;
    
    /// Stage name for observability.
    fn name(&self) -> &str;
    
    /// Number of output partitions (parallelism).
    fn partition_count(&self) -> usize;
    
    /// Create a task for a specific partition.
    fn create_task(&self, partition: PartitionId) -> Box<dyn Task<F>>;
    
    /// Input dependencies (object refs from previous stages).
    fn inputs(&self) -> Vec<ObjectRef>;
    
    /// Stage-level resources required.
    fn resource_profile(&self) -> ResourceProfile;
    
    /// Shuffle strategy for output (None = no shuffle).
    fn shuffle(&self) -> Option<ShuffleSpec>;
    
    /// Whether this stage can be retried on failure.
    fn is_deterministic(&self) -> bool;
}

/// Marker trait for format-specific stages
pub trait FormatStage<F: DatasetFormat>: Stage<F> {
    fn format(&self) -> &'static dyn DatasetFormat {
        &F::NAME
    }
}

/// Shuffle specification for data redistribution between stages.
/// From Spark: partition by key, sort, aggregate.
#[derive(Debug, Clone)]
pub struct ShuffleSpec {
    /// How to partition output.
    pub partition_by: PartitionStrategy,
    /// Sort within each partition.
    pub sort_by: Option<Vec<SortKey>>,
    /// Buffer size before spilling to disk.
    pub buffer_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StageId(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PartitionId(u64);
```

### 2. Task — Atomic Work Unit (from Ray)

A **Task** is the smallest unit of work. It's deterministic, retryable, and trackable via lineage. Tasks consume input **ObjectRefs** and produce output **ObjectRefs**.

**Key Design**: Tasks are **generic over DatasetFormat**, enabling format-specific operations.

```rust
/// Task is an atomic, idempotent unit of work.
/// Generic over DatasetFormat for type-safe format-specific processing.
pub trait Task<F: DatasetFormat>: Send + Sync {
    /// Task identifier (unique globally).
    fn id(&self) -> TaskId;
    
    /// Which stage this task belongs to.
    fn stage_id(&self) -> StageId;
    
    /// Which partition this task processes.
    fn partition(&self) -> PartitionId;
    
    /// Input object references.
    fn inputs(&self) -> Vec<ObjectRef>;
    
    /// Execute the task. Must be idempotent (retryable).
    async fn execute(&self, ctx: &TaskContext<F>) -> TaskResult;
    
    /// Resource requirements for this task.
    fn resources(&self) -> ResourceRequest;
    
    /// Estimated cost for scheduling (duration, cpu, memory).
    fn estimated_cost(&self) -> Cost;
    
    /// Lineage info for recovery.
    fn lineage(&self) -> TaskLineage;
    
    /// Get format-specific metadata output
    fn episode_metadata(&self) -> Option<F::EpisodeMetadata> {
        None
    }
}

/// Task execution context with format-specific capabilities.
pub struct TaskContext<F: DatasetFormat> {
    /// Slot assigned to this task.
    pub slot: SlotId,
    /// Object store for intermediate data.
    pub object_store: Arc<dyn ObjectStore>,
    /// Cancellation token.
    pub cancel: CancellationToken,
    /// Metrics recorder.
    pub metrics: TaskMetrics,
    /// Format-specific writer (for TransformStage)
    pub writer: Option<Arc<F::Writer>>,
    /// Format configuration
    pub config: F::Config,
}

/// Task execution result.
pub struct TaskResult {
    /// Output objects (references).
    pub outputs: Vec<ObjectRef>,
    /// Metrics collected during execution.
    pub metrics: TaskMetrics,
    /// Task status.
    pub status: TaskStatus,
}
```

### 3. ObjectRef — Content-Addressed Output (from Ray)

**ObjectRef** represents an immutable, content-addressed output object stored in the distributed object store. This enables:
- **Lineage tracking**: Track which task produced which object
- **Automatic recovery**: Recompute lost objects from lineage
- **Deduplication**: Same inputs produce same object (deterministic)
- **Reference counting**: Automatic garbage collection

```rust
/// Object reference for lineage tracking and distributed object store.
/// From Ray's object store model.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct ObjectRef {
    /// Object ID (content-addressed: hash of data + task inputs).
    pub id: ObjectId,
    /// Size in bytes.
    pub size: u64,
    /// Owner task (for lineage tracking).
    pub owner: TaskId,
    /// Location hints (which workers have this object).
    pub locations: Vec<WorkerId>,
}

/// Object store for intermediate data between stages.
pub trait ObjectStore: Send + Sync {
    /// Get object by reference.
    async fn get(&self, obj: &ObjectRef) -> Result<Vec<u8>>;
    
    /// Put object into store, returns reference.
    async fn put(&self, data: Vec<u8>, owner: TaskId) -> Result<ObjectRef>;
    
    /// Check if object exists.
    async fn contains(&self, obj: &ObjectRef) -> bool;
    
    /// Add reference (increment ref count).
    async fn add_ref(&self, obj: &ObjectRef);
    
    /// Remove reference (decrement ref count, may GC).
    async fn remove_ref(&self, obj: &ObjectRef);
}
```

### 4. Slot — Resource Unit (from Spark)

A **Slot** represents a resource allocation unit. Each slot can run one task at a time. This model comes from Spark's executor slots.

```rust
/// Slot represents a resource allocation for task execution.
/// From Spark's slot model: each executor has N slots.
#[derive(Debug, Clone)]
pub struct Slot {
    /// Slot identifier.
    pub id: SlotId,
    /// Worker this slot belongs to.
    pub worker_id: WorkerId,
    /// Current state.
    pub state: SlotState,
    /// Current task (if occupied).
    pub task: Option<TaskId>,
    /// Resource capacity.
    pub capacity: ResourceCapacity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    /// Available for task assignment.
    Free,
    /// Reserved for a specific task.
    Reserved,
    /// Currently executing a task.
    Busy,
    /// Draining (no new tasks).
    Draining,
}

/// Resource capacity of a slot.
pub struct ResourceCapacity {
    pub cpu_cores: f64,
    pub memory_gb: f64,
    pub gpu_count: u32,
}

/// Slot pool manages available slots per worker.
pub struct SlotPool {
    slots: Vec<Slot>,
    policy: SlotPolicy,
}

impl SlotPool {
    /// Acquire a slot for a task (blocking if none available).
    pub async fn acquire(&self, request: &ResourceRequest) -> Option<SlotGuard>;
    
    /// Get current slot utilization.
    pub fn utilization(&self) -> f64;
    
    /// Release a slot back to the pool.
    pub fn release(&self, slot_id: SlotId);
}
```

### 5. Lineage — Fault Tolerance (from Ray)

**Lineage** tracks the dependency graph of tasks, enabling automatic recomputation on failure. This is Ray's key fault tolerance mechanism.

```rust
/// Lineage tracks task dependencies for recovery.
/// From Ray's lineage-based fault tolerance.
pub trait Lineage: Send + Sync {
    /// Record a task's lineage info.
    fn record(&self, task: &TaskLineage);
    
    /// Get all ancestors of a task.
    fn ancestors(&self, task_id: TaskId) -> Vec<TaskId>;
    
    /// Check if a task can be recomputed (all inputs available).
    fn can_recompute(&self, task_id: TaskId) -> bool;
    
    /// Get recompute plan for failed tasks.
    fn recompute_plan(&self, failed: &[TaskId]) -> Vec<TaskId>;
    
    /// Recompute a lost object from lineage.
    async fn recompute_object(&self, obj: &ObjectRef) -> Result<Vec<u8>>;
}

/// Task lineage information.
#[derive(Debug, Clone)]
pub struct TaskLineage {
    /// Task identifier.
    pub task_id: TaskId,
    /// Function/operation name.
    pub operation: String,
    /// Input object references.
    pub inputs: Vec<ObjectRef>,
    /// Output object references.
    pub outputs: Vec<ObjectRef>,
    /// Deterministic flag (if false, cannot recompute).
    pub deterministic: bool,
    /// Stage this task belongs to.
    pub stage_id: StageId,
    /// Timestamp when recorded.
    pub recorded_at: DateTime<Utc>,
}
```

### 6. Pipeline — Stage Composition (from Spark DAG)

A **Pipeline** is a DAG of stages that defines the complete execution plan. **Pipelines are generic over DatasetFormat**, ensuring compile-time type safety across all stages.

```rust
/// Pipeline is a DAG of stages parameterized by DatasetFormat.
/// All stages in the pipeline must agree on the format type.
pub struct Pipeline<F: DatasetFormat> {
    /// All stages in this pipeline.
    stages: Vec<Arc<dyn Stage<F>>>,
    /// Stage dependency graph.
    dag: Dag<StageId>,
    /// Pipeline metadata.
    metadata: PipelineMetadata,
    /// Phantom marker for format type.
    _format: PhantomData<F>,
}

impl<F: DatasetFormat> Pipeline<F> {
    /// Create a new pipeline from stages.
    pub fn new(stages: Vec<Arc<dyn Stage<F>>>) -> Result<Self, PipelineError>;
    
    /// Get stages in topological order.
    pub fn stages(&self) -> &[Arc<dyn Stage<F>>];
    
    /// Get ready stages (all dependencies complete).
    pub fn ready_stages(&self, completed: &HashSet<StageId>) -> Vec<StageId>;
    
    /// Validate the pipeline DAG.
    pub fn validate(&self) -> Result<(), PipelineError>;
    
    /// Estimate total resource requirements.
    pub fn resource_estimate(&self) -> ResourceProfile;
    
    /// Get the format identifier.
    pub fn format(&self) -> &'static str {
        F::NAME
    }
}

/// Type-safe pipeline builder.
pub struct PipelineBuilder<F: DatasetFormat> {
    stages: Vec<Arc<dyn Stage<F>>>,
    edges: Vec<(StageId, StageId)>,
}

impl<F: DatasetFormat> PipelineBuilder<F> {
    /// Create a builder for a specific format.
    pub fn new() -> Self {
        Self {
            stages: Vec::new(),
            edges: Vec::new(),
        }
    }
    
    /// Add a stage to the pipeline.
    pub fn stage(mut self, stage: Arc<dyn Stage<F>>) -> Self {
        self.stages.push(stage);
        self
    }
    
    /// Add dependency: `from` must complete before `to`.
    pub fn dependency(mut self, from: StageId, to: StageId) -> Self {
        self.edges.push((from, to));
        self
    }
    
    /// Build the pipeline.
    pub fn build(self) -> Result<Pipeline<F>, PipelineError> {
        Pipeline::new(self.stages)
    }
}
```

## Component Details

### Control Plane

The Control Plane handles coordination and state management using TiKV for distributed consistency.

| Component | Responsibility |
|-----------|---------------|
| **Scanner** | Discovers files from S3/OSS, validates accessibility, creates WorkUnits |
| **Reaper** | Detects stale work (timeout), reclaims zombie tasks, handles retries |
| **Finalizer** | Aggregates results, writes LeRobot metadata (info.json, episodes.jsonl, etc.) |
| **BatchController** | Manages batch lifecycle, phase transitions, work distribution |
| **TiKV** | Distributed state store for coordination, episode allocation, checkpoints |

### Stage Scheduler

The Stage Scheduler orchestrates execution of a DAG of stages. It:
1. Determines which stages are ready to execute (dependencies satisfied)
2. Schedules tasks within each stage to available slots
3. Manages shuffle/exchange operations between stages
4. Handles stage-level retries and fault recovery

```
┌──────────────────────────────────────────────────────────────────────────┐
│                      Stage Scheduler                                      │
│                                                                           │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │                     Pipeline DAG                                   │  │
│  │                                                                   │  │
│  │   Stage 0 (Discover)                                              │  │
│  │   ┌─────────────────┐                                             │  │
│  │   │  Scan Source    │                                             │  │
│  │   │  Validate Files │                                             │  │
│  │   │  Create Tasks   │                                             │  │
│  │   └────────┬────────┘                                             │  │
│  │            │                                                        │  │
│  │            ▼  Shuffle: partition by file hash                       │  │
│  │   Stage 1 (Convert)        Stage 1 (Convert)       Stage 1         │  │
│  │   ┌─────────────────┐     ┌─────────────────┐     ┌──────────────┐ │  │
│  │   │ Process files   │     │ Process files   │     │ Process ...  │ │  │
│  │   │ 0-99            │     │ 100-199         │     │ 200-299      │ │  │
│  │   │                 │     │                 │     │              │ │  │
│  │   │ Output: Objects │     │ Output: Objects │     │ Output: ...  │ │  │
│  │   └────────┬────────┘     └────────┬────────┘     └──────┬───────┘ │  │
│  │            │                       │                      │         │  │
│  │            ▼                       ▼                      ▼         │  │
│  │            └───────────────────────┼──────────────────────┘         │  │
│  │                                    │ Shuffle: gather to single task │  │
│  │                                    ▼                                 │  │
│  │   Stage 2 (Merge)                                                   │  │
│  │   ┌───────────────────────────────────────────────────────────────┐ │  │
│  │   │  Collect all objects                                          │ │  │
│  │   │  Concatenate videos                                           │ │  │
│  │   │  Merge parquet files                                          │ │  │
│  │   │  Write LeRobot metadata                                       │ │  │
│  │   │    - info.json                                                │ │  │
│  │   │    - episodes.jsonl                                           │ │  │
│  │   │    - episodes_stats.jsonl                                     │ │  │
│  │   │    - tasks.jsonl                                              │ │  │
│  │   └───────────────────────────────────────────────────────────────┘ │  │
│  │                                                                     │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                           │
│  Key Concepts:                                                            │
│  - Stages execute sequentially (DAG order)                                │
│  - Tasks within a stage execute in parallel (partitioned)                 │
│  - Shuffle boundaries redistribute data between stages                    │
│  - Objects are immutable and content-addressed                            │
└──────────────────────────────────────────────────────────────────────────┘
```

#### Stage Execution Flow

```rust
/// Stage scheduler orchestrates stage execution.
pub struct StageScheduler {
    /// Object store for intermediate data.
    object_store: Arc<dyn ObjectStore>,
    /// Lineage tracker for fault tolerance.
    lineage: Arc<dyn Lineage>,
    /// Slot pool for resource management.
    slot_pool: Arc<SlotPool>,
    /// Task executor.
    executor: Arc<TaskExecutor>,
}

impl StageScheduler {
    /// Execute a pipeline to completion.
    pub async fn execute_pipeline(
        &self,
        pipeline: &Pipeline,
    ) -> Result<PipelineResult, SchedulerError> {
        let mut completed_stages = HashSet::new();
        let mut stage_outputs: HashMap<StageId, Vec<ObjectRef>> = HashMap::new();
        
        while let Some(ready_stages) = pipeline.ready_stages(&completed_stages) {
            if ready_stages.is_empty() {
                break;
            }
            
            // Execute ready stages in parallel
            for stage_id in ready_stages {
                let stage = pipeline.get_stage(stage_id)?;
                let outputs = self.execute_stage(stage, &stage_outputs).await?;
                stage_outputs.insert(stage_id, outputs);
                completed_stages.insert(stage_id);
            }
        }
        
        // Collect final outputs from terminal stages
        let final_outputs = pipeline.terminal_stages()
            .iter()
            .flat_map(|s| stage_outputs.get(&s.id()).unwrap_or(&vec![]).clone())
            .collect();
        
        Ok(PipelineResult { outputs: final_outputs })
    }
    
    /// Execute a single stage.
    async fn execute_stage(
        &self,
        stage: &dyn Stage,
        previous_outputs: &HashMap<StageId, Vec<ObjectRef>>,
    ) -> Result<Vec<ObjectRef>, StageError> {
        let partition_count = stage.partition_count();
        let mut tasks: Vec<Box<dyn Task>> = Vec::new();
        
        // Create tasks for all partitions
        for partition_id in 0..partition_count {
            let task = stage.create_task(PartitionId(partition_id as u64));
            tasks.push(task);
        }
        
        // Execute tasks in parallel (bounded by slot pool)
        let results: Vec<TaskResult> = futures::stream::iter(tasks)
            .map(|task| self.executor.execute_task(task))
            .buffer_unordered(self.slot_pool.available())
            .try_collect()
            .await?;
        
        // Collect outputs
        let outputs: Vec<ObjectRef> = results
            .into_iter()
            .flat_map(|r| r.outputs)
            .collect();
        
        // Handle shuffle if specified
        if let Some(shuffle_spec) = stage.shuffle() {
            self.shuffle_outputs(&outputs, shuffle_spec).await?
        } else {
            Ok(outputs)
        }
    }
}
```

## Concrete Stages for Roboflow

The following are the concrete stage implementations for converting robotics bag/MCAP files to dataset formats. **Stages are format-specific** - each `DatasetFormat` has its own stage implementations.

### Format-Specific Stage Pattern

```rust
/// Discover stage is format-agnostic (same for all formats)
pub struct DiscoverStage;

impl<F: DatasetFormat> Stage<F> for DiscoverStage {
    // Implementation works for any format
}

/// Transform stage is format-specific
pub struct TransformStage<F: DatasetFormat> {
    writer: Arc<F::Writer>,
    config: F::Config,
}

impl<F: DatasetFormat> Stage<F> for TransformStage<F> {
    fn create_task(&self, partition: PartitionId) -> Box<dyn Task<F>> {
        Box::new(TransformTask::<F> {
            writer: self.writer.clone(),
            config: self.config.clone(),
            partition,
        })
    }
    
    fn resource_profile(&self) -> ResourceProfile {
        F::Writer::resource_profile()  // Format-specific resources
    }
}

/// Metadata stage is format-specific
pub struct MetadataStage<F: DatasetFormat> {
    generator: Arc<F::MetadataGenerator>,
}

impl<F: DatasetFormat> Stage<F> for MetadataStage<F> {
    // Uses F::MetadataGenerator to create format-specific metadata
}
```

### Stage 0: DiscoverStage (Format-Agnostic)

**Purpose**: Scan source storage and create work units for each file. This stage is **the same for all formats**.

**Input**: Source URL (s3://bucket/input/)
**Output**: List of files as `ObjectRef`s
**Parallelism**: 1 (single discovery task)
**Shuffle**: Hash partition by file path → N TransformStage tasks

```rust
/// Stage 0: File Discovery
/// Generic over F but doesn't use format-specific features
pub struct DiscoverStage {
    source_prefix: String,
    config_hash: String,
}

impl<F: DatasetFormat> Stage<F> for DiscoverStage {
    fn id(&self) -> StageId { StageId(0) }
    fn name(&self) -> &str { "discover" }
    fn partition_count(&self) -> usize { 1 }
    
    fn create_task(&self, partition: PartitionId) -> Box<dyn Task<F>> {
        Box::new(DiscoverTask {
            source_prefix: self.source_prefix.clone(),
            config_hash: self.config_hash.clone(),
        })
    }
    
    fn shuffle(&self) -> Option<ShuffleSpec> {
        Some(ShuffleSpec {
            partition_by: PartitionStrategy::Hash("file_path"),
            sort_by: None,
            buffer_size: 10000,
        })
    }
}

impl<F: DatasetFormat> Task<F> for DiscoverTask {
    async fn execute(&self, ctx: &TaskContext<F>) -> TaskResult {
        let files = list_files(&self.source_prefix).await?;
        let valid_files: Vec<_> = files.into_iter()
            .filter(|f| validate_file(f).await.unwrap_or(false))
            .collect();
        
        let obj_ref = ctx.object_store.put(serialize(&valid_files)?, self.id()).await?;
        
        TaskResult {
            outputs: vec![obj_ref],
            metrics: ctx.metrics.clone(),
            status: TaskStatus::Success,
        }
    }
}
```

### Stage 1: TransformStage (Format-Specific)

**Purpose**: Transform bag/MCAP files → episodes. This stage is **format-specific** - each format has its own implementation.

**Input**: File list from DiscoverStage (via ObjectRef)
**Output**: Episode metadata as `ObjectRef`s (lightweight, not video data)
**Parallelism**: N (configurable based on format)
**Shuffle**: Gather → single MetadataStage task

**Key Design Points**:
- One bag/mcap file = One episode
- Video encoding happens locally (no network I/O for frames)
- Only lightweight metadata flows through object store
- Format-specific writers implement `EpisodeWriter` trait

```rust
/// Stage 1: Transform input files to episodes (generic)
pub struct TransformStage<F: DatasetFormat> {
    writer: Arc<F::Writer>,
    config: F::Config,
    output_prefix: String,
    partition_count: usize,
}

impl<F: DatasetFormat> Stage<F> for TransformStage<F> {
    fn id(&self) -> StageId { StageId(1) }
    fn name(&self) -> &str { "transform" }
    fn dependencies(&self) -> Vec<StageId> { vec![StageId(0)] }
    fn partition_count(&self) -> usize { self.partition_count }
    
    fn create_task(&self, partition: PartitionId) -> Box<dyn Task<F>> {
        Box::new(TransformTask::<F> {
            writer: self.writer.clone(),
            config: self.config.clone(),
            output_prefix: self.output_prefix.clone(),
            partition,
        })
    }
    
    fn resource_profile(&self) -> ResourceProfile {
        // Format-specific resource requirements
        F::Writer::resource_profile()
    }
    
    fn shuffle(&self) -> Option<ShuffleSpec> {
        // Gather: all outputs go to single metadata stage
        Some(ShuffleSpec {
            partition_by: PartitionStrategy::Gather,
            sort_by: None,
            buffer_size: 10000,
        })
    }
}

impl<F: DatasetFormat> Task<F> for TransformTask<F> {
    async fn execute(&self, ctx: &TaskContext<F>) -> TaskResult {
        let input_obj = self.inputs()[0];
        let input_data = ctx.object_store.get(&input_obj).await?;
        let files: Vec<FileInfo> = deserialize(&input_data)?;
        
        let mut outputs = Vec::new();
        for file in files_for_partition(&files, self.partition) {
            // Allocate episode index via TiKV
            let episode_alloc = allocate_episode_index().await?;
            
            // Process bag file → episode (format-specific writer)
            let metadata = self.process_file(&file, episode_alloc, ctx).await?;
            
            // Store only lightweight metadata (not video data!)
            let obj_ref = ctx.object_store.put(
                serialize(&metadata)?, 
                self.id()
            ).await?;
            outputs.push(obj_ref);
        }
        
        TaskResult {
            outputs,
            metrics: ctx.metrics.clone(),
            status: TaskStatus::Success,
        }
    }
    
    async fn process_file(
        &self,
        file: &FileInfo,
        episode_alloc: EpisodeAllocation,
        ctx: &TaskContext,
    ) -> Result<EpisodeOutput, TaskError> {
        // Setup LerobotWriter with episode allocation
        let writer = LerobotWriter::new(&self.output_prefix, self.config.clone())
            .with_episode_index(episode_alloc.episode_index)
            .with_episodes_per_chunk(self.episodes_per_chunk);
        
        // Create source from file
        let source = create_source(&file.url).await?;
        
        // Run pipeline with memory-bounded encoding
        let executor = PipelineExecutor::new(writer, self.config.streaming.clone());
        let stats = executor.run(source, ctx.cancel.clone()).await?;
        
        Ok(EpisodeOutput {
            episode_index: episode_alloc.episode_index,
            chunk_index: episode_alloc.chunk_index,
            parquet_path: writer.parquet_path(),
            video_paths: writer.video_paths(),
            frame_count: stats.frames_written,
            metadata: EpisodeMetadata {
                episode_index: episode_alloc.episode_index,
                tasks: vec![file.task_description.clone()],
                length: stats.frames_written,
            },
        })
    }
}
```

### Stage 2: MetadataStage (Format-Specific)

**Purpose**: Collect all episode metadata and write format-specific dataset metadata files. This stage is **format-specific** - each format generates its own metadata structure.

**Input**: Episode metadata from all TransformStage partitions
**Output**: Format-specific dataset metadata files
**Parallelism**: 1 (single task - only processing metadata)
**Shuffle**: None (final stage)

**Key Design Points**:
- Only processes lightweight metadata (~100KB-10MB), not video data
- Format-specific metadata generator creates appropriate files
- For LeRobot: writes info.json, episodes.jsonl, episodes_stats.jsonl, tasks.jsonl
- For other formats: writes their respective metadata files

```rust
/// Stage 2: Generate format-specific metadata
pub struct MetadataStage<F: DatasetFormat> {
    generator: Arc<F::MetadataGenerator>,
    output_path: String,
}

impl<F: DatasetFormat> Stage<F> for MetadataStage<F> {
    fn id(&self) -> StageId { StageId(2) }
    fn name(&self) -> &str { "metadata" }
    fn dependencies(&self) -> Vec<StageId> { vec![StageId(1)] }
    fn partition_count(&self) -> usize { 1 } // Single metadata aggregation task
    
    fn create_task(&self, partition: PartitionId) -> Box<dyn Task> {
        Box::new(MergeTask {
            output_path: self.output_path.clone(),
            temp_dir: self.temp_dir.clone(),
        })
    }
    
    fn shuffle(&self) -> Option<ShuffleSpec> {
        None // Final stage, no shuffle
    }
}

impl MergeTask {
    async fn execute(&self, ctx: &TaskContext) -> TaskResult {
        // 1. Get all episode outputs from object store
        let mut episodes = Vec::new();
        for input_obj in &self.inputs() {
            let data = ctx.object_store.get(input_obj).await?;
            let episode: EpisodeOutput = deserialize(&data)?;
            episodes.push(episode);
        }
        
        // 2. Sort episodes by episode_index
        episodes.sort_by_key(|e| e.episode_index);
        
        // 3. Merge parquet files
        self.merge_parquet_files(&episodes).await?;
        
        // 4. Concatenate video segments per camera
        self.concatenate_videos(&episodes).await?;
        
        // 5. Write LeRobot metadata files
        self.write_info_json(&episodes).await?;
        self.write_episodes_jsonl(&episodes).await?;
        self.write_episodes_stats_jsonl(&episodes).await?;
        self.write_tasks_jsonl(&episodes).await?;
        
        // 6. Return final dataset reference
        let output_data = serialize(&DatasetOutput {
            output_path: self.output_path.clone(),
            total_episodes: episodes.len(),
            total_frames: episodes.iter().map(|e| e.frame_count).sum(),
        })?;
        let obj_ref = ctx.object_store.put(output_data, self.id()).await?;
        
        TaskResult {
            outputs: vec![obj_ref],
            metrics: ctx.metrics.clone(),
            status: TaskStatus::Success,
        }
    }
    
    async fn write_info_json(&self, episodes: &[EpisodeOutput]) -> Result<()> {
        // Compute feature statistics across all episodes
        let features = compute_feature_statistics(episodes)?;
        
        let info = LeRobotInfo {
            codebase_version: "v2.1".to_string(),
            robot_type: self.config.robot_type.clone(),
            fps: self.config.fps,
            total_episodes: episodes.len() as u64,
            total_frames: episodes.iter().map(|e| e.frame_count).sum(),
            total_tasks: count_unique_tasks(episodes),
            total_videos: count_total_videos(episodes),
            splits: json!({"train": format!("0:{}", episodes.len())}),
            features,
            camera_keys: extract_camera_keys(episodes),
        };
        
        let info_json = serde_json::to_string_pretty(&info)?;
        write_file(&format!("{}/meta/info.json", self.output_path), info_json).await?;
        
        Ok(())
    }
    
    async fn write_episodes_jsonl(&self, episodes: &[EpisodeOutput]) -> Result<()> {
        let mut lines = Vec::new();
        for ep in episodes {
            let record = json!({
                "episode_index": ep.episode_index,
                "tasks": ep.metadata.tasks,
                "length": ep.frame_count,
            });
            lines.push(serde_json::to_string(&record)?);
        }
        
        let content = lines.join("\n");
        write_file(&format!("{}/meta/episodes.jsonl", self.output_path), content).await?;
        
        Ok(())
    }
    
    async fn write_episodes_stats_jsonl(&self, episodes: &[EpisodeOutput]) -> Result<()> {
        let mut lines = Vec::new();
        for ep in episodes {
            let stats = compute_episode_stats(ep)?;
            let record = json!({
                "episode_index": ep.episode_index,
                "stats": stats,
            });
            lines.push(serde_json::to_string(&record)?);
        }
        
        let content = lines.join("\n");
        write_file(&format!("{}/meta/episodes_stats.jsonl", self.output_path), content).await?;
        
        Ok(())
    }
}
```

### Building the Pipeline

```rust
/// Build the roboflow pipeline for converting bag files to LeRobot format.
pub fn roboflow_pipeline(
    source_prefix: String,
    output_prefix: String,
    config: LerobotConfig,
) -> Result<Pipeline, PipelineError> {
    PipelineBuilder::new()
        .stage(Arc::new(DiscoverStage { 
            source_prefix, 
            config_hash: config.hash() 
        }))
        .stage(Arc::new(ConvertStage { 
            config: config.clone(), 
            output_prefix: output_prefix.clone(),
            episodes_per_chunk: 500, // LeRobot v2.1 default
        }))
        .stage(Arc::new(MergeStage { 
            output_path: output_prefix, 
            temp_dir: PathBuf::from("/tmp") 
        }))
        .dependency(StageId(0), StageId(1))
        .dependency(StageId(1), StageId(2))
        .build()
}
```

#### Shuffle/Exchange Operations

Data redistribution between stages is handled via **shuffle** (Spark terminology) or **exchange** (Trino terminology). This is necessary when stage N needs data from multiple partitions of stage N-1.

```rust
/// Shuffle operation redistributes data between stages.
pub struct ShuffleOperation {
    /// Input objects from previous stage.
    inputs: Vec<ObjectRef>,
    /// Shuffle specification.
    spec: ShuffleSpec,
    /// Object store for intermediate data.
    object_store: Arc<dyn ObjectStore>,
}

impl ShuffleOperation {
    /// Execute shuffle: read all inputs, partition, write to object store.
    pub async fn execute(&self) -> Result<Vec<ObjectRef>, ShuffleError> {
        // 1. Read all input objects
        let mut all_data: Vec<Record> = Vec::new();
        for input in &self.inputs {
            let data = self.object_store.get(input).await?;
            let records: Vec<Record> = deserialize(&data)?;
            all_data.extend(records);
        }
        
        // 2. Partition records according to strategy
        let partitioned = self.partition_records(all_data)?;
        
        // 3. Write each partition as new object
        let mut output_refs = Vec::new();
        for partition in partitioned {
            let data = serialize(&partition)?;
            let obj_ref = self.object_store.put(data, TaskId::shuffle()).await?;
            output_refs.push(obj_ref);
        }
        
        Ok(output_refs)
    }
    
    fn partition_records(&self, records: Vec<Record>) -> Result<Vec<Vec<Record>>, ShuffleError> {
        match &self.spec.partition_by {
            PartitionStrategy::Hash(column) => {
                // Hash partitioning: records with same key go to same partition
                let num_partitions = self.spec.num_partitions;
                let mut partitions: Vec<Vec<Record>> = 
                    (0..num_partitions).map(|_| Vec::new()).collect();
                
                for record in records {
                    let key = record.get(column)?;
                    let hash = hash_key(key);
                    let partition_idx = hash % num_partitions as u64;
                    partitions[partition_idx as usize].push(record);
                }
                
                Ok(partitions)
            }
            PartitionStrategy::Range(column) => {
                // Range partitioning: sort by column, then split evenly
                let mut records = records;
                records.sort_by_key(|r| r.get(column).unwrap_or_default());
                
                let num_partitions = self.spec.num_partitions;
                let chunk_size = records.len() / num_partitions;
                
                let mut partitions = Vec::new();
                for i in 0..num_partitions {
                    let start = i * chunk_size;
                    let end = if i == num_partitions - 1 {
                        records.len()
                    } else {
                        (i + 1) * chunk_size
                    };
                    partitions.push(records[start..end].to_vec());
                }
                
                Ok(partitions)
            }
            PartitionStrategy::Gather => {
                // Gather: all records to single partition
                Ok(vec![records])
            }
        }
    }
}
```

#### TaskExecutor

The executor manages task execution with:

1. **Slot Pool**: Limits concurrent resource usage
2. **Task Registry**: Tracks active tasks for cancellation
3. **Injectable Dependencies**: Enables testing without infrastructure

```rust
/// Task executor with resource management and injectable dependencies.
pub struct TaskExecutor<SP, CP, JR>
where
    SP: SourceProvider,     // Creates data sources
    CP: ConfigProvider,     // Loads configurations
    JR: JobRegistry,        // Manages job lifecycle
{
    /// Provider for creating data sources (file, S3, mock).
    source_provider: SP,

    /// Provider for loading configurations (TiKV, memory).
    config_provider: CP,

    /// Registry for job cancellation and monitoring.
    job_registry: JR,

    /// Available execution slots.
    slot_pool: SlotPool,

    /// Registry of active tasks.
    task_registry: TaskRegistry,

    /// Stage pipeline to execute.
    stages: Vec<Box<dyn Stage>>,

    /// Default timeout for tasks.
    timeout: Duration,
}
```

#### Injectable Dependencies

All external dependencies are injected via traits:

```rust
/// Trait for creating data sources.
#[async_trait]
pub trait SourceProvider: Send + Sync + 'static {
    /// Create a source from the given configuration.
    async fn create_source(&self, config: &SourceConfig) -> Result<Box<dyn Source>>;
}

/// Trait for loading configurations.
#[async_trait]
pub trait ConfigProvider: Send + Sync + 'static {
    /// Load configuration by hash/key.
    async fn load_config(&self, key: &str) -> Result<LerobotConfig>;
}

/// Trait for job lifecycle management.
#[async_trait]
pub trait JobRegistry: Send + Sync + 'static {
    /// Register a job for monitoring.
    async fn register(&self, job_id: String, token: Arc<CancellationToken>);

    /// Unregister a completed job.
    async fn unregister(&self, job_id: &str);

    /// Cancel a running job.
    async fn cancel(&self, job_id: &str);
}
```

#### Slot Pool

Resource management based on slots:

```rust
/// Manages execution slots for resource control.
pub struct SlotPool {
    /// Maximum slots available.
    max_slots: usize,

    /// Currently available slots.
    available: Arc<Semaphore>,

    /// Slot assignments (slot -> task).
    assignments: RwLock<HashMap<usize, String>>,
}

impl SlotPool {
    /// Acquire a slot for execution.
    pub async fn acquire(&self) -> Result<SlotGuard> {
        let permit = self.available.acquire().await?;
        Ok(SlotGuard { permit })
    }

    /// Get current slot utilization.
    pub fn utilization(&self) -> f64 {
        let used = self.max_slots - self.available.available_permits();
        used as f64 / self.max_slots as f64
    }
}
```

### Pipeline Runner

The `PipelineRunner` executes the core data processing loop with detailed timing:

```rust
/// Statistics from pipeline execution.
pub struct PipelineRunStats {
    /// Total frames written.
    pub frames_written: usize,

    /// Total messages processed.
    pub messages_processed: usize,

    /// Total execution duration.
    pub total_duration: Duration,

    /// Time spent reading from source.
    pub read_time: Duration,

    /// Time spent processing (includes encoding).
    pub process_time: Duration,

    /// Time spent finalizing (flush, merge).
    pub finalize_time: Duration,
}

/// Executes the data processing pipeline.
pub struct PipelineRunner {
    batch_size: usize,
}

impl PipelineRunner {
    /// Run the pipeline with timing collection.
    pub async fn run<W: DatasetWriter>(
        &self,
        source: &mut dyn Source,
        executor: PipelineExecutor<W>,
        config: &SourceConfig,
        cancel_token: Option<CancellationToken>,
    ) -> Result<PipelineRunStats>;
}
```

## Format Extensibility

The architecture supports adding new dataset formats through the `DatasetFormat` trait. Each format defines its own writer, metadata generator, and configuration.

### Adding a New Format

To add support for a new format (e.g., WebDataset):

```rust
// 1. Define the format
pub struct WebDataset;

impl DatasetFormat for WebDataset {
    const NAME: &'static str = "webdataset";
    const VERSION: &'static str = "1.0";
    
    type Writer = WebDatasetWriter;      // Writes .tar shards
    type MetadataGenerator = WDSMetadataGenerator;
    type Config = WebDatasetConfig;
    type EpisodeMetadata = WDSEpisodeMeta;
    type DatasetMetadata = WDSDatasetMeta;
}

// 2. Implement the writer
pub struct WebDatasetWriter;

impl EpisodeWriter for WebDatasetWriter {
    async fn write_episode(&self, episode: EpisodeData) -> Result<()> {
        // Write to .tar shard
    }
    
    fn resource_profile() -> ResourceProfile {
        ResourceProfile {
            cpu: 2.0,
            memory_gb: 4.0,
            ..Default::default()
        }
    }
}

// 3. Implement the metadata generator
pub struct WDSMetadataGenerator;

impl MetadataGenerator for WDSMetadataGenerator {
    async fn generate_metadata(
        &self,
        episodes: &[WDSEpisodeMeta],
        output_path: &Path,
    ) -> Result<()> {
        // Write WebDataset-specific metadata
    }
}

// 4. Build format-specific pipeline
let pipeline = PipelineBuilder::<WebDataset>::new()
    .stage(DiscoverStage::new())
    .stage(TransformStage::<WebDataset>::new(
        WebDatasetWriter::new(),
        config,
    ))
    .stage(MetadataStage::<WebDataset>::new(
        WDSMetadataGenerator::new(),
    ))
    .dependency(StageId(0), StageId(1))
    .dependency(StageId(1), StageId(2))
    .build()?;
```

### Format Comparison

| Format | Writer | Resource Profile | Chunking |
|--------|--------|------------------|----------|
| **LeRobot** | Parquet + MP4 | CPU: 4, Memory: 8GB | 500 episodes |
| **RLDS** | TFRecord | CPU: 2, Memory: 4GB | 1000 episodes |
| **WebDataset** | TAR shards | CPU: 2, Memory: 4GB | 10_000 episodes |
| **HDF5** | HDF5 file | CPU: 1, Memory: 16GB | Single file |

### Compile-Time Type Safety

The generic architecture prevents mixing formats at compile time:

```rust
// This compiles - all stages use LeRobot
let lerobot_pipeline = PipelineBuilder::<LeRobotV21>::new()
    .stage(TransformStage::<LeRobotV21>::new(...))
    .stage(MetadataStage::<LeRobotV21>::new(...))
    .build()?;

// This fails at compile time - type mismatch!
let bad_pipeline = PipelineBuilder::<LeRobotV21>::new()
    .stage(TransformStage::<RLDS>::new(...))  // ERROR!
    .build()?;
```

## Implementation Plan

Since no backward compatibility is needed, we can make aggressive changes:

### Phase 1: Injectable Executor (Done ✓)

- [x] Create `InjectableTaskExecutor` with generic dependencies
- [x] Implement `SourceProvider`, `ConfigProvider`, `JobRegistry` traits
- [x] Add `NoOpJobRegistry` and `InMemoryConfigProvider` for testing
- [x] Create `PipelineRunner` for testable pipeline execution
- [x] Write correctness tests using mock providers

### Phase 2: Crate Restructuring

- [ ] Create new `roboflow-control` crate
- [ ] Create new `roboflow-executor` crate
- [ ] Move control logic from `roboflow-distributed` to `roboflow-control`
- [ ] Move executor logic from `roboflow-distributed` to `roboflow-executor`
- [ ] **DELETE** `roboflow-distributed` crate entirely
- [ ] Update workspace Cargo.toml

### Phase 3: Stage Architecture

- [ ] Define `Stage` trait with idempotency and observability
- [ ] Implement `DiscoverStage` (source validation)
- [ ] Implement `ConvertStage` (data processing + video encoding)
- [ ] Implement `MergeStage` (output finalization)
- [ ] Create `StageScheduler` to orchestrate execution
- [ ] **REMOVE** monolithic `execute()` functions

### Phase 4: Resource Management

- [ ] Implement `SlotPool` for concurrent task limiting
- [ ] Add `TaskRegistry` for active task tracking
- [ ] Add resource-aware task scheduling
- [ ] **DELETE** old worker code that doesn't use slots

### Phase 5: Clean Up

- [ ] Remove all dead code
- [ ] Remove unused trait implementations
- [ ] Remove compatibility shims
- [ ] Update all imports to new crate structure
- [ ] Delete deprecated modules

### Phase 6: Testing Infrastructure

- [ ] Create comprehensive mock providers
- [ ] Add benchmark harness with timing breakdowns
- [ ] Implement chaos testing (random failures)
- [ ] Add property-based testing for stages
- [ ] **DELETE** tests that relied on old structure

## Testing Strategy

### Unit Tests (No Infrastructure)

```rust
#[tokio::test]
async fn test_executor_with_mocks() {
    let source_provider = MockSourceProvider::new()
        .with_messages(create_test_messages(100))
        .with_metadata(create_test_metadata());

    let config_provider = InMemoryConfigProvider::new()
        .with_config("test_hash", create_test_config());

    let job_registry = NoOpJobRegistry::new();

    let executor = TaskExecutor::new(
        source_provider,
        config_provider,
        job_registry,
        "/tmp/output".to_string(),
        Duration::from_secs(60),
    );

    let result = executor.execute(&work_unit).await;
    assert!(matches!(result, ProcessingResult::Success { .. }));
}
```

### Integration Tests (Real Bag Files)

```rust
#[test]
#[ignore = "Requires real bag file - run manually"]
fn test_executor_with_real_bag() {
    roboflow_sources::register_builtin_sources();

    let executor = TaskExecutor::new(
        ProductionSourceProvider::new(),  // Real source provider
        InMemoryConfigProvider::new().with_config("hash", config),
        NoOpJobRegistry::new(),
        output_path,
        Duration::from_secs(3600),
    );

    let result = executor.execute(&work_unit).await;

    match result {
        ProcessingResult::Success { frame_count, .. } => {
            assert!(frame_count > 0);
            assert_output_files_exist(&output_path);
        }
        _ => panic!("Expected success"),
    }
}
```

### Benchmark Tests (Performance)

```rust
#[test]
fn benchmark_pipeline_stages() {
    let stats = run_pipeline_with_timing(bag_path, config).await;

    println!("Stage Timing Breakdown:");
    println!("  Read:    {:?} ({:.1}%)", stats.read_time, stats.read_percentage());
    println!("  Process: {:?} ({:.1}%)", stats.process_time, stats.process_percentage());
    println!("  Finalize:{:?} ({:.1}%)", stats.finalize_time, stats.finalize_percentage());
    println!("  Total:   {:?}", stats.total_duration);
    println!("  FPS:     {:.1}", stats.fps());
}
```

### Property-Based Testing (QuickCheck)

Test invariants hold across random inputs:

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_discover_stage_output_validity(
        files in prop::collection::vec(file_strategy(), 1..100)
    ) {
        let stage = DiscoverStage::new("s3://test/", "config_hash");
        let task = stage.create_task(PartitionId(0));
        
        let ctx = MockTaskContext::new();
        let result = task.execute(&ctx).await?;
        
        // Invariants:
        // 1. Output must be non-empty
        assert!(!result.outputs.is_empty());
        
        // 2. All discovered files must be valid
        let discovered: Vec<FileInfo> = deserialize(
            &ctx.object_store.get(&result.outputs[0]).await?
        )?;
        for file in discovered {
            assert!(file.url.starts_with("s3://"));
            assert!(file.size > 0);
        }
    }
    
    #[test]
    fn test_convert_stage_determinism(
        input_files in prop::collection::vec(file_strategy(), 1..10),
        seed: u64
    ) {
        // Execute convert stage twice with same inputs
        let stage = ConvertStage::new(config, output_prefix, 500);
        
        let result1 = run_stage_with_seed(&stage, &input_files, seed).await?;
        let result2 = run_stage_with_seed(&stage, &input_files, seed).await?;
        
        // Deterministic: same inputs produce same outputs
        assert_eq!(result1.outputs.len(), result2.outputs.len());
        for (o1, o2) in result1.outputs.iter().zip(result2.outputs.iter()) {
            assert_eq!(o1.id, o2.id); // Content-addressed
        }
    }
    
    #[test]
    fn test_shuffle_partitioning_properties(
        records in prop::collection::vec(record_strategy(), 1..1000),
        num_partitions in 1usize..10
    ) {
        let shuffle = ShuffleSpec {
            partition_by: PartitionStrategy::Hash("key".to_string()),
            num_partitions,
            buffer_size: 10000,
        };
        
        let partitioned = shuffle.partition_records(records.clone()).await?;
        
        // Invariants:
        // 1. All records preserved
        let total_partitioned: usize = partitioned.iter().map(|p| p.len()).sum();
        assert_eq!(total_partitioned, records.len());
        
        // 2. No record appears in multiple partitions
        let mut all_records = Vec::new();
        for partition in &partitioned {
            all_records.extend(partition.clone());
        }
        assert_eq!(all_records.len(), records.len());
        
        // 3. Partitions within bounds
        assert_eq!(partitioned.len(), num_partitions);
    }
}
```

### Chaos Testing (Fault Injection)

Test recovery from random failures:

```rust
#[tokio::test]
async fn test_stage_recovery_with_random_failures() {
    let failure_injector = FailureInjector::new()
        .with_failure_rate(0.1) // 10% failure rate
        .with_failure_types(&[
            FailureType::NetworkTimeout,
            FailureType::ObjectStoreUnavailable,
            FailureType::OutOfMemory,
        ]);
    
    let object_store = InjectingObjectStore::new(
        InMemoryObjectStore::new(),
        failure_injector,
    );
    
    let scheduler = StageScheduler::new(
        Arc::new(object_store),
        Arc::new(InMemoryLineage::new()),
        Arc::new(SlotPool::new(4)),
    );
    
    let pipeline = create_test_pipeline();
    
    // Should eventually succeed despite random failures
    let result = scheduler.execute_pipeline(&pipeline).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_lineage_based_recovery() {
    let lineage = Arc::new(InMemoryLineage::new());
    let object_store = Arc::new(InMemoryObjectStore::new());
    
    // Execute stage that produces output
    let stage = ConvertStage::new(config, output_prefix, 500);
    let task = stage.create_task(PartitionId(0));
    
    let ctx = TaskContext::new(object_store.clone(), lineage.clone());
    let result = task.execute(&ctx).await?;
    
    // Simulate object loss
    let output = result.outputs[0];
    object_store.delete(&output).await?;
    
    // Lineage should allow recomputation
    let recovered = lineage.recompute_object(&output).await?;
    assert_eq!(recovered, original_data);
}
```

### Stage Isolation Tests

Test each stage in isolation with mocked dependencies:

```rust
/// Mock stage for testing
struct MockStage {
    name: String,
    output_count: usize,
    execution_time: Duration,
    should_fail: bool,
}

#[tokio::test]
async fn test_scheduler_with_mock_stages() {
    let stages: Vec<Arc<dyn Stage>> = vec![
        Arc::new(MockStage::new("stage0", 2, Duration::from_millis(100), false)),
        Arc::new(MockStage::new("stage1", 1, Duration::from_millis(50), false)),
        Arc::new(MockStage::new("stage2", 1, Duration::from_millis(200), false)),
    ];
    
    let pipeline = PipelineBuilder::new()
        .stage(stages[0].clone())
        .stage(stages[1].clone())
        .stage(stages[2].clone())
        .dependency(StageId(0), StageId(1))
        .dependency(StageId(1), StageId(2))
        .build()
        .unwrap();
    
    let scheduler = create_test_scheduler();
    let result = scheduler.execute_pipeline(&pipeline).await;
    
    assert!(result.is_ok());
    // Verify stages executed in correct order
    // Verify outputs propagated correctly
}

#[tokio::test]
async fn test_scheduler_handles_stage_failure() {
    let stages: Vec<Arc<dyn Stage>> = vec![
        Arc::new(MockStage::new("stage0", 1, Duration::from_millis(100), false)),
        Arc::new(MockStage::new("stage1", 1, Duration::from_millis(50), true)), // Fails
        Arc::new(MockStage::new("stage2", 1, Duration::from_millis(200), false)),
    ];
    
    let pipeline = PipelineBuilder::new()
        .stage(stages[0].clone())
        .stage(stages[1].clone())
        .stage(stages[2].clone())
        .dependency(StageId(0), StageId(1))
        .dependency(StageId(1), StageId(2))
        .build()
        .unwrap();
    
    let scheduler = create_test_scheduler();
    let result = scheduler.execute_pipeline(&pipeline).await;
    
    assert!(result.is_err());
    // Stage 2 should not have executed
}
```

## Inspiration from Industry

This design synthesizes proven patterns from three major distributed systems:

### Apache Spark

**Core Concepts:**
- **DAG (Directed Acyclic Graph)**: Execution plan as a graph of stages
- **Stages**: Execution units separated by shuffle boundaries
- **Shuffle**: Data redistribution between stages (hash/range partitioning)
- **Slots**: Fixed resource units per executor for task scheduling
- **RDD Lineage**: Fault tolerance through immutable data lineage
- **Partitioning**: Data split into partitions for parallel processing

**Adopted Patterns:**
| Spark Concept | Our Implementation |
|---------------|-------------------|
| `Stage` | `Stage` trait with `partition_count()` and `shuffle()` |
| `Task` | `Task` trait with partition-aware execution |
| `Shuffle` | `ShuffleSpec` and `ShuffleOperation` |
| `Slot` | `SlotPool` for resource management |
| `DAGScheduler` | `Pipeline` and `StageScheduler` |
| `RDD` | `ObjectRef` for immutable data |

**Key Insight**: Spark's stage boundaries allow for efficient fault recovery—only failed stages need recomputation, not the entire job.

### Trino (formerly PrestoSQL)

**Core Concepts:**
- **Fragments**: Query execution units distributed to workers
- **Exchange**: Data movement between fragments (shuffle)
- **Pipeline**: In-memory operator chaining for efficiency
- **Memory Pools**: Per-query and per-operator memory limits
- **Split**: Work unit representing a slice of data

**Adopted Patterns:**
| Trino Concept | Our Implementation |
|---------------|-------------------|
| `Fragment` | `Stage` with well-defined inputs/outputs |
| `Exchange` | `ShuffleSpec` for data redistribution |
| `Split` | `Task` with partition assignment |
| `MemoryPool` | `MemoryPool` in `TaskContext` |
| `Pipeline` | Stage execution within slots |

**Key Insight**: Trino's eager execution model (stream results as produced) and exchange abstraction enable efficient memory-bounded processing.

### Ray

**Core Concepts:**
- **Tasks**: Stateless, idempotent functions for parallel execution
- **Actors**: Stateful workers for maintaining state across calls
- **Object Store**: Distributed, content-addressed storage for task outputs
- **ObjectRef**: Reference to immutable object in the store
- **Lineage**: Dependency graph for automatic fault recovery
- **Resource Labels**: Tasks specify resource requirements (CPU, GPU, memory)

**Adopted Patterns:**
| Ray Concept | Our Implementation |
|-------------|-------------------|
| `@ray.remote` | `Task` trait with `execute()` method |
| `ObjectRef` | `ObjectRef` with content-addressed IDs |
| `ObjectStore` | `ObjectStore` trait for intermediate data |
| `Lineage` | `Lineage` trait for dependency tracking |
| `recompute()` | `recompute_object()` for fault recovery |
| Resource labels | `ResourceRequest` in task definition |

**Key Insight**: Ray's lineage-based fault tolerance eliminates the need for expensive checkpointing—lost data is automatically recomputed from source.

### Synthesis: Why These Patterns Work Together

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Synthesized Architecture                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Spark Stages          Trino Exchange         Ray Objects          │
│  ┌─────────┐           ┌──────────┐          ┌──────────┐         │
│  │Stage 0  │──────────▶│Shuffle   │─────────▶│ObjectRef │         │
│  │         │           │(Exchange)│          │  (Ray)   │         │
│  └─────────┘           └──────────┘          └──────────┘         │
│       │                                         │                  │
│       │          Spark Slots                    │                  │
│       │          ┌──────────────────┐           │                  │
│       │          │  Slot Pool       │           │                  │
│       │          │  [Slot 1][Slot 2]│           │                  │
│       │          └──────────────────┘           │                  │
│       │                                         │                  │
│       ▼                                         ▼                  │
│  ┌─────────┐           Ray Lineage          ┌──────────┐         │
│  │Stage 1  │◄───────────────────────────────│Recovery  │         │
│  │         │                               │          │         │
│  └─────────┘                               └──────────┘         │
│                                                                     │
│  Result: Clean, composable, fault-tolerant distributed execution   │
└─────────────────────────────────────────────────────────────────────┘
```

**Why This Combination Works:**

1. **Spark's stages** provide clear boundaries for observability and fault isolation
2. **Trino's exchange** abstraction cleanly handles data movement between stages
3. **Ray's object store** and lineage enable automatic recovery without checkpoints
4. **Spark's slots** provide predictable resource management
5. **Ray's resource labels** enable heterogeneous task scheduling

### Comparison Matrix

| Feature | Spark | Trino | Ray | Our Design |
|---------|-------|-------|-----|------------|
| Execution Model | Batch | Streaming | Task/Actor | **Stage-based** |
| Fault Tolerance | Lineage + Checkpoint | Retry | Lineage | **Lineage** |
| Resource Mgmt | Slots | Memory Pools | Resource Labels | **Slots + Labels** |
| Data Transfer | Shuffle | Exchange | Object Store | **Shuffle + Objects** |
| Scheduling | DAG-based | Fragment-based | Ownership | **DAG + Lineage** |
| State Management | RDDs | Stateless | Actors/Objects | **Object Store** |

### Design Philosophy

Our architecture follows these principles derived from the three systems:

1. **Immutable Data Flow** (from Ray): Data flows as `ObjectRef`s between stages
2. **Explicit Boundaries** (from Spark): Stages have clear inputs/outputs
3. **Resource Awareness** (from Spark/Ray): Tasks declare requirements, slots enforce limits
4. **Fault Recovery** (from Ray): Lineage enables automatic recomputation
5. **Testability** (from all three): Clean abstractions enable mocking at every layer

### References

- **Apache Spark**: [Spark Internals](https://spark.apache.org/docs/latest/cluster-overview.html), [RDD Paper](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf)
- **Trino**: [Trino Documentation](https://trino.io/docs/current/), [Presto Paper](https://trino.io/Presto_SQL_on_Everything.pdf)
- **Ray**: [Ray Documentation](https://docs.ray.io/), [Ray Paper](https://www.usenix.org/system/files/osdi18-moritz.pdf)

## Crate Restructuring

Since no backward compatibility is needed, we can significantly simplify the crate structure:

### Current State (Can Be Removed/Rewritten)

| Crate | Status | Action |
|-------|--------|--------|
| `roboflow-distributed` | Mixed concerns | Split into control-plane and data-plane |
| `roboflow-core` | OK | Keep, minimal changes |
| `roboflow-sources` | OK | Keep as-is |
| `roboflow-sinks` | OK | Keep as-is |
| `roboflow-dataset` | OK | Keep as-is |
| `roboflow-video` | OK | Keep as-is |

### Proposed New Crate Structure

```
roboflow-workspace/
│
├── roboflow-core/              # Core types, traits, errors
│   └── src/
│       ├── error.rs
│       ├── result.rs
│       └── timestamp.rs
│
├── roboflow-control/           # Control Plane (NEW)
│   └── src/
│       ├── lib.rs
│       ├── scanner.rs          # File discovery
│       ├── reaper.rs           # Timeout/retry handling
│       ├── finalizer.rs        # Result aggregation
│       ├── batch.rs            # Batch lifecycle management
│       ├── episode.rs          # Episode allocation
│       └── tikv/               # TiKV coordination
│           ├── mod.rs
│           ├── client.rs
│           └── catalog.rs
│
├── roboflow-executor/          # Data Plane (NEW - extracted from distributed)
│   └── src/
│       ├── lib.rs
│       │
│       ├── executor/           # Task execution
│       │   ├── mod.rs
│       │   ├── task.rs         # Task definition
│       │   ├── executor.rs     # TaskExecutor
│       │   ├── slot_pool.rs    # Resource slots
│       │   └── registry.rs     # Active tasks
│       │
│       ├── stages/             # Execution stages
│       │   ├── mod.rs
│       │   ├── stage.rs        # Stage trait
│       │   ├── discover.rs     # Source validation
│       │   ├── convert.rs      # Data processing
│       │   └── merge.rs        # Output finalization
│       │
│       ├── pipeline/           # Pipeline runner
│       │   ├── mod.rs
│       │   ├── runner.rs       # PipelineRunner
│       │   └── stats.rs        # Timing/metrics
│       │
│       └── providers/          # Injectable dependencies
│           ├── mod.rs
│           ├── source.rs       # SourceProvider
│           ├── config.rs       # ConfigProvider
│           ├── job.rs          # JobRegistry
│           └── mock.rs         # Test implementations
│
├── roboflow-sources/           # Data sources (keep as-is)
├── roboflow-sinks/             # Data sinks (keep as-is)
├── roboflow-dataset/           # Dataset writers (keep as-is)
├── roboflow-video/             # Video encoding (keep as-is)
│
└── roboflow/                   # Facade crate (simplified)
    └── src/
        ├── lib.rs              # Re-exports only
        └── prelude.rs
```

### Key Changes

1. **Split `roboflow-distributed`** into two crates:
   - `roboflow-control`: Coordination logic (scanner, reaper, finalizer, TiKV)
   - `roboflow-executor`: Execution logic (stages, executor, providers)

2. **Cleaner dependency graph**:
   ```
   roboflow-control ──► roboflow-core
         │
         ▼
   roboflow-executor ──► roboflow-sources
         │              ──► roboflow-sinks
         │              ──► roboflow-dataset
         ▼
   roboflow (facade)
   ```

3. **Remove dead code**: No backward compatibility means we can delete unused modules immediately

4. **Simplified facade**: `roboflow` crate just re-exports, no implementation

## Configuration

```toml
[worker]
# Resource management
slots = 4                          # Concurrent task slots
slot_timeout = "30m"               # Max time per slot

# Execution
timeout = "1h"                     # Default task timeout
batch_size = 1000                  # Messages per batch

# Stage configuration
[worker.stages.discover]
enabled = true
timeout = "5m"

[worker.stages.convert]
enabled = true
timeout = "1h"
memory_limit = "2GB"

[worker.stages.merge]
enabled = true
timeout = "30m"

# Providers
[worker.providers.source]
type = "production"                # or "mock" for testing

[worker.providers.config]
type = "tikv"                      # or "memory" for testing

[worker.providers.job]
type = "production"                # or "noop" for testing
```

## Metrics and Observability

### Executor Metrics

```rust
pub struct ExecutorMetrics {
    // Throughput
    pub tasks_completed: AtomicU64,
    pub tasks_failed: AtomicU64,
    pub frames_processed: AtomicU64,

    // Latency
    pub task_duration_ms: Histogram,
    pub stage_durations: HashMap<String, Histogram>,

    // Resources
    pub slots_used: AtomicU64,
    pub slots_available: AtomicU64,
    pub memory_used: AtomicU64,

    // Errors
    pub errors_by_type: HashMap<String, AtomicU64>,
}
```

### Stage Metrics

```rust
pub struct StageMetrics {
    pub name: String,
    pub duration: Duration,
    pub input_bytes: usize,
    pub output_bytes: usize,
    pub items_processed: usize,
    pub errors: Vec<StageError>,
}
```

## Code to Remove (No Backward Compatibility)

Since we don't need backward compatibility, the following can be **deleted immediately**:

### In `roboflow-distributed`

| File/Module | Action | Reason |
|-------------|--------|--------|
| `worker/executor.rs::TaskExecutor` | Delete | Replaced by `InjectableTaskExecutor` |
| `worker/mod.rs::Worker` | Delete | Will be replaced by simpler worker in `roboflow-executor` |
| `worker/coordinator.rs` | Move | Move to `roboflow-control` |
| `converter/` | Move | Move to `roboflow-executor/stages/` |
| `merge/` | Move | Move to `roboflow-executor/stages/` |
| `batch.rs::BatchController` | Move | Move to `roboflow-control` |
| `scanner.rs` | Move | Move to `roboflow-control` |
| `reaper.rs` | Move | Move to `roboflow-control` |
| `finalizer.rs` | Move | Move to `roboflow-control` |
| `providers/` | Keep | Move to `roboflow-executor/providers/` |
| `worker/injectable.rs` | Keep | Becomes `roboflow-executor/executor/executor.rs` |
| `worker/pipeline_runner.rs` | Keep | Becomes `roboflow-executor/pipeline/runner.rs` |

### After Restructuring

```
DELETE: roboflow-distributed/        (entire crate)

KEEP & MOVE:
  - control logic → roboflow-control/
  - executor logic → roboflow-executor/
  - providers → roboflow-executor/providers/
```

### Deprecated Patterns to Remove

1. **Monolithic execute()**: Replace with staged execution
2. **TiKV in executor**: Use `ConfigProvider` trait instead
3. **Hardcoded dependencies**: Use dependency injection
4. **Worker as god object**: Split into coordinator + executor
5. **Global state**: Pass dependencies explicitly

## Summary

This architecture achieves:

1. **Testability**: All dependencies injectable, stages testable in isolation
2. **Observability**: Detailed timing and metrics at every level
3. **Resource Awareness**: Slot-based management prevents resource exhaustion
4. **Composability**: Stages can be added, removed, or reordered
5. **Simplicity**: Clean abstractions following proven industry patterns

The key insight is that separation of concerns (Control vs Data Plane) combined with dependency injection enables both production robustness and testing simplicity.

**Remember**: We can delete anything that doesn't fit this new architecture. No backward compatibility required.
