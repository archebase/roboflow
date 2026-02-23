# ADR-008: Distributed Dataset Metadata Management

**Author**: Sisyphus (AI Agent)
**Date**: 2026-02-23
**Status**: Accepted
**Related**: [executor-architecture.md](./executor-architecture.md), [data-pipeline-design.md](./data-pipeline-design.md)

## Context

Roboflow's default operating mode is converting 100,000+ bag files using a distributed cluster. Each bag file becomes one episode in a LeRobot dataset. This creates a metadata coordination challenge:

- **100K episodes** need consistent feature specifications across all workers
- **Task deduplication** must be global (same task description = same index everywhere)
- **Final metadata** (info.json, episodes.jsonl, tasks.jsonl, episodes_stats.jsonl) describes the entire dataset
- **Workers are stateless** - they process one bag file and exit
- **No single worker** sees all episodes

The existing `MetadataCollector` in `roboflow-dataset` is designed for single-process conversion. We need a distributed coordination layer for metadata aggregation.

## Decision

Use **TiKV as the source of truth** for distributed metadata coordination:

1. **Workers** register tasks and features in TiKV during conversion
2. **Workers** write partial episode metadata to TiKV after each bag conversion
3. **Finalizer** aggregates all partial metadata from TiKV
4. **Finalizer** writes LeRobot v2.1 metadata files to storage

This follows a **registry pattern**: TiKV holds the authoritative metadata state; storage holds the actual data files.

## Key Design Decisions

### 1. TiKV Key Schema

```
/roboflow/v1/batch/{batch_id}/
├── episode_counter              → Episode allocation (existing)
├── task_counter                 → Task index allocation
├── tasks/{task_hash}            → Task description → global index
├── features/{feature_name}      → Unified feature specification
├── metadata/episode/{idx:06}    → Per-episode metadata
└── stats/episode/{idx}          → Per-episode statistics (existing)
```

### 2. Task Deduplication via Content Hashing

Tasks are content-addressed to prevent duplicates:

```rust
pub struct TaskEntry {
    pub task_index: usize,
    pub task: String,
}

// Task hash = blake3(task_description)
let task_hash = blake3::hash(task.as_bytes()).to_hex();
let key = format!("/roboflow/v1/batch/{}/tasks/{}", batch_id, task_hash);
```

**Benefits:**
- Idempotent registration (same task → same key)
- No coordination needed for common tasks
- Automatic deduplication across workers

### 3. Feature Unification with Validation

Features must be consistent across all episodes:

```rust
pub struct FeatureSpec {
    pub dtype: String,           // "float32", "video", "int64"
    pub shape: Vec<usize>,       // [dim] for states, [H, W, 3] for images
    pub names: Option<Vec<String>>, // ["height", "width", "channel"]
    pub video_info: Option<VideoInfo>, // codec, fps for video
}

impl FeatureSpec {
    /// Check if two specs are compatible (for validation)
    pub fn is_compatible(&self, other: &FeatureSpec) -> bool {
        self.dtype == other.dtype &&
        self.shape == other.shape
        // video_info can differ (e.g., different episodes use same camera)
    }
}
```

**Validation:** Workers fail fast if feature specs conflict.

### 4. Partial Episode Metadata

Workers write this after converting each bag:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialEpisodeMetadata {
    pub episode_index: usize,     // Globally allocated from TiKV
    pub length: usize,            // Frame count
    pub tasks: Vec<String>,       // Task descriptions (not indices yet)
    pub feature_shapes: HashMap<String, FeatureShape>,
    pub parquet_path: String,     // Relative path in storage
    pub video_paths: HashMap<String, String>, // camera → relative path
    pub stats: EpisodeFeatureStats, // Min/max/mean/std per feature
}
```

**Storage layout:** Each episode's metadata is stored separately for efficient retrieval.

### 5. Global Metadata Assembly

The finalizer aggregates all metadata into LeRobot format:

```rust
pub struct GlobalMetadataAssembler {
    registry: DatasetMetadataRegistry,
    storage: Arc<dyn Storage>,
    batch_id: String,
    output_prefix: String,
}

impl GlobalMetadataAssembler {
    pub async fn assemble_and_write(
        &self,
        config: &LerobotConfig,
    ) -> Result<(), TikvError> {
        // 1. Scan all episode metadata from TiKV
        let episodes = self.collect_episode_metadata().await?;

        // 2. Build tasks.jsonl from task registry
        let tasks = self.build_task_list().await?;

        // 3. Build unified feature specs
        let features = self.build_feature_specs().await?;

        // 4. Aggregate statistics using parallel Welford's algorithm
        let episode_stats = self.aggregate_statistics(&episodes).await?;

        // 5. Write LeRobot v2.1 metadata files
        self.write_info_json(&episodes, &features, config).await?;
        self.write_episodes_jsonl(&episodes).await?;
        self.write_tasks_jsonl(&tasks).await?;
        self.write_episodes_stats_jsonl(&episode_stats).await?;

        Ok(())
    }
}
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              TiKV Cluster                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│  /roboflow/v1/batch/{id}/episode_counter    → Episode allocation             │
│  /roboflow/v1/batch/{id}/tasks/{hash}       → Global task registry           │
│  /roboflow/v1/batch/{id}/features/{name}    → Unified feature specs          │
│  /roboflow/v1/batch/{id}/metadata/episode/{idx}→ Per-episode metadata        │
│  /roboflow/v1/batch/{id}/stats/episode/{idx}→ Per-episode stats              │
└─────────────────────────────────────────────────────────────────────────────┘
         ↑                                      ↑
         │                                      │
    ┌────┴──────────────┐                ┌──────┴──────────────┐
    │   Worker (1..N)   │                │     Finalizer       │
    │                   │                │                     │
    │ 1. Allocate ep    │                │ 1. Scan for         │
    │    index from TiKV│                │    complete batches │
    │                   │                │                     │
    │ 2. Convert bag →  │                │ 2. Collect from TiKV│
    │    episode data   │                │    - All episodes   │
    │                   │                │    - Task registry  │
    │ 3. Register in TiKV                │    - Feature specs  │
    │    - Tasks (hash) │                │                     │
    │    - Features     │                │ 3. Aggregate stats  │
    │    - Episode meta │                │                     │
    │                   │                │ 4. Write to storage │
    └───────────────────┘                │    - info.json      │
                                         │    - episodes.jsonl │
                                         │    - tasks.jsonl    │
                                         │    - stats.jsonl    │
                                         └─────────────────────┘
```

## Implementation

### DatasetMetadataRegistry (roboflow-distributed)

```rust
/// Global metadata registry backed by TiKV.
pub struct DatasetMetadataRegistry {
    tikv: Arc<TikvClient>,
    batch_id: String,
}

impl DatasetMetadataRegistry {
    /// Register or get existing task index (global deduplication)
    pub async fn register_task(&self, task: &str) -> Result<usize, TikvError> {
        let task_hash = blake3::hash(task.as_bytes()).to_hex();
        let key = MetadataKeys::task(&self.batch_id, &task_hash);

        // Try to get existing
        if let Some(data) = self.tikv.get(key.clone()).await? {
            let entry: TaskEntry = bincode::deserialize(&data)?;
            return Ok(entry.task_index);
        }

        // Allocate new index via CAS on counter
        let counter_key = MetadataKeys::task_counter(&self.batch_id);
        loop {
            let counter_data = self.tikv.get(counter_key.clone()).await?;
            let (current, _): (u64, u64) = match counter_data {
                Some(d) => bincode::deserialize(&d)?,
                None => (0, 0),
            };

            let new_index = current;
            let entry = TaskEntry {
                task_index: new_index as usize,
                task: task.to_string(),
            };

            // Atomic transaction: update counter AND store task
            let txn = self.tikv.begin_optimistic().await?;
            txn.put(counter_key.clone(),
                    bincode::serialize(&(current + 1, 0))?)?;
            txn.put(key.clone(), bincode::serialize(&entry)?)?;

            match txn.commit().await {
                Ok(_) => return Ok(new_index as usize),
                Err(_) => continue, // Retry on conflict
            }
        }
    }

    /// Register feature spec (with validation)
    pub async fn register_feature(
        &self,
        name: &str,
        spec: FeatureSpec
    ) -> Result<(), TikvError> {
        let key = MetadataKeys::feature(&self.batch_id, name);

        // Check existing spec
        if let Some(data) = self.tikv.get(key.clone()).await? {
            let existing: FeatureSpec = bincode::deserialize(&data)?;
            if !existing.is_compatible(&spec) {
                return Err(TikvError::Other(format!(
                    "Feature '{}' spec mismatch: existing {:?} vs new {:?}",
                    name, existing, spec
                )));
            }
            return Ok(());
        }

        // Store new spec
        self.tikv.put(key, bincode::serialize(&spec)?).await?;
        Ok(())
    }

    /// Store partial episode metadata
    pub async fn store_episode_metadata(
        &self,
        metadata: &PartialEpisodeMetadata,
    ) -> Result<(), TikvError> {
        let key = MetadataKeys::episode_metadata(
            &self.batch_id,
            metadata.episode_index
        );
        self.tikv.put(key, bincode::serialize(metadata)?).await?;
        Ok(())
    }
}
```

### Worker Integration

```rust
/// Worker-side metadata submission after bag conversion
async fn submit_episode_metadata(
    registry: &DatasetMetadataRegistry,
    episode_index: usize,
    conversion_result: &ConversionResult,
) -> Result<(), TikvError> {
    // Register tasks (global deduplication)
    let task_indices: Vec<usize> = futures::future::try_join_all(
        conversion_result.tasks.iter()
            .map(|t| registry.register_task(t))
    ).await?;

    // Register features (with validation)
    for (name, shape) in &conversion_result.feature_shapes {
        registry.register_feature(name, shape.to_spec()).await?;
    }

    // Store episode metadata
    let metadata = PartialEpisodeMetadata {
        episode_index,
        length: conversion_result.stats.frames_written,
        tasks: conversion_result.tasks.clone(),
        feature_shapes: conversion_result.feature_shapes.clone(),
        parquet_path: conversion_result.parquet_path.clone(),
        video_paths: conversion_result.video_paths.clone(),
        stats: conversion_result.stats.clone(),
    };

    registry.store_episode_metadata(&metadata).await?;
    Ok(())
}
```

### Finalizer Integration

```rust
/// Modified Finalizer::finalize_batch
async fn finalize_batch(
    &self,
    batch: &BatchSummary,
    spec: &BatchSpec,
) -> Result<bool, TikvError> {
    // ... existing stats aggregation ...

    // NEW: Assemble global metadata
    let assembler = GlobalMetadataAssembler::new(
        registry.clone(),
        storage.clone(),
        batch.id.clone(),
        output_path.clone(),
    );

    match assembler.assemble_and_write(&config).await {
        Ok(_) => info!("Global metadata assembly complete"),
        Err(e) => {
            error!(error = %e, "Failed to assemble global metadata");
            return Err(e);
        }
    }

    // Continue with merge coordination...
}
```

## Consequences

### Positive

| Aspect | Before | After |
|--------|--------|-------|
| **Task indices** | Per-episode, duplicates | Globally deduplicated via TiKV |
| **Feature specs** | Per-episode, may conflict | Validated centrally, fail fast |
| **Metadata assembly** | Not possible | Finalizer aggregates from TiKV |
| **Scalability** | Single-process only | 100K+ episodes supported |
| **Consistency** | Best-effort | Strong consistency via TiKV transactions |

### Trade-offs

| Aspect | Consideration |
|--------|--------------|
| **TiKV load** | Additional reads/writes per episode |
| **Latency** | Network round-trip for task/feature registration |
| **Complexity** | Additional registry layer |
| **Storage** | Metadata duplicated (TiKV + final files) |

### Mitigations

- **Batching**: Register tasks in batches to reduce round-trips
- **Caching**: Workers cache task indices locally during conversion
- **Cleanup**: Delete TiKV metadata after successful finalization
- **Scans**: Bounded scans (100K limit) with pagination for larger datasets

## Error Handling

| Scenario | Handling |
|----------|----------|
| Feature spec conflict | Worker fails task, error logged |
| Task registration race | CAS retry loop, eventual consistency |
| TiKV scan timeout | Retry with exponential backoff |
| Partial metadata missing | Finalizer logs warning, skips episode |
| Duplicate episode index | TiKV key prevents overwrite |
| Worker dies mid-write | Heartbeat timeout → requeue work unit |

## Storage Layout (After Finalization)

```
s3://bucket/datasets/{dataset_name}/
├── meta/
│   ├── info.json              # Dataset-level metadata
│   ├── episodes.jsonl         # Episode list with lengths
│   ├── tasks.jsonl            # Task descriptions (deduplicated)
│   └── episodes_stats.jsonl   # Per-episode statistics
├── data/
│   └── chunk-000/
│       ├── episode_000000.parquet
│       ├── episode_000001.parquet
│       └── ... (500 episodes per chunk)
│   └── chunk-001/
│       └── ...
├── videos/
│   └── chunk-000/
│       ├── observation.images.cam_high/
│       │   ├── episode_000000.mp4
│       │   └── ...
│       └── observation.images.cam_left/
│           └── ...
└── parameters/
    └── camera_params.json
```

## Implementation Plan

### Phase 1: Core Registry (roboflow-distributed)

1. Implement `DatasetMetadataRegistry` with TiKV operations
2. Add key schema and serialization
3. Unit tests with mock TiKV

### Phase 2: Worker Integration (roboflow-dataset)

1. Add metadata submission to worker completion flow
2. Integrate with existing `LerobotWriter`
3. Task caching in workers

### Phase 3: Finalizer Assembly (roboflow-distributed)

1. Implement `GlobalMetadataAssembler`
2. Storage backend writes
3. Integration with existing `Finalizer`

### Phase 4: Validation & Monitoring

1. Metadata validation tool
2. Metrics for registry operations
3. End-to-end integration tests

## Related Decisions

- [ADR-001: Pipeline/Writer Separation](./adr-001-pipeline-writer-storage-separation.md) - Writers produce operations, not direct storage
- [ADR-002: Crate Architecture](./adr-002-crate-architecture-refactoring.md) - Separation of concerns
- [executor-architecture.md](./executor-architecture.md) - Distributed execution
- [data-pipeline-design.md](./data-pipeline-design.md) - Data flow design

## Open Questions

1. **Should we support incremental metadata updates?**
   - Option A: Only finalizer writes (simpler, single writer)
   - Option B: Workers write directly to storage (more complex, concurrent writers)

2. **How to handle missing episode metadata?**
   - Option A: Fail finalization
   - Option B: Skip episode and warn
   - Option C: Retry metadata collection

3. **Should we keep TiKV metadata after finalization?**
   - Option A: Delete for cleanup
   - Option B: Keep for audit/debugging
   - Option C: Archive to storage
