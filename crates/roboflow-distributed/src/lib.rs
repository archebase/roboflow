// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # roboflow-distributed
//!
//! Distributed coordination for roboflow.
//!
//! This crate provides coordination primitives for distributed dataset processing:
//! - **TiKV backend** - Production coordination with strong consistency
//! - **Catalog** - Metadata storage for episodes and uploads
//! - **Coordinator traits** - Unified abstraction
//!
//! ## Design Philosophy
//!
//! TiKV is the production coordination layer for distributed workloads.
//! All coordination features are **always available** (no feature flags).

pub mod batch;
pub mod catalog;
pub mod converter;
pub mod episode;
pub mod work_unit_executor;
pub mod finalizer;
pub mod heartbeat;
pub mod merge;
pub mod providers;
pub mod reaper;
pub mod scanner;
pub mod shutdown;
pub mod slot_pool;
pub mod state;
pub mod stats;
pub mod tikv;
pub mod worker;

pub use providers::{
    ConfigProvider, InMemoryConfigProvider, ProductionSourceProvider, ProviderFactory,
    SourceProvider, TikvConfigProvider,
};

#[cfg(test)]
pub use providers::mock::{MockFrame, MockLerobotWriter, MockSource, MockSourceProvider};

// Re-export public types from state (unified state lifecycle)
pub use state::{StateLifecycle, StateTransitionError};

// Re-export public types from tikv (distributed coordination)
pub use tikv::{
    CheckpointConfig, CheckpointState, CircuitBreaker, CircuitConfig, CircuitState,
    DEFAULT_CHECKPOINT_INTERVAL_FRAMES, DEFAULT_CHECKPOINT_INTERVAL_SECS, HeartbeatRecord,
    LockGuard, LockManager, LockManagerConfig, LockRecord, ParquetUploadState, TikvClient,
    TikvConfig, TikvError, UploadedPart, VideoUploadState, WorkerStatus,
};

// Re-export public types from batch (declarative batch processing)
pub use batch::{
    API_VERSION, BatchController, BatchIndexKeys, BatchKeys, BatchMetadata, BatchPhase, BatchSpec,
    BatchSpecError, BatchStatus, BatchSummary, ControllerConfig, DiscoveryStatus, FailedWorkUnit,
    KIND_BATCH_JOB, PartitionStrategy, SourceUrl, WorkFile, WorkUnit, WorkUnitConfig,
    WorkUnitError, WorkUnitStatus, WorkUnitSummary, update_phase_index,
};

// Re-export public types from catalog (metadata storage)
pub use catalog::{EpisodeMetadata, SegmentMetaData, TiKVCatalog, TiKVConfig, UploadStatus};

// Re-export public types from scanner (file discovery actor)
pub use scanner::{MetricsSnapshot, ScanStats, Scanner, ScannerConfig, ScannerMetrics};

// Re-export public types from worker (job processing actor)
pub use worker::{ProcessingResult, Worker, WorkerConfig, WorkerMetrics, WorkerMetricsSnapshot};

// Re-export public types from heartbeat (liveness tracking)
pub use heartbeat::{
    DEFAULT_HEARTBEAT_INTERVAL_SECS as HEARTBEAT_DEFAULT_INTERVAL_SECS,
    DEFAULT_STALE_THRESHOLD_SECS as HEARTBEAT_DEFAULT_STALE_THRESHOLD_SECS, HeartbeatConfig,
    HeartbeatManager, HeartbeatMetrics, HeartbeatMetricsSnapshot,
};

// Re-export public types from reaper (zombie detection)
pub use reaper::{
    DEFAULT_MAX_RECLAIMS_PER_ITERATION, DEFAULT_REAPER_INTERVAL_SECS,
    DEFAULT_STALE_THRESHOLD_SECS as REAPER_DEFAULT_STALE_THRESHOLD_SECS, ReaperConfig,
    ReaperMetrics, ReaperMetricsSnapshot, ReclaimResult, ZombieReaper,
};

// Re-export constants from tikv config
pub use tikv::config::{DEFAULT_CONNECTION_TIMEOUT_SECS, DEFAULT_PD_ENDPOINTS, KEY_PREFIX};

// Re-export public types from finalizer (batch completion)
pub use finalizer::{Finalizer, FinalizerConfig};

// Re-export public types from shutdown (graceful shutdown)
pub use shutdown::{
    DEFAULT_SHUTDOWN_TIMEOUT_SECS as SHUTDOWN_DEFAULT_TIMEOUT_SECS, ShutdownHandler,
    ShutdownInterrupted,
};

// Re-export public types from merge (staging + merge coordination)
pub use merge::{
    DEFAULT_MAX_CONCURRENT_MERGES as MERGE_DEFAULT_MAX_CONCURRENT, MergeConfig, MergeCoordinator,
    MergePermit, MergeResult, MergeSemaphore, MergeSemaphoreMetrics,
};

// Re-export public types from episode (episode index allocation)
pub use episode::{
    EpisodeAllocation, EpisodeAllocator, EpisodeAllocatorError, LocalEpisodeAllocator,
    TiKVEpisodeAllocator, chunk_index_from_episode,
};

// Re-export public types from slot_pool (resource management)
pub use slot_pool::{SlotGuard, SlotPool, SlotPoolError};

// Re-export public types from stats (episode statistics collection)
pub use stats::{
    BatchStatsSummary, EpisodeStats, FeatureStats, StatsCollector, StatsKeys, TiKVStatsCollector,
};

// Re-export public types from converter (LeRobot converter orchestrator)
pub use converter::{
    ConverterConfig, ConverterError,
    DEFAULT_EPISODES_PER_CHUNK as CONVERTER_DEFAULT_EPISODES_PER_CHUNK, LeRobotConverter,
};

// Re-export public types from work_unit_executor (stage-based executor integration)
pub use work_unit_executor::WorkUnitExecutor;

// =============================================================================
// Coordinator Traits
// =============================================================================

use std::time::Duration;

use roboflow_core::Result;

/// Coordinator trait for distributed job coordination.
pub trait Coordinator: Send + Sync {
    /// Acquire a distributed lock on a resource.
    fn acquire_lock(&self, resource: &str, owner: &str, ttl: Duration) -> Result<bool>;
    /// Release a previously acquired lock.
    fn release_lock(&self, resource: &str, owner: &str) -> Result<bool>;

    /// Send a heartbeat to indicate the worker is still alive.
    fn heartbeat(&self, pod_id: &str, status: &WorkerStatus) -> Result<()>;

    /// Save checkpoint state for recovery.
    fn save_checkpoint(&self, state: &CheckpointState) -> Result<()>;
    /// Load checkpoint state for a file.
    fn load_checkpoint(&self, file_hash: &str) -> Result<Option<CheckpointState>>;
}

/// Catalog trait for episode metadata.
pub trait Catalog: Send + Sync {
    /// Save episode metadata to the catalog.
    fn save_episode(&self, metadata: &EpisodeMetadata) -> Result<()>;
    /// Get episode metadata by ID.
    fn get_episode(&self, id: &str) -> Result<Option<EpisodeMetadata>>;
    /// List episodes with a given prefix.
    fn list_episodes(&self, prefix: &str) -> Result<Vec<EpisodeMetadata>>;

    /// Mark an upload as started.
    fn start_upload(&self, id: &str) -> Result<()>;
    /// Mark an upload as completed.
    fn complete_upload(&self, id: &str) -> Result<()>;
    /// Get the current upload status.
    fn get_upload_status(&self, id: &str) -> Result<UploadStatus>;
}
