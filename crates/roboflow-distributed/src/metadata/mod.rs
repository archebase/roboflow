// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Distributed dataset metadata management.
//!
//! This module provides coordination for LeRobot dataset metadata across
//! distributed workers. It handles:
//!
//! - **Task deduplication** - Global task → index mapping via TiKV
//! - **Feature unification** - Consistent feature specs across episodes
//! - **Metadata aggregation** - Collect partial metadata from workers
//! - **Final assembly** - Write LeRobot v2.1 metadata files
//!
//! # Architecture
//!
//! ```text
//! Workers (per-episode):
//!   1. Allocate episode index from TiKV
//!   2. Convert bag file → episode data
//!   3. Register tasks in TiKV (global deduplication)
//!   4. Register feature specs (with validation)
//!   5. Store partial episode metadata in TiKV
//!
//! Finalizer (once per batch):
//!   1. Scan all episode metadata from TiKV
//!   2. Build tasks.jsonl from task registry
//!   3. Build unified feature specs
//!   4. Aggregate statistics
//!   5. Write LeRobot metadata files to storage
//! ```
//!
//! # TiKV Key Schema
//!
//! ```text
//! /roboflow/v1/batch/{batch_id}/
//! ├── task_counter              → Task index allocation
//! ├── tasks/{task_hash}         → Task description → global index
//! ├── features/{feature_name}   → Unified feature specification
//! └── metadata/episode/{idx:06} → Per-episode metadata
//! ```

mod assembler;
mod keys;
mod registry;
mod types;

// Public exports
pub use assembler::{GlobalMetadataAssembler, MetadataAssemblyError};
pub use keys::MetadataKeys;
pub use registry::DatasetMetadataRegistry;
pub use types::{
    EpisodeInfo, EpisodeStatsEntry, FeatureInfo, FeatureShape, FeatureSpec, LerobotInfo,
    PartialEpisodeMetadata, TaskEntry, TaskInfo, VideoFeatureInfo, VideoInfo,
};
