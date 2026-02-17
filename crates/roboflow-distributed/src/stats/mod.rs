// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Episode statistics collection for distributed LeRobot dataset generation.
//!
//! This module provides a clean architecture for collecting, aggregating, and
//! persisting episode statistics across distributed workers. The design follows
//! these principles:
//!
//! - **Trait-based abstraction**: `StatsCollector` trait enables pluggable backends
//! - **TiKV-backed storage**: Primary production implementation using TiKV
//! - **Incremental aggregation**: Workers push stats as they process episodes
//! - **Finalizer integration**: Stats are merged and written to LeRobot metadata
//!
//! # Architecture
//!
//! ```text
//! Worker                    TiKV                     Finalizer
//!   │                        │                          │
//!   │ record_episode_stats() │                          │
//!   │───────────────────────>│                          │
//!   │                        │                          │
//!   │        ...             │    get_batch_stats()     │
//!   │                        │<─────────────────────────│
//!   │                        │                          │
//!   │                        │    BatchStatsSummary     │
//!   │                        │─────────────────────────>│
//!   │                        │                          │
//!   │                        │    write episodes_stats  │
//!   │                        │                          │──> meta/
//! ```
//!
//! # Usage
//!
//! ```ignore
//! use roboflow_distributed::stats::{StatsCollector, TiKVStatsCollector, EpisodeStats};
//!
//! // In worker: record stats after processing episode
//! let collector = TiKVStatsCollector::new(tikv_client);
//! collector.record_episode_stats("batch-123", 42, episode_stats).await?;
//!
//! // In finalizer: aggregate all stats
//! let summary = collector.get_batch_stats("batch-123").await?;
//! // Write to metadata...
//! ```

mod collector;
mod keys;
mod tikv_collector;
mod types;

pub use collector::StatsCollector;
pub use keys::StatsKeys;
pub use tikv_collector::TiKVStatsCollector;
pub use types::{BatchStatsSummary, EpisodeStats, FeatureStats};
