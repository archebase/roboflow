// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! LeRobot converter orchestrator for distributed processing.
//!
//! This module provides the `LeRobotConverter` which coordinates:
//! - Episode index allocation (via `EpisodeAllocator`)
//! - LerobotWriter configuration with dynamic episode/chunk indices
//! - Checkpoint state management for recovery
//!
//! # Example
//!
//! ```ignore
//! use roboflow_distributed::{
//!     LeRobotConverter, ConverterConfig, TiKVEpisodeAllocator,
//! };
//!
//! // Create with TiKV backend for distributed processing
//! let allocator = Arc::new(TiKVEpisodeAllocator::new(
//!     tikv_client,
//!     "batch-001".to_string(),
//!     500, // episodes_per_chunk
//! ));
//!
//! let config = ConverterConfig::with_batch(
//!     "batch-001",
//!     "s3://bucket/dataset",
//!     500,
//! );
//!
//! let mut converter = LeRobotConverter::new(allocator, config);
//!
//! // Allocate episode
//! let allocation = converter.allocate_episode().await?;
//!
//! // Configure writer
//! converter.configure_writer(&mut writer, &allocation)?;
//!
//! // Process file...
//!
//! // Update checkpoint periodically
//! converter.update_checkpoint(frame_idx, byte_offset)?;
//! ```

mod orchestrator;

pub use orchestrator::{
    ConverterConfig, ConverterError, DEFAULT_EPISODES_PER_CHUNK, LeRobotConverter,
};
