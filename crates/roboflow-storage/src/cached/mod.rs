// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Cached storage backend with local buffering and background uploads.
//!
//! This module provides a caching layer that combines:
//! - **Read-through caching**: Check local cache first, download from remote on miss
//! - **Write-behind caching**: Write to local cache, queue for background upload
//! - **LRU eviction**: Automatically evict oldest cached files when size limit is reached
//! - **Graceful shutdown**: Flush pending uploads before shutdown
//!
//! # Example
//!
//! ```ignore
//! use roboflow::storage::{Storage, LocalStorage, cached::{CachedStorage, CacheConfig}};
//! use std::sync::Arc;
//!
//! let remote = Arc::new(S3Storage::new(...)?);
//! let cache_dir = "/tmp/cache";
//! let config = CacheConfig::new(cache_dir);
//! let storage = CachedStorage::new(remote, config)?;
//!
//! // Reads check cache first
//! let reader = storage.reader(Path::new("dataset.bag"))?;
//!
//! // Writes go to cache and are uploaded in background
//! let writer = storage.writer(Path::new("output.bag"))?;
//! writer.write_all(data)?;
//! drop(writer); // Triggers background upload
//!
//! // Graceful shutdown
//! storage.flush()?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod eviction;
mod storage;
mod upload;

// Re-export public API
pub use eviction::EvictionPolicy;
pub use storage::{CacheConfig, CachedStorage, CachedWriter};
pub use upload::CacheStats;
