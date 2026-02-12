// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # roboflow-storage
//!
//! Storage abstraction layer for roboflow.
//!
//! This crate provides a unified storage abstraction that supports multiple backends:
//! - **Local filesystem** - Always available for development/testing
//! - **S3-compatible storage** - Amazon S3, Alibaba OSS, MinIO, etc. (always available)
//!
//! ## Design Philosophy
//!
//! **S3 is the production storage layer** for distributed systems.
//! Local filesystem is for development/testing only.
//! **No feature flags** - all storage backends are always available.
//!
//! ## Example
//!
//! ```ignore
//! use roboflow_storage::{Storage, LocalStorage, S3Storage};
//!
//! // Local storage (for development)
//! let local = LocalStorage::new("/tmp")?;
//!
//! // Cloud storage (for production - S3-compatible: Amazon S3, Alibaba OSS, MinIO, etc.)
//! let s3 = S3Storage::new("bucket", "endpoint", "key", "secret")?;
//!
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

// Core modules (split for maintainability)
mod error;
mod metadata;
mod traits;

// Implementation modules
pub mod async_storage;
pub mod cached;
pub mod config_file;
pub mod factory;
pub mod local;
pub mod multipart;
pub mod multipart_parallel;
pub mod retry;
pub mod s3;
pub mod streaming;
pub mod streaming_upload;
pub mod url;

// Re-export core types from error module
pub use error::{StorageError, StorageResult};

// Re-export core types from metadata module
pub use metadata::{ObjectMetadata, StreamingConfig};

// Re-export core traits from traits module
pub use traits::{SeekRead, SeekableStorage, Storage, StreamingRead};

// Re-export implementation types
pub use async_storage::AsyncStorage;
pub use cached::{CacheConfig, CacheStats, CachedStorage, EvictionPolicy};
pub use config_file::{ConfigError, RoboflowConfig};
pub use factory::{StorageConfig, StorageFactory};
pub use local::LocalStorage;

// Re-export object_store for multipart upload
pub use multipart::{
    MultipartConfig, MultipartStats, MultipartUploader, ProgressCallback, upload_multipart,
};
pub use multipart_parallel::{
    ParallelMultipartStats, ParallelMultipartUploader, ParallelUploadConfig, UploadedPart,
    is_upload_expired, upload_multipart_parallel,
};
pub use object_store;
pub use object_store::path::Path as ObjectPath;
// S3-compatible storage (Amazon S3, Alibaba OSS, MinIO, etc.)
pub use retry::{RetryConfig, RetryingStorage, retry_with_backoff};
pub use s3::{AsyncS3Storage, S3Config, S3Storage};
pub use streaming_upload::{
    CloudMultipartUpload, LocalMultipartUpload, MultipartUpload, StorageStreamingExt, UploadStats,
};
pub use url::StorageUrl;
