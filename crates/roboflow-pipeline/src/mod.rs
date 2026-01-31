// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! High-performance pipeline for robotics data formats.
//!
//! This module provides a production-grade 7-stage hyper pipeline that maximizes
//! CPU utilization through zero-copy operations, platform-specific I/O optimization,
//! and lock-free inter-stage communication.
//!
//! # Architecture
//!
//! The hyper pipeline consists of 7 stages:
//!
//! ```text
//! Prefetcher → Parser → Batcher → Transform → Compressor → CRC → Writer
//!   (io_uring)   (mmap)    (align)     (topic)    (zstd)    (pack)  (seq)
//! ```
//!
//! # Modules
//!
//! - `types` - Core data structures (MessageChunk, BufferPool)
//! - `stages` - Pipeline stage implementations
//! - `compression` - Parallel compression utilities
//! - `config` - Pipeline configuration types
//! - `auto_config` - Automatic hardware-aware configuration
//! - `gpu` - GPU compression (experimental, requires "gpu" feature)
//! - `hyper` - 7-stage hyper pipeline implementation
//! - `fluent` - Fluent API for pipeline construction
//! - `dataset_converter` - Direct dataset format conversion
//!
//! # Example
//!
//! ```no_run
//! use roboflow::Robocodec;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let report = Robocodec::open(vec!["input.bag"])?
//!         .write_to("output.mcap")
//!         .run()?;
//!
//!     println!("Throughput: {:.2} MB/s", report.throughput_mb_s);
//!     Ok(())
//! }
//! ```
//!

// Core data structures
#[cfg(not(doctest))]
pub mod types;

// Hardware detection for auto-tuning
pub mod hardware;

// Pipeline stages
pub mod stages;

// Compression utilities
pub mod compression;

// GPU compression module (experimental, requires "gpu" feature)
#[cfg(feature = "gpu")]
pub mod gpu;

// Pipeline configuration
pub mod auto_config;
pub mod config;
pub mod dataset_converter;

// 7-stage hyper-pipeline for maximum throughput
#[cfg(not(doctest))]
pub mod hyper;

// Fluent API for batch processing
pub mod fluent;

// Re-exports for convenience
pub use auto_config::PerformanceMode;
pub use compression::ParallelCompressor;
pub use config::CompressionConfig;
pub use dataset_converter::{DatasetConverter, DatasetConverterStats};
pub use fluent::{BatchReport, CompressionPreset, PipelineMode, ReadOptions, Robocodec};
pub use hardware::{HardwareInfo, detect_cpu_count};
pub use stages::TransformStage;

// HyperPipeline re-exports
#[cfg(not(doctest))]
pub use hyper::{HyperPipeline, HyperPipelineConfig, HyperPipelineReport};
