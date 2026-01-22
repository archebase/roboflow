// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Parallel read-compress-write pipeline for robotics data formats.
//!
//! This module provides a production-grade pipeline that maximizes CPU utilization
//! through multi-stage parallelism, auto-tuning, and backpressure handling.
//!
//! # Architecture
//!
//! The pipeline consists of 4 stages:
//!
//! ```text
//! Reader → Transform → Compression → Writer
//!  (1th)     (1th)        (N threads)    (1th)
//! ```
//!
//! # Modules
//!
//! - `types` - Core data structures (MessageChunk, MessageArena, BufferPool)
//! - `stages` - Pipeline stage implementations (Reader, Transform, Compression, Writer)
//! - `compression` - Parallel compression utilities
//! - `orchestrator` - Main pipeline coordinator and builder
//! - `config` - Pipeline configuration types
//! - `auto_config` - Automatic hardware-aware configuration
//! - `gpu` - GPU compression (experimental, requires "gpu" feature)
//!
//! # Example
//!
//! ```no_run
//! use roboflow::pipeline::PipelineBuilder;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let pipeline = PipelineBuilder::new()
//!         .input_path("input.bag")
//!         .output_path("output.mcap")
//!         .build()?;
//!
//!     let report = pipeline.run()?;
//!     println!("Throughput: {:.2} MB/s", report.average_throughput_mb_s);
//!     Ok(())
//! }
//! ```

// Core data structures
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

// Pipeline configuration and coordinator
pub mod auto_config;
pub mod config;
pub mod orchestrator;

// 7-stage hyper-pipeline for maximum throughput
pub mod hyper;

// Fluent API for batch processing
pub mod fluent;

// Re-exports for convenience
pub use auto_config::{PerformanceMode, PipelineAutoConfig};
pub use compression::ParallelCompressor;
pub use config::{CompressionConfig, CompressionTarget};
pub use fluent::{BatchReport, CompressionPreset, PipelineMode, ReadOptions, Robocodec};
pub use hardware::{detect_cpu_count, HardwareInfo};
pub use orchestrator::{AsyncPipeline, PipelineBuilder, PipelineReport};
pub use stages::{TransformStage, TransformStageConfig};
pub use types::{ArenaMessage, CompressedChunk, MessageArena, MessageChunk};

// HyperPipeline re-exports
pub use hyper::{HyperPipeline, HyperPipelineConfig, HyperPipelineReport};
