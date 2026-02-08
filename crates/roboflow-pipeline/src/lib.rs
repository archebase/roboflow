// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # roboflow-pipeline
//!
//! Processing pipeline for roboflow.
//!
//! This crate provides high-performance message processing:
//! - **New Framework** - Pluggable Source/Sink architecture for flexible pipelines
//! - **Hyper pipeline** - 7-stage optimized pipeline with zero-copy
//! - **Hardware detection** - Automatic CPU feature detection

#![cfg(not(doctest))]

pub mod auto_config;
pub mod compression;
pub mod config;
pub mod framework;
pub mod hardware;
#[cfg(not(doctest))]
pub mod hyper;
#[cfg(not(doctest))]
pub mod types;

// Re-export public types (always available)
pub use auto_config::PerformanceMode;
pub use config::CompressionConfig;

// New framework exports
pub use framework::{DistributedExecutor, Pipeline, PipelineConfig, PipelineReport};

// Hyper pipeline types (not available during doctests)
#[cfg(not(doctest))]
pub use hyper::{HyperPipeline, HyperPipelineConfig, HyperPipelineReport};
