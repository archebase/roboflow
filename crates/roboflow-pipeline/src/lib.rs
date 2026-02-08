// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # roboflow-pipeline
//!
//! Processing pipeline for roboflow.
//!
//! This crate provides high-performance message processing:
//! - **Hyper pipeline** - 7-stage optimized pipeline with zero-copy
//! - **Hardware detection** - Automatic CPU feature detection
//! - **Dataset converter** - Direct conversion to dataset formats

#![cfg(not(doctest))]

pub mod auto_config;
pub mod compression;
pub mod config;
pub mod dataset_converter;
pub mod hardware;
#[cfg(not(doctest))]
pub mod hyper;
#[cfg(not(doctest))]
pub mod types;

// Re-export public types from submodules
pub use dataset_converter::{DatasetConverter, DatasetConverterStats};

// Re-export public types (always available)
pub use auto_config::PerformanceMode;
pub use config::CompressionConfig;

// Hyper pipeline types (not available during doctests)
#[cfg(not(doctest))]
pub use hyper::{HyperPipeline, HyperPipelineConfig, HyperPipelineReport};
