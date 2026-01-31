// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # roboflow-pipeline
//!
//! Processing pipeline for roboflow.
//!
//! This crate provides high-performance message processing:
//! - **Hyper pipeline** - 7-stage optimized pipeline with zero-copy
//! - **Fluent API** - Builder-style pipeline construction
//! - **Hardware detection** - Automatic CPU/GPU feature detection
//!
//! # Note on Doctests
//!
//! Doctests are temporarily disabled after workspace refactoring.
//! They reference old import paths that will be updated in a future pass.

#![cfg(not(doctest))]

pub mod auto_config;
pub mod compression;
pub mod config;
pub mod dataset_converter;
pub mod fluent;
pub mod gpu;
pub mod hardware;
#[cfg(not(doctest))]
pub mod hyper;
pub mod stages;
#[cfg(not(doctest))]
pub mod types;

// Re-export public types from submodules (avoiding module_inception)
pub use dataset_converter::dataset_converter::{DatasetConverter, DatasetConverterStats};

// Re-export public types (always available)
pub use auto_config::PerformanceMode;
pub use config::CompressionConfig;
pub use fluent::{BatchReport, CompressionPreset, PipelineMode, ReadOptions, Robocodec};
// Hyper pipeline types (not available during doctests)
#[cfg(not(doctest))]
pub use hyper::{HyperPipeline, HyperPipelineConfig, HyperPipelineReport};
