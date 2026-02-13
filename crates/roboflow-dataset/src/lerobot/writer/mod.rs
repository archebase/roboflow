// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! LeRobot v2.1 dataset writer.
//!
//! Writes robotics data in LeRobot v2.1 format with:
//! - Parquet files for frame data (one per episode)
//! - MP4 videos for camera observations (one per camera per episode)
//! - Camera parameters (intrinsic/extrinsic) in `parameters/` directory
//! - Complete metadata files
//!
//! # Example
//!
//! ```ignore
//! use roboflow::dataset::lerobot::{LerobotWriter, LerobotConfig};
//!
//! // Local filesystem output
//! let config = LerobotConfig::default();
//! let writer = LerobotWriter::new_local("/output/dataset", config)?;
//!
//! // Or using the builder pattern
//! let writer = LerobotWriter::builder()
//!     .output_dir("/output/dataset")
//!     .config(config)
//!     .build()?;
//!
//! // Cloud storage output
//! let storage = StorageFactory::new().create("s3://my-bucket/datasets")?;
//! let writer = LerobotWriter::builder()
//!     .storage(storage)
//!     .output_prefix("my_dataset".to_string())
//!     .local_buffer("/tmp/roboflow_buffer")
//!     .config(config)
//!     .build()?;
//! ```
//!
//! # Module Organization
//!
//! - [`LerobotWriter`] - Main writer type (re-exported from `writer_impl`)
//! - [`LerobotWriterBuilder`] - Builder for configuring writers
//! - [`LerobotFrame`] - Frame data structure
//! - [`CameraIntrinsic`], [`CameraExtrinsic`], [`ExtrinsicData`] - Camera calibration types

mod camera;
mod cloud_upload;
mod encoding;
mod frame;
mod parquet;
mod stats;
mod writer_impl;

// Re-export public API
pub use camera::{CameraExtrinsic, CameraIntrinsic, ExtrinsicData};
pub use frame::LerobotFrame;
pub use writer_impl::{LerobotWriter, LerobotWriterBuilder};
