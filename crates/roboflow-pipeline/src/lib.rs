// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Roboflow Pipeline - Format-agnostic dataset writing.
//!
//! This crate provides infrastructure for converting robotics data
//! (bag/MCAP files) to trainable dataset formats (LeRobot, HDF5, etc.).
//!
//! # Architecture
//!
//! - [`core`] - Core traits and types for format-agnostic writing
//! - [`formats`] - Format-specific implementations (LeRobot, etc.)
//! - [`media`] - Media handling (video encoding, image decoding)
//! - [`sources`] - Data source abstractions (bag, MCAP)
//!
//! # Example
//!
//! ```rust,ignore
//! use roboflow_pipeline::core::{FormatWriter, AlignedFrame};
//!
//! let mut writer = LerobotWriter::builder()
//!     .output_dir("/output")
//!     .config(config)
//!     .build()?;
//!
//! writer.start_episode(None)?;
//! for frame in frames {
//!     writer.write_frame(&frame)?;
//! }
//! writer.finish_episode()?;
//! let stats = writer.finalize()?;
//! ```

pub mod core;
pub mod formats;
pub mod media;
pub mod sources;
pub mod storage_sink;

// Re-export format submodules for convenient access
pub use formats::alignment;
pub use formats::common;
pub use formats::lerobot;

// Re-export commonly used types
pub use sources::{
    BagSource, BagSourceBatched, BagSourceBlocking, McapSource, Source, SourceConfig, SourceType,
    TimestampedMessage, create_source,
};

pub use formats::{OutputConfig, OutputFormat};

pub use formats::lerobot::{LerobotWriterConfig, LerobotWriterResult, create_lerobot_writer};

pub use formats::common::{CameraInfo, DatasetFrame, ImageData};

pub use formats::{DatasetWriter, PipelineConfig, PipelineExecutor, PipelineStats};

pub use formats::lerobot::{
    DatasetConfig, LerobotConfig, LerobotWriter, Mapping, MappingType, StreamingConfig, VideoConfig,
};

// Re-export video types from media::video for backward compatibility
pub use media::video::{
    FragmentEncoder, FragmentEncoderConfig, PixelFormat, StreamingEncoderConfig,
    StreamingMp4Encoder, VideoEncoderConfig, VideoFrame,
};

pub use storage_sink::StorageSink;
