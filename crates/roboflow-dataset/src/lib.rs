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
//! - [`formats`] - Format-specific implementations (LeRobot, etc.)
//! - [`core`] - Core traits and types for format-agnostic writing
//! - [`media`] - Media handling (video encoding, image decoding)
//! - [`sources`] - Data source abstractions (bag, MCAP)
//!
//! # Media Types
//!
//! This crate re-exports media types from `roboflow-media` for convenience:
//! - [`image`] - Image decoding, format detection, and passthrough utilities
//! - [`video`] - Video encoding with hardware acceleration support
//! - [`ImageData`], [`CameraInfo`] - Core media data structures
//!
//! For direct media processing without dataset conversion, use the
//! `roboflow-media` crate directly.
//!
//! # Low-Level API
//!
//! For more control, you can use the lower-level APIs directly:
//!
//! ```rust,ignore
//! use roboflow_dataset::core::{FormatWriter, AlignedFrame};
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
pub mod sources;

// Re-export media types from roboflow-media
pub use roboflow_media::{image, video};

pub mod testing;

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

pub use formats::DatasetWriter;

pub use formats::lerobot::{
    DatasetConfig, LerobotConfig, LerobotWriter, Mapping, MappingType, StreamingConfig, VideoConfig,
};

// Re-export video types from roboflow_media
pub use roboflow_media::video::{
    ConcurrentEncoderConfig, ConcurrentEncoderResult, ConcurrentVideoEncoder, PixelFormat,
    VideoEncoderConfig, VideoFrame,
};

// Re-export unified encoder (OutputConfig aliased to avoid conflict with formats::OutputConfig)
pub use roboflow_media::video::OutputConfig as VideoOutputConfig;
pub use roboflow_media::video::{EncodingResult, VideoEncoder};
