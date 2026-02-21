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
//! - [`conversion`] - High-level conversion API (recommended entry point)
//! - [`core`] - Core traits and types for format-agnostic writing
//! - [`formats`] - Format-specific implementations (LeRobot, etc.)
//! - [`media`] - Media handling (video encoding, image decoding)
//! - [`sources`] - Data source abstractions (bag, MCAP)
//!
//! # Quick Start
//!
//! ```rust,ignore
//! use roboflow_dataset::conversion::{convert_file, ConversionConfig};
//! use roboflow_dataset::formats::{DatasetConfig, DatasetFormat};
//!
//! let config = ConversionConfig::new(
//!     DatasetConfig::new(DatasetFormat::Lerobot, "my_dataset", 30, None)
//! );
//!
//! let result = convert_file(
//!     Path::new("recording.bag"),
//!     Path::new("./output"),
//!     &config,
//! )?;
//! ```
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

pub mod conversion;
pub mod core;
pub mod executor;
pub mod formats;
pub mod media;
pub mod sources;

// Internal module for local file operations
mod storage_sink;

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

pub use formats::{DatasetWriter, PipelineConfig, PipelineExecutor, PipelineStats};

pub use formats::lerobot::{
    DatasetConfig, LerobotConfig, LerobotWriter, Mapping, MappingType, StreamingConfig, VideoConfig,
};

// Re-export video types from media::video for backward compatibility
pub use media::video::{
    ConcurrentEncoderConfig, ConcurrentEncoderResult, ConcurrentVideoEncoder, FragmentEncoder,
    FragmentEncoderConfig, PixelFormat, StreamingEncoderConfig, StreamingMp4Encoder,
    VideoEncoderConfig, VideoFrame,
};

// Re-export conversion API
pub use conversion::{
    ConversionConfig, ConversionResult, ConversionStats, OutputFiles, convert_file,
};
