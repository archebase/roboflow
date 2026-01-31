// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Streaming dataset conversion with bounded memory footprint.
//!
//! This module provides a true streaming conversion system that processes
//! robotics data files (MCAP/Bag) to dataset formats (LeRobot, KPS) without
//! buffering entire datasets in memory.
//!
//! # Zero Intermediate Conversion Guarantee
//!
//! **CRITICAL**: This module performs direct format conversion with ZERO intermediate
//! MCAP conversion at any point:
//!
//! - **BAG files** → RoboReader decodes BAG format directly → in-memory structures
//! - **MCAP files** → RoboReader decodes MCAP format directly → in-memory structures
//! - **NO on-disk intermediate files** (no temporary MCAP, no temporary BAG files)
//! - **NO in-memory MCAP structures** (messages decoded to simple HashMaps via CodecValue)
//!
//! The data path is:
//! ```text
//! Input File (BAG or MCAP)
//!     ↓
//! RoboReader (native format parsing from robocodec crate)
//!     ↓
//! TimestampedDecodedMessage (decoded message + timestamp)
//!     ↓
//! TimestampedMessage (our internal struct: HashMap<String, CodecValue>)
//!     ↓
//! FrameAlignmentBuffer (bounded streaming buffer)
//!     ↓
//! DatasetWriter (LeRobot/KPS writers)
//!     ↓
//! Output Files (Parquet+MP4 or HDF5+Parquet)
//! ```
//!
//! # Architecture
//!
//! ```text
//! Input File → StreamingDatasetConverter → FrameAlignmentBuffer → DatasetWriter → Output
//!              (orchestration)           (bounded buffer)     (streaming)
//! ```
//!
//! # Key Features
//!
//! - **Fixed memory footprint**: Only incomplete frames are buffered
//! - **Progressive output**: Frames are written as soon as they're complete
//! - **Backpressure handling**: Memory limits force frame completion
//! - **Out-of-order handling**: Completion window tolerates late messages
//! - **Observable**: Progress tracking and statistics throughout
//! - **Zero intermediate conversion**: Direct BAG/MCAP → dataset format
//!
//! # Example
//!
//! ```rust,ignore
//! use roboflow::dataset::streaming::{StreamingDatasetConverter, StreamingConfig};
//!
//! let config = StreamingConfig {
//!     fps: 30,
//!     completion_window_frames: 5,
//!     max_buffered_frames: 300,
//!     ..Default::default()
//! };
//!
//! let converter = StreamingDatasetConverter::new(
//!     "/output".into(),
//!     roboflow::dataset::DatasetFormat::Lerobot,
//!     lerobot_config,
//!     config,
//! )?;
//!
//! let stats = converter.convert("/input.bag")?;
//! println!("Converted {} frames", stats.frames_written);
//! ```

pub mod alignment;
pub mod backpressure;
pub mod completion;
pub mod config;
pub mod converter;
pub mod download;
pub mod stats;
pub mod temp_file;

pub use alignment::{FrameAlignmentBuffer, PartialFrame};
pub use backpressure::{BackpressureHandler, BackpressureStrategy};
pub use completion::FrameCompletionCriteria;
pub use config::{FeatureRequirement, LateMessageStrategy, StreamingConfig};
pub use converter::StreamingDatasetConverter;
pub use stats::{AlignmentStats, StreamingStats};
pub use temp_file::TempFileManager;
