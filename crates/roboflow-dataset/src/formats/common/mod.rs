// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Common dataset writing utilities.
//!
//! This module contains shared components used by different dataset formats
//! (KPS, LeRobot, etc.) to avoid code duplication.
//!
//! # Key Exports
//!
//! - [`DatasetWriter`] - Core trait for all dataset writers
//! - [`AlignedFrame`] - Universal aligned frame data structure
//! - [`ImageData`], [`AudioData`] - Shared multimedia types
//! - [`WriterStats`] - Common statistics structure
//! - [`ProgressSender`] - Channel-based progress reporting

pub mod base;
pub mod config;
pub mod message_utils;
pub mod operation;
pub mod parquet_base;
pub mod progress;
pub mod ring_buffer;

// Re-export core types (shared across all formats)
pub use base::{
    AlignedFrame, AudioData, CameraInfo, DatasetFrame, DatasetWriter, DatasetWriterError,
    ImageData, WriterStats,
};

// Re-export shared config types
pub use config::{DatasetBaseConfig, Mapping, MappingType};

// Re-export parquet utilities
pub use parquet_base::{FeatureStats, ParquetWriterBase, calculate_stats};

// Re-export progress utilities
pub use progress::{ProgressReceiver, ProgressSender, ProgressUpdate};

// Re-export image format detection from the image module (canonical location)
pub use crate::media::image::{ImageFormat, can_passthrough, detect_image_format};

// Re-export image decode utilities from media/image (canonical location)
pub use crate::media::image::{decode_image_to_rgb, decode_to_rgb};

// Re-export ring buffer for streaming frame processing
pub use ring_buffer::{FrameRingBuffer, RingBufferError, RingBufferSnapshot};

// Re-export SIMD RGB to YUV conversion
pub use crate::media::video::{ConversionStrategy, optimal_strategy, rgb_to_nv12, rgb_to_yuv420p};

// Re-export EncodedChunk (used for streaming output)
pub use crate::media::video::EncodedChunk;

// Re-export concurrent video encoder
pub use crate::media::video::{
    ConcurrentEncoderConfig, ConcurrentEncoderResult, ConcurrentVideoEncoder,
};

// Re-export unified encoder types
pub use crate::media::video::OutputConfig as VideoOutputConfig;
pub use crate::media::video::{EncodingResult, VideoEncoder};

// Re-export DatasetStats for return values, but keep WriteOperation/Sink/VecSink internal
pub use operation::DatasetStats;

pub use message_utils::{extract_image_bytes, extract_u32, is_camera_info_topic};
