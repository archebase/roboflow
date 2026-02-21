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
pub mod camera_streaming_pipeline;
pub mod config;
pub mod image_decode;
pub mod message_utils;
pub mod operation;
pub mod parquet_base;
pub mod progress;
pub mod ring_buffer;
pub mod video;

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

// Re-export image decode utilities
pub use image_decode::{decode_image_to_rgb, decode_to_rgb};

// Re-export ring buffer for streaming frame processing
pub use ring_buffer::{FrameRingBuffer, RingBufferError, RingBufferSnapshot};

// Re-export video utilities from the video module (which re-exports from roboflow-video)
#[cfg(target_os = "macos")]
pub use video::VideoToolboxEncoder;
pub use video::{
    DepthEncoderConfig, DepthMkvEncoder, EncoderChoice, Mp4Encoder, NvencEncoder,
    VideoEncoderConfig, VideoEncoderError, VideoFrame, VideoFrameBuffer, available_encoders,
    check_nvenc_available, check_videotoolbox_available, is_encoder_available,
    print_encoder_diagnostics, select_best_encoder,
};

// Re-export SIMD RGB to YUV conversion from roboflow-video (canonical location)
pub use crate::media::video::{ConversionStrategy, optimal_strategy, rgb_to_nv12, rgb_to_yuv420p};

// Platform-specific re-exports
#[cfg(target_os = "macos")]
pub use video::VideoToolboxEncoder as AppleVideoEncoder;

// Re-export streaming encoder from roboflow-video (canonical location)
pub use crate::media::video::streaming::{
    EncodedChunk, StreamingEncoderConfig, StreamingMp4Encoder,
};

// Re-export camera streaming pipeline
pub use camera_streaming_pipeline::{
    CameraStreamingPipeline, StreamingCommand, StreamingPipelineConfig, StreamingPipelineHandle,
    StreamingPipelineResult, StreamingUploadCommand, spawn_streaming_pipeline,
};

// Re-export concurrent video encoder from media/video (canonical location)
pub use crate::media::video::{
    ConcurrentEncoderConfig, ConcurrentEncoderResult, ConcurrentVideoEncoder,
};

// Re-export DatasetStats for return values, but keep WriteOperation/Sink/VecSink internal
pub use operation::DatasetStats;

pub use message_utils::{extract_image_bytes, extract_u32, is_camera_info_topic};
