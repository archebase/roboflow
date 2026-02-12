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
pub mod camera_pipeline;
pub mod concurrent_video_encoder;
pub mod config;
pub mod fragment_encoder;
pub mod fragment_uploader;
pub mod image_decode;
pub mod image_format;
pub mod parquet_base;
pub mod progress;
pub mod ring_buffer;
pub mod rsmpeg_encoder;
pub mod simd_convert;
pub mod streaming_uploader;
pub mod video;

// Re-export core types (shared across all formats)
pub use base::{
    AlignedFrame, AudioData, DatasetWriter, DatasetWriterError, ImageData, WriterStats,
};

// Re-export shared config types
pub use config::{DatasetBaseConfig, Mapping, MappingType};

// Re-export parquet utilities
pub use parquet_base::{FeatureStats, ParquetWriterBase, calculate_stats};

// Re-export progress utilities
pub use progress::{ProgressReceiver, ProgressSender, ProgressUpdate};

// Re-export image format detection
pub use image_format::{ImageFormat, can_passthrough, detect_image_format};

// Re-export image decode utilities
pub use image_decode::{decode_image_to_rgb, decode_to_rgb};

// Re-export ring buffer for streaming frame processing
pub use ring_buffer::{FrameRingBuffer, RingBufferError, RingBufferSnapshot};

// Re-export video utilities including hardware-accelerated encoders
pub use video::{
    DepthMkvEncoder, EncoderChoice, Mp4Encoder, NvencEncoder, VideoFrame, VideoFrameBuffer,
    VideoToolboxEncoder, available_encoders, check_nvenc_available, check_videotoolbox_available,
    is_encoder_available, print_encoder_diagnostics, select_best_encoder,
};

// Re-export SIMD RGB to YUV conversion
pub use simd_convert::{ConversionStrategy, optimal_strategy, rgb_to_nv12, rgb_to_yuv420p};

// Platform-specific re-exports
#[cfg(target_os = "macos")]
pub use video::VideoToolboxEncoder as AppleVideoEncoder;

// Re-export streaming uploader
pub use streaming_uploader::{StreamingUploader, UploadConfig, UploadProgress, UploadStats};

// Re-export rsmpeg encoder
pub use rsmpeg_encoder::{
    EncodeFrame, RsmpegEncoder, RsmpegEncoderConfig, RsmpegMp4Encoder, default_codec_name,
    is_hardware_encoding_available, is_rsmpeg_available,
};

// Re-export fragment encoder
pub use fragment_encoder::{FragmentEncoder, FragmentEncoderConfig, FragmentInfo};

// Re-export fragment uploader
pub use fragment_uploader::UploadCommand;

// Re-export camera pipeline
pub use camera_pipeline::{
    CameraPipeline, CameraPipelineConfig, CameraPipelineHandle, CameraPipelineResult,
    PipelineCommand, spawn_camera_pipeline,
};

// Re-export concurrent video encoder
pub use concurrent_video_encoder::{
    ConcurrentEncoderConfig, ConcurrentEncoderResult, ConcurrentVideoEncoder,
};
