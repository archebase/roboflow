// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # Rsmpeg Native Streaming Encoder
//!
//! This module provides high-performance video encoding using native FFmpeg bindings
//! via the rsmpeg library.
//!
//! ## Features
//!
//! - In-process FFmpeg encoding (no subprocess overhead)
//! - RGB to YUV420P/NV12 conversion via SWScale
//! - Hardware encoder support (NVENC, VideoToolbox) with fallback to libx264
//!
//! ## Performance
//!
//! - Target: 1200 MB/s encoding throughput
//! - 2-3x faster than FFmpeg CLI for CPU encoding
//! - 5-10x faster with hardware encoders

mod config;
mod encoder;
mod frame;
mod mp4;
mod storage;
mod utils;

// Re-export rsmpeg types selectively to avoid ambiguous glob re-exports
pub use rsmpeg::{
    avcodec::{AVCodec, AVCodecContext, AVCodecID, AVPacket},
    avformat::AVFormatContextOutput,
    avutil::{AVFrame, AVRational},
    error::RsmpegError,
    swscale::SwsContext,
};

// Re-export VideoEncoderConfig for API compatibility
pub use crate::common::video::VideoEncoderConfig;

// Public API
pub use config::RsmpegEncoderConfig;
pub use encoder::RsmpegEncoder;
pub use frame::EncodeFrame;
pub use mp4::RsmpegMp4Encoder;
pub use storage::StorageRsmpegEncoder;
pub use utils::{default_codec_name, is_hardware_encoding_available, is_rsmpeg_available};
