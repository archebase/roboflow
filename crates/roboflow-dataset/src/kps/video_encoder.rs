// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Re-export video encoder from common for backward compatibility.
//!
//! The actual implementation lives in [`crate::common::video`].
//! This module re-exports everything so existing `kps::video_encoder` paths continue to work.

pub use crate::common::video::{
    DepthEncoderConfig, DepthFrame, DepthFrameBuffer, DepthMkvEncoder, Mp4Encoder,
    VideoEncoderConfig, VideoEncoderError, VideoFrame, VideoFrameBuffer,
};
