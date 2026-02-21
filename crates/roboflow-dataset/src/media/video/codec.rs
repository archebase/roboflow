// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Shared codec initialization helpers for video encoders.
//!
//! This module extracts the common codec setup sequence that was previously
//! duplicated across `RsmpegEncoder`, `RsmpegMp4Encoder`, `PersistentEncoder`,
//! and `StreamingMp4Encoder`. Each encoder delegates to these helpers instead
//! of reimplementing codec detection, context configuration, and pixel format
//! conversion.

use std::ffi::CStr;

use rsmpeg::avcodec::{AVCodec, AVCodecContext};
use rsmpeg::avutil::{AVDictionary, AVFrame, AVRational};
use rsmpeg::ffi;
use rsmpeg::swscale::SwsContext;

// =============================================================================
// Types
// =============================================================================

/// Parameters for codec context configuration.
pub struct CodecParams {
    pub width: u32,
    pub height: u32,
    pub fps: u32,
    pub bitrate: i64,
    pub gop_size: u32,
    pub max_b_frames: u32,
}

/// Fully initialized codec context with SWScale converter.
///
/// Created by [`CodecContext::new`] for encoders that need the standard
/// codec + sws setup in one step.
pub struct CodecContext {
    pub codec_ctx: AVCodecContext,
    pub sws_ctx: Option<SwsContext>,
    pub pixel_format: i32,
    pub codec_name: String,
    pub supports_flush: bool,
}

impl CodecContext {
    /// Create a fully initialized codec context with SWScale.
    ///
    /// This performs the complete initialization sequence:
    /// 1. Find codec (with libx264 fallback)
    /// 2. Allocate and configure codec context
    /// 3. Set full color range (JPEG)
    /// 4. Open codec (with libx264 preset)
    /// 5. Create SWScale context for RGB24 → target format
    pub fn new(codec_name: &str, pix_fmt: i32, params: &CodecParams) -> Result<Self, String> {
        let (mut codec_ctx, actual_name, supports_flush) = find_and_create_context(codec_name)?;
        configure_codec_context(&mut codec_ctx, params, pix_fmt);
        set_full_color_range_ctx(&mut codec_ctx);
        open_codec(&mut codec_ctx, codec_name)?;

        let sws_ctx = create_sws_context(params.width as i32, params.height as i32, pix_fmt);

        Ok(Self {
            codec_ctx,
            sws_ctx,
            pixel_format: pix_fmt,
            codec_name: actual_name,
            supports_flush,
        })
    }

    /// Create a codec context without SWScale.
    ///
    /// Used by encoders that create SWScale lazily (e.g., `PersistentEncoder`).
    pub fn new_codec_only(
        codec_name: &str,
        pix_fmt: i32,
        params: &CodecParams,
    ) -> Result<Self, String> {
        let (mut codec_ctx, actual_name, supports_flush) = find_and_create_context(codec_name)?;
        configure_codec_context(&mut codec_ctx, params, pix_fmt);
        set_full_color_range_ctx(&mut codec_ctx);
        open_codec(&mut codec_ctx, codec_name)?;

        Ok(Self {
            codec_ctx,
            sws_ctx: None,
            pixel_format: pix_fmt,
            codec_name: actual_name,
            supports_flush,
        })
    }
}

// =============================================================================
// Codec Detection
// =============================================================================

/// Detect the best available codec for the platform.
///
/// Priority: NVENC (Linux) → VideoToolbox (macOS) → libx264 (fallback).
/// Returns `(codec_name, pixel_format_name)`.
pub fn detect_best_codec() -> (&'static str, &'static str) {
    #[cfg(target_os = "linux")]
    {
        if AVCodec::find_encoder_by_name(c"h264_nvenc").is_some() {
            tracing::info!("Detected NVENC encoder for hardware acceleration");
            return ("h264_nvenc", "nv12");
        }
    }

    #[cfg(target_os = "macos")]
    {
        if AVCodec::find_encoder_by_name(c"h264_videotoolbox").is_some() {
            tracing::info!("Detected VideoToolbox encoder for hardware acceleration");
            return ("h264_videotoolbox", "nv12");
        }
    }

    tracing::info!("Using libx264 CPU encoder");
    ("libx264", "yuv420p")
}

// =============================================================================
// Codec Lookup
// =============================================================================

/// Find a codec by name (with libx264 fallback) and create an `AVCodecContext`.
///
/// Returns `(codec_context, actual_codec_name, supports_flush)`.
///
/// This function keeps the `AVCodecRef` local so it doesn't escape
/// the borrow on the codec name CStr.
fn find_and_create_context(name: &str) -> Result<(AVCodecContext, String, bool), String> {
    let name_with_nul = format!("{}\0", name);
    let codec_name = CStr::from_bytes_with_nul(name_with_nul.as_bytes())
        .map_err(|_| "Invalid codec name".to_string())?;

    let codec = AVCodec::find_encoder_by_name(codec_name)
        .or_else(|| {
            tracing::warn!(codec = %name, "Codec not found, falling back to libx264");
            AVCodec::find_encoder(ffi::AV_CODEC_ID_H264)
        })
        .ok_or_else(|| "No H.264 encoder available".to_string())?;

    let actual_name = codec.name().to_str().unwrap_or("unknown").to_string();
    let codec_caps = unsafe { (*codec.as_ptr()).capabilities };
    let supports_flush = (codec_caps & ffi::AV_CODEC_CAP_ENCODER_FLUSH as i32) != 0;

    let ctx = AVCodecContext::new(&codec);
    Ok((ctx, actual_name, supports_flush))
}

// =============================================================================
// Pixel Format
// =============================================================================

/// Resolve a pixel format string to the FFmpeg constant.
pub fn resolve_pixel_format(format_str: &str) -> i32 {
    match format_str {
        "nv12" => ffi::AV_PIX_FMT_NV12,
        _ => ffi::AV_PIX_FMT_YUV420P,
    }
}

/// Determine pixel format from a codec name.
///
/// Hardware encoders (NVENC, VideoToolbox) prefer NV12; software encoders
/// use YUV420P.
pub fn pixel_format_for_codec(codec_name: &str) -> i32 {
    if codec_name.contains("nvenc") || codec_name.contains("videotoolbox") {
        ffi::AV_PIX_FMT_NV12
    } else {
        ffi::AV_PIX_FMT_YUV420P
    }
}

// =============================================================================
// Codec Context Configuration
// =============================================================================

/// Configure codec context with standard encoding parameters.
pub fn configure_codec_context(ctx: &mut AVCodecContext, params: &CodecParams, pix_fmt: i32) {
    ctx.set_width(params.width as i32);
    ctx.set_height(params.height as i32);
    ctx.set_bit_rate(params.bitrate);
    ctx.set_time_base(AVRational {
        num: 1,
        den: params.fps as i32,
    });
    ctx.set_framerate(AVRational {
        num: params.fps as i32,
        den: 1,
    });
    ctx.set_gop_size(params.gop_size as i32);
    ctx.set_max_b_frames(params.max_b_frames as i32);
    ctx.set_pix_fmt(pix_fmt);
}

/// Set full color range (JPEG) on a codec context.
///
/// RGB from decoded images uses full range [0, 255]. Without this,
/// FFmpeg defaults to limited range [16, 235] causing washed-out colors.
///
/// # Safety
///
/// Uses `as_mut_ptr()` for exclusive mutable access to a properly initialized
/// `AVCodecContext`. The field write is safe.
pub fn set_full_color_range_ctx(ctx: &mut AVCodecContext) {
    unsafe {
        (*ctx.as_mut_ptr()).color_range = ffi::AVCOL_RANGE_JPEG;
    }
}

/// Set full color range (JPEG) on a frame.
///
/// # Safety
///
/// Uses `as_mut_ptr()` for exclusive mutable access to a properly allocated
/// `AVFrame`. The field write is safe.
pub fn set_full_color_range_frame(frame: &mut AVFrame) {
    unsafe {
        (*frame.as_mut_ptr()).color_range = ffi::AVCOL_RANGE_JPEG;
    }
}

// =============================================================================
// Codec Open
// =============================================================================

/// Open codec with appropriate options.
///
/// libx264 requires a `preset` option to initialize properly; other codecs
/// are opened without extra options.
pub fn open_codec(ctx: &mut AVCodecContext, codec_name: &str) -> Result<(), String> {
    let codec_opts = if codec_name == "libx264" {
        Some(AVDictionary::new(c"preset", c"ultrafast", 0))
    } else {
        None
    };
    ctx.open(codec_opts)
        .map(|_| ())
        .map_err(|e| format!("Failed to open codec: {}", e))
}

// =============================================================================
// SWScale
// =============================================================================

/// Create an SWScale context for RGB24 → target pixel format conversion.
pub fn create_sws_context(width: i32, height: i32, dst_pix_fmt: i32) -> Option<SwsContext> {
    SwsContext::get_context(
        width,
        height,
        ffi::AV_PIX_FMT_RGB24,
        width,
        height,
        dst_pix_fmt,
        ffi::SWS_BILINEAR,
        None,
        None,
        None,
    )
}
