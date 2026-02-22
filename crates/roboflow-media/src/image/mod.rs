// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Image decoding support for compressed image formats.
//!
//! This module provides decoding capabilities for compressed image formats
//! commonly found in robotics datasets (JPEG, PNG) with GPU acceleration support.
//!
//! # Architecture
//!
//! The decoding system is organized into several components:
//!
//! - **[`format`]: Format detection from magic bytes and ROS strings
//! - **[`backend`]: `ImageDecoderBackend` trait with CPU/GPU/Apple implementations
//! - **[`config`]: Decoder configuration with builder pattern
//! - **[`factory`]: Auto-detection and fallback management
//! - **[`memory`]: GPU-friendly memory allocation strategies
//! - **[`gpu`]: NVIDIA nvJPEG decoder (Linux only)
//! - **[`apple`]: Apple hardware-accelerated decoder (macOS only)
//!
//! Image decoding (CPU + GPU/Apple when available) is always enabled for LeRobot and streaming conversion.
//!
//! # Usage
//!
//! ```rust,ignore
//! use crate::formats::image::{ImageDecoderFactory, ImageDecoderConfig};
//!
//! let config = ImageDecoderConfig::max_throughput();
//! let mut factory = ImageDecoderFactory::new(&config);
//!
//! let decoder = factory.get_decoder();
//! let rgb_image = decoder.decode(&jpeg_bytes, ImageFormat::Jpeg)?;
//! ```
//!
//! # Integration with Streaming Converter
//!
//! The image decoder integrates with the streaming converter via
//! `FrameAlignmentBuffer`. When `CompressedImage` messages are received,
//! they are automatically decoded to RGB before being passed to the writer.
//!
//! ```rust,ignore
//! let config = StreamingConfig::with_fps(30)
//!     .with_decoder_config(ImageDecoderConfig::max_throughput());
//!
//! let converter = StreamingDatasetConverter::new_lerobot(output_dir, lerobot_config)
//!     .with_decoder_config(decoder_config)?;
//! ```

pub mod apple;
pub mod backend;
pub mod config;
pub mod decode;
pub mod factory;
pub mod format;
pub mod gpu;
pub mod memory;
pub mod parallel;

// Re-export commonly used types
pub use backend::{DecodedImage, DecoderType, ImageDecoderBackend};
pub use config::{DecoderBackendType as ImageDecoderBackendType, ImageDecoderConfig};
pub use decode::{decode_image_to_rgb, decode_to_rgb};
pub use factory::{DecodeStats, GpuDeviceInfo, ImageDecoderFactory};
pub use format::{ImageFormat, can_passthrough, detect_image_format, detect_jpeg, detect_png};
pub use memory::{AlignedImageBuffer, MemoryStrategy};
pub use parallel::{ParallelDecodeStats, decode_images_parallel, decode_images_parallel_with_dims};

/// Image decoding errors.
#[derive(Debug, thiserror::Error)]
pub enum ImageError {
    /// The image format is not supported.
    #[error("Unsupported image format: {0}")]
    UnsupportedFormat(String),

    /// Decoding the image failed.
    #[error("Image decoding failed: {0}")]
    DecodeFailed(String),

    /// Image decoding feature is not enabled in this build.
    #[error("Image decoding not enabled")]
    NotEnabled,

    /// The image data is invalid or corrupted.
    #[error("Invalid image data: {0}")]
    InvalidData(String),

    /// GPU decoder is not available.
    #[error("GPU decoder unavailable: {0}")]
    GpuUnavailable(String),
}

/// Result type for image operations.
pub type Result<T> = std::result::Result<T, ImageError>;

/// High-level function to decode a compressed image.
///
/// This is a convenience function that uses the default configuration.
/// For more control, create an `ImageDecoderFactory` with a custom config.
///
/// # Arguments
///
/// * `data` - Compressed image bytes (JPEG/PNG)
/// * `format` - Image format hint
///
/// # Returns
///
/// Decoded RGB image with dimensions
///
/// # Example
///
/// ```rust,ignore
/// use crate::formats::image::decode_compressed_image;
///
/// let jpeg_data = std::fs::read("image.jpg")?;
/// let rgb_image = decode_compressed_image(&jpeg_data, ImageFormat::Jpeg)?;
/// ```
/// Process-wide shared decoder for decode_compressed_image so we don't create (and log) per frame.
fn shared_decoder() -> &'static dyn ImageDecoderBackend {
    use std::sync::OnceLock;
    static DECODER: OnceLock<Box<dyn ImageDecoderBackend>> = OnceLock::new();
    DECODER
        .get_or_init(|| {
            tracing::debug!("shared_decoder: initializing process-wide decoder");
            let config = ImageDecoderConfig::new();
            let mut factory = ImageDecoderFactory::new(&config);
            let decoder = factory.create_decoder().unwrap_or_else(|e| {
                tracing::warn!(
                    "shared_decoder: failed to create decoder: {}, using CPU fallback",
                    e
                );
                Box::new(backend::CpuImageDecoder::new(
                    memory::MemoryStrategy::Heap,
                    1,
                ))
            });
            tracing::debug!(
                "shared_decoder: decoder created successfully, type={:?}",
                decoder.decoder_type()
            );
            decoder
        })
        .as_ref()
}

/// Decode a compressed image using the shared decoder.
///
/// This is a convenience function that reuses a process-wide decoder
/// to avoid per-frame initialization overhead.
pub fn decode_compressed_image(data: &[u8], format: ImageFormat) -> Result<DecodedImage> {
    shared_decoder().decode(data, format)
}
