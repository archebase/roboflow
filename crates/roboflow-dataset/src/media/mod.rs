// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Media handling modules for video and image processing.
//!
//! This module provides abstractions for video encoding and image decoding
//! that are format-agnostic and can be used across different dataset formats.

pub mod image;
pub mod video;

// Re-export commonly used types
pub use image::{
    DecodedImage, ImageDecoderBackend, ImageDecoderConfig, ImageDecoderFactory, ImageError,
    ImageFormat, decode_compressed_image,
};
pub use video::{
    FlatVideoPathScheme, LeRobotVideoPathScheme, RldsVideoPathScheme, VideoPathScheme,
};
