// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Media handling modules for video and image processing.
//!
//! This module provides abstractions for video encoding and image decoding
//! that are format-agnostic and can be used across different dataset formats.
//!
//! For video path schemes (LeRobot, RLDS, etc.), see `formats::common` module.

pub mod image;
pub mod video;

// Re-export commonly used types from image module
pub use image::{
    DecodedImage, ImageDecoderBackend, ImageDecoderConfig, ImageDecoderFactory, ImageError,
    ImageFormat, decode_compressed_image,
};

// Re-export VideoPathScheme trait from core (for backward compatibility)
// For path scheme implementations, use formats::common::{LeRobotVideoPathScheme, ...}
pub use crate::core::VideoPathScheme;
