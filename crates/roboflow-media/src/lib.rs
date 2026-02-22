// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Image and video encoding/decoding for robotics datasets.
//!
//! This crate provides media processing capabilities for compressed image
//! formats (JPEG, PNG) and video encoding (H.264, H.265) with hardware
//! acceleration support.

pub mod frame;
pub mod image;
pub mod video;

// Re-export core frame types for convenience
pub use frame::{AudioData, CameraInfo, ImageData, ImageDataError};
