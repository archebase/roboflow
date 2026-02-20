// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Re-exports of frame types from formats/common.
//!
//! This module provides backward-compatible re-exports of the core frame types
//! that are used across all dataset formats. The actual implementations are
//! in [`crate::formats::common`].

// Re-export frame types from formats/common
pub use crate::formats::common::{AlignedFrame, AudioData, CameraInfo, DatasetFrame, ImageData};

// Re-export error types that exist
pub use crate::formats::common::DatasetWriterError;

// UploadState is in formats::common::base, re-export from there
pub use crate::formats::common::base::UploadState;
