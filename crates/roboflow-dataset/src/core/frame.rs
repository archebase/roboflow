// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Re-exports of frame types from formats/common and roboflow_core.
//!
//! This module provides re-exports of the core frame types that are used
//! across all dataset formats.
//!
//! # Design Note
//!
//! The actual implementations are in [`crate::formats::common`] for dataset-specific
//! types (AlignedFrame, DatasetFrame) and in [`roboflow_core`] for the fundamental
//! frame types (ImageData, AudioData, CameraInfo). This is a deliberate design
//! choice to avoid duplicating the substantial frame-related code.

// Re-export frame types from formats/common
pub use crate::formats::common::{AlignedFrame, DatasetFrame};

// Re-export fundamental frame types from roboflow_media
pub use roboflow_media::{AudioData, CameraInfo, ImageData};

// Re-export error types that exist
pub use crate::formats::common::DatasetWriterError;
