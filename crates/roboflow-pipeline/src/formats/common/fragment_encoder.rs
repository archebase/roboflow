// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Fragment-based video encoding for streaming uploads.
//!
//! This module re-exports types from the `roboflow_video` crate.
//! The canonical implementations are in `roboflow-video/src/fragment.rs`.

// Re-export all fragment types from roboflow-video crate (canonical location)
pub use crate::video::{FragmentEncoder, FragmentEncoderConfig, FragmentInfo};
