// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Hardware capability detection for pipeline optimization.
//!
//! This module provides runtime detection of available hardware acceleration
//! features (CUDA, NVENC, VideoToolbox, QSV, VAAPI) to enable optimal
//! processing strategies.

mod detection;
mod strategy;

pub use detection::{detect_hardware, HardwareCapabilities};
pub use strategy::{PipelineStrategy, StrategySelection};
