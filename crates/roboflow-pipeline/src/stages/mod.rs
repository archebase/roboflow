// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Pipeline stages for async data processing.
//!
//! The chunk-based stages (Reader, Compression, Writer) have been removed.
//! Format conversion is now handled by RoboRewriter via HyperPipeline.
