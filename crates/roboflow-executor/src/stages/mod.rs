// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Stage implementations for the executor framework.

pub mod convert;
pub mod discover;
pub mod merge;
pub mod transform;

pub use convert::ConvertStage;
pub use discover::DiscoverStage;
pub use merge::MergeStage;
pub use transform::TransformStage;
