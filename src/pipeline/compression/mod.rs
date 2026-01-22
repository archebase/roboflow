// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Parallel compression utilities.

mod compress;
mod parallel;

pub use compress::{ChunkToCompress, CompressedDataChunk, CompressionPool};
pub use parallel::ParallelCompressor;
