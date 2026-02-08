// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Compression utilities.

mod compress;

pub use compress::{
    ChunkToCompress, CompressedDataChunk, CompressionPool, compress_data, compress_with,
    create_zstd_compressor,
};
