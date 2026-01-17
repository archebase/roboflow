//! Parallel compression utilities.

mod compress;
mod parallel;

pub use compress::{ChunkToCompress, CompressedDataChunk, CompressionPool};
pub use parallel::ParallelCompressor;
