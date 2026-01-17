//! Pipeline stages for the hyper-pipeline.
//!
//! Each stage runs in its own thread(s) and communicates via bounded channels.

pub mod batcher;
pub mod crc_packetizer;
pub mod parser_slicer;
pub mod prefetcher;

// io_uring-based prefetcher for Linux (optional)
#[cfg(all(target_os = "linux", feature = "io-uring-io"))]
pub mod io_uring_prefetcher;

pub use batcher::{BatcherStage, BatcherStageConfig};
pub use crc_packetizer::{CrcPacketizerConfig, CrcPacketizerStage};
pub use parser_slicer::{ParserSlicerConfig, ParserSlicerStage};
pub use prefetcher::{PrefetcherStage, PrefetcherStageConfig};

#[cfg(all(target_os = "linux", feature = "io-uring-io"))]
pub use io_uring_prefetcher::{IoUringPrefetcher, IoUringPrefetcherConfig};
