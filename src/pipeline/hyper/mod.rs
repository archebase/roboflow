//! 7-Stage Hyper-Pipeline for maximum throughput.
//!
//! This module implements a high-performance pipeline with 7 isolated stages:
//!
//! 1. **Prefetcher** - Platform-specific I/O (madvise/io_uring)
//! 2. **Parser/Slicer** - Parse message boundaries, arena allocation
//! 3. **Batcher/Router** - Batch messages, assign sequence IDs
//! 4. **Transform** - Pass-through (metadata transforms only)
//! 5. **Compressor** - Parallel ZSTD compression
//! 6. **CRC/Packetizer** - CRC32 checksum, MCAP framing
//! 7. **Writer** - Sequential output with ordering
//!
//! # Design Goals
//!
//! - **2000+ MB/s throughput** on modern hardware
//! - **Zero-copy** message handling via arena allocation
//! - **Lock-free** inter-stage communication
//! - **Platform-optimized** I/O (madvise on macOS, io_uring on Linux)
//!
//! # Usage
//!
//! ```no_run
//! use robocodec::pipeline::hyper::{HyperPipeline, HyperPipelineConfig};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let config = HyperPipelineConfig::new("input.bag", "output.mcap");
//! let pipeline = HyperPipeline::new(config)?;
//! let report = pipeline.run()?;
//! println!("Throughput: {:.2} MB/s", report.throughput_mb_s);
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod orchestrator;
pub mod stages;
pub mod types;
pub mod utils;

pub use config::{HyperPipelineBuilder, HyperPipelineConfig};
pub use orchestrator::{HyperPipeline, HyperPipelineReport};
pub use types::*;
