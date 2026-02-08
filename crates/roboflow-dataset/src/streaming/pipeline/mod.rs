// Streaming dataset pipeline module

//! High-performance 7-stage pipeline for dataset conversion.
//!
//! # Architecture
//!
//! The pipeline consists of 7 stages connected by lock-free channels:
//!
//! 1. **Prefetcher** - Platform-optimized I/O for input file
//! 2. **ParallelDecoder** - Multi-threaded message decoding
//! 3. **FrameAligner** - Frame alignment by timestamp
//! 4. **FeatureTransformer** - Topic → feature mapping
//! 5. **VideoEncoder** - Parallel MP4 encoding
//! 6. **ParquetWriter** - Streaming Parquet writes
//! 7. **UploadCoordinator** - Incremental cloud uploads
//!
//! # Example
//!
//! ```no_run
//! use roboflow_dataset::streaming::pipeline::{StreamingDatasetPipeline, PipelineBuilder};
//! use roboflow_dataset::lerobot::config::LerobotConfig;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let lerobot_config = LerobotConfig::default();
//!
//! let pipeline = PipelineBuilder::new()
//!     .input_path("input.bag")
//!     .lerobot_config(lerobot_config)
//!     .high_throughput()
//!     .build()?;
//!
//! let report = pipeline.run()?;
//! println!("Processed {} frames at {:.1} fps",
//!     report.frames_written,
//!     report.throughput_fps
//! );
//! # Ok(())
//! # }
//! ```

mod config;
mod orchestrator;
mod stage;
pub mod stages;
mod types;

pub use config::{
    AlignerConfig, DecoderConfig, PipelineConfig, TransformerConfig, UploadConfig,
    VideoEncoderConfig, VideoEncoderPreset,
};
pub use orchestrator::{PipelineBuilder, StreamingDatasetPipeline};
pub use stage::ChannelConfig;
pub use types::{
    CodecValue, DatasetFrame, DecodedMessage, EncodedVideo, ParquetRow, PipelineError,
    PipelineReport, PipelineResult, StageStats, TransformableFrame,
};

/// Re-export common types for convenience
pub use crate::common::{AlignedFrame, ImageData};
