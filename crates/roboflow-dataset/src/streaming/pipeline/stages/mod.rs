// Individual pipeline stage implementations

pub mod aligner;
pub mod decoder;
pub mod parquet_writer;
pub mod transformer;
pub mod upload;
pub mod video_encoder;

pub use aligner::FrameAlignerStage;
pub use decoder::DecoderStage;
pub use parquet_writer::{ParquetWriterConfig, ParquetWriterStage};
pub use transformer::FeatureTransformerStage;
pub use upload::UploadCoordinatorStage;
pub use video_encoder::{VideoEncoderConfig, VideoEncoderStage};

use crossbeam_channel::{Receiver, Sender};

/// Helper to create channels for a stage.
pub fn create_stage_channels<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    crossbeam_channel::bounded(capacity)
}
