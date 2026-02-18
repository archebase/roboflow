// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

pub mod formats;
pub mod sources;
pub mod video;

// Re-export commonly used types
pub use sources::{
    BagSource, BagSourceBatched, BagSourceBlocking, McapSource, Source, SourceConfig, SourceType,
    TimestampedMessage, create_source,
};

pub use formats::{OutputConfig, OutputFormat};

pub use formats::lerobot::{LerobotWriterConfig, LerobotWriterResult, create_lerobot_writer};

pub use formats::common::{CameraInfo, DatasetFrame, ImageData};

pub use formats::{DatasetWriter, PipelineConfig, PipelineExecutor, PipelineStats};

pub use formats::lerobot::{
    DatasetConfig, LerobotConfig, LerobotWriter, Mapping, MappingType, StreamingConfig, VideoConfig,
};

pub use video::{
    FragmentEncoder, FragmentEncoderConfig, PixelFormat, StreamingEncoderConfig,
    StreamingMp4Encoder, VideoEncoderConfig, VideoFrame,
};
