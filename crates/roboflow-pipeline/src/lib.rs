// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

pub mod sources;
pub mod video;
pub mod formats;

// Re-export commonly used types
pub use sources::{
    Source, SourceConfig, SourceType, TimestampedMessage,
    create_source, McapSource, BagSource, BagSourceBatched, BagSourceBlocking,
};

pub use formats::{
    OutputConfig, OutputFormat,
};

pub use formats::lerobot::{
    create_lerobot_writer, LerobotWriterConfig, LerobotWriterResult,
};

pub use formats::common::{
    CameraInfo, DatasetFrame, ImageData,
};

pub use formats::{
    DatasetWriter, PipelineExecutor, PipelineConfig, PipelineStats,
};

pub use formats::lerobot::{
    LerobotWriter, LerobotConfig, DatasetConfig, StreamingConfig,
    VideoConfig, Mapping, MappingType,
};

pub use video::{
    FragmentEncoder, FragmentEncoderConfig, StreamingEncoderConfig,
    StreamingMp4Encoder, VideoEncoderConfig, VideoFrame, PixelFormat,
};
