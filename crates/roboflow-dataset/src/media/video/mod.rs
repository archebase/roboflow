pub mod arena;
pub mod codec;
pub mod composer;
pub mod concurrent;
pub mod config;
pub mod convert;
pub mod decode;
pub mod encoder_pool;
pub mod fragment;
pub mod frame;
pub mod hardware;
pub mod hardware_config;
pub mod path;
pub mod pipeline;
pub mod reorder;
pub mod rsmpeg;
pub mod service;
pub mod simd;
pub mod streaming;
pub mod test_utils;

pub use arena::{FramePool, FramePoolConfig};
pub use composer::{RsmpegVideoComposer, VideoComposer};
pub use config::{DepthEncoderConfig, VideoEncoderConfig};
pub use convert::{ConvertPool, ConvertPoolConfig, TargetFormat};
pub use decode::{DecodePool, DecodePoolConfig};
pub use encoder_pool::{EncoderPool, EncoderPoolConfig};
pub use fragment::{FragmentEncoder, FragmentEncoderConfig, FragmentInfo};
pub use frame::{
    DepthFrame, DepthFrameBuffer, FrameBuffer, PixelFormat, VideoEncoderError, VideoFrame,
    VideoFrameBuffer,
};
#[cfg(target_os = "macos")]
pub use hardware::VideoToolboxEncoder;
pub use hardware::{
    DepthMkvEncoder, EncoderChoice, Mp4Encoder, NvencEncoder, available_encoders,
    check_nvenc_available, check_videotoolbox_available, is_encoder_available,
    print_encoder_diagnostics, select_best_encoder,
};
pub use hardware_config::{HardwareBackend, HardwareConfig, detect_hardware_backend};
pub use path::{FlatVideoPathScheme, LeRobotVideoPathScheme, RldsVideoPathScheme};
pub use rsmpeg::{
    EncodeFrame, RsmpegEncoder, RsmpegEncoderConfig, RsmpegMp4Encoder, default_codec_name,
    is_hardware_encoding_available, is_rsmpeg_available,
};
pub use service::{EncoderResult, VideoEncoderService, VideoServiceConfig};
pub use simd::{
    ConversionStrategy, optimal_strategy, rgb_batch_to_nv12, rgb_batch_to_yuv420p, rgb_to_nv12,
    rgb_to_nv12_in_place, rgb_to_yuv420p,
};
pub use streaming::{EncodedChunk, StreamingEncoderConfig, StreamingMp4Encoder};

pub use pipeline::{
    PipelineConfig, PipelineHandle, PipelineResult, VideoPipeline, VideoPipelineConfig,
};

// Re-export concurrent video encoder
pub use concurrent::{ConcurrentEncoderConfig, ConcurrentEncoderResult, ConcurrentVideoEncoder};

// Re-export VideoPathScheme from core traits
pub use crate::core::VideoPathScheme;

// Re-export ImageData from formats::common for convenience
pub use crate::formats::common::ImageData;
