// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Streaming bridge using robocodec's FrameStream for fast frame alignment.

use crate::formats::common::{AlignedFrame, ImageData};
use robocodec::io::streaming::{
    AlignedFrame as CodecFrame, FrameAlignmentConfig, StreamConfig, StreamingRoboReader,
};
use roboflow_core::CodecError;

/// Configuration for streaming bridge
pub struct StreamingBridgeConfig {
    pub fps: u32,
    pub image_topics: Vec<String>,
    pub state_topics: Vec<String>,
    pub max_state_latency_ms: u64,
}

impl StreamingBridgeConfig {
    pub fn new(fps: u32) -> Self {
        Self {
            fps,
            image_topics: Vec::new(),
            state_topics: Vec::new(),
            max_state_latency_ms: 50, // 50ms default
        }
    }

    pub fn with_image_topic(mut self, topic: impl Into<String>) -> Self {
        self.image_topics.push(topic.into());
        self
    }

    pub fn with_state_topic(mut self, topic: impl Into<String>) -> Self {
        self.state_topics.push(topic.into());
        self
    }

    pub fn with_max_latency(mut self, latency_ms: u64) -> Self {
        self.max_state_latency_ms = latency_ms;
        self
    }
}

/// Process a file using robocodec's streaming FrameStream
pub fn process_file_with_streaming<F>(
    path: &str,
    config: StreamingBridgeConfig,
    mut frame_callback: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnMut(AlignedFrame) -> Result<(), CodecError>,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| CodecError::Other(format!("Failed to create Tokio runtime: {e}")))?;

    rt.block_on(async {
        let stream_config = StreamConfig::new();
        let reader = StreamingRoboReader::open(path, stream_config)
            .await
            .map_err(|e| CodecError::Other(format!("Failed to open file: {e}")))?;

        let frame_config = FrameAlignmentConfig::new(config.fps)
            .with_max_latency(config.max_state_latency_ms * 1_000_000); // Convert to ns

        // Add image topics
        let frame_config = config.image_topics.iter().fold(frame_config, |cfg, topic| {
            cfg.with_image_topic(topic.clone())
        });

        // Add state topics
        let frame_config = config.state_topics.iter().fold(frame_config, |cfg, topic| {
            cfg.with_state_topic(topic.clone())
        });

        reader
            .process_frames(frame_config, |codec_frame: CodecFrame| {
                // Convert robocodec frame to roboflow frame
                let roboflow_frame = convert_frame(&codec_frame)
                    .map_err(|e| CodecError::Other(format!("Frame conversion: {e}")))?;
                frame_callback(roboflow_frame)
            })
            .map_err(|e| CodecError::Other(format!("Frame processing error: {e}")))?;

        Ok(())
    })
}

/// Convert robocodec's AlignedFrame to roboflow's AlignedFrame
fn convert_frame(codec_frame: &CodecFrame) -> Result<AlignedFrame, CodecError> {
    let mut frame = AlignedFrame::new(codec_frame.frame_index, codec_frame.timestamp);

    // Convert images
    for (name, image_data) in &codec_frame.images {
        let roboflow_image =
            ImageData::encoded(image_data.width, image_data.height, image_data.data.clone());
        frame.add_image(name.clone(), roboflow_image);
    }

    // Convert states
    for (name, state_data) in &codec_frame.states {
        frame.add_state(name.clone(), state_data.clone());
    }

    Ok(frame)
}
