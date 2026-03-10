// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Test fixtures for roboflow-dataset testing.
//!
//! Provides reusable test data and utilities for generating test fixtures.

use roboflow_dataset::formats::common::{AlignedFrame, ImageData};
use roboflow_dataset::sources::TimestampedMessage;
use roboflow_dataset::testing::{FrameBuilder, MessageBuilder, generate_test_jpeg};
use std::collections::HashMap;
use roboflow_core::CodecValue;

// ============================================================================
// Message Fixtures
// ============================================================================

/// Create a minimal set of test messages (10 frames).
pub fn minimal_messages() -> Vec<TimestampedMessage> {
    (0..10)
        .map(|i| {
            MessageBuilder::new("/test")
                .with_timestamp(i as u64 * 33_333_333)
                .float_array(vec![i as f32])
                .build()
        })
        .collect()
}

/// Create a multi-topic message set for frame alignment testing.
pub fn multi_topic_messages(frame_count: usize, fps: f64) -> Vec<TimestampedMessage> {
    let ns_per_frame = (1_000_000_000.0 / fps) as u64;
    let mut messages = Vec::new();

    for i in 0..frame_count {
        let ts = i as u64 * ns_per_frame;

        // Camera
        messages.push(MessageBuilder::new("/camera/image")
            .with_timestamp(ts)
            .image(640, 480)
            .build());

        // State
        messages.push(MessageBuilder::new("/state")
            .with_timestamp(ts + 1_000_000) // Slight offset
            .float_array(vec![i as f32, (i + 1) as f32])
            .build());

        // Action
        messages.push(MessageBuilder::new("/action")
            .with_timestamp(ts)
            .float_array(vec![(i + 2) as f32])
            .build());
    }

    messages
}

/// Create messages with intentional timestamp gaps.
pub fn messages_with_gaps() -> Vec<TimestampedMessage> {
    let timestamps = vec![0, 1, 3, 5, 8, 13, 21]; // Fibonacci-like gaps
    let ns_per_frame = 33_333_333u64;

    timestamps.iter()
        .map(|&frame_idx| {
            MessageBuilder::new("/camera/image")
                .with_timestamp(frame_idx as u64 * ns_per_frame)
                .image(640, 480)
                .build()
        })
        .collect()
}

// ============================================================================
// Frame Fixtures
// ============================================================================

/// Create a minimal aligned frame.
pub fn minimal_frame(frame_index: usize) -> AlignedFrame {
    FrameBuilder::new(frame_index)
        .add_state("observation.state", vec![frame_index as f32])
        .build()
}

/// Create a complete aligned frame with all common features.
pub fn complete_frame(frame_index: usize, timestamp: u64) -> AlignedFrame {
    FrameBuilder::new(frame_index)
        .with_timestamp(timestamp)
        .add_state("observation.state", vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0])
        .add_action("action", vec![0.5, -0.5, 0.0])
        .add_encoded_image("observation.camera_0", 640, 480)
        .add_encoded_image("observation.camera_1", 640, 480)
        .add_timestamp("timestamp.original", timestamp)
        .build()
}

/// Create a batch of frames for testing.
pub fn frame_batch(count: usize) -> Vec<AlignedFrame> {
    (0..count)
        .map(|i| minimal_frame(i))
        .collect()
}

/// Create frames for an episode.
pub fn episode_frames(episode_index: usize, frame_count: usize) -> Vec<AlignedFrame> {
    let base_ts = episode_index as u64 * 1_000_000_000_000; // 1 second per episode

    (0..frame_count)
        .map(|i| {
            let ts = base_ts + (i as u64 * 33_333_333);
            complete_frame(i, ts)
        })
        .collect()
}

// ============================================================================
// Image Fixtures
// ============================================================================

/// Create a test RGB image.
pub fn test_rgb_image(width: u32, height: u32) -> ImageData {
    let size = (width * height * 3) as usize;
    let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
    ImageData::new(width, height, data)
}

/// Create a test encoded (JPEG) image.
pub fn test_encoded_image(width: u32, height: u32, pattern: u8) -> ImageData {
    let data = generate_test_jpeg(width, height, pattern);
    ImageData::encoded(width, height, data)
}

/// Create a test depth image.
pub fn test_depth_image(width: u32, height: u32) -> ImageData {
    let size = (width * height * 2) as usize; // 16-bit depth
    let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
    ImageData::depth(width, height, data)
}

// ============================================================================
// Resolution Fixtures
// ============================================================================

/// Common video resolutions for testing.
pub const COMMON_RESOLUTIONS: &[(u32, u32, &str)] = &[
    (320, 240, "QVGA"),
    (640, 480, "VGA"),
    (1280, 720, "HD"),
    (1920, 1080, "FHD"),
    (2560, 1440, "QHD"),
];

/// Create frames at various resolutions.
pub fn multi_resolution_frames() -> HashMap<String, AlignedFrame> {
    COMMON_RESOLUTIONS.iter()
        .map(|(w, h, name)| {
            let frame = FrameBuilder::new(0)
                .add_image(&format!("observation.{}", name), *w, *h)
                .build();
            (name.to_string(), frame)
        })
        .collect()
}

// ============================================================================
// Statistics Fixtures
// ============================================================================

use roboflow_dataset::core::stats::{EpisodeStats, WriterStats};
use std::time::Duration;

/// Create test episode statistics.
pub fn test_episode_stats(episode_index: usize, frames: usize) -> EpisodeStats {
    EpisodeStats {
        frames,
        images_encoded: frames,
        bytes_written: frames as u64 * 1024,
        duration: Duration::from_secs_f64(frames as f64 / 30.0),
        episode_index,
        task_index: None,
        video_files: vec![("camera_0".to_string(), format!("episode_{:06d}.mp4", episode_index))],
        parquet_path: Some(format!("episode_{:06d}.parquet", episode_index)),
    }
}

/// Create test writer statistics.
pub fn test_writer_stats(episode_count: usize, frames_per_episode: usize) -> WriterStats {
    let mut stats = WriterStats::new();

    for ep in 0..episode_count {
        let ep_stats = test_episode_stats(ep, frames_per_episode);
        stats.add_episode(ep_stats);
    }

    stats.duration = Duration::from_secs_f64((episode_count * frames_per_episode) as f64 / 30.0);
    stats
}

// ============================================================================
// Config Fixtures
// ============================================================================

use roboflow_dataset::formats::alignment::config::StreamingConfig;
use roboflow_dataset::formats::alignment::completion::FrameCompletionCriteria;

/// Create a test streaming configuration.
pub fn test_streaming_config() -> StreamingConfig {
    StreamingConfig::with_fps(30.0)
        .with_completion_window(std::time::Duration::from_millis(100))
        .require_feature("/camera/image")
        .require_feature("/state")
}

/// Create a test completion criteria.
pub fn test_completion_criteria() -> FrameCompletionCriteria {
    FrameCompletionCriteria::new()
        .require_feature("/camera/image")
        .require_feature("/state")
        .with_min_completeness(1.0)
}

// ============================================================================
// Unit Tests for Fixtures
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_minimal_messages_count() {
        let messages = minimal_messages();
        assert_eq!(messages.len(), 10);
    }

    #[test]
    fn test_multi_topic_messages_structure() {
        let messages = multi_topic_messages(10, 30.0);
        assert_eq!(messages.len(), 30); // 10 frames * 3 topics
    }

    #[test]
    fn test_messages_with_gaps_has_gaps() {
        let messages = messages_with_gaps();
        assert!(messages.len() > 1);

        // Verify timestamps are not consecutive
        let ts0 = messages[0].log_time;
        let ts1 = messages[1].log_time;
        assert_ne!(ts1 - ts0, 33_333_333); // Not consecutive
    }

    #[test]
    fn test_complete_frame_has_all_features() {
        let frame = complete_frame(0, 0);

        assert!(frame.states.contains_key("observation.state"));
        assert!(frame.actions.contains_key("action"));
        assert!(frame.image_refs.contains_key("observation.camera_0"));
        assert!(frame.image_refs.contains_key("observation.camera_1"));
        assert!(frame.timestamps.contains_key("timestamp.original"));
    }

    #[test]
    fn test_episode_frames_correct_count() {
        let frames = episode_frames(0, 100);
        assert_eq!(frames.len(), 100);
    }

    #[test]
    fn test_test_episode_stats() {
        let stats = test_episode_stats(5, 100);

        assert_eq!(stats.episode_index, 5);
        assert_eq!(stats.frames, 100);
        assert!(stats.fps() > 0.0);
    }

    #[test]
    fn test_test_writer_stats() {
        let stats = test_writer_stats(5, 100);

        assert_eq!(stats.episodes_written, 5);
        assert_eq!(stats.frames_written, 500);
    }
}
