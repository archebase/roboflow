// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Property-based tests as defined in ADR-004.
//!
//! Uses proptest to verify invariants across a wide range of inputs.

use proptest::prelude::*;
use roboflow_dataset::testing::{FrameBuilder, InMemoryWriter, MessageBuilder, MockSource};
use roboflow_dataset::core::traits::FormatWriter;
use roboflow_dataset::formats::common::ImageData;
use roboflow_dataset::core::stats::{EpisodeStats, WriterStats};
use std::collections::HashMap;

// ============================================================================
// Custom Strategies
// ============================================================================

/// Strategy for generating valid frame indices
fn frame_index_strategy() -> impl Strategy<Value = usize> {
    0..1000usize
}

/// Strategy for generating timestamps (nanoseconds)
fn timestamp_strategy() -> impl Strategy<Value = u64> {
    0..1_000_000_000_000u64 // Up to ~16 minutes in nanoseconds
}

/// Strategy for generating state vectors
fn state_vector_strategy() -> impl Strategy<Value = Vec<f32>> {
    proptest::collection::vec(any::<f32>(), 1..20)
}

/// Strategy for generating image dimensions
fn image_dim_strategy() -> impl Strategy<Value = (u32, u32)> {
    (16..2048u32, 16..2048u32).prop_map(|(w, h)| {
        // Round to common resolutions
        let w = (w / 16) * 16;
        let h = (h / 16) * 16;
        (w.max(16), h.max(16))
    })
}

// ============================================================================
// Frame Tests
// ============================================================================

proptest! {
    #[test]
    fn test_frame_builder_preserves_frame_index(
        frame_index in frame_index_strategy()
    ) {
        let frame = FrameBuilder::new(frame_index).build();
        prop_assert_eq!(frame.frame_index, frame_index);
    }

    #[test]
    fn test_frame_builder_preserves_timestamp(
        timestamp in timestamp_strategy()
    ) {
        let frame = FrameBuilder::new(0)
            .with_timestamp(timestamp)
            .build();
        prop_assert_eq!(frame.timestamp, timestamp);
    }

    #[test]
    fn test_frame_builder_preserves_state(
        state in state_vector_strategy()
    ) {
        let frame = FrameBuilder::new(0)
            .add_state("observation.state", state.clone())
            .build();

        prop_assert!(frame.states.contains_key("observation.state"));
        prop_assert_eq!(frame.states.get("observation.state"), Some(&state));
    }

    #[test]
    fn test_frame_multiple_states(
        states in proptest::collection::vec(state_vector_strategy(), 1..5)
    ) {
        let mut builder = FrameBuilder::new(0);
        for (i, state) in states.iter().enumerate() {
            builder = builder.add_state(&format!("state_{}", i), state.clone());
        }
        let frame = builder.build();

        prop_assert_eq!(frame.states.len(), states.len());
    }
}

// ============================================================================
// Image Data Tests
// ============================================================================

proptest! {
    #[test]
    fn test_image_data_size_calculation(
        (width, height) in image_dim_strategy()
    ) {
        let size = (width * height * 3) as usize;
        let data = vec![0u8; size];
        let image = ImageData::new(width, height, data);

        prop_assert_eq!(image.width, width);
        prop_assert_eq!(image.height, height);
        prop_assert_eq!(image.pixel_count(), (width * height) as usize);
        prop_assert_eq!(image.rgb_size(), size);
    }

    #[test]
    fn test_image_data_validate(
        width in 16..1024u32,
        height in 16..1024u32
    ) {
        let correct_size = (width * height * 3) as usize;
        let valid_image = ImageData::new(width, height, vec![0u8; correct_size]);
        prop_assert!(valid_image.validate().is_ok());

        let invalid_image = ImageData::new(width, height, vec![0u8; 10]);
        prop_assert!(invalid_image.validate().is_err());
    }
}

// ============================================================================
// Writer Tests
// ============================================================================

proptest! {
    #[test]
    fn test_in_memory_writer_frame_count(
        frame_count in 1..100usize
    ) {
        let mut writer = InMemoryWriter::new();

        for i in 0..frame_count {
            writer.write_frame(&FrameBuilder::new(i).build()).unwrap();
        }

        prop_assert_eq!(writer.len(), frame_count);
        prop_assert_eq!(writer.frame_count(), frame_count);
    }

    #[test]
    fn test_in_memory_writer_episode_independence(
        episode_count in 1..10usize,
        frames_per_episode in 1..20usize
    ) {
        let mut writer = InMemoryWriter::new();

        for ep in 0..episode_count {
            writer.start_episode(Some(ep)).unwrap();
            for i in 0..frames_per_episode {
                writer.write_frame(&FrameBuilder::new(i).build()).unwrap();
            }
            writer.finish_episode().unwrap();
        }
        writer.finalize().unwrap();

        let expected_total = episode_count * frames_per_episode;
        prop_assert_eq!(writer.len(), expected_total);

        // Each episode should have the correct number of frames
        for ep in 0..episode_count {
            prop_assert_eq!(
                writer.episode_frames(ep).map(|f| f.len()),
                Some(frames_per_episode)
            );
        }
    }
}

// ============================================================================
// Stats Tests
// ============================================================================

proptest! {
    #[test]
    fn test_writer_stats_merge_associativity(
        frames1 in 0..100usize,
        frames2 in 0..100usize,
        frames3 in 0..100usize
    ) {
        let mut stats1 = WriterStats { frames_written: frames1, ..Default::default() };
        let mut stats2 = WriterStats { frames_written: frames2, ..Default::default() };
        let stats3 = WriterStats { frames_written: frames3, ..Default::default() };

        // (stats1 + stats2) + stats3
        stats1.merge(&stats2);
        stats1.merge(&stats3);
        let result1 = stats1.frames_written;

        // Reset
        let mut stats1 = WriterStats { frames_written: frames1, ..Default::default() };
        let mut stats2 = WriterStats { frames_written: frames2, ..Default::default() };

        // stats1 + (stats2 + stats3)
        stats2.merge(&stats3);
        stats1.merge(&stats2);
        let result2 = stats1.frames_written;

        prop_assert_eq!(result1, result2);
        prop_assert_eq!(result1, frames1 + frames2 + frames3);
    }

    #[test]
    fn test_writer_stats_merge_identity(
        frames in 0..100usize
    ) {
        let mut stats = WriterStats { frames_written: frames, ..Default::default() };
        let identity = WriterStats::new();

        stats.merge(&identity);

        prop_assert_eq!(stats.frames_written, frames);
    }
}

// ============================================================================
// Message Tests
// ============================================================================

proptest! {
    #[test]
    fn test_message_builder_preserves_topic(
        topic in "camera|state|action|joint_[0-9]+"
    ) {
        let msg = MessageBuilder::new(&topic).build();
        prop_assert_eq!(msg.topic, topic);
    }

    #[test]
    fn test_message_builder_preserves_timestamp(
        ts in timestamp_strategy()
    ) {
        let msg = MessageBuilder::new("/test")
            .with_timestamp(ts)
            .build();
        prop_assert_eq!(msg.log_time, ts);
    }

    #[test]
    fn test_message_timestamps_are_ordered(
        timestamps in proptest::collection::vec(timestamp_strategy(), 2..100)
    ) {
        let mut messages: Vec<_> = timestamps.iter()
            .map(|&ts| MessageBuilder::new("/test").with_timestamp(ts).build())
            .collect();

        messages.sort_by_key(|m| m.log_time);

        // Verify ordering
        for i in 1..messages.len() {
            prop_assert!(messages[i].log_time >= messages[i-1].log_time);
        }
    }
}

// ============================================================================
// MockSource Tests
// ============================================================================

proptest! {
    #[test]
    fn test_mock_source_message_count(
        count in 0..100usize
    ) {
        let source = MockSource::with_count(count);

        // Run async test
        let rt = tokio::runtime::Runtime::new().unwrap();
        let actual_count = rt.block_on(async {
            let mut s = source;
            let mut c = 0;
            while let Ok(Some(batch)) = s.read_batch(10).await {
                c += batch.len();
            }
            c
        });

        prop_assert_eq!(actual_count, count);
    }
}

// ============================================================================
// Invariant Tests
// ============================================================================

proptest! {
    #[test]
    fn test_frame_builder_always_produces_valid_frame(
        frame_index in frame_index_strategy(),
        timestamp in timestamp_strategy(),
        states in proptest::collection::hash_map(
            "[a-z_]+",
            state_vector_strategy(),
            0..5
        ),
        actions in proptest::collection::hash_map(
            "[a-z_]+",
            state_vector_strategy(),
            0..3
        )
    ) {
        let mut builder = FrameBuilder::new(frame_index)
            .with_timestamp(timestamp);

        for (name, values) in &states {
            builder = builder.add_state(name, values.clone());
        }
        for (name, values) in &actions {
            builder = builder.add_action(name, values.clone());
        }

        let frame = builder.build();

        prop_assert_eq!(frame.frame_index, frame_index);
        prop_assert_eq!(frame.timestamp, timestamp);
        prop_assert_eq!(frame.states.len(), states.len());
        prop_assert_eq!(frame.actions.len(), actions.len());
    }
}
