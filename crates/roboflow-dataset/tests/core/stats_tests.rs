// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Core layer tests as defined in ADR-004.
//!
//! Tests cover:
//! - FormatWriter trait implementation
//! - Episode stats aggregation
//! - Writer stats merging

use roboflow_dataset::testing::{InMemoryWriter, FrameBuilder, MockStorage, StorageOperation};
use roboflow_dataset::core::traits::FormatWriter;
use roboflow_dataset::core::stats::{EpisodeStats, WriterStats, ProgressStats};
use std::time::Duration;

// ============================================================================
// InMemoryWriter Tests
// ============================================================================

#[test]
fn test_in_memory_writer_trait_object_safety() {
    // Verify FormatWriter trait is dyn-compatible
    let mut writers: Vec<Box<dyn FormatWriter>> = vec![
        Box::new(InMemoryWriter::new()),
    ];

    for writer in &mut writers {
        writer.write_frame(&FrameBuilder::new(0).add_state("pos", vec![0.0]).build()).unwrap();
        let stats = writer.finalize().unwrap();
        assert!(stats.frames_written > 0);
    }
}

#[test]
fn test_in_memory_writer_episodes() {
    let mut writer = InMemoryWriter::new();

    // Episode 0
    writer.start_episode(None).unwrap();
    writer.write_frame(&FrameBuilder::new(0).add_state("pos", vec![0.0]).build()).unwrap();
    writer.write_frame(&FrameBuilder::new(1).add_state("pos", vec![1.0]).build()).unwrap();
    writer.finish_episode().unwrap();

    // Episode 1
    writer.start_episode(None).unwrap();
    writer.write_frame(&FrameBuilder::new(0).add_state("pos", vec![2.0]).build()).unwrap();
    writer.finish_episode().unwrap();

    writer.finalize().unwrap();

    assert_eq!(writer.len(), 3);
    assert_eq!(writer.episode_frames(0).unwrap().len(), 2);
    assert_eq!(writer.episode_frames(1).unwrap().len(), 1);
}

#[test]
fn test_in_memory_writer_format_info() {
    let writer = InMemoryWriter::new();

    assert_eq!(writer.format_name(), "InMemory");
    assert_eq!(writer.format_version(), "test-1.0");
    assert!(writer.supports_episodes());
}

#[test]
fn test_in_memory_writer_downcast() {
    let mut writer: Box<dyn FormatWriter> = Box::new(InMemoryWriter::new());

    writer.write_frame(&FrameBuilder::new(0).build()).unwrap();

    // Downcast to concrete type
    let concrete = writer.as_any().downcast_ref::<InMemoryWriter>();
    assert!(concrete.is_some());
    assert_eq!(concrete.unwrap().len(), 1);
}

// ============================================================================
// EpisodeStats Tests
// ============================================================================

#[test]
fn test_episode_stats_new() {
    let stats = EpisodeStats::new();
    assert_eq!(stats.frames, 0);
    assert_eq!(stats.images_encoded, 0);
    assert_eq!(stats.bytes_written, 0);
}

#[test]
fn test_episode_stats_for_episode() {
    let stats = EpisodeStats::for_episode(5);
    assert_eq!(stats.episode_index, 5);
}

#[test]
fn test_episode_stats_fps() {
    let stats = EpisodeStats {
        frames: 300,
        duration: Duration::from_secs(10),
        ..Default::default()
    };

    assert!((stats.fps() - 30.0).abs() < 0.1);
}

#[test]
fn test_episode_stats_fps_zero_duration() {
    let stats = EpisodeStats {
        frames: 100,
        duration: Duration::ZERO,
        ..Default::default()
    };

    assert_eq!(stats.fps(), 0.0);
}

#[test]
fn test_episode_stats_mb_per_sec() {
    let stats = EpisodeStats {
        bytes_written: 10_485_760, // 10 MB
        duration: Duration::from_secs(2),
        ..Default::default()
    };

    assert!((stats.mb_per_sec() - 5.0).abs() < 0.1);
}

// ============================================================================
// WriterStats Tests
// ============================================================================

#[test]
fn test_writer_stats_new() {
    let stats = WriterStats::new();
    assert_eq!(stats.frames_written, 0);
    assert_eq!(stats.images_encoded, 0);
    assert_eq!(stats.output_bytes, 0);
}

#[test]
fn test_writer_stats_merge() {
    let mut stats1 = WriterStats {
        frames_written: 100,
        images_encoded: 50,
        state_records: 100,
        output_bytes: 1024,
        duration: Duration::from_secs(10),
        episodes_written: 1,
        episode_stats: vec![EpisodeStats::for_episode(0)],
    };

    let stats2 = WriterStats {
        frames_written: 200,
        images_encoded: 100,
        state_records: 200,
        output_bytes: 2048,
        duration: Duration::from_secs(15),
        episodes_written: 1,
        episode_stats: vec![EpisodeStats::for_episode(1)],
    };

    stats1.merge(&stats2);

    assert_eq!(stats1.frames_written, 300);
    assert_eq!(stats1.images_encoded, 150);
    assert_eq!(stats1.state_records, 300);
    assert_eq!(stats1.output_bytes, 3072);
    assert_eq!(stats1.episodes_written, 2);
    assert_eq!(stats1.episode_stats.len(), 2);
    // Duration should be max of the two
    assert_eq!(stats1.duration, Duration::from_secs(15));
}

#[test]
fn test_writer_stats_add_episode() {
    let mut stats = WriterStats::new();
    let episode = EpisodeStats {
        frames: 100,
        images_encoded: 50,
        bytes_written: 1024,
        episode_index: 0,
        ..Default::default()
    };

    stats.add_episode(episode);

    assert_eq!(stats.frames_written, 100);
    assert_eq!(stats.images_encoded, 50);
    assert_eq!(stats.output_bytes, 1024);
    assert_eq!(stats.episodes_written, 1);
    assert_eq!(stats.episode_stats.len(), 1);
}

#[test]
fn test_writer_stats_fps() {
    let stats = WriterStats {
        frames_written: 300,
        duration: Duration::from_secs(10),
        ..Default::default()
    };

    assert!((stats.fps() - 30.0).abs() < 0.1);
}

#[test]
fn test_writer_stats_mb_per_sec() {
    let stats = WriterStats {
        output_bytes: 10_485_760, // 10 MB
        duration: Duration::from_secs(2),
        ..Default::default()
    };

    assert!((stats.mb_per_sec() - 5.0).abs() < 0.1);
}

// ============================================================================
// ProgressStats Tests
// ============================================================================

#[test]
fn test_progress_stats_progress() {
    let progress = ProgressStats {
        current_frame: 50,
        total_frames: Some(100),
        ..Default::default()
    };

    assert!((progress.progress().unwrap() - 0.5).abs() < 0.01);
}

#[test]
fn test_progress_stats_fps() {
    let progress = ProgressStats {
        current_frame: 100,
        elapsed: Duration::from_secs(5),
        ..Default::default()
    };

    assert!((progress.fps() - 20.0).abs() < 0.1);
}

#[test]
fn test_progress_stats_estimate_remaining() {
    let progress = ProgressStats {
        current_frame: 50,
        total_frames: Some(100),
        elapsed: Duration::from_secs(5),
        ..Default::default()
    };

    let remaining = progress.estimate_remaining().unwrap();
    // At 10 fps, 50 remaining frames = 5 seconds
    assert_eq!(remaining, Duration::from_secs(5));
}

#[test]
fn test_progress_stats_no_total_frames() {
    let progress = ProgressStats {
        current_frame: 50,
        total_frames: None,
        ..Default::default()
    };

    assert!(progress.progress().is_none());
    assert!(progress.estimate_remaining().is_none());
}

// ============================================================================
// MockStorage Tests
// ============================================================================

#[test]
fn test_mock_storage_operations() {
    let storage = MockStorage::new();

    storage.record_upload("file1.txt", b"data1").unwrap();
    storage.record_upload("file2.txt", b"data2").unwrap();

    let ops = storage.get_operations();
    assert_eq!(ops.len(), 2);

    assert!(matches!(&ops[0], StorageOperation::Upload { key, .. } if key == "file1.txt"));
    assert!(matches!(&ops[1], StorageOperation::Upload { key, .. } if key == "file2.txt"));
}

#[test]
fn test_mock_storage_has_file() {
    let storage = MockStorage::new();

    assert!(!storage.has_file("test.txt"));

    storage.record_upload("test.txt", b"hello").unwrap();
    assert!(storage.has_file("test.txt"));
}

#[test]
fn test_mock_storage_get_file() {
    let storage = MockStorage::new();

    storage.record_upload("test.txt", b"hello world").unwrap();

    let content = storage.get_file("test.txt");
    assert_eq!(content, Some(b"hello world".to_vec()));
}

#[test]
fn test_mock_storage_failure() {
    let storage = MockStorage::new();
    storage.fail_after(2);

    storage.record_upload("file1.txt", b"data").unwrap();
    storage.record_upload("file2.txt", b"data").unwrap();

    // Third operation should fail
    let result = storage.record_upload("file3.txt", b"data");
    assert!(result.is_err());
}

#[test]
fn test_mock_storage_clear() {
    let storage = MockStorage::new();

    storage.record_upload("test.txt", b"hello").unwrap();
    assert!(storage.has_file("test.txt"));

    storage.clear();
    assert!(!storage.has_file("test.txt"));
    assert!(storage.get_operations().is_empty());
}
