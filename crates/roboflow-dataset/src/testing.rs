// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Test utilities for roboflow-dataset.
//!
//! This module provides mock implementations and test builders for testing
//! dataset writers and pipeline components.
//!
//! # Mock Implementations
//!
//! - [`MockSource`] - Mock data source with pre-defined messages
//! - [`InMemoryWriter`] - In-memory dataset writer for testing
//! - [`MockStorage`] - Mock storage backend for testing uploads
//!
//! # Test Builders
//!
//! - [`FrameBuilder`] - Builder for creating test frames
//! - [`MessageBuilder`] - Builder for creating test messages
//!
//! # Example
//!
//! ```rust,ignore
//! use roboflow_dataset::testing::{MockSource, InMemoryWriter, FrameBuilder};
//!
//! let source = MockSource::with_messages(vec![
//!     MessageBuilder::new("/camera").image(640, 480).build(),
//! ]);
//!
//! let writer = InMemoryWriter::new();
//! writer.write_frame(&FrameBuilder::new(0).add_state("pos", vec![0.0]).build());
//! ```

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

use crate::core::stats::EpisodeStats;
use crate::core::traits::FormatWriter;
use crate::formats::common::{AlignedFrame, ImageRef, WriterStats};
use crate::sources::{Source, SourceConfig, SourceMetadata, SourceResult, TimestampedMessage};

// ============================================================================
// Mock Source
// ============================================================================

/// Mock source for testing.
///
/// Provides pre-defined messages without reading from actual files.
pub struct MockSource {
    messages: Vec<TimestampedMessage>,
    index: usize,
    error_at: Option<usize>,
}

impl MockSource {
    /// Create a mock source with the given messages.
    pub fn with_messages(messages: Vec<TimestampedMessage>) -> Self {
        Self {
            messages,
            index: 0,
            error_at: None,
        }
    }

    /// Create a mock source that produces N empty messages.
    pub fn with_count(count: usize) -> Self {
        let messages = (0..count)
            .map(|i| TimestampedMessage {
                topic: "/test".to_string(),
                log_time: i as u64 * 1_000_000_000,
                data: roboflow_core::CodecValue::Null,
            })
            .collect();
        Self {
            messages,
            index: 0,
            error_at: None,
        }
    }

    /// Create a mock source that will error at the given message index.
    pub fn with_error_at(error_index: usize) -> Self {
        Self {
            messages: vec![],
            index: 0,
            error_at: Some(error_index),
        }
    }

    /// Create a mock source with camera images at specified fps.
    pub fn with_camera_images(camera_name: &str, frame_count: usize, fps: f64) -> Self {
        let ns_per_frame = (1_000_000_000.0 / fps) as u64;
        let messages: Vec<TimestampedMessage> = (0..frame_count)
            .map(|i| {
                let topic = format!("/{camera_name}/image");
                let image_data = generate_test_jpeg(320, 240, i as u8);
                TimestampedMessage {
                    topic,
                    log_time: i as u64 * ns_per_frame,
                    data: roboflow_core::CodecValue::Bytes(image_data),
                }
            })
            .collect();
        Self::with_messages(messages)
    }

    /// Create a mock source with state messages.
    pub fn with_state_messages(topic: &str, frame_count: usize, fps: f64, dim: usize) -> Self {
        let ns_per_frame = (1_000_000_000.0 / fps) as u64;
        let messages: Vec<TimestampedMessage> = (0..frame_count)
            .map(|i| {
                let values: Vec<roboflow_core::CodecValue> = (0..dim)
                    .map(|j| roboflow_core::CodecValue::Float32((i * dim + j) as f32))
                    .collect();
                TimestampedMessage {
                    topic: topic.to_string(),
                    log_time: i as u64 * ns_per_frame,
                    data: roboflow_core::CodecValue::Array(values),
                }
            })
            .collect();
        Self::with_messages(messages)
    }

    /// Create a multi-topic source with camera and state data.
    pub fn with_multi_topic(frame_count: usize, fps: f64) -> Self {
        let ns_per_frame = (1_000_000_000.0 / fps) as u64;
        let mut messages = Vec::new();

        for i in 0..frame_count {
            let ts = i as u64 * ns_per_frame;

            // Camera image
            messages.push(TimestampedMessage {
                topic: "/camera/image".to_string(),
                log_time: ts,
                data: roboflow_core::CodecValue::Bytes(generate_test_jpeg(320, 240, i as u8)),
            });

            // State
            messages.push(TimestampedMessage {
                topic: "/state".to_string(),
                log_time: ts,
                data: roboflow_core::CodecValue::Array(vec![
                    roboflow_core::CodecValue::Float32(i as f32),
                    roboflow_core::CodecValue::Float32((i + 1) as f32),
                ]),
            });

            // Action
            messages.push(TimestampedMessage {
                topic: "/action".to_string(),
                log_time: ts,
                data: roboflow_core::CodecValue::Array(vec![roboflow_core::CodecValue::Float32(
                    (i + 2) as f32,
                )]),
            });
        }

        Self::with_messages(messages)
    }

    /// Set messages for an error-at source.
    pub fn set_messages(&mut self, messages: Vec<TimestampedMessage>) {
        self.messages = messages;
        self.index = 0;
    }
}

#[async_trait]
impl Source for MockSource {
    async fn initialize(&mut self, _config: &SourceConfig) -> SourceResult<SourceMetadata> {
        Ok(
            SourceMetadata::new("mock".to_string(), "memory".to_string())
                .with_message_count(self.messages.len() as u64),
        )
    }

    async fn read_batch(&mut self, size: usize) -> SourceResult<Option<Vec<TimestampedMessage>>> {
        // Check for simulated error
        if let Some(error_at) = self.error_at
            && self.index >= error_at
        {
            return Err(crate::sources::error::SourceError::ReadFailed(
                "Simulated error".to_string(),
            ));
        }

        if self.index >= self.messages.len() {
            return Ok(None);
        }

        let end = (self.index + size).min(self.messages.len());
        let batch = self.messages[self.index..end].to_vec();
        self.index = end;

        Ok(Some(batch))
    }

    async fn metadata(&self) -> SourceResult<SourceMetadata> {
        Ok(
            SourceMetadata::new("mock".to_string(), "memory".to_string())
                .with_message_count(self.messages.len() as u64),
        )
    }
}

// ============================================================================
// In-Memory Writer
// ============================================================================

/// In-memory dataset writer for testing.
///
/// Stores all frames in memory for inspection in tests.
pub struct InMemoryWriter {
    frames: Vec<AlignedFrame>,
    episode_frames: HashMap<usize, Vec<AlignedFrame>>,
    current_episode: usize,
    finalized: bool,
    stats: WriterStats,
}

impl InMemoryWriter {
    /// Create a new in-memory writer.
    pub fn new() -> Self {
        Self {
            frames: Vec::new(),
            episode_frames: HashMap::new(),
            current_episode: 0,
            finalized: false,
            stats: WriterStats::default(),
        }
    }

    /// Get all written frames.
    pub fn frames(&self) -> &[AlignedFrame] {
        &self.frames
    }

    /// Get frames for a specific episode.
    pub fn episode_frames(&self, episode: usize) -> Option<&[AlignedFrame]> {
        self.episode_frames.get(&episode).map(|v| v.as_slice())
    }

    /// Get the number of frames written.
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    /// Check if no frames were written.
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// Check if finalize was called.
    pub fn is_finalized(&self) -> bool {
        self.finalized
    }

    /// Get the current episode index.
    pub fn current_episode(&self) -> usize {
        self.current_episode
    }

    /// Clear all stored frames.
    pub fn clear(&mut self) {
        self.frames.clear();
        self.episode_frames.clear();
        self.stats = WriterStats::default();
    }
}

impl Default for InMemoryWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl FormatWriter for InMemoryWriter {
    fn write_frame(&mut self, frame: &AlignedFrame) -> roboflow_core::Result<()> {
        self.frames.push(frame.clone());
        self.episode_frames
            .entry(self.current_episode)
            .or_default()
            .push(frame.clone());
        self.stats.frames_written += 1;
        Ok(())
    }

    fn finalize(&mut self) -> roboflow_core::Result<WriterStats> {
        self.finalized = true;
        Ok(self.stats.clone())
    }

    fn frame_count(&self) -> usize {
        self.frames.len()
    }

    fn start_episode(&mut self, task_index: Option<usize>) -> roboflow_core::Result<usize> {
        let _ = task_index;
        self.current_episode = self.episode_frames.len();
        Ok(self.current_episode)
    }

    fn finish_episode(&mut self) -> roboflow_core::Result<EpisodeStats> {
        let frames = self
            .episode_frames
            .get(&self.current_episode)
            .map(|v| v.len())
            .unwrap_or(0);
        let mut stats = EpisodeStats::for_episode(self.current_episode);
        stats.frames = frames;
        Ok(stats)
    }

    fn episode_index(&self) -> Option<usize> {
        Some(self.current_episode)
    }

    fn supports_episodes(&self) -> bool {
        true
    }

    fn format_name(&self) -> &'static str {
        "InMemory"
    }

    fn format_version(&self) -> &'static str {
        "test-1.0"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

// ============================================================================
// Mock Storage
// ============================================================================

/// Operation recorded by MockStorage.
#[derive(Debug, Clone, PartialEq)]
pub enum StorageOperation {
    /// Upload operation
    Upload { key: String, size: usize },
    /// Download operation
    Download { key: String },
    /// Delete operation
    Delete { key: String },
    /// List operation
    List { prefix: String },
}

/// Mock storage backend for testing uploads.
pub struct MockStorage {
    operations: Arc<Mutex<Vec<StorageOperation>>>,
    uploaded_files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    should_fail: AtomicU64,
}

impl MockStorage {
    /// Create a new mock storage.
    pub fn new() -> Self {
        Self {
            operations: Arc::new(Mutex::new(Vec::new())),
            uploaded_files: Arc::new(Mutex::new(HashMap::new())),
            should_fail: AtomicU64::new(0),
        }
    }

    /// Set to fail after N successful operations.
    /// After N successful uploads, the next one will fail.
    pub fn fail_after(&self, count: u64) {
        self.should_fail.store(count, Ordering::SeqCst);
    }

    /// Get all recorded operations.
    pub fn get_operations(&self) -> Vec<StorageOperation> {
        self.operations.lock().unwrap().clone()
    }

    /// Check if a file was uploaded.
    pub fn has_file(&self, key: &str) -> bool {
        self.uploaded_files.lock().unwrap().contains_key(key)
    }

    /// Get uploaded file content.
    pub fn get_file(&self, key: &str) -> Option<Vec<u8>> {
        self.uploaded_files.lock().unwrap().get(key).cloned()
    }

    /// Record an upload operation.
    pub fn record_upload(&self, key: &str, data: &[u8]) -> roboflow_core::Result<()> {
        let fail_at = self.should_fail.load(Ordering::SeqCst);
        if fail_at > 0 {
            let count = self.operations.lock().unwrap().len() as u64;
            if count >= fail_at {
                return Err(roboflow_core::RoboflowError::storage(
                    "mock",
                    "Mock storage failure",
                    false,
                ));
            }
        }

        self.operations
            .lock()
            .unwrap()
            .push(StorageOperation::Upload {
                key: key.to_string(),
                size: data.len(),
            });
        self.uploaded_files
            .lock()
            .unwrap()
            .insert(key.to_string(), data.to_vec());
        Ok(())
    }

    /// Clear all recorded operations.
    pub fn clear(&self) {
        self.operations.lock().unwrap().clear();
        self.uploaded_files.lock().unwrap().clear();
        self.should_fail.store(0, Ordering::SeqCst);
    }
}

impl Default for MockStorage {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Test Builders
// ============================================================================

/// Builder for creating test frames.
#[derive(Debug, Clone)]
pub struct FrameBuilder {
    frame_index: usize,
    timestamp: u64,
    image_refs: HashMap<String, ImageRef>,
    states: HashMap<String, Vec<f32>>,
    actions: HashMap<String, Vec<f32>>,
    timestamps: HashMap<String, u64>,
}

impl FrameBuilder {
    /// Create a new frame builder.
    pub fn new(frame_index: usize) -> Self {
        Self {
            frame_index,
            timestamp: frame_index as u64 * 33_333_333, // ~30fps
            image_refs: HashMap::new(),
            states: HashMap::new(),
            actions: HashMap::new(),
            timestamps: HashMap::new(),
        }
    }

    /// Set the timestamp.
    pub fn with_timestamp(mut self, timestamp: u64) -> Self {
        self.timestamp = timestamp;
        self
    }

    /// Add an image observation (stores only ImageRef metadata).
    pub fn add_image(mut self, name: &str, width: u32, height: u32) -> Self {
        self.image_refs
            .insert(name.to_string(), ImageRef { width, height });
        self
    }

    /// Add an encoded image observation (stores only ImageRef metadata).
    pub fn add_encoded_image(mut self, name: &str, width: u32, height: u32) -> Self {
        self.image_refs
            .insert(name.to_string(), ImageRef { width, height });
        self
    }

    /// Add a state observation.
    pub fn add_state(mut self, name: &str, values: Vec<f32>) -> Self {
        self.states.insert(name.to_string(), values);
        self
    }

    /// Add an action.
    pub fn add_action(mut self, name: &str, values: Vec<f32>) -> Self {
        self.actions.insert(name.to_string(), values);
        self
    }

    /// Add a timestamp.
    pub fn add_timestamp(mut self, name: &str, ts: u64) -> Self {
        self.timestamps.insert(name.to_string(), ts);
        self
    }

    /// Build the frame.
    pub fn build(self) -> AlignedFrame {
        AlignedFrame {
            frame_index: self.frame_index,
            timestamp: self.timestamp,
            image_refs: self.image_refs,
            states: self.states,
            actions: self.actions,
            timestamps: self.timestamps,
            audio: HashMap::new(),
        }
    }
}

/// Builder for creating test messages.
pub struct MessageBuilder {
    topic: String,
    log_time: u64,
    data: roboflow_core::CodecValue,
}

impl MessageBuilder {
    /// Create a new message builder.
    pub fn new(topic: &str) -> Self {
        Self {
            topic: topic.to_string(),
            log_time: 0,
            data: roboflow_core::CodecValue::Null,
        }
    }

    /// Set the timestamp.
    pub fn with_timestamp(mut self, log_time: u64) -> Self {
        self.log_time = log_time;
        self
    }

    /// Set image data.
    pub fn image(mut self, width: u32, height: u32) -> Self {
        let data = generate_test_jpeg(width, height, 0);
        self.data = roboflow_core::CodecValue::Bytes(data);
        self
    }

    /// Set float array data.
    pub fn float_array(mut self, values: Vec<f32>) -> Self {
        let codec_values: Vec<roboflow_core::CodecValue> = values
            .into_iter()
            .map(roboflow_core::CodecValue::Float32)
            .collect();
        self.data = roboflow_core::CodecValue::Array(codec_values);
        self
    }

    /// Set raw bytes data.
    pub fn bytes(mut self, data: Vec<u8>) -> Self {
        self.data = roboflow_core::CodecValue::Bytes(data);
        self
    }

    /// Build the message.
    pub fn build(self) -> TimestampedMessage {
        TimestampedMessage {
            topic: self.topic,
            log_time: self.log_time,
            data: self.data,
        }
    }
}

// ============================================================================
// Test Helpers
// ============================================================================

/// Generate a minimal valid JPEG for testing.
pub fn generate_test_jpeg(width: u32, height: u32, pattern: u8) -> Vec<u8> {
    // Create a minimal JPEG-like header
    // This is NOT a valid JPEG but serves as test data
    let size = (width * height * 3 / 10) as usize; // Compressed estimate
    let mut data = Vec::with_capacity(size + 100);

    // SOI marker
    data.extend_from_slice(&[0xFF, 0xD8]);
    // Fake APP0 marker
    data.extend_from_slice(&[0xFF, 0xE0, 0x00, 0x10]);
    data.extend_from_slice(b"JFIF\x00");
    data.extend_from_slice(&[0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00]);
    // DQT marker (quantization table)
    data.extend_from_slice(&[0xFF, 0xDB, 0x00, 0x43, 0x00]);
    data.extend_from_slice(&[pattern; 64]);
    // Image data placeholder
    data.extend_from_slice(&[0xFF, 0xDA]);
    data.extend_from_slice(&vec![pattern; size.min(1000)]);
    // EOI marker
    data.extend_from_slice(&[0xFF, 0xD9]);

    data
}

/// Generate test video frames.
pub fn generate_test_frames(count: usize, width: u32, height: u32) -> Vec<AlignedFrame> {
    (0..count)
        .map(|i| {
            FrameBuilder::new(i)
                .add_encoded_image("observation.camera_0", width, height)
                .add_state("observation.state", vec![i as f32])
                .build()
        })
        .collect()
}

/// Generate test messages with specified fps.
pub fn generate_test_messages(count: usize, fps: f64, topic: &str) -> Vec<TimestampedMessage> {
    let ns_per_frame = (1_000_000_000.0 / fps) as u64;
    (0..count)
        .map(|i| {
            MessageBuilder::new(topic)
                .with_timestamp(i as u64 * ns_per_frame)
                .float_array(vec![i as f32])
                .build()
        })
        .collect()
}

/// Count messages from a source.
pub async fn count_messages(source: &mut MockSource) -> usize {
    let mut count = 0;
    while let Ok(Some(batch)) = source.read_batch(100).await {
        count += batch.len();
    }
    count
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_source_read_batch() {
        let messages = vec![
            TimestampedMessage {
                topic: "/test".to_string(),
                log_time: 0,
                data: roboflow_core::CodecValue::Null,
            },
            TimestampedMessage {
                topic: "/test".to_string(),
                log_time: 1_000_000_000,
                data: roboflow_core::CodecValue::Null,
            },
        ];

        let mut source = MockSource::with_messages(messages);

        let batch = source.read_batch(1).await.unwrap();
        assert_eq!(batch.unwrap().len(), 1);

        let batch = source.read_batch(1).await.unwrap();
        assert_eq!(batch.unwrap().len(), 1);

        let batch = source.read_batch(1).await.unwrap();
        assert!(batch.is_none());
    }

    #[tokio::test]
    async fn test_mock_source_with_count() {
        let mut source = MockSource::with_count(10);
        let count = count_messages(&mut source).await;
        assert_eq!(count, 10);
    }

    #[tokio::test]
    async fn test_mock_source_with_error() {
        let mut source = MockSource::with_error_at(5);
        source.set_messages(
            (0..10)
                .map(|i| TimestampedMessage {
                    topic: "/test".to_string(),
                    log_time: i as u64,
                    data: roboflow_core::CodecValue::Null,
                })
                .collect(),
        );

        // Should succeed for first 5 reads
        for _ in 0..5 {
            let batch = source.read_batch(1).await.unwrap();
            assert!(batch.is_some());
        }

        // Should fail on 6th read
        let result = source.read_batch(1).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_in_memory_writer() {
        let mut writer = InMemoryWriter::new();

        writer.start_episode(None).unwrap();
        writer
            .write_frame(&FrameBuilder::new(0).add_state("pos", vec![0.0]).build())
            .unwrap();
        writer
            .write_frame(&FrameBuilder::new(1).add_state("pos", vec![1.0]).build())
            .unwrap();
        writer.finish_episode().unwrap();
        writer.finalize().unwrap();

        assert_eq!(writer.len(), 2);
        assert!(writer.is_finalized());
        assert!(writer.episode_frames(0).is_some());
    }

    #[test]
    fn test_frame_builder() {
        let frame = FrameBuilder::new(0)
            .with_timestamp(1_000_000_000)
            .add_state("observation.state", vec![0.0, 1.0, 2.0])
            .add_action("action", vec![1.0])
            .add_image("observation.camera_0", 640, 480)
            .build();

        assert_eq!(frame.frame_index, 0);
        assert_eq!(frame.timestamp, 1_000_000_000);
        assert!(frame.states.contains_key("observation.state"));
        assert!(frame.actions.contains_key("action"));
        assert!(frame.image_refs.contains_key("observation.camera_0"));
    }

    #[test]
    fn test_message_builder() {
        let msg = MessageBuilder::new("/camera")
            .with_timestamp(1_000_000)
            .float_array(vec![1.0, 2.0])
            .build();

        assert_eq!(msg.topic, "/camera");
        assert_eq!(msg.log_time, 1_000_000);
    }

    #[test]
    fn test_mock_storage() {
        let storage = MockStorage::new();

        storage.record_upload("test/file.txt", b"hello").unwrap();
        assert!(storage.has_file("test/file.txt"));
        assert_eq!(storage.get_file("test/file.txt"), Some(b"hello".to_vec()));

        let ops = storage.get_operations();
        assert_eq!(ops.len(), 1);
        assert!(matches!(&ops[0], StorageOperation::Upload { key, .. } if key == "test/file.txt"));
    }

    #[test]
    fn test_mock_storage_failure() {
        let storage = MockStorage::new();
        storage.fail_after(1); // Fail after 1 successful operation

        storage.record_upload("file1.txt", b"data").unwrap();

        // Second operation should fail
        let result = storage.record_upload("file2.txt", b"data");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_mock_source_multi_topic() {
        let source = MockSource::with_multi_topic(10, 30.0);
        let mut source = source;

        // Should have 30 messages (10 frames * 3 topics)
        let count = count_messages(&mut source).await;
        assert_eq!(count, 30);
    }
}
