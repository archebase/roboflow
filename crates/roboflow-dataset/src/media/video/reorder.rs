// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Frame reorder buffer for maintaining sequence ordering.
//!
//! This module provides utilities to track and reorder frames
//! per camera, ensuring frames are processed in the correct order
//! even when processed in parallel.

use std::collections::{BTreeMap, HashMap};
use tracing::{debug, warn};

/// Tracker for a single camera's sequence.
#[derive(Debug, Default)]
pub struct CameraSequence {
    /// Next expected sequence number.
    next_expected: u64,
    /// Buffered results waiting for their turn.
    buffer: BTreeMap<u64, SequencedItem>,
    /// Maximum buffer size before warning.
    max_buffer_size: usize,
}

/// An item with a sequence number.
#[derive(Debug)]
pub struct SequencedItem {
    /// Sequence number.
    pub sequence: u64,
    /// Camera ID.
    pub camera_id: String,
    /// Item type marker (for type safety).
    pub _marker: (),
}

impl CameraSequence {
    /// Create a new camera sequence tracker.
    pub fn new() -> Self {
        Self {
            next_expected: 0,
            buffer: BTreeMap::new(),
            max_buffer_size: 256,
        }
    }

    /// Create with custom max buffer size.
    pub fn with_max_buffer(max_buffer_size: usize) -> Self {
        Self {
            next_expected: 0,
            buffer: BTreeMap::new(),
            max_buffer_size,
        }
    }

    /// Add an item to the buffer.
    ///
    /// If the buffer exceeds max_buffer_size, the oldest item (smallest sequence) is evicted.
    pub fn push(&mut self, item: SequencedItem) {
        self.buffer.insert(item.sequence, item);

        // Hard limit: evict oldest item if buffer exceeds max size
        if self.buffer.len() > self.max_buffer_size
            && let Some((&oldest_seq, _)) = self.buffer.iter().next()
        {
            warn!(
                next_expected = self.next_expected,
                evicted_sequence = oldest_seq,
                buffer_size = self.buffer.len(),
                "Buffer overflow, evicting oldest item"
            );
            self.buffer.remove(&oldest_seq);

            // Update next_expected if we evicted what we were waiting for
            if oldest_seq < self.next_expected
                && let Some(&next_avail) = self.buffer.keys().next()
            {
                self.next_expected = next_avail;
            }
        }
    }

    /// Check if there's an item ready to pop.
    pub fn has_ready(&self) -> bool {
        self.buffer.contains_key(&self.next_expected)
    }

    /// Pop the next item in sequence.
    pub fn pop(&mut self) -> Option<SequencedItem> {
        if let Some(item) = self.buffer.remove(&self.next_expected) {
            self.next_expected += 1;
            return Some(item);
        }
        None
    }

    /// Pop all ready items.
    pub fn pop_all_ready(&mut self) -> Vec<SequencedItem> {
        let mut items = Vec::new();
        while let Some(item) = self.pop() {
            items.push(item);
        }
        items
    }

    /// Get current buffer size.
    pub fn buffer_size(&self) -> usize {
        self.buffer.len()
    }

    /// Get next expected sequence.
    pub fn next_expected(&self) -> u64 {
        self.next_expected
    }

    /// Check if buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Skip to a specific sequence (dropping missing items).
    pub fn skip_to(&mut self, sequence: u64) {
        if sequence > self.next_expected {
            // Remove items before the new expected
            self.buffer.retain(|&seq, _| seq >= sequence);
            self.next_expected = sequence;
        }
    }

    /// Clear the buffer and reset sequence.
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.next_expected = 0;
    }
}

/// Multi-camera sequence tracker.
#[derive(Debug)]
pub struct FrameReorderBuffer<T> {
    /// Per-camera sequence trackers.
    cameras: HashMap<String, CameraSequenceTracker<T>>,
    /// Maximum buffer size per camera.
    max_buffer_size: usize,
}

/// Sequence tracker for a single camera with typed items.
#[derive(Debug)]
struct CameraSequenceTracker<T> {
    /// Next expected sequence.
    next_expected: u64,
    /// Buffered items.
    buffer: BTreeMap<u64, T>,
    /// Max buffer size.
    max_buffer_size: usize,
}

impl<T> CameraSequenceTracker<T> {
    fn new(max_buffer_size: usize) -> Self {
        Self {
            next_expected: 0,
            buffer: BTreeMap::new(),
            max_buffer_size,
        }
    }

    fn push(&mut self, sequence: u64, item: T) {
        self.buffer.insert(sequence, item);

        // Hard limit: evict oldest item if buffer exceeds max size
        if self.buffer.len() > self.max_buffer_size
            && let Some((&oldest_seq, _)) = self.buffer.iter().next()
        {
            debug!(
                next_expected = self.next_expected,
                evicted_sequence = oldest_seq,
                buffer_size = self.buffer.len(),
                "Buffer overflow, evicting oldest item"
            );
            self.buffer.remove(&oldest_seq);

            // Update next_expected if we evicted what we were waiting for
            if oldest_seq < self.next_expected
                && let Some(&next_avail) = self.buffer.keys().next()
            {
                self.next_expected = next_avail;
            }
        }
    }

    fn pop(&mut self) -> Option<T> {
        if let Some(item) = self.buffer.remove(&self.next_expected) {
            self.next_expected += 1;
            return Some(item);
        }
        None
    }

    fn has_ready(&self) -> bool {
        self.buffer.contains_key(&self.next_expected)
    }

    fn buffer_size(&self) -> usize {
        self.buffer.len()
    }

    fn next_expected(&self) -> u64 {
        self.next_expected
    }
}

impl<T> FrameReorderBuffer<T> {
    /// Create a new frame reorder buffer.
    pub fn new() -> Self {
        Self {
            cameras: HashMap::new(),
            max_buffer_size: 256,
        }
    }

    /// Create with custom max buffer size.
    pub fn with_max_buffer(max_buffer_size: usize) -> Self {
        Self {
            cameras: HashMap::new(),
            max_buffer_size,
        }
    }

    /// Ensure a camera tracker exists.
    fn ensure_camera(&mut self, camera_id: &str) {
        if !self.cameras.contains_key(camera_id) {
            self.cameras.insert(
                camera_id.to_string(),
                CameraSequenceTracker::new(self.max_buffer_size),
            );
        }
    }

    /// Add an item for a camera.
    pub fn push(&mut self, camera_id: &str, sequence: u64, item: T) {
        self.ensure_camera(camera_id);
        if let Some(tracker) = self.cameras.get_mut(camera_id) {
            tracker.push(sequence, item);
        }
    }

    /// Pop the next ready item for a camera.
    pub fn pop(&mut self, camera_id: &str) -> Option<T> {
        self.cameras.get_mut(camera_id).and_then(|t| t.pop())
    }

    /// Check if a camera has a ready item.
    pub fn has_ready(&self, camera_id: &str) -> bool {
        self.cameras
            .get(camera_id)
            .map(|t| t.has_ready())
            .unwrap_or(false)
    }

    /// Get all cameras that have ready items.
    pub fn ready_cameras(&self) -> Vec<&str> {
        self.cameras
            .iter()
            .filter(|(_, t)| t.has_ready())
            .map(|(id, _)| id.as_str())
            .collect()
    }

    /// Get total buffer size across all cameras.
    pub fn total_buffer_size(&self) -> usize {
        self.cameras.values().map(|t| t.buffer_size()).sum()
    }

    /// Get buffer size for a specific camera.
    pub fn buffer_size(&self, camera_id: &str) -> usize {
        self.cameras
            .get(camera_id)
            .map(|t| t.buffer_size())
            .unwrap_or(0)
    }

    /// Get all camera IDs.
    pub fn cameras(&self) -> impl Iterator<Item = &str> {
        self.cameras.keys().map(|s| s.as_str())
    }

    /// Get next expected sequence for a camera.
    pub fn next_expected(&self, camera_id: &str) -> u64 {
        self.cameras
            .get(camera_id)
            .map(|t| t.next_expected())
            .unwrap_or(0)
    }

    /// Check if all cameras have empty buffers.
    pub fn all_empty(&self) -> bool {
        self.cameras.values().all(|t| t.buffer_size() == 0)
    }

    /// Clear all buffers.
    pub fn clear(&mut self) {
        self.cameras.clear();
    }

    /// Pop all ready items from all cameras.
    pub fn pop_all_ready(&mut self) -> Vec<(String, T)> {
        let mut items = Vec::new();
        let camera_ids: Vec<String> = self.cameras.keys().cloned().collect();

        for camera_id in camera_ids {
            while let Some(item) = self.cameras.get_mut(&camera_id).and_then(|t| t.pop()) {
                items.push((camera_id.clone(), item));
            }
        }

        items
    }
}

impl<T> Default for FrameReorderBuffer<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_camera_sequence_basic() {
        let mut seq = CameraSequence::new();

        // Add items out of order
        seq.push(SequencedItem {
            sequence: 2,
            camera_id: "cam0".to_string(),
            _marker: (),
        });
        seq.push(SequencedItem {
            sequence: 0,
            camera_id: "cam0".to_string(),
            _marker: (),
        });
        seq.push(SequencedItem {
            sequence: 1,
            camera_id: "cam0".to_string(),
            _marker: (),
        });

        // Pop in order
        assert!(seq.has_ready());
        assert_eq!(seq.pop().unwrap().sequence, 0);
        assert_eq!(seq.pop().unwrap().sequence, 1);
        assert_eq!(seq.pop().unwrap().sequence, 2);
        assert!(seq.pop().is_none());
        assert!(!seq.has_ready());
    }

    #[test]
    fn test_camera_sequence_missing() {
        let mut seq = CameraSequence::new();

        seq.push(SequencedItem {
            sequence: 0,
            camera_id: "cam0".to_string(),
            _marker: (),
        });
        seq.push(SequencedItem {
            sequence: 2,
            camera_id: "cam0".to_string(),
            _marker: (),
        });
        seq.push(SequencedItem {
            sequence: 3,
            camera_id: "cam0".to_string(),
            _marker: (),
        });

        // Can pop 0
        assert_eq!(seq.pop().unwrap().sequence, 0);

        // Can't pop 2 or 3 because 1 is missing
        assert!(!seq.has_ready());
        assert!(seq.pop().is_none());
        assert_eq!(seq.buffer_size(), 2);
    }

    #[test]
    fn test_camera_sequence_skip() {
        let mut seq = CameraSequence::new();

        seq.push(SequencedItem {
            sequence: 5,
            camera_id: "cam0".to_string(),
            _marker: (),
        });
        seq.push(SequencedItem {
            sequence: 6,
            camera_id: "cam0".to_string(),
            _marker: (),
        });

        // Skip to 5
        seq.skip_to(5);

        // Now can pop 5 and 6
        assert_eq!(seq.pop().unwrap().sequence, 5);
        assert_eq!(seq.pop().unwrap().sequence, 6);
    }

    #[test]
    fn test_frame_reorder_buffer_multi_camera() {
        let mut buffer: FrameReorderBuffer<String> = FrameReorderBuffer::new();

        // Add items for multiple cameras
        buffer.push("cam0", 0, "frame0".to_string());
        buffer.push("cam0", 2, "frame2".to_string());
        buffer.push("cam1", 0, "cam1_frame0".to_string());
        buffer.push("cam1", 1, "cam1_frame1".to_string());

        // Check readiness
        assert!(buffer.has_ready("cam0"));
        assert!(buffer.has_ready("cam1"));

        // Pop from cam1 (complete sequence)
        assert_eq!(buffer.pop("cam1").unwrap(), "cam1_frame0");
        assert_eq!(buffer.pop("cam1").unwrap(), "cam1_frame1");
        assert!(buffer.pop("cam1").is_none());

        // Pop from cam0 (missing frame 1)
        assert_eq!(buffer.pop("cam0").unwrap(), "frame0");
        assert!(!buffer.has_ready("cam0"));

        // Add missing frame
        buffer.push("cam0", 1, "frame1".to_string());
        assert!(buffer.has_ready("cam0"));
        assert_eq!(buffer.pop("cam0").unwrap(), "frame1");
        assert_eq!(buffer.pop("cam0").unwrap(), "frame2");
    }

    #[test]
    fn test_frame_reorder_buffer_ready_cameras() {
        let mut buffer: FrameReorderBuffer<i32> = FrameReorderBuffer::new();

        buffer.push("cam0", 0, 1);
        buffer.push("cam1", 1, 2); // Missing sequence 0
        buffer.push("cam2", 0, 3);

        let ready: Vec<_> = buffer.ready_cameras();
        assert_eq!(ready.len(), 2);
        assert!(ready.contains(&"cam0"));
        assert!(ready.contains(&"cam2"));
        assert!(!ready.contains(&"cam1"));
    }

    #[test]
    fn test_frame_reorder_buffer_pop_all() {
        let mut buffer: FrameReorderBuffer<i32> = FrameReorderBuffer::new();

        buffer.push("cam0", 0, 1);
        buffer.push("cam0", 1, 2);
        buffer.push("cam1", 0, 3);
        buffer.push("cam1", 2, 5); // Missing 1

        let items = buffer.pop_all_ready();

        // Should get cam0: 1, 2 and cam1: 3
        assert_eq!(items.len(), 3);

        // Check that we got the expected items
        let cam0_items: Vec<_> = items.iter().filter(|(c, _)| c == "cam0").collect();
        let cam1_items: Vec<_> = items.iter().filter(|(c, _)| c == "cam1").collect();

        assert_eq!(cam0_items.len(), 2);
        assert_eq!(cam1_items.len(), 1);

        // cam1 should still have item for sequence 2
        assert_eq!(buffer.buffer_size("cam1"), 1);
    }

    #[test]
    fn test_total_buffer_size() {
        let mut buffer: FrameReorderBuffer<i32> = FrameReorderBuffer::new();

        assert_eq!(buffer.total_buffer_size(), 0);

        buffer.push("cam0", 0, 1);
        buffer.push("cam0", 2, 2);
        buffer.push("cam1", 1, 3);
        buffer.push("cam2", 0, 4);

        assert_eq!(buffer.total_buffer_size(), 4);

        buffer.pop("cam0"); // Pops 1 item
        buffer.pop("cam2"); // Pops 1 item

        assert_eq!(buffer.total_buffer_size(), 2);
    }
}
