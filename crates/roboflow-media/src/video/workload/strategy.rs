// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Encoding strategy definitions for video workload.
//!
//! This module defines how frames are buffered and encoded for each stream.

/// How frames are buffered and encoded for a stream.
///
/// This enum defines the memory and encoding strategy for a single output stream.
/// Different streams in a workload can use different strategies.
#[derive(Debug, Clone, Default)]
pub enum EncodingStrategy {
    /// Standard encoding: buffer all frames in memory, encode at finalize.
    ///
    /// Fastest performance but uses unbounded memory (not suitable for long videos).
    /// Best for short videos or when memory is not a concern.
    #[default]
    Standard,

    /// Fragment encoding: bounded memory via periodic flush.
    ///
    /// Frames are buffered until flush triggers are reached, then encoded to
    /// temporary fragment files. All fragments are concatenated at finalize.
    /// Memory usage stays bounded regardless of video length.
    Fragment {
        /// Auto-flush triggers for fragment creation.
        triggers: FragmentTriggers,
    },

    /// Streaming encoding: encode and send chunks immediately.
    ///
    /// Frames are encoded and sent to a channel as they arrive. Best for
    /// real-time streaming or when output should be processed incrementally.
    Streaming {
        /// Minimum chunk size before sending to channel (bytes).
        chunk_size: usize,
    },
}

impl EncodingStrategy {
    /// Create a standard (unbounded) encoding strategy.
    pub fn standard() -> Self {
        Self::Standard
    }

    /// Create a fragment encoding strategy with auto-flush after N frames.
    pub fn fragment_by_frames(frames: u32) -> Self {
        Self::Fragment {
            triggers: FragmentTriggers {
                frame_count: Some(frames),
                ..Default::default()
            },
        }
    }

    /// Create a fragment encoding strategy with auto-flush after N bytes.
    pub fn fragment_by_memory(bytes: usize) -> Self {
        Self::Fragment {
            triggers: FragmentTriggers {
                memory_bytes: Some(bytes),
                ..Default::default()
            },
        }
    }

    /// Create a fragment encoding strategy with auto-flush after N seconds.
    pub fn fragment_by_duration(secs: f64) -> Self {
        Self::Fragment {
            triggers: FragmentTriggers {
                duration_secs: Some(secs),
                ..Default::default()
            },
        }
    }

    /// Create a fragment encoding strategy with custom triggers.
    pub fn fragment(triggers: FragmentTriggers) -> Self {
        Self::Fragment { triggers }
    }

    /// Create a streaming encoding strategy.
    pub fn streaming(chunk_size: usize) -> Self {
        Self::Streaming { chunk_size }
    }

    /// Check if this strategy uses bounded memory.
    pub fn is_bounded_memory(&self) -> bool {
        matches!(self, Self::Fragment { .. })
    }

    /// Check if this strategy is streaming.
    pub fn is_streaming(&self) -> bool {
        matches!(self, Self::Streaming { .. })
    }
}

/// Triggers for automatic fragment flushing.
///
/// Multiple triggers can be combined; the first one reached will cause a flush.
#[derive(Debug, Clone, Default)]
pub struct FragmentTriggers {
    /// Auto-flush after N frames (None = no frame-based trigger).
    pub frame_count: Option<u32>,

    /// Auto-flush after N bytes buffered (None = no memory-based trigger).
    pub memory_bytes: Option<usize>,

    /// Auto-flush after N seconds of video (None = no duration-based trigger).
    /// Calculated as: frames / fps.
    pub duration_secs: Option<f64>,
}

impl FragmentTriggers {
    /// Create a new fragment triggers configuration with manual flush only.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create triggers for frame-based flushing.
    pub fn frame_count(frames: u32) -> Self {
        Self {
            frame_count: Some(frames),
            memory_bytes: None,
            duration_secs: None,
        }
    }

    /// Create triggers for memory-based flushing.
    pub fn memory_bytes(bytes: usize) -> Self {
        Self {
            frame_count: None,
            memory_bytes: Some(bytes),
            duration_secs: None,
        }
    }

    /// Create triggers for duration-based flushing.
    pub fn duration_secs(secs: f64) -> Self {
        Self {
            frame_count: None,
            memory_bytes: None,
            duration_secs: Some(secs),
        }
    }

    /// Check if any trigger is configured.
    pub fn has_triggers(&self) -> bool {
        self.frame_count.is_some() || self.memory_bytes.is_some() || self.duration_secs.is_some()
    }

    /// Add frame count trigger to existing configuration.
    pub fn with_frame_count(mut self, frames: u32) -> Self {
        self.frame_count = Some(frames);
        self
    }

    /// Add memory bytes trigger to existing configuration.
    pub fn with_memory_bytes(mut self, bytes: usize) -> Self {
        self.memory_bytes = Some(bytes);
        self
    }

    /// Add duration trigger to existing configuration.
    pub fn with_duration_secs(mut self, secs: f64) -> Self {
        self.duration_secs = Some(secs);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encoding_strategy_standard() {
        let strategy = EncodingStrategy::standard();
        assert!(!strategy.is_bounded_memory());
        assert!(!strategy.is_streaming());
    }

    #[test]
    fn test_encoding_strategy_fragment_by_frames() {
        let strategy = EncodingStrategy::fragment_by_frames(300);
        assert!(strategy.is_bounded_memory());
        assert!(!strategy.is_streaming());
    }

    #[test]
    fn test_encoding_strategy_fragment_by_memory() {
        let strategy = EncodingStrategy::fragment_by_memory(100_000_000);
        assert!(strategy.is_bounded_memory());
    }

    #[test]
    fn test_encoding_strategy_fragment_by_duration() {
        let strategy = EncodingStrategy::fragment_by_duration(10.0);
        assert!(strategy.is_bounded_memory());
    }

    #[test]
    fn test_encoding_strategy_streaming() {
        let strategy = EncodingStrategy::streaming(256 * 1024);
        assert!(!strategy.is_bounded_memory());
        assert!(strategy.is_streaming());
    }

    #[test]
    fn test_fragment_triggers_new() {
        let triggers = FragmentTriggers::new();
        assert!(!triggers.has_triggers());
        assert!(triggers.frame_count.is_none());
        assert!(triggers.memory_bytes.is_none());
        assert!(triggers.duration_secs.is_none());
    }

    #[test]
    fn test_fragment_triggers_frame_count() {
        let triggers = FragmentTriggers::frame_count(100);
        assert!(triggers.has_triggers());
        assert_eq!(triggers.frame_count, Some(100));
    }

    #[test]
    fn test_fragment_triggers_memory_bytes() {
        let triggers = FragmentTriggers::memory_bytes(1024 * 1024);
        assert!(triggers.has_triggers());
        assert_eq!(triggers.memory_bytes, Some(1024 * 1024));
    }

    #[test]
    fn test_fragment_triggers_duration_secs() {
        let triggers = FragmentTriggers::duration_secs(5.0);
        assert!(triggers.has_triggers());
        assert_eq!(triggers.duration_secs, Some(5.0));
    }

    #[test]
    fn test_fragment_triggers_chaining() {
        let triggers = FragmentTriggers::new()
            .with_frame_count(100)
            .with_memory_bytes(1024 * 1024)
            .with_duration_secs(5.0);

        assert!(triggers.has_triggers());
        assert_eq!(triggers.frame_count, Some(100));
        assert_eq!(triggers.memory_bytes, Some(1024 * 1024));
        assert_eq!(triggers.duration_secs, Some(5.0));
    }

    #[test]
    fn test_encoding_strategy_default() {
        let strategy = EncodingStrategy::default();
        assert!(matches!(strategy, EncodingStrategy::Standard));
    }

    #[test]
    fn test_fragment_triggers_default() {
        let triggers = FragmentTriggers::default();
        assert!(!triggers.has_triggers());
    }
}
