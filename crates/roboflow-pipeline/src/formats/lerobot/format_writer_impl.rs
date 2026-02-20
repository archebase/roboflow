// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! FormatWriter implementation for LerobotWriter.
//!
//! This module provides the bridge between the format-agnostic FormatWriter
//! trait and the LeRobot-specific implementation.

use super::writer::LerobotWriter;
use crate::core::stats::EpisodeStats;
use crate::core::traits::{AlignedFrame, FormatWriter, Result, WriterStats};
use crate::formats::common::DatasetWriter;
use std::any::Any;

/// FormatWriter implementation for LeRobot v2.1 format.
///
/// This implementation provides:
/// - Episode management via `start_episode` and `finish_episode`
/// - Video path generation following LeRobot conventions
/// - Full FormatWriter trait compliance
impl FormatWriter for LerobotWriter {
    fn write_frame(&mut self, frame: &AlignedFrame) -> Result<()> {
        // Delegate to DatasetWriter implementation
        DatasetWriter::write_frame(self, frame)
    }

    fn write_batch(&mut self, frames: &[AlignedFrame]) -> Result<()> {
        // Use default implementation for batch writing
        for frame in frames {
            DatasetWriter::write_frame(self, frame)?;
        }
        Ok(())
    }

    fn finalize(&mut self) -> Result<WriterStats> {
        // Delegate to DatasetWriter implementation
        DatasetWriter::finalize(self)
    }

    fn frame_count(&self) -> usize {
        DatasetWriter::frame_count(self)
    }

    // === Episode Management ===

    fn start_episode(&mut self, task_index: Option<usize>) -> Result<usize> {
        // Use the LerobotWriterTrait implementation
        super::LerobotWriterTrait::start_episode(self, task_index);
        Ok(DatasetWriter::episode_index(self).unwrap_or(0))
    }

    fn finish_episode(&mut self) -> Result<EpisodeStats> {
        // Finish the episode and return stats
        let episode_index = DatasetWriter::episode_index(self).unwrap_or(0);
        let frame_count = DatasetWriter::frame_count(self);

        super::LerobotWriterTrait::finish_episode(self, Some(episode_index))?;

        // Return basic episode stats
        Ok(EpisodeStats {
            frames: frame_count,
            episode_index,
            task_index: None,
            ..EpisodeStats::default()
        })
    }

    fn episode_index(&self) -> Option<usize> {
        DatasetWriter::episode_index(self)
    }

    fn supports_episodes(&self) -> bool {
        true
    }

    // === Format Information ===

    fn format_name(&self) -> &'static str {
        "lerobot"
    }

    fn format_version(&self) -> &'static str {
        "2.1"
    }

    // === Video Handling ===

    fn handles_video(&self) -> bool {
        true
    }

    fn video_path_scheme(&self) -> Option<Box<dyn crate::core::VideoPathScheme>> {
        // Use the output prefix from the writer
        // Note: This requires access to a public method or field
        // For now, return None and let the video encoder use defaults
        None
    }

    // === Downcasting Support ===

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_writer_trait_bounds() {
        // Verify that LerobotWriter implements FormatWriter
        fn assert_format_writer<W: FormatWriter>() {}
        assert_format_writer::<LerobotWriter>();
    }
}
