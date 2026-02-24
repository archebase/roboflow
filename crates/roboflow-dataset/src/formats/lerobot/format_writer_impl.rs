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
    use crate::formats::common::AlignedFrame;
    use crate::formats::common::config::DatasetBaseConfig;
    use crate::formats::lerobot::config::{
        DatasetConfig, FlushingConfig, LerobotConfig, StreamingConfig, VideoConfig,
    };

    fn test_config() -> LerobotConfig {
        LerobotConfig {
            dataset: DatasetConfig {
                base: DatasetBaseConfig {
                    name: "format_writer_impl_tests".to_string(),
                    fps: 30,
                    robot_type: None,
                },
                env_type: None,
            },
            mappings: Vec::new(),
            video: VideoConfig::default(),
            annotation_file: None,
            flushing: FlushingConfig::default(),
            streaming: StreamingConfig::default(),
        }
    }

    fn make_stateful_frame(index: usize) -> AlignedFrame {
        let mut frame = AlignedFrame::new(index, (index as u64) * 33_333_333);
        frame.add_state("observation.state".to_string(), vec![index as f32, 1.0]);
        frame.add_action("action".to_string(), vec![0.1, 0.2]);
        frame
    }

    #[test]
    fn test_format_writer_trait_bounds() {
        // Verify that LerobotWriter implements FormatWriter
        fn assert_format_writer<W: FormatWriter>() {}
        assert_format_writer::<LerobotWriter>();
    }

    #[test]
    fn test_format_writer_methods_smoke() {
        let temp = tempfile::tempdir().expect("create temp dir");
        let mut writer =
            LerobotWriter::new_local(temp.path(), test_config()).expect("create writer");

        assert_eq!(FormatWriter::format_name(&writer), "lerobot");
        assert_eq!(FormatWriter::format_version(&writer), "2.1");
        assert!(FormatWriter::supports_episodes(&writer));
        assert!(FormatWriter::handles_video(&writer));
        assert!(FormatWriter::video_path_scheme(&writer).is_none());

        let episode = FormatWriter::start_episode(&mut writer, Some(0)).expect("start episode");
        assert_eq!(episode, 0);
        assert_eq!(FormatWriter::episode_index(&writer), Some(0));

        let frames = vec![make_stateful_frame(0), make_stateful_frame(1)];
        FormatWriter::write_batch(&mut writer, &frames).expect("write batch");
        assert_eq!(FormatWriter::frame_count(&writer), 2);

        let stats = FormatWriter::finish_episode(&mut writer).expect("finish episode");
        assert_eq!(stats.frames, 2);
        assert_eq!(stats.episode_index, 0);

        let final_stats = FormatWriter::finalize(&mut writer).expect("finalize writer");
        assert_eq!(final_stats.frames_written, 2);

        assert!(FormatWriter::as_any(&writer).is::<LerobotWriter>());
        assert!(FormatWriter::as_any_mut(&mut writer).is::<LerobotWriter>());
    }
}
