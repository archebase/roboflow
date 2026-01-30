// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Streaming dataset converter with bounded memory footprint.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use tracing::{info, instrument, warn};

use crate::RoboReader;
use crate::core::Result;
use crate::dataset::DatasetFormat;
use crate::dataset::common::DatasetWriter;
use crate::dataset::streaming::{
    BackpressureHandler, FrameAlignmentBuffer, StreamingConfig, StreamingStats,
};

/// Streaming dataset converter.
///
/// Converts input files (MCAP/Bag) directly to dataset formats using
/// a streaming architecture with bounded memory footprint.
pub struct StreamingDatasetConverter {
    /// Output directory
    output_dir: PathBuf,

    /// Dataset format
    format: DatasetFormat,

    /// Configuration for KPS format
    kps_config: Option<crate::dataset::kps::config::KpsConfig>,

    /// Configuration for LeRobot format
    lerobot_config: Option<crate::dataset::lerobot::config::LerobotConfig>,

    /// Streaming configuration
    config: StreamingConfig,
}

impl StreamingDatasetConverter {
    /// Create a new streaming converter for KPS format.
    pub fn new_kps<P: AsRef<Path>>(
        output_dir: P,
        kps_config: crate::dataset::kps::config::KpsConfig,
        config: StreamingConfig,
    ) -> Result<Self> {
        Ok(Self {
            output_dir: output_dir.as_ref().to_path_buf(),
            format: DatasetFormat::Kps,
            kps_config: Some(kps_config),
            lerobot_config: None,
            config,
        })
    }

    /// Create a new streaming converter for LeRobot format.
    pub fn new_lerobot<P: AsRef<Path>>(
        output_dir: P,
        lerobot_config: crate::dataset::lerobot::config::LerobotConfig,
    ) -> Result<Self> {
        let fps = lerobot_config.dataset.fps;
        Ok(Self {
            output_dir: output_dir.as_ref().to_path_buf(),
            format: DatasetFormat::Lerobot,
            kps_config: None,
            lerobot_config: Some(lerobot_config),
            config: StreamingConfig::with_fps(fps),
        })
    }

    /// Set the completion window (in frames).
    pub fn with_completion_window(mut self, frames: usize) -> Self {
        self.config.completion_window_frames = frames;
        self
    }

    /// Set the maximum buffered frames.
    pub fn with_max_buffered_frames(mut self, max: usize) -> Self {
        self.config.max_buffered_frames = max;
        self
    }

    /// Set the maximum buffered memory (in MB).
    pub fn with_max_memory_mb(mut self, mb: usize) -> Self {
        self.config.max_buffered_memory_mb = mb;
        self
    }

    /// Convert input file to dataset format.
    #[instrument(skip_all, fields(
        input = %input_path.as_ref().display(),
        output = %self.output_dir.display(),
        format = ?self.format,
    ))]
    pub fn convert<P: AsRef<Path>>(self, input_path: P) -> Result<StreamingStats> {
        let input_path = input_path.as_ref();

        info!(
            input = %input_path.display(),
            output = %self.output_dir.display(),
            format = ?self.format,
            "Starting streaming dataset conversion"
        );

        let start_time = Instant::now();

        // Create the dataset writer
        let mut writer = self.create_writer()?;
        writer.initialize(self.get_config_any()?)?;

        // Create alignment buffer
        let mut aligner = FrameAlignmentBuffer::new(self.config.clone());

        // Create backpressure handler
        let mut backpressure = BackpressureHandler::from_config(&self.config);

        // Build topic mappings
        let topic_mappings = self.build_topic_mappings()?;

        // Open input file
        // NOTE: RoboReader decodes BAG/MCAP files directly to TimestampedDecodedMessage.
        // There is NO intermediate MCAP conversion - neither in memory nor on disk.
        // BAG format is parsed natively, messages are decoded directly to HashMap<String, CodecValue>.
        let reader = RoboReader::open(input_path)?;

        info!(
            mappings = topic_mappings.len(),
            "Starting message processing"
        );

        // Stream messages
        let mut stats = StreamingStats::default();
        let mut unmapped_warning_shown: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        for result in reader.decode_messages_with_timestamp()? {
            let (timestamped_msg, channel) = result?;
            stats.messages_processed += 1;

            // Find mapping for this topic
            let mapping = match topic_mappings.get(&channel.topic) {
                Some(m) => m,
                None => {
                    // Log warning once per unmapped topic to avoid spam
                    if unmapped_warning_shown.insert(channel.topic.clone()) {
                        tracing::warn!(
                            topic = %channel.topic,
                            "Message from unmapped topic will be ignored. Add this topic to your configuration if needed."
                        );
                    }
                    aligner.stats_mut().record_unmapped_message();
                    continue;
                }
            };

            // Convert to our TimestampedMessage type
            let msg = crate::dataset::streaming::alignment::TimestampedMessage {
                log_time: timestamped_msg.log_time,
                message: timestamped_msg.message,
            };

            // Process message through alignment buffer
            let completed_frames = aligner.process_message(&msg, &mapping.feature);

            // Write completed frames immediately
            for frame in completed_frames {
                writer.write_frame(&frame)?;
                stats.frames_written += 1;

                // Update memory estimate
                backpressure.update_memory_estimate(&aligner);
            }

            // Apply backpressure if needed
            if backpressure.should_apply_backpressure(&aligner) && !backpressure.is_in_cooldown() {
                info!(
                    buffer_size = aligner.len(),
                    memory_mb = backpressure.memory_mb(),
                    "Applying backpressure"
                );

                let force_completed = aligner.flush();
                for frame in force_completed {
                    writer.write_frame(&frame)?;
                    stats.frames_written += 1;
                    stats.force_completed_frames += 1;
                }

                backpressure.record_backpressure();
            }

            // Progress reporting every 1000 messages
            if stats.messages_processed % 1000 == 0 {
                let elapsed = start_time.elapsed().as_secs_f64();
                let throughput = stats.messages_processed as f64 / elapsed;
                info!(
                    messages = stats.messages_processed,
                    frames = stats.frames_written,
                    buffer = aligner.len(),
                    throughput = format!("{:.0} msg/s", throughput),
                    "Progress update"
                );
            }
        }

        // Flush remaining frames
        info!(
            remaining_frames = aligner.len(),
            "Flushing remaining frames"
        );

        let remaining = aligner.flush();
        for frame in remaining {
            writer.write_frame(&frame)?;
            stats.frames_written += 1;
            stats.force_completed_frames += 1;
        }

        // Finalize writer
        let writer_stats = writer.finalize(self.get_config_any()?)?;

        // Compile final statistics
        stats.duration_sec = start_time.elapsed().as_secs_f64();
        stats.writer_stats = writer_stats;
        stats.avg_buffer_size = aligner.stats().peak_buffer_size as f32;
        stats.peak_memory_mb = backpressure.memory_mb();

        info!(
            frames_written = stats.frames_written,
            messages = stats.messages_processed,
            duration_sec = stats.duration_sec,
            throughput_fps = stats.throughput_fps(),
            "Streaming conversion complete"
        );

        Ok(stats)
    }

    /// Create the appropriate dataset writer.
    fn create_writer(&self) -> Result<Box<dyn DatasetWriter>> {
        use crate::dataset::create_dataset_writer;

        match self.format {
            DatasetFormat::Kps => {
                let config = self.kps_config.as_ref().ok_or_else(|| {
                    crate::RoboflowError::parse(
                        "StreamingConverter",
                        "KPS config required but not provided",
                    )
                })?;
                create_dataset_writer(self.format, &self.output_dir, config).map_err(|e| {
                    crate::RoboflowError::encode(
                        "StreamingConverter",
                        format!(
                            "Failed to create KPS writer at {}: {}",
                            self.output_dir.display(),
                            e
                        ),
                    )
                })
            }
            DatasetFormat::Lerobot => {
                let config = self.lerobot_config.as_ref().ok_or_else(|| {
                    crate::RoboflowError::parse(
                        "StreamingConverter",
                        "LeRobot config required but not provided",
                    )
                })?;
                create_dataset_writer(self.format, &self.output_dir, config).map_err(|e| {
                    crate::RoboflowError::encode(
                        "StreamingConverter",
                        format!(
                            "Failed to create LeRobot writer at {}: {}",
                            self.output_dir.display(),
                            e
                        ),
                    )
                })
            }
        }
    }

    /// Get the config as `dyn Any` for passing to writer.
    fn get_config_any(&self) -> Result<&dyn std::any::Any> {
        if let Some(kps_config) = &self.kps_config {
            Ok(kps_config)
        } else if let Some(lerobot_config) = &self.lerobot_config {
            Ok(lerobot_config)
        } else {
            Err(crate::RoboflowError::parse(
                "StreamingConverter",
                "No config available",
            ))
        }
    }

    /// Build topic -> feature mapping lookup.
    fn build_topic_mappings(&self) -> Result<MappingMap> {
        let mut map = HashMap::new();

        match self.format {
            DatasetFormat::Kps => {
                if let Some(config) = &self.kps_config {
                    for mapping in &config.mappings {
                        map.insert(
                            mapping.topic.clone(),
                            Mapping {
                                feature: mapping.feature.clone(),
                                mapping_type: match mapping.mapping_type {
                                    crate::dataset::kps::MappingType::Image => "image",
                                    crate::dataset::kps::MappingType::State => "state",
                                    crate::dataset::kps::MappingType::Action => "action",
                                    _ => "state",
                                },
                            },
                        );
                    }
                }
            }
            DatasetFormat::Lerobot => {
                if let Some(config) = &self.lerobot_config {
                    for mapping in &config.mappings {
                        map.insert(
                            mapping.topic.clone(),
                            Mapping {
                                feature: mapping.feature.clone(),
                                mapping_type: match mapping.mapping_type {
                                    crate::dataset::lerobot::config::MappingType::Image => "image",
                                    crate::dataset::lerobot::config::MappingType::State => "state",
                                    crate::dataset::lerobot::config::MappingType::Action => {
                                        "action"
                                    }
                                    crate::dataset::lerobot::config::MappingType::Timestamp => {
                                        "timestamp"
                                    }
                                },
                            },
                        );
                    }
                }
            }
        }

        Ok(map)
    }
}

/// Topic mapping for looking up feature names.
type MappingMap = HashMap<String, Mapping>;

/// Mapping from topic to feature.
#[derive(Debug, Clone)]
struct Mapping {
    feature: String,
    /// Data type for validation/routing (reserved for future use)
    /// Values: "image", "state", "action", "timestamp"
    #[allow(dead_code)]
    mapping_type: &'static str,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_converter_creation() {
        // Basic test that the converter can be created
        let lerobot_config = crate::dataset::lerobot::config::LerobotConfig {
            dataset: crate::dataset::lerobot::config::DatasetConfig {
                name: "test".to_string(),
                fps: 30,
                robot_type: None,
                env_type: None,
            },
            mappings: vec![],
            video: Default::default(),
            annotation_file: None,
        };

        let converter = StreamingDatasetConverter::new_lerobot("/tmp/test", lerobot_config);

        assert!(converter.is_ok());
    }
}
