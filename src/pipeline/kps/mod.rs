//! Kps pipeline integration.
//!
//! This module provides pipeline stages and orchestration for converting
//! robotics data to Kps dataset format with streaming support.
//!
//! # Fluent API
//!
//! The module provides a fluent API for convenient conversion:
//!
//! ```no_run
//! use robocodec::pipeline::kps::KpsConverter;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Simple conversion
//!     let _report = KpsConverter::new("input.mcap", "output_dir")
//!         .config("config.toml")
//!         .run()?;
//!
//!     // V1.2 delivery with statistics tracking
//!     let _report = KpsConverter::new("input.mcap", "output_dir")
//!         .config("config.toml")
//!         .v12_delivery()
//!         .robot("Kuavo4Pro")
//!         .end_effector("Dexhand")
//!         .scene("Housekeeper")
//!         .sub_scene("Kitchen")
//!         .task("Dispose_of_takeout_containers")
//!         .with_statistics()
//!         .run()?;
//!     Ok(())
//! }
//! ```

pub mod config;
pub mod fluent;
pub mod stages;
pub mod traits;

pub use config::{KpsPipelineConfig, TimeAlignerConfig};
pub use fluent::{convert_to_kps, KpsConverter};
pub use stages::{KpsWriterStage, KpsWriterStageConfig};
pub use traits::time_alignment::{
    HoldLastValue, LinearInterpolation, NearestNeighbor, TemporalBuffer,
    TimeAlignerConfig as TimeAlignmentConfig, TimeAlignmentStrategy, TimeAlignmentStrategyType,
};

use std::collections::HashMap;
use std::path::Path;
use std::thread;
use std::time::Instant;

use crossbeam_channel::{bounded, Receiver, Sender};
use tracing::{info, warn};

use crate::core::{CodecError, Result};
use crate::format::reader::McapReader;
use crate::io::kps::{create_writer, AlignedFrame, CameraParamCollector};
use crate::io::metadata::ChannelInfo;

#[allow(unused_imports)]

/// Report from a Kps pipeline execution.
#[derive(Debug, Clone)]
pub struct KpsReport {
    /// Number of frames written.
    pub frames_written: usize,

    /// Number of images encoded.
    pub images_encoded: usize,

    /// Number of state records written.
    pub state_records: usize,

    /// Total processing duration.
    pub duration_sec: f64,

    /// Output directory path.
    pub output_dir: String,
}

/// Kps conversion pipeline.
///
/// This pipeline converts MCAP/BAG data to Kps format with streaming support,
/// time alignment, and camera parameter extraction.
pub struct KpsPipeline {
    /// Pipeline configuration.
    config: KpsPipelineConfig,

    /// Input file path.
    input_path: std::path::PathBuf,

    /// Output directory path.
    output_dir: std::path::PathBuf,
}

impl KpsPipeline {
    /// Create a new Kps pipeline.
    pub fn new(
        input_path: impl AsRef<Path>,
        output_dir: impl AsRef<Path>,
        config: KpsPipelineConfig,
    ) -> Result<Self> {
        let input_path = input_path.as_ref();
        let output_dir = output_dir.as_ref();

        if !input_path.exists() {
            return Err(CodecError::parse(
                "KpsPipeline",
                format!("Input file not found: {}", input_path.display()),
            ));
        }

        // Create output directory
        std::fs::create_dir_all(output_dir).map_err(|e| {
            CodecError::encode(
                "KpsPipeline",
                format!("Failed to create output directory: {}", e),
            )
        })?;

        Ok(Self {
            input_path: input_path.to_path_buf(),
            output_dir: output_dir.to_path_buf(),
            config,
        })
    }

    /// Run the pipeline and return the report.
    pub fn run(self) -> Result<KpsReport> {
        let start = Instant::now();

        info!(
            input = %self.input_path.display(),
            output = %self.output_dir.display(),
            fps = self.config.kps_config.dataset.fps,
            "Starting Kps conversion"
        );

        // Create channels for pipeline communication
        let capacity = self.config.channel_capacity;
        let (decoded_sender, decoded_receiver) = bounded::<DecodedMessage>(capacity);
        let (aligned_sender, _aligned_receiver) = bounded::<AlignedFrame>(capacity);

        // Get channel info from input file
        let reader = McapReader::open(&self.input_path)?;
        let channels: HashMap<u16, ChannelInfo> = reader
            .channels()
            .iter()
            .map(|(id, info)| {
                (
                    *id,
                    ChannelInfo {
                        id: *id,
                        topic: info.topic.clone(),
                        message_type: info.message_type.clone(),
                        encoding: info.encoding.clone(),
                        schema: info.schema.clone(),
                        schema_data: info.schema_data.clone(),
                        schema_encoding: info.schema_encoding.clone(),
                        message_count: info.message_count,
                        callerid: info.callerid.clone(),
                    },
                )
            })
            .collect();

        // Spawn camera extractor in parallel
        let camera_handle = if self.config.camera_extractor.enabled {
            Some(self.spawn_camera_extractor(&self.input_path, channels.clone())?)
        } else {
            None
        };

        // Spawn time alignment stage
        let time_aligner_handle = self.spawn_time_aligner(
            decoded_receiver,
            aligned_sender,
            self.config.time_aligner.clone(),
        )?;

        // Spawn writer stage
        let mut writer = create_writer(&self.output_dir, 0, &self.config.kps_config)
            .map_err(|e| CodecError::encode("KpsWriter", e.to_string()))?;

        writer
            .initialize(&self.config.kps_config, &channels)
            .map_err(|e| CodecError::encode("KpsWriter", e.to_string()))?;

        // Decode messages from input file
        let _decode_result = self.decode_messages(reader, decoded_sender);

        // Wait for stages to complete
        let _ = time_aligner_handle.join().map_err(|e| {
            CodecError::encode("KpsPipeline", format!("Time aligner panicked: {:?}", e))
        })??;

        // Collect camera parameters
        let camera_params = if let Some(handle) = camera_handle {
            match handle.join().map_err(|e| {
                CodecError::encode("KpsPipeline", format!("Camera extractor panicked: {:?}", e))
            })? {
                Ok(inner) => inner,
                Err(e) => {
                    warn!("Failed to extract camera parameters: {}", e);
                    None
                }
            }
        } else {
            None
        };

        // Finalize writer
        let stats = writer
            .finalize(&self.config.kps_config, camera_params.as_ref())
            .map_err(|e| CodecError::encode("KpsWriter", e.to_string()))?;

        let duration = start.elapsed();

        info!(
            frames = stats.frames_written,
            images = stats.images_encoded,
            duration_sec = duration.as_secs_f64(),
            "Kps conversion complete"
        );

        Ok(KpsReport {
            frames_written: stats.frames_written,
            images_encoded: stats.images_encoded,
            state_records: stats.state_records,
            duration_sec: duration.as_secs_f64(),
            output_dir: self.output_dir.display().to_string(),
        })
    }

    /// Spawn the camera parameter extraction task.
    fn spawn_camera_extractor(
        &self,
        input_path: &std::path::Path,
        _channels: HashMap<u16, ChannelInfo>,
    ) -> Result<std::thread::JoinHandle<Result<Option<CameraParamCollector>>>> {
        use crate::format::reader::McapReader;

        let input_path = input_path.to_path_buf();
        let camera_topics = self.config.camera_extractor.camera_topics.clone();
        let parent_frame = self.config.camera_extractor.parent_frame.clone();

        let handle = thread::spawn(move || {
            // Open MCAP reader for camera extraction
            let reader = McapReader::open(&input_path).map_err(|e| {
                CodecError::parse(
                    "CameraExtractor",
                    format!("Failed to open MCAP for camera extraction: {}", e),
                )
            })?;

            let mut collector = CameraParamCollector::new();

            // Extract camera parameters if camera topics are configured
            if !camera_topics.is_empty() {
                collector
                    .extract_from_mcap(&reader, camera_topics, &parent_frame)
                    .map_err(|e| {
                        CodecError::parse(
                            "CameraExtractor",
                            format!("Failed to extract camera parameters: {}", e),
                        )
                    })?;

                info!(
                    cameras = collector.camera_names().len(),
                    "Camera parameters extracted"
                );

                Ok(Some(collector))
            } else {
                info!("No camera topics configured, skipping camera extraction");
                Ok(None)
            }
        });

        Ok(handle)
    }

    /// Spawn the time alignment stage.
    fn spawn_time_aligner(
        &self,
        decoded_receiver: Receiver<DecodedMessage>,
        aligned_sender: Sender<AlignedFrame>,
        config: TimeAlignerConfig,
    ) -> Result<std::thread::JoinHandle<Result<()>>> {
        // Pass the mappings to the time aligner
        let mappings = self.config.kps_config.mappings.clone();

        let handle = thread::spawn(move || {
            Self::run_time_aligner(decoded_receiver, aligned_sender, config, mappings)
        });
        Ok(handle)
    }

    /// Run the time alignment stage.
    fn run_time_aligner(
        decoded_receiver: Receiver<DecodedMessage>,
        aligned_sender: Sender<AlignedFrame>,
        config: TimeAlignerConfig,
        mappings: Vec<crate::io::kps::Mapping>,
    ) -> Result<()> {
        use crate::io::kps::writers::base::{ImageData, MessageExtractor};
        use crate::io::kps::MappingType;

        let strategy = config.strategy.create();

        // Build topic to mappings lookup
        let mut topic_mappings: HashMap<String, Vec<&crate::io::kps::Mapping>> = HashMap::new();
        for mapping in &mappings {
            topic_mappings
                .entry(mapping.topic.clone())
                .or_insert_with(Vec::new)
                .push(mapping);
        }

        // Buffer messages by topic
        let mut message_buffers: HashMap<String, Vec<(u64, DecodedMessage)>> = HashMap::new();

        // Track time bounds
        let mut start_time = None;
        let mut end_time = None;
        let mut frame_index = 0;

        // First pass: collect all messages to determine time bounds
        while let Ok(msg) = decoded_receiver.recv() {
            let entry = (msg.timestamp, msg);

            if start_time.is_none() || Some(entry.0) < start_time {
                start_time = Some(entry.0);
            }
            if end_time.is_none() || Some(entry.0) > end_time {
                end_time = Some(entry.0);
            }

            message_buffers
                .entry(entry.1.topic.clone())
                .or_insert_with(Vec::new)
                .push(entry);
        }

        let (Some(start), Some(end)) = (start_time, end_time) else {
            warn!("No messages received for time alignment");
            return Ok(());
        };

        // Generate target timestamps
        let target_times = strategy.generate_target_timestamps(start, end, config.target_fps)?;

        // For each target timestamp, create an aligned frame
        for target_time in target_times {
            let mut frame = AlignedFrame::new(frame_index, target_time);

            // Process each mapped topic
            for (topic, messages) in &message_buffers {
                // Skip if no mappings for this topic
                let Some(mappings_for_topic) = topic_mappings.get(topic) else {
                    continue;
                };

                // Find the nearest message
                if let Some((msg_time, msg)) = messages.iter().min_by_key(|(t, _)| {
                    if t > &target_time {
                        t - target_time
                    } else {
                        target_time - t
                    }
                }) {
                    // Check if within tolerance
                    let dist = if *msg_time > target_time {
                        *msg_time - target_time
                    } else {
                        target_time - *msg_time
                    };

                    // Use different tolerances for different data types
                    for mapping in mappings_for_topic {
                        let tolerance = match mapping.mapping_type {
                            MappingType::Image => config.image_sync_tolerance_ns,
                            MappingType::State
                            | MappingType::Action
                            | MappingType::Timestamp
                            | MappingType::OtherSensor
                            | MappingType::Audio => config.state_interpolation_max_gap_ns,
                        };

                        if dist > tolerance {
                            continue;
                        }

                        // Extract data based on mapping type
                        match &mapping.mapping_type {
                            MappingType::Image => {
                                if let Some(image_data) = MessageExtractor::extract_image(&msg.data)
                                {
                                    frame.add_image(
                                        mapping.feature.clone(),
                                        ImageData {
                                            width: image_data.width,
                                            height: image_data.height,
                                            data: image_data.data,
                                            original_timestamp: *msg_time,
                                            is_encoded: image_data.is_encoded,
                                        },
                                    );
                                }
                            }
                            MappingType::State | MappingType::Action | MappingType::OtherSensor => {
                                if let Ok(values) = MessageExtractor::extract_float_array(&msg.data)
                                {
                                    if mapping.feature.starts_with("observation.") {
                                        frame.add_state(mapping.feature.clone(), values);
                                    } else if mapping.feature.starts_with("action.") {
                                        frame.add_action(mapping.feature.clone(), values);
                                    }
                                }
                            }
                            MappingType::Timestamp => {
                                frame.add_timestamp(mapping.feature.clone(), *msg_time);
                            }
                            MappingType::Audio => {
                                // Audio data is handled separately - extract for metadata
                                if let Ok(values) = MessageExtractor::extract_float_array(&msg.data)
                                {
                                    // Store as state for now - audio writer handles conversion
                                    frame.add_state(mapping.feature.clone(), values);
                                }
                            }
                        }
                    }
                }
            }

            // Only send non-empty frames
            if !frame.is_empty() {
                aligned_sender.send(frame).map_err(|e| {
                    CodecError::encode("KpsPipeline", format!("Channel send error: {}", e))
                })?;
                frame_index += 1;
            }
        }

        info!(frames = frame_index, "Time alignment complete");

        Ok(())
    }

    /// Decode messages from the input file.
    fn decode_messages(&self, reader: McapReader, sender: Sender<DecodedMessage>) -> Result<()> {
        // Use decode_messages_with_timestamp to get timestamps
        for result in reader.decode_messages_with_timestamp()? {
            let (timestamped_msg, channel_info) = result?;

            let decoded = DecodedMessage {
                timestamp: timestamped_msg.log_time,
                topic: channel_info.topic.clone(),
                data: timestamped_msg.message.into_iter().collect(),
            };

            sender.send(decoded).map_err(|e| {
                CodecError::encode("KpsPipeline", format!("Channel send error: {}", e))
            })?;
        }
        Ok(())
    }
}

/// A decoded message ready for time alignment.
#[derive(Debug, Clone)]
pub struct DecodedMessage {
    /// Message timestamp (nanoseconds).
    pub timestamp: u64,

    /// Topic name.
    pub topic: String,

    /// Decoded message fields.
    pub data: Vec<(String, crate::CodecValue)>,
}
