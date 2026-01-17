//! Kps writer stage for the pipeline.
//!
//! This stage receives aligned frames and writes them to Kps format
//! using the configured writer backend.

use std::collections::HashMap;
use std::path::PathBuf;
use std::thread;
use std::time::Instant;

use crossbeam_channel::Receiver;
use tracing::{debug, info};

use crate::core::Result;
use crate::io::kps::{AlignedFrame, CameraParamCollector, KpsConfig, KpsWriter, WriterStats};

/// Configuration for the Kps writer stage.
#[derive(Debug, Clone)]
pub struct KpsWriterStageConfig {
    /// Kps dataset configuration.
    pub kps_config: KpsConfig,

    /// Output directory path.
    pub output_dir: PathBuf,

    /// Episode ID.
    pub episode_id: usize,
}

impl KpsWriterStageConfig {
    /// Create a new writer stage config.
    pub fn new(output_dir: PathBuf, episode_id: usize, kps_config: KpsConfig) -> Self {
        Self {
            kps_config,
            output_dir,
            episode_id,
        }
    }
}

/// Statistics from the Kps writer stage.
#[derive(Debug, Clone)]
pub struct KpsWriterStageStats {
    /// Number of frames written.
    pub frames_written: usize,

    /// Number of images encoded.
    pub images_encoded: usize,

    /// Number of state records written.
    pub state_records: usize,

    /// Processing duration in seconds.
    pub duration_sec: f64,
}

/// Kps writer stage for the pipeline.
///
/// This stage receives aligned frames through a channel and writes them
/// to Kps format using the configured writer backend.
pub struct KpsWriterStage {
    /// Stage configuration.
    config: KpsWriterStageConfig,

    /// Channel for receiving aligned frames.
    receiver: Receiver<StageMessage>,

    /// The underlying writer.
    writer: Option<Box<dyn KpsWriter>>,

    /// Camera parameters (if extraction was enabled).
    camera_params: Option<CameraParamCollector>,
}

/// Messages sent to the writer stage.
pub enum StageMessage {
    /// An aligned frame to write.
    Frame(AlignedFrame),

    /// Camera parameters to include.
    CameraParams(CameraParamCollector),

    /// Signal to finalize.
    Finalize,
}

impl KpsWriterStage {
    /// Create a new Kps writer stage.
    pub fn new(config: KpsWriterStageConfig, receiver: Receiver<StageMessage>) -> Self {
        Self {
            config,
            receiver,
            writer: None,
            camera_params: None,
        }
    }

    /// Initialize the writer with channel information.
    fn initialize_writer(
        &mut self,
        channels: &HashMap<u16, crate::io::metadata::ChannelInfo>,
    ) -> Result<()> {
        use crate::core::CodecError;
        use crate::io::kps::create_writer;

        let mut writer = create_writer(
            &self.config.output_dir,
            self.config.episode_id,
            &self.config.kps_config,
        )
        .map_err(|e| CodecError::encode("KpsWriter", e.to_string()))?;

        writer
            .initialize(&self.config.kps_config, channels)
            .map_err(|e| CodecError::encode("KpsWriter", e.to_string()))?;

        self.writer = Some(writer);
        Ok(())
    }

    /// Run the writer stage.
    ///
    /// This method should be called in a separate thread. It processes
    /// messages from the channel until it receives a Finalize signal.
    pub fn run(
        mut self,
        channels: HashMap<u16, crate::io::metadata::ChannelInfo>,
    ) -> Result<KpsWriterStageStats> {
        let start = Instant::now();

        info!(
            output_dir = %self.config.output_dir.display(),
            episode_id = self.config.episode_id,
            "Starting Kps writer stage"
        );

        // Initialize the writer
        self.initialize_writer(&channels)?;

        let mut frames_written = 0;

        // Process messages
        while let Ok(msg) = self.receiver.recv() {
            match msg {
                StageMessage::Frame(frame) => {
                    if let Some(ref mut writer) = self.writer {
                        writer.write_frame(&frame).map_err(|e| {
                            crate::core::CodecError::encode("KpsWriter", e.to_string())
                        })?;
                        frames_written += 1;
                    }

                    if frames_written % 100 == 0 {
                        debug!(frames_written, "Kps writer progress");
                    }
                }
                StageMessage::CameraParams(params) => {
                    self.camera_params = Some(params);
                }
                StageMessage::Finalize => {
                    break;
                }
            }
        }

        // Finalize the writer
        let stats = if let Some(writer) = self.writer.as_mut() {
            writer
                .finalize(&self.config.kps_config, self.camera_params.as_ref())
                .map_err(|e| crate::core::CodecError::encode("KpsWriter", e.to_string()))?
        } else {
            WriterStats::default()
        };

        let duration = start.elapsed();

        info!(
            frames_written = stats.frames_written,
            images_encoded = stats.images_encoded,
            duration_sec = duration.as_secs_f64(),
            "Kps writer stage complete"
        );

        Ok(KpsWriterStageStats {
            frames_written: stats.frames_written,
            images_encoded: stats.images_encoded,
            state_records: stats.state_records,
            duration_sec: duration.as_secs_f64(),
        })
    }

    /// Spawn the writer stage in a new thread.
    pub fn spawn(
        self,
        channels: HashMap<u16, crate::io::metadata::ChannelInfo>,
    ) -> Result<thread::JoinHandle<Result<KpsWriterStageStats>>> {
        let handle = thread::spawn(move || self.run(channels));
        Ok(handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stage_config() {
        let config = KpsWriterStageConfig {
            kps_config: KpsConfig {
                dataset: crate::io::kps::DatasetConfig {
                    name: "test".to_string(),
                    fps: 30,
                    robot_type: None,
                },
                mappings: vec![],
                output: crate::io::kps::OutputConfig::default(),
            },
            output_dir: std::path::PathBuf::from("/tmp"),
            episode_id: 0,
        };

        assert_eq!(config.episode_id, 0);
        assert_eq!(config.output_dir, std::path::PathBuf::from("/tmp"));
    }
}
