// Video encoder stage - streaming MP4 encoding via ffmpeg stdin

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use crossbeam_channel::{Receiver, Sender};

use crate::streaming::pipeline::types::{
    DatasetFrame, EncodedVideo, PipelineError, PipelineResult,
};

/// Statistics from the video encoder stage.
#[derive(Debug, Clone)]
pub struct VideoEncoderStats {
    /// Frames processed
    pub frames_processed: usize,
    /// Videos produced
    pub videos_produced: usize,
    /// Total frames encoded
    pub frames_encoded: usize,
    /// Processing time in seconds
    pub duration_sec: f64,
}

/// Video encoder stage configuration.
#[derive(Debug, Clone)]
pub struct VideoEncoderConfig {
    /// Video codec (default: libx264)
    pub codec: String,
    /// Pixel format (default: yuv420p)
    pub pixel_format: String,
    /// Frame rate for output video
    pub fps: u32,
    /// CRF quality value (0-51, lower = better)
    pub crf: u32,
    /// Encoder preset
    pub preset: String,
    /// Number of encoding threads
    pub num_threads: usize,
}

impl Default for VideoEncoderConfig {
    fn default() -> Self {
        Self {
            codec: "libx264".to_string(),
            pixel_format: "yuv420p".to_string(),
            fps: 30,
            crf: 23,
            preset: "fast".to_string(),
            num_threads: 2,
        }
    }
}

/// The video encoder stage.
///
/// Receives DatasetFrames and encodes images to MP4 videos.
/// Uses ffmpeg with stdin streaming for zero-copy encoding.
pub struct VideoEncoderStage {
    /// Episode index
    episode_index: usize,
    /// Input receiver
    input_rx: Receiver<DatasetFrame>,
    /// Output sender for encoded videos
    output_tx: Sender<EncodedVideo>,
    /// Configuration
    config: VideoEncoderConfig,
    /// Output directory for temporary video files
    output_dir: PathBuf,
}

impl VideoEncoderStage {
    /// Create a new video encoder stage.
    pub fn new(
        episode_index: usize,
        input_rx: Receiver<DatasetFrame>,
        output_tx: Sender<EncodedVideo>,
        config: VideoEncoderConfig,
        output_dir: PathBuf,
    ) -> Self {
        Self {
            episode_index,
            input_rx,
            output_tx,
            config,
            output_dir,
        }
    }

    /// Spawn the encoder in a thread.
    pub fn spawn(
        self,
    ) -> JoinHandle<PipelineResult<(VideoEncoderStats, crate::streaming::pipeline::StageStats)>>
    {
        thread::spawn(move || {
            let name = "VideoEncoder";
            tracing::debug!("{name} starting");

            let start = Instant::now();
            let result = self.run_internal();
            let duration = start.elapsed();

            match &result {
                Ok((encoder_stats, _stage_stats)) => {
                    tracing::debug!(
                        duration_sec = duration.as_secs_f64(),
                        frames = encoder_stats.frames_processed,
                        videos = encoder_stats.videos_produced,
                        "{name} completed"
                    );
                }
                Err(e) => {
                    tracing::error!(error = %e, "{name} failed");
                }
            }

            result
        })
    }

    fn run_internal(
        &self,
    ) -> PipelineResult<(VideoEncoderStats, crate::streaming::pipeline::StageStats)> {
        use std::fs;

        // Create output directory
        fs::create_dir_all(&self.output_dir).map_err(|e| PipelineError::ExecutionFailed {
            stage: "VideoEncoder".to_string(),
            reason: format!("failed to create output directory: {e}"),
        })?;

        let mut frames_processed = 0usize;
        let mut videos_produced = 0usize;
        let mut total_frames_encoded = 0usize;

        // Group frames by camera (image feature name)
        // Each camera gets its own MP4 video
        let mut camera_buffers: HashMap<String, Vec<(u32, u32, Vec<u8>)>> = HashMap::new();
        let mut camera_dimensions: HashMap<String, (u32, u32)> = HashMap::new();

        loop {
            match self.input_rx.recv() {
                Ok(frame) => {
                    frames_processed += 1;

                    // Group images by feature name
                    for (camera_name, (width, height, data)) in &frame.images {
                        let buffer = camera_buffers.entry(camera_name.clone()).or_default();
                        buffer.push((*width, *height, data.clone()));

                        // Track dimensions (should be consistent)
                        camera_dimensions
                            .entry(camera_name.clone())
                            .or_insert((*width, *height));
                    }

                    // Check if we should finalize videos
                    // For now, we finalize when the channel closes
                }
                Err(_) => {
                    // Channel closed - encode all pending videos
                    tracing::debug!(cameras = camera_buffers.len(), "Encoding final videos");

                    for (camera_name, frames) in camera_buffers {
                        if frames.is_empty() {
                            continue;
                        }

                        let output_path = self.output_dir.join(format!(
                            "episode_{:05}_{}.mp4",
                            self.episode_index, camera_name
                        ));

                        let frame_count = frames.len();
                        match self.encode_frames(&frames, &output_path, self.config.fps) {
                            Ok(_) => {
                                // Get file size
                                let size = fs::metadata(&output_path).map(|m| m.len()).unwrap_or(0);

                                let duration = frame_count as f64 / self.config.fps as f64;

                                let encoded = EncodedVideo {
                                    episode_index: self.episode_index,
                                    camera_name: camera_name.clone(),
                                    local_path: output_path,
                                    size,
                                    duration,
                                };

                                self.output_tx.send(encoded).map_err(|e| {
                                    PipelineError::ChannelError {
                                        from: "VideoEncoder".to_string(),
                                        to: "Upload".to_string(),
                                        reason: e.to_string(),
                                    }
                                })?;

                                videos_produced += 1;
                                total_frames_encoded += frame_count;
                            }
                            Err(e) => {
                                tracing::error!(
                                    camera = %camera_name,
                                    error = %e,
                                    "Failed to encode video"
                                );
                            }
                        }
                    }
                    break;
                }
            }
        }

        Ok((
            VideoEncoderStats {
                frames_processed,
                videos_produced,
                frames_encoded: total_frames_encoded,
                duration_sec: 0.0,
            },
            crate::streaming::pipeline::StageStats {
                stage: "VideoEncoder".to_string(),
                items_processed: frames_processed,
                items_produced: videos_produced,
                duration_sec: 0.0,
                peak_memory_mb: None,
                metrics: [
                    (
                        "videos_produced".to_string(),
                        serde_json::json!(videos_produced),
                    ),
                    (
                        "frames_encoded".to_string(),
                        serde_json::json!(total_frames_encoded),
                    ),
                ]
                .into_iter()
                .collect(),
            },
        ))
    }

    /// Encode frames to MP4 using ffmpeg stdin streaming.
    fn encode_frames(
        &self,
        frames: &[(u32, u32, Vec<u8>)],
        output_path: &PathBuf,
        fps: u32,
    ) -> PipelineResult<()> {
        if frames.is_empty() {
            return Err(PipelineError::ExecutionFailed {
                stage: "VideoEncoder".to_string(),
                reason: "No frames to encode".to_string(),
            });
        }

        let _width = frames[0].0;
        let _height = frames[0].1;

        // Build ffmpeg command
        let mut child = Command::new("ffmpeg")
            .arg("-y") // Overwrite output
            .arg("-f") // Input format
            .arg("image2pipe")
            .arg("-vcodec")
            .arg("ppm")
            .arg("-r")
            .arg(fps.to_string())
            .arg("-i")
            .arg("-") // Read from stdin
            .arg("-vf")
            .arg("pad=ceil(iw/2)*2:ceil(ih/2)*2") // Ensure even dimensions
            .arg("-c:v")
            .arg(&self.config.codec)
            .arg("-pix_fmt")
            .arg(&self.config.pixel_format)
            .arg("-preset")
            .arg(&self.config.preset)
            .arg("-crf")
            .arg(self.config.crf.to_string())
            .arg("-movflags")
            .arg("+faststart") // Enable fast start for web playback
            .arg(output_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_| PipelineError::ExecutionFailed {
                stage: "VideoEncoder".to_string(),
                reason: "ffmpeg not found or failed to start".to_string(),
            })?;

        // Write frames to ffmpeg stdin as PPM format
        if let Some(mut stdin) = child.stdin.take() {
            for (frame_width, frame_height, data) in frames {
                self.write_ppm_frame(&mut stdin, *frame_width, *frame_height, data)
                    .map_err(|e| PipelineError::ExecutionFailed {
                        stage: "VideoEncoder".to_string(),
                        reason: format!("Failed to write frame to ffmpeg: {e}"),
                    })?;
            }
            // Drop stdin to signal EOF
            drop(stdin);
        }

        // Wait for ffmpeg to finish
        let status = child.wait().map_err(|e| PipelineError::ExecutionFailed {
            stage: "VideoEncoder".to_string(),
            reason: format!("Failed to wait for ffmpeg: {e}"),
        })?;

        if !status.success() {
            return Err(PipelineError::ExecutionFailed {
                stage: "VideoEncoder".to_string(),
                reason: format!("ffmpeg failed with status {:?}", status),
            });
        }

        Ok(())
    }

    /// Write a single frame as PPM format to stdin.
    fn write_ppm_frame(
        &self,
        stdin: &mut impl std::io::Write,
        width: u32,
        height: u32,
        data: &[u8],
    ) -> std::io::Result<()> {
        // PPM header: "P6\nwidth height\n255\n"
        write!(stdin, "P6\n{} {}\n255\n", width, height)?;
        stdin.write_all(data)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_video_encoder_config_default() {
        let config = VideoEncoderConfig::default();
        assert_eq!(config.codec, "libx264");
        assert_eq!(config.fps, 30);
        assert_eq!(config.crf, 23);
    }
}
