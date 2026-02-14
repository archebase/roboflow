// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Piped frame encoder orchestrating the full pipeline.
//!
//! This module provides the main entry point for the pipelined video
//! encoding system, coordinating decode, encode, and reorder components.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, bounded};
use roboflow_storage::Storage;
use tokio::runtime::Handle;
use tracing::{info, trace, warn};

use crate::ImageData;
use crate::decode::{DecodePool, DecodePoolConfig, FifoCollector};
use crate::encoder_pool::{EncoderPool, EncoderPoolConfig};
use crate::fragment::FragmentInfo;
use crate::reorder::FrameReorderBuffer;

/// Configuration for the piped frame encoder.
#[derive(Debug, Clone)]
pub struct PipedEncoderConfig {
    /// Decode pool configuration.
    pub decode_config: DecodePoolConfig,
    /// Encoder pool configuration.
    pub encoder_config: EncoderPoolConfig,
    /// Frames per fragment.
    pub frames_per_fragment: usize,
    /// Key prefix for storage (e.g., "dataset/episode_001").
    pub key_prefix: String,
    /// Chunk index for video organization.
    pub chunk_index: u32,
    /// Episode index for video organization.
    pub episode_index: u32,
    /// Enable metrics collection.
    pub enable_metrics: bool,
}

impl Default for PipedEncoderConfig {
    fn default() -> Self {
        Self {
            decode_config: DecodePoolConfig::default(),
            encoder_config: EncoderPoolConfig::default(),
            frames_per_fragment: 30,
            key_prefix: String::new(),
            chunk_index: 0,
            episode_index: 0,
            enable_metrics: true,
        }
    }
}

/// Metrics for the piped encoder.
#[derive(Debug, Clone, Copy, Default)]
pub struct PipedEncoderMetrics {
    /// Total frames submitted.
    pub frames_submitted: u64,
    /// Total frames decoded.
    pub frames_decoded: u64,
    /// Total fragments encoded.
    pub fragments_encoded: u64,
    /// Total fragments uploaded.
    pub fragments_uploaded: u64,
    /// Total decode time.
    pub decode_time_ms: u64,
    /// Total encode time.
    pub encode_time_ms: u64,
    /// Total upload time.
    pub upload_time_ms: u64,
    /// Average decode latency.
    pub avg_decode_latency_us: u64,
    /// Average encode latency.
    pub avg_encode_latency_us: u64,
}

/// Result of encoding a camera stream.
#[derive(Debug)]
pub struct CameraEncodeResult {
    /// Camera identifier.
    pub camera_id: String,
    /// Storage URL.
    pub url: String,
    /// Frames encoded.
    pub frames_encoded: u64,
    /// Fragments created.
    pub fragments_created: u64,
}

/// Internal command for the pipeline.
enum PipelineCommand {
    /// Add a frame for encoding.
    AddFrame { camera_id: String, image: ImageData },
    /// Finalize encoding for all cameras.
    Finalize {
        result_tx: Sender<io::Result<Vec<CameraEncodeResult>>>,
    },
    /// Abort encoding.
    Abort,
}

/// Piped frame encoder orchestrating the full pipeline.
pub struct PipedFrameEncoder {
    /// Command sender.
    cmd_tx: Sender<PipelineCommand>,
    /// Pipeline thread handle.
    thread_handle: Option<std::thread::JoinHandle<()>>,
    /// Configuration.
    config: PipedEncoderConfig,
    /// Metrics.
    metrics: PipedEncoderMetrics,
}

impl PipedFrameEncoder {
    /// Create a new piped frame encoder.
    pub fn new<S: Storage + Send + Sync + 'static>(
        config: PipedEncoderConfig,
        storage: Arc<S>,
        runtime: Handle,
    ) -> io::Result<Self> {
        let (cmd_tx, cmd_rx) = bounded(64);

        let config_clone = config.clone();
        let handle = std::thread::Builder::new()
            .name("piped-encoder".to_string())
            .spawn(move || {
                run_pipeline(cmd_rx, config_clone, storage, runtime);
            })
            .map_err(|e| io::Error::other(e.to_string()))?;

        Ok(Self {
            cmd_tx,
            thread_handle: Some(handle),
            config,
            metrics: PipedEncoderMetrics::default(),
        })
    }

    /// Add a frame for a camera.
    pub fn add_frame(&self, camera_id: &str, image: ImageData) -> io::Result<()> {
        self.cmd_tx
            .send(PipelineCommand::AddFrame {
                camera_id: camera_id.to_string(),
                image,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Pipeline shut down"))
    }

    /// Finalize encoding and get results.
    pub fn finalize(&mut self) -> io::Result<Vec<CameraEncodeResult>> {
        let (result_tx, result_rx) = bounded(1);

        self.cmd_tx
            .send(PipelineCommand::Finalize { result_tx })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Pipeline shut down"))?;

        result_rx
            .recv()
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Pipeline shut down"))?
    }

    /// Abort encoding.
    pub fn abort(&mut self) {
        let _ = self.cmd_tx.send(PipelineCommand::Abort);
    }

    /// Get metrics.
    pub fn metrics(&self) -> &PipedEncoderMetrics {
        &self.metrics
    }

    /// Get configuration.
    pub fn config(&self) -> &PipedEncoderConfig {
        &self.config
    }
}

impl Drop for PipedFrameEncoder {
    fn drop(&mut self) {
        // Ensure thread is joined
        if let Some(handle) = self.thread_handle.take() {
            // Try to abort first
            let _ = self.cmd_tx.send(PipelineCommand::Abort);
            let _ = handle.join();
        }
    }
}

/// Run the pipeline.
fn run_pipeline<S: Storage + Send + Sync + 'static>(
    cmd_rx: Receiver<PipelineCommand>,
    config: PipedEncoderConfig,
    _storage: Arc<S>,
    _runtime: Handle,
) {
    info!("Piped frame encoder starting");

    // Create decode pool
    let decode_pool = match DecodePool::new(config.decode_config.clone()) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "Failed to create decode pool");
            return;
        }
    };

    // Create encoder pool
    let encoder_pool = match EncoderPool::new(config.encoder_config.clone()) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "Failed to create encoder pool");
            decode_pool.shutdown();
            return;
        }
    };

    // Track state per camera
    let mut camera_states: std::collections::HashMap<String, CameraState> =
        std::collections::HashMap::new();

    // FIFO collectors for reordering
    let mut fifo_collectors: std::collections::HashMap<String, FifoCollector> =
        std::collections::HashMap::new();

    // Fragment reorder buffer (for future upload ordering)
    let _fragment_buffer: FrameReorderBuffer<FragmentInfo> =
        FrameReorderBuffer::with_max_buffer(256);

    // Pending uploads
    let mut pending_uploads: std::collections::HashMap<String, Vec<FragmentInfo>> =
        std::collections::HashMap::new();

    let mut running = true;

    while running {
        // Check for commands (with timeout to allow processing)
        match cmd_rx.recv_timeout(Duration::from_millis(10)) {
            Ok(cmd) => match cmd {
                PipelineCommand::AddFrame { camera_id, image } => {
                    // Submit to decode pool
                    match decode_pool.submit(camera_id.clone(), image) {
                        Ok(seq) => {
                            trace!(camera = %camera_id, sequence = seq, "Frame submitted to decode pool");
                        }
                        Err(e) => {
                            warn!(camera = %camera_id, error = %e, "Failed to submit frame");
                        }
                    }
                }
                PipelineCommand::Finalize { result_tx } => {
                    // Flush remaining frames and get results
                    let results = finalize_encoding(
                        &decode_pool,
                        &encoder_pool,
                        &mut camera_states,
                        &mut fifo_collectors,
                        &mut pending_uploads,
                        &config,
                    );
                    let _ = result_tx.send(results);
                    running = false;
                }
                PipelineCommand::Abort => {
                    info!("Pipeline aborting");
                    running = false;
                }
            },
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                // Timeout - process decoded frames and encoded fragments
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                info!("Pipeline channel disconnected, exiting");
                running = false;
            }
        }

        // Process decoded frames
        while let Some(result) = decode_pool.try_recv() {
            let camera_id = result.camera_id.clone();

            // Ensure collector exists
            fifo_collectors.entry(camera_id.clone()).or_default();

            if let Some(collector) = fifo_collectors.get_mut(&camera_id) {
                collector.push(result);
            }
        }

        // Pop ordered decode results and submit for encoding
        for (camera_id, collector) in fifo_collectors.iter_mut() {
            while let Some(result) = collector.pop() {
                if let Ok(Some(frame)) = result.result {
                    // Get or create camera state
                    let state = camera_states
                        .entry(camera_id.clone())
                        .or_insert_with(|| CameraState::new(&config));

                    state.frame_buffer.push(frame);

                    // Check if we have enough frames for a fragment
                    if state.frame_buffer.len() >= config.frames_per_fragment {
                        let frames: Vec<_> = state
                            .frame_buffer
                            .drain(..config.frames_per_fragment)
                            .collect();
                        let cmd = crate::encoder_pool::EncodeCommand {
                            sequence: state.next_fragment_seq,
                            camera_id: camera_id.clone(),
                            frames,
                            fragment_index: state.fragment_index,
                        };
                        state.next_fragment_seq += 1;
                        state.fragment_index += 1;

                        if let Err(e) = encoder_pool.submit(cmd) {
                            warn!(camera = %camera_id, error = %e, "Failed to submit encode command");
                        }
                    }
                }
            }
        }

        // Process encoded fragments
        while let Some(result) = encoder_pool.try_recv() {
            if let Ok(fragment) = result.result {
                // Track for upload
                let uploads = pending_uploads.entry(result.camera_id.clone()).or_default();
                uploads.push(fragment);
            }
        }
    }

    // Cleanup
    decode_pool.shutdown();
    encoder_pool.shutdown();

    info!("Piped frame encoder stopped");
}

/// Camera state tracking.
struct CameraState {
    /// Buffered frames waiting for fragment.
    frame_buffer: Vec<crate::decode::DecodedFrame>,
    /// Current fragment index.
    fragment_index: u32,
    /// Next fragment sequence.
    next_fragment_seq: u64,
}

impl CameraState {
    fn new(config: &PipedEncoderConfig) -> Self {
        Self {
            frame_buffer: Vec::with_capacity(config.frames_per_fragment),
            fragment_index: 0,
            next_fragment_seq: 0,
        }
    }
}

/// Finalize encoding and collect results.
fn finalize_encoding(
    decode_pool: &DecodePool,
    encoder_pool: &EncoderPool,
    camera_states: &mut std::collections::HashMap<String, CameraState>,
    fifo_collectors: &mut std::collections::HashMap<String, FifoCollector>,
    pending_uploads: &mut std::collections::HashMap<String, Vec<FragmentInfo>>,
    config: &PipedEncoderConfig,
) -> io::Result<Vec<CameraEncodeResult>> {
    info!("Finalizing encoding");

    // Drain remaining decode results
    while let Some(result) = decode_pool.try_recv() {
        if let Some(collector) = fifo_collectors.get_mut(&result.camera_id) {
            collector.push(result);
        }
    }

    // Process remaining ordered results
    for (camera_id, collector) in fifo_collectors.iter_mut() {
        while let Some(result) = collector.pop() {
            if let Ok(Some(frame)) = result.result
                && let Some(state) = camera_states.get_mut(camera_id)
            {
                state.frame_buffer.push(frame);
            }
        }
    }

    // Submit remaining frames for encoding
    for (camera_id, state) in camera_states.iter_mut() {
        if !state.frame_buffer.is_empty() {
            let frames: Vec<_> = state.frame_buffer.drain(..).collect();
            let cmd = crate::encoder_pool::EncodeCommand {
                sequence: state.next_fragment_seq,
                camera_id: camera_id.clone(),
                frames,
                fragment_index: state.fragment_index,
            };

            if let Err(e) = encoder_pool.submit(cmd) {
                warn!(camera = %camera_id, error = %e, "Failed to submit final encode");
            }
        }
    }

    // Wait for all encodes to complete by draining results
    // Use a loop with timeout instead of busy-wait
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        match encoder_pool.try_recv() {
            Some(result) => {
                if let Ok(fragment) = result.result {
                    let uploads = pending_uploads.entry(result.camera_id.clone()).or_default();
                    uploads.push(fragment);
                }
            }
            None => {
                // No more results available
                if std::time::Instant::now() >= deadline {
                    warn!("Timeout waiting for encoder results");
                    break;
                }
                // Brief sleep to avoid busy-waiting
                std::thread::sleep(Duration::from_millis(10));
                // Check again after sleep - if still no results, we're done
                if encoder_pool.try_recv().is_none() {
                    break;
                }
            }
        }
    }

    // Build results
    let results: Vec<CameraEncodeResult> = camera_states
        .iter()
        .map(|(camera_id, state)| {
            let url = format!(
                "{}/videos/chunk-{}/{}/episode_{:06}.mp4",
                config.key_prefix, config.chunk_index, camera_id, config.episode_index
            );

            CameraEncodeResult {
                camera_id: camera_id.clone(),
                url,
                frames_encoded: 0, // Would need to track properly
                fragments_created: state.fragment_index as u64,
            }
        })
        .collect();

    info!(cameras = results.len(), "Finalization complete");

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = PipedEncoderConfig::default();
        assert!(config.frames_per_fragment > 0);
        assert!(config.key_prefix.is_empty());
    }

    #[test]
    fn test_metrics_default() {
        let metrics = PipedEncoderMetrics::default();
        assert_eq!(metrics.frames_submitted, 0);
        assert_eq!(metrics.fragments_encoded, 0);
    }

    #[test]
    fn test_camera_state_new() {
        let config = PipedEncoderConfig::default();
        let state = CameraState::new(&config);
        assert!(state.frame_buffer.is_empty());
        assert_eq!(state.fragment_index, 0);
    }
}
