// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Piped frame encoder orchestrating the full pipeline.
//!
//! This module provides the main entry point for the pipelined video
//! encoding system, coordinating decode, encode, and reorder components.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, bounded};
use roboflow_storage::AsyncStorage;
use tokio::runtime::Handle;
use tracing::{info, trace, warn};

use crate::ImageData;
use crate::convert::{ConvertCommand, ConvertPool, ConvertPoolConfig, ConvertResult, TargetFormat};
use crate::decode::{DecodePool, DecodePoolConfig, FifoCollector};
use crate::encoder_pool::{EncoderPool, EncoderPoolConfig};
use crate::fragment::FragmentInfo;
use crate::frame::VideoFrame;

/// Configuration for the piped frame encoder.
#[derive(Debug, Clone)]
pub struct PipedEncoderConfig {
    /// Decode pool configuration.
    pub decode_config: DecodePoolConfig,
    /// Convert pool configuration.
    pub convert_config: ConvertPoolConfig,
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
        let cpu_count = num_cpus::get();
        // Optimal thread distribution for 3-stage pipeline (10 cores):
        // - Decode: 50% (5 workers) - JPEG decode is CPU-intensive
        // - Convert: 10% (1 worker) - SIMD is fast, single worker sufficient
        // - Encode: 40% (4 workers) - VideoToolbox is hardware-accelerated
        let decode_workers = cpu_count.div_ceil(2).max(2);
        let convert_workers = 1;
        let encode_workers = (cpu_count * 2).div_ceil(5).max(1);

        Self {
            decode_config: DecodePoolConfig {
                worker_count: decode_workers,
                ..Default::default()
            },
            convert_config: ConvertPoolConfig {
                worker_count: convert_workers,
                target_format: TargetFormat::Nv12,
                zero_copy: true,
                ..Default::default()
            },
            encoder_config: EncoderPoolConfig {
                worker_count: encode_workers,
                ..Default::default()
            },
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

/// Simple FIFO buffer for convert results.
struct ConvertFifo {
    /// Buffered results waiting for their turn.
    buffer: BTreeMap<u64, ConvertResult>,
    /// Next expected sequence number.
    next_expected: u64,
}

impl ConvertFifo {
    fn new() -> Self {
        Self {
            buffer: BTreeMap::new(),
            next_expected: 0,
        }
    }

    fn push(&mut self, result: ConvertResult) {
        self.buffer.insert(result.sequence, result);
    }

    fn pop(&mut self) -> Option<ConvertResult> {
        if let Some(result) = self.buffer.remove(&self.next_expected) {
            self.next_expected += 1;
            Some(result)
        } else {
            None
        }
    }
}

impl Default for ConvertFifo {
    fn default() -> Self {
        Self::new()
    }
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
    pub fn new<S: AsyncStorage + Send + Sync + 'static>(
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

/// Upload a fragment to storage.
fn upload_fragment<S: AsyncStorage + Send + Sync + 'static>(
    storage: &Arc<S>,
    runtime: &Handle,
    config: &PipedEncoderConfig,
    camera_id: &str,
    fragment_index: u32,
    fragment: &FragmentInfo,
) -> io::Result<String> {
    // Build the destination URL - include fragment_index for unique filenames
    let url = format!(
        "{}/videos/chunk-{}/{}/episode_{:06}_fragment_{:04}.mp4",
        config.key_prefix, config.chunk_index, camera_id, config.episode_index, fragment_index
    );

    // Read fragment data from temp file
    let data = std::fs::read(&fragment.path)?;

    // Upload to storage (blocking in async context)
    runtime
        .block_on(async {
            storage
                .write(std::path::Path::new(&url), bytes::Bytes::from(data))
                .await
        })
        .map_err(|e| io::Error::other(e.to_string()))?;

    Ok(url)
}

/// Run the pipeline with 3-stage parallelism (Decode → Convert → Encode).
fn run_pipeline<S: AsyncStorage + Send + Sync + 'static>(
    cmd_rx: Receiver<PipelineCommand>,
    config: PipedEncoderConfig,
    storage: Arc<S>,
    runtime: Handle,
) {
    info!("Piped frame encoder starting (3-stage: decode → convert → encode)");

    // Create decode pool
    let decode_pool = match DecodePool::new(config.decode_config.clone()) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "Failed to create decode pool");
            return;
        }
    };

    // Create convert pool
    let convert_pool = match ConvertPool::new(config.convert_config.clone()) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "Failed to create convert pool");
            decode_pool.shutdown();
            return;
        }
    };

    // Create encoder pool
    let encoder_pool = match EncoderPool::new(config.encoder_config.clone()) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "Failed to create encoder pool");
            decode_pool.shutdown();
            convert_pool.shutdown();
            return;
        }
    };

    info!(
        decode_workers = config.decode_config.worker_count,
        convert_workers = config.convert_config.worker_count,
        encode_workers = config.encoder_config.worker_count,
        "Pipeline workers created"
    );

    // Track state per camera
    let mut camera_states: std::collections::HashMap<String, CameraState> =
        std::collections::HashMap::new();

    // FIFO collectors for decoded results
    let mut decode_collectors: std::collections::HashMap<String, FifoCollector> =
        std::collections::HashMap::new();

    // FIFO collectors for converted results
    let mut convert_collectors: std::collections::HashMap<String, ConvertFifo> =
        std::collections::HashMap::new();

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
                        &convert_pool,
                        &encoder_pool,
                        &mut camera_states,
                        &mut decode_collectors,
                        &mut convert_collectors,
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
                // Timeout - process all pipeline stages
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                info!("Pipeline channel disconnected, exiting");
                running = false;
            }
        }

        // Stage 1: Process decoded frames → submit to convert pool
        while let Some(result) = decode_pool.try_recv() {
            let camera_id = result.camera_id.clone();

            // Ensure collector exists
            decode_collectors.entry(camera_id.clone()).or_default();

            if let Some(collector) = decode_collectors.get_mut(&camera_id) {
                collector.push(result);
            }
        }

        // Pop ordered decode results and submit to convert pool (individual frames)
        for (camera_id, collector) in decode_collectors.iter_mut() {
            while let Some(result) = collector.pop() {
                if let Ok(Some(frame)) = result.result {
                    // Get or create camera state for convert sequence tracking
                    let state = camera_states
                        .entry(camera_id.clone())
                        .or_insert_with(|| CameraState::new(&config));

                    // Submit individual frame to convert pool
                    let cmd = ConvertCommand {
                        sequence: state.next_convert_seq,
                        camera_id: camera_id.clone(),
                        frames: vec![frame],
                    };
                    state.next_convert_seq += 1;

                    if let Err(e) = convert_pool.submit(cmd) {
                        warn!(camera = %camera_id, error = %e, "Failed to submit convert command");
                    }
                }
            }
        }

        // Stage 2: Process converted frames → buffer and submit to encode pool
        while let Some(result) = convert_pool.try_recv() {
            let camera_id = result.camera_id.clone();

            // Ensure collector exists
            convert_collectors.entry(camera_id.clone()).or_default();

            if let Some(collector) = convert_collectors.get_mut(&camera_id) {
                collector.push(result);
            }
        }

        // Pop ordered convert results and submit for encoding
        for (camera_id, collector) in convert_collectors.iter_mut() {
            while let Some(result) = collector.pop() {
                if let Ok(converted_frames) = result.result {
                    // Convert to VideoFrame
                    let video_frames: Vec<VideoFrame> = converted_frames
                        .into_iter()
                        .map(|f| f.to_video_frame())
                        .collect();

                    // Get or create camera state
                    let state = camera_states
                        .entry(camera_id.clone())
                        .or_insert_with(|| CameraState::new(&config));

                    // Buffer converted frames
                    state.video_frame_buffer.extend(video_frames);

                    // Check if we have enough frames for a fragment
                    while state.video_frame_buffer.len() >= config.frames_per_fragment {
                        let frames: Vec<_> = state
                            .video_frame_buffer
                            .drain(..config.frames_per_fragment)
                            .collect();
                        let cmd = crate::encoder_pool::EncodeCommand::new(
                            state.next_fragment_seq,
                            camera_id.clone(),
                            frames,
                            state.fragment_index,
                        );
                        state.next_fragment_seq += 1;
                        state.fragment_index += 1;

                        if let Err(e) = encoder_pool.submit(cmd) {
                            warn!(camera = %camera_id, error = %e, "Failed to submit encode command");
                        }
                    }
                }
            }
        }

        // Stage 3: Process encoded fragments and upload
        while let Some(result) = encoder_pool.try_recv() {
            if let Ok(fragment) = result.result {
                // Upload immediately
                let camera_id = result.camera_id.clone();
                let fragment_index = camera_states
                    .get(&camera_id)
                    .map(|s| s.fragment_index)
                    .unwrap_or(0);

                match upload_fragment(
                    &storage,
                    &runtime,
                    &config,
                    &camera_id,
                    fragment_index,
                    &fragment,
                ) {
                    Ok(url) => {
                        info!(camera = %camera_id, fragment_index, url = %url, "Fragment uploaded");
                        pending_uploads.entry(camera_id).or_default().push(fragment);
                    }
                    Err(e) => {
                        warn!(camera = %camera_id, fragment_index, error = %e, "Failed to upload fragment");
                    }
                }
            }
        }
    }

    // Cleanup
    decode_pool.shutdown();
    convert_pool.shutdown();
    encoder_pool.shutdown();

    info!("Piped frame encoder stopped");
}

/// Camera state tracking.
struct CameraState {
    /// Buffered converted frames waiting for encoding.
    video_frame_buffer: Vec<VideoFrame>,
    /// Current fragment index.
    fragment_index: u32,
    /// Next fragment sequence for encoding.
    next_fragment_seq: u64,
    /// Next convert sequence.
    next_convert_seq: u64,
}

impl CameraState {
    fn new(config: &PipedEncoderConfig) -> Self {
        Self {
            video_frame_buffer: Vec::with_capacity(config.frames_per_fragment),
            fragment_index: 0,
            next_fragment_seq: 0,
            next_convert_seq: 0,
        }
    }
}

/// Finalize encoding and collect results.
#[allow(clippy::too_many_arguments)]
fn finalize_encoding(
    decode_pool: &DecodePool,
    convert_pool: &ConvertPool,
    encoder_pool: &EncoderPool,
    camera_states: &mut std::collections::HashMap<String, CameraState>,
    decode_collectors: &mut std::collections::HashMap<String, FifoCollector>,
    convert_collectors: &mut std::collections::HashMap<String, ConvertFifo>,
    pending_uploads: &mut std::collections::HashMap<String, Vec<FragmentInfo>>,
    config: &PipedEncoderConfig,
) -> io::Result<Vec<CameraEncodeResult>> {
    info!("Finalizing encoding");

    // Stage 1: Drain remaining decode results → submit to convert pool
    while let Some(result) = decode_pool.try_recv() {
        if let Some(collector) = decode_collectors.get_mut(&result.camera_id) {
            collector.push(result);
        }
    }

    // Process remaining ordered decode results
    for (camera_id, collector) in decode_collectors.iter_mut() {
        while let Some(result) = collector.pop() {
            if let Ok(Some(frame)) = result.result {
                let state = camera_states
                    .entry(camera_id.clone())
                    .or_insert_with(|| CameraState::new(config));

                let cmd = ConvertCommand {
                    sequence: state.next_convert_seq,
                    camera_id: camera_id.clone(),
                    frames: vec![frame],
                };
                state.next_convert_seq += 1;

                if let Err(e) = convert_pool.submit(cmd) {
                    warn!(camera = %camera_id, error = %e, "Failed to submit convert during finalize");
                }
            }
        }
    }

    // Stage 2: Drain remaining convert results → submit to encode pool
    while let Some(result) = convert_pool.try_recv() {
        if let Some(collector) = convert_collectors.get_mut(&result.camera_id) {
            collector.push(result);
        }
    }

    // Process remaining ordered convert results
    for (camera_id, collector) in convert_collectors.iter_mut() {
        while let Some(result) = collector.pop() {
            if let Ok(converted_frames) = result.result {
                let video_frames: Vec<VideoFrame> = converted_frames
                    .into_iter()
                    .map(|f| f.to_video_frame())
                    .collect();

                let state = camera_states
                    .entry(camera_id.clone())
                    .or_insert_with(|| CameraState::new(config));

                state.video_frame_buffer.extend(video_frames);

                // Submit full batches
                while state.video_frame_buffer.len() >= config.frames_per_fragment {
                    let frames: Vec<_> = state
                        .video_frame_buffer
                        .drain(..config.frames_per_fragment)
                        .collect();
                    let cmd = crate::encoder_pool::EncodeCommand::new(
                        state.next_fragment_seq,
                        camera_id.clone(),
                        frames,
                        state.fragment_index,
                    );
                    state.next_fragment_seq += 1;
                    state.fragment_index += 1;

                    if let Err(e) = encoder_pool.submit(cmd) {
                        warn!(camera = %camera_id, error = %e, "Failed to submit encode during finalize");
                    }
                }
            }
        }
    }

    // Stage 3: Submit remaining converted frames for encoding
    for (camera_id, state) in camera_states.iter_mut() {
        if !state.video_frame_buffer.is_empty() {
            let frames = std::mem::take(&mut state.video_frame_buffer);
            let cmd = crate::encoder_pool::EncodeCommand::new(
                state.next_fragment_seq,
                camera_id.clone(),
                frames,
                state.fragment_index,
            );
            state.next_fragment_seq += 1;
            state.fragment_index += 1;

            if let Err(e) = encoder_pool.submit(cmd) {
                warn!(camera = %camera_id, error = %e, "Failed to submit final encode");
            }
        }
    }

    // Wait for all encodes to complete by draining results
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
                if std::time::Instant::now() >= deadline {
                    warn!("Timeout waiting for encoder results");
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
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
                frames_encoded: 0,
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
        assert!(state.video_frame_buffer.is_empty());
        assert_eq!(state.fragment_index, 0);
    }
}
