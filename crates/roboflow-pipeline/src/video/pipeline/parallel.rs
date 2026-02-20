// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Parallel video pipeline: Parallel decode + convert + encode.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::Sender;
use std::thread::JoinHandle;
use std::time::Duration;

use tracing::{debug, info, warn};

use super::{PipelineConfig, PipelineHandle, PipelineResult};
use crate::video::{
    ImageData,
    config::VideoEncoderConfig,
    convert::{ConvertCommand, ConvertPool, ConvertPoolConfig, TargetFormat},
    decode::{DecodePool, DecodePoolConfig, DecodedFrame},
    encoder_pool::{EncodeCommand, EncoderPool, EncoderPoolConfig},
    fragment::FragmentEncoderConfig,
    streaming::EncodedChunk,
};

/// Configuration for the parallel video pipeline.
#[derive(Debug, Clone)]
pub struct VideoPipelineConfig {
    /// Camera name.
    pub camera: String,
    /// Video encoder configuration.
    pub video_config: VideoEncoderConfig,
    /// Decode worker count (None = auto).
    pub decode_workers: Option<usize>,
    /// Convert worker count (None = auto).
    pub convert_workers: Option<usize>,
    /// Encode worker count (None = auto).
    pub encode_workers: Option<usize>,
    /// Channel capacities.
    pub pending_capacity: usize,
    pub completed_capacity: usize,
    /// Frames per fragment (batch size for encoding).
    pub frames_per_fragment: usize,
    /// Chunk size threshold (bytes).
    pub chunk_size: usize,
}

impl Default for VideoPipelineConfig {
    fn default() -> Self {
        let cpu_count = num_cpus::get();
        Self {
            camera: String::new(),
            video_config: VideoEncoderConfig::default(),
            decode_workers: Some((cpu_count * 2 / 5).max(1)), // 40% for decode
            convert_workers: Some((cpu_count / 5).max(1)),    // 20% for convert
            encode_workers: Some((cpu_count * 2 / 5).max(2)), // 40% for encode
            pending_capacity: 512,
            completed_capacity: 512,
            frames_per_fragment: 30,
            chunk_size: 256 * 1024,
        }
    }
}

impl PipelineConfig for VideoPipelineConfig {
    fn create_pipeline(
        &self,
        upload_tx: Sender<EncodedChunk>,
    ) -> io::Result<Box<dyn PipelineHandle>> {
        VideoPipeline::new(self.clone(), upload_tx).map(|p| Box::new(p) as Box<dyn PipelineHandle>)
    }
}

/// Parallel video pipeline handle.
pub struct VideoPipeline {
    #[allow(dead_code)] // Stored for debugging and potential future use
    camera: String,
    cmd_tx: Sender<PipelineCommand>,
    thread_handle: Option<JoinHandle<std::io::Result<PipelineResult>>>,
}

enum PipelineCommand {
    AddFrame(ImageData),
    Flush,
    Shutdown,
}

impl VideoPipeline {
    pub fn new(config: VideoPipelineConfig, upload_tx: Sender<EncodedChunk>) -> io::Result<Self> {
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();

        // Clone camera BEFORE moving config into the closure
        let camera_for_struct = config.camera.clone();
        let camera = camera_for_struct.clone();
        let chunk_size = config.chunk_size;

        let handle = std::thread::Builder::new()
            .name(format!("video-pipeline-{}", camera))
            .spawn(move || run_coordinator(camera, cmd_rx, upload_tx, config, chunk_size))
            .map_err(|e| io::Error::other(e.to_string()))?;

        Ok(Self {
            camera: camera_for_struct,
            cmd_tx,
            thread_handle: Some(handle),
        })
    }
}

fn run_coordinator(
    camera: String,
    cmd_rx: std::sync::mpsc::Receiver<PipelineCommand>,
    upload_tx: Sender<EncodedChunk>,
    config: VideoPipelineConfig,
    chunk_size: usize,
) -> io::Result<PipelineResult> {
    let decode_workers = config.decode_workers.unwrap_or(2);
    let convert_workers = config.convert_workers.unwrap_or(1);
    let encode_workers = config.encode_workers.unwrap_or(2);

    // Create decode pool
    let decode_config = DecodePoolConfig {
        worker_count: decode_workers,
        pending_capacity: config.pending_capacity,
        completed_capacity: config.completed_capacity,
        ..Default::default()
    };
    let decode_pool = DecodePool::new(decode_config)?;

    // Create convert pool
    let convert_config = ConvertPoolConfig {
        worker_count: convert_workers,
        pending_capacity: config.pending_capacity / 2,
        completed_capacity: config.completed_capacity / 2,
        target_format: TargetFormat::Nv12,
        ..Default::default()
    };
    let convert_pool = ConvertPool::new(convert_config)?;

    // Create encode pool
    let fragment_config = FragmentEncoderConfig {
        video: config.video_config,
        camera_id: camera.clone(),
        max_frames_per_fragment: config.frames_per_fragment,
        ..Default::default()
    };
    let encode_config = EncoderPoolConfig {
        worker_count: encode_workers,
        pending_capacity: config.pending_capacity / 2,
        completed_capacity: config.completed_capacity / 2,
        fragment_config,
        ..Default::default()
    };
    let encode_pool = EncoderPool::new(encode_config)?;

    info!(
        camera = %camera,
        decode_workers,
        convert_workers,
        encode_workers,
        "Three-stage pipeline initialized"
    );

    coordinator_loop(
        camera,
        cmd_rx,
        upload_tx,
        decode_pool,
        convert_pool,
        encode_pool,
        config.frames_per_fragment,
        chunk_size,
    )
}

#[allow(clippy::too_many_arguments)]
fn coordinator_loop(
    camera: String,
    cmd_rx: std::sync::mpsc::Receiver<PipelineCommand>,
    upload_tx: Sender<EncodedChunk>,
    decode_pool: DecodePool,
    convert_pool: ConvertPool,
    encode_pool: EncoderPool,
    frames_per_fragment: usize,
    chunk_size: usize,
) -> io::Result<PipelineResult> {
    let mut sequence = 0u64;
    let mut frames_encoded = 0usize;
    let mut frames_skipped = 0usize;
    let mut frame_buffer: Vec<DecodedFrame> = Vec::new();
    let mut flush_requested = false;

    // Track in-flight operations
    let pending_decodes = Arc::new(AtomicUsize::new(0));
    let pending_converts = Arc::new(AtomicUsize::new(0));
    let pending_encodes = Arc::new(AtomicUsize::new(0));

    // Main event loop - polling approach since pools use try_recv()
    loop {
        let mut progress = false;

        // Check for shutdown when all pending work is done and flush was requested
        let all_done = pending_decodes.load(Ordering::Relaxed) == 0
            && pending_converts.load(Ordering::Relaxed) == 0
            && pending_encodes.load(Ordering::Relaxed) == 0
            && frame_buffer.is_empty();

        if all_done && flush_requested {
            break;
        }

        if all_done {
            // Try to receive next command with timeout
            match cmd_rx.recv_timeout(Duration::from_millis(10)) {
                Ok(PipelineCommand::AddFrame(image)) => {
                    submit_decode(&decode_pool, &camera, image, sequence, &pending_decodes)?;
                    sequence += 1;
                    continue;
                }
                Ok(PipelineCommand::Flush) => {
                    flush_requested = true;
                    continue;
                }
                Ok(PipelineCommand::Shutdown) => {
                    return Err(io::Error::other("Pipeline shutdown requested"));
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    // Channel closed, exit loop
                    break;
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    // No new commands, exit
                    break;
                }
            }
        }

        // Process commands (non-blocking)
        while let Ok(cmd) = cmd_rx.try_recv() {
            progress = true;
            match cmd {
                PipelineCommand::AddFrame(image) => {
                    submit_decode(&decode_pool, &camera, image, sequence, &pending_decodes)?;
                    sequence += 1;
                }
                PipelineCommand::Flush => {
                    flush_requested = true;
                }
                PipelineCommand::Shutdown => {
                    return Err(io::Error::other("Pipeline shutdown requested"));
                }
            }
        }

        // Process decode results
        while let Some(decode_result) = decode_pool.try_recv() {
            progress = true;
            pending_decodes.fetch_sub(1, Ordering::Relaxed);

            match decode_result.result {
                Ok(Some(frame)) => {
                    frame_buffer.push(frame);

                    if frame_buffer.len() >= frames_per_fragment {
                        submit_convert_batch(
                            &convert_pool,
                            &camera,
                            std::mem::take(&mut frame_buffer),
                            sequence,
                            &pending_converts,
                        )?;
                        sequence += 1;
                    }
                }
                Ok(None) => {
                    frames_skipped += 1;
                }
                Err(e) => {
                    warn!(
                        camera = %camera,
                        error = %e,
                        "Decode failed"
                    );
                    frames_skipped += 1;
                }
            }
        }

        // Process convert results
        while let Some(convert_result) = convert_pool.try_recv() {
            progress = true;
            pending_converts.fetch_sub(1, Ordering::Relaxed);

            match convert_result.result {
                Ok(converted_frames) => {
                    let video_frames: Vec<crate::VideoFrame> = converted_frames
                        .into_iter()
                        .map(|f| f.to_video_frame())
                        .collect();

                    submit_encode(
                        &encode_pool,
                        &camera,
                        video_frames,
                        convert_result.sequence as u32,
                        &pending_encodes,
                    )?;
                }
                Err(e) => {
                    warn!(
                        camera = %camera,
                        error = %e,
                        "Convert failed"
                    );
                    frames_skipped += frames_per_fragment;
                }
            }
        }

        // Process encode results
        while let Some(encode_result) = encode_pool.try_recv() {
            progress = true;
            pending_encodes.fetch_sub(1, Ordering::Relaxed);

            match encode_result.result {
                Ok(fragment) => {
                    frames_encoded += fragment.frame_count;

                    // Read the fragment data and send as chunks
                    match fragment.read_data() {
                        Ok(data) => {
                            for chunk_data in data.chunks(chunk_size) {
                                let chunk = EncodedChunk::new(chunk_data.to_vec());
                                if upload_tx.send(chunk).is_err() {
                                    warn!(camera = %camera, "Upload channel closed");
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            warn!(
                                camera = %camera,
                                error = %e,
                                "Failed to read fragment data"
                            );
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        camera = %camera,
                        error = %e,
                        "Encode failed"
                    );
                }
            }
        }

        // Small sleep if no progress to avoid busy-waiting
        if !progress {
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    // Flush remaining frames
    if !frame_buffer.is_empty() {
        submit_convert_batch(
            &convert_pool,
            &camera,
            frame_buffer,
            sequence,
            &pending_converts,
        )?;
    }

    // Wait for all pending operations to complete
    while pending_converts.load(Ordering::Relaxed) > 0
        || pending_encodes.load(Ordering::Relaxed) > 0
    {
        while let Some(convert_result) = convert_pool.try_recv() {
            pending_converts.fetch_sub(1, Ordering::Relaxed);
            if let Ok(converted_frames) = convert_result.result {
                let video_frames: Vec<crate::VideoFrame> = converted_frames
                    .into_iter()
                    .map(|f| f.to_video_frame())
                    .collect();
                submit_encode(
                    &encode_pool,
                    &camera,
                    video_frames,
                    convert_result.sequence as u32,
                    &pending_encodes,
                )?;
            }
        }

        while let Some(encode_result) = encode_pool.try_recv() {
            pending_encodes.fetch_sub(1, Ordering::Relaxed);
            if let Ok(fragment) = encode_result.result {
                frames_encoded += fragment.frame_count;
                if let Ok(data) = fragment.read_data() {
                    for chunk_data in data.chunks(chunk_size) {
                        let chunk = EncodedChunk::new(chunk_data.to_vec());
                        if upload_tx.send(chunk).is_err() {
                            break;
                        }
                    }
                }
            }
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    debug!(
        camera = %camera,
        frames_encoded,
        frames_skipped,
        "Three-stage pipeline completed"
    );

    Ok(PipelineResult {
        camera,
        frames_encoded,
        frames_skipped,
    })
}

fn submit_decode(
    decode_pool: &DecodePool,
    camera: &str,
    image: ImageData,
    _sequence: u64,
    pending: &Arc<AtomicUsize>,
) -> io::Result<()> {
    let video_image = crate::ImageData {
        is_depth: false,
        original_timestamp: 0,
        width: image.width,
        height: image.height,
        data: image.data,
        is_encoded: image.is_encoded,
    };

    decode_pool.submit(camera.to_string(), video_image)?;
    pending.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

fn submit_convert_batch(
    convert_pool: &ConvertPool,
    camera: &str,
    frames: Vec<DecodedFrame>,
    sequence: u64,
    pending: &Arc<AtomicUsize>,
) -> io::Result<()> {
    let cmd = ConvertCommand {
        sequence,
        camera_id: camera.to_string(),
        frames,
    };
    convert_pool.submit(cmd)?;
    pending.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

fn submit_encode(
    encode_pool: &EncoderPool,
    camera: &str,
    frames: Vec<crate::VideoFrame>,
    fragment_index: u32,
    pending: &Arc<AtomicUsize>,
) -> io::Result<()> {
    let cmd = EncodeCommand::new(
        fragment_index as u64,
        camera.to_string(),
        frames,
        fragment_index,
    );
    encode_pool.submit(cmd)?;
    pending.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

impl PipelineHandle for VideoPipeline {
    fn add_frame(&self, image: ImageData) -> io::Result<()> {
        self.cmd_tx
            .send(PipelineCommand::AddFrame(image))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Pipeline thread terminated"))
    }

    fn flush(&self) -> io::Result<()> {
        self.cmd_tx
            .send(PipelineCommand::Flush)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Pipeline thread terminated"))
    }

    fn shutdown(&self) -> io::Result<()> {
        self.cmd_tx
            .send(PipelineCommand::Shutdown)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Pipeline thread terminated"))
    }

    fn join(mut self: Box<Self>) -> io::Result<PipelineResult> {
        let handle = self
            .thread_handle
            .take()
            .ok_or_else(|| io::Error::other("Pipeline already joined"))?;

        handle
            .join()
            .map_err(|e| io::Error::other(format!("Pipeline thread panicked: {:?}", e)))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = VideoPipelineConfig::default();
        assert_eq!(config.camera, "");
        assert_eq!(config.frames_per_fragment, 30);
    }
}
