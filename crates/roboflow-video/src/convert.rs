// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Color space conversion pool for RGB→YUV conversion.
//!
//! This module provides a dedicated pool for SIMD-accelerated color space
//! conversion, separating this CPU-intensive work from decode and encode stages.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread::JoinHandle;

use crossbeam_channel::{Receiver, Sender, bounded};
use tracing::{debug, trace, warn};

use crate::decode::DecodedFrame;
use crate::frame::{FrameBuffer, FrameFormat, PixelFormat, VideoFrame};
use crate::simd::{ConversionStrategy, rgb_to_nv12, rgb_to_nv12_in_place, rgb_to_yuv420p};

/// Color space converter trait.
///
/// Implementations convert RGB frames to YUV formats suitable for video encoding.
pub trait ColorSpaceConverter: Send + Sync {
    /// Convert RGB frame to the target format.
    fn convert(&self, frame: &DecodedFrame) -> io::Result<ConvertedFrame>;

    /// Convert RGB frame to the target format, attempting zero-copy if possible.
    ///
    /// This method takes ownership of the frame buffer and attempts to convert
    /// in-place if it's the only reference. If not, it falls back to allocation.
    fn convert_zero_copy(&self, frame: DecodedFrame) -> io::Result<ConvertedFrameZeroCopy> {
        // Default implementation: fall back to regular conversion
        let width = frame.width();
        let height = frame.height();
        let regular = self.convert(&frame)?;
        Ok(ConvertedFrameZeroCopy::Owned(regular, width, height))
    }
}

/// A frame that has been converted to a target pixel format.
#[derive(Debug)]
pub struct ConvertedFrame {
    /// Frame width in pixels.
    pub width: u32,
    /// Frame height in pixels.
    pub height: u32,
    /// Pixel format.
    pub format: FrameFormat,
    /// Frame data (format-dependent).
    pub data: FrameData,
}

/// Frame data after color conversion.
#[derive(Debug)]
pub enum FrameData {
    /// NV12 format: (Y plane, UV plane).
    Nv12 { y_plane: Vec<u8>, uv_plane: Vec<u8> },
    /// YUV420P format: (Y plane, U plane, V plane).
    Yuv420p {
        y_plane: Vec<u8>,
        u_plane: Vec<u8>,
        v_plane: Vec<u8>,
    },
}

impl ConvertedFrame {
    /// Convert to a VideoFrame for encoding.
    pub fn to_video_frame(&self) -> VideoFrame {
        match &self.data {
            FrameData::Nv12 { y_plane, uv_plane } => {
                VideoFrame::from_nv12(self.width, self.height, y_plane.clone(), uv_plane.clone())
            }
            FrameData::Yuv420p {
                y_plane,
                u_plane,
                v_plane,
            } => {
                // For YUV420P, we need to convert to the internal VideoFrame format
                // Currently VideoFrame only supports NV12 directly, so we convert
                VideoFrame::from_yuv420p(
                    self.width,
                    self.height,
                    y_plane.clone(),
                    u_plane.clone(),
                    v_plane.clone(),
                )
            }
        }
    }
}

/// Zero-copy converted frame result.
///
/// This enum represents the result of a zero-copy conversion attempt:
/// - If in-place conversion succeeded, contains the original Arc<FrameBuffer>
/// - If fallback was needed, contains the traditional ConvertedFrame
#[derive(Debug)]
pub enum ConvertedFrameZeroCopy {
    /// In-place conversion succeeded (zero-copy).
    InPlace(Arc<FrameBuffer>),
    /// Fallback to allocation (copy required).
    Owned(ConvertedFrame, u32, u32),
}

impl ConvertedFrameZeroCopy {
    /// Convert to a VideoFrame for encoding.
    pub fn to_video_frame(self) -> VideoFrame {
        match self {
            ConvertedFrameZeroCopy::InPlace(buffer) => VideoFrame::from_arc_frame_buffer(buffer),
            ConvertedFrameZeroCopy::Owned(frame, _width, _height) => frame.to_video_frame(),
        }
    }
}

/// SIMD-accelerated NV12 converter.
pub struct SimdNv12Converter {
    strategy: ConversionStrategy,
}

impl SimdNv12Converter {
    /// Create a new SIMD NV12 converter.
    pub fn new() -> Self {
        Self {
            strategy: ConversionStrategy::optimal(),
        }
    }
}

impl Default for SimdNv12Converter {
    fn default() -> Self {
        Self::new()
    }
}

impl ColorSpaceConverter for SimdNv12Converter {
    fn convert(&self, frame: &DecodedFrame) -> io::Result<ConvertedFrame> {
        let (y_plane, uv_plane) = rgb_to_nv12(
            frame.data(),
            frame.width() as usize,
            frame.height() as usize,
        )
        .map_err(|e| io::Error::other(e.to_string()))?;

        trace!(
            width = frame.width(),
            height = frame.height(),
            strategy = ?self.strategy,
            "RGB→NV12 conversion completed"
        );

        Ok(ConvertedFrame {
            width: frame.width(),
            height: frame.height(),
            format: FrameFormat::Nv12,
            data: FrameData::Nv12 { y_plane, uv_plane },
        })
    }

    fn convert_zero_copy(&self, frame: DecodedFrame) -> io::Result<ConvertedFrameZeroCopy> {
        let width = frame.width();
        let height = frame.height();

        // Get the Arc<FrameBuffer> from the frame
        let mut buffer = frame.into_buffer();

        // Try in-place conversion if the buffer is RGB24 and we have exclusive access
        if buffer.format() == PixelFormat::Rgb24 {
            // Try to get exclusive access via Arc::try_unwrap
            // If successful, we can do in-place conversion
            match Arc::try_unwrap(buffer) {
                Ok(mut frame_buffer) => {
                    // We have exclusive access - do in-place conversion
                    let new_len = rgb_to_nv12_in_place(
                        frame_buffer.data_mut(),
                        width as usize,
                        height as usize,
                    )
                    .map_err(|e| io::Error::other(e.to_string()))?;

                    // Update the buffer metadata
                    frame_buffer.set_format(PixelFormat::Nv12);
                    frame_buffer.set_len(new_len);

                    trace!(
                        width,
                        height,
                        strategy = ?self.strategy,
                        "RGB→NV12 zero-copy in-place conversion completed"
                    );

                    return Ok(ConvertedFrameZeroCopy::InPlace(Arc::new(frame_buffer)));
                }
                Err(shared_buffer) => {
                    // Buffer is shared - allocate new buffer for converted data
                    trace!("RGB→NV12: buffer is shared, allocating new buffer");
                    buffer = shared_buffer;
                }
            }
        }

        // Fallback: convert to new Arc<FrameBuffer> to avoid double-copy
        // This allocates once, then wraps in Arc for zero-copy passing
        let (y_plane, uv_plane) = rgb_to_nv12(buffer.data(), width as usize, height as usize)
            .map_err(|e| io::Error::other(e.to_string()))?;

        // Calculate NV12 data size
        let y_size = (width as usize) * (height as usize);
        let uv_size = (width as usize / 2) * (height as usize / 2) * 2;
        let total_size = y_size + uv_size;

        // Allocate single contiguous buffer for NV12 data
        let mut nv12_data = Vec::with_capacity(total_size);
        nv12_data.extend_from_slice(&y_plane);
        nv12_data.extend_from_slice(&uv_plane);

        // Create Arc<FrameBuffer> directly - eliminates clone in to_video_frame()
        let frame_buffer = FrameBuffer::from_owned(nv12_data, PixelFormat::Nv12, width, height);

        trace!(
            width,
            height,
            strategy = ?self.strategy,
            "RGB→NV12 zero-copy allocated conversion completed"
        );

        Ok(ConvertedFrameZeroCopy::InPlace(Arc::new(frame_buffer)))
    }
}

/// SIMD-accelerated YUV420P converter.
pub struct SimdYuv420pConverter {
    strategy: ConversionStrategy,
}

impl SimdYuv420pConverter {
    /// Create a new SIMD YUV420P converter.
    pub fn new() -> Self {
        Self {
            strategy: ConversionStrategy::optimal(),
        }
    }
}

impl Default for SimdYuv420pConverter {
    fn default() -> Self {
        Self::new()
    }
}

impl ColorSpaceConverter for SimdYuv420pConverter {
    fn convert(&self, frame: &DecodedFrame) -> io::Result<ConvertedFrame> {
        let (y_plane, u_plane, v_plane) = rgb_to_yuv420p(
            frame.data(),
            frame.width() as usize,
            frame.height() as usize,
        )
        .map_err(|e| io::Error::other(e.to_string()))?;

        trace!(
            width = frame.width(),
            height = frame.height(),
            strategy = ?self.strategy,
            "RGB→YUV420P conversion completed"
        );

        Ok(ConvertedFrame {
            width: frame.width(),
            height: frame.height(),
            format: FrameFormat::Yuv420p,
            data: FrameData::Yuv420p {
                y_plane,
                u_plane,
                v_plane,
            },
        })
    }

    fn convert_zero_copy(&self, frame: DecodedFrame) -> io::Result<ConvertedFrameZeroCopy> {
        let width = frame.width();
        let height = frame.height();

        // Get the Arc<FrameBuffer> from the frame
        let buffer = frame.into_buffer();

        // Note: We don't have an in-place YUV420P converter yet, so we always
        // allocate a new buffer. This still avoids the second clone that would
        // happen in ConvertedFrame::to_video_frame().

        // Convert to new Arc<FrameBuffer> to avoid double-copy
        let (y_plane, u_plane, v_plane) =
            rgb_to_yuv420p(buffer.data(), width as usize, height as usize)
                .map_err(|e| io::Error::other(e.to_string()))?;

        // Calculate YUV420P data size
        let y_size = (width as usize) * (height as usize);
        let uv_size = (width as usize / 2) * (height as usize / 2);
        let total_size = y_size + uv_size * 2;

        // Allocate single contiguous buffer for YUV420P data
        let mut yuv_data = Vec::with_capacity(total_size);
        yuv_data.extend_from_slice(&y_plane);
        yuv_data.extend_from_slice(&u_plane);
        yuv_data.extend_from_slice(&v_plane);

        // Create Arc<FrameBuffer> directly - eliminates clone in to_video_frame()
        let frame_buffer = FrameBuffer::from_owned(yuv_data, PixelFormat::Yuv420p, width, height);

        trace!(
            width,
            height,
            strategy = ?self.strategy,
            "RGB→YUV420P zero-copy allocated conversion completed"
        );

        Ok(ConvertedFrameZeroCopy::InPlace(Arc::new(frame_buffer)))
    }
}

/// Command for the convert pool.
#[derive(Debug)]
pub struct ConvertCommand {
    /// Sequence number for ordering.
    pub sequence: u64,
    /// Camera identifier.
    pub camera_id: String,
    /// Frames to convert.
    pub frames: Vec<DecodedFrame>,
}

/// Result from the convert pool.
#[derive(Debug)]
pub struct ConvertResult {
    /// Sequence number for ordering.
    pub sequence: u64,
    /// Camera identifier.
    pub camera_id: String,
    /// Conversion result.
    pub result: io::Result<Vec<ConvertedFrameZeroCopy>>,
}

impl ConvertResult {
    /// Convert all frames to VideoFrame (handles both zero-copy and copy paths).
    pub fn to_video_frames(self) -> io::Result<Vec<VideoFrame>> {
        self.result
            .map(|frames| frames.into_iter().map(|f| f.to_video_frame()).collect())
    }
}

/// Configuration for the convert pool.
#[derive(Debug, Clone)]
pub struct ConvertPoolConfig {
    /// Number of worker threads.
    pub worker_count: usize,
    /// Pending command capacity.
    pub pending_capacity: usize,
    /// Completed result capacity.
    pub completed_capacity: usize,
    /// Target pixel format (NV12 or YUV420P).
    pub target_format: TargetFormat,
    /// Enable zero-copy conversion (in-place when possible).
    pub zero_copy: bool,
}

impl Default for ConvertPoolConfig {
    fn default() -> Self {
        let cpu_count = num_cpus::get();
        // Allocate ~20% of CPUs to conversion (SIMD is fast)
        let convert_workers = (cpu_count * 2 / 10).max(1);

        Self {
            worker_count: convert_workers,
            pending_capacity: 512,
            completed_capacity: 512,
            target_format: TargetFormat::Nv12,
            zero_copy: true, // Enable zero-copy by default
        }
    }
}

/// Target format for conversion.
#[derive(Debug, Clone, Copy, Default)]
pub enum TargetFormat {
    /// NV12 (semi-planar, preferred for hardware encoders).
    #[default]
    Nv12,
    /// YUV420P (planar, for software encoders).
    Yuv420p,
}

/// Statistics for the convert pool.
#[derive(Debug, Clone, Copy, Default)]
pub struct ConvertPoolStats {
    /// Total frames converted.
    pub frames_converted: u64,
    /// Total frames failed.
    pub frames_failed: u64,
    /// Current pending queue size.
    pub pending_count: usize,
    /// Active workers.
    pub active_workers: usize,
}

/// Parallel color space conversion pool.
pub struct ConvertPool {
    /// Worker handles.
    workers: Vec<JoinHandle<()>>,
    /// Channel to send convert commands.
    cmd_tx: Sender<ConvertCommand>,
    /// Channel to receive convert results.
    result_rx: Receiver<ConvertResult>,
    /// Worker count.
    worker_count: usize,
    /// Frames converted (shared with workers).
    stats_converted: Arc<AtomicU64>,
    /// Frames failed (shared with workers).
    stats_failed: Arc<AtomicU64>,
    /// Active worker counter.
    active_workers: Arc<AtomicUsize>,
    /// In-flight commands being processed by workers.
    in_flight: Arc<AtomicUsize>,
}

impl ConvertPool {
    /// Create a new convert pool.
    pub fn new(config: ConvertPoolConfig) -> io::Result<Self> {
        let (cmd_tx, cmd_rx) = bounded(config.pending_capacity);
        let (result_tx, result_rx) = bounded(config.completed_capacity);

        // Spawn workers
        let mut workers = Vec::with_capacity(config.worker_count);
        let cmd_rx = Arc::new(cmd_rx);
        let result_tx = Arc::new(result_tx);

        // Create shared stats counters
        let stats_converted = Arc::new(AtomicU64::new(0));
        let stats_failed = Arc::new(AtomicU64::new(0));
        let active_workers = Arc::new(AtomicUsize::new(config.worker_count));
        let in_flight = Arc::new(AtomicUsize::new(0));

        for worker_id in 0..config.worker_count {
            let cmd_rx = Arc::clone(&cmd_rx);
            let result_tx = Arc::clone(&result_tx);
            let target_format = config.target_format;
            let zero_copy = config.zero_copy;
            let stats_converted = Arc::clone(&stats_converted);
            let stats_failed = Arc::clone(&stats_failed);
            let active_workers = Arc::clone(&active_workers);
            let in_flight = Arc::clone(&in_flight);

            let handle = std::thread::Builder::new()
                .name(format!("convert-worker-{}", worker_id))
                .spawn(move || {
                    convert_worker_loop(
                        worker_id,
                        cmd_rx,
                        result_tx,
                        target_format,
                        zero_copy,
                        &stats_converted,
                        &stats_failed,
                        &active_workers,
                        &in_flight,
                    );
                })
                .map_err(|e| io::Error::other(e.to_string()))?;

            workers.push(handle);
        }

        // Drop original senders
        drop(cmd_rx);
        drop(result_tx);

        Ok(Self {
            workers,
            cmd_tx,
            result_rx,
            worker_count: config.worker_count,
            stats_converted,
            stats_failed,
            active_workers,
            in_flight,
        })
    }

    /// Submit frames for conversion.
    pub fn submit(&self, cmd: ConvertCommand) -> io::Result<()> {
        self.cmd_tx
            .send(cmd)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Convert pool shut down"))
    }

    /// Try to receive a conversion result (non-blocking).
    pub fn try_recv(&self) -> Option<ConvertResult> {
        self.result_rx.try_recv().ok()
    }

    /// Receive a conversion result (blocking).
    pub fn recv(&self) -> io::Result<ConvertResult> {
        self.result_rx
            .recv()
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Convert pool shut down"))
    }

    /// Get statistics.
    pub fn stats(&self) -> ConvertPoolStats {
        ConvertPoolStats {
            frames_converted: self.stats_converted.load(Ordering::Relaxed),
            frames_failed: self.stats_failed.load(Ordering::Relaxed),
            pending_count: self.cmd_tx.len() + self.in_flight.load(Ordering::Relaxed),
            active_workers: self.active_workers.load(Ordering::Relaxed),
        }
    }

    /// Shutdown the pool.
    pub fn shutdown(self) {
        drop(self.cmd_tx);
        for worker in self.workers {
            let _ = worker.join();
        }
    }

    /// Get worker count.
    pub fn worker_count(&self) -> usize {
        self.worker_count
    }
}

/// Convert worker loop.
#[allow(clippy::too_many_arguments)]
fn convert_worker_loop(
    worker_id: usize,
    cmd_rx: Arc<Receiver<ConvertCommand>>,
    result_tx: Arc<Sender<ConvertResult>>,
    target_format: TargetFormat,
    zero_copy: bool,
    stats_converted: &Arc<AtomicU64>,
    stats_failed: &Arc<AtomicU64>,
    active_workers: &Arc<AtomicUsize>,
    in_flight: &Arc<AtomicUsize>,
) {
    debug!(
        worker_id,
        format = ?target_format,
        zero_copy,
        "Convert worker started"
    );

    // Create converter based on target format
    let converter: Box<dyn ColorSpaceConverter> = match target_format {
        TargetFormat::Nv12 => Box::new(SimdNv12Converter::new()),
        TargetFormat::Yuv420p => Box::new(SimdYuv420pConverter::new()),
    };

    while let Ok(cmd) = cmd_rx.recv() {
        in_flight.fetch_add(1, Ordering::Relaxed);

        trace!(
            worker_id,
            sequence = cmd.sequence,
            camera = %cmd.camera_id,
            frames = cmd.frames.len(),
            "Processing convert command"
        );

        // Convert all frames - use zero-copy path when enabled
        let mut converted_frames = Vec::with_capacity(cmd.frames.len());
        let mut all_ok = true;

        for frame in cmd.frames {
            let convert_result = if zero_copy {
                // New path: zero-copy conversion (in-place when possible)
                converter.convert_zero_copy(frame)
            } else {
                // Legacy path: traditional conversion with copy
                let width = frame.width();
                let height = frame.height();
                converter
                    .convert(&frame)
                    .map(|f| ConvertedFrameZeroCopy::Owned(f, width, height))
            };

            match convert_result {
                Ok(converted) => {
                    stats_converted.fetch_add(1, Ordering::Relaxed);
                    converted_frames.push(converted);
                }
                Err(e) => {
                    stats_failed.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        worker_id,
                        error = %e,
                        "Frame conversion failed"
                    );
                    all_ok = false;
                    break;
                }
            }
        }

        let result = if all_ok {
            ConvertResult {
                sequence: cmd.sequence,
                camera_id: cmd.camera_id,
                result: Ok(converted_frames),
            }
        } else {
            ConvertResult {
                sequence: cmd.sequence,
                camera_id: cmd.camera_id,
                result: Err(io::Error::other("Frame conversion failed")),
            }
        };

        in_flight.fetch_sub(1, Ordering::Relaxed);
        if result_tx.send(result).is_err() {
            break;
        }
    }

    active_workers.fetch_sub(1, Ordering::Relaxed);
    debug!(worker_id, "Convert worker exiting");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::FrameBuffer;

    fn make_test_frame(width: u32, height: u32) -> DecodedFrame {
        let data: Vec<u8> = (0..(width * height * 3)).map(|i| (i % 256) as u8).collect();
        DecodedFrame::new(FrameBuffer::rgb24(width, height, data))
    }

    #[test]
    fn test_simd_nv12_converter() {
        let converter = SimdNv12Converter::new();
        let frame = make_test_frame(64, 64);

        let result = converter.convert(&frame);
        assert!(result.is_ok());

        let converted = result.unwrap();
        assert_eq!(converted.width, 64);
        assert_eq!(converted.height, 64);
        assert!(matches!(converted.format, FrameFormat::Nv12));
    }

    #[test]
    fn test_simd_yuv420p_converter() {
        let converter = SimdYuv420pConverter::new();
        let frame = make_test_frame(64, 64);

        let result = converter.convert(&frame);
        assert!(result.is_ok());

        let converted = result.unwrap();
        assert_eq!(converted.width, 64);
        assert_eq!(converted.height, 64);
    }

    #[test]
    fn test_convert_pool_config_default() {
        let config = ConvertPoolConfig::default();
        assert!(config.worker_count >= 1);
        assert!(config.pending_capacity > 0);
    }

    #[test]
    fn test_convert_pool_create() {
        let config = ConvertPoolConfig {
            worker_count: 2,
            ..Default::default()
        };
        let pool = ConvertPool::new(config);
        assert!(pool.is_ok());

        let pool = pool.unwrap();
        assert_eq!(pool.worker_count(), 2);
        pool.shutdown();
    }

    #[test]
    fn test_convert_pool_basic() {
        let config = ConvertPoolConfig {
            worker_count: 1,
            pending_capacity: 8,
            completed_capacity: 8,
            target_format: TargetFormat::Nv12,
            zero_copy: true,
        };

        let pool = ConvertPool::new(config).unwrap();

        // Submit a frame for conversion
        let cmd = ConvertCommand {
            sequence: 0,
            camera_id: "test".to_string(),
            frames: vec![make_test_frame(64, 64)],
        };

        assert!(pool.submit(cmd).is_ok());

        // Receive the result
        let result = pool.recv().expect("Should receive result");
        assert_eq!(result.sequence, 0);
        assert!(result.result.is_ok());

        let frames = result.result.unwrap();
        assert_eq!(frames.len(), 1);

        pool.shutdown();
    }
}
