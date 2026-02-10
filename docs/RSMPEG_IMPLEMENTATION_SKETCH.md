// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # rsmpeg Native Streaming Encoder - Implementation Sketch
//!
//! This document provides detailed implementation guidance for the rsmpeg-based
//! streaming encoder that achieves 1200 MB/s throughput.
//!
//! ## Key Components
//!
//! 1. **RsmpegEncoder** - Native FFmpeg encoder using rsmpeg bindings
//! 2. **AVIOCallback** - Custom write callback for streaming output
//! 3. **FragmentAccumulator** - Buffers fMP4 fragments for S3 upload
//! 4. **EncoderThread** - Per-camera encoding worker

// =============================================================================
// DEPENDENCY UPDATE
// =============================================================================

// In crates/roboflow-dataset/Cargo.toml, make rsmpeg non-optional:
//
// [dependencies]
// rsmpeg = { version = "0.18", features = ["link_system_ffmpeg"] }
//                                ^^^^ REMOVE: optional = true

// =============================================================================
// AVIO WRITE CALLBACK
// =============================================================================

use std::sync::mpsc::Sender;
use std::os::raw::{c_int, c_void};
use std::slice;

/// User data for AVIO write callback
struct AvioOpaque {
    /// Channel to send encoded fragments
    tx: Sender<Vec<u8>>,
    /// Buffer for accumulating small writes
    buffer: Vec<u8>,
    /// Target fragment size before sending
    fragment_size: usize,
}

impl AvioOpaque {
    fn new(tx: Sender<Vec<u8>>, fragment_size: usize) -> Self {
        Self {
            tx,
            buffer: Vec::with_capacity(fragment_size),
            fragment_size,
        }
    }
}

/// Custom write callback for AVIO context.
///
/// This function is called by FFmpeg when encoded data is written.
/// We accumulate data into a buffer and send full fragments through the channel.
///
/// # Safety
///
/// This function must be called with valid pointers from FFmpeg.
extern "C" fn avio_write_callback(
    opaque: *mut c_void,
    buf: *mut u8,
    buf_size: c_int,
) -> c_int {
    unsafe {
        let opaque = &mut *(opaque as *mut AvioOpaque);
        let data = slice::from_raw_parts(buf, buf_size as usize);

        // Extend buffer with new data
        opaque.buffer.extend_from_slice(data);

        // Send full fragments immediately
        while opaque.buffer.len() >= opaque.fragment_size {
            let fragment: Vec<u8> = opaque.buffer.drain(..opaque.fragment_size).collect();

            // Non-blocking send - if channel is full, we'll block here
            // which provides natural backpressure
            if let Err(_) = opaque.tx.send(fragment) {
                // Channel closed - return error
                return ffi::AVERROR_EXTERNAL;
            }
        }

        // Return bytes written (success)
        buf_size
    }
}

/// Seek callback (optional, for non-seekable output)
extern "C" fn avio_seek_callback(
    _opaque: *mut c_void,
    _offset: i64,
    _whence: c_int,
) -> i64 {
    // Non-seekable output - return error
    ffi::AVERROR_EIO
}

// =============================================================================
// RSMPEG ENCODER
// =============================================================================

use rsmpeg::avcodec::*;
use rsmpeg::avformat::*;
use rsmpeg::avutil::*;
use rsmpeg::swscale::*;
use rsmpeg::util::avio::*;
use std::sync::mpsc::{Sender, channel};
use std::time::Duration;

/// Configuration for rsmpeg encoder
#[derive(Debug, Clone)]
pub struct RsmpegEncoderConfig {
    /// Video width
    pub width: u32,

    /// Video height
    pub height: u32,

    /// Frame rate
    pub fps: u32,

    /// Target bitrate (bps)
    pub bitrate: u64,

    /// Codec name (e.g., "h264_nvenc", "libx264")
    pub codec: String,

    /// Pixel format for encoding
    pub pixel_format: &'static str,

    /// CRF quality (0-51 for H.264)
    pub crf: u32,

    /// Preset (e.g., "fast", "medium", "slow")
    pub preset: String,

    /// GOP size (keyframe interval)
    pub gop_size: u32,

    /// Fragment size for fMP4 output
    pub fragment_size: usize,
}

impl Default for RsmpegEncoderConfig {
    fn default() -> Self {
        Self {
            width: 640,
            height: 480,
            fps: 30,
            bitrate: 5_000_000,  // 5 Mbps
            codec: "h264_nvenc".to_string(),
            pixel_format: "nv12",
            crf: 23,
            preset: "p4".to_string(),  // NVENC preset: p1-p7 (p4 = medium)
            gop_size: 30,
            fragment_size: 1024 * 1024,  // 1MB fragments
        }
    }
}

/// Native rsmpeg encoder for streaming video encoding
///
/// This encoder uses FFmpeg libraries directly (in-process) for maximum
/// performance, avoiding the overhead of FFmpeg CLI process spawning.
pub struct RsmpegEncoder {
    /// FFmpeg codec context
    codec_context: AVCodecContext,

    /// SWScale context for pixel format conversion
    sws_context: Option<SwsContext>,

    /// Output format context
    format_context: AVFormatContext,

    /// Custom AVIO context for in-memory output
    _avio_custom: AVIOContextCustom,

    /// Channel for encoded fragments
    encoded_tx: Sender<Vec<u8>>,

    /// Frame counter for PTS
    frame_count: u64,

    /// Configuration
    config: RsmpegEncoderConfig,

    /// Whether the header has been written
    header_written: bool,

    /// Whether the encoder is finalized
    finalized: bool,
}

impl RsmpegEncoder {
    /// Create a new rsmpeg encoder
    ///
    /// # Arguments
    ///
    /// * `config` - Encoder configuration
    /// * `encoded_tx` - Channel to send encoded fragments
    pub fn new(
        config: RsmpegEncoderConfig,
        encoded_tx: Sender<Vec<u8>>,
    ) -> Result<Self, RoboflowError> {
        // =============================================================
        // STEP 1: Find and open codec
        // =============================================================

        // Try NVENC first, fallback to libx264
        let codec = match AVCodec::find_encoder_by_name(&config.codec) {
            Ok(c) => c,
            Err(_) => {
                tracing::warn!(
                    codec = %config.codec,
                    "Codec not found, falling back to libx264"
                );
                AVCodec::find_encoder_by_id(c"AV_CODEC_ID_H264")
                    .map_err(|_| RoboflowError::unsupported("No H.264 encoder available"))?
            }
        };

        tracing::info!(
            codec = codec.name(),
            description = codec.description(),
            "Found encoder"
        );

        // =============================================================
        // STEP 2: Allocate and configure codec context
        // =============================================================

        let mut codec_context = AVCodecContext::new(&codec)
            .map_err(|e| RoboflowError::encode("RsmpegEncoder", format!("Failed to create codec context: {}", e)))?;

        codec_context.set_width(config.width);
        codec_context.set_height(config.height);
        codec_context.set_bit_rate(config.bitrate as i64);
        codec_context.set_time_base(AVRational { num: 1, den: config.fps as i32 });
        codec_context.set_framerate(AVRational { num: config.fps as i32, den: 1 });
        codec_context.set_gop_size(config.gop_size as i32);
        codec_context.set_max_b_frames(1);

        // Set pixel format
        let pix_fmt = match config.pixel_format {
            "nv12" | "yuv420p" => c"yuv420p",
            _ => c"yuv420p",
        };

        // NVENC-specific settings
        if codec.name().contains("nvenc") {
            unsafe {
                let ctx = codec_context.as_mut_ptr();
                // Set RC mode to CBR/VBR
                (*ctx).rc_max_rate = 0;
                (*ctx).rc_buffer_size = 0;
                // Set preset via AVOption
                ffi::av_opt_set(
                    (*ctx).priv_data,
                    c"preset".as_ptr(),
                    config.preset.as_ptr() as *const i8,
                    0,
                );
                // Set CRF
                (*ctx).crf = config.crf as i32;
            }
            codec_context.set_pix_fmt(c"nv12");
        } else {
            // libx264 settings
            unsafe {
                let ctx = codec_context.as_mut_ptr();
                (*ctx).crf = config.crf as i32;

                // Set preset
                ffi::av_opt_set(
                    (*ctx).priv_data,
                    c"preset".as_ptr(),
                    c"medium".as_ptr(),
                    0,
                );
            }
            codec_context.set_pix_fmt(c"yuv420p");
        }

        // Open codec
        codec_context
            .open(&codec, None)
            .map_err(|e| RoboflowError::encode("RsmpegEncoder", format!("Failed to open codec: {}", e)))?;

        // =============================================================
        // STEP 3: Create SWScale context for RGB → YUV conversion
        // =============================================================

        let sws_context = SwsContext::get_context(
            config.width,
            config.height,
            c"rgb24",  // Input format (ImageData is RGB8)
            config.width,
            config.height,
            pix_fmt,
            SWS_BILINEAR,
        ).ok();

        // =============================================================
        // STEP 4: Set up custom AVIO context
        // =============================================================

        // Create opaque data for callback
        let opaque = Box::new(AvioOpaque::new(
            encoded_tx.clone(),
            config.fragment_size,
        ));

        // Create write buffer for AVIO
        let write_buffer = AVMem::new(4 * 1024 * 1024)  // 4MB write buffer
            .map_err(|e| RoboflowError::encode("RsmpegEncoder", format!("Failed to allocate AVIO buffer: {}", e)))?;

        // Create custom AVIO context
        let avio_custom = unsafe {
            AVIOContextCustom::alloc_context_raw(
                write_buffer,
                true,  // write_flag
                Box::into_raw(opaque) as *mut c_void,
                None,  // read_packet
                Some(avio_write_callback),
                Some(avio_seek_callback),
            )
        };

        // =============================================================
        // STEP 5: Create format context
        // =============================================================

        let output_format = AVOutputFormat::muxer_by_name(c"mp4")
            .map_err(|_| RoboflowError::unsupported("MP4 muxer not available"))?;

        let mut format_context = unsafe {
            let mut ptr = std::ptr::null_mut();
            let ret = ffi::avformat_alloc_output_context2(
                &mut ptr,
                std::ptr::null_mut(),
                c"mp4".as_ptr(),
                b"output.mp4\0".as_ptr() as *const i8,
            );
            if ret < 0 || ptr.is_null() {
                return Err(RoboflowError::encode(
                    "RsmpegEncoder",
                    "Failed to allocate output context",
                ));
            }
            AVFormatContext::wrap_pointer(ptr)
        };

        // Set AVIO context (custom I/O)
        format_context.set_pb(Some(avio_custom.inner().clone()));
        format_context.set_oformat(output_format);
        format_context.set_max_interleave_delta(0);

        // =============================================================
        // STEP 6: Create video stream
        // =============================================================

        let stream = format_context
            .new_stream()
            .map_err(|e| RoboflowError::encode("RsmpegEncoder", format!("Failed to create stream: {}", e)))?;

        // Extract codec parameters from codec context
        let codecpar = codec_context.extract_codecpar();
        stream.set_codecpar(codecpar);
        stream.set_time_base(AVRational { num: 1, den: config.fps as i32 });

        tracing::info!(
            width = config.width,
            height = config.height,
            fps = config.fps,
            bitrate = config.bitrate,
            codec = codec.name(),
            "RsmpegEncoder initialized"
        );

        Ok(Self {
            codec_context,
            sws_context,
            format_context,
            _avio_custom: avio_custom,
            encoded_tx,
            frame_count: 0,
            config,
            header_written: false,
            finalized: false,
        })
    }

    /// Write the MP4 header with fragmented MP4 settings
    fn write_header(&mut self) -> Result<(), RoboflowError> {
        if self.header_written {
            return Ok(());
        }

        // Set movflags for fragmented MP4
        let mut opts = vec![
            (c"movflags", c"frag_keyframe+empty_moov+default_base_moof"),
        ];

        // Convert to AVDictionary format for rsmpeg
        unsafe {
            let mut dict = std::ptr::null_mut();
            for (key, val) in opts {
                ffi::av_opt_set(
                    &mut dict as *mut _,
                    key.as_ptr() as *const i8,
                    val.as_ptr() as *const i8,
                    0,
                );
            }

            let ret = ffi::avformat_write_header(
                self.format_context.as_mut_ptr(),
                &dict as *const _,
            );

            ffi::av_dict_free(&mut dict);

            if ret < 0 {
                return Err(RoboflowError::encode(
                    "RsmpegEncoder",
                    format!("Failed to write header: {}", ret),
                ));
            }
        }

        self.header_written = true;
        Ok(())
    }

    /// Add a frame for encoding
    ///
    /// This method:
    /// 1. Converts RGB24 input to the encoder's pixel format
    /// 2. Sends the frame to the encoder
    /// 3. Receives encoded packets
    /// 4. Sends fragments through the channel
    ///
    /// # Arguments
    ///
    /// * `rgb_data` - Raw RGB8 image data (width × height × 3 bytes)
    pub fn add_frame(&mut self, rgb_data: &[u8]) -> Result<(), RoboflowError> {
        if self.finalized {
            return Err(RoboflowError::encode(
                "RsmpegEncoder",
                "Cannot add frame to finalized encoder",
            ));
        }

        // Write header on first frame
        if !self.header_written {
            self.write_header()?;
        }

        let width = self.config.width as usize;
        let height = self.config.height as usize;

        // =============================================================
        // STEP 1: Allocate and populate input frame
        // =============================================================

        let mut input_frame = AVFrame::new();
        input_frame.set_width(width);
        input_frame.set_height(height);
        input_frame.set_format(c"rgb24");

        input_frame
            .get_buffer()
            .map_err(|e| RoboflowError::encode("RsmpegEncoder", format!("Failed to allocate input frame: {}", e)))?;

        // Copy RGB data to frame
        let frame_data = input_frame.data_mut(0).unwrap();
        frame_data[..rgb_data.len()].copy_from_slice(rgb_data);

        // =============================================================
        // STEP 2: Convert pixel format
        // =============================================================

        let mut yuv_frame = AVFrame::new();
        yuv_frame.set_width(width);
        yuv_frame.set_height(height);
        yuv_frame.set_format(self.codec_context.pix_fmt());

        yuv_frame
            .get_buffer()
            .map_err(|e| RoboflowError::encode("RsmpegEncoder", format!("Failed to allocate YUV frame: {}", e)))?;

        // Perform pixel format conversion
        if let Some(ref sws) = self.sws_context {
            sws.scale(
                &input_frame,
                0,  // src slice start
                height,
                &mut yuv_frame,
            ).map_err(|e| RoboflowError::encode("RsmpegEncoder", format!("Pixel format conversion failed: {}", e)))?;
        } else {
            // Direct assignment if no conversion needed
            // (unlikely for RGB24 → YUV420P/NV12)
        }

        // =============================================================
        // STEP 3: Set timestamp
        // =============================================================

        yuv_frame.set_pts(self.frame_count as i64);
        self.frame_count += 1;

        // =============================================================
        // STEP 4: Encode frame
        // =============================================================

        // Send frame to encoder
        self.codec_context
            .send_frame(Some(&yuv_frame))
            .map_err(|e| RoboflowError::encode("RsmpegEncoder", format!("Failed to send frame: {}", e)))?;

        // =============================================================
        // STEP 5: Receive and write encoded packets
        // =============================================================

        loop {
            match self.codec_context.receive_packet() {
                Ok(mut pkt) => {
                    // Write packet to format context (triggers AVIO callback)
                    unsafe {
                        let ret = ffi::av_write_frame(
                            self.format_context.as_mut_ptr(),
                            pkt.as_mut_ptr(),
                        );

                        if ret < 0 {
                            return Err(RoboflowError::encode(
                                "RsmpegEncoder",
                                format!("Failed to write frame: {}", ret),
                            ));
                        }
                    }
                }
                Err(RsmpegError::EncoderAgain) | Err(RsmpegError::EncoderEof) => {
                    // Need more input or end of stream
                    break;
                }
                Err(e) => {
                    return Err(RoboflowError::encode(
                        "RsmpegEncoder",
                        format!("Failed to receive packet: {}", e),
                    ));
                }
            }
        }

        Ok(())
    }

    /// Finalize encoding and write trailer
    pub fn finalize(mut self) -> Result<(), RoboflowError> {
        if self.finalized {
            return Ok(());
        }

        self.finalized = true;

        // =============================================================
        // STEP 1: Flush encoder
        // =============================================================

        // Send NULL frame to signal EOF
        let _ = self.codec_context.send_frame::<AVFrame>(None);

        // Drain remaining packets
        loop {
            match self.codec_context.receive_packet() {
                Ok(mut pkt) => {
                    unsafe {
                        let ret = ffi::av_write_frame(
                            self.format_context.as_mut_ptr(),
                            pkt.as_mut_ptr(),
                        );
                        if ret < 0 {
                            tracing::error!("Failed to write final packet: {}", ret);
                        }
                    }
                }
                Err(RsmpegError::EncoderEof) => break,
                Err(_) => break,
            }
        }

        // =============================================================
        // STEP 2: Write trailer
        // =============================================================

        unsafe {
            let ret = ffi::av_write_trailer(self.format_context.as_mut_ptr());
            if ret < 0 {
                tracing::warn!("Failed to write trailer: {}", ret);
            }
        }

        // =============================================================
        // STEP 3: Flush any remaining AVIO buffer
        // =============================================================

        // The AVIO callback should handle this automatically

        tracing::info!(
            frames = self.frame_count,
            "RsmpegEncoder finalized"
        );

        Ok(())
    }

    /// Get the number of frames encoded
    pub fn frame_count(&self) -> u64 {
        self.frame_count
    }
}

// =============================================================================
// ENCODER THREAD WORKER
// =============================================================================

use std::thread;
use std::sync::{Arc, mpsc};

/// Command sent to encoder thread
pub enum EncoderCommand {
    /// Add a frame for encoding
    AddFrame { image: Arc<ImageData> },

    /// Finish encoding and upload
    Flush,

    /// Shutdown the encoder
    Shutdown,
}

/// Per-camera encoder thread
pub struct EncoderThreadWorker {
    /// Thread handle
    handle: Option<thread::JoinHandle<Result<()>>>,

    /// Command sender
    cmd_tx: mpsc::SyncSender<EncoderCommand>,
}

impl EncoderThreadWorker {
    /// Spawn a new encoder thread for a camera
    ///
    /// # Arguments
    ///
    /// * `camera` - Camera name
    /// * `s3_url` - Destination S3 URL
    /// * `config` - Encoder configuration
    /// * `store` - Object store for upload
    /// * `runtime` - Tokio runtime handle
    pub fn spawn(
        camera: String,
        s3_url: String,
        config: RsmpegEncoderConfig,
        store: Arc<dyn object_store::ObjectStore>,
        runtime: tokio::runtime::Handle,
    ) -> Result<Self, RoboflowError> {
        let (cmd_tx, cmd_rx) = mpsc::sync_channel(64);  // 64 frame buffer

        let handle = thread::spawn(move || {
            Self::worker_loop(camera, s3_url, config, store, runtime, cmd_rx)
        });

        Ok(Self {
            handle: Some(handle),
            cmd_tx,
        })
    }

    /// Worker loop for encoder thread
    fn worker_loop(
        camera: String,
        s3_url: String,
        config: RsmpegEncoderConfig,
        store: Arc<dyn object_store::ObjectStore>,
        runtime: tokio::runtime::Handle,
        cmd_rx: mpsc::Receiver<EncoderCommand>,
    ) -> Result<()> {
        // =============================================================
        // SETUP: Create channels and uploader
        // =============================================================

        let (encoded_tx, encoded_rx) = mpsc::channel::<Vec<u8>>();

        // Parse S3 URL
        let key = parse_s3_url_to_key(&s3_url)?;

        // Create multipart upload
        let multipart = runtime.block_on(async {
            store.put_multipart(&key).await
        }).map_err(|e| RoboflowError::encode("EncoderThread", e.to_string()))?;

        let part_size = config.fragment_size * 16;  // 16 fragments per part

        // =============================================================
        // SPAWN UPLOAD THREAD
        // =============================================================

        let upload_store = Arc::clone(&store);
        let upload_key = key.clone();
        let upload_handle = thread::spawn(move || {
            Self::upload_worker(encoded_rx, upload_store, upload_key, part_size, runtime)
        });

        // =============================================================
        // CREATE ENCODER
        // =============================================================

        let mut encoder = RsmpegEncoder::new(config, encoded_tx)
            .map_err(|e| RoboflowError::encode("EncoderThread", format!("Failed to create encoder: {}", e)))?;

        // =============================================================
        // MAIN LOOP: Process commands
        // =============================================================

        for cmd in cmd_rx {
            match cmd {
                EncoderCommand::AddFrame { image } => {
                    if let Err(e) = encoder.add_frame(&image.data) {
                        tracing::error!(
                            camera = %camera,
                            error = %e,
                            "Failed to encode frame"
                        );
                    }
                }

                EncoderCommand::Flush => {
                    encoder.finalize()?;
                    break;
                }

                EncoderCommand::Shutdown => {
                    encoder.finalize()?;
                    break;
                }
            }
        }

        // =============================================================
        // CLEANUP: Wait for upload thread
        // =============================================================

        upload_handle.join().map_err(|_| {
            RoboflowError::encode("EncoderThread", "Upload thread panicked")
        })??;

        tracing::info!(
            camera = %camera,
            frames = encoder.frame_count(),
            "Encoder thread completed"
        );

        Ok(())
    }

    /// Upload worker - receives encoded fragments and uploads to S3
    fn upload_worker(
        encoded_rx: mpsc::Receiver<Vec<u8>>,
        store: Arc<dyn object_store::ObjectStore>,
        key: ObjectPath,
        part_size: usize,
        runtime: tokio::runtime::Handle,
    ) -> Result<()> {
        let mut buffer = Vec::with_capacity(part_size);
        let mut multipart = object_store::WriteMultipart::new_with_chunk_size(
            runtime.block_on(async {
                store.put_multipart(&key).await
            }).map_err(|e| RoboflowError::encode("UploadWorker", e.to_string()))?,
            part_size,
        );

        for fragment in encoded_rx {
            buffer.extend_from_slice(&fragment);

            // Upload full parts
            while buffer.len() >= part_size {
                let part: Vec<u8> = buffer.drain(..part_size).collect();

                runtime.block_on(async {
                    multipart.put_part(part).await
                }).map_err(|e| RoboflowError::encode("UploadWorker", e.to_string()))?;
            }
        }

        // Upload remaining data
        if !buffer.is_empty() {
            runtime.block_on(async {
                multipart.put_part(buffer).await
            }).map_err(|e| RoboflowError::encode("UploadWorker", e.to_string()))?;
        }

        // Complete multipart upload
        runtime.block_on(async {
            multipart.finish().await
        }).map_err(|e| RoboflowError::encode("UploadWorker", e.to_string()))?;

        Ok(())
    }

    /// Add a frame to the encoder
    pub fn add_frame(&self, image: Arc<ImageData>) -> Result<()> {
        self.cmd_tx.try_send(EncoderCommand::AddFrame { image })
            .map_err(|_| RoboflowError::encode("EncoderThread", "Encoder thread unavailable"))
    }

    /// Flush and finalize encoding
    pub fn flush(self) -> Result<()> {
        // Drop handle and let thread finish naturally
        drop(self.cmd_tx);
        if let Some(handle) = self.handle {
            handle.join().map_err(|_| {
                RoboflowError::encode("EncoderThread", "Thread panicked")
            })?
        }
        Ok(())
    }
}
