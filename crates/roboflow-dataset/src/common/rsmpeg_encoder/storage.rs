// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Streaming encoder with storage upload.

use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use roboflow_core::{Result, RoboflowError};
use roboflow_storage::Storage;

use super::config::RsmpegEncoderConfig;
use super::encoder::RsmpegEncoder;

/// Streaming encoder that writes encoded video directly to cloud/local storage.
///
/// This combines the RsmpegEncoder with storage upload.
pub struct StorageRsmpegEncoder {
    /// Inner encoder
    encoder: RsmpegEncoder,

    /// Storage backend
    storage: Arc<dyn Storage>,

    /// Destination path
    dest_path: String,

    /// Shared buffer for encoded data
    encoded_data: Arc<std::sync::Mutex<Vec<u8>>>,

    /// Join handle for the collector thread
    collector_handle: Option<std::thread::JoinHandle<()>>,

    /// Sender to signal collector thread to stop
    collector_stop_tx: Option<std::sync::mpsc::Sender<()>>,

    /// Frames encoded
    frames_encoded: usize,
}

impl StorageRsmpegEncoder {
    /// Create a new storage rsmpeg encoder.
    ///
    /// # Arguments
    ///
    /// * `dest_path` - Destination path (e.g., "s3://bucket/path/video.mp4" or "/local/path/video.mp4")
    /// * `storage` - Storage backend
    /// * `config` - Encoder configuration
    pub fn new(
        dest_path: &str,
        storage: Arc<dyn Storage>,
        config: RsmpegEncoderConfig,
    ) -> Result<Self> {
        // Create channel for encoded fragments and stop signal
        let (encoded_tx, encoded_rx) = std::sync::mpsc::channel();
        let (stop_tx, stop_rx) = std::sync::mpsc::channel();

        // Create the encoder
        let encoder = RsmpegEncoder::new(config, encoded_tx)?;

        let encoded_data: Arc<std::sync::Mutex<Vec<u8>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));

        // Spawn collector thread with proper handle storage
        let data_ref = Arc::clone(&encoded_data);
        let handle = std::thread::spawn(move || {
            loop {
                // Check for stop signal first
                match stop_rx.try_recv() {
                    Ok(_) | Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
                    Err(std::sync::mpsc::TryRecvError::Empty) => {}
                }

                match encoded_rx.recv_timeout(std::time::Duration::from_millis(100)) {
                    Ok(fragment) => {
                        if let Ok(mut data) = data_ref.lock() {
                            data.extend_from_slice(&fragment);
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        // Timeout is ok, check stop signal again
                        continue;
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                        // Channel closed, encoder is done
                        break;
                    }
                }
            }
        });

        Ok(Self {
            encoder,
            storage,
            dest_path: dest_path.to_string(),
            encoded_data,
            collector_handle: Some(handle),
            collector_stop_tx: Some(stop_tx),
            frames_encoded: 0,
        })
    }

    /// Add a frame for encoding.
    pub fn add_frame(&mut self, rgb_data: &[u8]) -> Result<()> {
        self.encoder.add_frame(rgb_data)?;
        self.frames_encoded += 1;
        Ok(())
    }

    /// Add a frame from ImageData.
    pub fn add_image_frame(&mut self, image_data: &[u8]) -> Result<()> {
        self.encoder.add_frame(image_data)?;
        self.frames_encoded += 1;
        Ok(())
    }

    /// Finalize encoding and upload to storage.
    pub fn finalize(mut self) -> Result<(String, usize)> {
        // Finalize encoder (sends trailer and closes channel)
        self.encoder.finalize()?;

        // Signal collector thread to stop
        if let Some(tx) = self.collector_stop_tx.take() {
            let _ = tx.send(());
        }

        // Wait for collector thread to finish with a timeout
        if let Some(handle) = self.collector_handle.take()
            && let Err(e) = handle.join()
        {
            return Err(RoboflowError::encode(
                "StorageRsmpegEncoder",
                format!("Collector thread failed: {:?}", e),
            ));
        }

        // Get the encoded data
        let data = {
            let guard = self.encoded_data.lock().map_err(|e| {
                RoboflowError::encode(
                    "StorageRsmpegEncoder",
                    format!("Failed to acquire lock on encoded data: {}", e),
                )
            })?;
            guard.clone()
        };

        // Write to storage
        let path = Path::new(&self.dest_path);
        let mut writer = self.storage.writer(path).map_err(|e| {
            RoboflowError::encode(
                "StorageRsmpegEncoder",
                format!("Failed to create writer: {}", e),
            )
        })?;

        writer.write_all(&data).map_err(|e| {
            RoboflowError::encode(
                "StorageRsmpegEncoder",
                format!("Failed to write data: {}", e),
            )
        })?;

        writer.flush().map_err(|e| {
            RoboflowError::encode("StorageRsmpegEncoder", format!("Failed to flush: {}", e))
        })?;

        tracing::info!(
            bytes = data.len(),
            frames = self.frames_encoded,
            path = %self.dest_path,
            "Storage upload completed"
        );

        Ok((self.dest_path.clone(), self.frames_encoded))
    }

    /// Get the number of frames encoded.
    pub fn frame_count(&self) -> usize {
        self.frames_encoded
    }
}
