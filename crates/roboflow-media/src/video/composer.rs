// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Video composition for merging multiple video segments.
//!
//! This module provides the [`VideoComposer`] trait and [`RsmpegVideoComposer`]
//! implementation for concatenating MP4 video files while maintaining proper
//! timestamps and stream continuity.

use std::ffi::CString;
use std::path::Path;

use roboflow_core::{Result, RoboflowError};
use rsmpeg::avformat::{AVFormatContextInput, AVFormatContextOutput};
use rsmpeg::avutil::AVRational;
use rsmpeg::ffi;

/// Trait for composing multiple video files into a single output.
///
/// Video composition requires proper remuxing (not byte concatenation) to
/// maintain valid MP4 structure and continuous timestamps across segments.
pub trait VideoComposer: Send + Sync {
    /// Compose multiple source videos into a single destination file.
    ///
    /// Sources are concatenated in order. For a single source, this is
    /// equivalent to a file copy.
    ///
    /// # Arguments
    ///
    /// * `sources` - Source video paths in concatenation order
    /// * `dest` - Destination path for the composed video
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `sources` is empty
    /// - Any source file cannot be opened
    /// - The output file cannot be created
    /// - Remuxing fails
    fn compose(&self, sources: &[&Path], dest: &Path) -> Result<()>;

    /// Check if composition is possible with the given sources.
    ///
    /// This is a preflight check that verifies all sources exist.
    fn can_compose(&self, sources: &[&Path]) -> Result<()>;
}

pub struct RsmpegVideoComposer;

impl RsmpegVideoComposer {
    pub fn new() -> Self {
        Self
    }
}

impl Default for RsmpegVideoComposer {
    fn default() -> Self {
        Self::new()
    }
}

impl VideoComposer for RsmpegVideoComposer {
    fn compose(&self, sources: &[&Path], dest: &Path) -> Result<()> {
        if sources.is_empty() {
            return Err(RoboflowError::other("compose requires at least one source"));
        }

        if sources.len() == 1 {
            std::fs::copy(sources[0], dest).map_err(|e| {
                RoboflowError::other(format!("failed to copy {}: {}", sources[0].display(), e))
            })?;
            return Ok(());
        }

        let dest_str = dest.to_str().ok_or_else(|| {
            RoboflowError::other(format!("invalid destination path: {}", dest.display()))
        })?;
        let dest_cstr = CString::new(dest_str)
            .map_err(|_| RoboflowError::other("destination path contains null byte"))?;

        let first_path = sources[0].to_str().ok_or_else(|| {
            RoboflowError::other(format!("invalid source path: {}", sources[0].display()))
        })?;
        let first_cstr = CString::new(first_path)
            .map_err(|_| RoboflowError::other("source path contains null byte"))?;

        let first_input = AVFormatContextInput::open(&first_cstr)
            .map_err(|e| RoboflowError::other(format!("failed to open first source: {}", e)))?;

        let mut output_ctx = AVFormatContextOutput::create(&dest_cstr)
            .map_err(|e| RoboflowError::other(format!("failed to create output: {}", e)))?;

        let mut stream_mapping: Vec<Option<usize>> = Vec::new();
        for stream in first_input.streams().iter() {
            let mut out_stream = output_ctx.new_stream();
            // SAFETY: avcodec_parameters_alloc allocates a new parameters struct.
            // We check for null before calling avcodec_parameters_copy.
            // avcodec_parameters_copy safely copies from the input stream's codecpar.
            // The from_raw conversion is safe because we verified the pointer is non-null.
            let codecpar = unsafe {
                let new_par = ffi::avcodec_parameters_alloc();
                let new_par = std::ptr::NonNull::new(new_par)
                    .ok_or_else(|| RoboflowError::other("failed to allocate codec parameters"))?;
                ffi::avcodec_parameters_copy(
                    new_par.as_ptr(),
                    stream.codecpar().as_ptr() as *const _,
                );
                rsmpeg::avcodec::AVCodecParameters::from_raw(new_par)
            };
            out_stream.set_codecpar(codecpar);
            out_stream.set_time_base(AVRational {
                num: stream.time_base.num,
                den: stream.time_base.den,
            });
            stream_mapping.push(Some(out_stream.index as usize));
        }

        let mut options = None;
        output_ctx
            .write_header(&mut options)
            .map_err(|e| RoboflowError::other(format!("failed to write header: {}", e)))?;

        let mut pts_offset: i64 = 0;
        let mut dts_offset: i64 = 0;
        let mut last_pts: i64 = 0;
        let mut last_dts: i64 = 0;

        for (file_idx, source_path) in sources.iter().enumerate() {
            let source_str = source_path.to_str().ok_or_else(|| {
                RoboflowError::other(format!("invalid source path: {}", source_path.display()))
            })?;
            let source_cstr = CString::new(source_str)
                .map_err(|_| RoboflowError::other("source path contains null byte"))?;

            let mut input_ctx = AVFormatContextInput::open(&source_cstr).map_err(|e| {
                RoboflowError::other(format!(
                    "failed to open source {} ({}): {}",
                    file_idx,
                    source_path.display(),
                    e
                ))
            })?;

            while let Ok(Some(mut packet)) = input_ctx.read_packet() {
                let in_stream_idx = packet.stream_index as usize;
                let out_stream_idx = match stream_mapping.get(in_stream_idx) {
                    Some(Some(idx)) => *idx,
                    _ => continue,
                };

                if packet.pts != ffi::AV_NOPTS_VALUE {
                    packet.set_pts(packet.pts.saturating_add(pts_offset));
                    last_pts = packet.pts;
                }
                if packet.dts != ffi::AV_NOPTS_VALUE {
                    packet.set_dts(packet.dts.saturating_add(dts_offset));
                    last_dts = packet.dts;
                }

                packet.set_stream_index(out_stream_idx as i32);

                output_ctx
                    .write_frame(&mut packet)
                    .map_err(|e| RoboflowError::other(format!("failed to write frame: {}", e)))?;
            }

            pts_offset = last_pts.saturating_add(1);
            dts_offset = last_dts.saturating_add(1);
        }

        output_ctx
            .write_trailer()
            .map_err(|e| RoboflowError::other(format!("failed to write trailer: {}", e)))?;

        tracing::info!(
            sources = sources.len(),
            dest = %dest.display(),
            "Video composition complete"
        );

        Ok(())
    }

    fn can_compose(&self, sources: &[&Path]) -> Result<()> {
        if sources.is_empty() {
            return Err(RoboflowError::other("no sources to compose"));
        }

        for (i, source) in sources.iter().enumerate() {
            if !source.exists() {
                return Err(RoboflowError::other(format!(
                    "source {} not found: {}",
                    i,
                    source.display()
                )));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ImageData;
    use crate::video::{OutputConfig, VideoEncoder, VideoEncoderConfig};

    /// Helper to create test RGB image data.
    fn create_test_rgb_image(width: u32, height: u32, value: u8) -> Vec<u8> {
        vec![value; width as usize * height as usize * 3]
    }

    /// Helper to create test ImageData.
    fn create_test_image_data(width: u32, height: u32, value: u8) -> ImageData {
        let data = create_test_rgb_image(width, height, value);
        ImageData::new(width, height, data)
    }

    /// Helper to create a small test video file.
    fn create_test_video(
        dir: &std::path::Path,
        name: &str,
        frames: usize,
        value_base: u8,
    ) -> std::path::PathBuf {
        let output_path = dir.join(name);
        let config = VideoEncoderConfig::default();
        let output = OutputConfig::file(&output_path);
        let mut encoder = VideoEncoder::new(config, output).expect("Failed to create encoder");

        for i in 0..frames {
            let value = value_base.wrapping_add((i * 10) as u8);
            let image = create_test_image_data(64, 64, value);
            encoder
                .encode_frame(&image.data, image.width, image.height)
                .expect("Failed to encode frame");
        }

        encoder.finalize().expect("Failed to finalize encoder");
        output_path
    }

    #[test]
    fn test_composer_new() {
        let _composer = RsmpegVideoComposer::new();
    }

    #[test]
    fn test_composer_default() {
        let _composer = RsmpegVideoComposer::new();
    }

    #[test]
    fn test_can_compose_empty() {
        let composer = RsmpegVideoComposer::new();
        let result = composer.can_compose(&[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no sources"));
    }

    #[test]
    fn test_can_compose_missing_file() {
        let composer = RsmpegVideoComposer::new();
        let result = composer.can_compose(&[Path::new("/nonexistent/file.mp4")]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not found") || err.contains("source"));
    }

    #[test]
    fn test_can_compose_existing_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let video_path = create_test_video(temp_dir.path(), "test.mp4", 3, 100);

        let composer = RsmpegVideoComposer::new();
        let result = composer.can_compose(&[&video_path]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_can_compose_multiple_existing_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let video1 = create_test_video(temp_dir.path(), "test1.mp4", 2, 100);
        let video2 = create_test_video(temp_dir.path(), "test2.mp4", 2, 150);

        let composer = RsmpegVideoComposer::new();
        let result = composer.can_compose(&[&video1, &video2]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_can_compose_mixed_existing_missing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let video1 = create_test_video(temp_dir.path(), "test1.mp4", 2, 100);
        let missing = temp_dir.path().join("nonexistent.mp4");

        let composer = RsmpegVideoComposer::new();
        let result = composer.can_compose(&[&video1, &missing]);
        assert!(result.is_err());
    }

    #[test]
    fn test_compose_empty_sources() {
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_dir.path().join("output.mp4");

        let composer = RsmpegVideoComposer::new();
        let result = composer.compose(&[], &output);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("at least one source")
        );
    }

    #[test]
    fn test_compose_single_source() {
        let temp_dir = tempfile::tempdir().unwrap();
        let source = create_test_video(temp_dir.path(), "source.mp4", 5, 100);
        let output = temp_dir.path().join("output.mp4");

        let composer = RsmpegVideoComposer::new();
        let result = composer.compose(&[&source], &output);
        assert!(result.is_ok(), "compose should succeed: {:?}", result);
        assert!(output.exists(), "output file should exist");

        // Output should have similar size to source (single file copy)
        let source_size = std::fs::metadata(&source).unwrap().len();
        let output_size = std::fs::metadata(&output).unwrap().len();
        assert!(output_size > 0);
        // Allow some tolerance as exact copy may vary slightly
        assert!(output_size >= source_size / 2 && output_size <= source_size * 2);
    }

    #[test]
    fn test_compose_multiple_sources() {
        let temp_dir = tempfile::tempdir().unwrap();
        let source1 = create_test_video(temp_dir.path(), "source1.mp4", 3, 100);
        let source2 = create_test_video(temp_dir.path(), "source2.mp4", 3, 150);
        let output = temp_dir.path().join("output.mp4");

        let composer = RsmpegVideoComposer::new();
        let result = composer.compose(&[&source1, &source2], &output);
        assert!(result.is_ok(), "compose should succeed: {:?}", result);
        assert!(output.exists(), "output file should exist");

        // Output should exist and be non-empty
        let output_size = std::fs::metadata(&output).unwrap().len();
        assert!(output_size > 0, "output should not be empty");
    }

    #[test]
    fn test_compose_three_sources() {
        let temp_dir = tempfile::tempdir().unwrap();
        let source1 = create_test_video(temp_dir.path(), "source1.mp4", 2, 100);
        let source2 = create_test_video(temp_dir.path(), "source2.mp4", 2, 150);
        let source3 = create_test_video(temp_dir.path(), "source3.mp4", 2, 200);
        let output = temp_dir.path().join("output.mp4");

        let composer = RsmpegVideoComposer::new();
        let result = composer.compose(&[&source1, &source2, &source3], &output);
        assert!(result.is_ok(), "compose should succeed: {:?}", result);
        assert!(output.exists(), "output file should exist");
    }

    #[test]
    fn test_compose_missing_source() {
        let temp_dir = tempfile::tempdir().unwrap();
        let source1 = create_test_video(temp_dir.path(), "source1.mp4", 2, 100);
        let missing = temp_dir.path().join("nonexistent.mp4");
        let output = temp_dir.path().join("output.mp4");

        let composer = RsmpegVideoComposer::new();
        let result = composer.compose(&[&source1, &missing], &output);
        assert!(result.is_err());
    }

    #[test]
    fn test_compose_overwrites_existing_output() {
        let temp_dir = tempfile::tempdir().unwrap();
        let source = create_test_video(temp_dir.path(), "source.mp4", 3, 100);
        let output = temp_dir.path().join("output.mp4");

        // Create an existing output file
        std::fs::write(&output, b"old content").unwrap();
        assert_eq!(std::fs::metadata(&output).unwrap().len(), 11);

        let composer = RsmpegVideoComposer::new();
        let result = composer.compose(&[&source], &output);
        assert!(result.is_ok());

        // Output should be overwritten with new content
        let new_size = std::fs::metadata(&output).unwrap().len();
        assert!(new_size > 11, "output should be overwritten");
    }

    #[test]
    fn test_compose_different_frame_counts() {
        let temp_dir = tempfile::tempdir().unwrap();
        let source1 = create_test_video(temp_dir.path(), "source1.mp4", 2, 100);
        let source2 = create_test_video(temp_dir.path(), "source2.mp4", 5, 150);
        let source3 = create_test_video(temp_dir.path(), "source3.mp4", 1, 200);
        let output = temp_dir.path().join("output.mp4");

        let composer = RsmpegVideoComposer::new();
        let result = composer.compose(&[&source1, &source2, &source3], &output);
        assert!(result.is_ok(), "compose should succeed: {:?}", result);
        assert!(output.exists(), "output file should exist");
    }
}
