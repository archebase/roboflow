// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

use std::ffi::CString;
use std::path::Path;

use roboflow_core::{RoboflowError, VideoComposer};
use rsmpeg::avformat::{AVFormatContextInput, AVFormatContextOutput};
use rsmpeg::avutil::AVRational;
use rsmpeg::ffi;

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
    fn compose(&self, sources: &[&Path], dest: &Path) -> roboflow_core::Result<()> {
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
            let codecpar = unsafe {
                let new_par = ffi::avcodec_parameters_alloc();
                ffi::avcodec_parameters_copy(new_par, stream.codecpar().as_ptr() as *const _);
                rsmpeg::avcodec::AVCodecParameters::from_raw(
                    std::ptr::NonNull::new(new_par).unwrap(),
                )
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

    fn can_compose(&self, sources: &[&Path]) -> roboflow_core::Result<()> {
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

    #[test]
    fn test_can_compose_empty() {
        let composer = RsmpegVideoComposer::new();
        let result = composer.can_compose(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_can_compose_missing_file() {
        let composer = RsmpegVideoComposer::new();
        let result = composer.can_compose(&[Path::new("/nonexistent/file.mp4")]);
        assert!(result.is_err());
    }
}
