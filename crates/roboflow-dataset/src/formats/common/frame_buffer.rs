// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Frame buffer utilities for video encoding.

use roboflow_core::Result;
use roboflow_media::video::{VideoFrame, VideoFrameBuffer};

use crate::formats::common::{ImageData, decode_image_to_rgb};

/// Build a VideoFrameBuffer from a slice of ImageData.
///
/// Decodes encoded images (JPEG/PNG) to RGB and validates dimensions.
/// Returns (buffer, skipped_count) where skipped frames are those
/// with invalid dimensions or decode failures.
pub fn build_video_frame_buffer(images: &[ImageData]) -> Result<(VideoFrameBuffer, usize)> {
    let mut buffer = VideoFrameBuffer::new();
    let mut skipped = 0usize;

    for img in images {
        if img.width == 0 || img.height == 0 {
            skipped += 1;
            continue;
        }

        let (width, height, rgb_data) = if img.is_encoded {
            match decode_image_to_rgb(img) {
                Some((w, h, data)) => (w, h, data),
                None => {
                    skipped += 1;
                    continue;
                }
            }
        } else {
            (img.width, img.height, img.data.clone())
        };

        let video_frame = VideoFrame::new(width, height, rgb_data);
        if let Err(e) = buffer.add_frame(video_frame) {
            skipped += 1;
            tracing::debug!("Frame skipped due to dimension mismatch: {}", e);
        }
    }

    Ok((buffer, skipped))
}
