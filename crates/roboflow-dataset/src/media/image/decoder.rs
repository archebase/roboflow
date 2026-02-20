// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Image decoding for compressed formats (JPEG, PNG).
//!
//! This module provides helper functions for extracting image data from
//! ROS CompressedImage messages. The actual type definitions are
//! centralized in the `format` and `backend` modules.

use super::{ImageError, Result};
use super::{ImageFormat, DecodedImage};
use std::borrow::Cow;

/// Extract the format string from a CompressedImage message.
///
/// CompressedImage messages have a "format" field containing strings like
/// "jpeg", "png", "avi" (for some h.264 cameras).
pub fn extract_format_from_message(
    message_data: &[(String, robocodec::CodecValue)],
) -> ImageFormat {
    for (key, value) in message_data {
        if key == "format" {
            if let robocodec::CodecValue::String(fmt) = value {
                return ImageFormat::from_ros_format(fmt);
            }
        }
    }
    ImageFormat::Unknown
}

/// Extract the compressed data bytes from a CompressedImage message.
pub fn extract_data_from_message(
    message_data: &[(String, robocodec::CodecValue)],
) -> Option<Cow<[u8]>> {
    for (key, value) in message_data {
        if key == "data" {
            if let robocodec::CodecValue::Bytes(bytes) = value {
                return Some(Cow::Borrowed(bytes));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_format_from_message() {
        let message_data = vec![
            ("format".to_string(), robocodec::CodecValue::String("jpeg".to_string())),
            ("data".to_string(), robocodec::CodecValue::Bytes(&[0xFF, 0xD8])),
        ];

        let format = extract_format_from_message(&message_data);
        assert_eq!(format, ImageFormat::Jpeg);
    }

    #[test]
    fn test_extract_format_from_message_unknown() {
        let message_data = vec![
            ("data".to_string(), robocodec::CodecValue::Bytes(&[0xFF, 0xD8])),
        ];

        let format = extract_format_from_message(&message_data);
        assert_eq!(format, ImageFormat::Unknown);
    }

    #[test]
    fn test_extract_data_from_message() {
        let test_data = vec![0xFF, 0xD8, 0xFF];
        let message_data = vec![
            ("format".to_string(), robocodec::CodecValue::String("jpeg".to_string())),
            ("data".to_string(), robocodec::CodecValue::Bytes(test_data.as_slice())),
        ];

        let extracted = extract_data_from_message(&message_data);
        assert!(extracted.is_some());
        assert_eq!(extracted.unwrap(), &test_data[..]);
    }

    #[test]
    fn test_extract_data_from_message_missing() {
        let message_data = vec![
            ("format".to_string(), robocodec::CodecValue::String("jpeg".to_string())),
        ];

        let extracted = extract_data_from_message(&message_data);
        assert!(extracted.is_none());
    }
}
