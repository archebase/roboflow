// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Shared utilities for pipeline message processing.
//!
//! This module contains helper functions used by both PipelineExecutor
//! and ParallelPipelineExecutor to avoid code duplication.

use robocodec::CodecValue;
use std::collections::HashMap;

/// Extract u32 value from CodecValue
pub fn extract_u32(value: &CodecValue) -> Option<u32> {
    match value {
        CodecValue::UInt32(n) => Some(*n),
        CodecValue::UInt64(n) if *n <= u32::MAX as u64 => Some(*n as u32),
        CodecValue::Int32(n) if *n >= 0 => Some(*n as u32),
        CodecValue::Int64(n) if *n >= 0 && *n <= u32::MAX as i64 => Some(*n as u32),
        _ => None,
    }
}

/// Extract image bytes from a CodecValue map
pub fn extract_image_bytes(map: &HashMap<String, CodecValue>) -> Option<Vec<u8>> {
    let data = map.get("data")?;

    match data {
        CodecValue::Bytes(b) => Some(b.clone()),
        CodecValue::Array(arr) => {
            let bytes: Vec<u8> = arr
                .iter()
                .filter_map(|v| match v {
                    CodecValue::UInt8(b) => Some(*b),
                    CodecValue::Int8(b) if *b >= 0 => Some(*b as u8),
                    CodecValue::UInt16(b) if *b <= u8::MAX as u16 => Some(*b as u8),
                    CodecValue::Int16(b) if *b >= 0 && (*b as u16) <= u8::MAX as u16 => {
                        Some(*b as u8)
                    }
                    CodecValue::UInt32(b) if *b <= u8::MAX as u32 => Some(*b as u8),
                    CodecValue::Int32(b) if *b >= 0 && (*b as u32) <= u8::MAX as u32 => {
                        Some(*b as u8)
                    }
                    CodecValue::UInt64(b) if *b <= u8::MAX as u64 => Some(*b as u8),
                    CodecValue::Int64(b) if *b >= 0 && (*b as u64) <= u8::MAX as u64 => {
                        Some(*b as u8)
                    }
                    _ => None,
                })
                .collect();

            if bytes.is_empty() {
                // Try nested array
                for v in arr.iter() {
                    if let CodecValue::Array(inner) = v {
                        let inner_bytes: Vec<u8> = inner
                            .iter()
                            .filter_map(|v| match v {
                                CodecValue::UInt8(b) => Some(*b),
                                CodecValue::Int8(b) if *b >= 0 => Some(*b as u8),
                                _ => None,
                            })
                            .collect();
                        if !inner_bytes.is_empty() {
                            return Some(inner_bytes);
                        }
                    }
                }
                None
            } else {
                Some(bytes)
            }
        }
        _ => None,
    }
}

/// Check if a topic is camera info (has K and D matrices)
pub fn is_camera_info_topic(data: &CodecValue) -> bool {
    matches!(data, CodecValue::Struct(map) if map.contains_key("K") && map.contains_key("D"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use robocodec::CodecValue;
    use std::collections::HashMap;

    #[test]
    fn test_extract_u32_from_uint32() {
        assert_eq!(extract_u32(&CodecValue::UInt32(42)), Some(42));
    }

    #[test]
    fn test_extract_u32_from_uint64() {
        assert_eq!(extract_u32(&CodecValue::UInt64(42)), Some(42));
        assert_eq!(extract_u32(&CodecValue::UInt64(u32::MAX as u64 + 1)), None);
    }

    #[test]
    fn test_extract_u32_from_int32() {
        assert_eq!(extract_u32(&CodecValue::Int32(42)), Some(42));
        assert_eq!(extract_u32(&CodecValue::Int32(-1)), None);
    }

    #[test]
    fn test_extract_u32_invalid() {
        assert_eq!(extract_u32(&CodecValue::String("test".to_string())), None);
        assert_eq!(extract_u32(&CodecValue::Float32(1.0)), None);
    }

    #[test]
    fn test_extract_image_bytes_from_bytes() {
        let mut map = HashMap::new();
        map.insert("data".to_string(), CodecValue::Bytes(vec![1, 2, 3]));
        assert_eq!(extract_image_bytes(&map), Some(vec![1, 2, 3]));
    }

    #[test]
    fn test_extract_image_bytes_from_array() {
        let mut map = HashMap::new();
        map.insert(
            "data".to_string(),
            CodecValue::Array(vec![
                CodecValue::UInt8(1),
                CodecValue::UInt8(2),
                CodecValue::UInt8(3),
            ]),
        );
        assert_eq!(extract_image_bytes(&map), Some(vec![1, 2, 3]));
    }

    #[test]
    fn test_extract_image_bytes_no_data() {
        let map: HashMap<String, CodecValue> = HashMap::new();
        assert_eq!(extract_image_bytes(&map), None);
    }

    #[test]
    fn test_is_camera_info_topic_true() {
        let mut map = HashMap::new();
        map.insert("K".to_string(), CodecValue::Array(vec![]));
        map.insert("D".to_string(), CodecValue::Array(vec![]));
        assert!(is_camera_info_topic(&CodecValue::Struct(map)));
    }

    #[test]
    fn test_is_camera_info_topic_false() {
        let mut map = HashMap::new();
        map.insert("K".to_string(), CodecValue::Array(vec![]));
        assert!(!is_camera_info_topic(&CodecValue::Struct(map)));

        assert!(!is_camera_info_topic(&CodecValue::String(
            "test".to_string()
        )));
    }
}
