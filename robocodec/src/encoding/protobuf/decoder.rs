// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # Robocodec Protobuf Decoder
//!
//! Protobuf decoder for roboflow.
//!
//! This crate provides Protobuf decoding support using prost-reflect.
//! When the `reflect` feature is enabled, dynamic message decoding is available.
//!
//! ## Example
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use robocodec::encoding::protobuf::decoder::ProtobufDecoder;
//!
//! # let protobuf_bytes = vec![0u8; 100];
//! let decoder = ProtobufDecoder::new();
//! let decoded = decoder.decode(&protobuf_bytes)?;
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;

use crate::{CodecError, CodecValue, DecodedMessage, Result as CoreResult};

/// Protobuf decoder for decoding protobuf binary data.
pub struct ProtobufDecoder {
    /// Enable reflection-based decoding
    _private: (),
}

impl ProtobufDecoder {
    /// Create a new Protobuf decoder.
    pub fn new() -> Self {
        Self { _private: () }
    }

    /// Decode a protobuf message from raw bytes.
    ///
    /// This method provides basic protobuf parsing without requiring
    /// a FileDescriptorSet. It decodes the wire format into a generic
    /// CodecValue structure.
    ///
    /// # Arguments
    ///
    /// * `data` - The protobuf-encoded binary data
    ///
    /// # Limitations
    ///
    /// Without a FileDescriptorSet, this decoder:
    /// - Cannot resolve field names (uses field numbers)
    /// - Cannot distinguish between varint types (int32, uint32, bool, enum)
    /// - Treats all unknown fields as raw bytes
    ///
    /// For full semantic decoding, enable the `reflect` feature and use
    /// `decode_with_descriptor`.
    pub fn decode(&self, data: &[u8]) -> CoreResult<DecodedMessage> {
        let mut result = DecodedMessage::new();
        let mut pos = 0;

        while pos < data.len() {
            // Read tag (field_number << 3 | wire_type)
            let (tag, new_pos) = self.read_varint(data, pos)?;
            pos = new_pos;

            let field_number = tag >> 3;
            let wire_type = (tag & 0x07) as u32;

            match wire_type {
                // Varint (int32, int64, uint32, uint64, bool, enum)
                0 => {
                    let (value, new_pos) = self.read_varint(data, pos)?;
                    pos = new_pos;
                    // Default to int64 for varint
                    result.insert(field_number.to_string(), CodecValue::Int64(value as i64));
                }
                // 64-bit (fixed64, sfixed64, double)
                1 => {
                    if pos + 8 > data.len() {
                        return Err(CodecError::buffer_too_short(
                            8,
                            data.len() - pos,
                            pos as u64,
                        ));
                    }
                    let bytes = &data[pos..pos + 8];
                    // Try as double first, then as uint64
                    let value = bytes
                        .try_into()
                        .ok()
                        .map(|b: [u8; 8]| u64::from_le_bytes(b))
                        .unwrap_or(0);
                    result.insert(field_number.to_string(), CodecValue::UInt64(value));
                    pos += 8;
                }
                // Length-delimited (string, bytes, embedded messages, packed arrays)
                2 => {
                    let (len, new_pos) = self.read_varint(data, pos)?;
                    pos = new_pos;
                    let len = len as usize;

                    if pos + len > data.len() {
                        return Err(CodecError::buffer_too_short(
                            len,
                            data.len() - pos,
                            pos as u64,
                        ));
                    }

                    // Try to decode as UTF-8 string
                    let bytes = &data[pos..pos + len];
                    if let Ok(s) = std::str::from_utf8(bytes) {
                        result.insert(field_number.to_string(), CodecValue::String(s.to_string()));
                    } else {
                        // Not valid UTF-8, store as bytes
                        result.insert(field_number.to_string(), CodecValue::Bytes(bytes.to_vec()));
                    }
                    pos += len;
                }
                // 32-bit (fixed32, sfixed32, float)
                5 => {
                    if pos + 4 > data.len() {
                        return Err(CodecError::buffer_too_short(
                            4,
                            data.len() - pos,
                            pos as u64,
                        ));
                    }
                    let bytes = &data[pos..pos + 4];
                    // Try as float first, then as uint32
                    let value = bytes
                        .try_into()
                        .ok()
                        .map(|b: [u8; 4]| u32::from_le_bytes(b))
                        .unwrap_or(0);
                    result.insert(field_number.to_string(), CodecValue::UInt32(value));
                    pos += 4;
                }
                // Start group (deprecated) - not supported
                3 => {
                    return Err(CodecError::unsupported("group wire type (deprecated)"));
                }
                // End group (deprecated) - not supported
                4 => {
                    return Err(CodecError::unsupported("group wire type (deprecated)"));
                }
                _ => {
                    return Err(CodecError::parse(
                        "wire type",
                        format!("unknown: {wire_type}"),
                    ));
                }
            }
        }

        Ok(result)
    }

    /// Read a varint from the data.
    fn read_varint(&self, data: &[u8], pos: usize) -> CoreResult<(u64, usize)> {
        let mut result: u64 = 0;
        let mut shift = 0;
        let mut current_pos = pos;

        loop {
            if current_pos >= data.len() {
                return Err(CodecError::buffer_too_short(1, 0, current_pos as u64));
            }

            let byte = data[current_pos];
            current_pos += 1;

            result |= ((byte & 0x7F) as u64) << shift;
            shift += 7;

            if byte & 0x80 == 0 {
                break;
            }

            if shift >= 64 {
                return Err(CodecError::parse("varint", "overflow"));
            }
        }

        Ok((result, current_pos))
    }

    /// Decode a protobuf value from JSON format.
    ///
    /// This provides an alternative way to decode protobuf messages
    /// when the data is available in JSON format.
    pub fn decode_from_json(&self, json: &str) -> CoreResult<DecodedMessage> {
        let value: serde_json::Value =
            serde_json::from_str(json).map_err(|e| CodecError::parse("json", format!("{e}")))?;

        self.json_value_to_message(&value)
    }

    /// Convert a JSON value to a decoded message.
    fn json_value_to_message(&self, value: &serde_json::Value) -> CoreResult<DecodedMessage> {
        let mut result = DecodedMessage::new();

        if let Some(obj) = value.as_object() {
            for (key, val) in obj {
                let codec_value = self.json_value_to_codec_value(val)?;
                result.insert(key.clone(), codec_value);
            }
        }

        Ok(result)
    }

    /// Convert a JSON value to a codec value.
    #[allow(clippy::only_used_in_recursion)]
    fn json_value_to_codec_value(&self, value: &serde_json::Value) -> CoreResult<CodecValue> {
        match value {
            serde_json::Value::Null => Ok(CodecValue::Null),
            serde_json::Value::Bool(b) => Ok(CodecValue::Bool(*b)),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(CodecValue::Int64(i))
                } else if let Some(u) = n.as_u64() {
                    Ok(CodecValue::UInt64(u))
                } else if let Some(f) = n.as_f64() {
                    Ok(CodecValue::Float64(f))
                } else {
                    Err(CodecError::parse("number", "unknown number format"))
                }
            }
            serde_json::Value::String(s) => Ok(CodecValue::String(s.clone())),
            serde_json::Value::Array(arr) => {
                let mut values = Vec::new();
                for item in arr {
                    values.push(self.json_value_to_codec_value(item)?);
                }
                Ok(CodecValue::Array(values))
            }
            serde_json::Value::Object(obj) => {
                let mut map = HashMap::new();
                for (key, val) in obj {
                    map.insert(key.clone(), self.json_value_to_codec_value(val)?);
                }
                Ok(CodecValue::Struct(map))
            }
        }
    }
}

impl Default for ProtobufDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl crate::Decoder for ProtobufDecoder {
    /// Decode protobuf data.
    ///
    /// The `schema` and `type_name` parameters are ignored since protobuf
    /// decoding uses wire format parsing without requiring a schema.
    /// Future enhancements may support FileDescriptorSet via schema parameter.
    fn decode(
        &self,
        data: &[u8],
        _schema: &str,
        _type_name: Option<&str>,
    ) -> CoreResult<DecodedMessage> {
        // Protobuf decoder doesn't require schema - uses wire format
        self.decode(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_varint_field() {
        let decoder = ProtobufDecoder::new();
        // Field 1, varint wire type (0), value 42
        // Tag: (1 << 3) | 0 = 8
        // Value: 42
        let data = vec![0x08, 0x2A];

        let result = decoder.decode(&data).unwrap();
        assert_eq!(result.get("1"), Some(&CodecValue::Int64(42)));
    }

    #[test]
    fn test_decode_string_field() {
        let decoder = ProtobufDecoder::new();
        // Field 1, length-delimited (2), string "hello"
        // Tag: (1 << 3) | 2 = 10
        // Length: 5
        // "hello"
        let mut data = vec![0x0A, 0x05];
        data.extend_from_slice(b"hello");

        let result = decoder.decode(&data).unwrap();
        assert_eq!(
            result.get("1"),
            Some(&CodecValue::String("hello".to_string()))
        );
    }

    #[test]
    fn test_decode_64bit_field() {
        let decoder = ProtobufDecoder::new();
        // Field 1, 64-bit wire type (1), value 12345678901234567890
        // Tag: (1 << 3) | 1 = 9
        let mut data = vec![0x09];
        data.extend_from_slice(&12345678901234567890u64.to_le_bytes());

        let result = decoder.decode(&data).unwrap();
        assert_eq!(
            result.get("1"),
            Some(&CodecValue::UInt64(12345678901234567890))
        );
    }

    #[test]
    fn test_decode_multiple_fields() {
        let decoder = ProtobufDecoder::new();
        // Field 1: varint 42
        // Field 2: varint 100
        let data = vec![0x08, 0x2A, 0x10, 0x64];

        let result = decoder.decode(&data).unwrap();
        assert_eq!(result.get("1"), Some(&CodecValue::Int64(42)));
        assert_eq!(result.get("2"), Some(&CodecValue::Int64(100)));
    }

    #[test]
    fn test_decode_from_json() {
        let decoder = ProtobufDecoder::new();
        let json = r#"{"field1": "hello", "field2": 42, "field3": true}"#;

        let result = decoder.decode_from_json(json).unwrap();
        assert_eq!(
            result.get("field1"),
            Some(&CodecValue::String("hello".to_string()))
        );
        assert_eq!(result.get("field2"), Some(&CodecValue::Int64(42)));
        assert_eq!(result.get("field3"), Some(&CodecValue::Bool(true)));
    }
}
