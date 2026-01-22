// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # Robocodec JSON Decoder
//!
//! JSON decoder for roboflow.
//!
//! ## Example
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use robocodec::encoding::json::decoder::JsonDecoder;
//!
//! let decoder = JsonDecoder::new();
//! let decoded = decoder.decode(r#"{"x": 1, "y": 2}"#)?;
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;

use crate::{CodecError, CodecValue, DecodedMessage, Result as CoreResult};

/// JSON decoder for decoding JSON data.
pub struct JsonDecoder {
    /// Enable pretty printing
    _private: (),
}

impl JsonDecoder {
    /// Create a new JSON decoder.
    pub fn new() -> Self {
        Self { _private: () }
    }

    /// Decode a JSON string into a DecodedMessage.
    ///
    /// # Arguments
    ///
    /// * `json` - The JSON string to decode
    pub fn decode(&self, json: &str) -> CoreResult<DecodedMessage> {
        let value: serde_json::Value =
            serde_json::from_str(json).map_err(|e| CodecError::parse("json", format!("{e}")))?;

        self.json_value_to_message(&value)
    }

    /// Decode JSON bytes into a DecodedMessage.
    ///
    /// # Arguments
    ///
    /// * `data` - The JSON bytes to decode
    pub fn decode_bytes(&self, data: &[u8]) -> CoreResult<DecodedMessage> {
        let value: serde_json::Value =
            serde_json::from_slice(data).map_err(|e| CodecError::parse("json", format!("{e}")))?;

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
        } else if !value.is_null() {
            // Single value, not an object - wrap it
            let codec_value = self.json_value_to_codec_value(value)?;
            result.insert("value".to_string(), codec_value);
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

    /// Encode a DecodedMessage to a JSON string.
    ///
    /// # Arguments
    ///
    /// * `message` - The decoded message to encode
    /// * `pretty` - Whether to pretty-print the output
    pub fn encode(&self, message: &DecodedMessage, pretty: bool) -> CoreResult<String> {
        let json_value = self.message_to_json_value(message)?;

        if pretty {
            serde_json::to_string_pretty(&json_value)
                .map_err(|e| CodecError::parse("json encode", format!("{e}")))
        } else {
            serde_json::to_string(&json_value)
                .map_err(|e| CodecError::parse("json encode", format!("{e}")))
        }
    }

    /// Convert a decoded message to a JSON value.
    fn message_to_json_value(&self, message: &DecodedMessage) -> CoreResult<serde_json::Value> {
        let mut obj = serde_json::Map::new();

        for (key, value) in message {
            obj.insert(key.clone(), self.codec_value_to_json(value)?);
        }

        Ok(serde_json::Value::Object(obj))
    }

    /// Convert a codec value to a JSON value.
    #[allow(clippy::only_used_in_recursion)]
    fn codec_value_to_json(&self, value: &CodecValue) -> CoreResult<serde_json::Value> {
        match value {
            CodecValue::Null => Ok(serde_json::Value::Null),
            CodecValue::Bool(b) => Ok(serde_json::Value::Bool(*b)),
            CodecValue::Int8(i) => Ok(serde_json::Value::Number(serde_json::Number::from(*i))),
            CodecValue::Int16(i) => Ok(serde_json::Value::Number(serde_json::Number::from(*i))),
            CodecValue::Int32(i) => Ok(serde_json::Value::Number(serde_json::Number::from(*i))),
            CodecValue::Int64(i) => Ok(serde_json::Value::Number(serde_json::Number::from(*i))),
            CodecValue::UInt8(u) => Ok(serde_json::Value::Number(serde_json::Number::from(*u))),
            CodecValue::UInt16(u) => Ok(serde_json::Value::Number(serde_json::Number::from(*u))),
            CodecValue::UInt32(u) => Ok(serde_json::Value::Number(serde_json::Number::from(*u))),
            CodecValue::UInt64(u) => Ok(serde_json::Value::Number(serde_json::Number::from(*u))),
            CodecValue::Float32(f) => serde_json::Number::from_f64(*f as f64)
                .map(serde_json::Value::Number)
                .ok_or_else(|| CodecError::parse("float32", "not representable as JSON number")),
            CodecValue::Float64(f) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .ok_or_else(|| CodecError::parse("float64", "not representable as JSON number")),
            CodecValue::String(s) => Ok(serde_json::Value::String(s.clone())),
            CodecValue::Timestamp(nanos) => {
                // Represent timestamp as a string "Timestamp(N)"
                Ok(serde_json::Value::String(format!("Timestamp({nanos})")))
            }
            CodecValue::Duration(nanos) => {
                // Represent duration as a string "Duration(N)"
                Ok(serde_json::Value::String(format!("Duration({nanos})")))
            }
            CodecValue::Bytes(b) => {
                // Encode bytes as base64 string
                Ok(serde_json::Value::String(base64_encode(b)))
            }
            CodecValue::Array(arr) => {
                let mut values = Vec::new();
                for item in arr {
                    values.push(self.codec_value_to_json(item)?);
                }
                Ok(serde_json::Value::Array(values))
            }
            CodecValue::Struct(map) => {
                let mut obj = serde_json::Map::new();
                for (key, val) in map {
                    obj.insert(key.clone(), self.codec_value_to_json(val)?);
                }
                Ok(serde_json::Value::Object(obj))
            }
        }
    }
}

impl Default for JsonDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl crate::Decoder for JsonDecoder {
    /// Decode JSON data.
    ///
    /// The `schema` and `type_name` parameters are ignored since JSON
    /// is self-describing and doesn't require a schema for parsing.
    /// Future enhancements may use schema for validation.
    fn decode(
        &self,
        data: &[u8],
        _schema: &str,
        _type_name: Option<&str>,
    ) -> CoreResult<DecodedMessage> {
        // JsonDecoder doesn't require schema - JSON is self-describing
        self.decode_bytes(data)
    }
}

/// Simple base64 encoding for bytes (no std dependency).
fn base64_encode(data: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut result = String::new();
    let mut chunks = data.chunks(3);

    for chunk in &mut chunks {
        let mut buffer = [0u8; 3];
        buffer[..chunk.len()].copy_from_slice(chunk);

        result.push(TABLE[(buffer[0] >> 2) as usize] as char);
        result.push(TABLE[((buffer[0] & 0x03) << 4 | buffer[1] >> 4) as usize] as char);

        if chunk.len() > 1 {
            result.push(TABLE[((buffer[1] & 0x0F) << 2 | buffer[2] >> 6) as usize] as char);
        } else {
            result.push('=');
        }

        if chunk.len() > 2 {
            result.push(TABLE[(buffer[2] & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_object() {
        let decoder = JsonDecoder::new();
        let json = r#"{"x": 1, "y": 2, "z": 3}"#;

        let result = decoder.decode(json).unwrap();
        assert_eq!(result.get("x"), Some(&CodecValue::Int64(1)));
        assert_eq!(result.get("y"), Some(&CodecValue::Int64(2)));
        assert_eq!(result.get("z"), Some(&CodecValue::Int64(3)));
    }

    #[test]
    fn test_decode_nested() {
        let decoder = JsonDecoder::new();
        let json = r#"{"position": {"x": 1.0, "y": 2.0}, "name": "test"}"#;

        let result = decoder.decode(json).unwrap();
        if let Some(CodecValue::Struct(pos)) = result.get("position") {
            assert_eq!(pos.get("x"), Some(&CodecValue::Float64(1.0)));
            assert_eq!(pos.get("y"), Some(&CodecValue::Float64(2.0)));
        } else {
            panic!("Expected struct");
        }
        assert_eq!(
            result.get("name"),
            Some(&CodecValue::String("test".to_string()))
        );
    }

    #[test]
    fn test_decode_array() {
        let decoder = JsonDecoder::new();
        let json = r#"{"values": [1, 2, 3]}"#;

        let result = decoder.decode(json).unwrap();
        if let Some(CodecValue::Array(arr)) = result.get("values") {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], CodecValue::Int64(1));
            assert_eq!(arr[1], CodecValue::Int64(2));
            assert_eq!(arr[2], CodecValue::Int64(3));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_decode_null() {
        let decoder = JsonDecoder::new();
        let json = r#"{"value": null}"#;

        let result = decoder.decode(json).unwrap();
        assert_eq!(result.get("value"), Some(&CodecValue::Null));
    }

    #[test]
    fn test_encode_message() {
        let decoder = JsonDecoder::new();
        let mut message = DecodedMessage::new();
        message.insert("x".to_string(), CodecValue::Int64(42));
        message.insert("name".to_string(), CodecValue::String("test".to_string()));

        let encoded = decoder.encode(&message, false).unwrap();
        // Parse and check values (order may vary due to HashMap)
        let value: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(value["x"], 42);
        assert_eq!(value["name"], "test");
    }

    #[test]
    fn test_encode_pretty() {
        let decoder = JsonDecoder::new();
        let mut message = DecodedMessage::new();
        message.insert("x".to_string(), CodecValue::Int64(42));

        let encoded = decoder.encode(&message, true).unwrap();
        assert_eq!(encoded, "{\n  \"x\": 42\n}");
    }

    #[test]
    fn test_encode_array() {
        let decoder = JsonDecoder::new();
        let mut message = DecodedMessage::new();
        message.insert(
            "values".to_string(),
            CodecValue::Array(vec![CodecValue::Int64(1), CodecValue::Int64(2)]),
        );

        let encoded = decoder.encode(&message, false).unwrap();
        assert_eq!(encoded, r#"{"values":[1,2]}"#);
    }

    #[test]
    fn test_encode_bytes() {
        let decoder = JsonDecoder::new();
        let mut message = DecodedMessage::new();
        message.insert("data".to_string(), CodecValue::Bytes(vec![1, 2, 3]));

        let encoded = decoder.encode(&message, false).unwrap();
        // Base64 of [1, 2, 3] is "AQID"
        assert_eq!(encoded, r#"{"data":"AQID"}"#);
    }
}
