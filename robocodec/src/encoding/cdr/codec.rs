// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! CDR codec implementation wrapping existing decoder/encoder.

use crate::core::{CodecError, DecodedMessage, Encoding, Result};
use crate::encoding::transform::SchemaMetadata;
use crate::encoding::CdrDecoder;
use crate::encoding::CdrEncoder;
use crate::encoding::DynCodec;

/// CDR codec wrapper implementing the unified codec interface.
///
/// This wraps the existing `CdrDecoder` and `CdrEncoder` to work with
/// the unified codec system.
pub struct CdrCodec {
    /// Cached decoder (stateless, can be reused)
    decoder: CdrDecoder,
    /// Current encoder for encoding operations
    encoder: Option<CdrEncoder>,
}

impl CdrCodec {
    /// Create a new CDR codec.
    pub fn new() -> Self {
        Self {
            decoder: CdrDecoder::new(),
            encoder: None,
        }
    }

    /// Get the CDR decoder.
    pub fn decoder(&self) -> &CdrDecoder {
        &self.decoder
    }

    /// Get a mutable CDR encoder.
    pub fn encoder(&mut self) -> &mut CdrEncoder {
        if self.encoder.is_none() {
            self.encoder = Some(CdrEncoder::new());
        }
        self.encoder.as_mut().unwrap()
    }
}

impl Default for CdrCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl DynCodec for CdrCodec {
    fn decode_dynamic(&self, data: &[u8], schema: &SchemaMetadata) -> Result<DecodedMessage> {
        match schema {
            SchemaMetadata::Cdr {
                type_name,
                schema_text,
            } => {
                // Parse the schema text to get MessageSchema
                let parsed_schema = crate::schema::parse_schema(type_name, schema_text)?;

                // Decode using the existing CDR decoder
                self.decoder.decode(&parsed_schema, data, Some(type_name))
            }
            _ => Err(CodecError::invalid_schema(
                schema.type_name(),
                "Schema is not a CDR schema",
            )),
        }
    }

    fn encode_dynamic(
        &mut self,
        message: &DecodedMessage,
        schema: &SchemaMetadata,
    ) -> Result<Vec<u8>> {
        match schema {
            SchemaMetadata::Cdr {
                type_name,
                schema_text,
            } => {
                // Parse the schema text to get MessageSchema
                let parsed_schema = crate::schema::parse_schema(type_name, schema_text)?;

                // Encode using the CDR encoder
                let encoder = self.encoder();
                encoder.encode_message(message, &parsed_schema, type_name)?;
                // Take ownership of encoder to call finish
                let encoder = self.encoder.take().unwrap();
                Ok(encoder.finish())
            }
            _ => Err(CodecError::invalid_schema(
                schema.type_name(),
                "Schema is not a CDR schema",
            )),
        }
    }

    fn encoding_type(&self) -> Encoding {
        Encoding::Cdr
    }

    fn reset(&mut self) {
        self.encoder = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cdr_codec_creation() {
        let codec = CdrCodec::new();
        assert_eq!(codec.encoding_type(), Encoding::Cdr);
    }

    #[test]
    fn test_cdr_codec_default() {
        let codec = CdrCodec::default();
        assert_eq!(codec.encoding_type(), Encoding::Cdr);
    }

    #[test]
    fn test_cdr_codec_reset() {
        let mut codec = CdrCodec::new();

        // Get encoder to initialize it
        let _enc = codec.encoder();
        assert!(codec.encoder.is_some());

        // Reset
        codec.reset();
        assert!(codec.encoder.is_none());
    }

    #[test]
    fn test_cdr_codec_decode_invalid_schema() {
        let codec = CdrCodec::new();
        let schema = SchemaMetadata::protobuf("test.Type".to_string(), vec![1, 2, 3]);

        let result = codec.decode_dynamic(&[0x00, 0x00, 0x00, 0x00], &schema);
        assert!(result.is_err());
    }
}
