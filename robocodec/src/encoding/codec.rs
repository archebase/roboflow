// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified codec interface for encoding-agnostic message processing.
//!
//! This module provides a clean abstraction over different message encodings
//! (CDR, Protobuf, JSON) to support the MCAP rewriter in a format-agnostic way.
//!
//! ## Architecture
//!
//! The codec system is organized into layers:
//!
//! - **Core traits** ([`MessageCodec`], [`DynCodec`]) - Define the interface
//! - **Encoding-specific implementations** (cdr, protobuf) - Provide codec behavior
//! - **Factory** ([`CodecFactory`]) - Creates appropriate codec for each encoding
//!
//! ## Example
//!
//! ```no_run
//! use robocodec::encoding::CodecFactory;
//! use robocodec::Encoding;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut factory = CodecFactory::new();
//! let codec = factory.get_codec_mut(Encoding::Cdr)?;
//! let _encoding_type = codec.encoding_type();
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;

use crate::core::{CodecError, DecodedMessage, Encoding, Result};

pub use super::transform::{
    CdrSchemaTransformer, ProtobufSchemaTransformer, SchemaMetadata, SchemaTransformer,
    TransformResult,
};

pub use super::cdr::CdrCodec;
pub use super::protobuf::ProtobufCodec;

// =============================================================================
// Message Codec Trait
// =============================================================================

/// Unified codec interface for decoding and encoding messages.
///
/// This trait abstracts over different encoding formats (CDR, Protobuf, JSON)
/// to allow the rewriter to handle all formats through a single interface.
///
/// # Type Parameters
///
/// * `S` - Schema type (e.g., `MessageSchema` for CDR, `SchemaMetadata` for protobuf)
pub trait MessageCodec<S>: Send + Sync {
    /// Decode raw message data into a `DecodedMessage`.
    ///
    /// # Arguments
    ///
    /// * `data` - Raw message bytes
    /// * `schema` - Schema metadata for decoding
    ///
    /// # Returns
    ///
    /// A `DecodedMessage` containing decoded field-value pairs
    fn decode(&self, data: &[u8], schema: &S) -> Result<DecodedMessage>;

    /// Encode a `DecodedMessage` back to raw bytes.
    ///
    /// # Arguments
    ///
    /// * `message` - Decoded message to encode
    /// * `schema` - Schema metadata for encoding
    ///
    /// # Returns
    ///
    /// Encoded message bytes
    fn encode(&mut self, message: &DecodedMessage, schema: &S) -> Result<Vec<u8>>;

    /// Get the encoding type this codec handles.
    fn encoding_type(&self) -> Encoding;

    /// Reset encoder state for reuse.
    ///
    /// Some encoders maintain internal state (e.g., buffers). This method
    /// allows reusing the same encoder instance for multiple messages.
    fn reset(&mut self);
}

// =============================================================================
// Codec Factory
// =============================================================================

/// Factory for creating codec instances based on encoding type.
///
/// The factory manages codec instances and ensures proper initialization
/// with schema data.
pub struct CodecFactory {
    /// Cached codec instances
    codecs: HashMap<Encoding, Box<dyn DynCodec>>,
}

impl CodecFactory {
    /// Create a new codec factory with all supported codecs.
    pub fn new() -> Self {
        let mut codecs: HashMap<Encoding, Box<dyn DynCodec>> = HashMap::new();

        // Register CDR codec
        codecs.insert(Encoding::Cdr, Box::new(CdrCodec::new()));

        // Register Protobuf codec
        codecs.insert(Encoding::Protobuf, Box::new(ProtobufCodec::new()));

        Self { codecs }
    }

    /// Get a codec for the specified encoding.
    ///
    /// # Arguments
    ///
    /// * `encoding` - The encoding type
    ///
    /// # Returns
    ///
    /// A reference to the codec, or an error if the encoding is not supported
    pub fn get_codec(&self, encoding: Encoding) -> Result<&dyn DynCodec> {
        let encoding_str = format!("encoding: {encoding:?}");
        self.codecs
            .get(&encoding)
            .map(|b| b.as_ref())
            .ok_or_else(move || CodecError::unsupported(&encoding_str))
    }

    /// Get a mutable codec for the specified encoding.
    ///
    /// This is used for encode operations which may modify internal state.
    pub fn get_codec_mut(&mut self, encoding: Encoding) -> Result<&mut Box<dyn DynCodec>> {
        let encoding_str = format!("encoding: {encoding:?}");
        self.codecs
            .get_mut(&encoding)
            .ok_or_else(move || CodecError::unsupported(&encoding_str))
    }

    /// Detect encoding from channel metadata.
    ///
    /// # Arguments
    ///
    /// * `encoding_str` - Encoding string from MCAP channel
    /// * `schema_encoding` - Optional schema encoding string
    ///
    /// # Returns
    ///
    /// Detected `Encoding` type
    pub fn detect_encoding(&self, encoding_str: &str, schema_encoding: Option<&str>) -> Encoding {
        let encoding_lower = encoding_str.to_lowercase();

        // Check explicit encoding
        if encoding_lower.contains("cdr")
            || encoding_lower.contains("ros2")
            || encoding_lower.contains("ros2msg")
        {
            return Encoding::Cdr;
        }

        if encoding_lower.contains("protobuf") || encoding_lower.contains("proto") {
            return Encoding::Protobuf;
        }

        if encoding_lower.contains("json") {
            return Encoding::Json;
        }

        // Fallback to schema encoding
        if let Some(schema_enc) = schema_encoding {
            match schema_enc.to_lowercase().as_str() {
                "protobuf" | "proto" => return Encoding::Protobuf,
                "ros2msg" | "rosidl" => return Encoding::Cdr,
                "json" => return Encoding::Json,
                _ => {}
            }
        }

        // Default to CDR for backward compatibility
        Encoding::Cdr
    }
}

impl Default for CodecFactory {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Dynamic Codec Trait
// =============================================================================

/// Dynamic version of [`MessageCodec`] for use in trait objects.
///
/// This trait allows storing different codec implementations in a collection
/// and routing to the appropriate codec at runtime.
pub trait DynCodec: Send + Sync {
    /// Decode message data using schema metadata.
    fn decode_dynamic(&self, data: &[u8], schema: &SchemaMetadata) -> Result<DecodedMessage>;

    /// Encode a decoded message using schema metadata.
    fn encode_dynamic(
        &mut self,
        message: &DecodedMessage,
        schema: &SchemaMetadata,
    ) -> Result<Vec<u8>>;

    /// Get the encoding type this codec handles.
    fn encoding_type(&self) -> Encoding;

    /// Reset encoder state.
    fn reset(&mut self);

    /// Get a reference as `Any` for downcasting.
    fn as_any(&self) -> &dyn std::any::Any;
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encoding_detection_cdr() {
        let factory = CodecFactory::new();

        assert_eq!(factory.detect_encoding("cdr", None), Encoding::Cdr);
        assert_eq!(factory.detect_encoding("ros2", None), Encoding::Cdr);
        assert_eq!(factory.detect_encoding("ros2msg", None), Encoding::Cdr);
    }

    #[test]
    fn test_encoding_detection_protobuf() {
        let factory = CodecFactory::new();

        assert_eq!(
            factory.detect_encoding("protobuf", None),
            Encoding::Protobuf
        );
        assert_eq!(factory.detect_encoding("proto", None), Encoding::Protobuf);
    }

    #[test]
    fn test_encoding_detection_json() {
        let factory = CodecFactory::new();

        assert_eq!(factory.detect_encoding("json", None), Encoding::Json);
    }

    #[test]
    fn test_encoding_detection_from_schema() {
        let factory = CodecFactory::new();

        assert_eq!(
            factory.detect_encoding("unknown", Some("protobuf")),
            Encoding::Protobuf
        );
        assert_eq!(
            factory.detect_encoding("unknown", Some("ros2msg")),
            Encoding::Cdr
        );
    }

    #[test]
    fn test_encoding_is_methods() {
        assert!(Encoding::Cdr.is_cdr());
        assert!(!Encoding::Cdr.is_protobuf());

        assert!(Encoding::Protobuf.is_protobuf());
        assert!(!Encoding::Protobuf.is_cdr());

        assert!(Encoding::Json.is_json());
        assert!(!Encoding::Json.is_cdr());
    }
}
