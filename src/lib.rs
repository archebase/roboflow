//! # Robocodec
//!
//! A universal, schema-driven runtime decoding engine for Rust.
//! Supports CDR, Protobuf, and JSON message formats with full MCAP support.
//!
//! ## Modules
//!
//! - [`core`] - Core types (CodecValue, errors, registry)
//! - [`schema`] - IDL/MSG schema parser using Pest
//! - [`encoding`] - Message encoding/decoding (CDR, Protobuf, JSON)
//! - [`format`] - File format handlers (MCAP, ROS1 bag)
//!
//! ## Example
//!
//! ```ignore
//! use robocodec::{schema, encoding::CdrDecoder};
//!
//! // Parse a schema
//! let schema = schema::parse_schema("TestMsg", "int32 value\nfloat64 data")?;
//!
//! // Decode CDR data
//! let decoder = CdrDecoder::new();
//! let decoded = decoder.decode(&schema, &[0x2A, 0x00, 0x00, 0x00, ...])?;
//! ```

// =============================================================================
// Core modules
// =============================================================================
pub mod config;
pub mod core;
pub mod reader;
pub mod writer;
pub mod rewriter;

// =============================================================================
// Schema parsing
// =============================================================================
pub mod schema;

// =============================================================================
// Encoding/decoding
// =============================================================================
pub mod encoding;

// =============================================================================
// File formats
// =============================================================================
pub mod format;

// =============================================================================
// Re-exports for convenience
// =============================================================================

// Core types
pub use core::{
    CodecError, CodecValue, DecodedMessage, PrimitiveType, Result,
    SchemaProvider, TypeAccessor, TypeRegistry,
};

// Schema parsing
pub use schema::{parse_schema, Field, FieldType, MessageSchema, SchemaFormat};

// Encoding
pub use encoding::{
    CdrDecoder, CdrEncoder, CodecFactory, DynCodec, JsonDecoder, MessageCodec,
    ProtobufCodec, ProtobufDecoder,
};

// Schema transformers
pub use encoding::{
    CdrSchemaTransformer, ProtobufSchemaTransformer, SchemaMetadata, SchemaTransformer,
};

// File formats - MCAP
pub use format::{
    ChannelInfo, DecodedMessageIter, DecodedMessageStream, DecodedMessageWithTimestampIter,
    DecodedMessageWithTimestampStream, McapReader, McapRewriter, RawMessage, RawMessageIter, RawMessageStream,
    TimestampedDecodedMessage,
};

// Unified reader (supports both MCAP and BAG)
pub use reader::RoboReader;

// Unified writer (supports both MCAP and BAG)
pub use writer::RoboWriter;

// Unified rewriter
pub use rewriter::{detect_format, FormatRewriter, RewriteOptions, RewriteStats, RoboRewriter};

// File formats - ROS1 bag
pub use format::{BagMessage, BagWriter};

// File formats - LeRobot
pub use format::lerobot::{Hdf5LeRobotWriter, LeRobotConfig, Mapping, MappingType, ParquetLeRobotWriter};

// MCAP transforms
pub use format::mcap::transform::{
    McapTransform, TopicAwareTypeRenameTransform, TopicRenameTransform, TransformBuilder,
    TransformError, TransformPipeline, TransformedChannel, TypeNormalization, TypeRenameTransform,
};

// Configuration
pub use config::{NormalizeConfig, TopicMapping, TopicTypeMapping};

// MCAP rewrite engine (format-specific)
pub use format::mcap::{McapRewriteEngine, McapRewriteStats};

// =============================================================================
// Legacy Type Aliases for strata-core compatibility
// =============================================================================

/// Type alias for legacy code - use `CodecValue` in new code
pub type MessageValue = CodecValue;

/// Type alias for legacy code - use `ProtobufDecoder` in new code
pub type ProtoDecoder = ProtobufDecoder;

/// Decoder trait for generic decoding operations.
///
/// This trait provides a common interface for all decoders in robocodec.
pub trait Decoder: Send + Sync {
    /// Decode data into a DecodedMessage.
    fn decode(&self, data: &[u8], schema: &str, type_name: Option<&str>) -> Result<DecodedMessage>;
}

/// Encoding format identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Encoding {
    /// CDR (Common Data Representation) encoding
    Cdr,
    /// Protobuf encoding
    Protobuf,
    /// JSON encoding
    Json,
}

impl Encoding {
    /// Check if this encoding is CDR.
    pub fn is_cdr(&self) -> bool {
        matches!(self, Encoding::Cdr)
    }

    /// Check if this encoding is Protobuf.
    pub fn is_protobuf(&self) -> bool {
        matches!(self, Encoding::Protobuf)
    }

    /// Check if this encoding is JSON.
    pub fn is_json(&self) -> bool {
        matches!(self, Encoding::Json)
    }

    /// Convert to string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            Encoding::Cdr => "cdr",
            Encoding::Protobuf => "protobuf",
            Encoding::Json => "json",
        }
    }
}

/// Prelude module for common imports.
pub mod prelude {
    pub use crate::encoding::CdrDecoder;
    pub use crate::core::*;
    pub use crate::schema::{parse_schema, SchemaFormat};
    pub use crate::encoding::JsonDecoder;
    pub use crate::encoding::ProtobufDecoder;
}

// =============================================================================
// Tests (conditional compilation)
// =============================================================================
#[cfg(test)]
mod reader_tests;

// =============================================================================
// Python bindings (conditional compilation)
// =============================================================================
#[cfg(feature = "python")]
pub mod python;
