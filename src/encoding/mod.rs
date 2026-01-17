//! Message encoding/decoding implementations.
//!
//! This module provides codec implementations for various robotics message formats:
//! - [`cdr`] - CDR (Common Data Representation) encoding/decoding
//! - [`protobuf`] - Protobuf encoding/decoding
//! - [`json`] - JSON encoding/decoding
//! - [`codec`] - Unified codec interface

pub mod cdr;
pub mod codec;
pub mod json;
pub mod protobuf;
pub mod transform;

pub use cdr::{CdrDecoder, CdrEncoder};
pub use codec::{
    CdrSchemaTransformer, CodecFactory, DynCodec, MessageCodec, ProtobufCodec,
    ProtobufSchemaTransformer, SchemaMetadata, SchemaTransformer,
};
pub use json::JsonDecoder;
pub use protobuf::ProtobufDecoder;
