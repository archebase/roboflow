// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Message encoding/decoding implementations.
//!
//! This module provides codec implementations for various robotics message formats:
//! - [`cdr`] - CDR (Common Data Representation) encoding/decoding
//! - [`protobuf`] - Protobuf encoding/decoding
//! - [`json`] - JSON encoding/decoding
//! - [`codec`] - Unified codec interface
//! - [`registry`] - Codec registry for plugin-based codec selection

pub mod cdr;
pub mod codec;
pub mod json;
pub mod protobuf;
pub mod registry;
pub mod transform;

pub use cdr::{CdrDecoder, CdrEncoder};
pub use codec::{
    CdrSchemaTransformer, CodecFactory, DynCodec, MessageCodec, ProtobufCodec,
    ProtobufSchemaTransformer, SchemaMetadata, SchemaTransformer,
};
pub use json::JsonDecoder;
pub use protobuf::ProtobufDecoder;
pub use registry::{global_registry, Codec, CodecProviderFactory, CodecRegistry};
