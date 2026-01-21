// Copyright (c) 2026 ArcheBase
// Roboflow is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//     http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

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
