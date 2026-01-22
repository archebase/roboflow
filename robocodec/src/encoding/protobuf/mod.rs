// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Protobuf codec module.
//!
//! Provides Protobuf decoding support.

pub mod codec;
pub mod decoder;

pub use codec::ProtobufCodec;
pub use decoder::ProtobufDecoder;
