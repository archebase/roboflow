// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Codec value type system.
//!
//! Provides a unified value representation for decoded messages from CDR,
//! Protobuf, and JSON formats. All variants are serde-serializable.
//!
//! This module re-exports the CodecValue type from robocodec to avoid duplication.

pub use robocodec::{CodecValue, DecodedMessage};
