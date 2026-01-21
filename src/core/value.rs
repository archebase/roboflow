// Copyright (c) 2026 ArcheBase
// Roboflow is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//     http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

//! Codec value type system.
//!
//! Provides a unified value representation for decoded messages from CDR,
//! Protobuf, and JSON formats. All variants are serde-serializable.
//!
//! This module re-exports the CodecValue type from robocodec to avoid duplication.

pub use robocodec::{CodecValue, DecodedMessage};
