// Copyright (c) 2026 ArcheBase
// Roboflow is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//     http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

//! Core types used throughout roboflow.
//!
//! This module provides the foundational types for the library:
//! - [`Error`] - Comprehensive error handling
//! - [`CodecValue`] - Unified value representation
//! - [`TypeRegistry`] - Schema type registry
//!
//! Hardware detection utilities are in `pipeline::hardware`.

pub mod error;
pub mod registry;
pub mod value;

pub use error::{Result, RoboflowError};
pub use registry::{SchemaProvider, TypeAccessor, TypeRegistry};
pub use value::{CodecValue, DecodedMessage};
// PrimitiveType is in schema module
pub use robocodec::schema::PrimitiveType;
// CodecError from robocodec
pub use robocodec::core::CodecError;
