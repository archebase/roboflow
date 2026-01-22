// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

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
