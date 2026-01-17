//! Core types used throughout robocodec.
//!
//! This module provides the foundational types for the library:
//! - [`Error`] - Comprehensive error handling
//! - [`CodecValue`] - Unified value representation
//! - [`TypeRegistry`] - Schema type registry

pub mod error;
pub mod registry;
pub mod value;

pub use error::{CodecError, Result};
pub use registry::{SchemaProvider, TypeAccessor, TypeRegistry};
pub use value::{CodecValue, DecodedMessage, PrimitiveType};
