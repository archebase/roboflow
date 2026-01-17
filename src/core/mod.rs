//! Core types used throughout robocodec.
//!
//! This module provides the foundational types for the library:
//! - [`Error`] - Comprehensive error handling
//! - [`CodecValue`] - Unified value representation
//! - [`TypeRegistry`] - Schema type registry
//! - [`HardwareInfo`] - Hardware detection
//! - [`detect_cpu_count`] - Utility functions

pub mod error;
pub mod hardware;
pub mod registry;
pub mod utils;
pub mod value;

pub use error::{CodecError, Result};
pub use hardware::HardwareInfo;
pub use registry::{SchemaProvider, TypeAccessor, TypeRegistry};
pub use utils::detect_cpu_count;
pub use value::{CodecValue, DecodedMessage, PrimitiveType};
