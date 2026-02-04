// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # roboflow-core
//!
//! Core types used throughout the roboflow workspace.
//!
//! This crate provides the foundational types for all roboflow crates:
//! - [`RoboflowError`] - Comprehensive error handling
//! - [`CodecValue`] - Unified value representation
//! - [`TypeRegistry`] - Schema type registry
//! - [`Encoding`] - Message format identifier
//!
//! ## Design Philosophy
//!
//! This crate has NO feature flags and NO conditional compilation.
//! All types are always available for use by other roboflow crates.

pub mod error;
pub mod logging;
pub mod registry;
pub mod retry;
pub mod trace;
pub mod value;

// Re-export core types for convenience
pub use error::{ErrorCategory, Result, RoboflowError};
pub use logging::{LogFormat, LoggingConfig, init_logging, init_logging_with};
pub use registry::{Encoding, SchemaProvider, TypeAccessor, TypeRegistry};
pub use retry::{IsRetryableRef, RetryConfig, retry_with_backoff};
pub use trace::{
    generate_job_request_id, generate_request_id, with_dataset_span, with_job_span, with_request_id,
};
pub use value::{CodecValue, DecodedMessage};

// Re-export from robocodec
pub use robocodec::core::CodecError;
pub use robocodec::schema::PrimitiveType;
