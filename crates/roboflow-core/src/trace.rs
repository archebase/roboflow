// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # Request/Job ID propagation
//!
//! Provides utilities for generating and propagating request IDs through the call stack.
//!
//! ## Overview
//!
//! This module enables distributed tracing by:
//! - Generating unique request IDs
//! - Creating tracing spans with job/request context
//! - Propagating IDs through the call stack automatically
//!
//! ## Examples
//!
//! ```ignore
//! use roboflow_core::{with_request_id, generate_request_id, with_job_span};
//!
//! // Create a span with a request ID
//! let result = with_request_id(generate_request_id(), || {
//!     tracing::info!("This log includes request_id");
//!     42
//! });
//!
//! // Create a span with job context
//! with_job_span("job-123", || {
//!     tracing::info!("This log includes job_id and request_id");
//! });
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use tracing::info_span;

/// Global counter for request ID generation
static REQUEST_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate a unique request ID
///
/// Combines timestamp, counter, and random component for uniqueness
/// across distributed systems.
///
/// # Example
///
/// ```ignore
/// let request_id = generate_request_id();
/// // Returns: "req-1234567890123-0-a1b2c3d4"
/// ```
pub fn generate_request_id() -> String {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
        .as_micros();

    let counter = REQUEST_ID_COUNTER.fetch_add(1, Ordering::Relaxed);

    // Use a simple hash-based "random" component to avoid UUID dependency
    let random_part = {
        let nonce = (timestamp as u64)
            .wrapping_add(counter)
            .wrapping_mul(0x517cc1b727220a95);
        let hash = nonce.wrapping_mul(0x85ebca6b);
        format!("{:08x}", hash)
    };

    format!("req-{}-{}-{}", timestamp, counter, &random_part[..8])
}

/// Generate a job-scoped request ID
///
/// Combines job_id with a counter for job-specific tracing.
///
/// # Example
///
/// ```ignore
/// let request_id = generate_job_request_id("job-abc123");
/// // Returns: "job-job-abc123-0"
/// ```
pub fn generate_job_request_id(job_id: &str) -> String {
    let counter = REQUEST_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("job-{}-{}", job_id, counter)
}

/// Create a span with request ID
///
/// This function wraps the given closure with a tracing span that
/// includes the `request_id` field. All log statements within the
/// closure will automatically include this field.
///
/// # Example
///
/// ```ignore
/// let _span = with_request_id(generate_request_id(), || {
///     // Your code here - all logs will include request_id
///     tracing::info!("Processing request");
/// });
/// ```
pub fn with_request_id<F, R>(request_id: String, f: F) -> R
where
    F: FnOnce() -> R,
{
    let span = info_span!("request", request_id = %request_id);
    let _enter = span.enter();
    f()
}

/// Create a span with job ID and request ID
///
/// This function wraps the given closure with a tracing span that
/// includes both `job_id` and `request_id` fields. All log statements
/// within the closure will automatically include these fields.
///
/// # Example
///
/// ```ignore
/// with_job_span("job-abc123", || {
///     // All logs here include job_id and request_id
///     tracing::info!("Processing job");
/// });
/// ```
pub fn with_job_span<F, R>(job_id: &str, f: F) -> R
where
    F: FnOnce() -> R,
{
    let request_id = generate_job_request_id(job_id);
    let span = info_span!("job", job_id = %job_id, request_id = %request_id);
    let _enter = span.enter();
    f()
}

/// Create a span with dataset ID
///
/// Similar to `with_job_span` but for dataset-level tracing.
///
/// # Example
///
/// ```ignore
/// with_dataset_span("dataset-xyz", || {
///     tracing::info!("Processing dataset");
/// });
/// ```
pub fn with_dataset_span<F, R>(dataset_id: &str, f: F) -> R
where
    F: FnOnce() -> R,
{
    let span = info_span!("dataset", dataset_id = %dataset_id);
    let _enter = span.enter();
    f()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_request_id() {
        let id1 = generate_request_id();
        let id2 = generate_request_id();

        // Each ID should be unique
        assert_ne!(id1, id2);

        // Should start with "req-"
        assert!(id1.starts_with("req-"));

        // Should have format: req-{timestamp}-{counter}-{random}
        let parts: Vec<&str> = id1.split('-').collect();
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0], "req");
    }

    #[test]
    fn test_generate_job_request_id() {
        let id1 = generate_job_request_id("test-job");
        let id2 = generate_job_request_id("test-job");

        // Each ID should be unique
        assert_ne!(id1, id2);

        // Should start with "job-test-job-"
        assert!(id1.starts_with("job-test-job-"));
    }

    #[test]
    fn test_with_request_id() {
        let result = with_request_id("test-request-123".to_string(), || 42);
        assert_eq!(result, 42);
    }

    #[test]
    fn test_with_job_span() {
        let result = with_job_span("test-job", || "success");
        assert_eq!(result, "success");
    }

    #[test]
    fn test_with_dataset_span() {
        let result = with_dataset_span("test-dataset", || 123);
        assert_eq!(result, 123);
    }
}
