// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Audit logging for privileged operations.
//!
//! This module provides structured logging for security-relevant operations
//! such as job cancellation, deletion, and admin actions.

use chrono::{DateTime, Utc};
use serde::Serialize;

/// Audit log entry for privileged operations.
#[derive(Debug, Clone, Serialize)]
pub struct AuditEntry {
    /// Timestamp of the operation.
    pub timestamp: DateTime<Utc>,

    /// Type of operation performed.
    pub operation: AuditOperation,

    /// User who performed the operation.
    pub actor: String,

    /// Target resource (e.g., job ID).
    pub target: String,

    /// Additional context about the operation.
    pub context: AuditContext,

    /// Whether the operation succeeded.
    pub success: bool,

    /// Error message if operation failed.
    pub error: Option<String>,
}

/// Types of audited operations.
///
/// This enum defines all possible operation types that can be recorded in the audit log.
/// Some variants may not currently be used but are reserved for future API expansion.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)] // Public API with variants reserved for future use
pub enum AuditOperation {
    /// Job was cancelled.
    JobCancel,

    /// Job was deleted.
    JobDelete,

    /// Job was retried.
    JobRetry,

    /// Multiple jobs were deleted.
    BatchJobDelete,

    /// Admin action performed.
    AdminAction,

    /// Batch job was submitted.
    BatchSubmit,

    /// Batch job was queried.
    BatchQuery,

    /// Batch job was cancelled.
    BatchCancel,
}

/// Additional context for audit entries.
#[derive(Debug, Clone, Serialize)]
pub struct AuditContext {
    /// IP address or remote endpoint (if available).
    pub source: Option<String>,

    /// Additional key-value pairs.
    pub extra: Vec<(String, String)>,
}

impl AuditContext {
    /// Create a new empty context.
    pub fn new() -> Self {
        Self {
            source: None,
            extra: Vec::new(),
        }
    }

    /// Add a key-value pair to the context.
    pub fn add(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra.push((key.into(), value.into()));
        self
    }
}

impl Default for AuditContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Audit logger for recording privileged operations.
pub struct AuditLogger;

impl AuditLogger {
    /// Log an audit entry.
    pub fn log(entry: &AuditEntry) {
        // Use JSON serialization, falling back to Debug representation on failure
        let operation = serde_json::to_string(&entry.operation)
            .unwrap_or_else(|_| format!("{:?}", entry.operation));
        let context = serde_json::to_string(&entry.context)
            .unwrap_or_else(|_| format!("{:?}", entry.context));
        let timestamp = entry.timestamp.format("%Y-%m-%dT%H:%M:%SZ").to_string();

        // Warn if serialization failed (indicates potential audit data loss)
        // Debug format produces "(...)" while valid JSON produces "{...}"
        if !operation.contains('{') || !context.contains('{') {
            tracing::warn!(
                target: "audit",
                "JSON serialization failed for audit entry, using Debug format instead"
            );
        }

        if entry.success {
            tracing::info!(
                target: "audit",
                timestamp = %timestamp,
                operation = %operation,
                actor = %entry.actor,
                target = %entry.target,
                context = %context,
                success = entry.success,
                "Audit: {} by {} on {}",
                operation, entry.actor, entry.target
            );
        } else {
            tracing::warn!(
                target: "audit",
                timestamp = %timestamp,
                operation = %operation,
                actor = %entry.actor,
                target = %entry.target,
                context = %context,
                success = entry.success,
                error = %entry.error.as_deref().unwrap_or("unknown"),
                "Audit FAILED: {} by {} on {} - {}",
                operation, entry.actor, entry.target,
                entry.error.as_deref().unwrap_or("unknown")
            );
        }
    }

    /// Log a successful operation.
    pub fn log_success(
        operation: AuditOperation,
        actor: &str,
        target: &str,
        context: &AuditContext,
    ) {
        let entry = AuditEntry {
            timestamp: Utc::now(),
            operation,
            actor: actor.to_string(),
            target: target.to_string(),
            context: context.clone(),
            success: true,
            error: None,
        };
        Self::log(&entry);
    }

    /// Log a failed operation.
    pub fn log_failure(
        operation: AuditOperation,
        actor: &str,
        target: &str,
        context: &AuditContext,
        error: &str,
    ) {
        let entry = AuditEntry {
            timestamp: Utc::now(),
            operation,
            actor: actor.to_string(),
            target: target.to_string(),
            context: context.clone(),
            success: false,
            error: Some(error.to_string()),
        };
        Self::log(&entry);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_context_builder() {
        let context = AuditContext::new()
            .add("key1", "value1")
            .add("key2", "value2");

        assert_eq!(context.source, None);
        assert_eq!(context.extra.len(), 2);
    }
}
