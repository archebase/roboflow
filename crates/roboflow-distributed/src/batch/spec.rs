// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Batch job specification schema.
//!
//! Defines the desired state for a batch conversion job.
//! Inspired by Kubernetes Job specification.
//!
//! ## Example
//!
//! ```yaml
//! apiVersion: roboflow/v1
//! kind: BatchJob
//! metadata:
//!   name: "bag-conversion-20250130"
//! spec:
//!   sources:
//!     - url: "s3://bucket/path/*.bag"
//!   output: "s3://bucket/output/"
//!   config: "default"
//!   parallelism: 100
//!   backoffLimit: 10
//! ```

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// API version for batch job specifications.
pub const API_VERSION: &str = "roboflow/v1";

/// Kind of resource.
pub const KIND_BATCH_JOB: &str = "BatchJob";

/// Batch job specification.
///
/// This represents the "desired state" submitted by users.
/// The controller reconciles the actual state to match this spec.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSpec {
    /// API version for schema compatibility.
    pub api_version: String,

    /// Kind of resource (always "BatchJob").
    pub kind: String,

    /// Metadata for the batch job.
    pub metadata: BatchMetadata,

    /// Specification for the batch job.
    pub spec: BatchJobSpec,
}

/// Metadata for a batch job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchMetadata {
    /// Name of the batch job (must be unique).
    pub name: String,

    /// Namespace for grouping jobs (optional).
    #[serde(default)]
    pub namespace: String,

    /// User who submitted this job.
    #[serde(default)]
    pub submitted_by: Option<String>,

    /// Labels for filtering and grouping.
    #[serde(default)]
    pub labels: HashMap<String, String>,

    /// Annotations for arbitrary metadata.
    #[serde(default)]
    pub annotations: HashMap<String, String>,

    /// Creation timestamp (set by system).
    #[serde(default = "Utc::now")]
    pub created_at: DateTime<Utc>,
}

impl Default for BatchMetadata {
    fn default() -> Self {
        Self {
            name: String::new(),
            namespace: "default".to_string(),
            submitted_by: None,
            labels: HashMap::new(),
            annotations: HashMap::new(),
            created_at: Utc::now(),
        }
    }
}

/// Specification for a batch job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchJobSpec {
    /// Source URLs to process.
    /// Supports glob patterns: "s3://bucket/path/*.bag"
    pub sources: Vec<SourceUrl>,

    /// Output URL for converted dataset.
    pub output: String,

    /// Configuration to use (name or hash).
    /// "default" uses built-in config.
    /// Hash refers to a stored config in TiKV.
    pub config: String,

    /// Maximum parallelism (concurrent work units).
    /// 0 means unlimited (bounded by cluster size).
    #[serde(default = "default_parallelism")]
    pub parallelism: u32,

    /// Maximum number of work units to process.
    /// None means process all discovered files.
    pub completions: Option<u32>,

    /// Maximum number of retries per work unit.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Backoff limit for failed work units.
    /// Job is marked failed if this many work units fail.
    #[serde(default = "default_backoff_limit")]
    pub backoff_limit: u32,

    /// TTL for the batch job after completion (seconds).
    /// Jobs are auto-deleted after this time.
    #[serde(default = "default_ttl")]
    pub ttl_seconds: u32,

    /// Priority for this job (higher = scheduled first).
    #[serde(default = "default_priority")]
    pub priority: i32,

    /// Work unit configuration.
    #[serde(default)]
    pub work_unit_config: WorkUnitConfig,
}

fn default_parallelism() -> u32 {
    100
}

fn default_max_retries() -> u32 {
    3
}

fn default_backoff_limit() -> u32 {
    10
}

fn default_ttl() -> u32 {
    86400 // 24 hours
}

fn default_priority() -> i32 {
    0
}

impl Default for BatchJobSpec {
    fn default() -> Self {
        Self {
            sources: Vec::new(),
            output: String::new(),
            config: "default".to_string(),
            parallelism: default_parallelism(),
            completions: None,
            max_retries: default_max_retries(),
            backoff_limit: default_backoff_limit(),
            ttl_seconds: default_ttl(),
            priority: default_priority(),
            work_unit_config: WorkUnitConfig::default(),
        }
    }
}

/// Source URL configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceUrl {
    /// URL with optional glob pattern.
    pub url: String,

    /// Optional filter regex for file selection.
    pub filter: Option<String>,

    /// Optional size limit (bytes).
    pub max_size: Option<u64>,
}

/// Work unit configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkUnitConfig {
    /// Strategy for partitioning files into work units.
    #[serde(default = "default_partition_strategy")]
    pub partition_strategy: PartitionStrategy,

    /// Maximum files per work unit.
    #[serde(default = "default_files_per_unit")]
    pub max_files_per_unit: usize,

    /// Maximum bytes per work unit.
    #[serde(default = "default_bytes_per_unit")]
    pub max_bytes_per_unit: u64,
}

fn default_partition_strategy() -> PartitionStrategy {
    PartitionStrategy::FileBased
}

fn default_files_per_unit() -> usize {
    1
}

fn default_bytes_per_unit() -> u64 {
    5 * 1024 * 1024 * 1024 // 5GB
}

/// Strategy for partitioning work.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum PartitionStrategy {
    /// Each file is a separate work unit.
    #[serde(rename = "file")]
    #[default]
    FileBased,

    /// Group multiple files per work unit (by size).
    #[serde(rename = "size")]
    SizeBased,

    /// Use time-based grouping for time-series data.
    #[serde(rename = "time")]
    TimeBased,
}

impl BatchSpec {
    /// Create a new batch spec.
    pub fn new(name: impl Into<String>, sources: Vec<String>, output: String) -> Self {
        Self {
            api_version: API_VERSION.to_string(),
            kind: KIND_BATCH_JOB.to_string(),
            metadata: BatchMetadata {
                name: name.into(),
                namespace: "default".to_string(),
                submitted_by: None,
                labels: HashMap::new(),
                annotations: HashMap::new(),
                created_at: Utc::now(),
            },
            spec: BatchJobSpec {
                sources: sources
                    .into_iter()
                    .map(|url| SourceUrl {
                        url,
                        filter: None,
                        max_size: None,
                    })
                    .collect(),
                output,
                config: "default".to_string(),
                parallelism: default_parallelism(),
                completions: None,
                max_retries: default_max_retries(),
                backoff_limit: default_backoff_limit(),
                ttl_seconds: default_ttl(),
                priority: default_priority(),
                work_unit_config: WorkUnitConfig::default(),
            },
        }
    }

    /// Validate the batch spec.
    pub fn validate(&self) -> Result<(), BatchSpecError> {
        if self.metadata.name.is_empty() {
            return Err(BatchSpecError::InvalidName(
                "name cannot be empty".to_string(),
            ));
        }

        if self.spec.sources.is_empty() {
            return Err(BatchSpecError::Validation(
                "at least one source URL is required".to_string(),
            ));
        }

        for source in &self.spec.sources {
            self.validate_source_url(&source.url)?;
        }

        self.validate_output_url(&self.spec.output)?;

        Ok(())
    }

    fn validate_source_url(&self, url: &str) -> Result<(), BatchSpecError> {
        if !url.starts_with("s3://")
            && !url.starts_with("oss://")
            && !url.starts_with("file://")
        {
            return Err(BatchSpecError::InvalidUrl {
                url: url.to_string(),
                reason: "invalid scheme (must be s3://, oss://, or file://)".to_string(),
            });
        }
        Ok(())
    }

    fn validate_output_url(&self, url: &str) -> Result<(), BatchSpecError> {
        if !url.starts_with("s3://")
            && !url.starts_with("oss://")
            && !url.starts_with("file://")
        {
            return Err(BatchSpecError::InvalidUrl {
                url: url.to_string(),
                reason: "invalid scheme (must be s3://, oss://, or file://)".to_string(),
            });
        }
        Ok(())
    }

    /// Encode to JSON for storage.
    pub fn to_json(&self) -> Result<String, BatchSpecError> {
        serde_json::to_string_pretty(self)
            .map_err(|e| BatchSpecError::Serialization(format!("failed to encode: {}", e)))
    }

    /// Decode from JSON.
    pub fn from_json(json: &str) -> Result<Self, BatchSpecError> {
        serde_json::from_str(json)
            .map_err(|e| BatchSpecError::Serialization(format!("failed to decode: {}", e)))
    }

    /// Create from YAML.
    pub fn from_yaml(yaml: &str) -> Result<Self, BatchSpecError> {
        serde_yaml::from_str(yaml)
            .map_err(|e| BatchSpecError::Serialization(format!("failed to parse YAML: {}", e)))
    }

    /// Convert to YAML.
    pub fn to_yaml(&self) -> Result<String, BatchSpecError> {
        serde_yaml::to_string(self)
            .map_err(|e| BatchSpecError::Serialization(format!("failed to serialize: {}", e)))
    }

    /// Get the unique key for this batch spec.
    pub fn key(&self) -> String {
        format!("{}:{}", self.metadata.namespace, self.metadata.name)
    }
}

impl Default for WorkUnitConfig {
    fn default() -> Self {
        Self {
            partition_strategy: default_partition_strategy(),
            max_files_per_unit: default_files_per_unit(),
            max_bytes_per_unit: default_bytes_per_unit(),
        }
    }
}

/// Errors related to batch specification.
#[derive(Debug, thiserror::Error)]
pub enum BatchSpecError {
    #[error("invalid name: {0}")]
    InvalidName(String),

    #[error("validation error: {0}")]
    Validation(String),

    #[error("invalid URL '{url}': {reason}")]
    InvalidUrl { url: String, reason: String },

    #[error("serialization error: {0}")]
    Serialization(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_spec_new() {
        let spec = BatchSpec::new(
            "test-batch",
            vec!["s3://bucket/file.mcap".to_string()],
            "s3://bucket/output/".to_string(),
        );

        assert_eq!(spec.api_version, API_VERSION);
        assert_eq!(spec.kind, KIND_BATCH_JOB);
        assert_eq!(spec.metadata.name, "test-batch");
        assert_eq!(spec.spec.sources.len(), 1);
        assert_eq!(spec.spec.output, "s3://bucket/output/");
    }

    #[test]
    fn test_batch_spec_validate_success() {
        let spec = BatchSpec::new(
            "test-batch",
            vec!["s3://bucket/file.mcap".to_string()],
            "s3://bucket/output/".to_string(),
        );

        assert!(spec.validate().is_ok());
    }

    #[test]
    fn test_batch_spec_validate_empty_name() {
        let spec = BatchSpec::new(
            "",
            vec!["s3://bucket/file.mcap".to_string()],
            "s3://bucket/output/".to_string(),
        );

        match spec.validate() {
            Err(BatchSpecError::InvalidName(_)) => {}
            other => panic!("expected InvalidName error, got {:?}", other),
        }
    }

    #[test]
    fn test_batch_spec_validate_no_sources() {
        let spec = BatchSpec {
            api_version: API_VERSION.to_string(),
            kind: KIND_BATCH_JOB.to_string(),
            metadata: BatchMetadata {
                name: "test".to_string(),
                ..Default::default()
            },
            spec: BatchJobSpec {
                sources: vec![],
                output: "s3://bucket/output/".to_string(),
                ..Default::default()
            },
        };

        match spec.validate() {
            Err(BatchSpecError::Validation(_)) => {}
            other => panic!("expected Validation error, got {:?}", other),
        }
    }

    #[test]
    fn test_batch_spec_validate_invalid_scheme() {
        let spec = BatchSpec::new(
            "test-batch",
            vec!["ftp://bucket/file.mcap".to_string()],
            "s3://bucket/output/".to_string(),
        );

        match spec.validate() {
            Err(BatchSpecError::InvalidUrl { .. }) => {}
            other => panic!("expected InvalidUrl error, got {:?}", other),
        }
    }

    #[test]
    fn test_batch_spec_yaml_roundtrip() {
        let spec = BatchSpec::new(
            "test-batch",
            vec!["s3://bucket/file.mcap".to_string()],
            "s3://bucket/output/".to_string(),
        );

        let yaml = spec.to_yaml().unwrap();
        let decoded = BatchSpec::from_yaml(&yaml).unwrap();

        assert_eq!(decoded.metadata.name, "test-batch");
        assert_eq!(decoded.spec.sources.len(), 1);
    }

    #[test]
    fn test_partition_strategy_serialization() {
        let config = WorkUnitConfig::default();
        assert_eq!(config.partition_strategy, PartitionStrategy::FileBased);
        assert_eq!(config.max_files_per_unit, 1);
        assert_eq!(config.max_bytes_per_unit, 5 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_batch_spec_key() {
        let spec = BatchSpec::new(
            "my-batch",
            vec!["s3://bucket/*.bag".to_string()],
            "s3://out/".to_string(),
        );

        assert_eq!(spec.key(), "default:my-batch");
    }

    #[test]
    fn test_batch_spec_with_custom_metadata() {
        let mut spec = BatchSpec::new(
            "my-batch",
            vec!["s3://bucket/file.bag".to_string()],
            "s3://out/".to_string(),
        );

        spec.metadata.submitted_by = Some("user1".to_string());
        spec.metadata.labels.insert("project".to_string(), "robot-car".to_string());

        assert_eq!(spec.metadata.submitted_by, Some("user1".to_string()));
        assert_eq!(spec.metadata.labels.get("project"), Some(&"robot-car".to_string()));
    }

    #[test]
    fn test_batch_spec_json_roundtrip() {
        let spec = BatchSpec::new(
            "test-batch",
            vec!["oss://bucket/*.bag".to_string()],
            "oss://output/".to_string(),
        );

        let json = spec.to_json().unwrap();
        let decoded = BatchSpec::from_json(&json).unwrap();

        assert_eq!(decoded.metadata.name, spec.metadata.name);
        assert_eq!(decoded.spec.output, spec.spec.output);
    }
}
