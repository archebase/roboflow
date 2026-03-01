// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Output path resolution for batch processing.

use crate::batch::BatchSpec;
use roboflow_storage::StorageUrl;
use sha2::{Digest, Sha256};

/// Resolve the effective output path for a batch.
///
/// Behavior:
/// - Default: if output is a base S3 bucket URL (`s3://bucket` or `s3://bucket/`),
///   expand to deterministic prefix:
///   `s3://bucket/datasets/<hash>/`
/// - Explicit output prefixes always win (kept unchanged).
pub fn resolve_batch_output_path(spec: &BatchSpec) -> String {
    let configured = spec.spec.output.trim();

    let parsed: StorageUrl = match configured.parse() {
        Ok(v) => v,
        Err(_) => return configured.to_string(),
    };

    // Only expand base S3 bucket output.
    let Some(bucket) = parsed.bucket() else {
        return configured.to_string();
    };
    if !parsed.path().is_empty() {
        return configured.to_string();
    }

    let hash = compute_batch_identity_hash(spec);
    format!("s3://{}/datasets/{}/", bucket, hash)
}

/// Build staging path from an effective output path.
///
/// For remote S3 outputs, staging is always rooted at the bucket:
/// `s3://<bucket>/staging/{batch_id}/worker_{pod_id}/unit_{unit_id}`
///
/// For local outputs, staging remains under the local output root.
pub fn build_staging_path(
    output_path: &str,
    batch_id: &str,
    pod_id: &str,
    unit_id: &str,
) -> Result<String, String> {
    let parsed: StorageUrl = output_path
        .parse()
        .map_err(|e: roboflow_storage::StorageError| {
            format!("Invalid output path '{}': {}", output_path, e)
        })?;

    if parsed.is_remote() {
        let bucket = parsed
            .bucket()
            .ok_or_else(|| format!("Missing bucket in remote output path: {}", output_path))?;
        let scheme = output_path
            .split_once("://")
            .map(|(s, _)| s)
            .unwrap_or("s3");
        Ok(format!(
            "{}://{}/staging/{}/worker_{}/unit_{}",
            scheme, bucket, batch_id, pod_id, unit_id
        ))
    } else {
        Ok(format!(
            "{}/staging/{}/worker_{}/unit_{}",
            output_path.trim_end_matches('/'),
            batch_id,
            pod_id,
            unit_id
        ))
    }
}

fn compute_batch_identity_hash(spec: &BatchSpec) -> String {
    let mut hasher = Sha256::new();

    hasher.update(spec.api_version.as_bytes());
    hasher.update(b"\n");
    hasher.update(spec.kind.as_bytes());
    hasher.update(b"\n");
    hasher.update(spec.metadata.namespace.as_bytes());
    hasher.update(b"\n");
    hasher.update(spec.metadata.name.as_bytes());
    hasher.update(b"\n");
    hasher.update(spec.spec.config.as_bytes());
    hasher.update(b"\n");
    hasher.update(spec.spec.episodes_per_chunk.to_string().as_bytes());
    hasher.update(b"\n");

    for source in &spec.spec.sources {
        hasher.update(source.url.as_bytes());
        hasher.update(b"|");
        if let Some(filter) = &source.filter {
            hasher.update(filter.as_bytes());
        }
        hasher.update(b"|");
        if let Some(max_size) = source.max_size {
            hasher.update(max_size.to_string().as_bytes());
        }
        hasher.update(b"\n");
    }

    if let Some(tenant) = spec.metadata.labels.get("tenant") {
        hasher.update(b"tenant=");
        hasher.update(tenant.as_bytes());
        hasher.update(b"\n");
    }
    if let Some(project) = spec.metadata.labels.get("project") {
        hasher.update(b"project=");
        hasher.update(project.as_bytes());
        hasher.update(b"\n");
    }

    let digest = hasher.finalize();
    let hex = format!("{:x}", digest);
    hex[..16].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::BatchSpec;
    use std::{collections::HashSet, env};

    fn mk_spec(output: &str) -> BatchSpec {
        BatchSpec::new(
            "job-name",
            vec!["s3://roboflow-raw/path/file.bag".to_string()],
            output.to_string(),
        )
    }

    #[test]
    fn test_keep_explicit_output_prefix() {
        let spec = mk_spec("s3://roboflow-datasets/custom/prefix/");
        let old = env::var("OUTPUT_PATH_MODE").ok();
        // SAFETY: Tests run single-process here and this scoped set/remove is acceptable.
        unsafe { env::set_var("OUTPUT_PATH_MODE", "hashed_prefix") };
        let resolved = resolve_batch_output_path(&spec);
        if let Some(v) = old {
            // SAFETY: Restoring process env after this test.
            unsafe { env::set_var("OUTPUT_PATH_MODE", v) };
        } else {
            // SAFETY: Restoring process env after this test.
            unsafe { env::remove_var("OUTPUT_PATH_MODE") };
        }

        assert_eq!(resolved, "s3://roboflow-datasets/custom/prefix/");
    }

    #[test]
    fn test_default_mode_expands_base_bucket() {
        let spec = mk_spec("s3://roboflow-datasets/");
        let old = env::var("OUTPUT_PATH_MODE").ok();
        // SAFETY: Restoring process env after this test.
        unsafe { env::remove_var("OUTPUT_PATH_MODE") };
        let resolved = resolve_batch_output_path(&spec);
        if let Some(v) = old {
            // SAFETY: Restoring process env after this test.
            unsafe { env::set_var("OUTPUT_PATH_MODE", v) };
        }

        assert!(resolved.starts_with("s3://roboflow-datasets/datasets/"));
        assert!(resolved.ends_with('/'));
    }

    #[test]
    fn test_expand_base_bucket_to_hashed_prefix() {
        let spec = mk_spec("s3://roboflow-datasets/");
        let old = env::var("OUTPUT_PATH_MODE").ok();
        // SAFETY: Tests run single-process here and this scoped set/remove is acceptable.
        unsafe { env::set_var("OUTPUT_PATH_MODE", "hashed_prefix") };
        let resolved = resolve_batch_output_path(&spec);
        if let Some(v) = old {
            // SAFETY: Restoring process env after this test.
            unsafe { env::set_var("OUTPUT_PATH_MODE", v) };
        } else {
            // SAFETY: Restoring process env after this test.
            unsafe { env::remove_var("OUTPUT_PATH_MODE") };
        }

        assert!(resolved.starts_with("s3://roboflow-datasets/datasets/"));
        assert!(resolved.ends_with('/'));
    }

    #[test]
    fn test_staging_under_bucket_root() {
        let spec = mk_spec("s3://foo/");
        let output = resolve_batch_output_path(&spec);
        let staging = build_staging_path(&output, "jobs:abc", "pod-1", "unit-1")
            .expect("staging path should build");

        assert!(staging.starts_with("s3://foo/staging/"));
    }

    #[test]
    fn test_dataset_under_bucket_root() {
        let spec = mk_spec("s3://foo/");
        let output = resolve_batch_output_path(&spec);
        assert!(output.starts_with("s3://foo/datasets/"));
    }

    #[test]
    fn test_bucket_has_datasets_and_staging_roots() {
        let spec = mk_spec("s3://foo/");
        let output = resolve_batch_output_path(&spec);
        let staging = build_staging_path(&output, "jobs:abc", "pod-1", "unit-1")
            .expect("staging path should build");

        let output_url: StorageUrl = output.parse().expect("valid output url");
        let staging_url: StorageUrl = staging.parse().expect("valid staging url");

        let output_root = output_url.path().split('/').next().unwrap_or("");
        let staging_root = staging_url.path().split('/').next().unwrap_or("");

        let roots: HashSet<&str> = [output_root, staging_root].into_iter().collect();
        assert!(roots.contains("datasets"));
        assert!(roots.contains("staging"));
        assert_eq!(roots.len(), 2);
    }
}
