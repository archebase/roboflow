// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! S3 environment configuration bridge for robocodec.
//!
//! This module provides a two-layer pattern for S3 credential management:
//! 1. Read from outer env (RF_S3_* vars) with fallback to AWS-standard names
//! 2. Normalize into S3BridgeConfig
//! 3. Apply once to AWS env vars before any robocodec reads
//!
//! Precedence: explicit RF_S3_* > existing AWS_* env > defaults

use std::sync::OnceLock;

/// S3 configuration bridge for AWS SDK resolution.
#[derive(Debug, Clone, Default)]
pub struct S3BridgeConfig {
    /// AWS access key ID.
    pub access_key_id: Option<String>,
    /// AWS secret access key.
    pub secret_access_key: Option<String>,
    /// AWS session token (optional).
    pub session_token: Option<String>,
    /// AWS region (optional but recommended).
    pub region: Option<String>,
    /// AWS endpoint URL (optional for S3-compatible services).
    pub endpoint_url: Option<String>,
}

impl S3BridgeConfig {
    /// Read configuration from outer environment.
    ///
    /// Checks RF_S3_* vars first, then falls back to AWS-standard names.
    /// Precedence: RF_S3_* > AWS_* > defaults
    pub fn from_outer_env() -> Self {
        fn get(keys: &[&str]) -> Option<String> {
            keys.iter()
                .find_map(|k| std::env::var(k).ok().filter(|v| !v.is_empty()))
        }

        Self {
            // Prefer project-specific names, fallback to AWS names if already set
            access_key_id: get(&["RF_S3_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID"]),
            secret_access_key: get(&["RF_S3_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY"]),
            session_token: get(&["RF_S3_SESSION_TOKEN", "AWS_SESSION_TOKEN"]),
            region: get(&["RF_S3_REGION", "AWS_REGION", "AWS_DEFAULT_REGION"]),
            endpoint_url: get(&["RF_S3_ENDPOINT", "AWS_ENDPOINT_URL"]),
        }
    }

    /// Build from the default roboflow config file as fallback.
    pub fn from_roboflow_config(config: &roboflow_storage::RoboflowConfig) -> Self {
        Self {
            access_key_id: config.s3_access_key_id().map(String::from),
            secret_access_key: config.s3_access_key_secret().map(String::from),
            session_token: None,
            region: config.s3_region().map(String::from),
            endpoint_url: config.s3_endpoint().map(String::from),
        }
    }

    /// Validate that credentials are consistent.
    ///
    /// Returns error if only one of access_key/secret_key is provided.
    pub fn validate(&self) -> Result<(), String> {
        match (&self.access_key_id, &self.secret_access_key) {
            (Some(_), None) => Err("RF_S3_SECRET_ACCESS_KEY or AWS_SECRET_ACCESS_KEY must be set when access key is provided".into()),
            (None, Some(_)) => Err("RF_S3_ACCESS_KEY_ID or AWS_ACCESS_KEY_ID must be set when secret key is provided".into()),
            _ => Ok(()),
        }
    }

    /// Check if any S3 configuration is present.
    pub fn is_empty(&self) -> bool {
        self.access_key_id.is_none()
            && self.secret_access_key.is_none()
            && self.session_token.is_none()
            && self.region.is_none()
            && self.endpoint_url.is_none()
    }

    /// Apply configuration to AWS-standard environment variables.
    ///
    /// Only sets vars that are not already present (idempotent).
    /// Uses `unsafe` set_var as required by Rust 2024 edition.
    pub fn apply_to_aws_env_if_missing(&self) {
        fn set_if_missing(key: &str, val: &Option<String>) {
            if let (true, Some(v)) = (std::env::var_os(key).is_none(), val) {
                // Rust 2024: std::env::set_var is unsafe
                unsafe { std::env::set_var(key, v) };
            }
        }

        set_if_missing("AWS_ACCESS_KEY_ID", &self.access_key_id);
        set_if_missing("AWS_SECRET_ACCESS_KEY", &self.secret_access_key);
        set_if_missing("AWS_SESSION_TOKEN", &self.session_token);
        set_if_missing("AWS_REGION", &self.region);
        set_if_missing("AWS_ENDPOINT_URL", &self.endpoint_url);

        // robocodec-specific: reads S3_ENDPOINT when URL has no endpoint query
        set_if_missing("S3_ENDPOINT", &self.endpoint_url);

        // Service-specific aliases for AWS SDK compatibility
        set_if_missing("AWS_ENDPOINT_URL_S3", &self.endpoint_url);
        set_if_missing("AWS_DEFAULT_REGION", &self.region);

        // S3-compatible endpoints (MinIO/OSS) often require path-style addressing
        if self.endpoint_url.is_some() && std::env::var_os("AWS_S3_FORCE_PATH_STYLE").is_none() {
            unsafe { std::env::set_var("AWS_S3_FORCE_PATH_STYLE", "true") };
        }
    }

    /// Log configuration status (without secrets).
    pub fn log_status(&self) {
        if self.is_empty() {
            tracing::debug!("No S3 configuration found in environment");
        } else {
            tracing::info!(
                has_access_key = self.access_key_id.is_some(),
                has_secret_key = self.secret_access_key.is_some(),
                has_session_token = self.session_token.is_some(),
                region = ?self.region,
                endpoint = ?self.endpoint_url,
                "S3 configuration loaded from environment"
            );
        }
    }
}

/// Initialize S3 environment bridge once.
///
/// This should be called once at service startup before any robocodec operations.
/// It reads RF_S3_* and AWS_* env vars, validates them, and applies to AWS-standard names.
///
/// # Errors
///
/// Returns error if credentials are inconsistent (only key or only secret provided).
pub fn init_s3_env_bridge() -> Result<(), String> {
    static INIT: OnceLock<Result<(), String>> = OnceLock::new();

    INIT.get_or_init(|| {
        // 1. Read from outer env
        let mut config = S3BridgeConfig::from_outer_env();

        // 2. If RF_S3_* not set, try loading from roboflow config file
        if let (true, Ok(Some(cfg))) = (
            config.is_empty(),
            roboflow_storage::RoboflowConfig::load_default(),
        ) {
            config = S3BridgeConfig::from_roboflow_config(&cfg);
            tracing::debug!("Loaded S3 configuration from roboflow config file");
        }

        // 3. Validate
        config.validate()?;

        // 4. Log status (without secrets)
        config.log_status();

        // 5. Apply to AWS env vars
        config.apply_to_aws_env_if_missing();

        tracing::info!("S3 environment bridge initialized");
        Ok(())
    })
    .clone()
}

/// Legacy helper: Apply S3 env configuration if the URL is cloud-based.
///
/// This is a convenience wrapper that calls `init_s3_env_bridge()` lazily.
/// For explicit control, call `init_s3_env_bridge()` once at startup instead.
pub fn maybe_apply_s3_env_for_url(url: &str) {
    if !is_cloud_url(url) {
        return;
    }

    if let Err(e) = init_s3_env_bridge() {
        tracing::warn!(error = %e, "S3 env bridge initialization failed");
    }
}

fn is_cloud_url(url: &str) -> bool {
    url.starts_with("s3://") || url.starts_with("oss://")
}

/// Re-export S3BridgeConfig as S3EnvConfig for backward compatibility.
pub type S3EnvConfig = S3BridgeConfig;

/// Re-export init function for backward compatibility.
pub use init_s3_env_bridge as apply_s3_env_from_config_file;

/// Legacy apply function (deprecated, use init_s3_env_bridge).
#[deprecated(
    since = "0.2.0",
    note = "Use S3BridgeConfig::apply_to_aws_env_if_missing()"
)]
pub fn apply_s3_env(cfg: &S3BridgeConfig) {
    cfg.apply_to_aws_env_if_missing();
}
