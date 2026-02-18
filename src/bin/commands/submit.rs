// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Submit command for creating jobs in the distributed queue.
//!
//! ## Usage
//!
//! ```bash
//! # Submit a single file
//! roboflow submit oss://bucket/path/to/file.mcap --output oss://bucket/output/
//!
//! # Submit multiple files (glob pattern)
//! roboflow submit "oss://bucket/raw/*.mcap" --output oss://bucket/output/
//!
//! # Submit from manifest file
//! roboflow submit --manifest jobs.json
//! ```
//!
//! **Note:** Jobs are internally implemented as single-file batch jobs.
//! This provides a unified architecture where all work goes through the batch system.

use roboflow_distributed::TikvClient;
use roboflow_distributed::batch::{
    BatchController, BatchJobSpec, BatchMetadata, BatchPhase, BatchSpec, BatchSummary, SourceUrl,
    WorkUnitConfig,
};
use roboflow_distributed::tikv::schema::ConfigRecord;
use std::path::{Path, PathBuf};

/// Output format for submit command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Table,
    Json,
    Csv,
}

/// Maximum config file size (10MB) to prevent DoS.
const MAX_CONFIG_SIZE: usize = 10 * 1024 * 1024;

/// Maximum TOML nesting depth to prevent TOML bomb attacks.
const MAX_TOML_NESTING_DEPTH: usize = 32;

/// Maximum number of keys in TOML config.
const MAX_TOML_KEYS: usize = 1000;

/// Maximum array size in TOML config.
const MAX_TOML_ARRAY_SIZE: usize = 10_000;

/// Submit command options.
#[derive(Debug, Clone)]
pub struct SubmitCommand {
    /// Input URL(s) to process.
    pub inputs: Vec<String>,

    /// Output location for processed data.
    pub output: Option<String>,

    /// Path to manifest file.
    pub manifest: Option<String>,

    /// Dataset configuration hash.
    pub config_hash: Option<String>,

    /// Maximum attempts per job.
    pub max_attempts: Option<u32>,

    /// Dry run - show what would be submitted without actually submitting.
    pub dry_run: bool,

    /// Output format.
    pub output_format: OutputFormat,

    /// TiKV PD endpoints.
    pub tikv_endpoints: Option<String>,

    /// Verbose output.
    pub verbose: bool,

    /// Episodes per chunk for LeRobot v2.1 format.
    pub episodes_per_chunk: u32,
}

impl SubmitCommand {
    /// Parse submit command from CLI arguments.
    pub fn parse(args: &[String]) -> Result<Option<Self>, String> {
        let mut inputs = Vec::new();
        let mut output = None;
        let mut manifest = None;
        let mut config_hash = None;
        let mut max_attempts = None;
        let mut dry_run = false;
        let mut output_format = OutputFormat::Table;
        let mut tikv_endpoints = None;
        let mut verbose = false;
        let mut episodes_per_chunk = 500u32; // Default LeRobot v2.1 spec

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--output" | "-o" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("--output requires a value".to_string());
                    }
                    output = Some(args[i].clone());
                }
                "--manifest" | "-m" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("--manifest requires a value".to_string());
                    }
                    manifest = Some(args[i].clone());
                }
                "--config" | "-c" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("--config requires a value".to_string());
                    }
                    config_hash = Some(args[i].clone());
                }
                "--max-attempts" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("--max-attempts requires a value".to_string());
                    }
                    max_attempts = args[i].parse().ok();
                    if max_attempts.is_none() {
                        return Err(format!("invalid max-attempts: {}", args[i]));
                    }
                }
                "--dry-run" => {
                    dry_run = true;
                }
                "--json" => {
                    output_format = OutputFormat::Json;
                }
                "--csv" => {
                    output_format = OutputFormat::Csv;
                }
                "--tikv-endpoints" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("--tikv-endpoints requires a value".to_string());
                    }
                    tikv_endpoints = Some(args[i].clone());
                }
                "--verbose" | "-v" => {
                    verbose = true;
                }
                "--episodes-per-chunk" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("--episodes-per-chunk requires a value".to_string());
                    }
                    let val: u32 = args[i]
                        .parse()
                        .map_err(|_| format!("invalid episodes-per-chunk: {}", args[i]))?;
                    if val == 0 {
                        return Err("--episodes-per-chunk must be greater than 0".to_string());
                    }
                    episodes_per_chunk = val;
                }
                "--help" | "-h" => {
                    print_submit_help();
                    return Ok(None);
                }
                arg if arg.starts_with('-') => {
                    return Err(format!("unknown flag: {}", arg));
                }
                arg => {
                    inputs.push(arg.to_string());
                }
            }
            i += 1;
        }

        if manifest.is_none() && inputs.is_empty() {
            return Err("submit requires either INPUT URLs or --manifest".to_string());
        }

        Ok(Some(Self {
            inputs,
            output,
            manifest,
            config_hash,
            max_attempts,
            dry_run,
            output_format,
            tikv_endpoints,
            verbose,
            episodes_per_chunk,
        }))
    }

    /// Run the submit command.
    pub async fn run(&self) -> Result<(), String> {
        // Load manifest if provided
        if let Some(manifest_path) = &self.manifest {
            return self.submit_from_manifest(manifest_path).await;
        }

        // Validate inputs
        if self.inputs.is_empty() {
            return Err("No input URLs provided".to_string());
        }

        // Validate output
        let output = self
            .output
            .as_ref()
            .cloned()
            .or_else(|| std::env::var("ROBOFLOW_OUTPUT_PREFIX").ok())
            .unwrap_or_else(|| "output/".to_string());

        // Initialize TiKV client
        let tikv = if let Some(endpoints) = &self.tikv_endpoints {
            TikvClient::new(roboflow_distributed::TikvConfig::with_pd_endpoints(
                endpoints,
            ))
            .await
            .map_err(|e| format!("Failed to connect to TiKV: {}", e))?
        } else {
            TikvClient::from_env()
                .await
                .map_err(|e| format!("Failed to connect to TiKV: {}", e))?
        };

        // Load or store config in TiKV
        let config_hash = self.load_or_store_config(&tikv).await?;

        // Wrap in Arc for BatchController
        let tikv_arc = std::sync::Arc::new(tikv.clone());

        // Create batch controller
        let controller = BatchController::with_client(tikv_arc);

        // Track submitted batches
        let mut submitted_batches: Vec<BatchSummary> = Vec::new();
        let mut error_count = 0;

        // Process each input
        for input in &self.inputs {
            match self
                .submit_batch(input, &output, &config_hash, &controller)
                .await
            {
                Ok(batch) => {
                    submitted_batches.push(batch);
                }
                Err(e) => {
                    eprintln!("Error processing '{}': {}", input, e);
                    error_count += 1;
                }
            }
        }

        // Print results
        if self.dry_run {
            println!(
                "Dry run complete. Would submit {} batch jobs:",
                submitted_batches.len()
            );
        } else {
            println!(
                "Submitted {} batch jobs, errors {}",
                submitted_batches.len(),
                error_count
            );
        }

        if !submitted_batches.is_empty() {
            print_batch_summaries(&submitted_batches, self.output_format);
        }

        if error_count > 0 {
            return Err(format!("{} batch jobs failed to submit", error_count));
        }

        Ok(())
    }

    /// Submit a single file as a batch job.
    ///
    /// Jobs are internally implemented as single-file batch jobs.
    async fn submit_batch(
        &self,
        source: &str,
        output: &str,
        config_hash: &str,
        controller: &BatchController,
    ) -> Result<BatchSummary, String> {
        // Get submitter identity
        let submitter = std::env::var("ROBOFLOW_USER")
            .or_else(|_| std::env::var("USER"))
            .or_else(|_| std::env::var("USERNAME"))
            .unwrap_or_else(|_| {
                tracing::warn!(
                    "No user identity env vars (ROBOFLOW_USER, USER, USERNAME) set, using 'unknown'"
                );
                "unknown".to_string()
            });

        // Create batch metadata
        let display_name = self.generate_display_name(source);
        let batch_id = self.generate_batch_id();
        let metadata = BatchMetadata {
            name: batch_id.clone(),
            display_name: Some(display_name.clone()),
            namespace: "jobs".to_string(),
            submitted_by: Some(submitter),
            labels: std::collections::HashMap::new(),
            annotations: std::collections::HashMap::new(),
            created_at: chrono::Utc::now(),
        };

        // Create source URL
        let source_url = SourceUrl {
            url: source.to_string(),
            filter: None,
            max_size: None,
        };

        // Create batch spec
        let spec = BatchJobSpec {
            sources: vec![source_url],
            output: output.to_string(),
            config: config_hash.to_string(),
            parallelism: 1,
            completions: None,
            max_retries: self.max_attempts.unwrap_or(3),
            backoff_limit: 1,
            ttl_seconds: 86400, // 24 hours
            priority: 0,
            work_unit_config: WorkUnitConfig::default(),
            episodes_per_chunk: self.episodes_per_chunk,
        };

        let batch_spec = BatchSpec {
            api_version: "v1".to_string(),
            kind: "BatchJob".to_string(),
            metadata,
            spec,
        };

        if self.dry_run {
            if self.verbose {
                println!("Would submit batch: {}", batch_id);
            }
            // Return a mock summary for dry run
            return Ok(BatchSummary {
                id: format!("jobs:{}", batch_id),
                name: batch_spec
                    .metadata
                    .display_name
                    .clone()
                    .unwrap_or(batch_spec.metadata.name.clone()),
                namespace: batch_spec.metadata.namespace,
                phase: BatchPhase::Pending,
                files_total: 1,
                files_completed: 0,
                files_failed: 0,
                created_at: batch_spec.metadata.created_at,
                started_at: None,
                completed_at: None,
            });
        }

        // Submit the batch
        let returned_batch_id = controller
            .submit_batch(&batch_spec)
            .await
            .map_err(|e| format!("Failed to submit batch: {}", e))?;

        // Get the spec and status to create summary
        let spec = controller
            .get_batch_spec(&returned_batch_id)
            .await
            .map_err(|e| format!("Failed to get batch spec: {}", e))?
            .ok_or_else(|| "Batch spec not found after submission".to_string())?;

        let status = controller
            .get_batch_status(&returned_batch_id)
            .await
            .map_err(|e| format!("Failed to get batch status: {}", e))?
            .ok_or_else(|| "Batch status not found after submission".to_string())?;

        if self.verbose {
            println!("Submitted batch: {} ({})", display_name, returned_batch_id);
        }

        Ok(BatchSummary {
            id: returned_batch_id,
            name: spec
                .metadata
                .display_name
                .clone()
                .unwrap_or(spec.metadata.name.clone()),
            namespace: spec.metadata.namespace,
            phase: status.phase,
            files_total: status.files_total,
            files_completed: status.files_completed,
            files_failed: status.files_failed,
            created_at: spec.metadata.created_at,
            started_at: status.started_at,
            completed_at: status.completed_at,
        })
    }

    /// Generate a batch display name (just filename, no extension).
    fn generate_display_name(&self, source: &str) -> String {
        // Extract filename from URL
        let filename = if let Some(last_slash) = source.rfind('/') {
            &source[last_slash + 1..]
        } else {
            source
        };

        // Remove extension and sanitize
        filename
            .trim_end_matches(".mcap")
            .trim_end_matches(".bag")
            .chars()
            .map(|c| {
                if c.is_alphanumeric() {
                    c.to_ascii_lowercase()
                } else {
                    '-'
                }
            })
            .collect::<String>()
    }

    /// Generate a unique batch ID (8-character UUID).
    fn generate_batch_id(&self) -> String {
        uuid::Uuid::new_v4().to_string()[..8].to_string()
    }

    /// Submit jobs from a manifest file.
    async fn submit_from_manifest(&self, manifest_path: &str) -> Result<(), String> {
        // Read manifest file
        let manifest_content = std::fs::read_to_string(manifest_path)
            .map_err(|e| format!("Failed to read manifest '{}': {}", manifest_path, e))?;

        // Parse manifest JSON
        #[derive(serde::Deserialize)]
        struct Manifest {
            jobs: Vec<ManifestJob>,
        }

        #[derive(serde::Deserialize)]
        struct ManifestJob {
            source: String,
            output: Option<String>,
            config_hash: Option<String>,
            max_attempts: Option<u32>,
        }

        let manifest: Manifest = serde_json::from_str(&manifest_content)
            .map_err(|e| format!("Failed to parse manifest JSON: {}", e))?;

        if manifest.jobs.is_empty() {
            println!("Manifest contains no jobs");
            return Ok(());
        }

        // Initialize TiKV client and batch controller
        let tikv = if let Some(endpoints) = &self.tikv_endpoints {
            TikvClient::new(roboflow_distributed::TikvConfig::with_pd_endpoints(
                endpoints,
            ))
            .await
            .map_err(|e| format!("Failed to connect to TiKV: {}", e))?
        } else {
            TikvClient::from_env()
                .await
                .map_err(|e| format!("Failed to connect to TiKV: {}", e))?
        };

        // Wrap in Arc for BatchController
        let tikv_arc = std::sync::Arc::new(tikv.clone());
        let controller = BatchController::with_client(tikv_arc);

        // Get default output from command line or env
        let default_output = self
            .output
            .as_ref()
            .cloned()
            .or_else(|| std::env::var("ROBOFLOW_OUTPUT_PREFIX").ok())
            .unwrap_or_else(|| "output/".to_string());

        // Load or store config
        let config_hash = self.load_or_store_config(&tikv).await?;

        // Track submitted batches
        let mut submitted_batches = Vec::new();

        // Handle dry run mode for manifest
        if self.dry_run {
            for job_spec in &manifest.jobs {
                let display_name = self.generate_display_name(&job_spec.source);
                let batch_id = self.generate_batch_id();

                submitted_batches.push(BatchSummary {
                    id: format!("jobs:{}", batch_id),
                    name: display_name,
                    namespace: "jobs".to_string(),
                    phase: BatchPhase::Pending,
                    files_total: 1,
                    files_completed: 0,
                    files_failed: 0,
                    created_at: chrono::Utc::now(),
                    started_at: None,
                    completed_at: None,
                });
            }

            println!(
                "Dry run complete. Would submit {} batch jobs from manifest:",
                submitted_batches.len()
            );

            if !submitted_batches.is_empty() {
                print_batch_summaries(&submitted_batches, self.output_format);
            }

            return Ok(());
        }

        // Track errors for actual submission
        let mut error_count = 0;

        // Process each job in manifest
        for job_spec in &manifest.jobs {
            let output = job_spec
                .output
                .as_ref()
                .cloned()
                .unwrap_or_else(|| default_output.clone());

            let config = job_spec
                .config_hash
                .as_ref()
                .cloned()
                .unwrap_or_else(|| config_hash.clone());

            // For manifest jobs, create a batch with job-specific max_attempts
            // We'll create the batch spec directly here instead of using submit_batch
            let display_name = self.generate_display_name(&job_spec.source);
            let batch_id = self.generate_batch_id();

            let submitter = std::env::var("ROBOFLOW_USER")
                .or_else(|_| std::env::var("USER"))
                .or_else(|_| std::env::var("USERNAME"))
                .unwrap_or_else(|_| {
                    tracing::warn!("No user identity env vars (ROBOFLOW_USER, USER, USERNAME) set, using 'unknown'");
                    "unknown".to_string()
                });

            let metadata = BatchMetadata {
                name: batch_id.clone(),
                display_name: Some(display_name.clone()),
                namespace: "jobs".to_string(),
                submitted_by: Some(submitter),
                labels: std::collections::HashMap::new(),
                annotations: std::collections::HashMap::new(),
                created_at: chrono::Utc::now(),
            };

            let source_url = SourceUrl {
                url: job_spec.source.clone(),
                filter: None,
                max_size: None,
            };

            let max_attempts_override = job_spec.max_attempts.or(self.max_attempts).unwrap_or(3);

            let batch_spec_inner = BatchJobSpec {
                sources: vec![source_url],
                output: output.clone(),
                config: config.clone(),
                parallelism: 1,
                completions: None,
                max_retries: max_attempts_override,
                backoff_limit: 1,
                ttl_seconds: 86400,
                priority: 0,
                work_unit_config: WorkUnitConfig::default(),
                episodes_per_chunk: self.episodes_per_chunk,
            };

            let batch_spec = BatchSpec {
                api_version: "v1".to_string(),
                kind: "BatchJob".to_string(),
                metadata,
                spec: batch_spec_inner,
            };

            match controller.submit_batch(&batch_spec).await {
                Ok(batch_id) => {
                    // Get the spec and status to create summary
                    match (
                        controller.get_batch_spec(&batch_id).await,
                        controller.get_batch_status(&batch_id).await,
                    ) {
                        (Ok(Some(spec)), Ok(Some(status))) => {
                            submitted_batches.push(BatchSummary {
                                id: batch_id.clone(),
                                name: spec
                                    .metadata
                                    .display_name
                                    .clone()
                                    .unwrap_or(spec.metadata.name.clone()),
                                namespace: spec.metadata.namespace,
                                phase: status.phase,
                                files_total: status.files_total,
                                files_completed: status.files_completed,
                                files_failed: status.files_failed,
                                created_at: spec.metadata.created_at,
                                started_at: status.started_at,
                                completed_at: status.completed_at,
                            });
                        }
                        (spec_result, status_result) => {
                            // Log why we're using fallback
                            let spec_ok = spec_result.is_ok()
                                && spec_result.as_ref().map(|s| s.is_some()).unwrap_or(false);
                            let status_ok = status_result.is_ok()
                                && status_result.as_ref().map(|s| s.is_some()).unwrap_or(false);

                            let spec_err = if !spec_ok {
                                spec_result
                                    .err()
                                    .map(|e| format!("{:?}", e))
                                    .or_else(|| Some("not found".to_string()))
                            } else {
                                None
                            };
                            let status_err = if !status_ok {
                                status_result
                                    .err()
                                    .map(|e| format!("{:?}", e))
                                    .or_else(|| Some("not found".to_string()))
                            } else {
                                None
                            };

                            eprintln!(
                                "Warning: Could not retrieve spec/status for batch {}: spec={}, status={}. Using fallback summary.",
                                batch_id,
                                spec_err.unwrap_or_else(|| "ok".to_string()),
                                status_err.unwrap_or_else(|| "ok".to_string())
                            );

                            // Fallback: create minimal summary
                            submitted_batches.push(BatchSummary {
                                id: batch_id.clone(),
                                name: display_name.clone(),
                                namespace: "jobs".to_string(),
                                phase: BatchPhase::Pending,
                                files_total: 1,
                                files_completed: 0,
                                files_failed: 0,
                                created_at: chrono::Utc::now(),
                                started_at: None,
                                completed_at: None,
                            });
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error processing '{}': {}", job_spec.source, e);
                    error_count += 1;
                }
            }
        }

        // Print results
        println!(
            "Submitted {}/{} batches from manifest",
            submitted_batches.len(),
            manifest.jobs.len()
        );

        if !submitted_batches.is_empty() {
            print_batch_summaries(&submitted_batches, self.output_format);
        }

        if error_count > 0 {
            return Err(format!("{} batches failed to submit", error_count));
        }

        Ok(())
    }

    /// Load or store configuration in TiKV.
    ///
    /// If `config_hash` is a file path that exists, reads the file, computes SHA-256 hash,
    /// stores in TiKV (if not already present), and returns the hash.
    ///
    /// If `config_hash` is already a 64-character hex string (SHA-256 hash), verifies
    /// the config exists in TiKV before accepting it.
    ///
    /// Otherwise, treats it as-is (for backward compatibility with "default" hash).
    async fn load_or_store_config(&self, tikv: &TikvClient) -> Result<String, String> {
        let config_input = self
            .config_hash
            .as_ref()
            .cloned()
            .unwrap_or_else(|| "default".to_string());

        // Special case for "default" hash
        if config_input == "default" {
            tracing::debug!("Using default config hash");
            return Ok(config_input);
        }

        // Check if it's already a 64-char hex string (SHA-256 hash)
        if is_hex_hash(&config_input) {
            // CRITICAL: Verify the hash actually exists in TiKV before accepting it
            match tikv.get_config(&config_input).await {
                Ok(Some(_)) => {
                    tracing::debug!("Using existing config hash: {}", config_input);
                    return Ok(config_input);
                }
                Ok(None) => {
                    return Err(format!(
                        "Config hash '{}' not found in TiKV. Provide a valid file path or ensure config is stored.",
                        config_input
                    ));
                }
                Err(e) => {
                    return Err(format!("Failed to verify config in TiKV: {}", e));
                }
            }
        }

        // Check if it's a file path that exists
        let config_path = validate_config_path(&config_input)?;
        if config_path.exists() {
            let filename = config_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("config");

            tracing::info!("Reading config from file: {}", filename);
            let content = read_config_with_limit(&config_path)?;

            // Validate TOML structure to prevent TOML bomb attacks
            SafeTomlValidator::validate_toml_str(&content)
                .map_err(|e| format!("TOML validation failed in '{}': {}", filename, e))?;

            // Validate as actual LeRobotConfig
            if let Err(e) = roboflow_pipeline::formats::lerobot::LerobotConfig::from_toml(&content)
            {
                return Err(format!("Invalid LeRobot config in '{}': {}", filename, e));
            }

            // Compute hash
            let hash = ConfigRecord::compute_hash(&content);

            // Check if already exists in TiKV (race condition check)
            match tikv.get_config(&hash).await {
                Ok(Some(_)) => {
                    tracing::info!("Config already exists in TiKV: {}", hash);
                    return Ok(hash);
                }
                Ok(None) => {}
                Err(e) => {
                    return Err(format!("Failed to check config in TiKV: {}", e));
                }
            }

            // Store in TiKV (put_config is idempotent for same hash)
            let record = ConfigRecord::new(content);
            tikv.put_config(&record)
                .await
                .map_err(|e| format!("Failed to store config in TiKV: {}", e))?;
            tracing::info!("Stored config in TiKV: {}", hash);
            return Ok(hash);
        }

        // Not a hash, not a valid file - error
        Err(format!(
            "Config '{}' is not a valid hash (64 hex chars), existing .toml file, or 'default'",
            config_input
        ))
    }
}

/// Validate config file path for security.
///
/// Rejects:
/// - Absolute paths
/// - Path traversal sequences (..)
/// - Non-.toml files
fn validate_config_path(config_input: &str) -> Result<PathBuf, String> {
    let path = PathBuf::from(config_input);

    // Reject absolute paths for security
    if path.is_absolute() {
        return Err("Absolute paths are not allowed for config files".to_string());
    }

    // Reject path traversal
    if config_input.contains("..") {
        return Err("Path traversal sequences (..) are not allowed".to_string());
    }

    // Only allow .toml extension
    match path.extension().and_then(|e| e.to_str()) {
        Some("toml") => {}
        _ => {
            return Err("Config files must have .toml extension".to_string());
        }
    }

    Ok(path)
}

/// Read config file with size limit to prevent DoS.
fn read_config_with_limit(path: &Path) -> Result<String, String> {
    // Check file size first
    let metadata =
        std::fs::metadata(path).map_err(|e| format!("Cannot read file metadata: {}", e))?;

    if metadata.len() > MAX_CONFIG_SIZE as u64 {
        return Err(format!(
            "Config file too large: {} bytes (max: {} bytes)",
            metadata.len(),
            MAX_CONFIG_SIZE
        ));
    }

    let content =
        std::fs::read_to_string(path).map_err(|e| format!("Failed to read config file: {}", e))?;

    // Double-check content length
    if content.len() > MAX_CONFIG_SIZE {
        return Err("Config content exceeds maximum size".to_string());
    }

    Ok(content)
}

/// TOML validator to prevent TOML bomb attacks.
struct SafeTomlValidator {
    depth: usize,
    key_count: usize,
}

impl SafeTomlValidator {
    fn new() -> Self {
        Self {
            depth: 0,
            key_count: 0,
        }
    }

    /// Validate a TOML value for size and nesting limits.
    fn validate(&mut self, value: &toml::Value) -> Result<(), String> {
        match value {
            toml::Value::Table(table) => {
                if self.depth > MAX_TOML_NESTING_DEPTH {
                    return Err(format!(
                        "TOML nesting exceeds maximum depth of {}",
                        MAX_TOML_NESTING_DEPTH
                    ));
                }
                self.key_count += table.len();
                if self.key_count > MAX_TOML_KEYS {
                    return Err(format!(
                        "TOML key count exceeds maximum of {}",
                        MAX_TOML_KEYS
                    ));
                }
                self.depth += 1;
                for v in table.values() {
                    self.validate(v)?;
                }
                self.depth -= 1;
            }
            toml::Value::Array(arr) => {
                if arr.len() > MAX_TOML_ARRAY_SIZE {
                    return Err(format!(
                        "TOML array too large (max {} elements)",
                        MAX_TOML_ARRAY_SIZE
                    ));
                }
                for v in arr {
                    self.validate(v)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Validate TOML string directly.
    fn validate_toml_str(content: &str) -> Result<(), String> {
        let parsed: toml::Value =
            toml::from_str(content).map_err(|e| format!("Invalid TOML: {}", e))?;
        let mut validator = Self::new();
        validator.validate(&parsed)
    }
}

/// Check if a string is a 64-character hex string (SHA-256 hash).
fn is_hex_hash(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Print batch summaries in the specified format.
fn print_batch_summaries(batches: &[BatchSummary], format: OutputFormat) {
    match format {
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(batches).unwrap_or_else(|_| "[]".to_string());
            println!("{}", json);
        }
        OutputFormat::Csv => {
            println!("id,name,namespace,phase,files_total,files_completed,files_failed,created_at");
            for batch in batches {
                println!(
                    "{},{},{},{},{},{},{},{}",
                    batch.id,
                    batch.name,
                    batch.namespace,
                    format_phase(batch.phase),
                    batch.files_total,
                    batch.files_completed,
                    batch.files_failed,
                    batch.created_at.format("%Y-%m-%d %H:%M:%S")
                );
            }
        }
        OutputFormat::Table => {
            if batches.is_empty() {
                println!("No batches to display");
                return;
            }

            // Calculate column widths
            let id_width = batches
                .iter()
                .map(|b| b.id.len())
                .max()
                .unwrap_or(8)
                .min(20);
            let name_width = batches
                .iter()
                .map(|b| b.name.len())
                .max()
                .unwrap_or(4)
                .min(30);
            let phase_width = 12;

            // Print header
            println!(
                "{:<id_width$} {:<name_width$} {:<phase_width$} {:>10} {:>10} {:>10}",
                "ID",
                "NAME",
                "PHASE",
                "TOTAL",
                "DONE",
                "FAILED",
                id_width = id_width,
                name_width = name_width,
                phase_width = phase_width
            );
            println!(
                "{:-<id_width$} {:-<name_width$} {:-<phase_width$} {:>10} {:>10} {:>10}",
                "",
                "",
                "",
                "",
                "",
                "",
                id_width = id_width,
                name_width = name_width,
                phase_width = phase_width
            );

            // Print rows
            for batch in batches {
                println!(
                    "{:<id_width$} {:<name_width$} {:<phase_width$} {:>10} {:>10} {:>10}",
                    truncate(&batch.id, id_width),
                    truncate(&batch.name, name_width),
                    format_phase(batch.phase),
                    batch.files_total,
                    batch.files_completed,
                    batch.files_failed,
                    id_width = id_width,
                    name_width = name_width,
                    phase_width = phase_width
                );
            }
        }
    }
}

/// Format batch phase for display.
fn format_phase(phase: BatchPhase) -> String {
    match phase {
        BatchPhase::Pending => "Pending".to_string(),
        BatchPhase::Discovering => "Discovering".to_string(),
        BatchPhase::Running => "Running".to_string(),
        BatchPhase::Merging => "Merging".to_string(),
        BatchPhase::Complete => "Complete".to_string(),
        BatchPhase::Failed => "Failed".to_string(),
        BatchPhase::Cancelled => "Cancelled".to_string(),
        BatchPhase::Suspending => "Suspending".to_string(),
        BatchPhase::Suspended => "Suspended".to_string(),
    }
}

/// Truncate string to fit width.
fn truncate(s: &str, width: usize) -> String {
    if s.len() <= width {
        s.to_string()
    } else {
        format!("{}...", &s[..width - 3])
    }
}

/// Run the submit command from raw args.
pub async fn run_submit_command(args: &[String]) -> Result<(), String> {
    let cmd = SubmitCommand::parse(args)?;
    match cmd {
        Some(command) => command.run().await,
        None => Ok(()), // Help was printed
    }
}

/// Print submit command help.
fn print_submit_help() {
    println!(
        r#"Submit jobs to the distributed processing queue.

USAGE:
    roboflow submit [OPTIONS] [INPUT]...

ARGUMENTS:
    INPUT...                Input files or glob patterns to process
                            (e.g., oss://bucket/path/to/file.mcap)

OPTIONS:
    -o, --output <PREFIX>   Output location for processed data
                            (default: $ROBOFLOW_OUTPUT_PREFIX or "output/")
    -m, --manifest <PATH>   Load jobs from a JSON manifest file
    -c, --config <PATH>     Dataset configuration file path or hash
                            If a file path: reads file, stores in TiKV, uses hash
                            If a 64-char hex string: uses as hash directly
                            (default: "default")
        --max-attempts <N>  Maximum retry attempts per job (default: 3)
        --episodes-per-chunk <N>
                            Episodes per chunk for LeRobot v2.1 format
                            Controls chunk directory structure: chunk_index = episode_index / N
                            (default: 500)
        --dry-run           Show what would be submitted without submitting
        --json              Output in JSON format
        --csv               Output in CSV format
        --tikv-endpoints <ADDRS>
                            TiKV PD endpoints (default: $TIKV_PD_ENDPOINTS)
    -v, --verbose           Show detailed progress
    -h, --help              Print this help

MANIFEST FORMAT:
    The manifest file should be a JSON file with the following structure:

    {{
      "jobs": [
        {{
          "source": "oss://bucket/input1.mcap",
          "output": "oss://bucket/output/",
          "config_hash": "default",
          "max_attempts": 3
        }},
        {{
          "source": "oss://bucket/input2.mcap"
        }}
      ]
    }}

    All fields except "source" are optional and will use defaults
    or command-line values if not specified.

EXAMPLES:
    # Submit a single file
    roboflow submit oss://bucket/file.mcap --output oss://bucket/output/

    # Submit multiple files using glob pattern
    roboflow submit "oss://bucket/raw/*.mcap" --output oss://bucket/processed/

    # Submit from manifest file
    roboflow submit --manifest jobs.json

    # Dry run to see what would be submitted
    roboflow submit oss://bucket/*.mcap --dry-run

    # Submit with config file (will be stored in TiKV)
    roboflow submit file.mcap --config /path/to/config.toml

    # Submit with existing config hash (already in TiKV)
    roboflow submit file.mcap --config a3f5b...

ENVIRONMENT VARIABLES:
    ROBOFLOW_OUTPUT_PREFIX    Default output location
    TIKV_PD_ENDPOINTS         TiKV PD endpoints (default: 127.0.0.1:2379)
"#
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_hex_hash_valid() {
        // Valid SHA-256 hash (64 hex characters)
        assert!(is_hex_hash(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        ));
        assert!(is_hex_hash(
            "0000000000000000000000000000000000000000000000000000000000000000"
        ));
        assert!(is_hex_hash(
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        ));
    }

    #[test]
    fn test_is_hex_hash_invalid_characters() {
        // Contains non-hex characters
        assert!(!is_hex_hash(
            "g123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        )); // 'g' is not hex
        assert!(!is_hex_hash(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde"
        )); // contains 'z'
        assert!(!is_hex_hash(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdG"
        )); // 'G' is not hex
    }

    #[test]
    fn test_is_hex_hash_wrong_length() {
        // Too short (63 chars)
        assert!(!is_hex_hash(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde"
        ));
        // Too long (65 chars)
        assert!(!is_hex_hash(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0"
        ));
        // Empty
        assert!(!is_hex_hash(""));
        // Half length (32 chars)
        assert!(!is_hex_hash("0123456789abcdef0123456789abcdef"));
    }

    #[test]
    fn test_validate_config_path_valid() {
        // Valid relative paths with .toml extension
        assert!(validate_config_path("config.toml").is_ok());
        assert!(validate_config_path("path/to/config.toml").is_ok());
        assert!(validate_config_path("./config.toml").is_ok());
        assert!(validate_config_path("a/b/c/config.toml").is_ok());
    }

    #[test]
    fn test_validate_config_path_absolute_rejected() {
        // Absolute paths should be rejected
        assert!(validate_config_path("/etc/config.toml").is_err());
        assert!(validate_config_path("/home/user/config.toml").is_err());
        // Note: Windows paths like C:\ are relative paths on Unix, so skip that test
    }

    #[test]
    fn test_validate_config_path_traversal_rejected() {
        // Path traversal should be rejected
        assert!(validate_config_path("../config.toml").is_err());
        assert!(validate_config_path("path/../../config.toml").is_err());
        assert!(validate_config_path("./../config.toml").is_err());
        assert!(validate_config_path("path/../config.toml").is_err());
    }

    #[test]
    fn test_validate_config_path_wrong_extension() {
        // Non-.toml files should be rejected
        assert!(validate_config_path("config.txt").is_err());
        assert!(validate_config_path("config.json").is_err());
        assert!(validate_config_path("config").is_err());
        assert!(validate_config_path("config.toml.bak").is_err());
    }

    #[test]
    fn test_safe_toml_validator_simple() {
        let toml = r#"
[dataset]
name = "test"
fps = 30
"#;
        assert!(SafeTomlValidator::validate_toml_str(toml).is_ok());
    }

    #[test]
    fn test_safe_toml_validator_nesting_limit() {
        // Create deeply nested TOML
        let mut toml = String::from("[a]");
        for _ in 0..MAX_TOML_NESTING_DEPTH + 1 {
            toml = format!("[b.{}]", toml);
        }
        assert!(SafeTomlValidator::validate_toml_str(&toml).is_err());
    }

    #[test]
    fn test_safe_toml_validator_key_limit() {
        // Create TOML with too many keys
        let mut toml = String::from("[dataset]\n");
        for i in 0..=MAX_TOML_KEYS {
            toml.push_str(&format!("key{} = \"value\"\n", i));
        }
        assert!(SafeTomlValidator::validate_toml_str(&toml).is_err());
    }

    #[test]
    fn test_safe_toml_validator_array_limit() {
        // Create TOML with huge array
        let mut toml = String::from("[dataset]\nkeys = [");
        for i in 0..MAX_TOML_ARRAY_SIZE + 1 {
            if i > 0 {
                toml.push(',');
            }
            toml.push_str(&format!("\"{}\"", i));
        }
        toml.push(']');
        assert!(SafeTomlValidator::validate_toml_str(&toml).is_err());
    }

    #[test]
    fn test_safe_toml_validator_valid_lerobot_config() {
        let toml = r#"
[dataset]
name = "test_dataset"
fps = 30

[[mappings]]
topic = "/cam_h/color"
feature = "observation.images.cam_high"
mapping_type = "image"
"#;
        assert!(SafeTomlValidator::validate_toml_str(toml).is_ok());
    }

    // =========================================================================
    // Episodes Per Chunk CLI Tests
    // =========================================================================

    #[test]
    fn test_submit_command_episodes_per_chunk_default() {
        let args = vec![
            "submit".to_string(),
            "s3://bucket/file.mcap".to_string(),
            "--output".to_string(),
            "s3://output/".to_string(),
        ];
        let cmd = SubmitCommand::parse(&args).unwrap().unwrap();
        assert_eq!(cmd.episodes_per_chunk, 500); // Default LeRobot v2.1 spec
    }

    #[test]
    fn test_submit_command_episodes_per_chunk_custom() {
        let args = vec![
            "submit".to_string(),
            "s3://bucket/file.mcap".to_string(),
            "--output".to_string(),
            "s3://output/".to_string(),
            "--episodes-per-chunk".to_string(),
            "250".to_string(),
        ];
        let cmd = SubmitCommand::parse(&args).unwrap().unwrap();
        assert_eq!(cmd.episodes_per_chunk, 250);
    }

    #[test]
    fn test_submit_command_episodes_per_chunk_large() {
        // Test with 1000 episodes per chunk (100 chunks for 100K files)
        let args = vec![
            "submit".to_string(),
            "s3://bucket/file.mcap".to_string(),
            "--output".to_string(),
            "s3://output/".to_string(),
            "--episodes-per-chunk".to_string(),
            "1000".to_string(),
        ];
        let cmd = SubmitCommand::parse(&args).unwrap().unwrap();
        assert_eq!(cmd.episodes_per_chunk, 1000);
    }

    #[test]
    fn test_submit_command_episodes_per_chunk_zero_rejected() {
        let args = vec![
            "submit".to_string(),
            "s3://bucket/file.mcap".to_string(),
            "--output".to_string(),
            "s3://output/".to_string(),
            "--episodes-per-chunk".to_string(),
            "0".to_string(),
        ];
        let result = SubmitCommand::parse(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must be greater than 0"));
    }

    #[test]
    fn test_submit_command_episodes_per_chunk_invalid_rejected() {
        let args = vec![
            "submit".to_string(),
            "s3://bucket/file.mcap".to_string(),
            "--output".to_string(),
            "s3://output/".to_string(),
            "--episodes-per-chunk".to_string(),
            "abc".to_string(),
        ];
        let result = SubmitCommand::parse(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid episodes-per-chunk"));
    }

    #[test]
    fn test_submit_command_episodes_per_chunk_missing_value() {
        let args = vec![
            "submit".to_string(),
            "s3://bucket/file.mcap".to_string(),
            "--output".to_string(),
            "s3://output/".to_string(),
            "--episodes-per-chunk".to_string(),
        ];
        let result = SubmitCommand::parse(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("requires a value"));
    }
}
