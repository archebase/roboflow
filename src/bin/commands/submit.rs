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

use roboflow_distributed::tikv::schema::ConfigRecord;
use roboflow_distributed::{JobRecord, TikvClient};
use roboflow_storage::StorageFactory;

use crate::commands::jobs::{OutputFormat, print_job_output};
use crate::commands::utils::{compute_file_hash, glob_match, parse_storage_url};

use std::path::{Path, PathBuf};

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

        // Create storage factory
        let factory = StorageFactory::from_env();

        // Track submitted jobs
        let mut submitted_jobs: Vec<JobRecord> = Vec::new();
        let mut error_count = 0;

        // Process each input
        for input in &self.inputs {
            match self
                .submit_input(input, &output, &config_hash, &factory, &tikv)
                .await
            {
                Ok(job) => {
                    submitted_jobs.push(job);
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
                "Dry run complete. Would submit {} jobs:",
                submitted_jobs.len()
            );
        } else {
            println!(
                "Submitted {} jobs, errors {}",
                submitted_jobs.len(),
                error_count
            );
        }

        if !submitted_jobs.is_empty() {
            print_job_output(&submitted_jobs, self.output_format);
        }

        if error_count > 0 {
            return Err(format!("{} jobs failed to submit", error_count));
        }

        Ok(())
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

        // Get default output from command line or env
        let default_output = self
            .output
            .as_ref()
            .cloned()
            .or_else(|| std::env::var("ROBOFLOW_OUTPUT_PREFIX").ok())
            .unwrap_or_else(|| "output/".to_string());

        let default_config_hash = self
            .config_hash
            .as_ref()
            .cloned()
            .unwrap_or_else(|| "default".to_string());

        let default_max_attempts = self.max_attempts.unwrap_or(3);

        // Track submitted jobs
        let mut submitted_jobs = Vec::new();
        let mut error_count = 0;

        // Create storage factory
        let factory = StorageFactory::from_env();

        // Process each job in manifest
        for job_spec in &manifest.jobs {
            let output = job_spec
                .output
                .as_ref()
                .cloned()
                .unwrap_or_else(|| default_output.clone());

            let config_hash = job_spec
                .config_hash
                .as_ref()
                .cloned()
                .unwrap_or_else(|| default_config_hash.clone());

            let max_attempts = job_spec.max_attempts.unwrap_or(default_max_attempts);

            match self
                .submit_job_spec(
                    &job_spec.source,
                    &output,
                    &config_hash,
                    max_attempts,
                    &factory,
                    &tikv,
                )
                .await
            {
                Ok(job) => {
                    submitted_jobs.push(job);
                }
                Err(e) => {
                    eprintln!("Error processing '{}': {}", job_spec.source, e);
                    error_count += 1;
                }
            }
        }

        // Print results
        println!(
            "Submitted {}/{} jobs from manifest",
            submitted_jobs.len(),
            manifest.jobs.len()
        );

        if !submitted_jobs.is_empty() {
            print_job_output(&submitted_jobs, self.output_format);
        }

        if error_count > 0 {
            return Err(format!("{} jobs failed to submit", error_count));
        }

        Ok(())
    }

    /// Submit a single input (file or glob pattern).
    async fn submit_input(
        &self,
        input: &str,
        output: &str,
        config_hash: &str,
        factory: &StorageFactory,
        tikv: &TikvClient,
    ) -> Result<JobRecord, String> {
        let max_attempts = self.max_attempts.unwrap_or(3);

        // Check if input contains a glob pattern
        if input.contains('*') || input.contains('?') || input.contains('[') {
            // Expand glob pattern
            let storage_url = input
                .split('/')
                .take(3) // scheme://bucket/
                .collect::<Vec<_>>()
                .join("/");
            let pattern = input.split('/').skip(3).collect::<Vec<_>>().join("/");

            let storage = factory
                .create(&storage_url)
                .map_err(|e| format!("Failed to create storage: {}", e))?;

            // List files matching pattern
            let prefix = pattern.split('*').next().unwrap_or("");
            use std::path::Path;
            let files = storage
                .list(Path::new(prefix))
                .map_err(|e| format!("Failed to list files: {}", e))?;

            let mut matched_files = Vec::new();
            for meta in files {
                if glob_match(&pattern, &meta.path) {
                    matched_files.push(meta.path);
                }
            }

            if matched_files.is_empty() {
                return Err(format!("No files found matching pattern: {}", pattern));
            }

            if self.verbose {
                println!(
                    "Found {} files matching pattern: {}",
                    matched_files.len(),
                    input
                );
            }

            // Submit each matched file and track the first job
            let mut first_job = None;
            for file_path in &matched_files {
                let full_url = format!("{}/{}", storage_url.trim_end_matches('/'), file_path);
                let job = self
                    .submit_job_spec(&full_url, output, config_hash, max_attempts, factory, tikv)
                    .await?;
                if first_job.is_none() {
                    first_job = Some(job);
                }
            }

            // Return the first job as representative
            first_job.ok_or_else(|| "No files matched pattern".to_string())
        } else {
            // Single file
            self.submit_job_spec(input, output, config_hash, max_attempts, factory, tikv)
                .await
        }
    }

    /// Submit a job specification to TiKV.
    async fn submit_job_spec(
        &self,
        source: &str,
        output: &str,
        config_hash: &str,
        max_attempts: u32,
        factory: &StorageFactory,
        tikv: &TikvClient,
    ) -> Result<JobRecord, String> {
        // Parse source URL
        let (bucket, key) = parse_storage_url(source)?;

        // Create storage backend to get file size
        let storage = factory
            .create(source)
            .map_err(|e| format!("Failed to create storage: {}", e))?;

        // Get file size and compute hash
        use std::path::Path;
        let metadata = storage
            .metadata(Path::new(&key))
            .map_err(|e| format!("Failed to get file metadata: {}", e))?;

        let source_size = metadata.size;
        let job_id = compute_file_hash(&key, source_size);

        if self.verbose {
            println!(
                "Processing: {} (size: {} bytes, hash: {})",
                source, source_size, job_id
            );
        }

        // Check if job already exists
        if let Ok(Some(existing)) = tikv.get_job(&job_id).await {
            if existing.is_terminal() {
                println!(
                    "Job {} already exists with status: {:?}",
                    job_id, existing.status
                );
                return Ok(existing);
            }
            return Err(format!(
                "Job {} already exists with status: {:?}",
                job_id, existing.status
            ));
        }

        // Get submitter identity for authorization
        let submitter = std::env::var("ROBOFLOW_USER")
            .or_else(|_| std::env::var("USER"))
            .or_else(|_| std::env::var("USERNAME"))
            .unwrap_or_else(|_| "unknown".to_string());

        // Create job record
        let job = JobRecord::new(
            job_id.clone(),
            key,
            bucket,
            source_size,
            output.to_string(),
            config_hash.to_string(),
        );

        let mut job = job;
        job.max_attempts = max_attempts;
        job.submitted_by = Some(submitter);

        if self.dry_run {
            println!("Would submit job: {}", job_id);
            return Ok(job);
        }

        // Submit job to TiKV
        tikv.put_job(&job)
            .await
            .map_err(|e| format!("Failed to submit job: {}", e))?;

        println!("Submitted job: {}", job_id);

        Ok(job)
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
            if let Err(e) = roboflow_dataset::lerobot::LerobotConfig::from_toml(&content) {
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
}
