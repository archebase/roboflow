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

use roboflow_distributed::{JobRecord, TikvClient};
use roboflow_storage::StorageFactory;

use crate::commands::jobs::{OutputFormat, print_job_output};
use crate::commands::utils::{compute_file_hash, glob_match, parse_storage_url};

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

        // Get config hash
        let config_hash = self
            .config_hash
            .as_ref()
            .cloned()
            .unwrap_or_else(|| "default".to_string());

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

            // Submit each matched file
            for file_path in &matched_files {
                let full_url = format!("{}/{}", storage_url.trim_end_matches('/'), file_path);
                self.submit_job_spec(&full_url, output, config_hash, max_attempts, factory, tikv)
                    .await?;
            }

            // Return the first job as representative
            let first_url = format!("{}/{}", storage_url.trim_end_matches('/'), matched_files[0]);
            self.submit_job_spec(&first_url, output, config_hash, max_attempts, factory, tikv)
                .await
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
    -c, --config <HASH>     Dataset configuration hash
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

    # Submit with custom config hash
    roboflow submit file.mcap --config custom-config-v1

ENVIRONMENT VARIABLES:
    ROBOFLOW_OUTPUT_PREFIX    Default output location
    TIKV_PD_ENDPOINTS         TiKV PD endpoints (default: 127.0.0.1:2379)
"#
    );
}
