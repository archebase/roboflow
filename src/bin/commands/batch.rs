// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Batch command for managing batch jobs.
//!
//! ## Usage
//!
//! ```bash
//! # Submit a batch job from YAML spec
//! roboflow batch submit batch.yaml
//!
//! # Get batch status
//! roboflow batch status <batch-id>
//!
//! # List all batch jobs
//! roboflow batch list [--phase Pending|Discovering|Running|Complete|Failed|Cancelled]
//! roboflow batch list --namespace default
//!
//! # Cancel a batch job
//! roboflow batch cancel <batch-id>
//! ```

use chrono::Utc;
use roboflow_distributed::{
    BatchController, BatchPhase, BatchSpec, BatchStatus, BatchSummary, TikvClient,
};

use crate::commands::audit::{AuditContext, AuditLogger, AuditOperation};

/// Validate a batch ID to prevent injection attacks.
///
/// Batch IDs must follow the format `{namespace}:{name}` where both parts
/// are non-empty and contain only valid DNS label characters.
fn validate_batch_id(batch_id: &str) -> Result<(), String> {
    if batch_id.is_empty() {
        return Err("Batch ID cannot be empty".to_string());
    }

    if batch_id.len() > 1024 {
        return Err("Batch ID too long (max 1024 characters)".to_string());
    }

    // Check for null bytes
    if batch_id.contains('\0') {
        return Err("Batch ID contains null bytes".to_string());
    }

    // Check for control characters (except tab)
    if batch_id.chars().any(|c| c.is_control() && c != '\t') {
        return Err("Batch ID contains control characters".to_string());
    }

    // Check for shell metacharacters that might indicate injection
    const DANGEROUS_CHARS: &[char] = &[';', '|', '&', '$', '`', '\n', '\r'];
    if batch_id.chars().any(|c| DANGEROUS_CHARS.contains(&c)) {
        return Err("Batch ID contains invalid characters".to_string());
    }

    // Validate format: must be "namespace:name"
    let parts: Vec<&str> = batch_id.splitn(2, ':').collect();
    if parts.len() != 2 {
        return Err("Batch ID must be in format 'namespace:name'".to_string());
    }

    let namespace = parts[0];
    let name = parts[1];

    if namespace.is_empty() {
        return Err("Batch ID namespace cannot be empty".to_string());
    }

    if name.is_empty() {
        return Err("Batch ID name cannot be empty".to_string());
    }

    // Validate DNS label format (lowercase alphanumeric, hyphens, dots)
    let is_valid_label = |s: &str| -> bool {
        if s.is_empty() {
            return false;
        }
        s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
    };

    if !is_valid_label(namespace) {
        return Err(format!(
            "Batch ID namespace '{}' contains invalid characters (use lowercase letters, digits, hyphens, dots)",
            namespace
        ));
    }

    if !is_valid_label(name) {
        return Err(format!(
            "Batch ID name '{}' contains invalid characters (use lowercase letters, digits, hyphens, dots)",
            name
        ));
    }

    Ok(())
}

/// Output format for batch commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Table,
    Json,
    Csv,
}

/// Batch command options.
#[derive(Debug, Clone)]
pub enum BatchCommand {
    /// Submit a batch job from YAML spec
    Submit {
        /// Path to batch spec file (YAML)
        spec_file: String,
        /// TiKV endpoints (overrides config/env)
        tikv_endpoints: Option<String>,
    },

    /// Get batch job status
    Status {
        /// Batch ID (namespace:name format)
        batch_id: String,
        /// Output format
        format: OutputFormat,
        /// Watch for changes (continuously update)
        watch: bool,
        /// TiKV endpoints
        tikv_endpoints: Option<String>,
    },

    /// List batch jobs
    List {
        /// Filter by phase
        phase: Option<BatchPhase>,
        /// Filter by namespace
        namespace: Option<String>,
        /// Limit results
        limit: u32,
        /// Output format
        format: OutputFormat,
        /// TiKV endpoints
        tikv_endpoints: Option<String>,
    },

    /// Cancel a batch job
    Cancel {
        /// Batch ID (namespace:name format)
        batch_id: String,
        /// TiKV endpoints
        tikv_endpoints: Option<String>,
    },
}

impl BatchCommand {
    /// Parse batch command from CLI arguments.
    pub fn parse(args: &[String]) -> Result<Option<Self>, String> {
        if args.is_empty() {
            print_batch_help();
            return Ok(None);
        }

        let subcommand = args[0].as_str();
        let remaining = &args[1..];

        match subcommand {
            "submit" => Self::parse_submit(remaining),
            "status" => Self::parse_status(remaining),
            "list" => Self::parse_list(remaining),
            "cancel" => Self::parse_cancel(remaining),
            "--help" | "-h" | "help" => {
                print_batch_help();
                Ok(None)
            }
            unknown => Err(format!(
                "unknown batch command: {}\n\n{}",
                unknown,
                get_batch_help_summary()
            )),
        }
    }

    fn parse_submit(args: &[String]) -> Result<Option<Self>, String> {
        if args.is_empty() {
            return Err("submit requires a spec file".to_string());
        }

        let mut spec_file = None;
        let mut tikv_endpoints = None;

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--pd-endpoints" | "--tikv-endpoints" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("--pd-endpoints requires a value".to_string());
                    }
                    tikv_endpoints = Some(args[i].clone());
                }
                "--help" | "-h" => {
                    print_submit_help();
                    return Ok(None);
                }
                arg if !arg.starts_with('-') => {
                    if spec_file.is_some() {
                        return Err("submit accepts only one spec file".to_string());
                    }
                    spec_file = Some(arg.to_string());
                }
                unknown => {
                    return Err(format!("unknown flag for submit: {}", unknown));
                }
            }
            i += 1;
        }

        let spec_file = spec_file.ok_or_else(|| "submit requires a spec file".to_string())?;

        Ok(Some(BatchCommand::Submit {
            spec_file,
            tikv_endpoints,
        }))
    }

    fn parse_status(args: &[String]) -> Result<Option<Self>, String> {
        if args.is_empty() {
            return Err("status requires a batch ID".to_string());
        }

        let mut batch_id = None;
        let mut format = OutputFormat::Table;
        let mut watch = false;
        let mut tikv_endpoints = None;

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--json" => {
                    format = OutputFormat::Json;
                }
                "--csv" => {
                    format = OutputFormat::Csv;
                }
                "--watch" | "-w" => {
                    watch = true;
                }
                "--pd-endpoints" | "--tikv-endpoints" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("--pd-endpoints requires a value".to_string());
                    }
                    tikv_endpoints = Some(args[i].clone());
                }
                "--help" | "-h" => {
                    print_status_help();
                    return Ok(None);
                }
                arg if !arg.starts_with('-') => {
                    if batch_id.is_some() {
                        return Err("status accepts only one batch ID".to_string());
                    }
                    batch_id = Some(arg.to_string());
                }
                unknown => {
                    return Err(format!("unknown flag for status: {}", unknown));
                }
            }
            i += 1;
        }

        let batch_id = batch_id.ok_or_else(|| "status requires a batch ID".to_string())?;

        Ok(Some(BatchCommand::Status {
            batch_id,
            format,
            watch,
            tikv_endpoints,
        }))
    }

    fn parse_list(args: &[String]) -> Result<Option<Self>, String> {
        let mut phase = None;
        let mut namespace = None;
        let mut limit = 100;
        let mut format = OutputFormat::Table;
        let mut tikv_endpoints = None;

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--phase" | "-p" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("--phase requires a value".to_string());
                    }
                    phase = parse_batch_phase(&args[i]);
                    if phase.is_none() {
                        return Err(format!("invalid phase: {}", args[i]));
                    }
                }
                "--namespace" | "-n" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("--namespace requires a value".to_string());
                    }
                    namespace = Some(args[i].clone());
                }
                "--limit" | "-l" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("--limit requires a value".to_string());
                    }
                    let parsed_limit = args[i].parse::<u32>();
                    if parsed_limit.is_err() {
                        return Err(format!("invalid limit: {}", args[i]));
                    }
                    limit = parsed_limit.unwrap();
                }
                "--json" => {
                    format = OutputFormat::Json;
                }
                "--csv" => {
                    format = OutputFormat::Csv;
                }
                "--pd-endpoints" | "--tikv-endpoints" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("--pd-endpoints requires a value".to_string());
                    }
                    tikv_endpoints = Some(args[i].clone());
                }
                "--help" | "-h" => {
                    print_list_help();
                    return Ok(None);
                }
                unknown => {
                    return Err(format!("unknown flag for list: {}", unknown));
                }
            }
            i += 1;
        }

        Ok(Some(BatchCommand::List {
            phase,
            namespace,
            limit,
            format,
            tikv_endpoints,
        }))
    }

    fn parse_cancel(args: &[String]) -> Result<Option<Self>, String> {
        if args.is_empty() {
            return Err("cancel requires a batch ID".to_string());
        }

        let mut batch_id = None;
        let mut tikv_endpoints = None;

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--pd-endpoints" | "--tikv-endpoints" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("--pd-endpoints requires a value".to_string());
                    }
                    tikv_endpoints = Some(args[i].clone());
                }
                "--help" | "-h" => {
                    print_cancel_help();
                    return Ok(None);
                }
                arg if !arg.starts_with('-') => {
                    if batch_id.is_some() {
                        return Err("cancel accepts only one batch ID".to_string());
                    }
                    batch_id = Some(arg.to_string());
                }
                unknown => {
                    return Err(format!("unknown flag for cancel: {}", unknown));
                }
            }
            i += 1;
        }

        let batch_id = batch_id.ok_or_else(|| "cancel requires a batch ID".to_string())?;

        Ok(Some(BatchCommand::Cancel {
            batch_id,
            tikv_endpoints,
        }))
    }

    /// Run the batch command.
    pub async fn run(&self) -> Result<(), String> {
        match self {
            BatchCommand::Submit {
                spec_file,
                tikv_endpoints,
            } => self.run_submit(spec_file, tikv_endpoints).await,
            BatchCommand::Status {
                batch_id,
                format,
                watch,
                tikv_endpoints,
            } => {
                self.run_status(batch_id, *format, *watch, tikv_endpoints)
                    .await
            }
            BatchCommand::List {
                phase,
                namespace,
                limit,
                format,
                tikv_endpoints,
            } => {
                self.run_list(*phase, namespace.as_ref(), *limit, *format, tikv_endpoints)
                    .await
            }
            BatchCommand::Cancel {
                batch_id,
                tikv_endpoints,
            } => self.run_cancel(batch_id, tikv_endpoints).await,
        }
    }

    /// Submit a batch job from a YAML spec file.
    async fn run_submit(
        &self,
        spec_file: &str,
        tikv_endpoints: &Option<String>,
    ) -> Result<(), String> {
        // Get requester identity for audit
        let requester = std::env::var("ROBOFLOW_USER")
            .or_else(|_| std::env::var("USER"))
            .or_else(|_| std::env::var("USERNAME"))
            .unwrap_or_else(|_| "unknown".to_string());

        // Read spec file
        let spec_content = tokio::fs::read_to_string(spec_file)
            .await
            .map_err(|e| format!("Failed to read spec file '{}': {}", spec_file, e))?;
        let spec: BatchSpec = serde_yaml::from_str(&spec_content)
            .map_err(|e| format!("Failed to parse spec file: {}", e))?;

        // Connect to TiKV
        let client = create_client(tikv_endpoints).await?;
        let controller = BatchController::with_client(client);

        // Submit batch
        let batch_id = controller
            .submit_batch(&spec)
            .await
            .map_err(|e| format!("Failed to submit batch: {}", e))?;

        // Log successful submission
        AuditLogger::log_success(
            AuditOperation::BatchSubmit,
            &requester,
            &batch_id,
            &AuditContext::default()
                .add("spec_file", spec_file)
                .add("sources", spec.spec.sources.len().to_string())
                .add("output", &spec.spec.output),
        );

        println!("Batch job submitted successfully");
        println!("  Batch ID: {}", batch_id);
        println!("  Name: {}", spec.metadata.display_name.as_ref().unwrap_or(&spec.metadata.name));
        println!("  Sources: {}", spec.spec.sources.len());
        println!("  Output: {}", spec.spec.output);

        Ok(())
    }

    /// Get batch job status.
    async fn run_status(
        &self,
        batch_id: &str,
        format: OutputFormat,
        watch: bool,
        tikv_endpoints: &Option<String>,
    ) -> Result<(), String> {
        validate_batch_id(batch_id)?;

        if watch {
            self.run_status_watch(batch_id, format, tikv_endpoints)
                .await
        } else {
            self.run_status_once(batch_id, format, tikv_endpoints).await
        }
    }

    /// Show batch status once.
    async fn run_status_once(
        &self,
        batch_id: &str,
        format: OutputFormat,
        tikv_endpoints: &Option<String>,
    ) -> Result<(), String> {
        let client = create_client(tikv_endpoints).await?;
        let controller = BatchController::with_client(client);

        let status = controller
            .get_batch_status(batch_id)
            .await
            .map_err(|e| format!("Failed to get batch status: {}", e))?
            .ok_or_else(|| format!("Batch not found: {}", batch_id))?;

        match format {
            OutputFormat::Json => {
                println!("{}", serde_json::to_string_pretty(&status).unwrap());
            }
            OutputFormat::Csv => {
                println!("phase,files_total,files_completed,files_failed,progress");
                println!(
                    "{},{},{},{},{}",
                    status.phase,
                    status.files_total,
                    status.files_completed,
                    status.files_failed,
                    status.progress()
                );
            }
            OutputFormat::Table => {
                print_status_table(batch_id, &status);
            }
        }

        Ok(())
    }

    /// Watch batch status for changes.
    async fn run_status_watch(
        &self,
        batch_id: &str,
        format: OutputFormat,
        tikv_endpoints: &Option<String>,
    ) -> Result<(), String> {
        let client = create_client(tikv_endpoints).await?;
        let controller = BatchController::with_client(client.clone());

        let mut last_phase = None;

        loop {
            let status = controller
                .get_batch_status(batch_id)
                .await
                .map_err(|e| e.to_string())?;

            match &status {
                Some(s) => {
                    let phase_changed = last_phase != Some(s.phase);
                    last_phase = Some(s.phase);

                    if phase_changed || !s.phase.is_terminal() {
                        // Clear screen and show updated status
                        print!("\x1B[2J\x1B[1;1H"); // Clear screen
                        println!("Batch: {} (Phase: {})", batch_id, s.phase);
                        println!();

                        match format {
                            OutputFormat::Json => {
                                println!("{}", serde_json::to_string_pretty(&s).unwrap());
                            }
                            OutputFormat::Csv => {
                                println!("phase,files_total,files_completed,files_failed,progress");
                                println!(
                                    "{},{},{},{},{}",
                                    s.phase,
                                    s.files_total,
                                    s.files_completed,
                                    s.files_failed,
                                    s.progress()
                                );
                            }
                            OutputFormat::Table => {
                                print_status_table(batch_id, s);
                            }
                        }
                    }

                    if s.phase.is_terminal() {
                        println!("\nBatch job completed with phase: {}", s.phase);
                        break;
                    }
                }
                None => {
                    println!("Batch not found: {}", batch_id);
                    break;
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        }

        Ok(())
    }

    /// List batch jobs.
    async fn run_list(
        &self,
        phase: Option<BatchPhase>,
        namespace: Option<&String>,
        limit: u32,
        format: OutputFormat,
        tikv_endpoints: &Option<String>,
    ) -> Result<(), String> {
        let client = create_client(tikv_endpoints).await?;
        let controller = BatchController::with_client(client);

        let batches = controller
            .list_batches()
            .await
            .map_err(|e| format!("Failed to list batches: {}", e))?;

        // Filter results
        let batches: Vec<_> = batches
            .into_iter()
            .filter(|b| {
                if let Some(p) = phase
                    && b.phase != p
                {
                    return false;
                }
                if let Some(ns) = namespace
                    && &b.namespace != ns
                {
                    return false;
                }
                true
            })
            .take(limit as usize)
            .collect();

        match format {
            OutputFormat::Json => {
                println!("{}", serde_json::to_string_pretty(&batches).unwrap());
            }
            OutputFormat::Csv => {
                println!(
                    "id,name,namespace,phase,files_total,files_completed,files_failed,created_at"
                );
                for b in &batches {
                    println!(
                        "{},{},{},{},{},{},{},{}",
                        b.id,
                        b.name,
                        b.namespace,
                        b.phase,
                        b.files_total,
                        b.files_completed,
                        b.files_failed,
                        b.created_at.format("%Y-%m-%d %H:%M:%S")
                    );
                }
            }
            OutputFormat::Table => {
                print_batches_table(&batches);
            }
        }

        Ok(())
    }

    /// Cancel a batch job.
    async fn run_cancel(
        &self,
        batch_id: &str,
        tikv_endpoints: &Option<String>,
    ) -> Result<(), String> {
        validate_batch_id(batch_id)?;

        // Get requester identity for audit
        let requester = std::env::var("ROBOFLOW_USER")
            .or_else(|_| std::env::var("USER"))
            .or_else(|_| std::env::var("USERNAME"))
            .unwrap_or_else(|_| "unknown".to_string());

        let client = create_client(tikv_endpoints).await?;
        let controller = BatchController::with_client(client);

        let cancelled = controller
            .cancel_batch(batch_id)
            .await
            .map_err(|e| format!("Failed to cancel batch: {}", e))?;

        if cancelled {
            AuditLogger::log_success(
                AuditOperation::BatchCancel,
                &requester,
                batch_id,
                &AuditContext::default(),
            );

            println!("Batch job cancelled: {}", batch_id);
        } else {
            println!("Batch job not found or cannot be cancelled: {}", batch_id);
        }

        Ok(())
    }
}

/// Create a TiKV client from endpoints.
async fn create_client(
    tikv_endpoints: &Option<String>,
) -> Result<std::sync::Arc<TikvClient>, String> {
    let config = if let Some(eps) = tikv_endpoints {
        roboflow_distributed::TikvConfig::with_pd_endpoints(eps)
    } else {
        roboflow_distributed::TikvConfig::default()
    };

    TikvClient::new(config)
        .await
        .map(std::sync::Arc::new)
        .map_err(|e| format!("Failed to connect to TiKV: {}", e))
}

/// Parse a batch phase string.
fn parse_batch_phase(s: &str) -> Option<BatchPhase> {
    match s.to_lowercase().as_str() {
        "pending" => Some(BatchPhase::Pending),
        "discovering" => Some(BatchPhase::Discovering),
        "running" => Some(BatchPhase::Running),
        "complete" | "completed" => Some(BatchPhase::Complete),
        "failed" => Some(BatchPhase::Failed),
        "cancelled" | "canceled" => Some(BatchPhase::Cancelled),
        _ => None,
    }
}

/// Print batch status as a table.
fn print_status_table(batch_id: &str, status: &BatchStatus) {
    println!("Batch ID: {}", batch_id);
    println!("Phase:    {}", status.phase);
    println!(
        "Progress: {}/{} files ({}%)",
        status.files_completed,
        status.files_total,
        status.progress() as u32
    );
    println!();

    if status.files_total > 0 {
        println!("Files:");
        println!("  Total:     {}", status.files_total);
        println!("  Completed: {}", status.files_completed);
        println!("  Failed:    {}", status.files_failed);
        println!("  Active:    {}", status.files_active);
    }

    if status.work_units_total > 0 {
        println!();
        println!("Work Units:");
        println!("  Total:     {}", status.work_units_total);
        println!("  Completed: {}", status.work_units_completed);
        println!("  Failed:    {}", status.work_units_failed);
        println!("  Active:    {}", status.work_units_active);
    }

    if let Some(started) = status.started_at {
        let elapsed = Utc::now().signed_duration_since(started);
        println!();
        println!(
            "Started:  {} ({} ago)",
            started.format("%Y-%m-%d %H:%M:%S"),
            format_duration(elapsed)
        );
    }

    if let Some(error) = &status.error {
        println!();
        println!("Error: {}", error);
    }
}

/// Format a duration for display.
fn format_duration(d: chrono::Duration) -> String {
    let secs = d.num_seconds();
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    }
}

/// Print batches as a table.
fn print_batches_table(batches: &[BatchSummary]) {
    if batches.is_empty() {
        println!("No batch jobs found");
        return;
    }

    println!(
        "{:<20} {:<15} {:<12} {:>10} {:>10} {:>10} {:>10}",
        "ID", "NAME", "PHASE", "TOTAL", "DONE", "FAILED", "CREATED"
    );
    println!("{}", "-".repeat(100));

    for b in batches {
        println!(
            "{:<20} {:<15} {:<12} {:>10} {:>10} {:>10} {:>10}",
            truncate_id(&b.id, 20),
            truncate(&b.name, 15),
            format!("{:?}", b.phase),
            b.files_total,
            b.files_completed,
            b.files_failed,
            b.created_at.format("%m/%d %H:%M")
        );
    }

    println!();
    println!("Total: {} batch job(s)", batches.len());
}

/// Truncate a string to fit within max length.
fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}

/// Truncate an ID to fit within max length.
fn truncate_id(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        return s.to_string();
    }

    // If it's a namespace:name format, keep both parts truncated
    if let Some(idx) = s.find(':') {
        let ns = &s[..idx.min(8)];
        let name = &s[idx + 1..];
        let truncated_name = if name.len() > 8 {
            format!("{}...", &name[..8])
        } else {
            name.to_string()
        };
        format!("{}:{}", ns, truncated_name)
    } else {
        truncate(s, max_len)
    }
}

/// Run the batch command from raw args.
pub async fn run_batch_command(args: &[String]) -> Result<(), String> {
    let cmd = BatchCommand::parse(args)?;
    match cmd {
        Some(command) => command.run().await,
        None => Ok(()), // Help was printed
    }
}

fn get_batch_help_summary() -> String {
    r#"Available commands: submit, status, list, cancel

Run 'roboflow batch <command> --help' for more information."#
        .to_string()
}

fn print_batch_help() {
    println!(
        r#"Manage batch jobs for distributed processing.

USAGE:
    roboflow batch <COMMAND> [OPTIONS]

COMMANDS:
    submit      Submit a batch job from YAML spec
    status      Get batch job status
    list        List batch jobs
    cancel      Cancel a batch job

OPTIONS:
    -h, --help    Print this help

Run 'roboflow batch <command> --help' for more information on a specific command.

EXAMPLES:
    # Submit a batch job
    roboflow batch submit batch.yaml

    # Get batch status
    roboflow batch status default:my-batch

    # List all batches
    roboflow batch list

    # Watch batch status
    roboflow batch status default:my-batch --watch

    # Cancel a batch
    roboflow batch cancel default:my-batch
"#
    );
}

fn print_submit_help() {
    println!(
        r#"Submit a batch job from a YAML spec file.

USAGE:
    roboflow batch submit <SPEC_FILE> [OPTIONS]

ARGUMENTS:
    <SPEC_FILE>    Path to batch spec file (YAML)

OPTIONS:
        --pd-endpoints <ADDRS>
                     TiKV PD endpoints (default: $TIKV_PD_ENDPOINTS)
    -h, --help       Print this help

EXAMPLES:
    # Submit a batch job
    roboflow batch submit batch.yaml

    # Submit with custom TiKV endpoints
    roboflow batch submit batch.yaml --pd-endpoints 127.0.0.1:2379
"#
    );
}

fn print_status_help() {
    println!(
        r#"Get batch job status.

USAGE:
    roboflow batch status <BATCH_ID> [OPTIONS]

ARGUMENTS:
    <BATCH_ID>     Batch ID (namespace:name format)

OPTIONS:
        --json               Output in JSON format
        --csv                Output in CSV format
        --watch, -w          Watch for changes (continuously update)
        --pd-endpoints <ADDRS>
                             TiKV PD endpoints (default: $TIKV_PD_ENDPOINTS)
    -h, --help               Print this help

EXAMPLES:
    # Get batch status
    roboflow batch status default:my-batch

    # Get batch status as JSON
    roboflow batch status default:my-batch --json

    # Watch batch status
    roboflow batch status default:my-batch --watch
"#
    );
}

fn print_list_help() {
    println!(
        r#"List batch jobs.

USAGE:
    roboflow batch list [OPTIONS]

OPTIONS:
    -p, --phase <PHASE>     Filter by phase
                           (Pending|Discovering|Running|Complete|Failed|Cancelled)
    -n, --namespace <NS>    Filter by namespace
    -l, --limit <N>         Maximum number of batches to show (default: 100)
        --json              Output in JSON format
        --csv               Output in CSV format
        --pd-endpoints <ADDRS>
                           TiKV PD endpoints (default: $TIKV_PD_ENDPOINTS)
    -h, --help              Print this help

EXAMPLES:
    # List all batches
    roboflow batch list

    # List running batches
    roboflow batch list --phase Running

    # List batches in specific namespace
    roboflow batch list --namespace production

    # List batches as JSON
    roboflow batch list --json
"#
    );
}

fn print_cancel_help() {
    println!(
        r#"Cancel a batch job.

USAGE:
    roboflow batch cancel <BATCH_ID> [OPTIONS]

ARGUMENTS:
    <BATCH_ID>     Batch ID to cancel

OPTIONS:
        --pd-endpoints <ADDRS>
                     TiKV PD endpoints (default: $TIKV_PD_ENDPOINTS)
    -h, --help       Print this help

EXAMPLES:
    # Cancel a batch
    roboflow batch cancel default:my-batch

NOTE:
    Only Pending, Discovering, or Running batches can be cancelled.
"#
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_batch_id() {
        // Valid formats
        assert!(validate_batch_id("default:my-batch").is_ok());
        assert!(validate_batch_id("production:batch-123").is_ok());
        assert!(validate_batch_id("ns:batch").is_ok());
        assert!(validate_batch_id("a.b:batch-name").is_ok());

        // Invalid: empty
        assert!(validate_batch_id("").is_err());

        // Invalid: missing colon
        assert!(validate_batch_id("batch").is_err());

        // Invalid: empty namespace
        assert!(validate_batch_id(":batch").is_err());

        // Invalid: empty name
        assert!(validate_batch_id("namespace:").is_err());

        // Invalid: injection attempts
        assert!(validate_batch_id("batch; rm -rf /").is_err());
        assert!(validate_batch_id("batch`whoami`").is_err());

        // Invalid: uppercase characters (DNS labels are lowercase)
        assert!(validate_batch_id("Default:Batch").is_err());
        assert!(validate_batch_id("default:MyBatch").is_err());
    }

    #[test]
    fn test_truncate() {
        assert_eq!(truncate("hello", 10), "hello");
        assert_eq!(truncate("hello world", 8), "hello...");
        assert_eq!(truncate(&"a".repeat(20), 15), "aaaaaaaaaaaa...");
    }

    #[test]
    fn test_truncate_id() {
        assert_eq!(truncate_id("default:my-batch", 20), "default:my-batch");
        assert_eq!(
            truncate_id("production:very-long-batch-name", 20),
            "producti:very-lon..."
        );
    }

    #[test]
    fn test_parse_batch_phase() {
        assert_eq!(parse_batch_phase("Pending"), Some(BatchPhase::Pending));
        assert_eq!(parse_batch_phase("pending"), Some(BatchPhase::Pending));
        assert_eq!(parse_batch_phase("Running"), Some(BatchPhase::Running));
        assert_eq!(parse_batch_phase("Complete"), Some(BatchPhase::Complete));
        assert_eq!(parse_batch_phase("completed"), Some(BatchPhase::Complete));
        assert_eq!(parse_batch_phase("Failed"), Some(BatchPhase::Failed));
        assert_eq!(parse_batch_phase("Cancelled"), Some(BatchPhase::Cancelled));
        assert_eq!(parse_batch_phase("canceled"), Some(BatchPhase::Cancelled));
        assert_eq!(parse_batch_phase("invalid"), None);
    }
}
