// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Jobs command for managing distributed jobs.
//!
//! ## Usage
//!
//! ```bash
//! # List jobs
//! roboflow jobs list [--status pending|processing|completed|failed|dead]
//! roboflow jobs list --limit 50 --offset 0
//!
//! # Get job details
//! roboflow jobs get <job-id>
//!
//! # Retry a failed job
//! roboflow jobs retry <job-id>
//! roboflow jobs retry --all-failed
//!
//! # Cancel a job
//! roboflow jobs cancel <job-id>
//!
//! # Delete a job (and checkpoint)
//! roboflow jobs delete <job-id>
//! roboflow jobs delete --completed --older-than 7d
//!
//! # Get job statistics
//! roboflow jobs stats
//! ```

use chrono::{DateTime, Duration, Utc};
use roboflow_distributed::{JobRecord, JobStatus, TikvClient};
use serde::Serialize;

use crate::commands::utils::compute_file_hash;

/// Output format for job commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Table,
    Json,
    Csv,
}

/// Jobs command options.
#[derive(Debug, Clone)]
pub enum JobsCommand {
    /// List jobs
    List {
        /// Filter by status
        status: Option<JobStatus>,
        /// Limit results
        limit: Option<u32>,
        /// Offset for pagination
        offset: Option<u32>,
        /// Output format
        format: OutputFormat,
        /// TiKV endpoints
        tikv_endpoints: Option<String>,
    },
    /// Get job details
    Get {
        /// Job ID or file hash
        job_id: String,
        /// Output format
        format: OutputFormat,
        /// TiKV endpoints
        tikv_endpoints: Option<String>,
    },
    /// Retry failed jobs
    Retry {
        /// Job ID or file hash
        job_id: Option<String>,
        /// Retry all failed jobs
        all_failed: bool,
        /// TiKV endpoints
        tikv_endpoints: Option<String>,
    },
    /// Cancel jobs
    Cancel {
        /// Job ID or file hash
        job_id: String,
        /// TiKV endpoints
        tikv_endpoints: Option<String>,
    },
    /// Delete jobs
    Delete {
        /// Job IDs to delete
        job_ids: Vec<String>,
        /// Delete completed jobs
        completed: bool,
        /// Delete jobs older than duration (e.g., 7d, 24h)
        older_than: Option<String>,
        /// Force deletion without confirmation
        force: bool,
        /// Also delete checkpoint
        delete_checkpoint: bool,
        /// TiKV endpoints
        tikv_endpoints: Option<String>,
    },
    /// Show job statistics
    Stats {
        /// Output format
        format: OutputFormat,
        /// TiKV endpoints
        tikv_endpoints: Option<String>,
    },
}

impl JobsCommand {
    /// Parse jobs command from CLI arguments.
    pub fn parse(args: &[String]) -> Result<Option<Self>, String> {
        if args.is_empty() {
            print_jobs_help();
            return Ok(None);
        }

        let subcommand = args[0].as_str();
        let remaining = &args[1..];

        match subcommand {
            "list" => Self::parse_list(remaining),
            "get" => Self::parse_get(remaining),
            "retry" => Self::parse_retry(remaining),
            "cancel" => Self::parse_cancel(remaining),
            "delete" => Self::parse_delete(remaining),
            "stats" => Self::parse_stats(remaining),
            "--help" | "-h" | "help" => {
                print_jobs_help();
                Ok(None)
            }
            unknown => Err(format!(
                "unknown jobs command: {}\n\n{}",
                unknown,
                get_jobs_help_summary()
            )),
        }
    }

    fn parse_list(args: &[String]) -> Result<Option<Self>, String> {
        let mut status = None;
        let mut limit = None;
        let mut offset = None;
        let mut format = OutputFormat::Table;
        let mut tikv_endpoints = None;

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--status" | "-s" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("--status requires a value".to_string());
                    }
                    status = parse_job_status(&args[i]);
                    if status.is_none() {
                        return Err(format!("invalid status: {}", args[i]));
                    }
                }
                "--limit" | "-l" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("--limit requires a value".to_string());
                    }
                    limit = args[i].parse().ok();
                    if limit.is_none() {
                        return Err(format!("invalid limit: {}", args[i]));
                    }
                }
                "--offset" | "-o" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("--offset requires a value".to_string());
                    }
                    offset = args[i].parse().ok();
                    if offset.is_none() {
                        return Err(format!("invalid offset: {}", args[i]));
                    }
                }
                "--json" => {
                    format = OutputFormat::Json;
                }
                "--csv" => {
                    format = OutputFormat::Csv;
                }
                "--tikv-endpoints" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("--tikv-endpoints requires a value".to_string());
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

        Ok(Some(JobsCommand::List {
            status,
            limit,
            offset,
            format,
            tikv_endpoints,
        }))
    }

    fn parse_get(args: &[String]) -> Result<Option<Self>, String> {
        if args.is_empty() {
            return Err("get requires a job ID".to_string());
        }

        let mut format = OutputFormat::Table;
        let mut tikv_endpoints = None;
        let mut job_id = None;

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--json" => {
                    format = OutputFormat::Json;
                }
                "--csv" => {
                    format = OutputFormat::Csv;
                }
                "--tikv-endpoints" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("--tikv-endpoints requires a value".to_string());
                    }
                    tikv_endpoints = Some(args[i].clone());
                }
                "--help" | "-h" => {
                    print_get_help();
                    return Ok(None);
                }
                arg if !arg.starts_with('-') => {
                    if job_id.is_some() {
                        return Err("get accepts only one job ID".to_string());
                    }
                    job_id = Some(arg.to_string());
                }
                unknown => {
                    return Err(format!("unknown flag for get: {}", unknown));
                }
            }
            i += 1;
        }

        let job_id = job_id.ok_or_else(|| "get requires a job ID".to_string())?;

        Ok(Some(JobsCommand::Get {
            job_id,
            format,
            tikv_endpoints,
        }))
    }

    fn parse_retry(args: &[String]) -> Result<Option<Self>, String> {
        let mut job_id = None;
        let mut all_failed = false;
        let mut tikv_endpoints = None;

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--all-failed" => {
                    all_failed = true;
                }
                "--tikv-endpoints" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("--tikv-endpoints requires a value".to_string());
                    }
                    tikv_endpoints = Some(args[i].clone());
                }
                "--help" | "-h" => {
                    print_retry_help();
                    return Ok(None);
                }
                arg if !arg.starts_with('-') => {
                    if job_id.is_some() {
                        return Err("retry accepts only one job ID or --all-failed".to_string());
                    }
                    job_id = Some(arg.to_string());
                }
                unknown => {
                    return Err(format!("unknown flag for retry: {}", unknown));
                }
            }
            i += 1;
        }

        Ok(Some(JobsCommand::Retry {
            job_id,
            all_failed,
            tikv_endpoints,
        }))
    }

    fn parse_cancel(args: &[String]) -> Result<Option<Self>, String> {
        if args.is_empty() {
            return Err("cancel requires a job ID".to_string());
        }

        let mut job_id = None;
        let mut tikv_endpoints = None;

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--tikv-endpoints" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("--tikv-endpoints requires a value".to_string());
                    }
                    tikv_endpoints = Some(args[i].clone());
                }
                "--help" | "-h" => {
                    print_cancel_help();
                    return Ok(None);
                }
                arg if !arg.starts_with('-') => {
                    if job_id.is_some() {
                        return Err("cancel accepts only one job ID".to_string());
                    }
                    job_id = Some(arg.to_string());
                }
                unknown => {
                    return Err(format!("unknown flag for cancel: {}", unknown));
                }
            }
            i += 1;
        }

        let job_id = job_id.ok_or_else(|| "cancel requires a job ID".to_string())?;

        Ok(Some(JobsCommand::Cancel {
            job_id,
            tikv_endpoints,
        }))
    }

    fn parse_delete(args: &[String]) -> Result<Option<Self>, String> {
        let mut job_ids = Vec::new();
        let mut completed = false;
        let mut older_than = None;
        let mut force = false;
        let mut delete_checkpoint = true;
        let mut tikv_endpoints = None;

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--completed" => {
                    completed = true;
                }
                "--older-than" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("--older-than requires a value".to_string());
                    }
                    older_than = Some(args[i].clone());
                }
                "--force" | "-f" => {
                    force = true;
                }
                "--keep-checkpoint" => {
                    delete_checkpoint = false;
                }
                "--tikv-endpoints" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("--tikv-endpoints requires a value".to_string());
                    }
                    tikv_endpoints = Some(args[i].clone());
                }
                "--help" | "-h" => {
                    print_delete_help();
                    return Ok(None);
                }
                arg if !arg.starts_with('-') => {
                    job_ids.push(arg.to_string());
                }
                unknown => {
                    return Err(format!("unknown flag for delete: {}", unknown));
                }
            }
            i += 1;
        }

        Ok(Some(JobsCommand::Delete {
            job_ids,
            completed,
            older_than,
            force,
            delete_checkpoint,
            tikv_endpoints,
        }))
    }

    fn parse_stats(args: &[String]) -> Result<Option<Self>, String> {
        let mut format = OutputFormat::Table;
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
                "--tikv-endpoints" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("--tikv-endpoints requires a value".to_string());
                    }
                    tikv_endpoints = Some(args[i].clone());
                }
                "--help" | "-h" => {
                    print_stats_help();
                    return Ok(None);
                }
                unknown => {
                    return Err(format!("unknown flag for stats: {}", unknown));
                }
            }
            i += 1;
        }

        Ok(Some(JobsCommand::Stats {
            format,
            tikv_endpoints,
        }))
    }

    /// Run the jobs command.
    pub async fn run(&self) -> Result<(), String> {
        match self {
            JobsCommand::List {
                status,
                limit,
                offset,
                format,
                tikv_endpoints,
            } => {
                self.run_list(*status, *limit, *offset, *format, tikv_endpoints)
                    .await
            }
            JobsCommand::Get {
                job_id,
                format,
                tikv_endpoints,
            } => self.run_get(job_id, *format, tikv_endpoints).await,
            JobsCommand::Retry {
                job_id,
                all_failed,
                tikv_endpoints,
            } => {
                self.run_retry(job_id.as_deref(), *all_failed, tikv_endpoints)
                    .await
            }
            JobsCommand::Cancel {
                job_id,
                tikv_endpoints,
            } => self.run_cancel(job_id, tikv_endpoints).await,
            JobsCommand::Delete {
                job_ids,
                completed,
                older_than,
                force,
                delete_checkpoint,
                tikv_endpoints,
            } => {
                self.run_delete(
                    job_ids,
                    *completed,
                    older_than.as_deref(),
                    *force,
                    *delete_checkpoint,
                    tikv_endpoints,
                )
                .await
            }
            JobsCommand::Stats {
                format,
                tikv_endpoints,
            } => self.run_stats(*format, tikv_endpoints).await,
        }
    }

    /// Run the list command.
    async fn run_list(
        &self,
        status_filter: Option<JobStatus>,
        limit: Option<u32>,
        offset: Option<u32>,
        format: OutputFormat,
        tikv_endpoints: &Option<String>,
    ) -> Result<(), String> {
        let tikv = create_tikv_client(tikv_endpoints).await?;

        let limit = limit.unwrap_or(50);
        let offset = offset.unwrap_or(0);

        // Scan all jobs
        let prefix = roboflow_distributed::tikv::key::JobKeys::prefix();
        let scan_limit = limit + offset + 100; // Fetch extra for offset

        let results = tikv
            .scan(prefix, scan_limit)
            .await
            .map_err(|e| format!("Failed to scan jobs: {}", e))?;

        let mut jobs: Vec<JobRecord> = results
            .into_iter()
            .filter_map(|(_key, value)| bincode::deserialize::<JobRecord>(&value).ok())
            .filter(|job| {
                if let Some(status) = status_filter {
                    job.status == status
                } else {
                    true
                }
            })
            .collect();

        // Sort by created_at (newest first)
        jobs.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        // Apply offset and limit
        let total = jobs.len();
        let jobs: Vec<_> = jobs
            .into_iter()
            .skip(offset as usize)
            .take(limit as usize)
            .collect();

        if format == OutputFormat::Json {
            println!("{}", serde_json::to_string_pretty(&jobs).unwrap());
        } else {
            println!("Showing {} of {} jobs", jobs.len(), total);
            print_job_table(&jobs, format);
        }

        Ok(())
    }

    /// Run the get command.
    async fn run_get(
        &self,
        job_id: &str,
        format: OutputFormat,
        tikv_endpoints: &Option<String>,
    ) -> Result<(), String> {
        let tikv = create_tikv_client(tikv_endpoints).await?;

        // Try to get the job
        let job = match tikv.get_job(job_id).await {
            Ok(Some(job)) => job,
            Ok(None) => {
                // Try computing hash from file path
                let hash = compute_file_hash(job_id, 0);
                match tikv.get_job(&hash).await {
                    Ok(Some(job)) => job,
                    Ok(None) => return Err(format!("Job not found: {}", job_id)),
                    Err(e) => return Err(format!("Failed to get job: {}", e)),
                }
            }
            Err(e) => return Err(format!("Failed to get job: {}", e)),
        };

        // Get checkpoint if exists
        let checkpoint = tikv.get_checkpoint(job_id).await.ok().flatten();

        if format == OutputFormat::Json {
            let output = JobDetail { job, checkpoint };
            println!("{}", serde_json::to_string_pretty(&output).unwrap());
        } else {
            print_job_detail(&job, checkpoint.as_ref());
        }

        Ok(())
    }

    /// Run the retry command.
    async fn run_retry(
        &self,
        job_id: Option<&str>,
        all_failed: bool,
        tikv_endpoints: &Option<String>,
    ) -> Result<(), String> {
        let tikv = create_tikv_client(tikv_endpoints).await?;

        if all_failed {
            // Get all failed/dead jobs
            let prefix = roboflow_distributed::tikv::key::JobKeys::prefix();
            let results = tikv
                .scan(prefix, 1000)
                .await
                .map_err(|e| format!("Failed to scan jobs: {}", e))?;

            let mut retried = 0;
            for (_key, value) in results {
                if let Ok(mut job) = bincode::deserialize::<JobRecord>(&value)
                    && job.status.is_failed()
                {
                    // Reset job to pending
                    job.status = JobStatus::Pending;
                    job.owner = None;
                    job.attempts = 0;
                    job.error = None;
                    job.updated_at = Utc::now();

                    tikv.put_job(&job)
                        .await
                        .map_err(|e| format!("Failed to update job: {}", e))?;
                    retried += 1;
                }
            }

            println!("Retried {} failed jobs", retried);
        } else if let Some(job_id) = job_id {
            // Get the job
            let mut job = match tikv.get_job(job_id).await {
                Ok(Some(job)) => job,
                Ok(None) => return Err(format!("Job not found: {}", job_id)),
                Err(e) => return Err(format!("Failed to get job: {}", e)),
            };

            // Check if job is failed
            if !job.status.is_failed() {
                return Err(format!(
                    "Job cannot be retried (status: {:?}). Only Failed or Dead jobs can be retried.",
                    job.status
                ));
            }

            // Reset job to pending
            job.status = JobStatus::Pending;
            job.owner = None;
            job.attempts = 0;
            job.error = None;
            job.updated_at = Utc::now();

            tikv.put_job(&job)
                .await
                .map_err(|e| format!("Failed to update job: {}", e))?;

            println!("Retried job: {}", job_id);
        } else {
            return Err("retry requires either a job ID or --all-failed".to_string());
        }

        Ok(())
    }

    /// Run the cancel command.
    async fn run_cancel(
        &self,
        job_id: &str,
        tikv_endpoints: &Option<String>,
    ) -> Result<(), String> {
        let tikv = create_tikv_client(tikv_endpoints).await?;

        // Get the job
        let mut job = match tikv.get_job(job_id).await {
            Ok(Some(job)) => job,
            Ok(None) => return Err(format!("Job not found: {}", job_id)),
            Err(e) => return Err(format!("Failed to get job: {}", e)),
        };

        match job.status {
            JobStatus::Pending => {
                // Delete pending job
                let key = roboflow_distributed::tikv::key::JobKeys::record(job_id);
                tikv.delete(key)
                    .await
                    .map_err(|e| format!("Failed to delete job: {}", e))?;
                println!("Cancelled pending job: {}", job_id);
            }
            JobStatus::Processing => {
                // Mark as cancelled - note: workers don't currently check for cancellation
                // during processing. The job will continue until completion or failure.
                // A future enhancement would add periodic status checks in workers.
                job.status = JobStatus::Cancelled;
                job.updated_at = Utc::now();

                tikv.put_job(&job)
                    .await
                    .map_err(|e| format!("Failed to update job: {}", e))?;
                println!(
                    "Marked job {} as Cancelled. Note: worker may still complete processing.",
                    job_id
                );
            }
            _ => {
                return Err(format!(
                    "Job cannot be cancelled (status: {:?}). Only Pending or Processing jobs can be cancelled.",
                    job.status
                ));
            }
        }

        Ok(())
    }

    /// Run the delete command.
    async fn run_delete(
        &self,
        job_ids: &[String],
        completed: bool,
        older_than: Option<&str>,
        force: bool,
        delete_checkpoint: bool,
        tikv_endpoints: &Option<String>,
    ) -> Result<(), String> {
        let tikv = create_tikv_client(tikv_endpoints).await?;

        let mut jobs_to_delete = Vec::new();

        // Add explicitly specified job IDs
        for job_id in job_ids {
            jobs_to_delete.push(job_id.clone());
        }

        // Add jobs matching filters
        if completed || older_than.is_some() {
            let prefix = roboflow_distributed::tikv::key::JobKeys::prefix();
            let results = tikv
                .scan(prefix, 1000)
                .await
                .map_err(|e| format!("Failed to scan jobs: {}", e))?;

            let cutoff_time = if let Some(duration_str) = older_than {
                parse_duration(duration_str)?
            } else {
                Utc::now()
            };

            for (_key, value) in results {
                if let Ok(job) = bincode::deserialize::<JobRecord>(&value) {
                    let matches = if completed && older_than.is_some() {
                        job.status == JobStatus::Completed && job.created_at < cutoff_time
                    } else if completed {
                        job.status == JobStatus::Completed
                    } else {
                        job.created_at < cutoff_time
                    };

                    if matches {
                        jobs_to_delete.push(job.id.clone());
                    }
                }
            }
        }

        if jobs_to_delete.is_empty() {
            println!("No jobs to delete");
            return Ok(());
        }

        // Confirm unless force
        if !force {
            println!(
                "This will delete {} job(s). Continue? [y/N]",
                jobs_to_delete.len()
            );
            let mut input = String::new();
            std::io::stdin()
                .read_line(&mut input)
                .map_err(|e| format!("Failed to read input: {}", e))?;
            if !input.trim().to_lowercase().starts_with('y') {
                println!("Cancelled");
                return Ok(());
            }
        }

        // Delete each job
        let mut deleted = 0;
        for job_id in &jobs_to_delete {
            // Delete job record
            let key = roboflow_distributed::tikv::key::JobKeys::record(job_id);
            if tikv.delete(key).await.is_ok() {
                deleted += 1;
            }

            // Delete checkpoint if requested
            if delete_checkpoint {
                let checkpoint_key = roboflow_distributed::tikv::key::StateKeys::checkpoint(job_id);
                let _ = tikv.delete(checkpoint_key).await;
            }
        }

        println!("Deleted {} job(s)", deleted);

        Ok(())
    }

    /// Run the stats command.
    async fn run_stats(
        &self,
        format: OutputFormat,
        tikv_endpoints: &Option<String>,
    ) -> Result<(), String> {
        let tikv = create_tikv_client(tikv_endpoints).await?;

        // Scan all jobs
        let prefix = roboflow_distributed::tikv::key::JobKeys::prefix();
        let results = tikv
            .scan(prefix, 10000)
            .await
            .map_err(|e| format!("Failed to scan jobs: {}", e))?;

        let mut stats = JobStatistics::default();

        for (_key, value) in results {
            if let Ok(job) = bincode::deserialize::<JobRecord>(&value) {
                stats.total += 1;
                stats.source_bytes += job.source_size;

                match job.status {
                    JobStatus::Pending => stats.pending += 1,
                    JobStatus::Processing => stats.processing += 1,
                    JobStatus::Completed => {
                        stats.completed += 1;
                        stats.processed_bytes += job.source_size;
                    }
                    JobStatus::Failed => stats.failed += 1,
                    JobStatus::Dead => stats.dead += 1,
                    JobStatus::Cancelled => stats.cancelled += 1,
                }
            }
        }

        if format == OutputFormat::Json {
            println!("{}", serde_json::to_string_pretty(&stats).unwrap());
        } else {
            print_job_statistics(&stats);
        }

        Ok(())
    }
}

/// Create a TiKV client from endpoints or environment.
async fn create_tikv_client(tikv_endpoints: &Option<String>) -> Result<TikvClient, String> {
    if let Some(endpoints) = tikv_endpoints {
        TikvClient::new(roboflow_distributed::TikvConfig::with_pd_endpoints(
            endpoints,
        ))
        .await
        .map_err(|e| format!("Failed to connect to TiKV: {}", e))
    } else {
        TikvClient::from_env()
            .await
            .map_err(|e| format!("Failed to connect to TiKV: {}", e))
    }
}

/// Parse a job status string.
fn parse_job_status(s: &str) -> Option<JobStatus> {
    match s.to_lowercase().as_str() {
        "pending" => Some(JobStatus::Pending),
        "processing" => Some(JobStatus::Processing),
        "completed" => Some(JobStatus::Completed),
        "failed" => Some(JobStatus::Failed),
        "dead" => Some(JobStatus::Dead),
        "cancelled" | "canceled" => Some(JobStatus::Cancelled),
        _ => None,
    }
}

/// Parse a duration string (e.g., "7d", "24h", "60m") into a DateTime.
fn parse_duration(s: &str) -> Result<DateTime<Utc>, String> {
    let s = s.trim().to_lowercase();
    let (num_str, unit) = if let Some(pos) = s.find(|c: char| !c.is_numeric()) {
        (&s[..pos], &s[pos..])
    } else {
        return Err(format!("Invalid duration: {}", s));
    };

    let num: i64 = num_str
        .parse()
        .map_err(|_| format!("Invalid duration number: {}", num_str))?;

    let duration = match unit {
        "s" | "sec" | "second" | "seconds" => Duration::seconds(num),
        "m" | "min" | "minute" | "minutes" => Duration::minutes(num),
        "h" | "hour" | "hours" => Duration::hours(num),
        "d" | "day" | "days" => Duration::days(num),
        "w" | "week" | "weeks" => Duration::weeks(num),
        _ => return Err(format!("Invalid duration unit: {}", unit)),
    };

    Ok(Utc::now() - duration)
}

/// Print jobs in table format.
pub fn print_job_table(jobs: &[JobRecord], format: OutputFormat) {
    if jobs.is_empty() {
        println!("No jobs found");
        return;
    }

    match format {
        OutputFormat::Table => {
            // Calculate column widths
            let id_width = jobs.iter().map(|j| j.id.len()).max().unwrap_or(16).min(20);
            let status_width = 12;
            let owner_width = 12;

            println!(
                "{:<id_width$} {:<status_width$} {:<owner_width$} {:>10} CREATED",
                "ID", "STATUS", "OWNER", "ATTEMPTS"
            );

            for job in jobs {
                let status_str = format_status(job.status);
                let owner = job.owner.as_deref().unwrap_or("-");
                println!(
                    "{:<id_width$} {:<status_width$} {:<owner_width$} {:>10} {}",
                    &job.id[..job.id.len().min(id_width)],
                    status_str,
                    &owner[..owner.len().min(owner_width)],
                    job.attempts,
                    job.created_at.format("%Y-%m-%d %H:%M:%S")
                );
            }
        }
        OutputFormat::Csv => {
            println!("id,status,owner,attempts,created,source_bucket,source_key,output_prefix");
            for job in jobs {
                let owner = job.owner.as_deref().unwrap_or("");
                let created = job.created_at.to_rfc3339();
                println!(
                    "{},{},{},{},{},{},{},{}",
                    job.id,
                    format_status_csv(job.status),
                    owner,
                    job.attempts,
                    created,
                    job.source_bucket,
                    job.source_key,
                    job.output_prefix
                );
            }
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&jobs).unwrap());
        }
    }
}

/// Print a single job's details.
fn print_job_detail(job: &JobRecord, checkpoint: Option<&roboflow_distributed::CheckpointState>) {
    println!("Job ID:        {}", job.id);
    println!("Status:        {}", format_status(job.status));
    println!("Source:        {}/{}", job.source_bucket, job.source_key);
    println!("Source Size:   {} bytes", job.source_size);
    println!("Output:        {}", job.output_prefix);
    println!("Config Hash:   {}", job.config_hash);
    println!("Attempts:      {}/{}", job.attempts, job.max_attempts);
    println!("Owner:         {}", job.owner.as_deref().unwrap_or("-"));
    println!(
        "Created:       {}",
        job.created_at.format("%Y-%m-%d %H:%M:%S UTC")
    );
    println!(
        "Updated:       {}",
        job.updated_at.format("%Y-%m-%d %H:%M:%S UTC")
    );

    if let Some(error) = &job.error {
        println!("Error:         {}", error);
    }

    if let Some(cp) = checkpoint {
        println!();
        println!("Checkpoint:");
        println!("  Frame:         {} / {}", cp.last_frame, cp.total_frames);
        println!("  Byte Offset:   {}", cp.byte_offset);
        println!("  Episode:       {}", cp.episode_idx);
        println!("  Progress:      {:.1}%", cp.progress_percent());
        println!(
            "  Updated:       {}",
            cp.updated_at.format("%Y-%m-%d %H:%M:%S UTC")
        );
    }
}

/// Print job statistics.
fn print_job_statistics(stats: &JobStatistics) {
    println!("Job Statistics:");
    println!();
    println!("  Total Jobs:     {}", stats.total);
    println!("  Pending:        {}", stats.pending);
    println!("  Processing:     {}", stats.processing);
    println!("  Completed:      {}", stats.completed);
    println!("  Failed:         {}", stats.failed);
    println!("  Dead:           {}", stats.dead);
    println!("  Cancelled:      {}", stats.cancelled);
    println!();
    println!("  Total Bytes:    {}", stats.source_bytes);
    println!("  Processed:      {}", stats.processed_bytes);

    if stats.total > 0 {
        let success_rate = (stats.completed as f64 / stats.total as f64) * 100.0;
        println!("  Success Rate:   {:.1}%", success_rate);
    }
}

/// Format job status with colors.
fn format_status(status: JobStatus) -> String {
    match status {
        JobStatus::Pending => "\x1b[33mPending\x1b[0m".to_string(),
        JobStatus::Processing => "\x1b[34mProcessing\x1b[0m".to_string(),
        JobStatus::Completed => "\x1b[32mCompleted\x1b[0m".to_string(),
        JobStatus::Failed => "\x1b[31mFailed\x1b[0m".to_string(),
        JobStatus::Dead => "\x1b[31mDead\x1b[0m".to_string(),
        JobStatus::Cancelled => "\x1b[33mCancelled\x1b[0m".to_string(),
    }
}

/// Format job status for CSV (no colors).
fn format_status_csv(status: JobStatus) -> String {
    match status {
        JobStatus::Pending => "Pending".to_string(),
        JobStatus::Processing => "Processing".to_string(),
        JobStatus::Completed => "Completed".to_string(),
        JobStatus::Failed => "Failed".to_string(),
        JobStatus::Dead => "Dead".to_string(),
        JobStatus::Cancelled => "Cancelled".to_string(),
    }
}

/// Print job output in the specified format.
pub fn print_job_output(jobs: &[JobRecord], format: OutputFormat) {
    print_job_table(jobs, format);
}

/// Job detail with checkpoint for JSON output.
#[derive(Serialize)]
struct JobDetail {
    job: JobRecord,
    #[serde(skip_serializing_if = "Option::is_none")]
    checkpoint: Option<roboflow_distributed::CheckpointState>,
}

/// Job statistics.
#[derive(Default, Debug, Serialize)]
struct JobStatistics {
    total: usize,
    pending: usize,
    processing: usize,
    completed: usize,
    failed: usize,
    dead: usize,
    cancelled: usize,
    source_bytes: u64,
    processed_bytes: u64,
}

/// Run the jobs command from raw args.
pub async fn run_jobs_command(args: &[String]) -> Result<(), String> {
    let cmd = JobsCommand::parse(args)?;
    match cmd {
        Some(command) => command.run().await,
        None => Ok(()), // Help was printed
    }
}

/// Add Cancelled status to JobStatus (from schema)
/// This is needed since the schema doesn't include Cancelled yet
#[allow(dead_code)]
fn get_jobs_help_summary() -> String {
    r#"Available commands: list, get, retry, cancel, delete, stats

Run 'roboflow jobs <command> --help' for more information."#
        .to_string()
}

fn print_jobs_help() {
    println!(
        r#"Manage jobs in the distributed processing queue.

USAGE:
    roboflow jobs <COMMAND> [OPTIONS]

COMMANDS:
    list        List jobs with optional filtering
    get         Get detailed information about a job
    retry       Retry failed jobs
    cancel      Cancel a pending or processing job
    delete      Delete jobs and optionally checkpoints
    stats       Show job statistics

OPTIONS:
    -h, --help    Print this help

Run 'roboflow jobs <command> --help' for more information on a specific command.
"#
    );
}

fn print_list_help() {
    println!(
        r#"List jobs in the distributed queue.

USAGE:
    roboflow jobs list [OPTIONS]

OPTIONS:
    -s, --status <STATUS>     Filter by status
                             (pending|processing|completed|failed|dead|cancelled)
    -l, --limit <N>          Maximum number of jobs to show (default: 50)
    -o, --offset <N>         Skip N jobs before showing results
        --json               Output in JSON format
        --csv                Output in CSV format
        --tikv-endpoints <ADDRS>
                             TiKV PD endpoints (default: $TIKV_PD_ENDPOINTS)
    -h, --help               Print this help

EXAMPLES:
    # List all pending jobs
    roboflow jobs list --status pending

    # List last 20 completed jobs
    roboflow jobs list --status completed --limit 20

    # List jobs in JSON format
    roboflow jobs list --json
"#
    );
}

fn print_get_help() {
    println!(
        r#"Get detailed information about a job.

USAGE:
    roboflow jobs get <JOB-ID> [OPTIONS]

ARGUMENTS:
    <JOB-ID>    Job ID or file hash to query

OPTIONS:
        --json               Output in JSON format
        --csv                Output in CSV format
        --tikv-endpoints <ADDRS>
                             TiKV PD endpoints (default: $TIKV_PD_ENDPOINTS)
    -h, --help               Print this help

EXAMPLES:
    # Get job details
    roboflow jobs get abc123def456

    # Get job details as JSON
    roboflow jobs get abc123def456 --json
"#
    );
}

fn print_retry_help() {
    println!(
        r#"Retry failed jobs.

USAGE:
    roboflow jobs retry [JOB-ID] [OPTIONS]

ARGUMENTS:
    [JOB-ID]    Job ID to retry (or use --all-failed)

OPTIONS:
        --all-failed           Retry all failed and dead jobs
        --tikv-endpoints <ADDRS>
                               TiKV PD endpoints (default: $TIKV_PD_ENDPOINTS)
    -h, --help                  Print this help

EXAMPLES:
    # Retry a specific failed job
    roboflow jobs retry abc123def456

    # Retry all failed jobs
    roboflow jobs retry --all-failed
"#
    );
}

fn print_cancel_help() {
    println!(
        r#"Cancel a job.

USAGE:
    roboflow jobs cancel <JOB-ID> [OPTIONS]

ARGUMENTS:
    <JOB-ID>    Job ID to cancel

OPTIONS:
        --tikv-endpoints <ADDRS>
                             TiKV PD endpoints (default: $TIKV_PD_ENDPOINTS)
    -h, --help               Print this help

EXAMPLES:
    # Cancel a job
    roboflow jobs cancel abc123def456

NOTE:
    - Pending jobs will be deleted
    - Processing jobs will be marked as Cancelled (workers check this status)
"#
    );
}

fn print_delete_help() {
    println!(
        r#"Delete jobs from the queue.

USAGE:
    roboflow jobs delete [JOB-IDs] [OPTIONS]

ARGUMENTS:
    [JOB-IDs]   One or more job IDs to delete

OPTIONS:
        --completed           Delete all completed jobs
        --older-than <DUR>    Delete jobs older than duration (e.g., 7d, 24h)
        --keep-checkpoint     Don't delete associated checkpoints
        --force               Skip confirmation prompt
        --tikv-endpoints <ADDRS>
                             TiKV PD endpoints (default: $TIKV_PD_ENDPOINTS)
    -h, --help               Print this help

EXAMPLES:
    # Delete a specific job
    roboflow jobs delete abc123def456

    # Delete all completed jobs older than 7 days
    roboflow jobs delete --completed --older-than 7d

    # Delete without confirmation
    roboflow jobs delete abc123def456 --force
"#
    );
}

fn print_stats_help() {
    println!(
        r#"Show job statistics.

USAGE:
    roboflow jobs stats [OPTIONS]

OPTIONS:
        --json               Output in JSON format
        --csv                Output in CSV format
        --tikv-endpoints <ADDRS>
                             TiKV PD endpoints (default: $TIKV_PD_ENDPOINTS)
    -h, --help               Print this help

EXAMPLES:
    # Show job statistics
    roboflow jobs stats

    # Show statistics as JSON
    roboflow jobs stats --json
"#
    );
}
