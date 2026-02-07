// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Jobs command for managing distributed jobs.
//!
//! ## Deprecation Notice
//!
//! The job-based processing system has been replaced by a batch/WorkUnit-based system.
//! Use `roboflow batch` commands instead for managing distributed file processing.
//!
//! ## Legacy Usage
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

/// Error message for deprecated job commands.
const DEPRECATED_ERROR: &str = "Error: The 'jobs' command has been deprecated.
Please use the 'batch' command instead:
  - Use 'roboflow batch submit' to submit new batch jobs
  - Use 'roboflow batch list' to list batch jobs
  - Use 'roboflow batch status <batch-id>' to check batch status
  - Use 'roboflow batch cancel <batch-id>' to cancel batch jobs

The job-based system has been replaced by WorkUnit-based batch processing.";

/// Run the jobs command from raw args.
pub async fn run_jobs_command(args: &[String]) -> Result<(), String> {
    if args.is_empty() || args[0] == "-h" || args[0] == "--help" {
        print_jobs_help();
        return Ok(());
    }

    // Return deprecation error for all subcommands
    Err(DEPRECATED_ERROR.to_string())
}

fn print_jobs_help() {
    println!(
        r#"Manage jobs in the distributed processing queue.

DEPRECATION NOTICE: The 'jobs' command has been deprecated.
Please use 'roboflow batch' commands instead.

USAGE:
    roboflow jobs <COMMAND> [OPTIONS]

COMMANDS:
    list        List jobs with optional filtering (DEPRECATED - use 'roboflow batch list')
    get         Get detailed information about a job (DEPRECATED - use 'roboflow batch status')
    retry       Retry failed jobs (DEPRECATED - use 'roboflow batch' to resubmit)
    cancel      Cancel a pending or processing job (DEPRECATED - use 'roboflow batch cancel')
    delete      Delete jobs and optionally checkpoints (DEPRECATED)
    stats       Show job statistics (DEPRECATED - use 'roboflow batch list')

OPTIONS:
    -h, --help    Print this help

Run 'roboflow batch --help' for more information on the new batch commands.
"#
    );
}
