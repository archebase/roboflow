// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Roboflow CLI - unified distributed data processing pipeline.
//!
//! This binary provides a unified interface for all pipeline operations:
//!
//! ## Subcommands
//!
//! - `submit` - Submit jobs to the distributed queue
//! - `jobs` - Manage jobs (list, get, retry, cancel, delete, stats)
//! - `batch` - Manage batch jobs
//! - `run` - Run the unified service (worker + finalizer + reaper in one)
//!
//! ## Environment Variables
//!
//! ### TiKV Configuration
//! - `TIKV_PD_ENDPOINTS` - PD endpoints (default: 127.0.0.1:2379)
//! - `TIKV_CONNECTION_TIMEOUT_SECS` - Connection timeout (default: 10)
//! - `TIKV_OPERATION_TIMEOUT_SECS` - Operation timeout (default: 30)
//!
//! ### Storage Configuration
//! - `OSS_ACCESS_KEY_ID` - Alibaba OSS access key
//! - `OSS_ACCESS_KEY_SECRET` - Alibaba OSS secret key
//! - `OSS_ENDPOINT` - Alibaba OSS endpoint
//! - `AWS_ACCESS_KEY_ID` - AWS access key
//! - `AWS_SECRET_ACCESS_KEY` - AWS secret key
//! - `AWS_REGION` - AWS region
//!
//! ### Role Configuration (for `run` command)
//! - `ROLE` - Role to run: `worker`, `finalizer`, or `unified` (default)
//! - `POD_ID` - Unique identifier for this instance
//!
//! ### Worker Configuration (ROLE=worker or unified)
//! - `WORKER_POLL_INTERVAL_SECS` - Job poll interval (default: 5)
//! - `WORKER_MAX_CONCURRENT_JOBS` - Max concurrent jobs (default: 1)
//! - `WORKER_MAX_ATTEMPTS` - Max attempts per job (default: 3)
//! - `WORKER_JOB_TIMEOUT_SECS` - Job timeout (default: 3600)
//! - `WORKER_HEARTBEAT_INTERVAL_SECS` - Heartbeat interval (default: 30)
//! - `WORKER_CHECKPOINT_INTERVAL_FRAMES` - Checkpoint interval in frames (default: 100)
//! - `WORKER_CHECKPOINT_INTERVAL_SECS` - Checkpoint interval in seconds (default: 10)
//! - `WORKER_OUTPUT_PREFIX` - Fallback output prefix (default: output/) - only used if Batch output_path is empty
//! - `SOURCE_INIT_TIMEOUT_SECS` - Source initialization timeout (default: 300)
//!
//! ### Finalizer Configuration (ROLE=finalizer or unified)
//! - `FINALIZER_POLL_INTERVAL_SECS` - Poll interval for completed batches (default: 30)
//! - `FINALIZER_MERGE_TIMEOUT_SECS` - Merge operation timeout (default: 600)

use std::env;
use std::sync::Arc;

use futures::future::join_all;
use roboflow_distributed::{
    BatchController, Finalizer, FinalizerConfig, MergeCoordinator, ReaperConfig, Scanner,
    ScannerConfig, Worker, WorkerConfig, ZombieReaper,
};
use roboflow_storage::StorageFactory;
use tokio_util::sync::CancellationToken;

// Include CLI commands module
mod commands;

// =============================================================================
// Role Types
// =============================================================================

/// Runtime role for the unified service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Worker only - processes work units
    Worker,
    /// Finalizer only - merges completed batches
    Finalizer,
    /// Unified - runs all roles (worker, finalizer, reaper)
    Unified,
}

impl Role {
    /// Parse role from environment variable or string.
    pub fn try_from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "worker" => Some(Role::Worker),
            "finalizer" => Some(Role::Finalizer),
            "unified" => Some(Role::Unified),
            _ => None,
        }
    }

    /// Get role from ROLE env var, default to Unified.
    pub fn from_env() -> Self {
        env::var("ROLE")
            .ok()
            .and_then(|s| Self::try_from_str(&s))
            .unwrap_or(Role::Unified)
    }
}

/// CLI command.
enum Command {
    /// Submit jobs to the distributed queue
    Submit {
        /// Remaining arguments for submit command
        args: Vec<String>,
    },
    /// Manage jobs
    Jobs {
        /// Remaining arguments for jobs command
        args: Vec<String>,
    },
    /// Manage batch jobs
    Batch {
        /// Remaining arguments for batch command
        args: Vec<String>,
    },
    /// Run the unified service (worker + finalizer + reaper)
    Run {
        /// Role to run (worker, finalizer, unified)
        role: Option<String>,
        /// Pod ID for this instance
        pod_id: Option<String>,
    },
    /// Run a standalone health check server
    Health {
        /// Host to bind to
        host: Option<String>,
        /// Port to bind to
        port: Option<u16>,
    },
}

/// Get help text for the run command.
fn get_run_help() -> String {
    [
        "Run the unified service (worker + finalizer + reaper + scanner)",
        "",
        "USAGE:",
        "    roboflow run [OPTIONS]",
        "",
        "OPTIONS:",
        "    -r, --role <ROLE>      Role to run: worker, finalizer, unified [default: unified]",
        "    -p, --pod-id <ID>      Pod ID for this instance [default: auto-generated]",
        "    -h, --help             Print help information",
        "",
        "ENVIRONMENT VARIABLES:",
        "    ROLE                    Role to run (worker, finalizer, unified)",
        "    POD_ID                  Pod ID for this instance",
        "    TIKV_PD_ENDPOINTS        TiKV PD endpoints [default: 127.0.0.1:2379]",
        "",
        "ROLES:",
        "    unified    Run all components (scanner, worker, finalizer, reaper) [default]",
        "    worker     Run job processing only",
        "    finalizer  Run batch finalization and merge only",
        "",
        "EXAMPLES:",
        "    # Run unified service (all roles)",
        "    roboflow run",
        "",
        "    # Run as worker only",
        "    roboflow run --role worker",
        "",
        "    # Run as finalizer only",
        "    roboflow run --role finalizer",
        "",
        "    # Run with custom pod ID",
        "    roboflow run --pod-id my-pod-1",
    ]
    .join("\n")
}

/// Parse command-line arguments.
fn parse_args(args: &[String]) -> Result<Command, String> {
    if args.len() < 2 {
        return usage();
    }

    let command = &args[1];

    match command.as_str() {
        "submit" => {
            let submit_args: Vec<String> = args[2..].to_vec();
            Ok(Command::Submit { args: submit_args })
        }
        "jobs" => {
            let jobs_args: Vec<String> = args[2..].to_vec();
            Ok(Command::Jobs { args: jobs_args })
        }
        "batch" => {
            let batch_args: Vec<String> = args[2..].to_vec();
            Ok(Command::Batch { args: batch_args })
        }
        "run" => {
            let mut role = None;
            let mut pod_id = None;

            let mut i = 2;
            while i < args.len() {
                match args[i].as_str() {
                    "--role" | "-r" => {
                        i += 1;
                        if i >= args.len() {
                            return Err("--role requires a value".to_string());
                        }
                        role = Some(args[i].clone());
                    }
                    "--pod-id" | "-p" => {
                        i += 1;
                        if i >= args.len() {
                            return Err("--pod-id requires a value".to_string());
                        }
                        pod_id = Some(args[i].clone());
                    }
                    "--help" | "-h" => {
                        eprintln!("{}", get_run_help());
                        std::process::exit(0);
                    }
                    unknown => {
                        return Err(format!("unknown flag for run: {}", unknown));
                    }
                }
                i += 1;
            }

            Ok(Command::Run { role, pod_id })
        }
        "health" => {
            let mut host = None;
            let mut port = None;

            let mut i = 2;
            while i < args.len() {
                match args[i].as_str() {
                    "--host" => {
                        i += 1;
                        if i >= args.len() {
                            return Err("--host requires a value".to_string());
                        }
                        host = Some(args[i].clone());
                    }
                    "--port" => {
                        i += 1;
                        if i >= args.len() {
                            return Err("--port requires a value".to_string());
                        }
                        port = args[i].parse().ok();
                        if port.is_none() {
                            return Err(format!("invalid port: {}", args[i]));
                        }
                    }
                    "--help" | "-h" => {
                        return Ok(Command::Health {
                            host: None,
                            port: None,
                        });
                    }
                    unknown => {
                        return Err(format!("unknown flag for health: {}", unknown));
                    }
                }
                i += 1;
            }

            Ok(Command::Health { host, port })
        }
        "--help" | "-h" | "help" => usage(),
        unknown => {
            eprintln!("unknown command: {}\n", unknown);
            eprintln!("{}", get_help());
            Err("".to_string())
        }
    }
}

/// Print usage information and return an error.
fn usage() -> Result<Command, String> {
    eprintln!("{}", get_help());
    Err("".to_string())
}

/// Get help text.
fn get_help() -> String {
    [
        "Roboflow - Distributed robot data transformation pipeline",
        "",
        "USAGE:",
        "    roboflow <COMMAND> [OPTIONS]",
        "",
        "COMMANDS:",
        "    submit      Submit a conversion job to the distributed queue",
        "    jobs        Manage and inspect jobs",
        "    batch       Manage batch jobs",
        "    run         Run the unified service (worker + finalizer + reaper)",
        "    health      Run a standalone health check server",
        "    help        Print this help message",
        "",
        "RUN COMMAND:",
        "    roboflow run [OPTIONS]",
        "",
        "OPTIONS:",
        "    -r, --role <ROLE>      Role to run: worker, finalizer, unified [default: unified]",
        "    -p, --pod-id <ID>      Pod ID for this instance [default: auto-generated]",
        "    -h, --help             Print help information",
        "",
        "ENVIRONMENT VARIABLES:",
        "    ROLE                    Role to run (worker, finalizer, unified)",
        "    POD_ID                  Pod ID for this instance",
        "    TIKV_PD_ENDPOINTS        TiKV PD endpoints [default: 127.0.0.1:2379]",
        "",
        "EXAMPLES:",
        "    # Run unified service (all roles)",
        "    roboflow run",
        "",
        "    # Run as worker only",
        "    roboflow run --role worker",
        "",
        "    # Submit a job",
        "    roboflow submit s3://bucket/input.bag --output s3://bucket/output/",
        "",
        "    # List jobs",
        "    roboflow jobs list",
    ]
    .join("\n")
}

/// Generate a pod ID from environment or hostname + UUID.
fn generate_pod_id(prefix: &str) -> String {
    match env::var("POD_NAME") {
        Ok(name) => name,
        Err(_) => {
            let hostname = hostname::get()
                .map(|h| h.to_string_lossy().to_string())
                .unwrap_or_else(|_| "unknown".to_string());
            format!("{}-{}", prefix, hostname)
        }
    }
}

/// Create TiKV client from environment.
async fn create_tikv() -> Result<roboflow_distributed::TikvClient, String> {
    use roboflow_distributed::TikvConfig;

    let config = TikvConfig::default();

    roboflow_distributed::TikvClient::new(config)
        .await
        .map_err(|e| format!("Failed to create TiKV client: {}", e))
}

/// Create storage factory from environment.
fn create_storage_factory() -> StorageFactory {
    StorageFactory::from_env()
}

/// Create storage from environment.
fn create_storage() -> Result<Arc<dyn roboflow_storage::Storage>, String> {
    let storage_factory = create_storage_factory();

    // Determine storage URL from environment or use local
    let storage_url = if let Ok(endpoint) = env::var("AWS_S3_ENDPOINT") {
        let bucket = env::var("AWS_S3_BUCKET").unwrap_or_default();
        if !bucket.is_empty() {
            format!("s3://{}@{}", bucket, endpoint)
        } else {
            "file://./data".to_string()
        }
    } else if let Ok(_endpoint) = env::var("OSS_ENDPOINT") {
        let bucket = env::var("OSS_BUCKET").unwrap_or_default();
        if !bucket.is_empty() {
            format!("oss://{}", bucket)
        } else {
            "file://./data".to_string()
        }
    } else {
        "file://./data".to_string()
    };

    storage_factory
        .create(&storage_url)
        .map_err(|e| format!("Failed to create storage: {}", e))
}

/// Health check result.
#[derive(Debug)]
struct HealthCheckResult {
    /// Whether the system is healthy.
    is_healthy: bool,
    /// List of errors found (if unhealthy).
    errors: Vec<String>,
    /// Optional details about the health check.
    details: Option<std::collections::HashMap<String, String>>,
}

/// Run health checks against TiKV and storage.
async fn run_health_check() -> HealthCheckResult {
    let mut errors = Vec::new();
    let mut details = std::collections::HashMap::new();

    // Add PD endpoints info
    details.insert(
        "pd_endpoints".to_string(),
        env::var("TIKV_PD_ENDPOINTS").unwrap_or_default(),
    );

    // Check TiKV connectivity
    match create_tikv().await {
        Ok(tikv) => {
            // Verify TiKV is responsive with a simple scan operation
            match tikv.scan(vec![], 1).await {
                Ok(_) => {
                    details.insert("tikv".to_string(), "connected".to_string());
                }
                Err(e) => {
                    errors.push(format!("TiKV scan failed: {}", e));
                    details.insert("tikv".to_string(), "unresponsive".to_string());
                }
            }
        }
        Err(e) => {
            errors.push(format!("TiKV connection: {}", e));
            details.insert("tikv".to_string(), "failed".to_string());
        }
    }

    // Check storage connectivity
    match create_storage() {
        Ok(_) => {
            details.insert("storage".to_string(), "available".to_string());
        }
        Err(e) => {
            errors.push(format!("Storage: {}", e));
            details.insert("storage".to_string(), "failed".to_string());
        }
    }

    HealthCheckResult {
        is_healthy: errors.is_empty(),
        errors,
        details: Some(details),
    }
}

struct CliWorkProcessor {
    pod_id: String,
    tikv: Arc<roboflow_distributed::TikvClient>,
    merge_coordinator: Arc<roboflow_distributed::MergeCoordinator>,
    config: WorkerConfig,
}

impl CliWorkProcessor {
    fn new(
        pod_id: String,
        tikv: Arc<roboflow_distributed::TikvClient>,
        merge_coordinator: Arc<roboflow_distributed::MergeCoordinator>,
        config: WorkerConfig,
    ) -> Self {
        Self {
            pod_id,
            tikv,
            merge_coordinator,
            config,
        }
    }
}

#[async_trait::async_trait]
impl roboflow_distributed::worker::WorkProcessor for CliWorkProcessor {
    async fn process(
        &self,
        work_unit: &roboflow_distributed::WorkUnit,
    ) -> Result<roboflow_distributed::ProcessingResult, roboflow_distributed::TikvError> {
        use roboflow_dataset::formats::common::DatasetBaseConfig;
        use roboflow_dataset::formats::common::config::MappingType;
        use roboflow_dataset::formats::lerobot::{
            DatasetConfig, LerobotConfig, LerobotWriterConfig, VideoConfig, create_lerobot_writer,
        };
        use roboflow_dataset::sources::{SourceConfig, create_source};
        use roboflow_distributed::EpisodeAllocator;
        use roboflow_pipeline::{DatasetPipelineConfig, DatasetPipelineExecutor};

        // 1. Determine output mode (S3 vs local) from work_unit.output_path
        let is_cloud = work_unit.output_path.starts_with("s3://")
            || work_unit.output_path.starts_with("oss://");

        let input_file = work_unit
            .files
            .first()
            .map(|f| f.url.clone())
            .ok_or_else(|| roboflow_distributed::TikvError::Other("No input files".to_string()))?;

        tracing::info!(
            batch_id = %work_unit.batch_id,
            unit_id = %work_unit.id,
            input_file = %input_file,
            is_cloud = is_cloud,
            "CliWorkProcessor: starting to process work unit"
        );

        // 2. Create local temp directory for processing
        let local_temp = std::env::temp_dir().join(format!(
            "roboflow_worker_{}_{}",
            self.pod_id,
            work_unit.id.replace(|c: char| !c.is_alphanumeric(), "_")
        ));

        let output_dir = if is_cloud {
            local_temp.clone()
        } else {
            std::path::PathBuf::from(&work_unit.output_path)
        };

        // Create temp directory if it doesn't exist
        if is_cloud {
            std::fs::create_dir_all(&local_temp).map_err(|e| {
                roboflow_distributed::TikvError::Other(format!("Temp dir error: {e}"))
            })?;
        }

        let allocator = roboflow_distributed::TiKVEpisodeAllocator::new(
            self.tikv.clone(),
            work_unit.batch_id.clone(),
            self.config.episodes_per_chunk,
        );

        let allocation = allocator.allocate().await.map_err(|e| {
            roboflow_distributed::TikvError::Other(format!("Allocation failed: {e}"))
        })?;
        tracing::debug!(
            batch_id = %work_unit.batch_id,
            unit_id = %work_unit.id,
            episode_index = allocation.episode_index,
            "Episode allocated"
        );

        // Load lerobot config FIRST to get topic mappings for the source
        let lerobot_config = match self.tikv.get_config(&work_unit.config_hash).await {
            Ok(Some(config_record)) => LerobotConfig::from_toml(&config_record.content)
                .unwrap_or_else(|_| LerobotConfig {
                    dataset: DatasetConfig {
                        base: DatasetBaseConfig {
                            name: format!("episode_{:06}", allocation.episode_index),
                            fps: 30,
                            robot_type: None,
                        },
                        env_type: None,
                    },
                    mappings: vec![],
                    video: VideoConfig::default(),
                    annotation_file: None,
                    flushing: Default::default(),
                    streaming: Default::default(),
                }),
            _ => LerobotConfig {
                dataset: DatasetConfig {
                    base: DatasetBaseConfig {
                        name: format!("episode_{:06}", allocation.episode_index),
                        fps: 30,
                        robot_type: None,
                    },
                    env_type: None,
                },
                mappings: vec![],
                video: VideoConfig::default(),
                annotation_file: None,
                flushing: Default::default(),
                streaming: Default::default(),
            },
        };

        // Extract image and state topics from mappings for frame alignment
        let image_topics: Vec<String> = lerobot_config
            .mappings
            .iter()
            .filter(|m| m.mapping_type == MappingType::Image)
            .map(|m| m.topic.clone())
            .collect();
        let state_topics: Vec<String> = lerobot_config
            .mappings
            .iter()
            .filter(|m| m.mapping_type == MappingType::State)
            .map(|m| m.topic.clone())
            .collect();

        tracing::debug!(
            batch_id = %work_unit.batch_id,
            unit_id = %work_unit.id,
            image_topics = ?image_topics,
            state_topics = ?state_topics,
            "Extracted topics from config"
        );

        let source_config = if input_file.ends_with(".mcap") {
            SourceConfig::mcap(&input_file)
        } else if input_file.ends_with(".bag") {
            let mut config = SourceConfig::bag(&input_file);
            // Pass topics to the bag source for frame alignment
            if !image_topics.is_empty() {
                config = config.with_option("image_topics", serde_json::json!(image_topics));
            }
            if !state_topics.is_empty() {
                config = config.with_option("state_topics", serde_json::json!(state_topics));
            }
            config = config.with_option("fps", serde_json::json!(lerobot_config.dataset.fps));
            config
        } else {
            return Err(roboflow_distributed::TikvError::Other(format!(
                "Unsupported input source: {input_file}"
            )));
        };

        tracing::debug!(
            batch_id = %work_unit.batch_id,
            unit_id = %work_unit.id,
            source_type = if input_file.ends_with(".mcap") { "mcap" } else { "bag" },
            "Creating source"
        );

        let mut source = create_source(&source_config)
            .map_err(|e| roboflow_distributed::TikvError::Other(format!("Source error: {e}")))?;

        tracing::debug!(
            batch_id = %work_unit.batch_id,
            unit_id = %work_unit.id,
            "About to call source.initialize()"
        );

        // Add timeout to detect hangs (configurable via SOURCE_INIT_TIMEOUT_SECS)
        let init_timeout_secs: u64 = env::var("SOURCE_INIT_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(300);
        match tokio::time::timeout(
            std::time::Duration::from_secs(init_timeout_secs),
            source.initialize(&source_config),
        )
        .await
        {
            Ok(result) => {
                result.map_err(|e| {
                    roboflow_distributed::TikvError::Other(format!("Init error: {e}"))
                })?;
                tracing::debug!(
                    batch_id = %work_unit.batch_id,
                    unit_id = %work_unit.id,
                    "Source initialized successfully"
                );
            }
            Err(_) => {
                tracing::error!(
                    batch_id = %work_unit.batch_id,
                    unit_id = %work_unit.id,
                    timeout_secs = init_timeout_secs,
                    "Source initialization timed out"
                );
                return Err(roboflow_distributed::TikvError::Other(
                    "Source initialization timed out".to_string(),
                ));
            }
        }

        // lerobot_config already loaded above before source creation
        let mut lerobot_config = lerobot_config;
        lerobot_config.streaming.finalize_metadata_in_coordinator = true;

        let episode_output_dir =
            output_dir.join(format!("episode_{:06}", allocation.episode_index));
        std::fs::create_dir_all(&episode_output_dir)
            .map_err(|e| roboflow_distributed::TikvError::Other(format!("Mkdir error: {e}")))?;

        let writer_config = LerobotWriterConfig::new(
            episode_output_dir.to_string_lossy().to_string(),
            lerobot_config,
        );

        let writer = create_lerobot_writer(&writer_config)
            .map_err(|e| roboflow_distributed::TikvError::Other(format!("Writer error: {e}")))?
            .writer;

        let num_threads = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4);
        let mut executor = DatasetPipelineExecutor::parallel(
            writer,
            DatasetPipelineConfig::with_fps(30),
            num_threads,
        );

        tracing::info!(
            batch_id = %work_unit.batch_id,
            unit_id = %work_unit.id,
            "Starting to read and process messages"
        );

        let mut total_messages: u64 = 0;
        let mut last_log_time = std::time::Instant::now();
        let log_interval = std::time::Duration::from_secs(10);

        loop {
            match source.read_batch(100).await {
                Ok(Some(messages)) => {
                    let msg_count = messages.len() as u64;
                    total_messages += msg_count;

                    let now = std::time::Instant::now();
                    if now.duration_since(last_log_time) >= log_interval {
                        tracing::info!(
                            batch_id = %work_unit.batch_id,
                            unit_id = %work_unit.id,
                            total_messages = total_messages,
                            batch_size = msg_count,
                            "Processing progress"
                        );
                        last_log_time = now;
                    }

                    executor.process_messages(messages).map_err(|e| {
                        roboflow_distributed::TikvError::Other(format!("Pipeline error: {e}"))
                    })?;
                }
                Ok(None) => {
                    tracing::info!(
                        batch_id = %work_unit.batch_id,
                        unit_id = %work_unit.id,
                        total_messages = total_messages,
                        "Finished reading messages, finalizing"
                    );
                    break;
                }
                Err(e) => {
                    return Err(roboflow_distributed::TikvError::Other(format!(
                        "Read error: {e}"
                    )));
                }
            }
        }

        // 4. Finalize processing
        let pipeline_stats = executor
            .finalize()
            .map_err(|e| roboflow_distributed::TikvError::Other(format!("Finalize error: {e}")))?;

        let frame_count = pipeline_stats.frames_written as u64;

        // 5. If S3/cloud mode: upload to staging and register
        if is_cloud {
            // Create storage factory and storage for upload
            let storage_factory = create_storage_factory();
            let storage = storage_factory
                .create(&work_unit.output_path)
                .map_err(|e| {
                    roboflow_distributed::TikvError::Other(format!("Storage error: {e}"))
                })?;

            // Build staging path: {output_path}/staging/{batch_id}/{worker_id}/{unit_id}
            let staging_path = format!(
                "{}/staging/{}/worker_{}/unit_{}",
                work_unit.output_path.trim_end_matches('/'),
                work_unit.batch_id,
                self.pod_id,
                work_unit.id
            );

            tracing::info!(
                batch_id = %work_unit.batch_id,
                unit_id = %work_unit.id,
                staging_path = %staging_path,
                "Uploading to cloud storage staging"
            );

            // Upload the local temp directory to cloud staging
            let uploaded = roboflow_storage::upload::upload_directory_recursive(
                storage,
                &episode_output_dir,
                std::path::Path::new(&staging_path),
            )
            .map_err(|e| roboflow_distributed::TikvError::Other(format!("Upload error: {e}")))?;

            tracing::info!(
                batch_id = %work_unit.batch_id,
                unit_id = %work_unit.id,
                staging_path = %staging_path,
                files_uploaded = uploaded.len(),
                total_frames = frame_count,
                "Staging upload complete, registering with merge coordinator"
            );

            // Register with merge coordinator
            self.merge_coordinator
                .register_staging_complete(
                    &work_unit.batch_id,
                    &self.pod_id,
                    staging_path.clone(),
                    frame_count,
                )
                .await?;

            // Clean up local temp directory
            if let Err(e) = std::fs::remove_dir_all(&local_temp) {
                tracing::warn!(
                    batch_id = %work_unit.batch_id,
                    unit_id = %work_unit.id,
                    temp_dir = %local_temp.display(),
                    error = %e,
                    "Failed to clean up temp directory"
                );
            }

            Ok(roboflow_distributed::ProcessingResult::Success {
                episode_index: allocation.episode_index,
                frame_count,
                episode_stats: Some(roboflow_distributed::EpisodeStats {
                    episode_index: allocation.episode_index as usize,
                    frame_count: pipeline_stats.frames_written,
                    feature_stats: std::collections::HashMap::new(),
                    task_indices: Vec::new(),
                    recorded_at: Some(chrono::Utc::now().timestamp()),
                }),
            })
        } else {
            Ok(roboflow_distributed::ProcessingResult::Success {
                episode_index: allocation.episode_index,
                frame_count,
                episode_stats: Some(roboflow_distributed::EpisodeStats {
                    episode_index: allocation.episode_index as usize,
                    frame_count: pipeline_stats.frames_written,
                    feature_stats: std::collections::HashMap::new(),
                    task_indices: Vec::new(),
                    recorded_at: Some(chrono::Utc::now().timestamp()),
                }),
            })
        }
    }
}

/// Run the worker role.
async fn run_worker(
    pod_id: String,
    tikv: Arc<roboflow_distributed::TikvClient>,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = WorkerConfig::new();
    let merge_coordinator = Arc::new(MergeCoordinator::new(tikv.clone()));
    let processor: roboflow_distributed::worker::SharedWorkProcessor =
        Arc::new(CliWorkProcessor::new(
            pod_id.clone(),
            tikv.clone(),
            merge_coordinator,
            config.clone(),
        ));
    let mut worker = Worker::with_processor(pod_id, tikv, config, processor)?;

    worker.run().await.map_err(|e| e.into())
}

/// Run the finalizer role.
async fn run_finalizer(
    pod_id: String,
    tikv: Arc<roboflow_distributed::TikvClient>,
    batch_controller: Arc<BatchController>,
    merge_coordinator: Arc<MergeCoordinator>,
    cancel: CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = FinalizerConfig::from_env()?;
    let finalizer = Finalizer::new(pod_id, tikv, batch_controller, merge_coordinator, config)?;

    finalizer.run(cancel).await.map_err(|e| e.into())
}

/// Run unified service (all roles).
async fn run_unified(
    pod_id: String,
    tikv: Arc<roboflow_distributed::TikvClient>,
    cancel: CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    let worker_config = WorkerConfig::new();
    let finalizer_config = FinalizerConfig::from_env()?;
    let reaper_config = ReaperConfig::new();
    let scanner_config = ScannerConfig::from_env()?;

    let batch_controller = Arc::new(BatchController::with_client(tikv.clone()));
    let merge_coordinator = Arc::new(MergeCoordinator::new(tikv.clone()));

    // Clone cancel for tasks
    let cancel_clone = cancel.clone();

    // Create worker, finalizer, and reaper
    let worker_pod_id = format!("{}-worker", pod_id);
    let worker_processor: roboflow_distributed::worker::SharedWorkProcessor =
        Arc::new(CliWorkProcessor::new(
            worker_pod_id.clone(),
            tikv.clone(),
            merge_coordinator.clone(),
            worker_config.clone(),
        ));
    let mut worker =
        Worker::with_processor(worker_pod_id, tikv.clone(), worker_config, worker_processor)?;

    let finalizer = Finalizer::new(
        format!("{}-finalizer", pod_id),
        tikv.clone(),
        batch_controller.clone(),
        merge_coordinator,
        finalizer_config,
    )?;

    let mut reaper = ZombieReaper::new(tikv.clone(), reaper_config);

    // Scanner variables for spawning in task
    let scanner_pod_id = format!("{}-scanner", pod_id);
    let scanner_tikv = tikv.clone();
    let scanner_cancel = cancel.clone();

    // Start batch controller in background with error logging
    let batch_controller_id = pod_id.clone();
    tokio::spawn(async move {
        if let Err(e) = batch_controller.run().await {
            tracing::error!(
                pod_id = %batch_controller_id,
                error = %e,
                "BatchController failed"
            );
        }
    });

    // Spawn scanner task - runs its own leader election loop
    let mut scanner_handle = tokio::spawn(async move {
        let mut scanner = match Scanner::new(
            scanner_pod_id,
            scanner_tikv,
            Arc::new(StorageFactory::from_env()),
            scanner_config,
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "Failed to create scanner");
                return;
            }
        };

        tokio::select! {
            result = scanner.run() => {
                if let Err(e) = result {
                    tracing::error!(error = %e, "Scanner failed");
                }
            }
            _ = scanner_cancel.cancelled() => {
                tracing::info!("Scanner shutting down");
            }
        }
    });

    // Spawn all three tasks with error logging
    let worker_pod_id = pod_id.clone();
    let mut worker_handle = tokio::spawn(async move {
        if let Err(e) = worker.run().await {
            tracing::error!(
                pod_id = %worker_pod_id,
                error = %e,
                "Worker failed"
            );
        }
    });

    let reaper_pod_id = pod_id.clone();
    let mut reaper_handle = tokio::spawn(async move {
        if let Err(e) = reaper.run().await {
            tracing::error!(
                pod_id = %reaper_pod_id,
                error = %e,
                "ZombieReaper failed"
            );
        }
    });

    let finalizer_pod_id = pod_id.clone();
    let mut finalizer_handle = tokio::spawn(async move {
        if let Err(e) = finalizer.run(cancel_clone).await {
            tracing::error!(
                pod_id = %finalizer_pod_id,
                error = %e,
                "Finalizer failed"
            );
        }
    });

    // Wait for any task to complete (usually due to shutdown or error).
    // Track which handle completed so we don't poll it again (JoinHandle panics if polled after completion).
    let mut worker_done = false;
    let mut reaper_done = false;
    let mut finalizer_done = false;
    let mut scanner_done = false;
    tokio::select! {
        _ = &mut worker_handle => {
            cancel.cancel();
            worker_done = true;
        }
        _ = &mut reaper_handle => {
            cancel.cancel();
            reaper_done = true;
        }
        _ = &mut finalizer_handle => {
            cancel.cancel();
            finalizer_done = true;
        }
        _ = &mut scanner_handle => {
            cancel.cancel();
            scanner_done = true;
        }
    }

    // Build list of remaining handles and their abort handles so we can wait with a single
    // join_all (each handle polled at most once) and still abort on timeout.
    let mut remaining_handles = Vec::new();
    let mut abort_handles = Vec::new();
    if !worker_done {
        abort_handles.push(worker_handle.abort_handle());
        remaining_handles.push(worker_handle);
    }
    if !reaper_done {
        abort_handles.push(reaper_handle.abort_handle());
        remaining_handles.push(reaper_handle);
    }
    if !finalizer_done {
        abort_handles.push(finalizer_handle.abort_handle());
        remaining_handles.push(finalizer_handle);
    }
    if !scanner_done {
        abort_handles.push(scanner_handle.abort_handle());
        remaining_handles.push(scanner_handle);
    }

    if remaining_handles.is_empty() {
        return Ok(());
    }

    // Wait for all remaining with a deadline; each handle is only awaited once (inside join_all).
    const SHUTDOWN_TIMEOUT_SECS: u64 = 15;
    tracing::info!(
        timeout_secs = SHUTDOWN_TIMEOUT_SECS,
        "Waiting for remaining tasks to shut down"
    );
    let deadline =
        tokio::time::Instant::now() + std::time::Duration::from_secs(SHUTDOWN_TIMEOUT_SECS);
    let mut join_fut = join_all(remaining_handles);
    tokio::select! {
        _ = tokio::time::sleep_until(deadline) => {
            tracing::warn!(
                "Shutdown timeout reached, aborting remaining tasks so process can exit"
            );
            for a in &abort_handles {
                a.abort();
            }
            let _ = join_fut.await;
        }
        _ = &mut join_fut => {}
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    let command = parse_args(&args).unwrap_or_else(|e| {
        if !e.is_empty() {
            eprintln!("{}", e);
        }
        std::process::exit(1);
    });

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            env::var("RUST_LOG").unwrap_or_else(|_| "roboflow=info,tikv_client=warn".to_string()),
        )
        .init();

    // Initialize S3 environment bridge before any robocodec operations
    // This reads RF_S3_* / AWS_* env vars and applies to AWS-standard names
    if let Err(e) = roboflow_dataset::sources::init_s3_env_bridge() {
        tracing::warn!(error = %e, "Failed to initialize S3 environment bridge");
    }

    // Register all built-in source types (bag, mcap, rrd)
    roboflow_dataset::sources::register_builtin_sources();

    match command {
        Command::Submit { args } => {
            if let Err(e) = commands::run_submit_command(&args).await {
                eprintln!("{}", e);
                std::process::exit(1);
            }
        }
        Command::Jobs { args } => {
            if let Err(e) = commands::run_jobs_command(&args).await {
                eprintln!("{}", e);
                std::process::exit(1);
            }
        }
        Command::Batch { args } => {
            if let Err(e) = commands::run_batch_command(&args).await {
                eprintln!("{}", e);
                std::process::exit(1);
            }
        }
        Command::Run { role, pod_id } => {
            let role = role
                .and_then(|r| Role::try_from_str(&r))
                .unwrap_or_else(Role::from_env);

            let pod_id = pod_id.unwrap_or_else(|| generate_pod_id("roboflow"));

            tracing::info!(
                pod_id = %pod_id,
                role = ?role,
                "Starting unified service"
            );

            let tikv = Arc::new(create_tikv().await?);
            let cancel = CancellationToken::new();
            let cancel_clone = cancel.clone();

            tokio::spawn(async move {
                match tokio::signal::ctrl_c().await {
                    Ok(()) => {
                        tracing::info!("Received Ctrl+C, shutting down...");
                        cancel_clone.cancel();
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Failed to install Ctrl+C handler");
                        // Still cancel to ensure shutdown
                        cancel_clone.cancel();
                    }
                }
            });

            match role {
                Role::Worker => {
                    run_worker(pod_id, tikv).await?;
                }
                Role::Finalizer => {
                    let batch_controller = Arc::new(BatchController::with_client(tikv.clone()));
                    let merge_coordinator = Arc::new(MergeCoordinator::new(tikv.clone()));
                    run_finalizer(pod_id, tikv, batch_controller, merge_coordinator, cancel)
                        .await?;
                }
                Role::Unified => {
                    run_unified(pod_id, tikv, cancel).await?;
                }
            }
        }
        Command::Health {
            host: _host,
            port: _port,
        } => {
            // Health check - validate TiKV and storage connectivity
            let result = run_health_check().await;

            // Format output based on result
            let status = if result.is_healthy {
                "healthy"
            } else {
                "unhealthy"
            };
            println!("{}", status);

            if !result.is_healthy {
                for error in &result.errors {
                    eprintln!("  [ERROR] {}", error);
                }
                std::process::exit(1);
            }

            // Print detailed health info
            if let Some(details) = &result.details {
                println!("\nDetails:");
                for (key, value) in details {
                    println!("  {}: {}", key, value);
                }
            }
        }
    }

    Ok(())
}
