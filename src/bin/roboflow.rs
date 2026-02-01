// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Roboflow CLI - distributed data processing pipeline.
//!
//! This binary provides long-running worker and scanner processes for
//! distributed dataset processing using TiKV coordination.
//!
//! ## Subcommands
//!
//! - `worker` - Run a worker that claims and processes jobs from TiKV
//! - `scanner` - Run a scanner that discovers files and creates jobs
//! - `health` - Run a standalone health check server (for testing)
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
//! ### Worker Configuration
//! - `WORKER_POLL_INTERVAL_SECS` - Job poll interval (default: 5)
//! - `WORKER_MAX_CONCURRENT_JOBS` - Max concurrent jobs (default: 1)
//! - `WORKER_MAX_ATTEMPTS` - Max attempts per job (default: 3)
//! - `WORKER_JOB_TIMEOUT_SECS` - Job timeout (default: 3600)
//! - `WORKER_HEARTBEAT_INTERVAL_SECS` - Heartbeat interval (default: 30)
//! - `WORKER_CHECKPOINT_INTERVAL_FRAMES` - Checkpoint interval in frames (default: 100)
//! - `WORKER_CHECKPOINT_INTERVAL_SECS` - Checkpoint interval in seconds (default: 10)
//! - `WORKER_STORAGE_PREFIX` - Input storage prefix (default: input/)
//! - `WORKER_OUTPUT_PREFIX` - Output storage prefix (default: output/)
//!
//! ### Scanner Configuration
//! - `SCANNER_INPUT_PREFIX` - Input prefix to scan (default: input/)
//! - `SCANNER_SCAN_INTERVAL_SECS` - Scan interval (default: 60)
//! - `SCANNER_OUTPUT_PREFIX` - Output prefix for jobs (default: output/)
//! - `SCANNER_FILE_PATTERN` - Glob pattern for filtering files (optional)
//!
//! ### Health Server Configuration
//! - `HEALTH_PORT` - Health server port (default: 8080)
//! - `HEALTH_HOST` - Health server host (default: 0.0.0.0)
//!
//! ### Logging
//! - `LOG_FORMAT` - Log format: pretty or json (default: pretty)
//! - `LOG_LEVEL` - Log level (default: info)
//! - `RUST_LOG` - Per-module log levels

use std::env;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

#[cfg(feature = "distributed")]
use roboflow_distributed::{Scanner, ScannerConfig, Worker, WorkerConfig};
#[cfg(feature = "distributed")]
use roboflow_storage::StorageFactory;

// =============================================================================
// Command Types
// =============================================================================

/// Generate a pod ID from environment or hostname + UUID.
#[cfg(feature = "distributed")]
fn generate_pod_id(prefix: &str) -> String {
    match env::var("POD_NAME") {
        Ok(name) => name,
        Err(_) => {
            // Try to get hostname, fall back to "unknown"
            let hostname = hostname::get()
                .map(|h| h.to_string_lossy().to_string())
                .unwrap_or_else(|_| "unknown".to_string());
            format!("{}-{}", prefix, hostname)
        }
    }
}

/// CLI command.
enum Command {
    /// Run the worker loop
    Worker {
        /// Pod ID for this worker
        pod_id: Option<String>,
        /// Storage URL for input/output files
        storage_url: Option<String>,
    },
    /// Run the scanner loop
    Scanner {
        /// Pod ID for this scanner
        pod_id: Option<String>,
        /// Storage URL for scanning files
        storage_url: Option<String>,
    },
    /// Run a standalone health check server
    Health {
        /// Host to bind to
        host: Option<String>,
        /// Port to bind to
        port: Option<u16>,
    },
}

/// Parse command-line arguments.
fn parse_args(args: &[String]) -> Result<Command, String> {
    if args.len() < 2 {
        return usage();
    }

    let command = &args[1];

    match command.as_str() {
        "worker" => {
            let mut pod_id = None;
            let mut storage_url = None;

            let mut i = 2;
            while i < args.len() {
                match args[i].as_str() {
                    "--pod-id" | "-p" => {
                        i += 1;
                        if i >= args.len() {
                            return Err("--pod-id requires a value".to_string());
                        }
                        pod_id = Some(args[i].clone());
                    }
                    "--storage-url" | "-s" => {
                        i += 1;
                        if i >= args.len() {
                            return Err("--storage-url requires a value".to_string());
                        }
                        storage_url = Some(args[i].clone());
                    }
                    "--help" | "-h" => {
                        return Ok(Command::Worker {
                            pod_id: None,
                            storage_url: None,
                        });
                    }
                    unknown => {
                        return Err(format!("unknown flag for worker: {}", unknown));
                    }
                }
                i += 1;
            }

            Ok(Command::Worker {
                pod_id,
                storage_url,
            })
        }
        "scanner" => {
            let mut pod_id = None;
            let mut storage_url = None;

            let mut i = 2;
            while i < args.len() {
                match args[i].as_str() {
                    "--pod-id" | "-p" => {
                        i += 1;
                        if i >= args.len() {
                            return Err("--pod-id requires a value".to_string());
                        }
                        pod_id = Some(args[i].clone());
                    }
                    "--storage-url" | "-s" => {
                        i += 1;
                        if i >= args.len() {
                            return Err("--storage-url requires a value".to_string());
                        }
                        storage_url = Some(args[i].clone());
                    }
                    "--help" | "-h" => {
                        return Ok(Command::Scanner {
                            pod_id: None,
                            storage_url: None,
                        });
                    }
                    unknown => {
                        return Err(format!("unknown flag for scanner: {}", unknown));
                    }
                }
                i += 1;
            }

            Ok(Command::Scanner {
                pod_id,
                storage_url,
            })
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
        unknown => Err(format!("unknown command: {}\n\n{}", unknown, get_help())),
    }
}

/// Print usage information and return an error.
fn usage() -> Result<Command, String> {
    Err(get_help())
}

/// Get help text.
fn get_help() -> String {
    r#"Roboflow CLI - Distributed Data Processing Pipeline

USAGE:
    roboflow <COMMAND> [OPTIONS]

COMMANDS:
    worker       Run a worker that claims and processes jobs from TiKV
    scanner      Run a scanner that discovers files and creates jobs
    health       Run a standalone health check server

WORKER OPTIONS:
    -p, --pod-id <ID>           Pod ID for this worker (default: POD_NAME env var or hostname+UUID)
    -s, --storage-url <URL>     Storage URL for input/output files (default: auto-detected)

SCANNER OPTIONS:
    -p, --pod-id <ID>           Pod ID for this scanner (default: POD_NAME env var or hostname+UUID)
    -s, --storage-url <URL>     Storage URL for scanning files (default: auto-detected)

HEALTH OPTIONS:
        --host <HOST>           Host to bind to (default: 0.0.0.0 or HEALTH env var)
        --port <PORT>           Port to bind to (default: 8080 or HEALTH_PORT env var)

ENVIRONMENT VARIABLES:
    TiKV Configuration:
        TIKV_PD_ENDPOINTS                PD endpoints (default: 127.0.0.1:2379)
        TIKV_CONNECTION_TIMEOUT_SECS     Connection timeout (default: 10)
        TIKV_OPERATION_TIMEOUT_SECS      Operation timeout (default: 30)

    Storage Configuration:
        OSS_ACCESS_KEY_ID               Alibaba OSS access key
        OSS_ACCESS_KEY_SECRET            Alibaba OSS secret key
        OSS_ENDPOINT                     Alibaba OSS endpoint
        AWS_ACCESS_KEY_ID                AWS access key
        AWS_SECRET_ACCESS_KEY            AWS secret key
        AWS_REGION                       AWS region

    Worker Configuration:
        WORKER_POLL_INTERVAL_SECS        Job poll interval (default: 5)
        WORKER_MAX_CONCURRENT_JOBS       Max concurrent jobs (default: 1)
        WORKER_MAX_ATTEMPTS              Max attempts per job (default: 3)
        WORKER_JOB_TIMEOUT_SECS          Job timeout (default: 3600)
        WORKER_HEARTBEAT_INTERVAL_SECS   Heartbeat interval (default: 30)
        WORKER_CHECKPOINT_INTERVAL_FRAMES Checkpoint interval in frames (default: 100)
        WORKER_CHECKPOINT_INTERVAL_SECS  Checkpoint interval in seconds (default: 10)
        WORKER_STORAGE_PREFIX            Input storage prefix (default: input/)
        WORKER_OUTPUT_PREFIX             Output storage prefix (default: output/)

    Scanner Configuration:
        SCANNER_INPUT_PREFIX              Input prefix to scan (default: input/)
        SCANNER_SCAN_INTERVAL_SECS       Scan interval (default: 60)
        SCANNER_OUTPUT_PREFIX            Output prefix for jobs (default: output/)
        SCANNER_FILE_PATTERN             Glob pattern for filtering files

    Health Server Configuration:
        HEALTH_PORT                       Health server port (default: 8080)
        HEALTH_HOST                       Health server host (default: 0.0.0.0)

    Logging:
        LOG_FORMAT                        Log format: pretty or json (default: pretty)
        LOG_LEVEL                         Log level (default: info)
        RUST_LOG                          Per-module log levels

EXAMPLES:
    # Run worker with default settings
    roboflow worker

    # Run worker with custom pod ID
    roboflow worker --pod-id worker-1

    # Run scanner with custom storage
    roboflow scanner --storage-url s3://my-bucket

    # Run health server on custom port
    roboflow health --port 9090

    # Run with JSON logging
    LOG_FORMAT=json roboflow worker
"#
    .to_string()
}

// =============================================================================
// Worker Command
// =============================================================================

#[cfg(feature = "distributed")]
async fn run_worker(
    pod_id: Option<String>,
    storage_url: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::env;

    // Initialize TiKV client from environment
    let tikv = Arc::new(roboflow_distributed::TikvClient::from_env().await?);

    // Determine storage URL
    let storage_url = storage_url
        .unwrap_or_else(|| env::var("STORAGE_URL").unwrap_or_else(|_| "file://./data".to_string()));

    // Create storage backend using factory from environment
    let factory = StorageFactory::from_env();
    let storage = factory.create(&storage_url).map_err(|e| {
        anyhow::anyhow!(
            "Failed to create storage backend for URL '{}': {}",
            storage_url,
            e
        )
    })?;

    // Load worker configuration from environment
    let config = load_worker_config();

    // Generate or use provided pod ID
    let pod_id = pod_id.unwrap_or_else(|| generate_pod_id("worker"));

    tracing::info!(
        pod_id = %pod_id,
        storage_url = %storage_url,
        "Starting worker"
    );

    // Create worker
    let mut worker = Worker::new(pod_id, tikv, storage, config)?;

    // Start health server in background
    let health_handle = start_health_server_background().await?;

    // Run worker loop (this blocks until shutdown)
    worker.run().await?;

    // Shutdown health server
    health_handle.shutdown().await;

    Ok(())
}

#[cfg(feature = "distributed")]
fn load_worker_config() -> WorkerConfig {
    use std::env;

    let poll_interval = env::var("WORKER_POLL_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);

    let max_concurrent_jobs = env::var("WORKER_MAX_CONCURRENT_JOBS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let max_attempts = env::var("WORKER_MAX_ATTEMPTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);

    let job_timeout = env::var("WORKER_JOB_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3600);

    let heartbeat_interval = env::var("WORKER_HEARTBEAT_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);

    let checkpoint_interval_frames = env::var("WORKER_CHECKPOINT_INTERVAL_FRAMES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let checkpoint_interval_seconds = env::var("WORKER_CHECKPOINT_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    let storage_prefix = env::var("WORKER_STORAGE_PREFIX").unwrap_or_else(|_| "input/".to_string());

    let output_prefix = env::var("WORKER_OUTPUT_PREFIX").unwrap_or_else(|_| "output/".to_string());

    WorkerConfig::new()
        .with_max_concurrent_jobs(max_concurrent_jobs)
        .with_poll_interval(Duration::from_secs(poll_interval))
        .with_max_attempts(max_attempts)
        .with_job_timeout(Duration::from_secs(job_timeout))
        .with_heartbeat_interval(Duration::from_secs(heartbeat_interval))
        .with_checkpoint_interval_frames(checkpoint_interval_frames)
        .with_checkpoint_interval_seconds(checkpoint_interval_seconds)
        .with_storage_prefix(storage_prefix)
        .with_output_prefix(output_prefix)
}

#[cfg(not(feature = "distributed"))]
async fn run_worker(
    _pod_id: Option<String>,
    _storage_url: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    Err("Worker requires 'distributed' feature to be enabled".into())
}

// =============================================================================
// Scanner Command
// =============================================================================

#[cfg(feature = "distributed")]
async fn run_scanner(
    pod_id: Option<String>,
    storage_url: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::env;

    // Initialize TiKV client from environment
    let tikv = Arc::new(roboflow_distributed::TikvClient::from_env().await?);

    // Determine storage URL
    let storage_url = storage_url
        .unwrap_or_else(|| env::var("STORAGE_URL").unwrap_or_else(|_| "file://./data".to_string()));

    // Create storage backend using factory from environment
    let factory = StorageFactory::from_env();
    let storage = factory.create(&storage_url).map_err(|e| {
        anyhow::anyhow!(
            "Failed to create storage backend for URL '{}': {}",
            storage_url,
            e
        )
    })?;

    // Load scanner configuration from environment
    let config = load_scanner_config();

    // Generate or use provided pod ID
    let pod_id = pod_id.unwrap_or_else(|| generate_pod_id("scanner"));

    tracing::info!(
        pod_id = %pod_id,
        storage_url = %storage_url,
        "Starting scanner"
    );

    // Create scanner
    let mut scanner = Scanner::new(pod_id, tikv, storage, config)?;

    // Start health server in background
    let health_handle = start_health_server_background().await?;

    // Run scanner loop (this blocks until shutdown)
    scanner.run().await?;

    // Shutdown health server
    health_handle.shutdown().await;

    Ok(())
}

#[cfg(feature = "distributed")]
fn load_scanner_config() -> ScannerConfig {
    use std::env;

    let input_prefix = env::var("SCANNER_INPUT_PREFIX").unwrap_or_else(|_| "input/".to_string());

    let scan_interval = env::var("SCANNER_SCAN_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);

    let output_prefix = env::var("SCANNER_OUTPUT_PREFIX").unwrap_or_else(|_| "output/".to_string());

    let config_hash = env::var("SCANNER_CONFIG_HASH").unwrap_or_else(|_| "default".to_string());

    let mut config = ScannerConfig::new(input_prefix)
        .with_scan_interval(Duration::from_secs(scan_interval))
        .with_output_prefix(output_prefix)
        .with_config_hash(config_hash);

    // Apply file pattern if provided
    if let Ok(pattern) = env::var("SCANNER_FILE_PATTERN") {
        match config.clone().with_file_pattern(&pattern) {
            Ok(c) => config = c,
            Err(e) => {
                tracing::warn!(
                    pattern = %pattern,
                    error = %e,
                    "Invalid SCANNER_FILE_PATTERN, scanning without file filter"
                );
                // Keep config without pattern
            }
        }
    }

    config
}

#[cfg(not(feature = "distributed"))]
async fn run_scanner(
    _pod_id: Option<String>,
    _storage_url: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    Err("Scanner requires 'distributed' feature to be enabled".into())
}

// =============================================================================
// Health Server
// =============================================================================

/// Health server handle for background management.
pub struct HealthServerHandle {
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    _server_task: tokio::task::JoinHandle<()>,
}

impl HealthServerHandle {
    /// Shutdown the health server.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        // Wait for the server task to complete
        let _ = tokio::time::timeout(Duration::from_secs(5), self._server_task).await;
    }
}

/// Health server startup result.
enum HealthServerStartup {
    Ready,
    Failed(String),
}

/// Start the health server in the background.
/// Returns error if the server fails to bind within a short timeout.
#[cfg(feature = "distributed")]
async fn start_health_server_background() -> Result<HealthServerHandle, Box<dyn std::error::Error>> {
    use std::env;

    let host = env::var("HEALTH_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = env::var("HEALTH_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8080);

    let addr = format!("{}:{}", host, port);
    let addr_for_log = addr.clone();

    // Create a channel to verify successful startup
    let (startup_tx, mut startup_rx) = tokio::sync::oneshot::channel::<HealthServerStartup>();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    // Spawn the server task
    let server_task = tokio::spawn(async move {
        run_health_server(&addr, shutdown_rx, startup_tx).await;
    });

    // Wait for startup confirmation with a timeout
    let startup_result = tokio::time::timeout(Duration::from_millis(500), &mut startup_rx)
        .await
        .map_err(|_| {
            format!(
                "Health server startup timed out after 500ms - may have failed to bind to {}",
                addr_for_log
            )
        })?
        .map_err(|e| format!("Health server startup channel closed: {}", e))?;

    match startup_result {
        HealthServerStartup::Ready => {
            tracing::info!("Health server successfully started on {}", addr_for_log);
        }
        HealthServerStartup::Failed(err) => {
            return Err(format!("Health server failed to start: {}", err).into());
        }
    }

    Ok(HealthServerHandle {
        shutdown_tx: Some(shutdown_tx),
        _server_task: server_task,
    })
}

/// Run the health check server.
async fn run_health_server(
    addr: &str,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    startup_tx: tokio::sync::oneshot::Sender<HealthServerStartup>,
) {
    // Ready flag - set to true when service is ready
    let ready = Arc::new(AtomicBool::new(true));

    // Try to bind the listener
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => {
            // Send success signal
            let _ = startup_tx.send(HealthServerStartup::Ready);
            l
        }
        Err(e) => {
            let err_msg = format!("Failed to bind health server to {}: {}", addr, e);
            tracing::error!("{}", err_msg);
            let _ = startup_tx.send(HealthServerStartup::Failed(err_msg));
            return;
        }
    };

    // Use a simple TCP loop to handle HTTP requests
    let ready = Arc::clone(&ready);
    let mut shutdown_rx = shutdown_rx;

    loop {
        tokio::select! {
            result = tokio::time::timeout(
                Duration::from_secs(5),
                listener.accept()
            ) => {
                match result {
                    Ok(Ok((socket, peer_addr))) => {
                        let ready = Arc::clone(&ready);
                        tokio::spawn(async move {
                            use tokio::io::{AsyncReadExt, AsyncWriteExt};
                            let mut socket: tokio::net::TcpStream = socket;
                            let mut buf = [0u8; 2048]; // Increased buffer size
                            let response = match tokio::time::timeout(
                                Duration::from_secs(5),
                                socket.read(&mut buf)
                            ).await {
                                Ok(Ok(n)) if n > 0 => {
                                    let request = String::from_utf8_lossy(&buf[..n]);
                                    handle_health_request(&request, &ready)
                                }
                                Ok(Ok(_)) => {
                                    "HTTP/1.1 400 Bad Request\r\n\r\n".to_string()
                                }
                                Ok(Err(e)) => {
                                    tracing::warn!(
                                        peer = %peer_addr,
                                        error = %e,
                                        "Health server socket read error"
                                    );
                                    "HTTP/1.1 500 Internal Server Error\r\n\r\n".to_string()
                                }
                                Err(_) => {
                                    tracing::warn!(
                                        peer = %peer_addr,
                                        "Health server read timeout"
                                    );
                                    "HTTP/1.1 408 Request Timeout\r\n\r\n".to_string()
                                }
                            };

                            if let Err(e) = socket.write_all(response.as_bytes()).await {
                                tracing::warn!(
                                    peer = %peer_addr,
                                    error = %e,
                                    "Health server response write failed"
                                );
                            }
                            let _ = socket.shutdown().await;
                        });
                    }
                    Ok(Err(e)) => {
                        // Log at warn level (not debug) so production can see issues
                        tracing::warn!(
                            error = %e,
                            "Health server accept error - may indicate network issues or resource exhaustion"
                        );
                    }
                    Err(_) => {
                        // Timeout is expected - no connections, just loop again
                    }
                }
            }
            _ = &mut shutdown_rx => {
                tracing::info!("Health server shutting down");
                break;
            }
        }
    }
}

/// Handle a health check HTTP request.
fn handle_health_request(request: &str, ready: &AtomicBool) -> String {
    use std::sync::atomic::Ordering;

    // Basic request validation - check for HTTP/1.x GET request
    if !request.starts_with("GET /") {
        return "HTTP/1.1 400 Bad Request\r\n\r\n".to_string();
    }

    // Extract path more carefully
    let path_end = match request.find(' ') {
        Some(pos) if request.starts_with("GET ") => pos,
        _ => return "HTTP/1.1 400 Bad Request\r\n\r\n".to_string(),
    };

    // Skip "GET " to get the path
    let request_line = &request[4..path_end];

    let response = match request_line {
        "/health/live" => {
            // Liveness probe - always return 200 if we're responding
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"status\":\"alive\"}\r\n"
        }
        "/health/ready" => {
            // Readiness probe - check ready flag
            if ready.load(Ordering::Relaxed) {
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"status\":\"ready\"}\r\n"
            } else {
                "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\n\r\n{\"status\":\"not_ready\"}\r\n"
            }
        }
        "/health" => {
            // Basic health check
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"status\":\"healthy\"}\r\n"
        }
        "/metrics" => {
            // Prometheus metrics endpoint
            // Returns placeholder metrics - actual worker/scanner metrics
            // would need to be shared via a global registry
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\n\r\n\
            # HELP roboflow_up Whether the roboflow service is up\n\
            # TYPE roboflow_up gauge\n\
            roboflow_up 1\n\
            # HELP roboflow_health_server_ready Whether the service is ready\n\
            # TYPE roboflow_health_server_ready gauge\n\
            roboflow_health_server_ready 1\n\r\n"
        }
        _ => "HTTP/1.1 404 Not Found\r\n\r\n",
    };

    response.to_string()
}

#[cfg(not(feature = "distributed"))]
fn start_health_server_background() -> Result<HealthServerHandle, Box<dyn std::error::Error>> {
    Err("Health server requires 'distributed' feature to be enabled".into())
}

// =============================================================================
// Standalone Health Command
// =============================================================================

async fn run_health_command(
    host: Option<String>,
    port: Option<u16>,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::env;

    let host =
        host.unwrap_or_else(|| env::var("HEALTH_HOST").unwrap_or_else(|_| "0.0.0.0".to_string()));
    let port = port.unwrap_or_else(|| {
        env::var("HEALTH_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(8080)
    });

    tracing::info!("Starting health server on {}:{}", host, port);

    let addr = format!("{}:{}", host, port);

    #[cfg(feature = "distributed")]
    {
        let ready = Arc::new(AtomicBool::new(true));
        let listener = tokio::net::TcpListener::bind(&addr).await?;

        println!("Health server listening on http://{}", addr);
        println!("Endpoints:");
        println!("  http://{}/health/live   - Liveness probe", addr);
        println!("  http://{}/health/ready  - Readiness probe", addr);
        println!("  http://{}/health        - Basic health check", addr);
        println!("  http://{}/metrics       - Prometheus metrics", addr);

        loop {
            let (socket, _) = listener.accept().await?;

            let ready = Arc::clone(&ready);
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut socket = socket;
                let mut buf = [0u8; 2048];

                let response =
                    match tokio::time::timeout(Duration::from_secs(5), socket.read(&mut buf)).await
                    {
                        Ok(Ok(n)) if n > 0 => {
                            let request = String::from_utf8_lossy(&buf[..n]);
                            handle_health_request(&request, &ready)
                        }
                        Ok(Ok(_)) | Ok(Err(_)) => "HTTP/1.1 400 Bad Request\r\n\r\n".to_string(),
                        Err(_) => "HTTP/1.1 408 Request Timeout\r\n\r\n".to_string(),
                    };

                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            });
        }
    }

    #[cfg(not(feature = "distributed"))]
    {
        return Err(
            "Health command requires 'distributed' feature to be enabled. \
                    Please rebuild with: cargo build --features distributed"
                .into(),
        );
    }
}

// =============================================================================
// Main Entry Point
// =============================================================================

fn main() {
    // Initialize structured logging first
    roboflow_core::init_logging()
        .unwrap_or_else(|e| eprintln!("Failed to initialize logging: {}", e));

    // Parse command-line arguments
    let args: Vec<String> = env::args().collect();

    let command = match parse_args(&args) {
        Ok(cmd) => cmd,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };

    // Create Tokio runtime for async commands
    #[cfg(feature = "distributed")]
    {
        let rt = match tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                eprintln!("Failed to create tokio runtime: {}", e);
                std::process::exit(1);
            }
        };

        let result = rt.block_on(async {
            match command {
                Command::Worker {
                    pod_id,
                    storage_url,
                } => run_worker(pod_id, storage_url).await,
                Command::Scanner {
                    pod_id,
                    storage_url,
                } => run_scanner(pod_id, storage_url).await,
                Command::Health { host, port } => run_health_command(host, port).await,
            }
        });

        if let Err(e) = result {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    }

    #[cfg(not(feature = "distributed"))]
    {
        let result = match command {
            Command::Health { host, port } => {
                // Still allow health command without distributed feature
                let rt = match tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        eprintln!("Failed to create tokio runtime: {}", e);
                        std::process::exit(1);
                    }
                };
                rt.block_on(run_health_command(host, port))
            }
            _ => {
                // Error for other commands without distributed feature
                Err(
                    "Worker and scanner commands require 'distributed' feature. \
                     Please rebuild with: cargo build --features distributed"
                        .into(),
                )
            }
        };

        if let Err(e) = result {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    }
}
