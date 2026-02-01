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
use std::time::Duration;

#[cfg(feature = "distributed")]
use roboflow_distributed::{Scanner, ScannerConfig, Worker, WorkerConfig};
#[cfg(feature = "distributed")]
use roboflow_storage::StorageFactory;

// =============================================================================
// Command Types
// =============================================================================

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
"#.to_string()
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
    let storage_url = storage_url.unwrap_or_else(|| {
        env::var("STORAGE_URL").unwrap_or_else(|_| "file://./data".to_string())
    });

    // Create storage backend using factory from environment
    let factory = StorageFactory::from_env();
    let storage = factory.create(&storage_url)?;

    // Load worker configuration from environment
    let config = load_worker_config();

    // Generate or use provided pod ID
    let pod_id = pod_id.unwrap_or_else(Worker::generate_pod_id);

    tracing::info!(
        pod_id = %pod_id,
        storage_url = %storage_url,
        "Starting worker"
    );

    // Create worker
    let mut worker = Worker::new(pod_id, tikv, storage, config)?;

    // Start health server in background
    let health_handle = start_health_server_background()?;

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

    let storage_prefix =
        env::var("WORKER_STORAGE_PREFIX").unwrap_or_else(|_| "input/".to_string());

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
    let storage_url = storage_url.unwrap_or_else(|| {
        env::var("STORAGE_URL").unwrap_or_else(|_| "file://./data".to_string())
    });

    // Create storage backend using factory from environment
    let factory = StorageFactory::from_env();
    let storage = factory.create(&storage_url)?;

    // Load scanner configuration from environment
    let config = load_scanner_config();

    // Generate or use provided pod ID
    let pod_id = pod_id.unwrap_or_else(|| {
        env::var("POD_NAME").unwrap_or_else(|_| {
            format!(
                "scanner-{}",
                uuid::Uuid::new_v4()
            )
        })
    });

    tracing::info!(
        pod_id = %pod_id,
        storage_url = %storage_url,
        "Starting scanner"
    );

    // Create scanner
    let mut scanner = Scanner::new(pod_id, tikv, storage, config)?;

    // Start health server in background
    let health_handle = start_health_server_background()?;

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
        config = match config.clone().with_file_pattern(&pattern) {
            Ok(c) => c,
            Err(_) => config, // If pattern is invalid, keep config without pattern
        };
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
}

impl HealthServerHandle {
    /// Shutdown the health server.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Start the health server in the background.
#[cfg(feature = "distributed")]
fn start_health_server_background() -> Result<HealthServerHandle, Box<dyn std::error::Error>> {
    use std::env;

    let host = env::var("HEALTH_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = env::var("HEALTH_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8080);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        run_health_server(&host, port, shutdown_rx).await;
    });

    Ok(HealthServerHandle {
        shutdown_tx: Some(shutdown_tx),
    })
}

/// Run the health check server.
async fn run_health_server(
    host: &str,
    port: u16,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) {
    use std::sync::atomic::{AtomicBool, Ordering};

    // Ready flag - set to true when service is ready
    let ready = Arc::new(AtomicBool::new(true));

    // Simple HTTP implementation without external dependencies
    let addr = format!("{}:{}", host, port);
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => {
            tracing::info!("Health server listening on {}", addr);
            l
        }
        Err(e) => {
            tracing::error!("Failed to bind health server to {}: {}", addr, e);
            return;
        }
    };

    // Use a simple TCP loop to handle HTTP requests
    tokio::spawn(async move {
        let ready = Arc::clone(&ready);
        let mut shutdown_rx = shutdown_rx;

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((socket, _)) => {
                            let ready = Arc::clone(&ready);
                            tokio::spawn(async move {
                                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                                let mut socket: tokio::net::TcpStream = socket;
                                let mut buf = [0u8; 1024];
                                if let Ok(n) = socket.read(&mut buf).await {
                                    let request = String::from_utf8_lossy(&buf[..n]);
                                    let response = if request.contains("GET /health/live ") {
                                        // Liveness probe - always return 200 if we're responding
                                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"status\":\"alive\"}\r\n"
                                    } else if request.contains("GET /health/ready ") {
                                        // Readiness probe - check ready flag
                                        if ready.load(Ordering::Relaxed) {
                                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"status\":\"ready\"}\r\n"
                                        } else {
                                            "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\n\r\n{\"status\":\"not_ready\"}\r\n"
                                        }
                                    } else if request.contains("GET /health ") {
                                        // Basic health check
                                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"status\":\"healthy\"}\r\n"
                                    } else {
                                        "HTTP/1.1 404 Not Found\r\n\r\n"
                                    };

                                    let _ = socket.write_all(response.as_bytes()).await;
                                    let _ = socket.shutdown().await;
                                }
                            });
                        }
                        Err(e) => {
                            tracing::debug!("Health server accept error: {}", e);
                        }
                    }
                }
                _ = &mut shutdown_rx => {
                    tracing::info!("Health server shutting down");
                    break;
                }
            }
        }
    }).await.ok();
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

    let host = host.unwrap_or_else(|| env::var("HEALTH_HOST").unwrap_or_else(|_| "0.0.0.0".to_string()));
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
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind(&addr).await?;

        println!("Health server listening on http://{}", addr);
        println!("Endpoints:");
        println!("  http://{}/health/live   - Liveness probe", addr);
        println!("  http://{}/health/ready  - Readiness probe", addr);
        println!("  http://{}/health        - Basic health check", addr);

        loop {
            let (socket, _) = listener.accept().await?;

            tokio::spawn(async move {
                let mut socket = socket;
                let mut buf = [0u8; 1024];
                if let Ok(n) = socket.read(&mut buf).await {
                    let request = String::from_utf8_lossy(&buf[..n]);
                    let response = if request.contains("GET /health/live ") {
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"status\":\"alive\"}\r\n"
                    } else if request.contains("GET /health/ready ") {
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"status\":\"ready\"}\r\n"
                    } else if request.contains("GET /health ") {
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"status\":\"healthy\"}\r\n"
                    } else {
                        "HTTP/1.1 404 Not Found\r\n\r\n"
                    };

                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.shutdown().await;
                }
            });
        }
    }

    #[cfg(not(feature = "distributed"))]
    {
        return Err("Health command requires 'distributed' feature to be enabled. \
                    Please rebuild with: cargo build --features distributed".into());
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
                Command::Worker { pod_id, storage_url } => run_worker(pod_id, storage_url).await,
                Command::Scanner { pod_id, storage_url } => run_scanner(pod_id, storage_url).await,
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
                Err("Worker and scanner commands require 'distributed' feature. \
                     Please rebuild with: cargo build --features distributed".into())
            }
        };

        if let Err(e) = result {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    }
}
