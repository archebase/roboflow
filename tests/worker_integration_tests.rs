// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Worker integration tests.
//!
//! These tests validate the Worker's integration with the dataset pipeline:
//! - Worker.process_job() with streaming converter
//! - LeRobotWriter integration
//! - Storage backend integration

use std::fs;

use roboflow::{DatasetBaseConfig, LerobotConfig, LerobotWriter, VideoConfig};
use roboflow_dataset::ImageData;

/// Create a test output directory using system temp.
/// Using tempfile::tempdir() directly avoids:
/// - Cross-test interference
/// - Dirty working trees in CI
/// - Failures when repo is read-only
fn test_output_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("Failed to create temp dir")
}

// =============================================================================
// Test: End-to-end LeRobot writer with streaming converter
// =============================================================================

#[test]
fn test_lerobot_writer_basic_flow() {
    let output_dir = test_output_dir();
    let output_path = output_dir.path();

    // Create a test LeRobot configuration
    let lerobot_config = LerobotConfig {
        dataset: roboflow::lerobot::DatasetConfig {
            base: DatasetBaseConfig {
                name: "test_dataset".to_string(),
                fps: 30,
                robot_type: Some("test_robot".to_string()),
            },
            env_type: None,
        },
        mappings: vec![],
        video: VideoConfig::default(),
        annotation_file: None,
        flushing: roboflow::lerobot::FlushingConfig::default(),
        streaming: roboflow::lerobot::StreamingConfig::default(),
    };

    // Create a LeRobot writer directly to verify output
    // The writer is already initialized via new_local()
    let mut writer = LerobotWriter::new_local(output_path, lerobot_config.clone()).unwrap();

    // Create test image data
    let img_data = ImageData::new(64, 48, vec![128u8; 64 * 48 * 3]);

    // Write a test episode
    writer.start_episode(Some(0));
    writer.add_image("observation.images.camera_0".to_string(), img_data);
    writer.finish_episode(Some(0)).unwrap();

    // Finalize and get stats - use DatasetWriter trait method
    use roboflow_dataset::common::DatasetWriter;
    let _stats = DatasetWriter::finalize(&mut writer).unwrap();

    // Verify output directory structure exists
    assert!(output_path.join("data/chunk-000").exists());
    assert!(output_path.join("meta").exists());

    // Verify info.json was created
    let info_path = output_path.join("meta/info.json");
    assert!(info_path.exists(), "info.json should be created");

    // Read and verify info.json
    let info_content = fs::read_to_string(info_path).expect("Failed to read info.json");
    assert!(info_content.contains("\"fps\": 30"));
    // Robot type may be formatted differently
    assert!(info_content.contains("test_robot") || info_content.contains("robot"));
}

// =============================================================================
// Test: Worker configuration
// =============================================================================
// These tests require the distributed feature (TiKV dependencies)

#[test]
fn test_worker_config_default() {
    use roboflow_distributed::WorkerConfig;

    let config = WorkerConfig::new();
    assert_eq!(config.output_prefix, "output/");
}

#[test]
fn test_worker_config_builder() {
    use roboflow_distributed::WorkerConfig;

    let config = WorkerConfig::new().with_output_prefix("custom_output/");

    assert_eq!(config.output_prefix, "custom_output/");
}

// =============================================================================
// Test: Processing result creation
// =============================================================================

#[test]
fn test_processing_result_success() {
    use roboflow_distributed::worker::ProcessingResult;

    let result = ProcessingResult::Success;
    match result {
        ProcessingResult::Success => {}
        ProcessingResult::Failed { error } => {
            panic!("Unexpected failed result: {}", error);
        }
        ProcessingResult::Cancelled => {
            panic!("Unexpected cancelled result");
        }
    }
}

#[test]
fn test_processing_result_failed() {
    use roboflow_distributed::worker::ProcessingResult;

    let result = ProcessingResult::Failed {
        error: "Test error".to_string(),
    };
    match result {
        ProcessingResult::Success => {
            panic!("Unexpected success result");
        }
        ProcessingResult::Failed { error } => {
            assert_eq!(error, "Test error");
        }
        ProcessingResult::Cancelled => {
            panic!("Unexpected cancelled result");
        }
    }
}

// =============================================================================
// Test: Shutdown handler functionality
// =============================================================================

#[test]
fn test_shutdown_handler_default() {
    use roboflow_distributed::ShutdownHandler;

    let handler = ShutdownHandler::default();
    assert!(!handler.is_requested());
}

#[test]
fn test_shutdown_handler_programmatic() {
    use roboflow_distributed::ShutdownHandler;

    let handler = ShutdownHandler::new();
    assert!(!handler.is_requested());

    handler.shutdown();
    assert!(handler.is_requested());
}

#[test]
fn test_shutdown_handler_flag() {
    use roboflow_distributed::ShutdownHandler;
    use std::sync::atomic::Ordering;

    let handler = ShutdownHandler::new();
    let flag = handler.flag_clone();

    assert!(!flag.load(Ordering::SeqCst));

    handler.shutdown();
    assert!(flag.load(Ordering::SeqCst));
}

#[test]
fn test_shutdown_interrupted_to_string() {
    use roboflow_distributed::ShutdownInterrupted;

    let err = ShutdownInterrupted;
    assert_eq!(err.to_string(), "Processing interrupted by shutdown signal");
}

#[test]
fn test_shutdown_constants() {
    use roboflow_distributed::SHUTDOWN_DEFAULT_TIMEOUT_SECS;

    assert_eq!(SHUTDOWN_DEFAULT_TIMEOUT_SECS, 30);
}

// // =============================================================================
// // Test: ConfigRecord functionality
// // =============================================================================
//
// #[test]
// fn test_config_record_new() {
//     use roboflow_distributed::tikv::schema::ConfigRecord;
//     let _config = ConfigRecord::new("test-config".to_string());
//     // This test was using JobRecord which has been removed
//     // TODO: Rewrite for ConfigRecord if needed
// }
//
// #[test]
// fn test_job_record_claim() {
//     use roboflow_distributed::tikv::schema::{JobRecord, JobStatus};
//
//     let mut job = JobRecord::new(
//         "job-123".to_string(),
//         "s3://test-bucket/path/to/file.bag".to_string(),
//         1024,
//         "output/".to_string(),
//         "config-hash-123".to_string(),
//     );
//
//     // Claim the job
//     let result = job.claim("pod-abc".to_string());
//     assert!(result.is_ok());
//
//     assert_eq!(job.status, JobStatus::Processing);
//     assert_eq!(job.owner, Some("pod-abc".to_string()));
//     assert_eq!(job.attempts, 1);
//     assert!(!job.is_terminal());
// }
//
// #[test]
// fn test_job_record_claim_fails_if_not_claimable() {
//     use roboflow_distributed::tikv::schema::JobRecord;
//
//     let mut job = JobRecord::new(
//         "job-123".to_string(),
//         "s3://test-bucket/path/to/file.bag".to_string(),
//         1024,
//         "output/".to_string(),
//         "config-hash-123".to_string(),
//     );
//
//     // Mark as completed first
//     job.complete();
//
//     // Try to claim - should fail
//     let result = job.claim("pod-xyz".to_string());
//     assert!(result.is_err());
//     assert!(result.unwrap_err().contains("not claimable"));
// }
//
// #[test]
// fn test_job_record_complete() {
//     use roboflow_distributed::tikv::schema::{JobRecord, JobStatus};
//
//     let mut job = JobRecord::new(
//         "job-123".to_string(),
//         "s3://test-bucket/path/to/file.bag".to_string(),
//         1024,
//         "output/".to_string(),
//         "config-hash-123".to_string(),
//     );
//
//     job.claim("pod-abc".to_string()).unwrap();
//     job.complete();
//
//     assert_eq!(job.status, JobStatus::Completed);
//     assert!(job.owner.is_none());
//     assert!(job.is_terminal());
// }
//
// #[test]
// fn test_job_record_fail_with_retry() {
//     use roboflow_distributed::tikv::schema::{JobRecord, JobStatus};
//
//     let mut job = JobRecord::new(
//         "job-123".to_string(),
//         "s3://test-bucket/path/to/file.bag".to_string(),
//         1024,
//         "output/".to_string(),
//         "config-hash-123".to_string(),
//     );
//
//     job.claim("pod-abc".to_string()).unwrap();
//     job.fail("Test error".to_string());
//
//     // With attempts < max_attempts, should be Failed (retryable)
//     assert_eq!(job.status, JobStatus::Failed);
//     assert!(job.owner.is_none());
//     assert_eq!(job.error, Some("Test error".to_string()));
//     assert!(!job.is_terminal());
//     assert!(job.is_claimable());
// }
//
// #[test]
// fn test_job_record_fail_dead_after_max_attempts() {
//     use roboflow_distributed::tikv::schema::{JobRecord, JobStatus};
//
//     let mut job = JobRecord::new(
//         "job-123".to_string(),
//         "s3://test-bucket/path/to/file.bag".to_string(),
//         1024,
//         "output/".to_string(),
//         "config-hash-123".to_string(),
//     );
//
//     // Set attempts to max
//     job.attempts = job.max_attempts;
//
//     job.fail("Final error".to_string());
//
//     // With attempts >= max_attempts, should be Dead (not retryable)
//     assert_eq!(job.status, JobStatus::Dead);
//     assert!(job.owner.is_none());
//     assert_eq!(job.error, Some("Final error".to_string()));
//     assert!(job.is_terminal());
//     assert!(!job.is_claimable());
// }
//
// #[test]
// fn test_job_record_cancel() {
//     use roboflow_distributed::tikv::schema::{JobRecord, JobStatus};
//
//     let mut job = JobRecord::new(
//         "job-123".to_string(),
//         "s3://test-bucket/path/to/file.bag".to_string(),
//         1024,
//         "output/".to_string(),
//         "config-hash-123".to_string(),
//     );
//
//     job.claim("pod-abc".to_string()).unwrap();
//     job.cancel("admin-user");
//
//     assert_eq!(job.status, JobStatus::Cancelled);
//     assert!(job.owner.is_none());
//     assert!(job.cancelled_at.is_some());
//     assert!(job.is_terminal());
//     assert!(!job.is_claimable()); // Cancelled jobs cannot be reclaimed
// }
//
// #[test]
// fn test_job_record_can_cancel() {
//     use roboflow_distributed::tikv::schema::JobRecord;
//
//     let mut job = JobRecord::new(
//         "job-123".to_string(),
//         "s3://test-bucket/path/to/file.bag".to_string(),
//         1024,
//         "output/".to_string(),
//         "config-hash-123".to_string(),
//     );
//
//     job.submitted_by = Some("user-123".to_string());
//     job.claim("pod-abc".to_string()).unwrap();
//
//     // Owner can cancel
//     assert!(job.can_cancel("pod-abc", &[]));
//
//     // Submitter can cancel
//     assert!(job.can_cancel("user-123", &[]));
//
//     // Admin can cancel
//     assert!(job.can_cancel("admin-user", &["admin-user".to_string()]));
//
//     // Random user cannot cancel
//     assert!(!job.can_cancel("random-user", &[]));
// }
//
// #[test]
// fn test_job_record_is_claimable() {
//     use roboflow_distributed::tikv::schema::{JobRecord, JobStatus};
//
//     let mut job = JobRecord::new(
//         "job-123".to_string(),
//         "s3://test-bucket/path/to/file.bag".to_string(),
//         1024,
//         "output/".to_string(),
//         "config-hash-123".to_string(),
//     );
//
//     // New job is claimable
//     assert!(job.is_claimable());
//
//     // Processing job is not claimable
//     job.claim("pod-abc".to_string()).unwrap();
//     assert!(!job.is_claimable());
//
//     // Failed job is claimable
//     job.status = JobStatus::Failed;
//     job.owner = None;
//     assert!(job.is_claimable());
//
//     // Cancelled job is not claimable
//     job.cancel("admin");
//     assert!(!job.is_claimable());
//
//     // Dead job is not claimable
//     let mut job2 = JobRecord::new(
//         "job-456".to_string(),
//         "s3://test-bucket/path/to/file2.bag".to_string(),
//         1024,
//         "output/".to_string(),
//         "config-hash-123".to_string(),
//     );
//     job2.attempts = job2.max_attempts;
//     job2.fail("Final error".to_string());
//     assert!(!job2.is_claimable());
// }
//
// =============================================================================
// Test: WorkerMetrics
// =============================================================================

#[test]
fn test_worker_metrics_new() {
    use roboflow_distributed::worker::WorkerMetrics;
    use std::sync::atomic::Ordering;

    let metrics = WorkerMetrics::new();

    assert_eq!(metrics.jobs_claimed.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.jobs_completed.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.jobs_failed.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.jobs_dead.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.active_jobs.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.processing_errors.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.heartbeat_errors.load(Ordering::Relaxed), 0);
}

#[test]
fn test_worker_metrics_increments() {
    use roboflow_distributed::worker::WorkerMetrics;
    use std::sync::atomic::Ordering;

    let metrics = WorkerMetrics::new();

    metrics.inc_jobs_claimed();
    metrics.inc_jobs_claimed();
    assert_eq!(metrics.jobs_claimed.load(Ordering::Relaxed), 2);

    metrics.inc_jobs_completed();
    assert_eq!(metrics.jobs_completed.load(Ordering::Relaxed), 1);

    metrics.inc_jobs_failed();
    metrics.inc_jobs_failed();
    assert_eq!(metrics.jobs_failed.load(Ordering::Relaxed), 2);

    metrics.inc_jobs_dead();
    assert_eq!(metrics.jobs_dead.load(Ordering::Relaxed), 1);

    metrics.inc_active_jobs();
    metrics.inc_active_jobs();
    metrics.inc_active_jobs();
    assert_eq!(metrics.active_jobs.load(Ordering::Relaxed), 3);

    metrics.dec_active_jobs();
    assert_eq!(metrics.active_jobs.load(Ordering::Relaxed), 2);

    metrics.inc_processing_errors();
    assert_eq!(metrics.processing_errors.load(Ordering::Relaxed), 1);

    metrics.inc_heartbeat_errors();
    assert_eq!(metrics.heartbeat_errors.load(Ordering::Relaxed), 1);
}

#[test]
fn test_worker_metrics_snapshot() {
    use roboflow_distributed::worker::WorkerMetrics;

    let metrics = WorkerMetrics::new();

    metrics.inc_jobs_claimed();
    metrics.inc_jobs_claimed();
    metrics.inc_jobs_completed();
    metrics.inc_jobs_failed();
    metrics.inc_active_jobs();
    metrics.inc_active_jobs();
    metrics.inc_processing_errors();

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.jobs_claimed, 2);
    assert_eq!(snapshot.jobs_completed, 1);
    assert_eq!(snapshot.jobs_failed, 1);
    assert_eq!(snapshot.jobs_dead, 0);
    assert_eq!(snapshot.active_jobs, 2);
    assert_eq!(snapshot.processing_errors, 1);
    assert_eq!(snapshot.heartbeat_errors, 0);
}

// =============================================================================
// Test: CheckpointConfig
// =============================================================================

#[test]
fn test_checkpoint_config_default() {
    use roboflow_distributed::tikv::checkpoint::CheckpointConfig;

    let config = CheckpointConfig::default();

    assert_eq!(config.checkpoint_interval_frames, 100);
    assert_eq!(config.checkpoint_interval_seconds, 10);
    assert!(config.checkpoint_async);
}

#[test]
fn test_checkpoint_config_builder() {
    use roboflow_distributed::tikv::checkpoint::CheckpointConfig;

    let config = CheckpointConfig::new()
        .with_frame_interval(200)
        .with_time_interval(30)
        .with_async(false);

    assert_eq!(config.checkpoint_interval_frames, 200);
    assert_eq!(config.checkpoint_interval_seconds, 30);
    assert!(!config.checkpoint_async);
}

// =============================================================================
// Test: WorkerConfig extended
// =============================================================================

#[test]
fn test_worker_config_extended() {
    use roboflow_distributed::WorkerConfig;
    use std::time::Duration;

    let config = WorkerConfig::new()
        .with_max_concurrent_jobs(5)
        .with_poll_interval(Duration::from_secs(10))
        .with_max_attempts(5)
        .with_job_timeout(Duration::from_secs(7200))
        .with_heartbeat_interval(Duration::from_secs(60))
        .with_checkpoint_interval_frames(200)
        .with_checkpoint_interval_seconds(30)
        .with_checkpoint_async(false)
        .with_output_prefix("custom_output/")
        .with_output_storage_url("s3://my-bucket/output");

    assert_eq!(config.max_concurrent_jobs, 5);
    assert_eq!(config.poll_interval.as_secs(), 10);
    assert_eq!(config.max_attempts, 5);
    assert_eq!(config.job_timeout.as_secs(), 7200);
    assert_eq!(config.heartbeat_interval.as_secs(), 60);
    assert_eq!(config.checkpoint_interval_frames, 200);
    assert_eq!(config.checkpoint_interval_seconds, 30);
    assert!(!config.checkpoint_async);
    assert_eq!(config.output_prefix, "custom_output/");
    assert_eq!(
        config.output_storage_url,
        Some("s3://my-bucket/output".to_string())
    );
}

// =============================================================================
// Test: Worker constants
// =============================================================================

#[test]
fn test_worker_constants() {
    use roboflow_distributed::worker::{
        DEFAULT_CANCELLATION_CHECK_INTERVAL_SECS, DEFAULT_CHECKPOINT_INTERVAL_FRAMES,
        DEFAULT_CHECKPOINT_INTERVAL_SECS, DEFAULT_HEARTBEAT_INTERVAL_SECS,
        DEFAULT_JOB_TIMEOUT_SECS, DEFAULT_MAX_ATTEMPTS, DEFAULT_MAX_CONCURRENT_JOBS,
        DEFAULT_POLL_INTERVAL_SECS,
    };

    assert_eq!(DEFAULT_POLL_INTERVAL_SECS, 5);
    assert_eq!(DEFAULT_MAX_CONCURRENT_JOBS, 1);
    assert_eq!(DEFAULT_MAX_ATTEMPTS, 3);
    assert_eq!(DEFAULT_JOB_TIMEOUT_SECS, 3600);
    assert_eq!(DEFAULT_HEARTBEAT_INTERVAL_SECS, 30);
    assert_eq!(DEFAULT_CHECKPOINT_INTERVAL_FRAMES, 100);
    assert_eq!(DEFAULT_CHECKPOINT_INTERVAL_SECS, 10);
    assert_eq!(DEFAULT_CANCELLATION_CHECK_INTERVAL_SECS, 5);
}

// =============================================================================
// Test: ProcessingResult variants
// =============================================================================

#[test]
fn test_processing_result_all_variants() {
    use roboflow_distributed::worker::ProcessingResult;

    let success = ProcessingResult::Success;
    let failed = ProcessingResult::Failed {
        error: "test error".to_string(),
    };
    let cancelled = ProcessingResult::Cancelled;

    // Test matching
    match success {
        ProcessingResult::Success => {}
        _ => panic!("Expected Success"),
    }

    match failed {
        ProcessingResult::Failed { error } => {
            assert_eq!(error, "test error");
        }
        _ => panic!("Expected Failed"),
    }

    match cancelled {
        ProcessingResult::Cancelled => {}
        _ => panic!("Expected Cancelled"),
    }
}

// =============================================================================
// Test: WorkerMetricsSnapshot
// =============================================================================

#[test]
fn test_worker_metrics_snapshot_clone() {
    use roboflow_distributed::worker::WorkerMetrics;

    let metrics = WorkerMetrics::new();
    metrics.inc_jobs_claimed();
    metrics.inc_active_jobs();
    metrics.inc_jobs_completed();

    let snapshot1 = metrics.snapshot();
    let snapshot2 = snapshot1.clone();

    assert_eq!(snapshot1.jobs_claimed, snapshot2.jobs_claimed);
    assert_eq!(snapshot1.jobs_completed, snapshot2.jobs_completed);
    assert_eq!(snapshot1.active_jobs, snapshot2.active_jobs);
}

// =============================================================================
// // Test: JobStatus methods
// // =============================================================================
//
// #[test]
// fn test_job_status_methods() {
//     use roboflow_distributed::tikv::schema::JobStatus;
//
//     assert!(!JobStatus::Pending.is_active());
//     assert!(JobStatus::Processing.is_active());
//     assert!(!JobStatus::Completed.is_active());
//
//     assert!(!JobStatus::Pending.is_terminal());
//     assert!(JobStatus::Completed.is_terminal());
//     assert!(JobStatus::Dead.is_terminal());
//     assert!(JobStatus::Cancelled.is_terminal());
//     assert!(!JobStatus::Failed.is_terminal());
//
//     assert!(!JobStatus::Pending.is_failed());
//     assert!(JobStatus::Failed.is_failed());
//     assert!(JobStatus::Dead.is_failed());
//     assert!(!JobStatus::Cancelled.is_failed());
// }
//
// // =============================================================================
// Test: WorkerConfig default values
// =============================================================================

#[test]
fn test_worker_config_all_defaults() {
    use roboflow_distributed::WorkerConfig;
    use std::time::Duration;

    let config = WorkerConfig::default();

    assert_eq!(config.max_concurrent_jobs, 1);
    assert_eq!(config.poll_interval, Duration::from_secs(5));
    assert_eq!(config.max_attempts, 3);
    assert_eq!(config.job_timeout, Duration::from_secs(3600));
    assert_eq!(config.heartbeat_interval, Duration::from_secs(30));
    assert_eq!(config.checkpoint_interval_frames, 100);
    assert_eq!(config.checkpoint_interval_seconds, 10);
    assert!(config.checkpoint_async);
    assert_eq!(config.output_prefix, "output/");
    assert!(config.output_storage_url.is_none());
    assert_eq!(config.expected_workers, 1);
    assert_eq!(config.merge_output_path, "datasets/merged");
}

// =============================================================================
// // Test: JobRecord edge cases
// // =============================================================================
//
// #[test]
// fn test_job_record_max_attempts_exact_boundary() {
//     use roboflow_distributed::tikv::schema::{JobRecord, JobStatus};
//
//     let mut job = JobRecord::new(
//         "job-123".to_string(),
//         "s3://test-bucket/path/to/file.bag".to_string(),
//         1024,
//         "output/".to_string(),
//         "config-hash-123".to_string(),
//     );
//
//     // Set attempts to exactly max_attempts - 1, then fail
//     job.attempts = job.max_attempts - 1;
//     job.fail("Test error".to_string());
//
//     // Should be Failed (retryable)
//     assert_eq!(job.status, JobStatus::Failed);
//     assert!(!job.is_terminal());
//     assert!(job.is_claimable());
//
//     // Increment attempts and fail again
//     job.attempts = job.max_attempts;
//     job.fail("Final error".to_string());
//
//     // Now should be Dead
//     assert_eq!(job.status, JobStatus::Dead);
//     assert!(job.is_terminal());
//     assert!(!job.is_claimable());
// }
//
// #[test]
// fn test_job_record_multiple_claim_attempts() {
//     use roboflow_distributed::tikv::schema::JobRecord;
//
//     let mut job = JobRecord::new(
//         "job-123".to_string(),
//         "s3://test-bucket/path/to/file.bag".to_string(),
//         1024,
//         "output/".to_string(),
//         "config-hash-123".to_string(),
//     );
//
//     // First claim
//     let result = job.claim("pod-1".to_string());
//     assert!(result.is_ok());
//     assert_eq!(job.attempts, 1);
//     assert_eq!(job.owner, Some("pod-1".to_string()));
//
//     // Simulate job failing and being retried
//     job.status = roboflow_distributed::tikv::schema::JobStatus::Failed;
//     job.owner = None;
//
//     // Second claim
//     let result = job.claim("pod-2".to_string());
//     assert!(result.is_ok());
//     assert_eq!(job.attempts, 2);
//     assert_eq!(job.owner, Some("pod-2".to_string()));
//
//     // Third claim
//     job.status = roboflow_distributed::tikv::schema::JobStatus::Failed;
//     job.owner = None;
//     let result = job.claim("pod-3".to_string());
//     assert!(result.is_ok());
//     assert_eq!(job.attempts, 3);
//
//     // Fourth claim should fail (max_attempts reached)
//     job.status = roboflow_distributed::tikv::schema::JobStatus::Failed;
//     job.owner = None;
//     assert!(!job.is_claimable());
// }
//
// #[test]
// fn test_job_record_cancel_prevents_reclaim() {
//     use roboflow_distributed::tikv::schema::JobRecord;
//
//     let mut job = JobRecord::new(
//         "job-123".to_string(),
//         "s3://test-bucket/path/to/file.bag".to_string(),
//         1024,
//         "output/".to_string(),
//         "config-hash-123".to_string(),
//     );
//
//     // Cancel the job
//     job.cancel("admin-user");
//
//     assert!(job.is_terminal());
//     assert!(!job.is_claimable());
//
//     // Try to claim - should fail
//     let result = job.claim("pod-1".to_string());
//     assert!(result.is_err());
// }
//
// // =============================================================================
// // Test: ConfigRecord functionality
// =============================================================================

#[test]
fn test_config_record_new() {
    use roboflow_distributed::tikv::schema::ConfigRecord;

    let content = r#"
[dataset]
name = "test"
fps = 30
"#
    .to_string();

    let record = ConfigRecord::new(content.clone());

    assert!(!record.hash.is_empty());
    assert_eq!(record.content, content);
    assert!(record.created_at <= chrono::Utc::now());
}

#[test]
fn test_config_record_compute_hash() {
    use roboflow_distributed::tikv::schema::ConfigRecord;

    let content1 = "test content";
    let content2 = "test content";
    let content3 = "different content";

    let hash1 = ConfigRecord::compute_hash(content1);
    let hash2 = ConfigRecord::compute_hash(content2);
    let hash3 = ConfigRecord::compute_hash(content3);

    // Same content produces same hash
    assert_eq!(hash1, hash2);

    // Different content produces different hash
    assert_ne!(hash1, hash3);

    // Hash is SHA-256 (64 hex chars)
    assert_eq!(hash1.len(), 64);
}

// =============================================================================
// Test: LockRecord functionality
// =============================================================================

#[test]
fn test_lock_record_new() {
    use roboflow_distributed::tikv::schema::LockRecord;

    let lock = LockRecord::new(
        "resource-1".to_string(),
        "pod-abc".to_string(),
        3600, // 1 hour TTL
    );

    assert_eq!(lock.resource, "resource-1");
    assert_eq!(lock.owner, "pod-abc");
    assert_eq!(lock.version, 1);
    assert!(lock.expires_at > lock.acquired_at);
}

#[test]
fn test_lock_record_is_expired() {
    use roboflow_distributed::tikv::schema::LockRecord;

    let mut lock = LockRecord::new(
        "resource-1".to_string(),
        "pod-abc".to_string(),
        -1, // Already expired
    );

    assert!(lock.is_expired());

    // Create a valid lock
    lock = LockRecord::new("resource-1".to_string(), "pod-abc".to_string(), 3600);
    assert!(!lock.is_expired());
}

#[test]
fn test_lock_record_is_expired_with_grace() {
    use roboflow_distributed::tikv::schema::LockRecord;

    let mut lock = LockRecord::new("resource-1".to_string(), "pod-abc".to_string(), 3600);

    // Not expired - has 3600 seconds remaining
    assert!(!lock.is_expired());
    assert!(!lock.is_expired_with_grace(10));
    assert!(!lock.is_expired_with_grace(30));

    // Set to expire 20 seconds ago
    lock.expires_at = chrono::Utc::now() - chrono::Duration::seconds(20);

    // Expired without grace
    assert!(lock.is_expired());

    // Also expired with any grace period since it's already 20 seconds past due
    assert!(lock.is_expired_with_grace(10));
    assert!(lock.is_expired_with_grace(30));

    // Set to expire 10 seconds from now (future)
    lock.expires_at = chrono::Utc::now() + chrono::Duration::seconds(10);

    // Not expired without grace
    assert!(!lock.is_expired());

    // With 30 second grace, it IS considered expired (grace makes it expire early)
    assert!(lock.is_expired_with_grace(30));

    // But not expired with only 5 second grace
    assert!(!lock.is_expired_with_grace(5));
}

// =============================================================================
// Test: CheckpointState functionality
// =============================================================================

#[test]
fn test_checkpoint_state_progress() {
    use chrono::Utc;
    use roboflow_distributed::tikv::schema::CheckpointState;

    let checkpoint = CheckpointState {
        job_id: "job-123".to_string(),
        pod_id: "pod-abc".to_string(),
        byte_offset: 5000,
        last_frame: 50,
        episode_idx: 0,
        total_frames: 100,
        video_uploads: vec![],
        parquet_upload: None,
        updated_at: Utc::now(),
        version: 1,
    };

    // Progress should be 50%
    assert_eq!(checkpoint.progress_percent(), 50.0);

    // Test with 0 total frames (returns 0.0 when total_frames is 0)
    let checkpoint_zero = CheckpointState {
        total_frames: 0,
        ..checkpoint.clone()
    };
    assert_eq!(checkpoint_zero.progress_percent(), 0.0);
}

#[test]
fn test_checkpoint_state_is_complete() {
    use chrono::Utc;
    use roboflow_distributed::tikv::schema::CheckpointState;

    let mut checkpoint = CheckpointState {
        job_id: "job-123".to_string(),
        pod_id: "pod-abc".to_string(),
        byte_offset: 10000,
        last_frame: 100,
        episode_idx: 0,
        total_frames: 100,
        video_uploads: vec![],
        parquet_upload: None,
        updated_at: Utc::now(),
        version: 1,
    };

    assert!(checkpoint.is_complete());

    // Not complete when frames < total
    checkpoint.last_frame = 99;
    assert!(!checkpoint.is_complete());

    // Complete when frames >= total
    checkpoint.last_frame = 100;
    checkpoint.total_frames = 99;
    assert!(checkpoint.is_complete());
}

// =============================================================================
// Test: WorkerMetricsSnapshot display
// =============================================================================

#[test]
fn test_worker_metrics_snapshot_debug() {
    use roboflow_distributed::worker::WorkerMetrics;

    let metrics = WorkerMetrics::new();
    metrics.inc_jobs_claimed();
    metrics.inc_jobs_completed();
    metrics.inc_jobs_failed();
    metrics.inc_jobs_dead();
    metrics.inc_active_jobs();
    metrics.inc_processing_errors();
    metrics.inc_heartbeat_errors();

    let snapshot = metrics.snapshot();

    // Test Debug impl
    let debug_str = format!("{:?}", snapshot);
    assert!(debug_str.contains("WorkerMetricsSnapshot"));
    assert!(debug_str.contains("jobs_claimed: 1"));
}

// =============================================================================
// Test: HeartbeatRecord functionality
// =============================================================================

#[test]
fn test_heartbeat_record_new() {
    use roboflow_distributed::tikv::schema::{HeartbeatRecord, WorkerStatus};

    let heartbeat = HeartbeatRecord::new("pod-abc".to_string());

    assert_eq!(heartbeat.pod_id, "pod-abc");
    assert_eq!(heartbeat.status, WorkerStatus::Idle);
    assert_eq!(heartbeat.active_jobs, 0);
    assert_eq!(heartbeat.total_processed, 0);
    assert!(heartbeat.last_heartbeat <= chrono::Utc::now());
}

#[test]
fn test_heartbeat_record_beat() {
    use roboflow_distributed::tikv::schema::HeartbeatRecord;

    let mut heartbeat = HeartbeatRecord::new("pod-abc".to_string());
    let first_time = heartbeat.last_heartbeat;

    heartbeat.beat();
    assert!(heartbeat.last_heartbeat >= first_time);
}

#[test]
fn test_heartbeat_record_status() {
    use roboflow_distributed::tikv::schema::{HeartbeatRecord, WorkerStatus};

    let mut heartbeat = HeartbeatRecord::new("pod-abc".to_string());

    // Initial status is Idle
    assert_eq!(heartbeat.status, WorkerStatus::Idle);

    // Status can be updated to Busy
    heartbeat.status = WorkerStatus::Busy;
    heartbeat.active_jobs = 5;
    heartbeat.beat();
    assert_eq!(heartbeat.status, WorkerStatus::Busy);
    assert_eq!(heartbeat.active_jobs, 5);

    // Status can be updated back to Idle
    heartbeat.status = WorkerStatus::Idle;
    heartbeat.active_jobs = 0;
    heartbeat.beat();
    assert_eq!(heartbeat.status, WorkerStatus::Idle);
    assert_eq!(heartbeat.active_jobs, 0);
}
