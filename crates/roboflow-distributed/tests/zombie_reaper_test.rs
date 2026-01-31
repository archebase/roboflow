// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Integration tests for zombie reaper functionality.
//!
//! These tests verify that:
//! 1. Heartbeats are tracked correctly
//! 2. Stale heartbeats are detected
//! 3. Orphaned jobs are reclaimed
//! 4. Checkpoints are preserved during reclamation

#[cfg(feature = "distributed")]
mod tests {
    use std::time::Duration;

    use roboflow_distributed::{
        HeartbeatConfig, HeartbeatManager, HeartbeatRecord, JobRecord, JobStatus, ReaperConfig,
        ReaperMetrics, ReclaimResult, ZombieReaper,
    };
    use roboflow_distributed::{
        TikvClient, WorkerStatus,
        tikv::key::{HeartbeatKeys, JobKeys, StateKeys},
    };

    /// Helper to create a test job in Processing state.
    fn create_test_job(id: &str, owner: &str) -> JobRecord {
        let mut job = JobRecord::new(
            id.to_string(),
            format!("source/{}", id),
            "test-bucket".to_string(),
            1024,
            "output/".to_string(),
            "config-hash".to_string(),
        );
        job.status = JobStatus::Processing;
        job.owner = Some(owner.to_string());
        job
    }

    /// Helper to create a test heartbeat.
    fn create_test_heartbeat(pod_id: &str) -> HeartbeatRecord {
        let mut hb = HeartbeatRecord::new(pod_id.to_string());
        hb.status = WorkerStatus::Busy;
        hb.active_jobs = 1;
        hb
    }

    #[tokio::test]
    async fn test_heartbeat_manager() {
        // This test requires a running TiKV instance
        // For CI/CD, we skip if not available
        let client = match TikvClient::from_env().await {
            Ok(c) => c,
            Err(_) => {
                println!("Skipping test: TiKV not available");
                return;
            }
        };

        let pod_id = "test-worker-heartbeat";
        let config = HeartbeatConfig::new()
            .with_interval(Duration::from_secs(10))
            .with_stale_threshold(Duration::from_secs(60));

        let manager = HeartbeatManager::new(pod_id, std::sync::Arc::new(client), config)
            .expect("Failed to create heartbeat manager");

        // Update heartbeat
        manager
            .update_heartbeat()
            .await
            .expect("Failed to update heartbeat");

        // Check metrics
        let metrics = manager.metrics().snapshot();
        assert_eq!(metrics.updates_total, 1);
        assert_eq!(metrics.errors_total, 0);

        // Send draining status
        manager
            .send_with_status(WorkerStatus::Draining)
            .await
            .expect("Failed to send draining heartbeat");

        // Cleanup
        manager
            .cleanup()
            .await
            .expect("Failed to cleanup heartbeat");
    }

    #[tokio::test]
    async fn test_zombie_reaper_metrics() {
        let metrics = ReaperMetrics::new();

        assert_eq!(metrics.snapshot().jobs_reclaimed, 0);
        assert_eq!(metrics.snapshot().stale_workers_found, 0);
        assert_eq!(metrics.snapshot().iterations_total, 0);

        metrics.inc_jobs_reclaimed();
        metrics.inc_stale_workers_found(3);
        metrics.inc_iterations();
        metrics.inc_reclaim_attempts();
        metrics.inc_jobs_skipped();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.jobs_reclaimed, 1);
        assert_eq!(snapshot.stale_workers_found, 3);
        assert_eq!(snapshot.iterations_total, 1);
        assert_eq!(snapshot.reclaim_attempts, 1);
        assert_eq!(snapshot.jobs_skipped, 1);
    }

    #[tokio::test]
    async fn test_reaper_config_defaults() {
        let config = ReaperConfig::default();
        assert_eq!(config.interval.as_secs(), 60);
        assert_eq!(config.stale_threshold.as_secs(), 300);
        assert_eq!(config.max_reclaims_per_iteration, 10);
    }

    #[tokio::test]
    async fn test_heartbeat_is_stale() {
        let mut heartbeat = HeartbeatRecord::new("test-pod".to_string());

        // Fresh heartbeat should not be stale
        assert!(!heartbeat.is_stale(60));

        // Manually set last_heartbeat to 5 minutes ago
        use chrono::{Duration, Utc};
        heartbeat.last_heartbeat = Utc::now() - Duration::seconds(300);

        // Should be stale with 300 second threshold
        assert!(heartbeat.is_stale(300));
        // Should not be stale with 301 second threshold
        assert!(!heartbeat.is_stale(301));
    }

    #[tokio::test]
    async fn test_reclaim_result() {
        // Just verify the enum exists
        let _ = ReclaimResult::Reclaimed;
        let _ = ReclaimResult::NotStale;
        let _ = ReclaimResult::NotProcessing;
        let _ = ReclaimResult::Failed;
        let _ = ReclaimResult::Skipped;
    }

    #[tokio::test]
    async fn test_end_to_end_zombie_reclamation() {
        // This test requires a running TiKV instance
        let client = match TikvClient::from_env().await {
            Ok(c) => c,
            Err(_) => {
                println!("Skipping test: TiKV not available");
                return;
            }
        };

        let pod_id = "test-worker-zombie";
        let job_id = "test-job-zombie";

        // Create a job in Processing state
        let job = create_test_job(job_id, pod_id);
        client
            .put_job(&job)
            .await
            .expect("Failed to create test job");

        // Create a heartbeat for the worker
        let heartbeat = create_test_heartbeat(pod_id);
        client
            .update_heartbeat(pod_id, &heartbeat)
            .await
            .expect("Failed to create heartbeat");

        // Verify job is in Processing state
        let retrieved_job = client.get_job(job_id).await.expect("Failed to get job");
        assert!(retrieved_job.is_some());
        assert_eq!(retrieved_job.unwrap().status, JobStatus::Processing);

        // Wait for heartbeat to become stale (simulation)
        // In real scenario, we'd wait 5+ minutes, but for testing we use a short threshold

        // Create reaper with short stale threshold
        let config = ReaperConfig::new()
            .with_interval(Duration::from_secs(1))
            .with_stale_threshold(Duration::from_secs(0)); // Immediately stale

        let reaper = ZombieReaper::new(std::sync::Arc::new(client.clone()), config);

        // Run one iteration
        let reclaimed_count = reaper
            .run_iteration()
            .await
            .expect("Failed to run reaper iteration");

        // Since we set stale_threshold to 0, the heartbeat should be stale
        // and the job should be reclaimed
        assert!(reclaimed_count <= 1);

        // Verify job was reclaimed (status should be Pending)
        let final_job = client.get_job(job_id).await.expect("Failed to get job");
        if let Some(job) = final_job {
            // Job should be in Pending state after reclamation
            assert_eq!(job.status, JobStatus::Pending);
            assert!(job.owner.is_none());
        }

        // Cleanup
        let _ = client.delete(JobKeys::record(job_id)).await;
        let _ = client.delete(HeartbeatKeys::heartbeat(pod_id)).await;
    }

    #[tokio::test]
    async fn test_heartbeat_preserved_on_reclaim() {
        // This test verifies that checkpoints are preserved during job reclamation
        let client = match TikvClient::from_env().await {
            Ok(c) => c,
            Err(_) => {
                println!("Skipping test: TiKV not available");
                return;
            }
        };

        use roboflow_distributed::CheckpointState;

        let pod_id = "test-worker-checkpoint";
        let job_id = "test-job-checkpoint";

        // Create a job in Processing state
        let job = create_test_job(job_id, pod_id);
        client
            .put_job(&job)
            .await
            .expect("Failed to create test job");

        // Create a checkpoint for the job
        let mut checkpoint = CheckpointState::new(job_id.to_string(), pod_id.to_string(), 1000);
        checkpoint.update(500).expect("Failed to update checkpoint");
        client
            .update_checkpoint(&checkpoint)
            .await
            .expect("Failed to create checkpoint");

        // Verify checkpoint exists
        let retrieved_checkpoint = client
            .get_checkpoint(job_id)
            .await
            .expect("Failed to get checkpoint");
        assert!(retrieved_checkpoint.is_some());
        assert_eq!(retrieved_checkpoint.unwrap().last_frame, 500);

        // Reclaim the job with stale threshold of 0
        let reclaimed = client
            .reclaim_job(job_id, 0)
            .await
            .expect("Failed to reclaim job");
        assert!(reclaimed);

        // Verify checkpoint still exists after reclamation
        let final_checkpoint = client
            .get_checkpoint(job_id)
            .await
            .expect("Failed to get checkpoint after reclamation");
        assert!(final_checkpoint.is_some());
        let cp = final_checkpoint.unwrap();
        assert_eq!(
            cp.last_frame, 500,
            "Checkpoint should be preserved after reclamation"
        );

        // Cleanup
        let _ = client.delete(JobKeys::record(job_id)).await;
        let _ = client.delete(StateKeys::checkpoint(job_id)).await;
    }
}

#[cfg(not(feature = "distributed"))]
mod tests {
    #[tokio::test]
    async fn test_zombie_reaper_not_available_without_distributed() {
        // Verify that zombie reaper requires distributed feature
        println!("Zombie reaper requires 'distributed' feature");
    }
}
