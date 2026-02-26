// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Integration tests for zombie reaper functionality.
//!
//! These tests verify that:
//! 1. Heartbeats are tracked correctly
//! 2. Stale heartbeats are detected
//! 3. Orphaned work units are reclaimed
//! 4. Failed work units can be retried

mod tests {
    use std::time::Duration as StdDuration;

    use roboflow_distributed::batch::WorkUnitKeys;
    use roboflow_distributed::tikv::key::HeartbeatKeys;
    use roboflow_distributed::{
        HeartbeatConfig, HeartbeatManager, HeartbeatRecord, ReaperConfig, ReaperMetrics,
        ReclaimResult, ZombieReaper,
    };
    use roboflow_distributed::{TikvClient, WorkerStatus};
    use roboflow_distributed::{WorkFile, WorkUnit, WorkUnitStatus};

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

        let pod_id = format!("test-worker-heartbeat-{}", uuid::Uuid::new_v4());
        let config = HeartbeatConfig::new()
            .with_interval(StdDuration::from_secs(10))
            .with_stale_threshold(StdDuration::from_secs(60));

        // Clean up any existing heartbeat first
        let key = roboflow_distributed::tikv::key::HeartbeatKeys::heartbeat(&pod_id);
        let _ = client.delete(key).await;

        let manager = HeartbeatManager::new(&pod_id, std::sync::Arc::new(client), config)
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

        assert_eq!(metrics.snapshot().work_units_reclaimed, 0);
        assert_eq!(metrics.snapshot().stale_workers_found, 0);
        assert_eq!(metrics.snapshot().iterations_total, 0);

        metrics.inc_work_units_reclaimed();
        metrics.inc_stale_workers_found(3);
        metrics.inc_iterations();
        metrics.inc_reclaim_attempts();
        metrics.inc_work_units_skipped();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.work_units_reclaimed, 1);
        assert_eq!(snapshot.stale_workers_found, 3);
        assert_eq!(snapshot.iterations_total, 1);
        assert_eq!(snapshot.reclaim_attempts, 1);
        assert_eq!(snapshot.work_units_skipped, 1);
    }

    #[tokio::test]
    async fn test_reaper_config_defaults() {
        let config = ReaperConfig::default();
        assert_eq!(config.interval.as_secs(), 60);
        assert_eq!(config.stale_threshold.as_secs(), 300);
        assert_eq!(config.max_reclaims_per_iteration, 10);
        assert_eq!(config.max_work_unit_scan, 1000);
    }

    #[tokio::test]
    async fn test_heartbeat_is_stale() {
        let mut heartbeat = HeartbeatRecord::new("test-pod".to_string());

        // Fresh heartbeat should not be stale
        assert!(!heartbeat.is_stale(60));

        // Manually set last_heartbeat to 5 minutes ago
        use chrono::{Duration, Utc};
        let five_minutes_ago = Utc::now() - Duration::seconds(300);
        heartbeat.last_heartbeat = five_minutes_ago;

        // Age should be approximately 300 seconds
        let age = Utc::now()
            .signed_duration_since(heartbeat.last_heartbeat)
            .num_seconds();
        assert!((300..=302).contains(&age), "Expected age ~300, got {}", age);

        // Should be stale with 299 second threshold (age > threshold)
        assert!(heartbeat.is_stale(299));
        // Should not be stale with age+1 threshold (uses >, not >=)
        assert!(!heartbeat.is_stale(age + 1));
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

        let pod_id = format!("test-worker-zombie-{}", uuid::Uuid::new_v4());
        let batch_id = format!("test-batch-zombie-{}", uuid::Uuid::new_v4());
        let unit_id = format!("test-unit-zombie-{}", uuid::Uuid::new_v4());

        // Create a work unit in Processing state
        let mut work_unit = WorkUnit::with_id(
            unit_id.to_string(),
            batch_id.to_string(),
            vec![WorkFile::new(
                "s3://test-bucket/file.mcap".to_string(),
                1024,
            )],
            "s3://test-output/".to_string(),
            "test-config-hash".to_string(),
        );
        work_unit
            .claim(pod_id.to_string())
            .expect("Failed to claim work unit");
        let work_unit_key = WorkUnitKeys::unit(&batch_id, &unit_id);
        client
            .put(
                work_unit_key.clone(),
                bincode::serialize(&work_unit).expect("Failed to serialize work unit"),
            )
            .await
            .expect("Failed to create test work unit");

        // Create a heartbeat for the worker with last_heartbeat in the past
        let mut heartbeat = HeartbeatRecord::new(pod_id.to_string());
        use chrono::{Duration, Utc};
        heartbeat.last_heartbeat = Utc::now() - Duration::seconds(10); // 10 seconds old
        let heartbeat_key = HeartbeatKeys::heartbeat(&pod_id);
        client
            .put(
                heartbeat_key.clone(),
                bincode::serialize(&heartbeat).expect("Failed to serialize heartbeat"),
            )
            .await
            .expect("Failed to create heartbeat");

        // Verify work unit is in Processing state
        let retrieved_data = client
            .get(work_unit_key.clone())
            .await
            .expect("Failed to get work unit");
        assert!(retrieved_data.is_some());
        let retrieved_unit: WorkUnit = bincode::deserialize(&retrieved_data.unwrap())
            .expect("Failed to deserialize work unit");
        assert_eq!(retrieved_unit.status, WorkUnitStatus::Processing);
        assert_eq!(retrieved_unit.owner, Some(pod_id.to_string()));

        // Create reaper with short stale threshold
        let config = ReaperConfig::new()
            .with_interval(StdDuration::from_secs(1))
            .with_stale_threshold(StdDuration::from_secs(5)); // Stale after 5 seconds

        let reaper = ZombieReaper::new(std::sync::Arc::new(client.clone()), config);

        // Run one iteration
        let reclaimed_count = reaper
            .run_iteration()
            .await
            .expect("Failed to run reaper iteration");

        // Since we set stale_threshold to 5 seconds, the heartbeat should be stale
        // and the work unit should be reclaimed
        assert_eq!(
            reclaimed_count, 1,
            "Expected exactly one work unit to be reclaimed"
        );

        // Verify work unit was reclaimed (status should be Failed)
        let final_data = client
            .get(work_unit_key.clone())
            .await
            .expect("Failed to get work unit");
        assert!(
            final_data.is_some(),
            "Work unit should exist after reclamation"
        );
        if let Some(data) = final_data {
            let final_unit: WorkUnit =
                bincode::deserialize(&data).expect("Failed to deserialize work unit");
            // Work unit should be in Failed state after reclamation (allows retry)
            assert_eq!(final_unit.status, WorkUnitStatus::Failed);
            assert!(final_unit.owner.is_none());
            assert!(final_unit.error.is_some());
            assert!(
                final_unit
                    .error
                    .as_ref()
                    .unwrap()
                    .contains("Worker died during processing")
            );
        }

        // Cleanup
        let _ = client.delete(work_unit_key).await;
        let _ = client.delete(heartbeat_key).await;
    }
}
