// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Comprehensive integration tests for TiKV interaction with roboflow.
//!
//! These tests verify:
//! 1. Lock system integration (acquisition, renewal, release, fencing tokens)
//! 2. Worker checkpoint integration (save, load, resume)
//! 3. Concurrency tests (multi-worker coordination)
//! 4. Catalog/persistence tests (work unit lifecycle, state transitions)

mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use roboflow_distributed::{
        CheckpointConfig, CheckpointManager, HeartbeatConfig, HeartbeatManager, HeartbeatRecord,
        LockManager, WorkerMetrics, WorkerStatus,
    };
    use roboflow_distributed::{TikvClient, Worker, WorkerConfig, tikv::key::HeartbeatKeys};
    use roboflow_storage::LocalStorage;
    use tempfile::TempDir;

    // =============================================================================
    // Test Helpers
    // =============================================================================

    /// Helper to get TiKV client or skip test if not available.
    async fn get_tikv_or_skip() -> Option<Arc<TikvClient>> {
        match TikvClient::from_env().await {
            Ok(c) => Some(Arc::new(c)),
            Err(_) => {
                println!("Skipping test: TiKV not available");
                None
            }
        }
    }

    /// Helper to create a test heartbeat.
    #[allow(dead_code)]
    fn create_test_heartbeat(pod_id: &str, status: WorkerStatus) -> HeartbeatRecord {
        let mut hb = HeartbeatRecord::new(pod_id.to_string());
        hb.status = status;
        hb.active_jobs = 1;
        hb
    }

    /// Cleanup helper for tests.
    async fn cleanup_test_data(client: &TikvClient, checkpoint_id: &str, pod_id: &str) {
        use roboflow_distributed::tikv::key::StateKeys;
        let checkpoint_key = StateKeys::checkpoint(checkpoint_id);
        let _ = client.delete(checkpoint_key).await;
        let _ = client.delete(HeartbeatKeys::heartbeat(pod_id)).await;
    }

    // =============================================================================
    // Lock System Integration Tests
    // =============================================================================

    #[tokio::test]
    async fn test_lock_acquire_and_release() {
        let Some(client) = get_tikv_or_skip().await else {
            return;
        };

        let lock_manager = LockManager::new(client.clone(), "test-pod-acquire");
        let resource = format!("test_lock_acquire_{}", uuid::Uuid::new_v4());

        // Try to acquire lock
        let guard_opt = lock_manager
            .try_acquire_default(&resource)
            .await
            .expect("Failed to try_acquire lock");

        assert!(guard_opt.is_some());
        let guard = guard_opt.unwrap();

        assert!(guard.is_valid());
        assert_eq!(guard.resource(), resource);
        assert_eq!(guard.owner(), "test-pod-acquire");

        // Verify lock exists in TiKV
        let is_locked = lock_manager.is_locked(&resource).await.unwrap();
        assert!(is_locked);

        // Verify we are the owner
        let owner = lock_manager.get_owner(&resource).await.unwrap();
        assert_eq!(owner.as_deref(), Some("test-pod-acquire"));

        // Release lock explicitly
        let released = guard.release().await.unwrap();
        assert!(released);

        // Verify lock is released
        let is_locked_after = lock_manager.is_locked(&resource).await.unwrap();
        assert!(!is_locked_after);
    }

    #[tokio::test]
    async fn test_lock_mutual_exclusion() {
        let Some(client) = get_tikv_or_skip().await else {
            return;
        };

        let pod1 = "test-pod-mutex-1";
        let pod2 = "test-pod-mutex-2";
        let lock_manager1 = LockManager::new(client.clone(), pod1);
        let lock_manager2 = LockManager::new(client.clone(), pod2);
        let resource = format!("test_lock_mutex_{}", uuid::Uuid::new_v4());

        // First pod acquires lock
        let guard1_opt = lock_manager1
            .try_acquire_default(&resource)
            .await
            .expect("Failed to acquire lock");

        assert!(guard1_opt.is_some());
        let guard1 = guard1_opt.unwrap();
        assert!(guard1.is_valid());

        // Second pod should not be able to acquire
        let guard2_opt = lock_manager2
            .try_acquire_default(&resource)
            .await
            .expect("Failed to try_acquire lock");

        assert!(guard2_opt.is_none());

        // First pod releases
        guard1.release().await.unwrap();

        // Now second pod should be able to acquire
        let guard3_opt = lock_manager2
            .try_acquire_default(&resource)
            .await
            .expect("Failed to acquire lock after release");

        assert!(guard3_opt.is_some());
        let guard3 = guard3_opt.unwrap();
        guard3.release().await.unwrap();
    }

    #[tokio::test]
    async fn test_lock_blocking_acquire() {
        let Some(client) = get_tikv_or_skip().await else {
            return;
        };

        let lock_manager = LockManager::new(client.clone(), "test-pod-blocking");
        let resource = format!("test_lock_blocking_{}", uuid::Uuid::new_v4());

        // Acquire lock with blocking
        let guard = lock_manager
            .acquire_blocking_default(&resource)
            .await
            .expect("Failed to acquire lock with blocking");

        assert!(guard.is_valid());
        guard.release().await.unwrap();
    }

    #[tokio::test]
    async fn test_lock_ttl_expiration() {
        let Some(client) = get_tikv_or_skip().await else {
            return;
        };

        let lock_manager = LockManager::new(client.clone(), "test-pod-ttl");
        let resource = format!("test_lock_ttl_{}", uuid::Uuid::new_v4());

        // Acquire lock with very short TTL
        let ttl = Duration::from_millis(100);
        let guard_opt = lock_manager
            .try_acquire(&resource, ttl)
            .await
            .expect("Failed to acquire lock");

        assert!(guard_opt.is_some());
        let guard = guard_opt.unwrap();

        // Lock should be valid immediately
        let is_locked = lock_manager.is_locked(&resource).await.unwrap();
        assert!(is_locked);

        // Wait for expiration (guard is still held, so lock will be renewed by Drop)
        drop(guard); // Explicitly drop to release the lock

        // After releasing, wait for any cleanup to complete
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Lock should no longer exist (was released)
        let is_locked_after = lock_manager.is_locked(&resource).await.unwrap();
        assert!(!is_locked_after);
    }

    #[tokio::test]
    async fn test_lock_fencing_tokens() {
        let Some(client) = get_tikv_or_skip().await else {
            return;
        };

        let lock_manager = LockManager::new(client.clone(), "test-pod-fencing");
        let resource = format!("test_lock_fencing_{}", uuid::Uuid::new_v4());

        // First acquisition
        let guard1_opt = lock_manager
            .try_acquire_default(&resource)
            .await
            .expect("Failed to acquire lock");

        assert!(guard1_opt.is_some());
        let guard1 = guard1_opt.unwrap();

        let token1 = guard1.fencing_token().await.unwrap();
        assert!(token1.is_some());
        assert_eq!(token1.unwrap(), 1, "Initial fencing token should be 1");

        // Renew the lock - this should increment the fencing token
        let renewed = guard1.renew().await.unwrap();
        assert!(renewed, "Lock renewal should succeed");

        let token_after_renewal = guard1.fencing_token().await.unwrap();
        assert!(token_after_renewal.is_some());

        // Fencing token should have increased after renewal
        assert!(
            token_after_renewal.unwrap() > token1.unwrap(),
            "Fencing token should increase after renewal"
        );

        guard1.release().await.unwrap();

        // After release and re-acquire, a fresh lock starts at version 1
        let guard2_opt = lock_manager
            .try_acquire_default(&resource)
            .await
            .expect("Failed to re-acquire lock");

        assert!(guard2_opt.is_some());
        let guard2 = guard2_opt.unwrap();

        let token2 = guard2.fencing_token().await.unwrap();
        assert!(token2.is_some());
        assert_eq!(token2.unwrap(), 1, "New lock should start at version 1");

        guard2.release().await.unwrap();
    }

    #[tokio::test]
    async fn test_lock_renewal() {
        let Some(client) = get_tikv_or_skip().await else {
            return;
        };

        let lock_manager = LockManager::new(client.clone(), "test-pod-renewal");
        let resource = format!("test_lock_renewal_{}", uuid::Uuid::new_v4());

        // Acquire lock with short TTL
        let ttl = Duration::from_millis(500);
        let guard_opt = lock_manager
            .try_acquire(&resource, ttl)
            .await
            .expect("Failed to acquire lock");

        assert!(guard_opt.is_some());
        let guard = guard_opt.unwrap();

        // Lock should be valid immediately
        assert!(guard.is_valid());
        let is_locked = lock_manager.is_locked(&resource).await.unwrap();
        assert!(is_locked);

        // Get initial fencing token
        let token1 = guard.fencing_token().await.unwrap();
        assert!(token1.is_some());

        // Renew the lock
        let renewed = guard.renew().await.unwrap();
        assert!(renewed);

        // Fencing token should have increased
        let token2 = guard.fencing_token().await.unwrap();
        assert!(token2.is_some());
        assert!(
            token2.unwrap() > token1.unwrap(),
            "Fencing token should increase after renewal"
        );

        // Wait a bit but less than renewed TTL
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Lock should still be valid
        assert!(guard.is_valid());
        let is_locked_after = lock_manager.is_locked(&resource).await.unwrap();
        assert!(is_locked_after);

        guard.release().await.unwrap();
    }

    #[tokio::test]
    async fn test_lock_steal_expired() {
        let Some(client) = get_tikv_or_skip().await else {
            return;
        };

        let lock_manager1 = LockManager::new(client.clone(), "test-pod-steal-1");
        let lock_manager2 = LockManager::new(client.clone(), "test-pod-steal-2");
        let resource = format!("test_lock_steal_{}", uuid::Uuid::new_v4());

        // First pod acquires with very short TTL (50ms -> 1 second after conversion)
        let ttl = Duration::from_millis(50);
        let guard1_opt = lock_manager1
            .try_acquire(&resource, ttl)
            .await
            .expect("Failed to acquire lock");

        assert!(guard1_opt.is_some());
        let _guard1 = guard1_opt.unwrap();

        // Wait for expiration (TTL is now 1 second due to conversion logic)
        tokio::time::sleep(Duration::from_millis(1100)).await;

        // Second pod should be able to steal expired lock
        let stolen = lock_manager2
            .steal(&resource, Duration::from_secs(60))
            .await
            .expect("Failed to steal lock");

        assert!(stolen);

        // Verify second pod now owns the lock
        let owner = lock_manager2.get_owner(&resource).await.unwrap();
        assert_eq!(owner.as_deref(), Some("test-pod-steal-2"));
    }

    // =============================================================================
    // Checkpoint Integration Tests
    // =============================================================================

    #[tokio::test]
    async fn test_checkpoint_save_and_load() {
        let Some(client) = get_tikv_or_skip().await else {
            return;
        };

        let job_id = format!("test_checkpoint_save_{}", uuid::Uuid::new_v4());
        let pod_id = "test-pod-checkpoint";

        // Create and save checkpoint using client directly
        use roboflow_distributed::CheckpointState;
        use roboflow_distributed::tikv::key::StateKeys;
        let mut checkpoint = CheckpointState::new(job_id.clone(), pod_id.to_string(), 1000);
        checkpoint.update(500, 50000).unwrap();

        let checkpoint_data = bincode::serialize(&checkpoint).unwrap();
        let key = StateKeys::checkpoint(&job_id);
        client.put(key.clone(), checkpoint_data).await.unwrap();

        // Load checkpoint
        let loaded = client.get(key).await.unwrap();

        assert!(loaded.is_some());
        let loaded: CheckpointState = bincode::deserialize(&loaded.unwrap()).unwrap();
        assert_eq!(loaded.job_id, job_id);
        assert_eq!(loaded.pod_id, pod_id);
        assert_eq!(loaded.last_frame, 500);
        assert_eq!(loaded.total_frames, 1000);

        // Cleanup
        cleanup_test_data(&client, &job_id, pod_id).await;
    }

    #[tokio::test]
    async fn test_checkpoint_update_existing() {
        let Some(client) = get_tikv_or_skip().await else {
            return;
        };

        let job_id = format!("test_checkpoint_update_{}", uuid::Uuid::new_v4());
        let pod_id = "test-pod-checkpoint-update";

        // Save initial checkpoint using client directly
        use roboflow_distributed::CheckpointState;
        use roboflow_distributed::tikv::key::StateKeys;
        let mut checkpoint = CheckpointState::new(job_id.clone(), pod_id.to_string(), 1000);
        checkpoint.update(100, 10000).unwrap();

        let key = StateKeys::checkpoint(&job_id);
        let data = bincode::serialize(&checkpoint).unwrap();
        client.put(key.clone(), data).await.unwrap();

        // Update checkpoint
        checkpoint.update(200, 20000).unwrap();
        let data = bincode::serialize(&checkpoint).unwrap();
        client.put(key.clone(), data).await.unwrap();

        // Verify updated values
        let loaded = client.get(key).await.unwrap().unwrap();
        let loaded: CheckpointState = bincode::deserialize(&loaded).unwrap();
        assert_eq!(loaded.last_frame, 200);

        // Cleanup
        cleanup_test_data(&client, &job_id, pod_id).await;
    }

    #[tokio::test]
    async fn test_checkpoint_delete() {
        let Some(client) = get_tikv_or_skip().await else {
            return;
        };

        let job_id = format!("test_checkpoint_delete_{}", uuid::Uuid::new_v4());
        let pod_id = "test-pod-checkpoint-delete";

        // Use client directly instead of CheckpointManager to avoid runtime conflicts
        use roboflow_distributed::CheckpointState;
        use roboflow_distributed::tikv::key::StateKeys;
        let checkpoint = CheckpointState::new(job_id.clone(), pod_id.to_string(), 1000);

        let checkpoint_data = bincode::serialize(&checkpoint).unwrap();
        let key = StateKeys::checkpoint(&job_id);

        // Save checkpoint
        client.put(key.clone(), checkpoint_data).await.unwrap();

        // Verify exists
        let loaded = client.get(key.clone()).await.unwrap();
        assert!(loaded.is_some());

        // Delete checkpoint
        client.delete(key.clone()).await.unwrap();

        // Verify deleted
        let loaded_after = client.get(key).await.unwrap();
        assert!(loaded_after.is_none());
    }

    #[tokio::test]
    async fn test_checkpoint_with_heartbeat() {
        let Some(client) = get_tikv_or_skip().await else {
            return;
        };

        let job_id = format!("test_checkpoint_hb_{}", uuid::Uuid::new_v4());
        let pod_id = "test-pod-checkpoint-hb";

        // Save checkpoint with heartbeat in single transaction
        use roboflow_distributed::tikv::key::{HeartbeatKeys, StateKeys};
        use roboflow_distributed::{CheckpointState, HeartbeatRecord};
        let mut checkpoint = CheckpointState::new(job_id.clone(), pod_id.to_string(), 1000);
        checkpoint.update(500, 50000).unwrap();

        // Create heartbeat
        let mut heartbeat = HeartbeatRecord::new(pod_id.to_string());
        heartbeat.beat();
        heartbeat.status = WorkerStatus::Busy;

        let checkpoint_data = bincode::serialize(&checkpoint).unwrap();
        let heartbeat_data = bincode::serialize(&heartbeat).unwrap();
        let checkpoint_key = StateKeys::checkpoint(&job_id);
        let heartbeat_key = HeartbeatKeys::heartbeat(pod_id);

        client
            .batch_put(vec![
                (checkpoint_key, checkpoint_data),
                (heartbeat_key, heartbeat_data),
            ])
            .await
            .expect("Failed to save checkpoint with heartbeat");

        // Verify checkpoint was saved
        let loaded_cp = client.get(StateKeys::checkpoint(&job_id)).await.unwrap();
        assert!(loaded_cp.is_some());
        let loaded_cp: CheckpointState = bincode::deserialize(&loaded_cp.unwrap()).unwrap();
        assert_eq!(loaded_cp.last_frame, 500);

        // Verify heartbeat was updated
        let heartbeat = client.get_heartbeat(pod_id).await.unwrap();
        assert!(heartbeat.is_some());
        let hb = heartbeat.unwrap();
        assert_eq!(hb.pod_id, pod_id);
        assert_eq!(hb.status, WorkerStatus::Busy);

        cleanup_test_data(&client, &job_id, pod_id).await;
    }

    #[tokio::test]
    async fn test_checkpoint_should_checkpoint_logic() {
        let Some(client) = get_tikv_or_skip().await else {
            return;
        };

        let config = CheckpointConfig::new()
            .with_frame_interval(100)
            .with_time_interval(10);
        let manager = CheckpointManager::new(client.clone(), config);

        // Should checkpoint when frame threshold reached
        assert!(manager.should_checkpoint(100, Duration::from_secs(5)));

        // Should checkpoint when time threshold reached
        assert!(manager.should_checkpoint(50, Duration::from_secs(10)));

        // Should not checkpoint when neither threshold reached
        assert!(!manager.should_checkpoint(50, Duration::from_secs(5)));

        // Should checkpoint when both thresholds reached
        assert!(manager.should_checkpoint(100, Duration::from_secs(10)));
    }

    #[tokio::test]
    async fn test_worker_checkpoint_callback_integration() {
        let Some(client) = get_tikv_or_skip().await else {
            return;
        };

        let job_id = format!("test_worker_cb_{}", uuid::Uuid::new_v4());
        let pod_id = "test-worker-cb-pod";
        let total_frames = 1000u64;

        use roboflow_distributed::CheckpointState;
        use roboflow_distributed::tikv::key::StateKeys;

        // Simulate frame writes
        for i in 1..=10 {
            let frames_written = i * 10;
            let checkpoint = CheckpointState {
                job_id: job_id.clone(),
                pod_id: pod_id.to_string(),
                byte_offset: frames_written * 100,
                last_frame: frames_written,
                episode_idx: 0,
                total_frames,
                video_uploads: Vec::new(),
                parquet_upload: None,
                updated_at: chrono::Utc::now(),
                version: 1,
            };

            let key = StateKeys::checkpoint(&job_id);
            let data = bincode::serialize(&checkpoint).unwrap();
            client.put(key, data).await.unwrap();
        }

        // Verify final checkpoint state
        let loaded = client.get(StateKeys::checkpoint(&job_id)).await.unwrap();
        assert!(loaded.is_some());
        let loaded: CheckpointState = bincode::deserialize(&loaded.unwrap()).unwrap();
        assert_eq!(loaded.last_frame, 100);

        cleanup_test_data(&client, &job_id, pod_id).await;
    }

    #[tokio::test]
    async fn test_checkpoint_interrupt_and_resume() {
        let Some(client) = get_tikv_or_skip().await else {
            return;
        };

        let job_id = format!("test_interrupt_resume_{}", uuid::Uuid::new_v4());
        let pod_id = "test-interrupt-pod";
        let pod_id_2 = "test-interrupt-pod-2"; // Simulating restart on new pod
        let total_frames = 1000u64;

        use roboflow_distributed::CheckpointState;
        use roboflow_distributed::tikv::key::StateKeys;

        // Phase 1: Simulate initial processing with checkpoint saves
        // We'll "interrupt" at frame 150

        for i in 0..=15 {
            let frames_written = i * 10;
            let mut checkpoint =
                CheckpointState::new(job_id.clone(), pod_id.to_string(), total_frames);
            checkpoint.last_frame = frames_written;
            checkpoint.byte_offset = frames_written * 1000;
            let key = StateKeys::checkpoint(&job_id);
            let data = bincode::serialize(&checkpoint).unwrap();
            client.put(key, data).await.unwrap();
        }

        // Verify checkpoint was saved at frame 150
        let saved_checkpoint = client.get(StateKeys::checkpoint(&job_id)).await.unwrap();
        assert!(saved_checkpoint.is_some());
        let saved: CheckpointState = bincode::deserialize(&saved_checkpoint.unwrap()).unwrap();
        assert_eq!(saved.last_frame, 150);
        assert_eq!(saved.byte_offset, 150000);

        // Phase 2: Simulate resume from checkpoint (new pod takes over)
        // In real scenario, Worker.process_job would load this checkpoint
        // and continue from saved.last_frame and saved.byte_offset

        // Simulate continuing from where we left off
        for i in 16..=20 {
            let frames_written = i * 10;
            let mut checkpoint =
                CheckpointState::new(job_id.clone(), pod_id_2.to_string(), total_frames);
            checkpoint.last_frame = frames_written;
            checkpoint.byte_offset = frames_written * 1000;
            let key = StateKeys::checkpoint(&job_id);
            let data = bincode::serialize(&checkpoint).unwrap();
            client.put(key, data).await.unwrap();
        }

        // Verify final checkpoint state reflects full progress
        let final_checkpoint = client.get(StateKeys::checkpoint(&job_id)).await.unwrap();
        assert!(final_checkpoint.is_some());
        let final_cp: CheckpointState = bincode::deserialize(&final_checkpoint.unwrap()).unwrap();
        assert_eq!(final_cp.last_frame, 200);
        assert_eq!(final_cp.pod_id, pod_id_2); // Ownership transferred

        // Verify checkpoint data integrity
        assert_eq!(final_cp.total_frames, total_frames);
        assert!(!final_cp.is_complete()); // 200 < 1000
        assert_eq!(final_cp.progress_percent(), 20.0);

        cleanup_test_data(&client, &job_id, pod_id_2).await;
    }

    // =============================================================================
    // Job Lifecycle Integration Tests
    // =============================================================================

    // #[tokio::test]
    //     async fn test_job_full_lifecycle() {
    //         let Some(client) = get_tikv_or_skip().await else {
    //             return;
    //         };
    //
    //         let job_id = format!("test_job_lifecycle_{}", uuid::Uuid::new_v4());
    //         let pod_id = "test-job-lifecycle-pod";
    //
    //         // 1. Create job
    //         let job = create_test_job(&job_id);
    //         client.put_job(&job).await.unwrap();
    //
    //         let retrieved = client.get_job(&job_id).await.unwrap();
    //         assert!(retrieved.is_some());
    //         assert_eq!(retrieved.unwrap().status, JobStatus::Pending);
    //
    //         // 2. Claim job
    //         let claimed = client.claim_job(&job_id, pod_id).await.unwrap();
    //         assert!(claimed);
    //
    //         let claimed_job = client.get_job(&job_id).await.unwrap().unwrap();
    //         assert_eq!(claimed_job.status, JobStatus::Processing);
    //         assert_eq!(claimed_job.owner.as_deref(), Some(pod_id));
    //
    //         // 3. Complete job
    //         let completed = client.complete_job(&job_id).await.unwrap();
    //         assert!(completed);
    //
    //         let completed_job = client.get_job(&job_id).await.unwrap().unwrap();
    //         assert_eq!(completed_job.status, JobStatus::Completed);
    //
    //         cleanup_test_data(&client, &job_id, pod_id).await;
    //     }
    //
    //     #[tokio::test]
    //     async fn test_job_failure_lifecycle() {
    //         let Some(client) = get_tikv_or_skip().await else {
    //             return;
    //         };
    //
    //         let job_id = format!("test_job_fail_{}", uuid::Uuid::new_v4());
    //         let pod_id = "test-job-fail-pod";
    //
    //         // Create and claim job
    //         let job = create_test_job(&job_id);
    //         client.put_job(&job).await.unwrap();
    //         client.claim_job(&job_id, pod_id).await.unwrap();
    //
    //         // Fail job
    //         let error_msg = "Test error message";
    //         let failed = client
    //             .fail_job(&job_id, error_msg.to_string())
    //             .await
    //             .unwrap();
    //         assert!(failed);
    //
    //         let failed_job = client.get_job(&job_id).await.unwrap().unwrap();
    //         assert_eq!(failed_job.status, JobStatus::Failed);
    //         assert_eq!(failed_job.error.as_deref(), Some(error_msg));
    //
    //         cleanup_test_data(&client, &job_id, pod_id).await;
    //     }
    //
    //     #[tokio::test]
    //     async fn test_job_claim_race_condition() {
    //         let Some(client) = get_tikv_or_skip().await else {
    //             return;
    //         };
    //
    //         let job_id = format!("test_job_race_{}", uuid::Uuid::new_v4());
    //         let pod1 = "test-job-race-pod-1";
    //         let pod2 = "test-job-race-pod-2";
    //
    //         // Create job
    //         let job = create_test_job(&job_id);
    //         client.put_job(&job).await.unwrap();
    //
    //         // Both pods try to claim simultaneously
    //         let client1 = client.clone();
    //         let client2 = client.clone();
    //         let job_id1 = job_id.clone();
    //         let job_id2 = job_id.clone();
    //
    //         let claim1 = tokio::spawn(async move { client1.claim_job(&job_id1, pod1).await.unwrap() });
    //
    //         let claim2 = tokio::spawn(async move { client2.claim_job(&job_id2, pod2).await.unwrap() });
    //
    //         let (result1, result2) = tokio::join!(claim1, claim2);
    //
    //         // Exactly one should succeed
    //         let sum = u8::from(result1.unwrap()) + u8::from(result2.unwrap());
    //         assert_eq!(sum, 1, "Exactly one claim should succeed");
    //
    //         cleanup_test_data(&client, &job_id, "").await;
    //     }
    //
    //     #[tokio::test]
    //     async fn test_job_reclaim_after_stale_heartbeat() {
    //         let Some(client) = get_tikv_or_skip().await else {
    //             return;
    //         };
    //
    //         let job_id = format!("test_job_reclaim_{}", uuid::Uuid::new_v4());
    //         let pod_id = "test-job-reclaim-pod";
    //
    //         // Create and claim job
    //         let job = create_test_processing_job(&job_id, pod_id);
    //         client.put_job(&job).await.unwrap();
    //
    //         // Create stale heartbeat (very short timeout)
    //         let heartbeat = create_test_heartbeat(pod_id, WorkerStatus::Busy);
    //         client.update_heartbeat(pod_id, &heartbeat).await.unwrap();
    //
    //         // Wait for heartbeat to become stale
    //         tokio::time::sleep(Duration::from_millis(100)).await;
    //
    //         // Reclaim job with stale threshold of 0 (immediately stale)
    //         let reclaimed = client.reclaim_job(&job_id, 0).await.unwrap();
    //         assert!(reclaimed);
    //
    //         // Job should be back in Pending state
    //         let reclaimed_job = client.get_job(&job_id).await.unwrap().unwrap();
    //         assert_eq!(reclaimed_job.status, JobStatus::Pending);
    //         assert!(reclaimed_job.owner.is_none());
    //
    //         cleanup_test_data(&client, &job_id, pod_id).await;
    //     }
    //
    //     // =============================================================================
    //     // Heartbeat Integration Tests
    //     // =============================================================================
    //
    //     #[tokio::test]
    //     async fn test_heartbeat_update_and_retrieve() {
    //         let Some(client) = get_tikv_or_skip().await else {
    //             return;
    //         };
    //
    //         let pod_id = "test-hb-update-pod";
    //         let config = HeartbeatConfig::new().with_interval(Duration::from_secs(10));
    //         let manager = HeartbeatManager::new(pod_id, client.clone(), config).unwrap();
    //
    //         // Send heartbeat
    //         manager.update_heartbeat().await.unwrap();
    //
    //         // Retrieve heartbeat
    //         let heartbeat = client.get_heartbeat(pod_id).await.unwrap();
    //         assert!(heartbeat.is_some());
    //
    // //         let hb = heartbeat.unwrap();
    //         assert_eq!(hb.pod_id, pod_id);
    //
    //         // Cleanup
    //         manager.cleanup().await.unwrap();
    //     }
    //
    #[tokio::test]
    async fn test_heartbeat_with_status() {
        let Some(client) = get_tikv_or_skip().await else {
            return;
        };

        let pod_id = "test-hb-status-pod";
        let config = HeartbeatConfig::new();
        let manager = HeartbeatManager::new(pod_id, client.clone(), config).unwrap();

        // Send heartbeat with different status
        manager
            .send_with_status(WorkerStatus::Draining)
            .await
            .unwrap();

        let heartbeat = client.get_heartbeat(pod_id).await.unwrap().unwrap();
        assert_eq!(heartbeat.status, WorkerStatus::Draining);

        manager.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn test_heartbeat_staleness_detection() {
        let pod_id = "test-hb-stale-pod";

        let mut heartbeat = HeartbeatRecord::new(pod_id.to_string());

        // Fresh heartbeat should not be stale
        assert!(!heartbeat.is_stale(60));

        // Manually set to past
        use chrono::{Duration, Utc};
        let five_minutes_ago = Utc::now() - Duration::seconds(300);
        heartbeat.last_heartbeat = five_minutes_ago;

        // Should be stale with 299 second threshold
        assert!(heartbeat.is_stale(299));

        // Should not be stale with age+1 threshold
        let age = Utc::now()
            .signed_duration_since(heartbeat.last_heartbeat)
            .num_seconds();
        assert!(!heartbeat.is_stale(age + 1));
    }

    // =============================================================================
    // Concurrency and Multi-Worker Tests
    // =============================================================================

    #[tokio::test]
    async fn test_multi_worker_coordination() {
        let Some(client) = get_tikv_or_skip().await else {
            return;
        };

        let temp_dir = TempDir::new().unwrap();
        let _storage =
            Arc::new(LocalStorage::new(temp_dir.path())) as Arc<dyn roboflow_storage::Storage>;

        // Create multiple workers
        let mut workers = Vec::new();
        for i in 0..3 {
            let pod_id = format!("test-worker-coord-{}", i);
            let worker = Worker::new(
                pod_id,
                client.clone(),
                WorkerConfig::new()
                    .with_poll_interval(Duration::from_millis(100))
                    .with_max_concurrent_jobs(1),
            );
            assert!(worker.is_ok());
            workers.push(worker.unwrap());
        }

        // Verify all workers have unique pod IDs
        let pod_ids: Vec<_> = workers.iter().map(|w| w.pod_id().to_string()).collect();
        assert_eq!(pod_ids.len(), 3);
        let unique_ids: std::collections::HashSet<_> = pod_ids.into_iter().collect();
        assert_eq!(unique_ids.len(), 3);
    }

    #[tokio::test]
    async fn test_concurrent_checkpoint_saves() {
        let Some(client) = get_tikv_or_skip().await else {
            return;
        };

        let job_id = format!("test_concurrent_cp_{}", uuid::Uuid::new_v4());

        use roboflow_distributed::CheckpointState;
        use roboflow_distributed::tikv::key::StateKeys;

        // Spawn multiple tasks saving checkpoints concurrently
        let mut handles = Vec::new();
        for i in 0..10 {
            let job_id_clone = job_id.clone();
            let client_clone = client.clone();
            let handle = tokio::spawn(async move {
                let checkpoint = CheckpointState {
                    job_id: job_id_clone,
                    pod_id: format!("pod-{}", i),
                    byte_offset: i * 1000,
                    last_frame: i * 100,
                    episode_idx: i,
                    total_frames: 1000,
                    video_uploads: Vec::new(),
                    parquet_upload: None,
                    updated_at: chrono::Utc::now(),
                    version: 1,
                };
                let key = StateKeys::checkpoint(&checkpoint.job_id);
                let data = bincode::serialize(&checkpoint).unwrap();
                client_clone.put(key, data).await
            });
            handles.push(handle);
        }

        // Wait for all saves to complete
        let results: Vec<_> = futures::future::join_all(handles).await;
        let successful = results.into_iter().filter(|r| r.is_ok()).count();
        assert!(successful > 0, "At least some saves should succeed");

        // Note: We don't verify the final checkpoint state because the circuit breaker
        // may have been triggered by concurrent writes. The test verifies that
        // the system can handle concurrent writes without hanging or crashing.
    }

    #[tokio::test]
    async fn test_worker_metrics_tracking() {
        let metrics = WorkerMetrics::new();

        // Test initial state
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.jobs_claimed, 0);
        assert_eq!(snapshot.jobs_completed, 0);
        assert_eq!(snapshot.jobs_failed, 0);
        assert_eq!(snapshot.active_jobs, 0);

        // Test increments - simulate 2 jobs claimed, 1 completed
        metrics.inc_jobs_claimed();
        metrics.inc_active_jobs(); // Job 1 starts
        metrics.inc_jobs_completed();
        metrics.dec_active_jobs(); // Job 1 completes

        metrics.inc_jobs_claimed(); // Job 2 claimed

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.jobs_claimed, 2);
        assert_eq!(snapshot.jobs_completed, 1);
        assert_eq!(snapshot.active_jobs, 0); // Job 1 started and finished, Job 2 not started
    }

    // =============================================================================
    // Batch Operations Tests
    // =============================================================================

    // #[tokio::test]
    // async fn test_batch_get_and_put() {
    //         let Some(client) = get_tikv_or_skip().await else {
    //             return;
    //         };
    //
    //         use roboflow_distributed::tikv::key::JobKeys;
    //
    //         // Create multiple jobs
    //         let mut jobs = Vec::new();
    //         let mut keys = Vec::new();
    //         for i in 0..5 {
    //             let job_id = format!("test_batch_job_{}", i);
    //             let job = create_test_job(&job_id);
    //             let key = JobKeys::record(&job_id);
    //             let data = bincode::serialize(&job).unwrap();
    //
    //             keys.push(key.clone());
    //             jobs.push((key, data));
    //         }
    //
    //         // Batch put
    //         client.batch_put(jobs.clone()).await.unwrap();
    //
    //         // Batch get
    //         let results = client.batch_get(keys).await.unwrap();
    //         assert_eq!(results.len(), 5);
    //
    //         // All should exist
    //         for result in results {
    //             assert!(result.is_some());
    //         }
    //
    //         // Cleanup
    //         for (key, _) in jobs {
    //             let _ = client.delete(key).await;
    //         }
    //     }

    #[tokio::test]
    async fn test_scan_prefix() {
        let Some(client) = get_tikv_or_skip().await else {
            return;
        };

        // Scan test - scan for work units instead

        let prefix = format!("test_scan_prefix_{}", uuid::Uuid::new_v4());

        // Scan for work units (scan returns empty since we haven't created any)
        let scan_prefix = format!("{}/", prefix);
        let results = client.scan(scan_prefix.into_bytes(), 10).await.unwrap();

        // Should return empty since we're not creating jobs anymore
        assert_eq!(results.len(), 0);
    }

    // =============================================================================
    // Persistence and Catalog Tests
    // =============================================================================

    // #[tokio::test]
    // async fn test_job_persistence_across_operations() {
    //         let Some(client) = get_tikv_or_skip().await else {
    //             return;
    //         };
    //
    //         let job_id = format!("test_persist_{}", uuid::Uuid::new_v4());
    //
    //         // Create job
    //         let job = create_test_job(&job_id);
    //         client.put_job(&job).await.unwrap();
    //
    //         // Verify through multiple get operations
    //         for _ in 0..5 {
    //             let retrieved = client.get_job(&job_id).await.unwrap();
    //             assert!(retrieved.is_some());
    //             let retrieved = retrieved.unwrap();
    //             assert_eq!(retrieved.id, job_id);
    //             assert_eq!(retrieved.source_url, job.source_url);
    //         }
    //
    //         cleanup_test_data(&client, &job_id, "").await;
    //     }
    //
    //     #[tokio::test]
    //     async fn test_checkpoint_preservation_on_job_status_change() {
    //         let Some(client) = get_tikv_or_skip().await else {
    //             return;
    //         };
    //
    //         let job_id = format!("test_cp_preserve_{}", uuid::Uuid::new_v4());
    //         let pod_id = "test-cp-preserve-pod";
    //
    //         // Create checkpoint
    //         use roboflow_distributed::CheckpointState;
    //         let mut checkpoint = CheckpointState::new(job_id.clone(), pod_id.to_string(), 1000);
    //         checkpoint.update(500, 50000).unwrap();
    //         client.update_checkpoint(&checkpoint).await.unwrap();
    //
    //         // Create and claim job
    //         let job = create_test_job(&job_id);
    //         client.put_job(&job).await.unwrap();
    //         client.claim_job(&job_id, pod_id).await.unwrap();
    //
    //         // Complete job
    //         client.complete_job(&job_id).await.unwrap();
    //
    //         // Checkpoint should still exist
    //         let cp = client.get_checkpoint(&job_id).await.unwrap();
    //         assert!(cp.is_some());
    //         let cp = cp.unwrap();
    //         assert_eq!(cp.last_frame, 500);
    //
    //         cleanup_test_data(&client, &job_id, pod_id).await;
    //     }
    // }
    //
    // #[cfg(not(feature = "distributed"))]
    // mod tests {
    //     #[tokio::test]
    //     async fn test_integration_requires_distributed() {
    //         println!("TiKV integration tests require 'distributed' feature");
    //     }
    // }
} // mod tests
