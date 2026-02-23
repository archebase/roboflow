// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Integration tests for BatchController functionality.
//!
//! These tests verify the core controller operations including:
//! - Batch submission and status tracking
//! - Work unit lifecycle (claim, complete, fail)
//! - Phase transitions and reconciliation
//! - Failed batch recovery (manual retry scenario)

use roboflow_distributed::batch::{
    BatchController, BatchIndexKeys, BatchKeys, BatchPhase, BatchSpec, BatchStatus, WorkFile,
    WorkUnit, WorkUnitKeys, WorkUnitStatus,
};
use roboflow_distributed::tikv::client::TikvClient;
use std::sync::Arc;

/// Generate a unique batch ID for testing to avoid conflicts between tests
fn unique_batch_id(prefix: &str) -> String {
    format!("jobs:{}-{}", prefix, uuid::Uuid::new_v4())
}

/// Helper to skip tests when TiKV is unavailable
async fn get_tikv_client() -> Option<Arc<TikvClient>> {
    match TikvClient::from_env().await {
        Ok(client) => Some(Arc::new(client)),
        Err(e) => {
            println!("Skipping test: TiKV not available: {}", e);
            None
        }
    }
}

#[tokio::test]
async fn test_submit_batch_creates_spec_status_and_phase_index() {
    let tikv = match get_tikv_client().await {
        Some(client) => client,
        None => return,
    };
    let controller = BatchController::with_client(tikv.clone());

    let batch_id = unique_batch_id("test-submit");
    let batch_name = batch_id.strip_prefix("jobs:").unwrap();

    // Create and submit batch
    let spec = BatchSpec::new(
        batch_name,
        vec!["s3://test/file.bag".to_string()],
        "s3://output/".to_string(),
    );

    let result = controller.submit_batch(&spec).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), batch_id);

    // Verify spec was created
    let spec_data = tikv.get(BatchKeys::spec(&batch_id)).await.unwrap();
    assert!(spec_data.is_some());

    // Verify status was created with Pending phase
    let status_data = tikv.get(BatchKeys::status(&batch_id)).await.unwrap();
    assert!(status_data.is_some());
    let status: BatchStatus = bincode::deserialize(&status_data.unwrap()).unwrap();
    assert_eq!(status.phase, BatchPhase::Pending);

    // Verify phase index was created
    let phase_data = tikv
        .get(BatchIndexKeys::phase(BatchPhase::Pending, &batch_id))
        .await
        .unwrap();
    assert!(phase_data.is_some());

    // Cleanup
    let _ = tikv.delete(BatchKeys::spec(&batch_id)).await;
    let _ = tikv.delete(BatchKeys::status(&batch_id)).await;
    let _ = tikv
        .delete(BatchIndexKeys::phase(BatchPhase::Pending, &batch_id))
        .await;
}

#[tokio::test]
async fn test_get_batch_status_returns_none_for_nonexistent() {
    let tikv = match get_tikv_client().await {
        Some(client) => client,
        None => return,
    };
    let controller = BatchController::with_client(tikv.clone());

    let status = controller
        .get_batch_status("jobs:nonexistent-batch")
        .await
        .unwrap();
    assert!(status.is_none());
}

#[tokio::test]
async fn test_get_batch_status_returns_status_for_existing() {
    let tikv = match get_tikv_client().await {
        Some(client) => client,
        None => return,
    };
    let controller = BatchController::with_client(tikv.clone());

    let batch_id = unique_batch_id("test-get-status");
    let batch_name = batch_id.strip_prefix("jobs:").unwrap();

    // Create batch
    let spec = BatchSpec::new(
        batch_name,
        vec!["s3://test/file.bag".to_string()],
        "s3://output/".to_string(),
    );
    controller.submit_batch(&spec).await.unwrap();

    // Get status
    let status = controller.get_batch_status(&batch_id).await.unwrap();
    assert!(status.is_some());
    let status = status.unwrap();
    assert_eq!(status.phase, BatchPhase::Pending);

    // Cleanup
    let _ = tikv.delete(BatchKeys::spec(&batch_id)).await;
    let _ = tikv.delete(BatchKeys::status(&batch_id)).await;
    let _ = tikv
        .delete(BatchIndexKeys::phase(BatchPhase::Pending, &batch_id))
        .await;
}

#[tokio::test]
async fn test_cancel_batch_transitions_to_cancelled() {
    let tikv = match get_tikv_client().await {
        Some(client) => client,
        None => return,
    };
    let controller = BatchController::with_client(tikv.clone());

    let batch_id = unique_batch_id("test-cancel");
    let batch_name = batch_id.strip_prefix("jobs:").unwrap();

    // Create batch in Running phase
    let spec = BatchSpec::new(
        batch_name,
        vec!["s3://test/file.bag".to_string()],
        "s3://output/".to_string(),
    );
    controller.submit_batch(&spec).await.unwrap();

    // Transition to Running
    let mut status = BatchStatus::new();
    status.transition_to(BatchPhase::Running);
    let status_key = BatchKeys::status(&batch_id);
    tikv.put(status_key.clone(), bincode::serialize(&status).unwrap())
        .await
        .unwrap();

    // Cancel the batch
    let cancelled = controller.cancel_batch(&batch_id).await.unwrap();
    assert!(cancelled);

    // Verify status is Cancelled
    let updated_status = controller
        .get_batch_status(&batch_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(updated_status.phase, BatchPhase::Cancelled);

    // Cleanup
    let _ = tikv.delete(BatchKeys::spec(&batch_id)).await;
    let _ = tikv.delete(BatchKeys::status(&batch_id)).await;
    let _ = tikv
        .delete(BatchIndexKeys::phase(BatchPhase::Pending, &batch_id))
        .await;
    let _ = tikv
        .delete(BatchIndexKeys::phase(BatchPhase::Running, &batch_id))
        .await;
    let _ = tikv
        .delete(BatchIndexKeys::phase(BatchPhase::Cancelled, &batch_id))
        .await;
}

#[tokio::test]
async fn test_cancel_batch_returns_false_for_nonexistent() {
    let tikv = match get_tikv_client().await {
        Some(client) => client,
        None => return,
    };
    let controller = BatchController::with_client(tikv.clone());

    let cancelled = controller
        .cancel_batch("jobs:nonexistent-batch")
        .await
        .unwrap();
    assert!(!cancelled);
}

#[tokio::test]
async fn test_cancel_batch_returns_false_for_terminal_phase() {
    let tikv = match get_tikv_client().await {
        Some(client) => client,
        None => return,
    };
    let controller = BatchController::with_client(tikv.clone());

    let batch_id = unique_batch_id("test-cancel-terminal");
    let batch_name = batch_id.strip_prefix("jobs:").unwrap();

    // Create batch
    let spec = BatchSpec::new(
        batch_name,
        vec!["s3://test/file.bag".to_string()],
        "s3://output/".to_string(),
    );
    controller.submit_batch(&spec).await.unwrap();

    // Transition to Complete
    let mut status = BatchStatus::new();
    status.transition_to(BatchPhase::Complete);
    let status_key = BatchKeys::status(&batch_id);
    tikv.put(status_key.clone(), bincode::serialize(&status).unwrap())
        .await
        .unwrap();

    // Try to cancel - should fail since Complete is terminal
    let cancelled = controller.cancel_batch(&batch_id).await.unwrap();
    assert!(!cancelled);

    // Cleanup
    let _ = tikv.delete(BatchKeys::spec(&batch_id)).await;
    let _ = tikv.delete(BatchKeys::status(&batch_id)).await;
}

#[tokio::test]
async fn test_complete_work_unit_updates_status() {
    let tikv = match get_tikv_client().await {
        Some(client) => client,
        None => return,
    };
    let controller = BatchController::with_client(tikv.clone());

    let batch_id = unique_batch_id("test-complete-wu");
    let unit_id = "unit-1";

    // Create work unit in Processing state
    let mut work_unit = WorkUnit::with_id(
        unit_id.to_string(),
        batch_id.to_string(),
        vec![WorkFile::new("s3://test/file.bag".to_string(), 1024)],
        "s3://output/".to_string(),
        "config-hash".to_string(),
    );
    work_unit.claim("worker-1".to_string()).unwrap();
    assert_eq!(work_unit.status, WorkUnitStatus::Processing);

    let unit_key = WorkUnitKeys::unit(&batch_id, unit_id);
    tikv.put(unit_key.clone(), bincode::serialize(&work_unit).unwrap())
        .await
        .unwrap();

    // Complete the work unit
    let completed = controller
        .complete_work_unit(&batch_id, unit_id)
        .await
        .unwrap();
    assert!(completed);

    // Verify work unit status
    let updated_data = tikv.get(unit_key.clone()).await.unwrap().unwrap();
    let updated_unit: WorkUnit = bincode::deserialize(&updated_data).unwrap();
    assert_eq!(updated_unit.status, WorkUnitStatus::Complete);
    assert!(updated_unit.owner.is_none());

    // Cleanup
    let _ = tikv.delete(unit_key).await;
}

#[tokio::test]
async fn test_complete_work_unit_returns_false_for_nonexistent() {
    let tikv = match get_tikv_client().await {
        Some(client) => client,
        None => return,
    };
    let controller = BatchController::with_client(tikv.clone());

    let completed = controller
        .complete_work_unit("jobs:nonexistent", "unit-1")
        .await
        .unwrap();
    assert!(!completed);
}

#[tokio::test]
async fn test_fail_work_unit_with_retryable_error() {
    let tikv = match get_tikv_client().await {
        Some(client) => client,
        None => return,
    };
    let controller = BatchController::with_client(tikv.clone());

    let batch_id = unique_batch_id("test-fail-wu");
    let unit_id = "unit-1";

    // Create work unit (first attempt)
    let mut work_unit = WorkUnit::with_id(
        unit_id.to_string(),
        batch_id.to_string(),
        vec![WorkFile::new("s3://test/file.bag".to_string(), 1024)],
        "s3://output/".to_string(),
        "config-hash".to_string(),
    );
    work_unit.claim("worker-1".to_string()).unwrap();
    assert_eq!(work_unit.attempts, 1);

    let unit_key = WorkUnitKeys::unit(&batch_id, unit_id);
    tikv.put(unit_key.clone(), bincode::serialize(&work_unit).unwrap())
        .await
        .unwrap();

    // Fail the work unit
    let failed = controller
        .fail_work_unit(&batch_id, unit_id, "Temporary error".to_string())
        .await
        .unwrap();
    assert!(failed);

    // Verify work unit status is Failed (not Dead since attempts < max_attempts)
    let updated_data = tikv.get(unit_key.clone()).await.unwrap().unwrap();
    let updated_unit: WorkUnit = bincode::deserialize(&updated_data).unwrap();
    assert_eq!(updated_unit.status, WorkUnitStatus::Failed);
    assert_eq!(updated_unit.attempts, 1);
    assert!(updated_unit.error.is_some());

    // Verify pending queue entry was created for retry
    let pending_key = WorkUnitKeys::pending(&batch_id, unit_id);
    let pending_data = tikv.get(pending_key.clone()).await.unwrap();
    assert!(pending_data.is_some());

    // Cleanup
    let _ = tikv.delete(unit_key).await;
    let _ = tikv.delete(pending_key).await;
}

#[tokio::test]
async fn test_fail_work_unit_exceeding_max_attempts_goes_dead() {
    let tikv = match get_tikv_client().await {
        Some(client) => client,
        None => return,
    };
    let controller = BatchController::with_client(tikv.clone());

    let batch_id = unique_batch_id("test-fail-dead");
    let unit_id = "unit-1";

    // Create work unit at max attempts (already in Processing from claim)
    let mut work_unit = WorkUnit::with_id(
        unit_id.to_string(),
        batch_id.to_string(),
        vec![WorkFile::new("s3://test/file.bag".to_string(), 1024)],
        "s3://output/".to_string(),
        "config-hash".to_string(),
    );
    work_unit.attempts = 3; // Default max_attempts is 3
    // Manually set to Processing (as if already claimed)
    work_unit.status = WorkUnitStatus::Processing;
    work_unit.owner = Some("worker-1".to_string());
    assert_eq!(work_unit.attempts, 3);

    let unit_key = WorkUnitKeys::unit(&batch_id, unit_id);
    tikv.put(unit_key.clone(), bincode::serialize(&work_unit).unwrap())
        .await
        .unwrap();

    // Fail the work unit
    let failed = controller
        .fail_work_unit(&batch_id, unit_id, "Permanent error".to_string())
        .await
        .unwrap();
    assert!(failed);

    // Verify work unit status is Dead (attempts >= max_attempts)
    let updated_data = tikv.get(unit_key.clone()).await.unwrap().unwrap();
    let updated_unit: WorkUnit = bincode::deserialize(&updated_data).unwrap();
    assert_eq!(updated_unit.status, WorkUnitStatus::Dead);

    // Cleanup
    let _ = tikv.delete(unit_key).await;
}

// Note: Reconciliation tests require complex setup with proper phase index management.
// These are better covered by the integration tests in test_batch_workflow.rs and
// the e2e tests in tests/bag_processing_e2e_test.rs.

#[tokio::test]
async fn test_list_batches_returns_all_batches() {
    let tikv = match get_tikv_client().await {
        Some(client) => client,
        None => return,
    };
    let controller = BatchController::with_client(tikv.clone());

    let batch_id1 = "jobs:test-list-1";
    let batch_id2 = "jobs:test-list-2";

    // Create two batches
    for batch_id in [batch_id1, batch_id2] {
        let name = batch_id.strip_prefix("jobs:").unwrap();
        let spec = BatchSpec::new(
            name,
            vec!["s3://test/file.bag".to_string()],
            "s3://output/".to_string(),
        );
        controller.submit_batch(&spec).await.unwrap();
    }

    // List batches
    let batches = controller.list_batches().await.unwrap();
    let batch_ids: Vec<_> = batches.iter().map(|b| b.id.as_str()).collect();
    assert!(batch_ids.contains(&batch_id1));
    assert!(batch_ids.contains(&batch_id2));

    // Cleanup
    for batch_id in [batch_id1, batch_id2] {
        let _ = tikv.delete(BatchKeys::spec(batch_id)).await;
        let _ = tikv.delete(BatchKeys::status(batch_id)).await;
        let _ = tikv
            .delete(BatchIndexKeys::phase(BatchPhase::Pending, batch_id))
            .await;
    }
}
