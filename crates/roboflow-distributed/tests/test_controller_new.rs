// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Integration test for failed_work_units population in batch status.

use roboflow_distributed::batch::{
    BatchController, BatchIndexKeys, BatchKeys, BatchPhase, BatchSpec, BatchStatus, WorkFile,
    WorkUnit, WorkUnitKeys, WorkUnitStatus,
};
use roboflow_distributed::tikv::client::TikvClient;
use std::sync::Arc;

fn unique_batch_id(prefix: &str) -> String {
    format!("jobs:{}-{}", prefix, uuid::Uuid::new_v4())
}

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
async fn test_reconcile_populates_failed_work_units_with_error_details() {
    //! Verify that reconcile populates failed_work_units with error details.
    //!
    //! This tests the fix for the issue where work unit failures showed no error
    //! details in batch status output.
    let tikv = match get_tikv_client().await {
        Some(client) => client,
        None => return,
    };
    let controller = BatchController::with_client(tikv.clone());

    let batch_id = unique_batch_id("test-failed-work-units");
    let batch_name = batch_id.strip_prefix("jobs:").unwrap();

    // Create batch
    let spec = BatchSpec::new(
        batch_name,
        vec!["s3://test/file.bag".to_string()],
        "s3://output/".to_string(),
    );
    controller.submit_batch(&spec).await.unwrap();

    // Create work units: one complete, one failed with error
    let complete_unit_id = "unit-complete";
    let failed_unit_id = "unit-failed";
    let error_message = "Test error: codec failure";

    // Create complete work unit
    let mut complete_unit = WorkUnit::with_id(
        complete_unit_id.to_string(),
        batch_id.to_string(),
        vec![WorkFile::new("s3://test/file1.bag".to_string(), 1024)],
        "s3://output/".to_string(),
        "config-hash".to_string(),
    );
    complete_unit.status = WorkUnitStatus::Complete;

    // Create failed work unit with error
    let mut failed_unit = WorkUnit::with_id(
        failed_unit_id.to_string(),
        batch_id.to_string(),
        vec![WorkFile::new("s3://test/file2.bag".to_string(), 2048)],
        "s3://output/".to_string(),
        "config-hash".to_string(),
    );
    failed_unit.status = WorkUnitStatus::Dead;
    failed_unit.error = Some(error_message.to_string());
    failed_unit.attempts = 3;

    // Store work units in TiKV
    let complete_key = WorkUnitKeys::unit(&batch_id, complete_unit_id);
    let failed_key = WorkUnitKeys::unit(&batch_id, failed_unit_id);
    tikv.put(
        complete_key.clone(),
        bincode::serialize(&complete_unit).unwrap(),
    )
    .await
    .unwrap();
    tikv.put(
        failed_key.clone(),
        bincode::serialize(&failed_unit).unwrap(),
    )
    .await
    .unwrap();

    // Transition batch to Running phase
    let mut status = BatchStatus::new();
    status.transition_to(BatchPhase::Running);
    status.set_work_units_total(2);
    let status_key = BatchKeys::status(&batch_id);
    tikv.put(status_key.clone(), bincode::serialize(&status).unwrap())
        .await
        .unwrap();

    // Trigger reconciliation using public API
    let result = controller.reconcile_batch_id(&batch_id).await;
    assert!(result.is_ok(), "Reconciliation should succeed");

    // Get updated status
    let updated_status = controller
        .get_batch_status(&batch_id)
        .await
        .unwrap()
        .unwrap();

    // Verify failed_work_units is populated
    assert_eq!(
        updated_status.failed_work_units.len(),
        1,
        "Should have one failed work unit"
    );

    let failed = &updated_status.failed_work_units[0];
    assert_eq!(failed.id, failed_unit_id);
    assert_eq!(failed.source_file, "s3://test/file2.bag");
    assert_eq!(failed.error, error_message);
    assert_eq!(failed.retries, 3);

    // Verify counts
    assert_eq!(updated_status.work_units_completed, 1);
    assert_eq!(updated_status.work_units_failed, 1);

    // Cleanup
    let _ = tikv.delete(BatchKeys::spec(&batch_id)).await;
    let _ = tikv.delete(BatchKeys::status(&batch_id)).await;
    let _ = tikv.delete(complete_key).await;
    let _ = tikv.delete(failed_key).await;
    let _ = tikv
        .delete(BatchIndexKeys::phase(BatchPhase::Pending, &batch_id))
        .await;
    let _ = tikv
        .delete(BatchIndexKeys::phase(BatchPhase::Running, &batch_id))
        .await;
}
