// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Tests for batch workflow: Pending -> Discovering -> Running -> Merging -> Complete.
//!
//! Expected flow:
//! 1. Pending: batch submitted
//! 2. Discovering: scanner discovers files, creates work units
//! 3. Running: workers claim and process work units
//! 4. Merging: finalizer triggers merge (Running -> Merging via CAS)
//! 5. Complete: merge coordinator marks Complete after merge finishes
//!
//! Critical: The controller must NOT transition Running -> Complete. That would
//! bypass the merge step. Only the merge coordinator does Merging -> Complete.

use roboflow_distributed::batch::{
    BatchController, BatchIndexKeys, BatchKeys, BatchPhase, BatchSpec, BatchStatus, WorkFile,
    WorkUnit, WorkUnitKeys, WorkUnitStatus, batch_id_from_spec,
};
use roboflow_distributed::tikv::client::TikvClient;
use std::sync::Arc;

#[tokio::test]
#[ignore = "Requires TiKV setup for distributed testing"]
async fn test_controller_does_not_skip_merge_phase() {
    // When all work units are complete, the controller must leave the batch in
    // Running so the finalizer can trigger the merge. It must NOT transition
    // to Complete (which would bypass the merge).
    let tikv = Arc::new(TikvClient::from_env().await.unwrap());
    let controller = BatchController::with_client(tikv.clone());

    let batch_id = "jobs:workflow-test-batch";
    let unit_id = "unit-1";

    // Create spec
    let spec = BatchSpec::new(
        "workflow-test-batch",
        vec!["s3://test/file.bag".to_string()],
        "s3://output/".to_string(),
    );
    assert_eq!(batch_id_from_spec(&spec), batch_id);

    // Create batch status: Running, 1 work unit total
    let mut status = BatchStatus::new();
    status.transition_to(BatchPhase::Running);
    status.set_work_units_total(1);
    status.set_files_total(1);
    status.started_at = Some(chrono::Utc::now());

    // Create work unit with status Complete (simulating worker finished)
    let mut work_unit = WorkUnit::with_id(
        unit_id.to_string(),
        batch_id.to_string(),
        vec![WorkFile::new("s3://test/file.bag".to_string(), 1024)],
        "s3://output/".to_string(),
        "config-hash".to_string(),
    );
    work_unit.complete();
    assert_eq!(work_unit.status, WorkUnitStatus::Complete);

    // Write spec, status, phase index, work unit to TiKV
    let spec_key = BatchKeys::spec(batch_id);
    let spec_data = serde_yaml_ng::to_string(&spec).unwrap().into_bytes();
    let status_key = BatchKeys::status(batch_id);
    let status_data = bincode::serialize(&status).unwrap();
    let phase_key = BatchIndexKeys::phase(BatchPhase::Running, batch_id);
    let unit_key = WorkUnitKeys::unit(batch_id, unit_id);
    let unit_data = bincode::serialize(&work_unit).unwrap();

    tikv.batch_put(vec![
        (spec_key, spec_data),
        (status_key, status_data),
        (phase_key, vec![]),
        (unit_key.clone(), unit_data),
    ])
    .await
    .unwrap();

    // Run controller reconcile - it should update counts but NOT transition to Complete
    controller.reconcile_all().await.unwrap();

    // Read back status
    let updated = tikv
        .get(BatchKeys::status(batch_id))
        .await
        .unwrap()
        .unwrap();
    let status: BatchStatus = bincode::deserialize(&updated).unwrap();

    assert_eq!(
        status.phase,
        BatchPhase::Running,
        "Controller must NOT transition Running -> Complete; batch must stay Running for finalizer to trigger merge"
    );
    assert_eq!(status.work_units_completed, 1);
    assert_eq!(status.work_units_total, 1);
    assert!(status.is_complete());

    // Cleanup
    let _ = tikv.delete(BatchKeys::spec(batch_id)).await;
    let _ = tikv.delete(BatchKeys::status(batch_id)).await;
    let _ = tikv
        .delete(BatchIndexKeys::phase(BatchPhase::Running, batch_id))
        .await;
    let _ = tikv.delete(unit_key).await;
}
