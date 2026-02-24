// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

use std::path::Path;

use roboflow_distributed::batch::{BatchController, BatchPhase, BatchSpec};
use roboflow_distributed::tikv::client::TikvClient;

fn fixture_source_url() -> String {
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/roboflow_extracted.bag");
    assert!(fixture.exists(), "fixture missing: {}", fixture.display());
    let abs = fixture
        .canonicalize()
        .unwrap_or(fixture)
        .to_string_lossy()
        .to_string();
    format!("file://{}", abs)
}

#[tokio::test]
async fn batch_workflow_new_arch_e2e() {
    let tikv = TikvClient::from_env().await.expect("tikv available");
    let controller = BatchController::with_client(std::sync::Arc::new(tikv));

    let name = format!("new-arch-e2e-{}", uuid::Uuid::new_v4());
    let spec = BatchSpec::new(
        name,
        vec![fixture_source_url()],
        "file:///tmp/roboflow-new-arch-output".to_string(),
    );

    let batch_id = controller.submit_batch(&spec).await.expect("submit batch");

    let initial = controller
        .get_batch_status(&batch_id)
        .await
        .expect("status fetch")
        .expect("status exists");
    assert!(matches!(
        initial.phase,
        BatchPhase::Pending | BatchPhase::Running
    ));

    controller.reconcile_all().await.expect("reconcile all");

    let summaries = controller.list_batches().await.expect("list batches");
    assert!(summaries.iter().any(|s| s.id == batch_id));

    let after = controller
        .get_batch_status(&batch_id)
        .await
        .expect("status fetch after")
        .expect("status exists after");

    let stored_spec = controller
        .get_batch_spec(&batch_id)
        .await
        .expect("spec fetch")
        .expect("spec exists");

    assert_eq!(stored_spec.metadata.name, spec.metadata.name);
    assert!(after.work_units_total >= after.work_units_completed);
}
