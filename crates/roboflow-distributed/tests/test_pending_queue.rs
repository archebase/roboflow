// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Test pending queue workflow

use roboflow_distributed::batch::{WorkFile, WorkUnit, WorkUnitKeys};
use roboflow_distributed::tikv::client::TikvClient;

#[tokio::test]
async fn test_pending_queue_workflow() {
    // Create TiKV client
    let tikv = match TikvClient::from_env().await {
        Ok(client) => client,
        Err(e) => {
            println!("Skipping test: TiKV not available: {}", e);
            return;
        }
    };

    let batch_id = "test-batch-123";
    let unit_id = "test-unit-456";

    // Create a work unit
    let work_unit = WorkUnit::with_id(
        unit_id.to_string(),
        batch_id.to_string(),
        vec![WorkFile::new("s3://test/file.bag".to_string(), 1024)],
        "s3://output/".to_string(),
        "config-hash".to_string(),
    );

    // Store work unit
    let unit_key = WorkUnitKeys::unit(batch_id, unit_id);
    let unit_data = bincode::serialize(&work_unit).unwrap();
    tikv.put(unit_key.clone(), unit_data).await.unwrap();

    println!(
        "Work unit key: {}",
        String::from_utf8_lossy(&WorkUnitKeys::unit(batch_id, unit_id))
    );

    // Add to pending queue
    let pending_key = WorkUnitKeys::pending(batch_id, unit_id);
    let pending_data = batch_id.as_bytes().to_vec();
    tikv.put(pending_key.clone(), pending_data).await.unwrap();

    println!("Pending key: {}", String::from_utf8_lossy(&pending_key));
    println!(
        "Pending prefix: {}",
        String::from_utf8_lossy(&WorkUnitKeys::pending_prefix())
    );

    // Scan for pending entries
    let pending_prefix = WorkUnitKeys::pending_prefix();
    let results = tikv.scan(pending_prefix, 10).await.unwrap();

    println!("Found {} pending entries", results.len());
    for (key, value) in &results {
        println!("  Key: {}", String::from_utf8_lossy(key));
        println!("  Value: {}", String::from_utf8_lossy(value));
    }

    // Clean up
    let _ = tikv.delete(pending_key).await;
    let _ = tikv.delete(unit_key).await;

    assert!(!results.is_empty(), "Should have found pending entry");
}
