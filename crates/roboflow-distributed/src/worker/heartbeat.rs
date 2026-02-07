// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Heartbeat functionality for worker liveness tracking.

use super::WorkerMetrics;
use crate::tikv::{
    TikvError,
    client::TikvClient,
    schema::{HeartbeatRecord, WorkerStatus},
};

/// Send a heartbeat for a worker.
///
/// This inner function allows sending heartbeats from background tasks
/// without requiring a mutable Worker reference.
pub async fn send_heartbeat_inner(
    tikv: &TikvClient,
    pod_id: &str,
    metrics: &WorkerMetrics,
) -> Result<(), TikvError> {
    let active = metrics
        .active_jobs
        .load(std::sync::atomic::Ordering::Relaxed) as u32;
    let total_processed = metrics
        .jobs_completed
        .load(std::sync::atomic::Ordering::Relaxed);

    let mut heartbeat = tikv
        .get_heartbeat(pod_id)
        .await?
        .unwrap_or_else(|| HeartbeatRecord::new(pod_id.to_string()));

    heartbeat.beat();
    heartbeat.active_jobs = active;
    heartbeat.total_processed = total_processed;
    heartbeat.status = if active > 0 {
        WorkerStatus::Busy
    } else {
        WorkerStatus::Idle
    };

    tikv.update_heartbeat(pod_id, &heartbeat).await
}
