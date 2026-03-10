// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::RwLock;
use tokio::time::sleep;

use super::config::WorkerConfig;
use super::metrics::{ProcessingResult, WorkerMetrics};
use super::processor::{MissingWorkProcessor, SharedWorkProcessor};
use super::registry::JobRegistry;
use crate::batch::{BatchController, WorkUnit};
use crate::shutdown::ShutdownHandler;
use crate::slot_pool::SlotPool;
use crate::stats::StatsCollector;
use crate::tikv::{
    TikvError,
    client::TikvClient,
    schema::{HeartbeatRecord, WorkerStatus},
};

pub struct Coordinator {
    pod_id: String,
    tikv: Arc<TikvClient>,
    config: WorkerConfig,
    metrics: Arc<WorkerMetrics>,
    shutdown_handler: ShutdownHandler,
    batch_controller: BatchController,
    job_registry: Arc<RwLock<JobRegistry>>,
    stats_collector: Option<Arc<dyn StatsCollector>>,
    slot_pool: Arc<SlotPool>,
    processor: SharedWorkProcessor,
}

impl Coordinator {
    pub fn new(
        pod_id: impl Into<String>,
        tikv: Arc<TikvClient>,
        config: WorkerConfig,
        job_registry: Arc<RwLock<JobRegistry>>,
    ) -> Result<Self, TikvError> {
        let batch_controller = BatchController::with_client(tikv.clone());
        let slot_pool = Arc::new(SlotPool::new(config.max_concurrent_jobs));

        Ok(Self {
            pod_id: pod_id.into(),
            tikv,
            config,
            metrics: Arc::new(WorkerMetrics::new()),
            shutdown_handler: ShutdownHandler::new(),
            batch_controller,
            job_registry,
            stats_collector: None,
            slot_pool,
            processor: Arc::new(MissingWorkProcessor),
        })
    }

    pub fn with_processor(mut self, processor: SharedWorkProcessor) -> Self {
        self.processor = processor;
        self
    }

    pub fn with_stats_collector(mut self, stats_collector: Arc<dyn StatsCollector>) -> Self {
        self.stats_collector = Some(stats_collector);
        self
    }

    pub fn pod_id(&self) -> &str {
        &self.pod_id
    }

    pub fn metrics(&self) -> &WorkerMetrics {
        &self.metrics
    }

    pub fn config(&self) -> &WorkerConfig {
        &self.config
    }

    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_handler.is_requested()
    }

    pub fn shutdown(&self) -> Result<(), TikvError> {
        self.shutdown_handler.shutdown();
        Ok(())
    }

    pub async fn claim_work(&self) -> Result<Option<WorkUnit>, TikvError> {
        tracing::debug!(pod_id = %self.pod_id, "=== WORKER CLAIM START ===");
        match self.batch_controller.claim_work_unit(&self.pod_id).await {
            Ok(Some(unit)) => {
                tracing::info!(
                    pod_id = %self.pod_id,
                    unit_id = %unit.id,
                    batch_id = %unit.batch_id,
                    "=== WORKER CLAIM SUCCESS ==="
                );
                self.metrics.inc_jobs_claimed();
                self.metrics.inc_active_jobs();
                Ok(Some(unit))
            }
            Ok(None) => {
                tracing::debug!(pod_id = %self.pod_id, "=== WORKER CLAIM: NO WORK AVAILABLE ===");
                Ok(None)
            }
            Err(e) => {
                tracing::error!(
                    pod_id = %self.pod_id,
                    error = %e,
                    "=== WORKER CLAIM FAILED ==="
                );
                Err(e)
            }
        }
    }

    pub async fn complete_work(&self, batch_id: &str, unit_id: &str) -> Result<(), TikvError> {
        match self
            .batch_controller
            .complete_work_unit(batch_id, unit_id)
            .await
        {
            Ok(true) => {
                self.metrics.inc_jobs_completed();
                self.metrics.dec_active_jobs();
                Ok(())
            }
            Ok(false) => {
                self.metrics.dec_active_jobs();
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub async fn fail_work(
        &self,
        batch_id: &str,
        unit_id: &str,
        error: String,
    ) -> Result<(), TikvError> {
        match self
            .batch_controller
            .fail_work_unit(batch_id, unit_id, error)
            .await
        {
            Ok(true) => {
                self.metrics.inc_jobs_failed();
                self.metrics.dec_active_jobs();
                Ok(())
            }
            Ok(false) => {
                self.metrics.dec_active_jobs();
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub async fn send_heartbeat(&self) -> Result<(), TikvError> {
        let active = self.metrics.active_jobs.load(Ordering::Relaxed) as u32;
        let total_processed = self.metrics.jobs_completed.load(Ordering::Relaxed);

        let mut heartbeat = self
            .tikv
            .get_heartbeat(&self.pod_id)
            .await?
            .unwrap_or_else(|| HeartbeatRecord::new(self.pod_id.clone()));

        heartbeat.beat();
        heartbeat.active_jobs = active;
        heartbeat.total_processed = total_processed;
        heartbeat.status = if active > 0 {
            WorkerStatus::Busy
        } else {
            WorkerStatus::Idle
        };

        self.tikv.update_heartbeat(&self.pod_id, &heartbeat).await?;
        Ok(())
    }

    pub async fn run(&mut self) -> Result<(), TikvError> {
        tracing::info!(
            pod_id = %self.pod_id,
            max_concurrent = self.config.max_concurrent_jobs,
            poll_interval_secs = self.config.poll_interval.as_secs(),
            "=== WORKER RUN START ==="
        );

        let mut shutdown_rx = self.shutdown_handler.start_signal_handler();
        let shutdown_tx = self.shutdown_handler.sender();

        let registry_for_shutdown = self.job_registry.clone();
        let mut cancel_rx = self.shutdown_handler.subscribe();
        tokio::spawn(async move {
            let _ = cancel_rx.recv().await;
            let registry = registry_for_shutdown.read().await;
            registry.cancel_all();
            drop(registry);
        });

        let heartbeat_handle = self.spawn_heartbeat_task(shutdown_tx.subscribe());
        let loop_result = self.run_main_loop(&mut shutdown_rx).await;
        let _ = heartbeat_handle.await;

        tracing::info!(
            pod_id = %self.pod_id,
            result = ?loop_result,
            "=== WORKER RUN END ==="
        );

        loop_result
    }

    fn spawn_heartbeat_task(
        &self,
        mut heartbeat_rx: tokio::sync::broadcast::Receiver<()>,
    ) -> tokio::task::JoinHandle<()> {
        let tikv = self.tikv.clone();
        let pod_id = self.pod_id.clone();
        let metrics = self.metrics.clone();
        let heartbeat_interval = self.config.heartbeat_interval;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(heartbeat_interval);
            interval.tick().await;

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if send_heartbeat_inner(&tikv, &pod_id, &metrics).await.is_err() {
                            metrics.inc_heartbeat_errors();
                        }
                    }
                    _ = heartbeat_rx.recv() => break,
                }
            }
        })
    }

    async fn run_main_loop(
        &mut self,
        shutdown_rx: &mut tokio::sync::broadcast::Receiver<()>,
    ) -> Result<(), TikvError> {
        loop {
            let active_count = self.metrics.active_jobs.load(Ordering::Relaxed) as usize;
            let max_jobs = self.config.max_concurrent_jobs;

            tracing::debug!(
                pod_id = %self.pod_id,
                active_jobs = active_count,
                max_concurrent = max_jobs,
                "=== WORKER MAIN LOOP ITERATION ==="
            );

            if active_count < max_jobs {
                tracing::debug!(pod_id = %self.pod_id, "Worker has capacity, attempting to claim work");
                match self.claim_work().await? {
                    Some(unit) => {
                        tracing::info!(
                            pod_id = %self.pod_id,
                            batch_id = %unit.batch_id,
                            unit_id = %unit.id,
                            "=== WORKER PROCESSING START ==="
                        );
                        if self.process_work_unit(&unit).await? {
                            return Ok(());
                        }
                    }
                    None => {
                        tracing::debug!(
                            pod_id = %self.pod_id,
                            poll_interval_secs = self.config.poll_interval.as_secs(),
                            "No work available, waiting before retry"
                        );
                        if self
                            .wait_with_shutdown(shutdown_rx, self.config.poll_interval)
                            .await?
                        {
                            return Ok(());
                        }
                    }
                }
            } else {
                tracing::debug!(
                    pod_id = %self.pod_id,
                    active_jobs = active_count,
                    max_concurrent = max_jobs,
                    "Worker at capacity, waiting for slot"
                );
                if self
                    .wait_with_shutdown(shutdown_rx, Duration::from_millis(100))
                    .await?
                {
                    return Ok(());
                }
            }
        }
    }

    async fn process_work_unit(&self, unit: &WorkUnit) -> Result<bool, TikvError> {
        let batch_id = unit.batch_id.clone();
        let unit_id = unit.id.clone();

        tracing::debug!(
            batch_id = %batch_id,
            unit_id = %unit_id,
            "process_work_unit: starting"
        );

        if self.shutdown_handler.is_requested() {
            tracing::warn!(batch_id = %batch_id, unit_id = %unit_id, "Shutdown requested");
            return Ok(true);
        }

        tracing::debug!(batch_id = %batch_id, unit_id = %unit_id, "Acquiring slot");
        let _slot_guard = match self.slot_pool.acquire().await {
            Ok(guard) => {
                tracing::debug!(batch_id = %batch_id, unit_id = %unit_id, "Slot acquired");
                guard
            }
            Err(e) => {
                tracing::warn!(batch_id = %batch_id, unit_id = %unit_id, error = ?e, "Failed to acquire slot");
                self.metrics.dec_active_jobs();
                return Ok(false);
            }
        };

        tracing::info!(
            batch_id = %batch_id,
            unit_id = %unit_id,
            input_files = unit.files.len(),
            "Calling processor.process()"
        );

        let result = self.processor.process(unit).await;

        tracing::debug!(
            batch_id = %batch_id,
            unit_id = %unit_id,
            success = result.is_ok(),
            "processor.process() returned"
        );

        match result {
            Ok(processing_result) => {
                self.handle_execution_result(&batch_id, &unit_id, processing_result)
                    .await
            }
            Err(e) => {
                tracing::error!(
                    batch_id = %batch_id,
                    unit_id = %unit_id,
                    error = %e,
                    "processor.process() failed"
                );
                if let Err(fail_err) = self
                    .fail_work(&batch_id, &unit_id, format!("Execution error: {}", e))
                    .await
                {
                    self.metrics.inc_processing_errors();
                    return Err(fail_err);
                }

                Ok(false)
            }
        }
    }

    async fn handle_execution_result(
        &self,
        batch_id: &str,
        unit_id: &str,
        result: ProcessingResult,
    ) -> Result<bool, TikvError> {
        match result {
            ProcessingResult::Success {
                episode_index,
                frame_count,
                episode_stats,
            } => {
                if let (Some(collector), Some(stats)) = (&self.stats_collector, episode_stats)
                    && let Err(e) = collector.record_episode_stats(batch_id, stats).await
                {
                    tracing::warn!(
                        batch_id = %batch_id,
                        unit_id = %unit_id,
                        episode_index = episode_index,
                        error = %e,
                        "Failed to record episode stats"
                    );
                }

                tracing::info!(
                    batch_id = %batch_id,
                    unit_id = %unit_id,
                    episode_index = episode_index,
                    frame_count = frame_count,
                    "Work unit completed"
                );

                if let Err(e) = self.complete_work(batch_id, unit_id).await {
                    self.metrics.inc_processing_errors();
                    return Err(e);
                }
                Ok(false)
            }
            ProcessingResult::Failed { error } => {
                if error.contains("shutdown") {
                    return Ok(true);
                }
                tracing::error!(
                    batch_id = %batch_id,
                    unit_id = %unit_id,
                    error = %error,
                    "Work unit processing reported failure"
                );
                if let Err(e) = self.fail_work(batch_id, unit_id, error).await {
                    self.metrics.inc_processing_errors();
                    return Err(e);
                }
                Ok(false)
            }
            ProcessingResult::Cancelled => Ok(false),
        }
    }

    async fn wait_with_shutdown(
        &self,
        shutdown_rx: &mut tokio::sync::broadcast::Receiver<()>,
        duration: Duration,
    ) -> Result<bool, TikvError> {
        tokio::select! {
            _ = sleep(duration) => Ok(false),
            _ = shutdown_rx.recv() => Ok(true),
        }
    }
}

pub async fn send_heartbeat_inner(
    tikv: &TikvClient,
    pod_id: &str,
    metrics: &WorkerMetrics,
) -> Result<(), TikvError> {
    let active = metrics.active_jobs.load(Ordering::Relaxed) as u32;
    let total_processed = metrics.jobs_completed.load(Ordering::Relaxed);

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

    tikv.update_heartbeat(pod_id, &heartbeat).await?;
    Ok(())
}
