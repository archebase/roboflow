// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Progress callback for saving checkpoints during conversion.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio_util::sync::CancellationToken;

use crate::shutdown::ShutdownInterrupted;
use crate::tikv::checkpoint::CheckpointManager;
use crate::tikv::schema::CheckpointState;

// Import DatasetWriter trait for episode_index method
use roboflow_dataset::DatasetWriter;

/// Progress callback for saving checkpoints during conversion.
pub struct WorkerCheckpointCallback {
    /// Job ID for this conversion
    pub job_id: String,
    /// Pod ID of the worker
    pub pod_id: String,
    /// Total frames (estimated)
    pub total_frames: u64,
    /// Reference to checkpoint manager
    pub checkpoint_manager: CheckpointManager,
    /// Last checkpoint frame number
    pub last_checkpoint_frame: Arc<AtomicU64>,
    /// Last checkpoint time
    pub last_checkpoint_time: Arc<std::sync::Mutex<std::time::Instant>>,
    /// Shutdown flag for graceful interruption
    pub shutdown_flag: Arc<AtomicBool>,
    /// Cancellation token for job cancellation
    pub cancellation_token: Option<Arc<CancellationToken>>,
}

impl roboflow_dataset::streaming::converter::ProgressCallback for WorkerCheckpointCallback {
    fn on_frame_written(
        &self,
        frames_written: u64,
        messages_processed: u64,
        writer: &dyn std::any::Any,
    ) -> std::result::Result<(), String> {
        // Check for shutdown signal first
        if self.shutdown_flag.load(Ordering::SeqCst) {
            tracing::info!(
                job_id = %self.job_id,
                frames_written = frames_written,
                "Shutdown requested, interrupting conversion at checkpoint boundary"
            );
            return Err(ShutdownInterrupted.to_string());
        }

        // Check for job cancellation via token
        if let Some(token) = &self.cancellation_token
            && token.is_cancelled()
        {
            tracing::info!(
                job_id = %self.job_id,
                frames_written = frames_written,
                "Job cancellation detected, interrupting conversion at checkpoint boundary"
            );
            return Err("Job cancelled by user request".to_string());
        }

        let last_frame = self.last_checkpoint_frame.load(Ordering::Relaxed);
        let frames_since_last = frames_written.saturating_sub(last_frame);

        // Scope the lock tightly to avoid holding it during expensive operations
        let time_since_last = {
            let last_time = self
                .last_checkpoint_time
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            last_time.elapsed()
        };

        // Check if we should save a checkpoint
        if self
            .checkpoint_manager
            .should_checkpoint(frames_since_last, time_since_last)
        {
            // Extract episode index from writer if it's a LeRobotWriter
            use roboflow_dataset::lerobot::writer::LerobotWriter;
            let episode_idx = writer
                .downcast_ref::<LerobotWriter>()
                .and_then(|w| w.episode_index())
                .unwrap_or(0) as u64;

            // NOTE: Using messages_processed as byte_offset proxy.
            // Actual byte offset tracking requires robocodec modifications.
            // Resume works by re-reading from start and skipping messages.
            //
            // NOTE: Upload state tracking requires episode-level checkpointing.
            // Current frame-level checkpoints don't capture upload state because:
            // 1. Uploads happen after finish_episode(), not during frame processing
            // 2. The coordinator tracks completion, not in-progress multipart state
            // 3. Resume should check which episodes exist in cloud storage
            //
            // Episode-level upload state tracking is a future enhancement that would:
            // - Save episode completion to TiKV after each episode finishes
            // - Query cloud storage for completed episodes on resume
            // - Skip re-uploading episodes that already exist
            //
            // For now, the frame-level checkpoint is sufficient for resume
            // as episodes are written atomically and can be detected via
            // existence checks in the output storage.
            let checkpoint = CheckpointState {
                job_id: self.job_id.clone(),
                pod_id: self.pod_id.clone(),
                byte_offset: messages_processed,
                last_frame: frames_written,
                episode_idx,
                total_frames: self.total_frames,
                video_uploads: Vec::new(),
                parquet_upload: None,
                updated_at: chrono::Utc::now(),
                version: 1,
            };

            // Use save_async which respects checkpoint_async config:
            // - When async=true: spawns background task, non-blocking
            // - When async=false: falls back to synchronous save
            self.checkpoint_manager.save_async(checkpoint.clone());
            tracing::debug!(
                job_id = %self.job_id,
                last_frame = frames_written,
                progress = %checkpoint.progress_percent(),
                "Checkpoint save initiated"
            );
            self.last_checkpoint_frame
                .store(frames_written, Ordering::Relaxed);
            // Re-acquire lock only for the instant update
            // Use poison recovery to handle panics gracefully
            *self
                .last_checkpoint_time
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = std::time::Instant::now();
        }

        std::result::Result::Ok(())
    }
}
