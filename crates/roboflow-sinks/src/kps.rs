// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! KPS sink implementation.
//!
//! This module provides a Sink implementation for writing datasets in KPS format.

use crate::{DatasetFrame, Sink, SinkCheckpoint, SinkConfig, SinkError, SinkResult, SinkStats};
use std::collections::HashMap;

/// KPS dataset sink.
///
/// This sink writes robotics datasets in KPS (Knowledge-based Policy Sharing)
/// format, used for sharing robot manipulation policies.
pub struct KpsSink {
    /// Output directory path
    output_path: String,
    /// Whether the sink has been initialized
    initialized: bool,
    /// Frames written counter
    frames_written: usize,
    /// Episodes written counter
    episodes_written: usize,
    /// Start time for duration calculation
    start_time: Option<std::time::Instant>,
    /// Output bytes written
    output_bytes: u64,
}

impl KpsSink {
    /// Create a new KPS sink.
    pub fn new(path: impl Into<String>) -> SinkResult<Self> {
        Ok(Self {
            output_path: path.into(),
            initialized: false,
            frames_written: 0,
            episodes_written: 0,
            start_time: None,
            output_bytes: 0,
        })
    }

    /// Create a new KPS sink from a SinkConfig.
    pub fn from_config(config: &SinkConfig) -> SinkResult<Self> {
        match &config.sink_type {
            crate::SinkType::Kps { path } => Self::new(path),
            _ => Err(SinkError::InvalidConfig(
                "Invalid config for KpsSink".to_string(),
            )),
        }
    }
}

#[async_trait::async_trait]
impl Sink for KpsSink {
    async fn initialize(&mut self, _config: &SinkConfig) -> SinkResult<()> {
        // Create output directory
        let path = std::path::Path::new(&self.output_path);
        std::fs::create_dir_all(path).map_err(|e| SinkError::CreateFailed {
            path: path.to_path_buf(),
            error: Box::new(e),
        })?;

        self.initialized = true;
        self.start_time = Some(std::time::Instant::now());

        Ok(())
    }

    async fn write_frame(&mut self, frame: DatasetFrame) -> SinkResult<()> {
        if !self.initialized {
            return Err(SinkError::WriteFailed(
                "Sink not initialized. Call initialize() first.".to_string(),
            ));
        }

        // This is a simplified implementation.
        // A production implementation would:
        // 1. Convert DatasetFrame to KPS format
        // 2. Write Parquet files using roboflow_dataset::kps::ParquetKpsWriter
        // 3. Handle video encoding
        // 4. Write metadata

        // For now, just track the frame
        self.frames_written += 1;

        // Check for episode boundary (simple heuristic: frame_index reset)
        if frame.frame_index == 0 && self.frames_written > 1 {
            self.episodes_written += 1;
        }

        Ok(())
    }

    async fn flush(&mut self) -> SinkResult<()> {
        // Flush any buffered data
        Ok(())
    }

    async fn finalize(&mut self) -> SinkResult<SinkStats> {
        let duration = self
            .start_time
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0);

        Ok(SinkStats {
            frames_written: self.frames_written,
            episodes_written: self.episodes_written,
            duration_sec: duration,
            total_bytes: Some(self.output_bytes),
            metrics: HashMap::new(),
        })
    }

    async fn checkpoint(&self) -> SinkResult<SinkCheckpoint> {
        Ok(SinkCheckpoint {
            last_frame_index: self.frames_written,
            last_episode_index: self.episodes_written,
            checkpoint_time: chrono::Utc::now().timestamp(),
            data: HashMap::new(),
        })
    }

    fn supports_checkpointing(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kps_sink_creation() {
        let sink = KpsSink::new("/tmp/output");
        assert!(sink.is_ok());
        let sink = sink.unwrap();
        assert_eq!(sink.output_path, "/tmp/output");
    }

    #[test]
    fn test_kps_sink_from_config() {
        let config = SinkConfig::kps("/tmp/output");
        let sink = KpsSink::from_config(&config);
        assert!(sink.is_ok());
    }

    #[test]
    fn test_kps_sink_invalid_config() {
        let config = SinkConfig::lerobot("/tmp/output");
        let sink = KpsSink::from_config(&config);
        assert!(sink.is_err());
    }
}
