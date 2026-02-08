// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! HyperPipeline orchestrator - format conversion using RoboRewriter.
//!
//! Uses robocodec's unified RoboRewriter API for same-format conversion
//! (bag→bag, mcap→mcap). Cross-format conversion (bag→mcap) is supported
//! when input and output extensions match the rewriter's capability.

use std::time::{Duration, Instant};

use tracing::info;

use crate::hyper::config::HyperPipelineConfig;
use robocodec::RoboRewriter;
use roboflow_core::{Result, RoboflowError};

/// Hyper-Pipeline for format conversion using RoboRewriter.
///
/// Uses robocodec's unified RoboRewriter for message-level conversion.
/// Supports same-format rewriting: bag→bag, mcap→mcap.
///
/// # Supported Formats
///
/// - Input: ROS BAG files, MCAP files
/// - Output: Same format as input (bag→bag, mcap→mcap)
///
/// # Example
///
/// ```no_run
/// use roboflow::pipeline::hyper::{HyperPipeline, HyperPipelineConfig};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let config = HyperPipelineConfig::new("input.bag", "output.bag");
/// let pipeline = HyperPipeline::new(config)?;
/// let report = pipeline.run()?;
/// println!("Throughput: {:.2} MB/s", report.throughput_mb_s);
/// # Ok(())
/// # }
/// ```
pub struct HyperPipeline {
    config: HyperPipelineConfig,
}

impl HyperPipeline {
    /// Create a new hyper-pipeline.
    pub fn new(config: HyperPipelineConfig) -> Result<Self> {
        // Validate input file exists
        if !config.input_path.exists() {
            return Err(RoboflowError::parse(
                "HyperPipeline",
                format!("Input file not found: {}", config.input_path.display()),
            ));
        }

        Ok(Self { config })
    }

    /// Create a pipeline from builder.
    pub fn builder() -> crate::hyper::config::HyperPipelineBuilder {
        crate::hyper::config::HyperPipelineBuilder::new()
    }

    /// Run the pipeline to completion.
    pub fn run(self) -> Result<HyperPipelineReport> {
        let start = Instant::now();

        info!(
            input = %self.config.input_path.display(),
            output = %self.config.output_path.display(),
            "Starting HyperPipeline (RoboRewriter)"
        );

        // Ensure input and output have same format (RoboRewriter requirement)
        let input_ext = self
            .config
            .input_path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("");
        let output_ext = self
            .config
            .output_path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("");

        if input_ext != output_ext {
            return Err(RoboflowError::parse(
                "HyperPipeline",
                format!(
                    "Input and output formats must match. Got input .{} and output .{}",
                    input_ext, output_ext
                ),
            ));
        }

        // Get input file size
        let input_size = std::fs::metadata(&self.config.input_path)
            .map(|m| m.len())
            .unwrap_or(0);

        // Use RoboRewriter for format conversion
        let mut rewriter = RoboRewriter::open(&self.config.input_path).map_err(|e| {
            RoboflowError::parse("HyperPipeline", format!("Failed to open input: {}", e))
        })?;

        let stats = rewriter.rewrite(&self.config.output_path).map_err(|e| {
            RoboflowError::encode("HyperPipeline", format!("Rewrite failed: {}", e))
        })?;

        let duration = start.elapsed();

        // Get output file size
        let output_size = std::fs::metadata(&self.config.output_path)
            .map(|m| m.len())
            .unwrap_or(0);

        let compression_ratio = if input_size > 0 {
            output_size as f64 / input_size as f64
        } else {
            1.0
        };

        let throughput_mb_s = if duration.as_secs_f64() > 0.0 {
            (input_size as f64 / (1024.0 * 1024.0)) / duration.as_secs_f64()
        } else {
            0.0
        };

        info!(
            duration_sec = duration.as_secs_f64(),
            throughput_mb_s = throughput_mb_s,
            messages = stats.message_count,
            "HyperPipeline complete"
        );

        Ok(HyperPipelineReport {
            input_file: self.config.input_path.display().to_string(),
            output_file: self.config.output_path.display().to_string(),
            input_size_bytes: input_size,
            output_size_bytes: output_size,
            duration,
            throughput_mb_s,
            compression_ratio,
            message_count: stats.message_count,
            chunks_written: 0,
            crc_enabled: false,
        })
    }
}

/// Report from a hyper-pipeline run.
#[derive(Debug, Clone)]
pub struct HyperPipelineReport {
    /// Input file path
    pub input_file: String,
    /// Output file path
    pub output_file: String,
    /// Input file size in bytes
    pub input_size_bytes: u64,
    /// Output file size in bytes
    pub output_size_bytes: u64,
    /// Total duration
    pub duration: Duration,
    /// Throughput in MB/s
    pub throughput_mb_s: f64,
    /// Compression ratio (output / input)
    pub compression_ratio: f64,
    /// Number of messages processed
    pub message_count: u64,
    /// Number of chunks written
    pub chunks_written: u64,
    /// Whether CRC was enabled
    pub crc_enabled: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hyper_pipeline_builder() {
        let result = HyperPipeline::builder()
            .input_path("/nonexistent/input.bag")
            .output_path("/tmp/output.mcap")
            .compression_level(3)
            .enable_crc(true)
            .build();

        // Should fail because input doesn't exist
        // But builder should work
        assert!(result.is_ok());
    }
}
