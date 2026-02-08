// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Shared utilities for HyperPipeline stages.
//!
//! This module provides common utilities used across multiple stages:
//! - Worker thread error handling
//! - Channel metrics tracking
//! - Stage statistics collection

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use roboflow_core::{Result, RoboflowError};

// ============================================================================
// Worker Thread Error Handling
// ============================================================================

/// Join multiple worker threads and collect errors.
///
/// This utility handles the common pattern of spawning worker threads
/// and waiting for them to complete, collecting any errors.
///
/// # Arguments
///
/// * `handles` - Vector of thread join handles
/// * `stage_name` - Name of the stage for error messages
///
/// # Returns
///
/// * `Ok(results)` if all workers succeeded
/// * `Err` with aggregated error messages if any workers failed
///
/// # Example
///
/// ```no_run
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use roboflow::pipeline::hyper::utils::join_workers;
/// use std::thread;
///
/// let handles = vec![
///     thread::spawn(|| Ok(())),
///     thread::spawn(|| Ok(())),
/// ];
/// let results = join_workers(handles, "MyStage")?;
/// # Ok(())
/// # }
/// ```
pub fn join_workers<T>(
    handles: Vec<thread::JoinHandle<Result<T>>>,
    stage_name: &str,
) -> Result<Vec<T>> {
    let mut results = Vec::with_capacity(handles.len());
    let mut errors = Vec::new();

    for (i, handle) in handles.into_iter().enumerate() {
        match handle.join() {
            Ok(Ok(result)) => results.push(result),
            Ok(Err(e)) => errors.push(format!("Worker {}: {}", i, e)),
            Err(_) => errors.push(format!("{} worker {} panicked", stage_name, i)),
        }
    }

    if !errors.is_empty() {
        return Err(RoboflowError::encode(
            stage_name,
            format!("Worker errors: {}", errors.join(", ")),
        ));
    }

    Ok(results)
}

/// Join multiple worker threads that return `()` on success.
///
/// Simplified version for workers that don't return a value.
pub fn join_unit_workers(
    handles: Vec<thread::JoinHandle<Result<()>>>,
    stage_name: &str,
) -> Result<()> {
    join_workers(handles, stage_name)?;
    Ok(())
}

/// Join a single stage thread with a descriptive error message.
pub fn join_stage_thread<T>(handle: thread::JoinHandle<Result<T>>, stage_name: &str) -> Result<T> {
    handle.join().map_err(|_| {
        RoboflowError::encode("HyperPipeline", format!("{} thread panicked", stage_name))
    })?
}

// ============================================================================
// Channel Metrics
// ============================================================================

/// Metrics for monitoring inter-stage channel health.
///
/// Tracks queue depth, throughput, and timing to identify bottlenecks.
#[derive(Debug, Default)]
pub struct ChannelMetrics {
    /// Total items sent through the channel
    pub items_sent: AtomicU64,
    /// Total items received from the channel
    pub items_received: AtomicU64,
    /// Maximum queue depth observed
    pub max_queue_depth: AtomicUsize,
    /// Total time spent blocked on send (nanoseconds)
    pub send_blocked_ns: AtomicU64,
    /// Total time spent blocked on receive (nanoseconds)
    pub recv_blocked_ns: AtomicU64,
}

impl ChannelMetrics {
    /// Create new channel metrics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an item being sent.
    pub fn record_send(&self) {
        self.items_sent.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an item being received.
    pub fn record_recv(&self) {
        self.items_received.fetch_add(1, Ordering::Relaxed);
    }

    /// Update maximum queue depth.
    pub fn update_queue_depth(&self, depth: usize) {
        let mut current = self.max_queue_depth.load(Ordering::Relaxed);
        while depth > current {
            match self.max_queue_depth.compare_exchange_weak(
                current,
                depth,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => current = c,
            }
        }
    }

    /// Record time blocked on send operation.
    pub fn record_send_blocked(&self, nanos: u64) {
        self.send_blocked_ns.fetch_add(nanos, Ordering::Relaxed);
    }

    /// Record time blocked on receive operation.
    pub fn record_recv_blocked(&self, nanos: u64) {
        self.recv_blocked_ns.fetch_add(nanos, Ordering::Relaxed);
    }

    /// Get a snapshot of the current metrics.
    pub fn snapshot(&self) -> ChannelMetricsSnapshot {
        ChannelMetricsSnapshot {
            items_sent: self.items_sent.load(Ordering::Relaxed),
            items_received: self.items_received.load(Ordering::Relaxed),
            max_queue_depth: self.max_queue_depth.load(Ordering::Relaxed),
            send_blocked_ms: self.send_blocked_ns.load(Ordering::Relaxed) as f64 / 1_000_000.0,
            recv_blocked_ms: self.recv_blocked_ns.load(Ordering::Relaxed) as f64 / 1_000_000.0,
        }
    }
}

/// Snapshot of channel metrics at a point in time.
#[derive(Debug, Clone)]
pub struct ChannelMetricsSnapshot {
    pub items_sent: u64,
    pub items_received: u64,
    pub max_queue_depth: usize,
    pub send_blocked_ms: f64,
    pub recv_blocked_ms: f64,
}

impl ChannelMetricsSnapshot {
    /// Calculate the current queue depth (items in flight).
    pub fn queue_depth(&self) -> u64 {
        self.items_sent.saturating_sub(self.items_received)
    }

    /// Check if this channel appears to be a bottleneck.
    ///
    /// A channel is considered a bottleneck if:
    /// - Send time is high (producer blocking)
    /// - Queue depth is consistently at max
    pub fn is_bottleneck(&self, threshold_ms: f64) -> bool {
        self.send_blocked_ms > threshold_ms
    }
}

// ============================================================================
// Pipeline Metrics Aggregator
// ============================================================================

/// Aggregated metrics for all pipeline stages.
#[derive(Debug, Default)]
pub struct PipelineMetrics {
    /// Metrics for prefetcher → parser channel
    pub prefetch_to_parser: Arc<ChannelMetrics>,
    /// Metrics for parser → compression channel
    pub parser_to_compress: Arc<ChannelMetrics>,
    /// Metrics for compression → packetizer channel
    pub compress_to_packet: Arc<ChannelMetrics>,
    /// Metrics for packetizer → writer channel
    pub packet_to_writer: Arc<ChannelMetrics>,
}

impl PipelineMetrics {
    /// Create new pipeline metrics.
    pub fn new() -> Self {
        Self {
            prefetch_to_parser: Arc::new(ChannelMetrics::new()),
            parser_to_compress: Arc::new(ChannelMetrics::new()),
            compress_to_packet: Arc::new(ChannelMetrics::new()),
            packet_to_writer: Arc::new(ChannelMetrics::new()),
        }
    }

    /// Get a summary of all channel metrics.
    pub fn summary(&self) -> PipelineMetricsSummary {
        PipelineMetricsSummary {
            prefetch_to_parser: self.prefetch_to_parser.snapshot(),
            parser_to_compress: self.parser_to_compress.snapshot(),
            compress_to_packet: self.compress_to_packet.snapshot(),
            packet_to_writer: self.packet_to_writer.snapshot(),
        }
    }
}

/// Summary of pipeline metrics.
#[derive(Debug, Clone)]
pub struct PipelineMetricsSummary {
    pub prefetch_to_parser: ChannelMetricsSnapshot,
    pub parser_to_compress: ChannelMetricsSnapshot,
    pub compress_to_packet: ChannelMetricsSnapshot,
    pub packet_to_writer: ChannelMetricsSnapshot,
}

impl PipelineMetricsSummary {
    /// Identify the bottleneck stage, if any.
    ///
    /// Returns the name of the stage that appears to be the slowest
    /// based on channel blocking times.
    pub fn identify_bottleneck(&self, threshold_ms: f64) -> Option<&'static str> {
        let channels = [
            ("prefetcher", &self.prefetch_to_parser),
            ("parser", &self.parser_to_compress),
            ("compressor", &self.compress_to_packet),
            ("packetizer", &self.packet_to_writer),
        ];

        // Find channel with highest send blocked time (indicates slow consumer)
        let mut bottleneck: Option<(&'static str, f64)> = None;
        for (name, metrics) in &channels {
            if metrics.send_blocked_ms > threshold_ms {
                match &bottleneck {
                    Some((_, blocked)) if metrics.send_blocked_ms <= *blocked => {}
                    _ => bottleneck = Some((name, metrics.send_blocked_ms)),
                }
            }
        }

        bottleneck.map(|(name, _)| name)
    }

    /// Format a human-readable summary.
    pub fn format(&self) -> String {
        format!(
            "Pipeline Metrics:\n  \
            prefetch→parser: {} items, max depth {}, blocked {:.1}ms\n  \
            parser→compress: {} items, max depth {}, blocked {:.1}ms\n  \
            compress→packet: {} items, max depth {}, blocked {:.1}ms\n  \
            packet→writer:   {} items, max depth {}, blocked {:.1}ms",
            self.prefetch_to_parser.items_sent,
            self.prefetch_to_parser.max_queue_depth,
            self.prefetch_to_parser.send_blocked_ms,
            self.parser_to_compress.items_sent,
            self.parser_to_compress.max_queue_depth,
            self.parser_to_compress.send_blocked_ms,
            self.compress_to_packet.items_sent,
            self.compress_to_packet.max_queue_depth,
            self.compress_to_packet.send_blocked_ms,
            self.packet_to_writer.items_sent,
            self.packet_to_writer.max_queue_depth,
            self.packet_to_writer.send_blocked_ms,
        )
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join_workers_success() {
        let handles: Vec<thread::JoinHandle<Result<i32>>> = vec![
            thread::spawn(|| Ok(1)),
            thread::spawn(|| Ok(2)),
            thread::spawn(|| Ok(3)),
        ];

        let results = join_workers(handles, "TestStage").unwrap();
        assert_eq!(results, vec![1, 2, 3]);
    }

    #[test]
    fn test_join_workers_with_error() {
        let handles: Vec<thread::JoinHandle<Result<i32>>> = vec![
            thread::spawn(|| Ok(1)),
            thread::spawn(|| Err(RoboflowError::encode("Test", "worker failed"))),
        ];

        let result = join_workers(handles, "TestStage");
        assert!(result.is_err());
    }

    #[test]
    fn test_channel_metrics() {
        let metrics = ChannelMetrics::new();

        metrics.record_send();
        metrics.record_send();
        metrics.record_recv();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.items_sent, 2);
        assert_eq!(snapshot.items_received, 1);
        assert_eq!(snapshot.queue_depth(), 1);
    }

    #[test]
    fn test_channel_metrics_max_depth() {
        let metrics = ChannelMetrics::new();

        metrics.update_queue_depth(5);
        metrics.update_queue_depth(10);
        metrics.update_queue_depth(3);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.max_queue_depth, 10);
    }

    #[test]
    fn test_pipeline_metrics_summary() {
        let metrics = PipelineMetrics::new();

        metrics.prefetch_to_parser.record_send();
        metrics.prefetch_to_parser.record_send();
        metrics.parser_to_compress.record_send();

        let summary = metrics.summary();
        assert_eq!(summary.prefetch_to_parser.items_sent, 2);
        assert_eq!(summary.parser_to_compress.items_sent, 1);
    }

    #[test]
    fn test_bottleneck_identification() {
        let metrics = PipelineMetrics::new();

        // Simulate blocking on compress channel
        metrics.compress_to_packet.send_blocked_ns.store(
            100_000_000, // 100ms
            Ordering::Relaxed,
        );

        let summary = metrics.summary();
        let bottleneck = summary.identify_bottleneck(10.0);
        assert_eq!(bottleneck, Some("compressor"));
    }
}
