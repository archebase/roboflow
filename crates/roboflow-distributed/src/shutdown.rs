// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Graceful shutdown handling for distributed workers.
//!
//! This module provides signal handling for SIGTERM and SIGINT,
//! allowing workers to cleanly release jobs and clean up state
//! when being shut down (e.g., during K8s deployment updates).
//!
//! # Shutdown Sequence
//!
//! 1. Receive SIGTERM or SIGINT
//! 2. Set shutdown flag
//! 3. Stop accepting new jobs
//! 4. Complete current checkpoint (not entire job)
//! 5. Release job back to Pending
//! 6. Clear heartbeat
//! 7. Exit cleanly
//!
//! # Shutdown Behavior
//!
//! The worker will wait indefinitely for the current checkpoint to complete.
//! Jobs are released back to Pending status, allowing other workers to pick them up.
//! The heartbeat is cleared and the worker exits cleanly.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::broadcast;
use tokio::time::timeout;

use super::tikv::TikvError;

/// Default shutdown timeout in seconds.
pub const DEFAULT_SHUTDOWN_TIMEOUT_SECS: u64 = 30;

/// Shutdown handler for graceful worker termination.
///
/// This handler manages the shutdown process by:
/// - Registering signal handlers for SIGTERM and SIGINT
/// - Setting a shared shutdown flag when signals are received
/// - Broadcasting shutdown notifications to all listeners
/// - Enforcing a timeout for forced termination
#[derive(Clone)]
pub struct ShutdownHandler {
    /// Shared shutdown flag.
    shutdown_flag: Arc<AtomicBool>,

    /// Shutdown sender for broadcasting shutdown signals.
    shutdown_tx: broadcast::Sender<()>,
}

impl ShutdownHandler {
    /// Create a new shutdown handler.
    pub fn new() -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        Self {
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            shutdown_tx,
        }
    }

    /// Start listening for shutdown signals.
    ///
    /// This spawns a background task that listens for SIGTERM and SIGINT.
    /// When either signal is received, the shutdown flag is set and all
    /// subscribers are notified.
    ///
    /// # Returns
    ///
    /// A receiver that can be used to listen for shutdown notifications.
    pub fn start_signal_handler(&self) -> broadcast::Receiver<()> {
        let mut sigterm = signal(SignalKind::terminate()).expect("Failed to setup SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to setup SIGINT handler");

        let flag = self.shutdown_flag.clone();
        let tx = self.shutdown_tx.clone();

        tokio::spawn(async move {
            tokio::select! {
                _ = sigterm.recv() => {
                    tracing::info!("SIGTERM received, initiating graceful shutdown");
                }
                _ = sigint.recv() => {
                    tracing::info!("SIGINT received, initiating graceful shutdown");
                }
            }

            flag.store(true, Ordering::SeqCst);
            let _ = tx.send(());

            // Keep the task alive to maintain the broadcast channel
            std::future::pending::<()>().await;
        });

        self.shutdown_tx.subscribe()
    }

    /// Subscribe to shutdown notifications.
    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.shutdown_tx.subscribe()
    }

    /// Get a clone of the shutdown sender for creating subscriptions.
    pub fn sender(&self) -> broadcast::Sender<()> {
        self.shutdown_tx.clone()
    }

    /// Check if shutdown has been requested.
    pub fn is_requested(&self) -> bool {
        self.shutdown_flag.load(Ordering::SeqCst)
    }

    /// Request shutdown programmatically.
    pub fn shutdown(&self) {
        tracing::info!("Programmatic shutdown requested");
        self.shutdown_flag.store(true, Ordering::SeqCst);
        let _ = self.shutdown_tx.send(());
    }

    /// Wait for shutdown signal with a timeout.
    ///
    /// Returns `true` if shutdown was requested, `false` if timeout occurred.
    pub async fn wait_with_timeout(&self, timeout_secs: u64) -> bool {
        let mut rx = self.subscribe();
        let dur = Duration::from_secs(timeout_secs);

        match timeout(dur, rx.recv()).await {
            Ok(Ok(())) => true,
            Ok(Err(_)) => true, // Channel closed means sender dropped (shutdown)
            Err(_) => false,    // Timeout
        }
    }

    /// Get a reference to the shutdown flag for passing to callbacks.
    pub fn flag(&self) -> &Arc<AtomicBool> {
        &self.shutdown_flag
    }

    /// Get a clone of the shutdown flag.
    pub fn flag_clone(&self) -> Arc<AtomicBool> {
        self.shutdown_flag.clone()
    }
}

impl Default for ShutdownHandler {
    fn default() -> Self {
        Self::new()
    }
}

/// Error returned when shutdown is requested during processing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShutdownInterrupted;

impl std::fmt::Display for ShutdownInterrupted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Processing interrupted by shutdown signal")
    }
}

impl std::error::Error for ShutdownInterrupted {}

/// Convert ShutdownInterrupted to TikvError.
impl From<ShutdownInterrupted> for TikvError {
    fn from(_err: ShutdownInterrupted) -> Self {
        TikvError::Other("Job interrupted by shutdown".to_string())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shutdown_handler_default() {
        let handler = ShutdownHandler::default();
        assert!(!handler.is_requested());
    }

    #[test]
    fn test_shutdown_handler_new() {
        let handler = ShutdownHandler::new();
        assert!(!handler.is_requested());
    }

    #[test]
    fn test_shutdown_programmatic() {
        let handler = ShutdownHandler::new();
        assert!(!handler.is_requested());

        handler.shutdown();
        assert!(handler.is_requested());
    }

    #[test]
    fn test_shutdown_flag_clone() {
        let handler = ShutdownHandler::new();
        let flag = handler.flag_clone();

        assert!(!flag.load(Ordering::SeqCst));

        handler.shutdown();
        assert!(flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_SHUTDOWN_TIMEOUT_SECS, 30);
    }
}
