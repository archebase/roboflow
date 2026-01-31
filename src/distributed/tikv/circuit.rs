// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Circuit breaker pattern for TiKV fault tolerance.
//!
//! The circuit breaker prevents cascading failures by failing fast when
//! the TiKV service is experiencing issues. It has three states:
//!
//! - **Closed**: Normal operation, requests pass through
//! - **Open**: Service is down, requests fail immediately
//! - **Half-Open**: Testing if service has recovered (limited requests allowed)
//!
//! ## State Transitions
//!
//! ```text
//!                    failure threshold reached
//!     Closed  ----------------------------->  Open
//!        ^                                      |
//!        |                                      | timeout expires
//!        |                                      v
//!        |<--------------------------- Half-Open
//!        |                                   |
//!        | success (after attempts)           | failure
//!        +-----------------------------------+
//! ```
//!
//! ## Usage
//!
//! ```rust
//! use distributed::tikv::circuit::{CircuitBreaker, CircuitConfig};
//!
//! let config = CircuitConfig::default();
//! let breaker = CircuitBreaker::new(config);
//!
//! // Execute operation with circuit breaker protection
//! match breaker.call(|| async {
//!     // Your TiKV operation here
//!     Ok::<(), TikvError>(())
//! }).await {
//!     Ok(_) => println!("Operation succeeded"),
//!     Err(TikvError::CircuitOpen) => println!("Circuit is open, failing fast"),
//!     Err(e) => println!("Operation failed: {}", e),
//! }
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use super::error::{Result, TikvError};

/// Circuit breaker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Circuit is closed - normal operation.
    Closed = 0,
    /// Circuit is open - service is down, fail fast.
    Open = 1,
    /// Circuit is half-open - testing if service recovered.
    HalfOpen = 2,
}

impl CircuitState {
    /// Convert from u8.
    fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Closed),
            1 => Some(Self::Open),
            2 => Some(Self::HalfOpen),
            _ => None,
        }
    }
}

/// Configuration for the circuit breaker.
#[derive(Debug, Clone)]
pub struct CircuitConfig {
    /// Number of consecutive failures before opening the circuit.
    pub failure_threshold: u32,

    /// Number of successful attempts in half-open before closing.
    pub success_threshold: u32,

    /// How long to wait before attempting recovery (open -> half-open).
    pub open_timeout: Duration,

    /// Maximum number of calls allowed in half-open state.
    pub half_open_max_calls: u32,

    /// How long to keep half-open state before reverting to open.
    pub half_open_timeout: Duration,
}

impl Default for CircuitConfig {
    fn default() -> Self {
        Self {
            // Open circuit after 5 consecutive failures
            failure_threshold: 5,
            // Close circuit after 3 consecutive successes in half-open
            success_threshold: 3,
            // Wait 30 seconds before attempting recovery
            open_timeout: Duration::from_secs(30),
            // Allow up to 10 calls in half-open state
            half_open_max_calls: 10,
            // Give 5 seconds for half-open attempts
            half_open_timeout: Duration::from_secs(5),
        }
    }
}

impl CircuitConfig {
    /// Create a new configuration with custom failure threshold.
    pub fn with_failure_threshold(mut self, threshold: u32) -> Self {
        self.failure_threshold = threshold;
        self
    }

    /// Create a new configuration with custom open timeout.
    pub fn with_open_timeout(mut self, timeout: Duration) -> Self {
        self.open_timeout = timeout;
        self
    }

    /// Create a new configuration with custom success threshold.
    pub fn with_success_threshold(mut self, threshold: u32) -> Self {
        self.success_threshold = threshold;
        self
    }
}

/// Internal state shared across all clones of the circuit breaker.
struct CircuitInner {
    /// Current circuit state (atomic for lock-free reads).
    state: AtomicU8,

    /// Consecutive failure count.
    failures: AtomicU64,

    /// Consecutive success count (for half-open -> closed).
    successes: AtomicU64,

    /// Number of calls in half-open state.
    half_open_calls: AtomicU64,

    /// Timestamp of last state change (for timeout checks).
    last_state_change: AtomicU64,

    /// Configuration.
    config: CircuitConfig,
}

impl CircuitInner {
    fn new(config: CircuitConfig) -> Self {
        Self {
            state: AtomicU8::new(CircuitState::Closed as u8),
            failures: AtomicU64::new(0),
            successes: AtomicU64::new(0),
            half_open_calls: AtomicU64::new(0),
            last_state_change: AtomicU64::new(now_timestamp()),
            config,
        }
    }

    fn get_state(&self) -> CircuitState {
        CircuitState::from_u8(self.state.load(Ordering::Acquire)).unwrap_or(CircuitState::Closed)
    }

    fn set_state(&self, new_state: CircuitState) {
        self.state.store(new_state as u8, Ordering::Release);
        self.last_state_change
            .store(now_timestamp(), Ordering::Release);

        // Reset counters on state change
        match new_state {
            CircuitState::Closed => {
                self.failures.store(0, Ordering::Release);
                self.successes.store(0, Ordering::Release);
                self.half_open_calls.store(0, Ordering::Release);
            }
            CircuitState::Open => {
                self.successes.store(0, Ordering::Release);
            }
            CircuitState::HalfOpen => {
                self.half_open_calls.store(0, Ordering::Release);
            }
        }
    }

    fn should_attempt_reset(&self) -> bool {
        let last_change = self.last_state_change.load(Ordering::Acquire);
        let elapsed = Duration::from_secs(now_timestamp().saturating_sub(last_change));
        elapsed >= self.config.open_timeout
    }

    fn half_open_expired(&self) -> bool {
        let last_change = self.last_state_change.load(Ordering::Acquire);
        let elapsed = Duration::from_secs(now_timestamp().saturating_sub(last_change));
        elapsed >= self.config.half_open_timeout
    }

    fn increment_failures(&self) -> u64 {
        self.failures.fetch_add(1, Ordering::AcqRel) + 1
    }

    fn increment_successes(&self) -> u64 {
        self.successes.fetch_add(1, Ordering::AcqRel) + 1
    }

    fn increment_half_open_calls(&self) -> u64 {
        self.half_open_calls.fetch_add(1, Ordering::AcqRel) + 1
    }

    fn get_failure_count(&self) -> u64 {
        self.failures.load(Ordering::Acquire)
    }

    fn get_success_count(&self) -> u64 {
        self.successes.load(Ordering::Acquire)
    }

    fn get_half_open_calls(&self) -> u64 {
        self.half_open_calls.load(Ordering::Acquire)
    }
}

/// Circuit breaker for TiKV fault tolerance.
///
/// This type uses atomic operations for lock-free state management and
/// is cheap to clone.
#[derive(Clone)]
pub struct CircuitBreaker {
    inner: Arc<CircuitInner>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with default configuration.
    pub fn new() -> Self {
        Self::with_config(CircuitConfig::default())
    }

    /// Create a new circuit breaker with custom configuration.
    pub fn with_config(config: CircuitConfig) -> Self {
        Self {
            inner: Arc::new(CircuitInner::new(config)),
        }
    }

    /// Get the current circuit state.
    pub fn state(&self) -> CircuitState {
        self.inner.get_state()
    }

    /// Get the number of consecutive failures.
    pub fn failure_count(&self) -> u64 {
        self.inner.get_failure_count()
    }

    /// Get the number of consecutive successes (in half-open state).
    pub fn success_count(&self) -> u64 {
        self.inner.get_success_count()
    }

    /// Check if the circuit allows requests.
    pub fn is_call_permitted(&self) -> bool {
        match self.inner.get_state() {
            CircuitState::Closed => true,
            CircuitState::Open => {
                // Check if we should transition to half-open
                if self.inner.should_attempt_reset() {
                    tracing::info!(
                        state = "HalfOpen",
                        reason = "open_timeout_expired",
                        "Circuit breaker state transition"
                    );
                    self.inner.set_state(CircuitState::HalfOpen);
                    true
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => {
                // Check if half-open has expired
                if self.inner.half_open_expired() {
                    tracing::warn!(
                        state = "Open",
                        reason = "half_open_timeout_expired",
                        "Circuit breaker state transition"
                    );
                    self.inner.set_state(CircuitState::Open);
                    false
                } else if self.inner.get_half_open_calls()
                    >= self.inner.config.half_open_max_calls as u64
                {
                    tracing::warn!(
                        state = "Open",
                        reason = "half_open_max_calls_exceeded",
                        "Circuit breaker state transition"
                    );
                    self.inner.set_state(CircuitState::Open);
                    false
                } else {
                    true
                }
            }
        }
    }

    /// Record a successful call.
    pub fn record_success(&self) {
        match self.inner.get_state() {
            CircuitState::Closed => {
                // Reset failure count on success
                self.inner.failures.store(0, Ordering::Release);
            }
            CircuitState::HalfOpen => {
                let successes = self.inner.increment_successes();
                tracing::debug!(
                    successes = successes,
                    threshold = self.inner.config.success_threshold,
                    "Recording success in half-open state"
                );

                if successes >= self.inner.config.success_threshold as u64 {
                    tracing::info!(
                        state = "Closed",
                        successes = successes,
                        "Circuit breaker recovered"
                    );
                    self.inner.set_state(CircuitState::Closed);
                }
            }
            CircuitState::Open => {
                // Shouldn't happen, but reset if it does
                tracing::warn!("Recorded success while circuit is open");
            }
        }
    }

    /// Record a failed call.
    pub fn record_failure(&self) {
        let failures = self.inner.increment_failures();
        tracing::debug!(
            failures = failures,
            threshold = self.inner.config.failure_threshold,
            state = ?self.inner.get_state(),
            "Recording failure"
        );

        match self.inner.get_state() {
            CircuitState::Closed => {
                if failures >= self.inner.config.failure_threshold as u64 {
                    tracing::error!(
                        state = "Open",
                        failures = failures,
                        threshold = self.inner.config.failure_threshold,
                        "Circuit breaker opened due to consecutive failures"
                    );
                    self.inner.set_state(CircuitState::Open);
                }
            }
            CircuitState::HalfOpen => {
                // Any failure in half-open trips back to open
                tracing::warn!(
                    state = "Open",
                    reason = "failure_in_half_open",
                    "Circuit breaker re-opened"
                );
                self.inner.set_state(CircuitState::Open);
            }
            CircuitState::Open => {
                // Already open, nothing to do
            }
        }
    }

    /// Execute an operation with circuit breaker protection.
    ///
    /// Returns an error immediately if the circuit is open. Otherwise,
    /// executes the function and records success or failure.
    pub async fn call<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        if !self.is_call_permitted() {
            tracing::warn!(
                state = ?self.inner.get_state(),
                failures = self.inner.get_failure_count(),
                "Circuit breaker rejecting call"
            );
            return Err(TikvError::CircuitOpen {
                failures: self.inner.get_failure_count() as u32,
            });
        }

        // Increment half-open call counter if applicable
        if self.inner.get_state() == CircuitState::HalfOpen {
            self.inner.increment_half_open_calls();
        }

        match f().await {
            Ok(result) => {
                self.record_success();
                Ok(result)
            }
            Err(err) => {
                // Only record failure for retryable errors
                // Connection errors and timeouts indicate circuit issues
                if err.is_retryable() || matches!(err, TikvError::ConnectionFailed(_)) {
                    self.record_failure();
                }
                Err(err)
            }
        }
    }

    /// Manually reset the circuit to closed state.
    ///
    /// This is useful for testing or when you know the service has recovered.
    pub fn reset(&self) {
        tracing::info!(
            state = "Closed",
            reason = "manual_reset",
            "Circuit breaker reset"
        );
        self.inner.set_state(CircuitState::Closed);
    }

    /// Manually open the circuit.
    ///
    /// This is useful for testing or when you want to prevent calls.
    pub fn trip(&self) {
        tracing::warn!(
            state = "Open",
            reason = "manual_trip",
            "Circuit breaker tripped"
        );
        self.inner.set_state(CircuitState::Open);
    }
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new()
    }
}

/// Get current timestamp as seconds since epoch.
fn now_timestamp() -> u64 {
    #[cfg(feature = "distributed")]
    {
        use std::time::SystemTime;
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    #[cfg(not(feature = "distributed"))]
    {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_from_u8() {
        assert_eq!(CircuitState::from_u8(0), Some(CircuitState::Closed));
        assert_eq!(CircuitState::from_u8(1), Some(CircuitState::Open));
        assert_eq!(CircuitState::from_u8(2), Some(CircuitState::HalfOpen));
        assert_eq!(CircuitState::from_u8(3), None);
    }

    #[test]
    fn test_circuit_default() {
        let breaker = CircuitBreaker::new();
        assert_eq!(breaker.state(), CircuitState::Closed);
        assert_eq!(breaker.failure_count(), 0);
    }

    #[test]
    fn test_circuit_opens_on_failures() {
        let config = CircuitConfig {
            failure_threshold: 3,
            ..Default::default()
        };
        let breaker = CircuitBreaker::with_config(config);

        assert_eq!(breaker.state(), CircuitState::Closed);
        assert!(breaker.is_call_permitted());

        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Closed);
        assert!(breaker.is_call_permitted());

        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Closed);
        assert!(breaker.is_call_permitted());

        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);
        assert!(!breaker.is_call_permitted());
    }

    #[test]
    fn test_circuit_resets_on_success() {
        let config = CircuitConfig {
            failure_threshold: 3,
            success_threshold: 2,
            ..Default::default()
        };
        let breaker = CircuitBreaker::with_config(config);

        // Trip the circuit
        breaker.record_failure();
        breaker.record_failure();
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);

        // Reset to closed manually (simulating timeout)
        breaker.reset();
        assert_eq!(breaker.state(), CircuitState::Closed);

        // Successes in closed state reset failure count
        breaker.record_failure();
        breaker.record_success();
        assert_eq!(breaker.failure_count(), 0);
        assert_eq!(breaker.state(), CircuitState::Closed);
    }

    #[test]
    fn test_half_open_to_closed() {
        let config = CircuitConfig {
            failure_threshold: 2,
            success_threshold: 3,
            ..Default::default()
        };
        let breaker = CircuitBreaker::with_config(config);

        // Trip the circuit
        breaker.record_failure();
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);

        // Manually transition to half-open
        breaker.inner.set_state(CircuitState::HalfOpen);
        assert_eq!(breaker.state(), CircuitState::HalfOpen);
        assert!(breaker.is_call_permitted());

        // Successes in half-open should close circuit
        breaker.record_success();
        assert_eq!(breaker.state(), CircuitState::HalfOpen);

        breaker.record_success();
        assert_eq!(breaker.state(), CircuitState::HalfOpen);

        breaker.record_success();
        assert_eq!(breaker.state(), CircuitState::Closed);
    }

    #[test]
    fn test_half_open_failure_reopens() {
        let config = CircuitConfig {
            failure_threshold: 2,
            success_threshold: 3,
            ..Default::default()
        };
        let breaker = CircuitBreaker::with_config(config);

        // Trip the circuit
        breaker.record_failure();
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);

        // Manually transition to half-open
        breaker.inner.set_state(CircuitState::HalfOpen);

        // Record a success
        breaker.record_success();
        assert_eq!(breaker.success_count(), 1);

        // Any failure trips back to open
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);
        assert_eq!(breaker.success_count(), 0);
    }

    #[test]
    fn test_manual_trip_and_reset() {
        let breaker = CircuitBreaker::new();

        breaker.trip();
        assert_eq!(breaker.state(), CircuitState::Open);
        assert!(!breaker.is_call_permitted());

        breaker.reset();
        assert_eq!(breaker.state(), CircuitState::Closed);
        assert!(breaker.is_call_permitted());
    }

    #[test]
    fn test_config_builder() {
        let config = CircuitConfig::default()
            .with_failure_threshold(10)
            .with_open_timeout(Duration::from_secs(60))
            .with_success_threshold(5);

        assert_eq!(config.failure_threshold, 10);
        assert_eq!(config.open_timeout, Duration::from_secs(60));
        assert_eq!(config.success_threshold, 5);
    }
}
