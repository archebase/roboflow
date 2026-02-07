// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified state lifecycle traits for distributed system status types.
//!
//! This module provides a common trait (`StateLifecycle`) that all state enums
//! in the distributed system implement. This ensures consistent behavior for:
//! - Terminal state detection
//! - Claimability (for work units and jobs)
//! - State transition validation
//!
//! ## Implementing Types
//!
//! - [`JobStatus`](crate::tikv::schema::JobStatus) - Status of distributed jobs
//! - [`WorkUnitStatus`](crate::batch::work_unit::WorkUnitStatus) - Status of work units
//! - [`BatchPhase`](crate::batch::status::BatchPhase) - Phase of batch processing

use std::fmt::Debug;

/// Error type for invalid state transitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateTransitionError {
    /// Transition from terminal state is not allowed
    TerminalState {
        /// The current terminal state
        current: String,
    },
    /// Invalid transition between non-terminal states
    InvalidTransition {
        /// The source state
        from: String,
        /// The target state
        to: String,
    },
}

impl std::fmt::Display for StateTransitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TerminalState { current } => {
                write!(f, "Cannot transition from terminal state: {}", current)
            }
            Self::InvalidTransition { from, to } => {
                write!(f, "Invalid transition from {} to {}", from, to)
            }
        }
    }
}

impl std::error::Error for StateTransitionError {}

/// Unified lifecycle trait for all state enums in the distributed system.
///
/// This trait provides common operations for state machine types:
/// - Terminal state detection (no further transitions possible)
/// - Claimability (can a worker claim this job/work unit?)
/// - Transition validation (is this state change valid?)
///
/// # Example
///
/// ```rust,ignore
/// use roboflow_distributed::state::StateLifecycle;
///
/// let mut status = JobStatus::Pending;
///
/// assert!(!status.is_terminal());
/// assert!(status.is_claimable());
///
/// status.transition_to(JobStatus::Processing).unwrap();
/// assert!(!status.is_claimable());
/// ```
pub trait StateLifecycle: Clone + PartialEq + Eq + Send + Sync + 'static + Debug {
    /// Returns `true` if this state is terminal.
    ///
    /// Terminal states cannot transition to any other state.
    fn is_terminal(&self) -> bool;

    /// Returns `true` if a job/work unit in this state can be claimed by a worker.
    ///
    /// Claimable states are typically those where work has not yet started
    /// or has failed and can be retried.
    fn is_claimable(&self) -> bool;

    /// Returns `true` if a transition from `self` to `target` is valid.
    ///
    /// This method should check all valid state transitions without
    /// modifying the current state.
    fn can_transition_to(&self, target: &Self) -> bool;

    /// Attempt to transition to the target state.
    ///
    /// # Errors
    ///
    /// Returns [`StateTransitionError`] if the transition is invalid.
    fn transition_to(&mut self, target: Self) -> Result<(), StateTransitionError> {
        if self.is_terminal() {
            return Err(StateTransitionError::TerminalState {
                current: format!("{:?}", self),
            });
        }
        if !self.can_transition_to(&target) {
            return Err(StateTransitionError::InvalidTransition {
                from: format!("{:?}", self),
                to: format!("{:?}", target),
            });
        }
        *self = target;
        Ok(())
    }
}

/// Macro to implement `StateLifecycle` for enum types with simple transitions.
///
/// # Usage
///
/// ```rust,ignore
/// impl_state_lifecycle!(MyStatus,
///     terminal => [Completed, Failed],
///     claimable => [Pending, Failed],
///     transitions => {
///         Pending => [Processing, Failed, Cancelled],
///         Processing => [Completed, Failed, Cancelled],
///     }
/// );
/// ```
#[macro_export(local_inner_macros)]
macro_rules! impl_state_lifecycle {
    // Entry point - parses the entire definition
    (
        $ty:ty,
        terminal => [$($terminal:ident),* $(,)?],
        claimable => [$($claimable:ident),* $(,)?],
        transitions => {
            $($from:ident => [$($to:ident),* $(,)?]),* $(,)?
        }
    ) => {
        impl $crate::state::StateLifecycle for $ty {
            fn is_terminal(&self) -> bool {
                matches!(self, $($ty::$terminal)|*)
            }

            fn is_claimable(&self) -> bool {
                matches!(self, $($ty::$claimable)|*)
            }

            fn can_transition_to(&self, target: &Self) -> bool {
                // Self-transition is always allowed (idempotent)
                if self == target {
                    return true;
                }

                // Define valid transitions as a set of (from, to) pairs
                const VALID_TRANSITIONS: &[(($ty, $ty), bool)] = &[
                    $(
                        $(($ty::$from, $ty::$to), true)*
                    ),*
                ];

                // Check if (self, target) is in the valid transitions set
                VALID_TRANSITIONS
                    .iter()
                    .any(|(from_to, _enabled)| from_to.0 == *self && from_to.1 == *target)
            }
        }
    };
}
