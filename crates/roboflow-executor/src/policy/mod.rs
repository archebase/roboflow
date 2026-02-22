// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Execution policy for pipeline processing.
//!
//! This module provides the [`ExecutionPolicy`] trait which abstracts over
//! sequential vs parallel execution strategies. This allows pipeline executors
//! to be parameterized by execution strategy.
//!
//! # Policies
//!
//! - [`SequentialPolicy`] - Single-threaded sequential execution
//! - [`ParallelPolicy`] - Multi-threaded parallel execution using rayon
//!
//! # Example
//!
//! ```rust,ignore
//! use roboflow_executor::policy::{ExecutionPolicy, SequentialPolicy};
//!
//! let policy = SequentialPolicy;
//! let results = policy.execute_batch(items, |item| process(item));
//! ```

pub mod parallel;
pub mod sequential;

use roboflow_core::Result;

pub use parallel::ParallelPolicy;
pub use sequential::SequentialPolicy;

/// Execution policy trait for batch processing.
///
/// This trait abstracts over different execution strategies (sequential,
/// parallel) allowing pipeline executors to be parameterized by how
/// they process batches of work.
///
/// # Type Parameters
///
/// * `F` - Input item type (must be `Send` for parallel execution)
/// * `R` - Output item type (must be `Send` for parallel execution)
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` to allow use in multi-threaded
/// contexts. Input and output types must also be `Send` for parallel
/// execution.
pub trait ExecutionPolicy: Send + Sync {
    /// Execute a batch of items using the given processor function.
    ///
    /// # Arguments
    ///
    /// * `items` - Items to process
    /// * `processor` - Function to process each item
    ///
    /// # Returns
    ///
    /// Vector of results in the same order as inputs.
    fn execute_batch<F, R>(
        &self,
        items: Vec<F>,
        processor: impl Fn(F) -> R + Send + Sync + Clone,
    ) -> Vec<R>
    where
        F: Send,
        R: Send;

    /// Execute a fallible batch of items using the given processor function.
    ///
    /// # Arguments
    ///
    /// * `items` - Items to process
    /// * `processor` - Function to process each item, may fail
    ///
    /// # Returns
    ///
    /// Vector of results in the same order as inputs. Failed items
    /// will have their errors in the result vector.
    fn execute_batch_fallible<F, R>(
        &self,
        items: Vec<F>,
        processor: impl Fn(F) -> Result<R> + Send + Sync + Clone,
    ) -> Vec<Result<R>>
    where
        F: Send,
        R: Send;

    /// Get the name of this policy for logging/metrics.
    fn name(&self) -> &'static str;

    /// Get the parallelism level (1 for sequential, N for parallel).
    fn parallelism(&self) -> usize;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequential_policy_name() {
        let policy = SequentialPolicy;
        assert_eq!(policy.name(), "sequential");
        assert_eq!(policy.parallelism(), 1);
    }

    #[test]
    fn test_sequential_execute_batch() {
        let policy = SequentialPolicy;
        let items = vec![1, 2, 3, 4, 5];
        let results = policy.execute_batch(items, |x| x * 2);
        assert_eq!(results, vec![2, 4, 6, 8, 10]);
    }

    #[test]
    fn test_sequential_execute_batch_fallible() {
        let policy = SequentialPolicy;
        let items = vec![1, 2, 3, 4, 5];
        let results = policy.execute_batch_fallible(items, |x| {
            if x == 3 {
                Err(roboflow_core::RoboflowError::other("three is bad"))
            } else {
                Ok(x * 2)
            }
        });

        assert!(results[0].is_ok());
        assert!(results[1].is_ok());
        assert!(results[2].is_err());
        assert!(results[3].is_ok());
        assert!(results[4].is_ok());
    }
}
