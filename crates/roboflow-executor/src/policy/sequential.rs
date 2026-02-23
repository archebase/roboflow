// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Sequential execution policy.
//!
//! This module provides [`SequentialPolicy`] which processes items
//! one at a time in order on the current thread.

use roboflow_core::Result;

use super::ExecutionPolicy;

/// Sequential execution policy.
///
/// Processes items one at a time in order on the current thread.
/// This is the simplest policy and is useful for:
/// - Debugging
/// - Single-threaded environments
/// - When deterministic ordering is critical
///
/// # Example
///
/// ```rust,ignore
/// use roboflow_executor::policy::{ExecutionPolicy, SequentialPolicy};
///
/// let policy = SequentialPolicy;
/// let results = policy.execute_batch(vec![1, 2, 3], |x| x * 2);
/// assert_eq!(results, vec![2, 4, 6]);
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct SequentialPolicy;

impl ExecutionPolicy for SequentialPolicy {
    fn execute_batch<F, R>(
        &self,
        items: Vec<F>,
        processor: impl Fn(F) -> R + Send + Sync + Clone,
    ) -> Vec<R>
    where
        F: Send,
        R: Send,
    {
        items.into_iter().map(processor).collect()
    }

    fn execute_batch_fallible<F, R>(
        &self,
        items: Vec<F>,
        processor: impl Fn(F) -> Result<R> + Send + Sync + Clone,
    ) -> Vec<Result<R>>
    where
        F: Send,
        R: Send,
    {
        items.into_iter().map(processor).collect()
    }

    fn name(&self) -> &'static str {
        "sequential"
    }

    fn parallelism(&self) -> usize {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequential_basic() {
        let policy = SequentialPolicy;
        let items = vec![1, 2, 3, 4, 5];
        let results = policy.execute_batch(items, |x| x * 2);
        assert_eq!(results, vec![2, 4, 6, 8, 10]);
    }

    #[test]
    fn test_sequential_empty() {
        let policy = SequentialPolicy;
        let items: Vec<i32> = vec![];
        let results = policy.execute_batch(items, |x| x * 2);
        assert!(results.is_empty());
    }

    #[test]
    fn test_sequential_single_item() {
        let policy = SequentialPolicy;
        let items = vec![42];
        let results = policy.execute_batch(items, |x| x * 2);
        assert_eq!(results, vec![84]);
    }

    #[test]
    fn test_sequential_order_preserved() {
        let policy = SequentialPolicy;
        let items = vec![5, 4, 3, 2, 1];
        let results = policy.execute_batch(items, |x| x);
        assert_eq!(results, vec![5, 4, 3, 2, 1]);
    }

    #[test]
    fn test_sequential_fallible_all_success() {
        let policy = SequentialPolicy;
        let items = vec![1, 2, 3];
        let results: Vec<Result<i32>> = policy.execute_batch_fallible(items, |x| Ok(x * 2));
        assert!(results.iter().all(|r| r.is_ok()));
    }

    #[test]
    fn test_sequential_fallible_some_fail() {
        let policy = SequentialPolicy;
        let items = vec![1, 2, 3, 4, 5];
        let results = policy.execute_batch_fallible(items, |x| {
            if x % 2 == 0 {
                Err(roboflow_core::RoboflowError::other("even numbers are bad"))
            } else {
                Ok(x)
            }
        });

        assert_eq!(results.len(), 5);
        assert!(results[0].is_ok()); // 1 - odd
        assert!(results[1].is_err()); // 2 - even
        assert!(results[2].is_ok()); // 3 - odd
        assert!(results[3].is_err()); // 4 - even
        assert!(results[4].is_ok()); // 5 - odd
    }
}
