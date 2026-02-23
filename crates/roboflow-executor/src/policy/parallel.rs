// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Parallel execution policy using rayon.
//!
//! This module provides [`ParallelPolicy`] which processes items
//! in parallel using the rayon data parallelism library.

use std::sync::Arc;

use rayon::prelude::*;
use roboflow_core::Result;

use super::ExecutionPolicy;

/// Parallel execution policy using rayon.
///
/// Processes items in parallel using a thread pool. This policy
/// is useful for CPU-bound work that can be parallelized.
///
/// # Thread Pool
///
/// Uses a configurable rayon thread pool. If no pool is specified,
/// uses the global rayon thread pool.
///
/// # Example
///
/// ```rust,ignore
/// use roboflow_executor::policy::{ExecutionPolicy, ParallelPolicy};
///
/// let policy = ParallelPolicy::new(4);
/// let results = policy.execute_batch(vec![1, 2, 3, 4, 5], |x| x * 2);
/// assert_eq!(results, vec![2, 4, 6, 8, 10]);
/// ```
#[derive(Debug)]
pub struct ParallelPolicy {
    /// Optional custom thread pool (uses global if None)
    thread_pool: Option<Arc<rayon::ThreadPool>>,
    /// Number of threads
    num_threads: usize,
}

impl ParallelPolicy {
    /// Create a new parallel policy with the specified number of threads.
    ///
    /// Creates a custom rayon thread pool with the given thread count.
    ///
    /// # Arguments
    ///
    /// * `num_threads` - Number of worker threads to use
    ///
    /// # Panics
    ///
    /// Panics if `num_threads` is 0.
    pub fn new(num_threads: usize) -> Self {
        assert!(num_threads > 0, "num_threads must be > 0");

        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .thread_name(|i| format!("parallel-policy-worker-{}", i))
            .build()
            .expect("Failed to create rayon thread pool");

        Self {
            thread_pool: Some(Arc::new(thread_pool)),
            num_threads,
        }
    }

    /// Create a parallel policy using the global rayon thread pool.
    ///
    /// This uses whatever configuration the global rayon pool has,
    /// which is typically the number of CPU cores.
    pub fn global() -> Self {
        Self {
            thread_pool: None,
            num_threads: rayon::current_num_threads(),
        }
    }

    /// Get the number of threads this policy uses.
    pub fn num_threads(&self) -> usize {
        self.num_threads
    }

    /// Execute work using the thread pool (or global if none).
    fn with_pool<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R + Send,
        R: Send,
    {
        if let Some(pool) = &self.thread_pool {
            pool.install(f)
        } else {
            f()
        }
    }
}

impl Default for ParallelPolicy {
    fn default() -> Self {
        Self::global()
    }
}

impl Clone for ParallelPolicy {
    fn clone(&self) -> Self {
        Self {
            thread_pool: self.thread_pool.clone(),
            num_threads: self.num_threads,
        }
    }
}

impl ExecutionPolicy for ParallelPolicy {
    fn execute_batch<F, R>(
        &self,
        items: Vec<F>,
        processor: impl Fn(F) -> R + Send + Sync + Clone,
    ) -> Vec<R>
    where
        F: Send,
        R: Send,
    {
        self.with_pool(|| items.into_par_iter().map(processor).collect())
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
        self.with_pool(|| items.into_par_iter().map(processor).collect())
    }

    fn name(&self) -> &'static str {
        "parallel"
    }

    fn parallelism(&self) -> usize {
        self.num_threads
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parallel_basic() {
        let policy = ParallelPolicy::new(2);
        let items = vec![1, 2, 3, 4, 5];
        let mut results = policy.execute_batch(items, |x| x * 2);
        results.sort();
        assert_eq!(results, vec![2, 4, 6, 8, 10]);
    }

    #[test]
    fn test_parallel_empty() {
        let policy = ParallelPolicy::new(2);
        let items: Vec<i32> = vec![];
        let results = policy.execute_batch(items, |x| x * 2);
        assert!(results.is_empty());
    }

    #[test]
    fn test_parallel_single_item() {
        let policy = ParallelPolicy::new(2);
        let items = vec![42];
        let results = policy.execute_batch(items, |x| x * 2);
        assert_eq!(results, vec![84]);
    }

    #[test]
    fn test_parallel_fallible_all_success() {
        let policy = ParallelPolicy::new(2);
        let items = vec![1, 2, 3];
        let results: Vec<Result<i32>> = policy.execute_batch_fallible(items, |x| Ok(x * 2));
        let mut success_values: Vec<i32> = results.into_iter().map(|r| r.unwrap()).collect();
        success_values.sort();
        assert_eq!(success_values, vec![2, 4, 6]);
    }

    #[test]
    fn test_parallel_fallible_some_fail() {
        let policy = ParallelPolicy::new(2);
        let items = vec![1, 2, 3, 4, 5];
        let results = policy.execute_batch_fallible(items, |x| {
            if x % 2 == 0 {
                Err(roboflow_core::RoboflowError::other("even numbers are bad"))
            } else {
                Ok(x)
            }
        });

        assert_eq!(results.len(), 5);
        // Check that odd indices succeeded and even indices failed
        assert!(results[0].is_ok()); // 1 - odd
        assert!(results[1].is_err()); // 2 - even
        assert!(results[2].is_ok()); // 3 - odd
        assert!(results[3].is_err()); // 4 - even
        assert!(results[4].is_ok()); // 5 - odd
    }

    #[test]
    fn test_parallel_global() {
        let policy = ParallelPolicy::global();
        assert!(policy.num_threads() > 0);
        assert_eq!(policy.name(), "parallel");
    }

    #[test]
    fn test_parallel_default() {
        let policy = ParallelPolicy::default();
        assert!(policy.num_threads() > 0);
    }

    #[test]
    fn test_parallel_clone() {
        let policy = ParallelPolicy::new(4);
        let cloned = policy.clone();
        assert_eq!(policy.num_threads(), cloned.num_threads());
    }
}
