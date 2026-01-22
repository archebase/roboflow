// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Writing strategies for optimal data output.

/// Writing strategy selector.
///
/// Determines how data is written to the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriteStrategy {
    /// Sequential writing - processes messages one by one
    #[default]
    Sequential,
    /// Parallel writing - compresses chunks in parallel
    Parallel,
}

/// Sequential writing strategy.
///
/// Writes messages one at a time without parallel compression.
#[derive(Debug, Clone, Copy, Default)]
pub struct SequentialWrite;

impl SequentialWrite {
    pub fn new() -> Self {
        Self
    }
}

/// Parallel writing strategy.
///
/// Compresses chunks in parallel for improved throughput.
#[derive(Debug, Clone, Copy, Default)]
pub struct ParallelWrite {
    /// Number of compression threads
    pub num_threads: Option<usize>,
}

impl ParallelWrite {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_threads(mut self, num_threads: usize) -> Self {
        self.num_threads = Some(num_threads);
        self
    }
}
