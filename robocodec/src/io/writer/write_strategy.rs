// Copyright (c) 2026 ArcheBase
// Roboflow is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//     http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

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
