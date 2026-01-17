//! Writing strategies for optimal data output.

/// Writing strategy selector.
///
/// Determines how data is written to the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteStrategy {
    /// Sequential writing - processes messages one by one
    Sequential,
    /// Parallel writing - compresses chunks in parallel
    Parallel,
}

impl Default for WriteStrategy {
    fn default() -> Self {
        WriteStrategy::Sequential
    }
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
#[derive(Debug, Clone, Copy)]
pub struct ParallelWrite {
    /// Number of compression threads
    pub num_threads: Option<usize>,
}

impl Default for ParallelWrite {
    fn default() -> Self {
        Self { num_threads: None }
    }
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
