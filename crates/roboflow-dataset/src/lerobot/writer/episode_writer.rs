// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! EpisodeWriter trait for dataset writers that support episode-based output.
//!
//! This trait extends `DatasetWriter` with episode management capabilities,
//! allowing writers to handle multiple episodes with configurable chunking.

use crate::common::DatasetWriter;

/// Trait for dataset writers that support episode-based output.
///
/// This trait provides methods for managing episode indices and chunking,
/// which are essential for distributed processing and multi-episode datasets.
///
/// # Design
///
/// - **Episode Index**: A logical identifier for each episode (typically one per input file)
/// - **Chunk Index**: Physical grouping of episodes (e.g., 500 episodes per chunk)
/// - **Distributed Processing**: Episode indices are allocated centrally (e.g., via TiKV)
///   to ensure unique ordering across workers
///
/// # Lifecycle
///
/// 1. Allocate episode index (via EpisodeAllocator)
/// 2. Configure writer with `set_episode_index()`
/// 3. Write frames for the episode
/// 4. Finalize (may auto-increment episode or require manual management)
///
/// # Example
///
/// ```rust,ignore
/// use roboflow::dataset::common::{DatasetWriter, EpisodeWriter};
///
/// let mut writer = LerobotWriter::new_local("/output", config)?;
///
/// // Set episode index (from distributed allocator)
/// writer.set_episode_index(42);
/// writer.set_episodes_per_chunk(500);
///
/// // Write frames for this episode
/// for frame in frames {
///     writer.write_frame(&frame)?;
/// }
///
/// let stats = writer.finalize()?;
/// ```
pub trait EpisodeWriter: DatasetWriter {
    /// Set the current episode index.
    ///
    /// This should be called before writing frames for a new episode.
    /// In distributed processing, the episode index is allocated centrally
    /// to ensure unique ordering across all workers.
    ///
    /// # Arguments
    ///
    /// * `index` - The episode index (0-based, unique across the dataset)
    fn set_episode_index(&mut self, index: usize);

    /// Get the current episode index.
    ///
    /// Returns the episode index that was last set via `set_episode_index()`.
    fn get_episode_index(&self) -> usize;

    /// Set the number of episodes per chunk.
    ///
    /// LeRobot v2.1 organizes episodes into chunks (default: 500 episodes per chunk).
    /// Episodes 0-499 go to chunk-000, 500-999 to chunk-001, etc.
    ///
    /// # Arguments
    ///
    /// * `count` - Number of episodes per chunk (must be > 0)
    fn set_episodes_per_chunk(&mut self, count: u32);

    /// Get the current chunk index.
    ///
    /// The chunk index is computed as `episode_index / episodes_per_chunk`.
    /// This determines the output directory for the current episode.
    fn get_chunk_index(&self) -> u32;

    /// Get the number of episodes per chunk.
    ///
    /// Returns the value set via `set_episodes_per_chunk()`,
    /// or the default (500) if not explicitly set.
    fn get_episodes_per_chunk(&self) -> u32;
}
