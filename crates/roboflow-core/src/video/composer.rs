// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Video composition trait for merging MP4 segments.
//!
//! The `VideoComposer` trait provides proper MP4 remuxing (via FFmpeg concat demuxer)
//! instead of byte concatenation, which produces invalid files when multiple
//! segments are created during memory-bounded processing.

use std::path::Path;

use crate::Result;

/// Composes multiple video segments into a single valid video file.
///
/// This trait is SYNCHRONOUS because video composition is CPU-bound.
/// Implementations use rsmpeg (in-process FFmpeg) for composition.
///
/// # When is this needed?
///
/// - Small files (< 2GB default memory limit): Single segment, composition not needed
/// - Large files (> 2GB): Multiple segments created, composition REQUIRED
///
/// # Implementation Notes
///
/// - Uses FFmpeg concat demuxer with stream copy (no re-encode)
/// - All segments must have same codec, resolution, and frame rate
/// - Handles timestamp continuity across segments
pub trait VideoComposer: Send + Sync {
    /// Compose multiple video segments into a single file.
    ///
    /// # Arguments
    ///
    /// * `sources` - Paths to segment files (must be same codec/resolution/fps)
    /// * `dest` - Output path for merged video
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Sources have incompatible codecs/resolutions
    /// - FFmpeg composition fails
    /// - I/O error occurs
    fn compose(&self, sources: &[&Path], dest: &Path) -> Result<()>;

    /// Check if sources can be composed (same format, codec, etc.).
    ///
    /// This is optional validation before calling `compose`.
    fn can_compose(&self, _sources: &[&Path]) -> Result<()> {
        Ok(())
    }
}

/// Mock video composer for testing.
///
/// Records composition operations without actually performing them.
pub struct MockVideoComposer {
    operations: std::sync::Mutex<Vec<ComposeOperation>>,
}

/// Recorded compose operation for testing.
#[derive(Debug, Clone)]
pub struct ComposeOperation {
    /// Source paths.
    pub sources: Vec<std::path::PathBuf>,
    /// Destination path.
    pub dest: std::path::PathBuf,
}

impl MockVideoComposer {
    /// Create a new mock composer.
    pub fn new() -> Self {
        Self {
            operations: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Get all recorded operations.
    pub fn get_operations(&self) -> Vec<ComposeOperation> {
        self.operations.lock().unwrap().clone()
    }

    /// Get the number of operations.
    pub fn operation_count(&self) -> usize {
        self.operations.lock().unwrap().len()
    }

    /// Clear all recorded operations.
    pub fn clear(&self) {
        self.operations.lock().unwrap().clear();
    }
}

impl Default for MockVideoComposer {
    fn default() -> Self {
        Self::new()
    }
}

impl VideoComposer for MockVideoComposer {
    fn compose(&self, sources: &[&Path], dest: &Path) -> Result<()> {
        self.operations.lock().unwrap().push(ComposeOperation {
            sources: sources.iter().map(|p| p.to_path_buf()).collect(),
            dest: dest.to_path_buf(),
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_mock_composer_records_operations() {
        let composer = MockVideoComposer::new();
        let sources = [Path::new("seg0.mp4"), Path::new("seg1.mp4")];

        composer.compose(&sources, Path::new("out.mp4")).unwrap();

        let ops = composer.get_operations();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].sources.len(), 2);
        assert_eq!(ops[0].dest, PathBuf::from("out.mp4"));
    }

    #[test]
    fn test_mock_composer_clear() {
        let composer = MockVideoComposer::new();

        composer
            .compose(&[Path::new("a.mp4")], Path::new("out.mp4"))
            .unwrap();
        assert_eq!(composer.operation_count(), 1);

        composer.clear();
        assert_eq!(composer.operation_count(), 0);
    }
}
