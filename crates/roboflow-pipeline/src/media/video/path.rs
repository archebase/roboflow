// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Video path schemes for different dataset formats.
//!
//! This module provides implementations of the `VideoPathScheme` trait
//! for different dataset formats.

use crate::core::VideoPathScheme;
use std::path::{Path, PathBuf};

/// LeRobot v2.1 video path scheme.
///
/// Uses the directory structure:
/// ```text
/// videos/chunk-{chunk:03d}/{camera}/episode_{episode:06d}.mp4
/// ```
///
/// # Example
///
/// ```rust,ignore
/// use roboflow_pipeline::video::LeRobotVideoPathScheme;
///
/// let scheme = LeRobotVideoPathScheme::new("dataset/episode_001");
/// let path = scheme.video_path(0, "observation.images.cam_left", 0);
/// // Returns: "dataset/episode_001/videos/chunk-000/observation.images.cam_left/episode_000000.mp4"
/// ```
#[derive(Debug, Clone)]
pub struct LeRobotVideoPathScheme {
    /// Base path prefix (e.g., "dataset/episode_001").
    prefix: PathBuf,
}

impl LeRobotVideoPathScheme {
    /// Create a new LeRobot video path scheme.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Base path prefix (relative path within bucket)
    pub fn new(prefix: impl Into<PathBuf>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }

    /// Create with no prefix (videos at root).
    pub fn root() -> Self {
        Self {
            prefix: PathBuf::new(),
        }
    }

    /// Get the prefix.
    pub fn prefix(&self) -> &Path {
        &self.prefix
    }
}

impl VideoPathScheme for LeRobotVideoPathScheme {
    fn video_path(&self, episode: usize, camera: &str, chunk: usize) -> PathBuf {
        self.prefix
            .join("videos")
            .join(format!("chunk-{chunk:03}"))
            .join(camera)
            .join(format!("episode_{episode:06}.mp4"))
    }

    fn chunk_dir(&self, chunk: usize) -> PathBuf {
        self.prefix.join("videos").join(format!("chunk-{chunk:03}"))
    }

    fn parse_episode(&self, path: &Path) -> Option<usize> {
        // Extract episode number from filename like "episode_000000.mp4"
        let filename = path.file_name()?.to_str()?;
        if let Some(stripped) = filename.strip_prefix("episode_")
            && let Some(stripped) = stripped.strip_suffix(".mp4")
        {
            return stripped.parse().ok();
        }
        None
    }

    fn video_extension(&self) -> &str {
        "mp4"
    }

    fn scheme_name(&self) -> &'static str {
        "lerobot"
    }
}

/// RLDS video path scheme.
///
/// Uses the directory structure:
/// ```text
/// episode_{episode}/videos/{camera}.mp4
/// ```
#[derive(Debug, Clone)]
pub struct RldsVideoPathScheme {
    /// Base path prefix.
    prefix: PathBuf,
}

impl RldsVideoPathScheme {
    /// Create a new RLDS video path scheme.
    pub fn new(prefix: impl Into<PathBuf>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }

    /// Create with no prefix (videos at root).
    pub fn root() -> Self {
        Self {
            prefix: PathBuf::new(),
        }
    }
}

impl VideoPathScheme for RldsVideoPathScheme {
    fn video_path(&self, episode: usize, camera: &str, chunk: usize) -> PathBuf {
        let _ = chunk; // RLDS doesn't use chunks
        self.prefix
            .join(format!("episode_{episode}"))
            .join("videos")
            .join(format!("{camera}.mp4"))
    }

    fn chunk_dir(&self, chunk: usize) -> PathBuf {
        let _ = chunk;
        self.prefix.clone()
    }

    fn parse_episode(&self, path: &Path) -> Option<usize> {
        // Look for "episode_N" in the path
        for component in path.components() {
            if let Some(name) = component.as_os_str().to_str()
                && let Some(stripped) = name.strip_prefix("episode_")
                && let Ok(episode) = stripped.parse()
            {
                return Some(episode);
            }
        }
        None
    }

    fn video_extension(&self) -> &str {
        "mp4"
    }

    fn scheme_name(&self) -> &'static str {
        "rlds"
    }
}

/// Simple flat video path scheme.
///
/// Uses a simple structure:
/// ```text
/// {camera}/episode_{episode:06d}.mp4
/// ```
#[derive(Debug, Clone)]
pub struct FlatVideoPathScheme {
    /// Base path prefix.
    prefix: PathBuf,
}

impl FlatVideoPathScheme {
    /// Create a new flat video path scheme.
    pub fn new(prefix: impl Into<PathBuf>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }
}

impl VideoPathScheme for FlatVideoPathScheme {
    fn video_path(&self, episode: usize, camera: &str, chunk: usize) -> PathBuf {
        let _ = chunk;
        self.prefix
            .join(camera)
            .join(format!("episode_{episode:06}.mp4"))
    }

    fn chunk_dir(&self, chunk: usize) -> PathBuf {
        let _ = chunk;
        self.prefix.clone()
    }

    fn parse_episode(&self, path: &Path) -> Option<usize> {
        let filename = path.file_name()?.to_str()?;
        if let Some(stripped) = filename.strip_prefix("episode_")
            && let Some(stripped) = stripped.strip_suffix(".mp4")
        {
            return stripped.parse().ok();
        }
        None
    }

    fn video_extension(&self) -> &str {
        "mp4"
    }

    fn scheme_name(&self) -> &'static str {
        "flat"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lerobot_video_path() {
        let scheme = LeRobotVideoPathScheme::new("dataset/episode_001");
        let path = scheme.video_path(0, "observation.images.cam_left", 0);

        assert_eq!(
            path.to_str().unwrap(),
            "dataset/episode_001/videos/chunk-000/observation.images.cam_left/episode_000000.mp4"
        );
    }

    #[test]
    fn test_lerobot_video_path_different_chunk() {
        let scheme = LeRobotVideoPathScheme::new("dataset");
        let path = scheme.video_path(100, "cam_high", 5);

        assert_eq!(
            path.to_str().unwrap(),
            "dataset/videos/chunk-005/cam_high/episode_000100.mp4"
        );
    }

    #[test]
    fn test_lerobot_parse_episode() {
        let scheme = LeRobotVideoPathScheme::root();
        let path = Path::new("videos/chunk-000/cam/episode_000042.mp4");
        assert_eq!(scheme.parse_episode(path), Some(42));
    }

    #[test]
    fn test_lerobot_chunk_dir() {
        let scheme = LeRobotVideoPathScheme::new("dataset");
        let dir = scheme.chunk_dir(0);
        assert_eq!(dir.to_str().unwrap(), "dataset/videos/chunk-000");
    }

    #[test]
    fn test_rlds_video_path() {
        let scheme = RldsVideoPathScheme::new("rlds_data");
        let path = scheme.video_path(5, "camera_0", 0);

        assert_eq!(
            path.to_str().unwrap(),
            "rlds_data/episode_5/videos/camera_0.mp4"
        );
    }

    #[test]
    fn test_rlds_parse_episode() {
        let scheme = RldsVideoPathScheme::root();
        let path = Path::new("episode_10/videos/camera.mp4");
        assert_eq!(scheme.parse_episode(path), Some(10));
    }

    #[test]
    fn test_flat_video_path() {
        let scheme = FlatVideoPathScheme::new("output");
        let path = scheme.video_path(3, "cam_left", 0);

        assert_eq!(path.to_str().unwrap(), "output/cam_left/episode_000003.mp4");
    }
}
