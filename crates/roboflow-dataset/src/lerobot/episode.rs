// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Episode tracking and camera calibration conversion utilities.
//!
//! This module provides utilities for:
//! - Episode boundary tracking during dataset writing
//! - Converting ROS CameraInfo messages to LeRobot format

use std::collections::HashMap;

use crate::lerobot::writer::{CameraExtrinsic, CameraIntrinsic};

/// Camera calibration information (ROS CameraInfo compatible).
///
/// This is a local definition to avoid cyclic dependencies with roboflow-sinks.
/// The structure matches the ROS sensor_msgs/CameraInfo message format.
#[derive(Debug, Clone)]
pub struct CameraCalibration {
    /// Camera name/identifier
    pub camera_name: String,
    /// Image width
    pub width: u32,
    /// Image height
    pub height: u32,
    /// K matrix (3x3 row-major): [fx, 0, cx, 0, fy, cy, 0, 0, 1]
    pub k: [f64; 9],
    /// D vector (distortion coefficients)
    pub d: Vec<f64>,
    /// R matrix (3x3 row-major rectification matrix)
    pub r: Option<[f64; 9]>,
    /// P matrix (3x4 row-major projection matrix)
    pub p: Option<[f64; 12]>,
    /// Distortion model name (e.g., "plumb_bob", "rational_polynomial")
    pub distortion_model: String,
}

/// Action to take when tracking episode boundaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EpisodeAction {
    /// Continue with current episode
    Continue,
    /// Finish current episode and start a new one
    FinishAndStart { old_index: usize, new_index: usize },
}

/// Episode boundary tracker.
///
/// Tracks episode transitions during streaming data processing.
/// One bag file typically represents one episode, but episodes
/// can be split by time gaps or frame count.
///
/// # Example
///
/// ```rust,ignore
/// use roboflow_dataset::lerobot::episode::{EpisodeTracker, EpisodeAction};
///
/// let mut tracker = EpisodeTracker::new();
///
/// // Process frames with episode indices
/// for frame in frames {
///     match tracker.track_episode_index(frame.episode_index) {
///         EpisodeAction::FinishAndStart { old_index, .. } => {
///             writer.finish_episode(old_index)?;
///         }
///         EpisodeAction::Continue => {}
///     }
///     writer.write_frame(&frame)?;
/// }
/// ```
#[derive(Debug, Clone, Default)]
pub struct EpisodeTracker {
    /// Current episode index
    current_index: usize,
    /// Whether we've seen any frames yet
    has_frames: bool,
    /// Number of episodes completed
    episodes_completed: usize,
}

impl EpisodeTracker {
    /// Create a new episode tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Track episode based on episode index from the frame.
    ///
    /// # Arguments
    ///
    /// * `episode_index` - Episode index from the current frame
    ///
    /// # Returns
    ///
    /// The action to take based on episode boundary detection.
    pub fn track_episode_index(&mut self, episode_index: usize) -> EpisodeAction {
        if self.has_frames && episode_index != self.current_index {
            let old_index = self.current_index;
            self.current_index = episode_index;
            self.episodes_completed += 1;
            EpisodeAction::FinishAndStart {
                old_index,
                new_index: episode_index,
            }
        } else {
            self.current_index = episode_index;
            self.has_frames = true;
            EpisodeAction::Continue
        }
    }

    /// Get the current episode index.
    pub fn current_index(&self) -> usize {
        self.current_index
    }

    /// Get the number of completed episodes.
    pub fn episodes_completed(&self) -> usize {
        self.episodes_completed
    }

    /// Check if any frames have been processed.
    pub fn has_frames(&self) -> bool {
        self.has_frames
    }

    /// Manually advance to the next episode.
    ///
    /// This is useful when episodes are determined by external logic
    /// rather than frame metadata.
    pub fn advance_episode(&mut self) -> EpisodeAction {
        let old_index = self.current_index;
        self.current_index += 1;
        self.episodes_completed += 1;
        EpisodeAction::FinishAndStart {
            old_index,
            new_index: self.current_index,
        }
    }

    /// Reset the tracker (e.g., when starting a new source).
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

/// Convert camera calibration to LeRobot CameraIntrinsic.
///
/// Extracts intrinsic parameters (focal length, principal point, distortion).
///
/// # Arguments
///
/// * `calibration` - Camera calibration data
///
/// # Returns
///
/// LeRobot CameraIntrinsic structure
pub fn convert_camera_intrinsic(calibration: &CameraCalibration) -> CameraIntrinsic {
    CameraIntrinsic {
        fx: calibration.k[0],
        fy: calibration.k[4],
        ppx: calibration.k[2],
        ppy: calibration.k[5],
        distortion_model: calibration.distortion_model.clone(),
        k1: calibration.d.first().copied().unwrap_or(0.0),
        k2: calibration.d.get(1).copied().unwrap_or(0.0),
        k3: calibration.d.get(4).copied().unwrap_or(0.0),
        p1: calibration.d.get(2).copied().unwrap_or(0.0),
        p2: calibration.d.get(3).copied().unwrap_or(0.0),
    }
}

/// Convert camera calibration to LeRobot CameraExtrinsic.
///
/// Extracts extrinsic parameters (rotation, translation) from the
/// P (projection) matrix.
///
/// The P matrix (3x4 projection) contains extrinsic info when combined with K:
/// `P = K [R|t]` where R is rotation and t is translation.
///
/// We compute `[R|t] = K_inv * P` to extract the extrinsics.
///
/// # Arguments
///
/// * `calibration` - Camera calibration data
///
/// # Returns
///
/// LeRobot CameraExtrinsic structure if P matrix is available
pub fn convert_camera_extrinsic(calibration: &CameraCalibration) -> Option<CameraExtrinsic> {
    let p = calibration.p.as_ref()?;
    let k = &calibration.k;

    // Compute K inverse (simplified - K is usually upper triangular for cameras)
    // K = [fx  0  cx]     K_inv = [1/fx    0     -cx/fx   ]
    //     [ 0 fy  cy]            [  0   1/fy  -cy/fy   ]
    //     [ 0  0   1]            [  0     0       1     ]
    let fx = k[0];
    let fy = k[4];
    let cx = k[2];
    let cy = k[5];

    // P is 3x4: [P0 P1 P2 P3] where each Pi is a column
    // After K_inv * P, we get [R|t]
    let r0 = [p[0] / fx, p[1] / fx, p[2] / fx];
    let r1 = [p[4] / fy, p[5] / fy, p[6] / fy];
    let r2 = [
        p[8] - p[0] * cx / fx - p[4] * cy / fy,
        p[9] - p[1] * cx / fx - p[5] * cy / fy,
        p[10] - p[2] * cx / fx - p[6] * cy / fy,
    ];
    let t = [
        p[3] / fx,
        p[7] / fy,
        p[11] - p[3] * cx / fx - p[7] * cy / fy,
    ];

    let rotation_matrix = [r0, r1, r2];
    Some(CameraExtrinsic::new(rotation_matrix, t))
}

/// Convert camera calibration to both LeRobot intrinsic and extrinsic.
///
/// This is a convenience function that extracts both calibration
/// parameters from a single camera calibration data.
///
/// # Arguments
///
/// * `calibration` - Camera calibration data
///
/// # Returns
///
/// Tuple of (CameraIntrinsic, Option<CameraExtrinsic>)
pub fn convert_camera_calibration(
    calibration: &CameraCalibration,
) -> (CameraIntrinsic, Option<CameraExtrinsic>) {
    let intrinsic = convert_camera_intrinsic(calibration);
    let extrinsic = convert_camera_extrinsic(calibration);
    (intrinsic, extrinsic)
}

/// Apply camera calibration to a writer.
///
/// This helper function applies both intrinsic and extrinsic
/// calibration parameters from a map of camera calibrations
/// to a LeRobot writer.
///
/// # Arguments
///
/// * `writer` - Mutable reference to LeRobot writer
/// * `camera_calibration` - Map of camera name to calibration data
pub fn apply_camera_calibration<W>(
    writer: &mut W,
    camera_calibration: &HashMap<String, CameraCalibration>,
) where
    W: CalibrationWriter,
{
    for (camera_name, info) in camera_calibration {
        let (intrinsic, extrinsic) = convert_camera_calibration(info);
        writer.set_camera_intrinsics(camera_name.clone(), intrinsic);
        if let Some(ext) = extrinsic {
            writer.set_camera_extrinsics(camera_name.clone(), ext);
        }
    }
}

/// Trait for writers that accept camera calibration.
pub trait CalibrationWriter {
    /// Set camera intrinsics for the given camera.
    fn set_camera_intrinsics(&mut self, camera_name: String, intrinsic: CameraIntrinsic);

    /// Set camera extrinsics for the given camera.
    fn set_camera_extrinsics(&mut self, camera_name: String, extrinsic: CameraExtrinsic);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_episode_tracker_new() {
        let tracker = EpisodeTracker::new();
        assert_eq!(tracker.current_index(), 0);
        assert_eq!(tracker.episodes_completed(), 0);
        assert!(!tracker.has_frames());
    }

    #[test]
    fn test_episode_tracker_first_frame() {
        let mut tracker = EpisodeTracker::new();
        let action = tracker.track_episode_index(0);
        assert_eq!(action, EpisodeAction::Continue);
        assert_eq!(tracker.current_index(), 0);
        assert!(tracker.has_frames());
    }

    #[test]
    fn test_episode_tracker_same_episode() {
        let mut tracker = EpisodeTracker::new();
        tracker.track_episode_index(0);
        let action = tracker.track_episode_index(0);
        assert_eq!(action, EpisodeAction::Continue);
        assert_eq!(tracker.current_index(), 0);
    }

    #[test]
    fn test_episode_tracker_new_episode() {
        let mut tracker = EpisodeTracker::new();
        tracker.track_episode_index(0);
        let action = tracker.track_episode_index(1);
        assert!(matches!(
            action,
            EpisodeAction::FinishAndStart {
                old_index: 0,
                new_index: 1
            }
        ));
        assert_eq!(tracker.current_index(), 1);
        assert_eq!(tracker.episodes_completed(), 1);
    }

    #[test]
    fn test_episode_tracker_advance() {
        let mut tracker = EpisodeTracker::new();
        tracker.track_episode_index(0);
        let action = tracker.advance_episode();
        assert!(matches!(action, EpisodeAction::FinishAndStart { .. }));
        assert_eq!(tracker.current_index(), 1);
        assert_eq!(tracker.episodes_completed(), 1);
    }

    #[test]
    fn test_convert_camera_intrinsic() {
        let calibration = CameraCalibration {
            camera_name: "test_camera".to_string(),
            width: 640,
            height: 480,
            k: [500.0, 0.0, 320.0, 0.0, 500.0, 240.0, 0.0, 0.0, 1.0],
            d: vec![0.1, 0.2, 0.0, 0.0, 0.3],
            r: None,
            p: None,
            distortion_model: "plumb_bob".to_string(),
        };

        let intrinsic = convert_camera_intrinsic(&calibration);
        assert_eq!(intrinsic.fx, 500.0);
        assert_eq!(intrinsic.fy, 500.0);
        assert_eq!(intrinsic.ppx, 320.0);
        assert_eq!(intrinsic.ppy, 240.0);
        assert_eq!(intrinsic.k1, 0.1);
        assert_eq!(intrinsic.k2, 0.2);
        assert_eq!(intrinsic.k3, 0.3);
    }

    #[test]
    fn test_convert_camera_extrinsic() {
        // P = K * [R|t] where K is identity for simplicity
        let calibration = CameraCalibration {
            camera_name: "test_camera".to_string(),
            width: 640,
            height: 480,
            k: [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0],
            d: vec![],
            r: None,
            p: Some([
                1.0, 0.0, 0.0, 1.0, // R0 + t0
                0.0, 1.0, 0.0, 2.0, // R1 + t1
                0.0, 0.0, 1.0, 3.0, // R2 + t2
            ]),
            distortion_model: "plumb_bob".to_string(),
        };

        let extrinsic = convert_camera_extrinsic(&calibration);
        assert!(extrinsic.is_some());
    }

    #[test]
    fn test_convert_camera_calibration() {
        let calibration = CameraCalibration {
            camera_name: "test_camera".to_string(),
            width: 640,
            height: 480,
            k: [500.0, 0.0, 320.0, 0.0, 500.0, 240.0, 0.0, 0.0, 1.0],
            d: vec![0.1, 0.2, 0.0, 0.0, 0.3],
            r: None,
            p: Some([
                500.0, 0.0, 320.0, 100.0, 0.0, 500.0, 240.0, 200.0, 0.0, 0.0, 1.0, 300.0,
            ]),
            distortion_model: "plumb_bob".to_string(),
        };

        let (intrinsic, extrinsic) = convert_camera_calibration(&calibration);
        assert_eq!(intrinsic.fx, 500.0);
        assert!(extrinsic.is_some());
    }
}
