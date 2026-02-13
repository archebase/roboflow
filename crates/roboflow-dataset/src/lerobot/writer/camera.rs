// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Camera intrinsic and extrinsic parameters for LeRobot format.
//!
//! These types represent camera calibration data in the LeRobot v2.1 format.

use serde::{Deserialize, Serialize};

/// Camera intrinsic parameters in LeRobot format.
///
/// Intrinsic parameters describe the internal characteristics of a camera,
/// including focal length, principal point, and lens distortion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CameraIntrinsic {
    /// Focal length x (pixels)
    pub fx: f64,
    /// Focal length y (pixels)
    pub fy: f64,
    /// Principal point x (pixels)
    pub ppx: f64,
    /// Principal point y (pixels)
    pub ppy: f64,
    /// Distortion model name
    pub distortion_model: String,
    /// k1 distortion coefficient
    pub k1: f64,
    /// k2 distortion coefficient
    pub k2: f64,
    /// k3 distortion coefficient
    pub k3: f64,
    /// p1 distortion coefficient
    pub p1: f64,
    /// p2 distortion coefficient
    pub p2: f64,
}

/// Camera extrinsic parameters in LeRobot format.
///
/// Extrinsic parameters describe the camera's position and orientation
/// relative to a reference coordinate system (typically robot base).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CameraExtrinsic {
    /// Extrinsic data wrapper (matches LeRobot format)
    pub extrinsic: ExtrinsicData,
}

/// The actual extrinsic data.
///
/// Contains rotation matrix and translation vector representing
/// the camera-to-world transformation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtrinsicData {
    /// 3x3 rotation matrix (row-major)
    pub rotation_matrix: Vec<Vec<f64>>,
    /// Translation vector [x, y, z]
    pub translation_vector: Vec<f64>,
}

impl CameraExtrinsic {
    /// Create extrinsic from rotation matrix and translation.
    ///
    /// # Arguments
    ///
    /// * `rotation_matrix` - 3x3 rotation matrix as nested arrays
    /// * `translation` - Translation vector [x, y, z]
    pub fn new(rotation_matrix: [[f64; 3]; 3], translation: [f64; 3]) -> Self {
        Self {
            extrinsic: ExtrinsicData {
                rotation_matrix: vec![
                    rotation_matrix[0].to_vec(),
                    rotation_matrix[1].to_vec(),
                    rotation_matrix[2].to_vec(),
                ],
                translation_vector: translation.to_vec(),
            },
        }
    }

    /// Create extrinsic from flat arrays.
    ///
    /// # Arguments
    ///
    /// * `rotation_matrix` - 3x3 rotation matrix as flat array (row-major)
    /// * `translation` - Translation vector [x, y, z]
    pub fn from_arrays(rotation_matrix: [f64; 9], translation: [f64; 3]) -> Self {
        Self {
            extrinsic: ExtrinsicData {
                rotation_matrix: vec![
                    vec![rotation_matrix[0], rotation_matrix[1], rotation_matrix[2]],
                    vec![rotation_matrix[3], rotation_matrix[4], rotation_matrix[5]],
                    vec![rotation_matrix[6], rotation_matrix[7], rotation_matrix[8]],
                ],
                translation_vector: translation.to_vec(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_camera_intrinsic_creation() {
        let intrinsic = CameraIntrinsic {
            fx: 500.0,
            fy: 500.0,
            ppx: 320.0,
            ppy: 240.0,
            distortion_model: "plumb_bob".to_string(),
            k1: 0.1,
            k2: 0.01,
            k3: 0.001,
            p1: 0.0,
            p2: 0.0,
        };

        assert_eq!(intrinsic.fx, 500.0);
        assert_eq!(intrinsic.fy, 500.0);
        assert_eq!(intrinsic.ppx, 320.0);
        assert_eq!(intrinsic.ppy, 240.0);
        assert_eq!(intrinsic.distortion_model, "plumb_bob");
    }

    #[test]
    fn test_camera_intrinsic_serialization() {
        let intrinsic = CameraIntrinsic {
            fx: 500.0,
            fy: 500.0,
            ppx: 320.0,
            ppy: 240.0,
            distortion_model: "plumb_bob".to_string(),
            k1: 0.1,
            k2: 0.01,
            k3: 0.001,
            p1: 0.0,
            p2: 0.0,
        };

        let json = serde_json::to_string(&intrinsic).unwrap();
        let deserialized: CameraIntrinsic = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.fx, intrinsic.fx);
        assert_eq!(deserialized.fy, intrinsic.fy);
        assert_eq!(deserialized.distortion_model, intrinsic.distortion_model);
    }

    #[test]
    fn test_camera_extrinsic_new() {
        let rotation = [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]];
        let translation = [0.0, 0.0, 0.0];

        let extrinsic = CameraExtrinsic::new(rotation, translation);

        assert_eq!(extrinsic.extrinsic.rotation_matrix.len(), 3);
        assert_eq!(extrinsic.extrinsic.rotation_matrix[0], vec![1.0, 0.0, 0.0]);
        assert_eq!(extrinsic.extrinsic.translation_vector, vec![0.0, 0.0, 0.0]);
    }

    #[test]
    fn test_camera_extrinsic_from_arrays() {
        let rotation_flat = [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0];
        let translation = [1.0, 2.0, 3.0];

        let extrinsic = CameraExtrinsic::from_arrays(rotation_flat, translation);

        assert_eq!(extrinsic.extrinsic.rotation_matrix.len(), 3);
        assert_eq!(extrinsic.extrinsic.rotation_matrix[0], vec![1.0, 0.0, 0.0]);
        assert_eq!(extrinsic.extrinsic.rotation_matrix[1], vec![0.0, 1.0, 0.0]);
        assert_eq!(extrinsic.extrinsic.rotation_matrix[2], vec![0.0, 0.0, 1.0]);
        assert_eq!(extrinsic.extrinsic.translation_vector, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_camera_extrinsic_serialization() {
        let extrinsic = CameraExtrinsic::new([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]], [0.0, 0.0, 0.0]);

        let json = serde_json::to_string(&extrinsic).unwrap();
        let deserialized: CameraExtrinsic = serde_json::from_str(&json).unwrap();

        assert_eq!(
            deserialized.extrinsic.rotation_matrix,
            extrinsic.extrinsic.rotation_matrix
        );
        assert_eq!(
            deserialized.extrinsic.translation_vector,
            extrinsic.extrinsic.translation_vector
        );
    }
}
