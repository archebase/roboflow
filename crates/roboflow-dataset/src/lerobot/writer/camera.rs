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
