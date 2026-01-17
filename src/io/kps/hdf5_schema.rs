//! Kps HDF5 schema definitions.
//!
//! Defines the complete HDF5 structure as per the Kps data format specification v1.2.
//!
//! Structure:
//! ```text
//! / (root)
//! ├── timestamps                          (N,) int64 - aligned timestamps
//! ├── hand_right_color_mp4_timestamps     (N,) int64 - per-sensor timestamps
//! ├── hand_left_color_mp4_timestamps      (N,) int64
//! ├── eef_timestamps                      (N,) int64
//! ├── action/
//! │   ├── effector/
//! │   │   ├── position                    (N, P1) float32
//! │   │   └── names                       (P1,) str
//! │   ├── end/
//! │   │   ├── position                    (N, 2, 3) float32
//! │   │   └── orientation                 (N, 2, 4) float32
//! │   ├── head/
//! │   │   ├── position                    (N, P2) float32
//! │   │   ├── velocity                    (N, P2) float32
//! │   │   └── names                       (P2,) str
//! │   ├── joint/
//! │   │   ├── position                    (N, 14) float32
//! │   │   ├── velocity                    (N, 14) float32
//! │   │   └── names                       (14,) str
//! │   ├── leg/
//! │   │   ├── position                    (N, 12) float32
//! │   │   ├── velocity                    (N, 12) float32
//! │   │   └── names                       (12,) str
//! │   ├── robot/
//! │   │   ├── velocity                    (N, 2) float32
//! │   │   └── orientation                 (N, 4) float32
//! │   └── waist/
//! │       ├── position                    (N, P3) float32
//! │       ├── velocity                    (N, P3) float32
//! │       └── names                       (P3,) str
//! └── state/
//!     ├── effector/
//! │   ├── position                        (N, P1) float32
//! │   ├── force                           (N, P1) float32
//! │   └── names                           (P1,) str
//!     ├── end/
//! │   ├── angular                         (N, 2, 3) float32
//! │   ├── orientation                     (N, 2, 4) float32
//! │   ├── position                        (N, 2, 3) float32
//! │   ├── velocity                        (N, 2, 3) float32
//! │   └── wrench                          (N, 2, 6) float32
//!     ├── head/
//! │   ├── effort                          (N, P2) float32
//! │   ├── position                        (N, P2) float32
//! │   ├── velocity                        (N, P2) float32
//! │   └── names                           (P2,) str
//!     ├── joint/
//! │   ├── current_value                   (N, 14) float32
//! │   ├── effort                          (N, 14) float32
//! │   ├── position                        (N, 14) float32
//! │   ├── velocity                        (N, 14) float32
//! │   └── names                           (14,) str
//!     ├── leg/
//! │   ├── position                        (N, 12) float32
//! │   ├── velocity                        (N, 12) float32
//! │   └── names                           (12,) str
//!     ├── robot/
//! │   ├── orientation                     (N, 4) float32
//! │   ├── orientation_drift               (N, 4) float32
//! │   ├── position                        (N, 3) float32
//! │   └── position_drift                  (N, 3) float32
//!     └── waist/
//!         ├── effort                      (N, P3) float32
//!         ├── position                    (N, P3) float32
//!         ├── velocity                    (N, P3) float32
//!         └── names                       (P3,) str
//! ```

use std::collections::HashMap;

/// Joint group definitions with default names and dimensions.
#[derive(Debug, Clone, Default)]
pub struct JointGroupConfig {
    /// URDF joint names for this group
    pub names: Vec<String>,
    /// Dimension (number of joints)
    pub dimension: usize,
}

impl JointGroupConfig {
    /// Create a new joint group config.
    pub fn new(names: Vec<String>) -> Self {
        let dimension = names.len();
        Self { names, dimension }
    }

    /// Create an empty config with specified dimension.
    pub fn with_dimension(dimension: usize) -> Self {
        Self {
            names: (0..dimension).map(|i| format!("joint_{}", i)).collect(),
            dimension,
        }
    }
}

/// Default joint names for dual arm configuration.
pub fn default_arm_joint_names() -> Vec<String> {
    vec![
        "l_arm_pitch".to_string(),
        "l_arm_roll".to_string(),
        "l_arm_yaw".to_string(),
        "l_forearm".to_string(),
        "l_hand_yaw".to_string(),
        "l_hand_pitch".to_string(),
        "l_hand_roll".to_string(),
        "r_arm_pitch".to_string(),
        "r_arm_roll".to_string(),
        "r_arm_yaw".to_string(),
        "r_forearm".to_string(),
        "r_hand_yaw".to_string(),
        "r_hand_pitch".to_string(),
        "r_hand_roll".to_string(),
    ]
}

/// Default joint names for dual leg configuration.
pub fn default_leg_joint_names() -> Vec<String> {
    vec![
        "l_leg_roll".to_string(),
        "l_leg_yaw".to_string(),
        "l_leg_pitch".to_string(),
        "l_knee".to_string(),
        "l_foot_pitch".to_string(),
        "l_foot_roll".to_string(),
        "r_leg_roll".to_string(),
        "r_leg_yaw".to_string(),
        "r_leg_pitch".to_string(),
        "r_knee".to_string(),
        "r_foot_pitch".to_string(),
        "r_foot_roll".to_string(),
    ]
}

/// Default joint names for head configuration.
pub fn default_head_joint_names() -> Vec<String> {
    vec!["joint_head_yaw".to_string(), "joint_head_pitch".to_string()]
}

/// Default joint names for waist configuration.
pub fn default_waist_joint_names() -> Vec<String> {
    vec![
        "joint_waist_pitch".to_string(),
        "joint_waist_roll".to_string(),
        "joint_waist_yaw".to_string(),
    ]
}

/// Default names for dual end effector (gripper/dexhand).
pub fn default_effector_names() -> Vec<String> {
    vec!["l_gripper".to_string(), "r_gripper".to_string()]
}

/// Default names for dual end effector (6-DOF dexhand).
pub fn default_dexhand_names() -> Vec<String> {
    vec![
        "l_thumb_aux".to_string(),
        "l_thumb".to_string(),
        "l_index".to_string(),
        "l_middle".to_string(),
        "l_ring".to_string(),
        "l_pinky".to_string(),
        "r_thumb_aux".to_string(),
        "r_thumb".to_string(),
        "r_index".to_string(),
        "r_middle".to_string(),
        "r_ring".to_string(),
        "r_pinky".to_string(),
    ]
}

/// HDF5 dataset specification.
#[derive(Debug, Clone)]
pub struct DatasetSpec {
    /// Full path within HDF5 file (e.g., "action/joint/position")
    pub path: String,
    /// Shape as list of dimensions (e.g., [N, 14] for N frames, 14 DOF)
    pub shape: Vec<usize>,
    /// Data type (e.g., "float32", "int64", "string")
    pub dtype: DataType,
    /// Description
    pub description: String,
}

/// HDF5 data type.
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    String,
}

impl DataType {
    /// Get HDF5 datatype string.
    pub fn as_str(&self) -> &'static str {
        match self {
            DataType::Float32 => "float32",
            DataType::Float64 => "float64",
            DataType::Int8 => "int8",
            DataType::Int16 => "int16",
            DataType::Int32 => "int32",
            DataType::Int64 => "int64",
            DataType::UInt8 => "uint8",
            DataType::UInt16 => "uint16",
            DataType::UInt32 => "uint32",
            DataType::UInt64 => "uint64",
            DataType::String => "string",
        }
    }
}

/// Complete HDF5 schema for Kps format.
#[derive(Debug, Clone)]
pub struct KpsHdf5Schema {
    /// Joint group configurations
    pub joint_groups: HashMap<String, JointGroupConfig>,
    /// All dataset specifications
    pub datasets: Vec<DatasetSpec>,
}

impl Default for KpsHdf5Schema {
    fn default() -> Self {
        Self::new()
    }
}

impl KpsHdf5Schema {
    /// Create a new schema with default joint configurations.
    pub fn new() -> Self {
        let mut joint_groups = HashMap::new();

        joint_groups.insert("joint".to_string(), JointGroupConfig::new(default_arm_joint_names()));
        joint_groups.insert("leg".to_string(), JointGroupConfig::new(default_leg_joint_names()));
        joint_groups.insert("head".to_string(), JointGroupConfig::new(default_head_joint_names()));
        joint_groups.insert("waist".to_string(), JointGroupConfig::new(default_waist_joint_names()));
        joint_groups.insert("effector".to_string(), JointGroupConfig::new(default_effector_names()));

        let mut schema = Self {
            joint_groups,
            datasets: Vec::new(),
        };

        schema.build_action_datasets();
        schema.build_state_datasets();
        schema.build_root_datasets();

        schema
    }

    /// Create schema with custom URDF joint names.
    pub fn with_urdf_joint_names(mut self, group: &str, names: Vec<String>) -> Self {
        let dimension = names.len();
        self.joint_groups.insert(group.to_string(), JointGroupConfig { names, dimension });
        self
    }

    /// Build action group dataset specifications.
    fn build_action_datasets(&mut self) {
        let action_groups = ["effector", "end", "head", "joint", "leg", "robot", "waist"];

        for group in action_groups {
            match group {
                "effector" => {
                    let dim = self.joint_groups.get("effector").map_or(2, |g| g.dimension);
                    self.datasets.push(DatasetSpec {
                        path: "action/effector/position".to_string(),
                        shape: vec![0, dim], // 0 means variable first dimension
                        dtype: DataType::Float32,
                        description: "End effector joint angles (rad)".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "action/effector/names".to_string(),
                        shape: vec![dim],
                        dtype: DataType::String,
                        description: "End effector joint names".to_string(),
                    });
                }
                "end" => {
                    self.datasets.push(DatasetSpec {
                        path: "action/end/position".to_string(),
                        shape: vec![0, 2, 3],
                        dtype: DataType::Float32,
                        description: "Left/right end effector positions [x,y,z] (m)".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "action/end/orientation".to_string(),
                        shape: vec![0, 2, 4],
                        dtype: DataType::Float32,
                        description: "Left/right end effector orientations [x,y,z,w]".to_string(),
                    });
                }
                "head" => {
                    let dim = self.joint_groups.get("head").map_or(2, |g| g.dimension);
                    self.datasets.push(DatasetSpec {
                        path: "action/head/position".to_string(),
                        shape: vec![0, dim],
                        dtype: DataType::Float32,
                        description: "Head joint positions (rad)".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "action/head/velocity".to_string(),
                        shape: vec![0, dim],
                        dtype: DataType::Float32,
                        description: "Head joint velocities (rad/s)".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "action/head/names".to_string(),
                        shape: vec![dim],
                        dtype: DataType::String,
                        description: "Head joint names".to_string(),
                    });
                }
                "joint" => {
                    let dim = self.joint_groups.get("joint").map_or(14, |g| g.dimension);
                    self.datasets.push(DatasetSpec {
                        path: "action/joint/position".to_string(),
                        shape: vec![0, dim],
                        dtype: DataType::Float32,
                        description: "Dual arm joint positions, left[:, :7], right[:, 7:] (rad)".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "action/joint/velocity".to_string(),
                        shape: vec![0, dim],
                        dtype: DataType::Float32,
                        description: "Dual arm joint velocities (rad/s)".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "action/joint/names".to_string(),
                        shape: vec![dim],
                        dtype: DataType::String,
                        description: "Dual arm joint names matching URDF".to_string(),
                    });
                }
                "leg" => {
                    let dim = self.joint_groups.get("leg").map_or(12, |g| g.dimension);
                    self.datasets.push(DatasetSpec {
                        path: "action/leg/position".to_string(),
                        shape: vec![0, dim],
                        dtype: DataType::Float32,
                        description: "Dual leg joint positions, left[:, :6], right[:, 6:] (rad)".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "action/leg/velocity".to_string(),
                        shape: vec![0, dim],
                        dtype: DataType::Float32,
                        description: "Dual leg joint velocities (rad/s)".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "action/leg/names".to_string(),
                        shape: vec![dim],
                        dtype: DataType::String,
                        description: "Dual leg joint names matching URDF".to_string(),
                    });
                }
                "robot" => {
                    self.datasets.push(DatasetSpec {
                        path: "action/robot/velocity".to_string(),
                        shape: vec![0, 2],
                        dtype: DataType::Float32,
                        description: "Base velocity [linear, angular] in odom frame".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "action/robot/orientation".to_string(),
                        shape: vec![0, 4],
                        dtype: DataType::Float32,
                        description: "Base orientation [x,y,z,w] quaternion in odom frame".to_string(),
                    });
                }
                "waist" => {
                    let dim = self.joint_groups.get("waist").map_or(3, |g| g.dimension);
                    self.datasets.push(DatasetSpec {
                        path: "action/waist/position".to_string(),
                        shape: vec![0, dim],
                        dtype: DataType::Float32,
                        description: "Waist joint positions (rad or m for lift)".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "action/waist/velocity".to_string(),
                        shape: vec![0, dim],
                        dtype: DataType::Float32,
                        description: "Waist joint velocities (rad/s or m/s for lift)".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "action/waist/names".to_string(),
                        shape: vec![dim],
                        dtype: DataType::String,
                        description: "Waist joint names matching URDF".to_string(),
                    });
                }
                _ => {}
            }
        }
    }

    /// Build state group dataset specifications.
    fn build_state_datasets(&mut self) {
        let state_groups = ["effector", "end", "head", "joint", "leg", "robot", "waist"];

        for group in state_groups {
            match group {
                "effector" => {
                    let dim = self.joint_groups.get("effector").map_or(2, |g| g.dimension);
                    self.datasets.push(DatasetSpec {
                        path: "state/effector/position".to_string(),
                        shape: vec![0, dim],
                        dtype: DataType::Float32,
                        description: "End effector actual positions (rad or mm)".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "state/effector/force".to_string(),
                        shape: vec![0, dim],
                        dtype: DataType::Float32,
                        description: "End effector force/torque (Nm)".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "state/effector/names".to_string(),
                        shape: vec![dim],
                        dtype: DataType::String,
                        description: "End effector joint names".to_string(),
                    });
                }
                "end" => {
                    self.datasets.push(DatasetSpec {
                        path: "state/end/angular".to_string(),
                        shape: vec![0, 2, 3],
                        dtype: DataType::Float32,
                        description: "Left/right end effector angular velocities [wx,wy,wz] (rad/s)".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "state/end/orientation".to_string(),
                        shape: vec![0, 2, 4],
                        dtype: DataType::Float32,
                        description: "Left/right end effector orientations [x,y,z,w]".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "state/end/position".to_string(),
                        shape: vec![0, 2, 3],
                        dtype: DataType::Float32,
                        description: "Left/right end effector positions [x,y,z] (m)".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "state/end/velocity".to_string(),
                        shape: vec![0, 2, 3],
                        dtype: DataType::Float32,
                        description: "Left/right end effector spatial velocities [vx,vy,vz] (m/s)".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "state/end/wrench".to_string(),
                        shape: vec![0, 2, 6],
                        dtype: DataType::Float32,
                        description: "Left/right end effector wrench [fx,fy,fz,mx,my,mz]".to_string(),
                    });
                }
                "head" => {
                    let dim = self.joint_groups.get("head").map_or(2, |g| g.dimension);
                    self.datasets.push(DatasetSpec {
                        path: "state/head/effort".to_string(),
                        shape: vec![0, dim],
                        dtype: DataType::Float32,
                        description: "Head joint effort (torque)".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "state/head/position".to_string(),
                        shape: vec![0, dim],
                        dtype: DataType::Float32,
                        description: "Head joint actual positions (rad)".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "state/head/velocity".to_string(),
                        shape: vec![0, dim],
                        dtype: DataType::Float32,
                        description: "Head joint actual velocities (rad/s)".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "state/head/names".to_string(),
                        shape: vec![dim],
                        dtype: DataType::String,
                        description: "Head joint names".to_string(),
                    });
                }
                "joint" => {
                    let dim = self.joint_groups.get("joint").map_or(14, |g| g.dimension);
                    self.datasets.push(DatasetSpec {
                        path: "state/joint/current_value".to_string(),
                        shape: vec![0, dim],
                        dtype: DataType::Float32,
                        description: "Dual arm joint current values".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "state/joint/effort".to_string(),
                        shape: vec![0, dim],
                        dtype: DataType::Float32,
                        description: "Dual arm joint actual torque".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "state/joint/position".to_string(),
                        shape: vec![0, dim],
                        dtype: DataType::Float32,
                        description: "Dual arm joint actual positions (rad)".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "state/joint/velocity".to_string(),
                        shape: vec![0, dim],
                        dtype: DataType::Float32,
                        description: "Dual arm joint actual velocities (rad/s)".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "state/joint/names".to_string(),
                        shape: vec![dim],
                        dtype: DataType::String,
                        description: "Dual arm joint names".to_string(),
                    });
                }
                "leg" => {
                    let dim = self.joint_groups.get("leg").map_or(12, |g| g.dimension);
                    self.datasets.push(DatasetSpec {
                        path: "state/leg/position".to_string(),
                        shape: vec![0, dim],
                        dtype: DataType::Float32,
                        description: "Dual leg joint actual positions (rad)".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "state/leg/velocity".to_string(),
                        shape: vec![0, dim],
                        dtype: DataType::Float32,
                        description: "Dual leg joint actual velocities (rad/s)".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "state/leg/names".to_string(),
                        shape: vec![dim],
                        dtype: DataType::String,
                        description: "Dual leg joint names".to_string(),
                    });
                }
                "robot" => {
                    self.datasets.push(DatasetSpec {
                        path: "state/robot/orientation".to_string(),
                        shape: vec![0, 4],
                        dtype: DataType::Float32,
                        description: "Base orientation [x,y,z,w] in odom frame".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "state/robot/orientation_drift".to_string(),
                        shape: vec![0, 4],
                        dtype: DataType::Float32,
                        description: "Odom to map drift quaternion".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "state/robot/position".to_string(),
                        shape: vec![0, 3],
                        dtype: DataType::Float32,
                        description: "Base position {x,y,z} in odom frame".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "state/robot/position_drift".to_string(),
                        shape: vec![0, 3],
                        dtype: DataType::Float32,
                        description: "Odom to map drift position".to_string(),
                    });
                }
                "waist" => {
                    let dim = self.joint_groups.get("waist").map_or(3, |g| g.dimension);
                    self.datasets.push(DatasetSpec {
                        path: "state/waist/effort".to_string(),
                        shape: vec![0, dim],
                        dtype: DataType::Float32,
                        description: "Waist joint actual torque".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "state/waist/position".to_string(),
                        shape: vec![0, dim],
                        dtype: DataType::Float32,
                        description: "Waist joint actual positions (rad or m)".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "state/waist/velocity".to_string(),
                        shape: vec![0, dim],
                        dtype: DataType::Float32,
                        description: "Waist joint actual velocities (rad/s or m/s)".to_string(),
                    });
                    self.datasets.push(DatasetSpec {
                        path: "state/waist/names".to_string(),
                        shape: vec![dim],
                        dtype: DataType::String,
                        description: "Waist joint names".to_string(),
                    });
                }
                _ => {}
            }
        }
    }

    /// Build root-level dataset specifications (timestamps).
    fn build_root_datasets(&mut self) {
        // Main aligned timestamps
        self.datasets.push(DatasetSpec {
            path: "timestamps".to_string(),
            shape: vec![0],
            dtype: DataType::Int64,
            description: "Aligned unified timestamps (nanoseconds, Unix time)".to_string(),
        });

        // Per-sensor timestamps (will be added dynamically based on available sensors)
        let sensor_timestamps = [
            "hand_right_color_mp4_timestamps",
            "hand_left_color_mp4_timestamps",
            "eef_timestamps",
        ];

        for ts_name in sensor_timestamps {
            self.datasets.push(DatasetSpec {
                path: ts_name.to_string(),
                shape: vec![0],
                dtype: DataType::Int64,
                description: format!("Original timestamps for {}", ts_name),
            });
        }
    }

    /// Get joint names for a group.
    pub fn get_joint_names(&self, group: &str) -> Option<&[String]> {
        self.joint_groups.get(group).map(|g| g.names.as_slice())
    }

    /// Get joint dimension for a group.
    pub fn get_joint_dimension(&self, group: &str) -> Option<usize> {
        self.joint_groups.get(group).map(|g| g.dimension)
    }

    /// Get all dataset specifications.
    pub fn datasets(&self) -> &[DatasetSpec] {
        &self.datasets
    }

    /// Add a custom sensor timestamp dataset.
    pub fn add_sensor_timestamp(&mut self, sensor_name: &str) {
        let path = format!("{}_timestamps", sensor_name);
        self.datasets.push(DatasetSpec {
            path,
            shape: vec![0],
            dtype: DataType::Int64,
            description: format!("Original timestamps for {}", sensor_name),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_schema() {
        let schema = KpsHdf5Schema::new();

        // Check joint groups
        assert_eq!(schema.get_joint_dimension("joint"), Some(14));
        assert_eq!(schema.get_joint_dimension("leg"), Some(12));
        assert_eq!(schema.get_joint_dimension("head"), Some(2));
        assert_eq!(schema.get_joint_dimension("waist"), Some(3));

        // Check datasets exist
        let paths: Vec<_> = schema.datasets().iter().map(|d| d.path.clone()).collect();
        assert!(paths.contains(&"action/joint/position".to_string()));
        assert!(paths.contains(&"action/joint/names".to_string()));
        assert!(paths.contains(&"state/joint/position".to_string()));
        assert!(paths.contains(&"timestamps".to_string()));
    }

    #[test]
    fn test_custom_joint_names() {
        let custom_names = vec![
            "custom_joint_0".to_string(),
            "custom_joint_1".to_string(),
        ];
        let schema = KpsHdf5Schema::new()
            .with_urdf_joint_names("joint", custom_names.clone());

        let names = schema.get_joint_names("joint").unwrap();
        assert_eq!(names, custom_names.as_slice());
        assert_eq!(schema.get_joint_dimension("joint"), Some(2));
    }

    #[test]
    fn test_add_sensor_timestamp() {
        let mut schema = KpsHdf5Schema::new();
        schema.add_sensor_timestamp("custom_camera");

        let paths: Vec<_> = schema.datasets().iter().map(|d| d.path.clone()).collect();
        assert!(paths.contains(&"custom_camera_timestamps".to_string()));
    }
}
