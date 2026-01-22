// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Type normalization preset for brand-agnostic message types.
//!
//! Removes brand-specific package names and maps to generic equivalents.

use super::{TopicAwareTypeRenameTransform, TypeRenameTransform};
use std::collections::HashMap;

/// Brand-agnostic type normalization.
///
/// Combines topic-aware type mappings (for generic containers like nmx.msg.LowdimData)
/// with global type renames (for brand-specific packages).
pub struct TypeNormalization {
    /// Topic-aware type mappings for generic container types
    pub topic_aware: TopicAwareTypeRenameTransform,
    /// Global type mappings for brand-specific packages
    pub type_rename: TypeRenameTransform,
}

/// Type alias for topic-aware type mappings.
pub type TopicAwareMappings = HashMap<(String, String), String>;

/// Type alias for global type mappings.
pub type GlobalTypeMappings = HashMap<String, String>;

impl TypeNormalization {
    /// Create the full normalization preset with all brand mappings.
    pub fn full() -> Self {
        Self {
            topic_aware: Self::nmx_topic_mappings(),
            type_rename: Self::global_type_mappings(),
        }
    }

    /// Create nmx-specific normalization only.
    pub fn nmx() -> Self {
        Self {
            topic_aware: Self::nmx_topic_mappings(),
            type_rename: TypeRenameTransform::new(),
        }
    }

    /// Create genie_msgs-specific normalization only.
    pub fn genie() -> Self {
        Self {
            topic_aware: TopicAwareTypeRenameTransform::new(),
            type_rename: Self::genie_type_mappings(),
        }
    }

    /// Topic-aware mappings for nmx.msg.LowdimData container.
    ///
    /// The same schema type maps to different semantic types based on topic.
    fn nmx_topic_mappings() -> TopicAwareTypeRenameTransform {
        let mut mapping = TopicAwareTypeRenameTransform::new();

        // nmx.msg.LowdimData → roboflow.msg.* based on topic
        mapping.add_mapping(
            "/lowdim/joint",
            "nmx.msg.LowdimData",
            "roboflow.msg.JointStates",
        );
        mapping.add_mapping("/lowdim/tcp", "nmx.msg.LowdimData", "roboflow.msg.eef_pose");
        mapping.add_mapping(
            "/lowdim/ee_state",
            "nmx.msg.LowdimData",
            "roboflow.msg.ee_state",
        );
        mapping.add_mapping(
            "/lowdim/airexo_joint",
            "nmx.msg.LowdimData",
            "roboflow.msg.airexo_joint",
        );
        mapping.add_mapping(
            "/camera/intrinsic/camid_1",
            "nmx.msg.LowdimData",
            "roboflow.msg.camid_1_intrinsic",
        );

        // nmx.msg.Image → sensor_msgs.msg.CompressedImage
        mapping.add_mapping(
            "/camera/color/compressed/jpg/camid_1",
            "nmx.msg.Image",
            "sensor_msgs.msg.CompressedImage",
        );
        mapping.add_mapping(
            "/camera/depth/compressed/png/camid_1",
            "nmx.msg.Image",
            "sensor_msgs.msg.CompressedImage",
        );

        mapping
    }

    /// Global type mappings for brand-specific packages.
    fn global_type_mappings() -> TypeRenameTransform {
        let mut mapping = TypeRenameTransform::new();

        // genie_msgs/* → roboflow.msg/* or ROS standard types
        mapping.add_mapping("genie_msgs/msg/ArmState", "roboflow.msg.ArmState");
        mapping.add_mapping(
            "genie_msgs/msg/BatteryStatus",
            "sensor_msgs.msg.BatteryState",
        );
        mapping.add_mapping("genie_msgs/msg/EndState", "roboflow.msg.EndEffectorState");
        mapping.add_mapping("genie_msgs/msg/FaultStatus", "roboflow.msg.FaultStatus");
        mapping.add_mapping("genie_msgs/msg/HeadState", "roboflow.msg.HeadState");
        mapping.add_mapping("genie_msgs/msg/PeriStatus", "roboflow.msg.PeripheralStatus");
        mapping.add_mapping("genie_msgs/msg/Position", "geometry_msgs.msg.Pose");
        mapping.add_mapping("genie_msgs/msg/PublicFaultMsg", "roboflow.msg.FaultStatus");
        mapping.add_mapping("genie_msgs/msg/SceneStatus", "roboflow.msg.SceneStatus");
        mapping.add_mapping("genie_msgs/msg/WaistState", "roboflow.msg.TorsoState");
        mapping.add_mapping(
            "genie_msgs/msg/WholeBodyStatus",
            "roboflow.msg.WholeBodyStatus",
        );

        // kuovo_msgs → roboflow.msg
        mapping.add_mapping("kuavo_msgs/sensorsData", "roboflow.msg.SensorsData");

        // upperlimb → roboflow.msg
        mapping.add_mapping("upperlimb/Pose", "roboflow.msg.EndEffectorPose");

        // nmx specific types (non-LowdimData)
        mapping.add_mapping("nmx.msg.CameraExtrinsic", "geometry_msgs.msg.Transform");
        mapping.add_mapping("nmx.msg.JointStates", "sensor_msgs.msg.JointState");

        mapping
    }

    /// Genie-specific type mappings only.
    fn genie_type_mappings() -> TypeRenameTransform {
        let mut mapping = TypeRenameTransform::new();

        mapping.add_mapping("genie_msgs/msg/ArmState", "roboflow.msg.ArmState");
        mapping.add_mapping(
            "genie_msgs/msg/BatteryStatus",
            "sensor_msgs.msg.BatteryState",
        );
        mapping.add_mapping("genie_msgs/msg/EndState", "roboflow.msg.EndEffectorState");
        mapping.add_mapping("genie_msgs/msg/FaultStatus", "roboflow.msg.FaultStatus");
        mapping.add_mapping("genie_msgs/msg/HeadState", "roboflow.msg.HeadState");
        mapping.add_mapping("genie_msgs/msg/PeriStatus", "roboflow.msg.PeripheralStatus");
        mapping.add_mapping("genie_msgs/msg/Position", "geometry_msgs.msg.Pose");
        mapping.add_mapping("genie_msgs/msg/PublicFaultMsg", "roboflow.msg.FaultStatus");
        mapping.add_mapping("genie_msgs/msg/SceneStatus", "roboflow.msg.SceneStatus");
        mapping.add_mapping("genie_msgs/msg/WaistState", "roboflow.msg.TorsoState");
        mapping.add_mapping(
            "genie_msgs/msg/WholeBodyStatus",
            "roboflow.msg.WholeBodyStatus",
        );

        mapping
    }

    /// Get all mappings as a HashMap for serialization.
    pub fn as_maps(&self) -> (TopicAwareMappings, GlobalTypeMappings) {
        (
            self.topic_aware.mappings().clone(),
            self.type_rename.mappings().clone(),
        )
    }
}

impl Default for TypeNormalization {
    fn default() -> Self {
        Self::full()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nmx_joint_mapping() {
        let norm = TypeNormalization::nmx();
        assert_eq!(
            norm.topic_aware
                .apply_for_topic("/lowdim/joint", "nmx.msg.LowdimData"),
            "roboflow.msg.JointStates"
        );
    }

    #[test]
    fn test_nmx_tcp_mapping() {
        let norm = TypeNormalization::nmx();
        assert_eq!(
            norm.topic_aware
                .apply_for_topic("/lowdim/tcp", "nmx.msg.LowdimData"),
            "roboflow.msg.eef_pose"
        );
    }

    #[test]
    fn test_genie_arm_state_mapping() {
        let norm = TypeNormalization::genie();
        assert_eq!(
            norm.type_rename.apply_type("genie_msgs/msg/ArmState"),
            "roboflow.msg.ArmState"
        );
    }

    #[test]
    fn test_full_normalization() {
        let norm = TypeNormalization::full();

        // nmx topic-aware
        assert_eq!(
            norm.topic_aware
                .apply_for_topic("/lowdim/joint", "nmx.msg.LowdimData"),
            "roboflow.msg.JointStates"
        );

        // genie global
        assert_eq!(
            norm.type_rename.apply_type("genie_msgs/msg/ArmState"),
            "roboflow.msg.ArmState"
        );
    }
}
