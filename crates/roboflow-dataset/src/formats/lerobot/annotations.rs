// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! JSON annotation parsing for episode segmentation.
//!
//! Parses annotation JSON files that contain skill marks (pick/place/move)
//! for segmenting ROS bag data into episodes.

use std::fs;
use std::path::Path;

use serde::Deserialize;

use roboflow_core::Result;

/// Annotation data from JSON file.
#[derive(Debug, Clone, Deserialize)]
pub struct AnnotationData {
    /// Location name
    pub location: String,

    /// Primary scene
    #[serde(rename = "primaryScene")]
    pub primary_scene: String,

    /// Secondary scene
    #[serde(rename = "secondaryScene")]
    pub secondary_scene: String,

    /// Tertiary scene (task category)
    #[serde(rename = "tertiaryScene")]
    pub tertiary_scene: String,

    /// Initial scene description
    #[serde(rename = "initSceneText", default)]
    pub init_scene_text: String,

    /// Initial scene description in English
    #[serde(rename = "englishInitSceneText", default)]
    pub english_init_scene_text: String,

    /// Task name
    #[serde(rename = "taskName")]
    pub task_name: String,

    /// Task code
    #[serde(rename = "taskCode")]
    pub task_code: String,

    /// Device serial number
    #[serde(rename = "deviceSn")]
    pub device_sn: String,

    /// Task prompt
    #[serde(rename = "taskPrompt", default)]
    pub task_prompt: String,

    /// Skill marks for segmentation
    #[serde(default)]
    pub marks: Vec<SkillMark>,
}

/// A skill mark defining a segment of the recording.
#[derive(Debug, Clone, Deserialize)]
pub struct SkillMark {
    /// Task ID
    #[serde(rename = "taskId")]
    pub task_id: String,

    /// Start timestamp
    #[serde(rename = "markStart")]
    pub mark_start: String,

    /// End timestamp
    #[serde(rename = "markEnd")]
    pub mark_end: String,

    /// Duration in seconds
    pub duration: f64,

    /// Start position (normalized 0-1)
    #[serde(rename = "startPosition")]
    pub start_position: f64,

    /// End position (normalized 0-1)
    #[serde(rename = "endPosition")]
    pub end_position: f64,

    /// Atomic skill type (pick, place, move, etc.)
    #[serde(rename = "skillAtomic")]
    pub skill_atomic: String,

    /// Detailed description
    #[serde(rename = "skillDetail")]
    pub skill_detail: String,

    /// English detailed description
    #[serde(rename = "enSkillDetail")]
    pub en_skill_detail: String,

    /// Mark type
    #[serde(rename = "markType")]
    pub mark_type: String,
}

impl SkillMark {
    /// Get the task description for this mark.
    pub fn task_description(&self) -> String {
        format!("{}: {}", self.skill_atomic, self.en_skill_detail)
    }
}

impl AnnotationData {
    /// Load annotation data from a JSON file.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let content = fs::read_to_string(path).map_err(|e| {
            roboflow_core::RoboflowError::parse(
                "Annotation",
                format!("Failed to read {}: {}", path.display(), e),
            )
        })?;

        let data: AnnotationData = serde_json::from_str(&content).map_err(|e| {
            roboflow_core::RoboflowError::parse(
                "Annotation",
                format!("Failed to parse JSON: {}", e),
            )
        })?;

        Ok(data)
    }

    /// Get episode segments based on skill marks.
    ///
    /// Returns a list of (start_pos, end_pos, task_description) tuples.
    pub fn episode_segments(&self) -> Vec<(f64, f64, String)> {
        self.marks
            .iter()
            .map(|mark| {
                (
                    mark.start_position,
                    mark.end_position,
                    mark.task_description(),
                )
            })
            .collect()
    }

    /// Get the total task name for this dataset.
    pub fn task_name(&self) -> &str {
        &self.task_name
    }

    /// Get the robot type/device info.
    pub fn robot_type(&self) -> String {
        format!("kuavo_{}", self.device_sn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_annotation() {
        let json = r#"{
            "location": "Test",
            "primaryScene": "Scene1",
            "secondaryScene": "Scene2",
            "tertiaryScene": "Task1",
            "taskName": "Test Task",
            "taskCode": "TEST",
            "deviceSn": "P4-001",
            "marks": [
                {
                    "taskId": "123",
                    "markStart": "2025-01-01 00:00:00.000",
                    "markEnd": "2025-01-01 00:00:10.000",
                    "duration": 10.0,
                    "startPosition": 0.0,
                    "endPosition": 0.5,
                    "skillAtomic": "pick",
                    "skillDetail": "Pick up object",
                    "enSkillDetail": "Pick up object",
                    "markType": "step"
                },
                {
                    "taskId": "123",
                    "markStart": "2025-01-01 00:00:10.000",
                    "markEnd": "2025-01-01 00:00:20.000",
                    "duration": 10.0,
                    "startPosition": 0.5,
                    "endPosition": 1.0,
                    "skillAtomic": "place",
                    "skillDetail": "Place down",
                    "enSkillDetail": "Place object",
                    "markType": "step"
                }
            ]
        }"#;

        let data: AnnotationData = serde_json::from_str(json).unwrap();
        assert_eq!(data.task_name, "Test Task");
        assert_eq!(data.marks.len(), 2);
        assert_eq!(data.marks[0].skill_atomic, "pick");
    }

    #[test]
    fn test_skill_mark_task_description() {
        let mark = SkillMark {
            task_id: "task-123".to_string(),
            mark_start: "2025-01-01 00:00:00.000".to_string(),
            mark_end: "2025-01-01 00:00:10.000".to_string(),
            duration: 10.0,
            start_position: 0.0,
            end_position: 0.5,
            skill_atomic: "pick".to_string(),
            skill_detail: "抓取红色方块".to_string(),
            en_skill_detail: "Pick up red block".to_string(),
            mark_type: "step".to_string(),
        };

        let description = mark.task_description();
        assert_eq!(description, "pick: Pick up red block");
    }

    #[test]
    fn test_skill_mark_task_description_various_skills() {
        let skills = vec![
            ("pick", "Pick object"),
            ("place", "Place object"),
            ("move", "Move to position"),
            ("insert", "Insert into slot"),
        ];

        for (skill, detail) in skills {
            let mark = SkillMark {
                task_id: "test".to_string(),
                mark_start: String::new(),
                mark_end: String::new(),
                duration: 0.0,
                start_position: 0.0,
                end_position: 1.0,
                skill_atomic: skill.to_string(),
                skill_detail: String::new(),
                en_skill_detail: detail.to_string(),
                mark_type: "step".to_string(),
            };

            let description = mark.task_description();
            assert!(description.starts_with(&format!("{}:", skill)));
            assert!(description.contains(detail));
        }
    }

    #[test]
    fn test_annotation_data_episode_segments() {
        let json = r#"{
            "location": "Test",
            "primaryScene": "Scene1",
            "secondaryScene": "Scene2",
            "tertiaryScene": "Task1",
            "taskName": "Test Task",
            "taskCode": "TEST",
            "deviceSn": "P4-001",
            "marks": [
                {
                    "taskId": "123",
                    "markStart": "2025-01-01 00:00:00.000",
                    "markEnd": "2025-01-01 00:00:10.000",
                    "duration": 10.0,
                    "startPosition": 0.0,
                    "endPosition": 0.5,
                    "skillAtomic": "pick",
                    "skillDetail": "Pick",
                    "enSkillDetail": "Pick up",
                    "markType": "step"
                },
                {
                    "taskId": "123",
                    "markStart": "2025-01-01 00:00:10.000",
                    "markEnd": "2025-01-01 00:00:20.000",
                    "duration": 10.0,
                    "startPosition": 0.5,
                    "endPosition": 1.0,
                    "skillAtomic": "place",
                    "skillDetail": "Place",
                    "enSkillDetail": "Place down",
                    "markType": "step"
                }
            ]
        }"#;

        let data: AnnotationData = serde_json::from_str(json).unwrap();
        let segments = data.episode_segments();

        assert_eq!(segments.len(), 2);

        // First segment
        assert_eq!(segments[0].0, 0.0); // start_position
        assert_eq!(segments[0].1, 0.5); // end_position
        assert!(segments[0].2.contains("pick"));

        // Second segment
        assert_eq!(segments[1].0, 0.5);
        assert_eq!(segments[1].1, 1.0);
        assert!(segments[1].2.contains("place"));
    }

    #[test]
    fn test_annotation_data_episode_segments_empty() {
        let json = r#"{
            "location": "Test",
            "primaryScene": "Scene1",
            "secondaryScene": "Scene2",
            "tertiaryScene": "Task1",
            "taskName": "Test Task",
            "taskCode": "TEST",
            "deviceSn": "P4-001"
        }"#;

        let data: AnnotationData = serde_json::from_str(json).unwrap();
        let segments = data.episode_segments();
        assert!(segments.is_empty());
    }

    #[test]
    fn test_annotation_data_task_name() {
        let json = r#"{
            "location": "Test",
            "primaryScene": "Scene1",
            "secondaryScene": "Scene2",
            "tertiaryScene": "Task1",
            "taskName": "Pick and Place Task",
            "taskCode": "TEST",
            "deviceSn": "P4-001"
        }"#;

        let data: AnnotationData = serde_json::from_str(json).unwrap();
        assert_eq!(data.task_name(), "Pick and Place Task");
    }

    #[test]
    fn test_annotation_data_robot_type() {
        let json = r#"{
            "location": "Test",
            "primaryScene": "Scene1",
            "secondaryScene": "Scene2",
            "tertiaryScene": "Task1",
            "taskName": "Test Task",
            "taskCode": "TEST",
            "deviceSn": "P4-001"
        }"#;

        let data: AnnotationData = serde_json::from_str(json).unwrap();
        assert_eq!(data.robot_type(), "kuavo_P4-001");
    }

    #[test]
    fn test_annotation_data_robot_type_various() {
        let device_sns = vec!["P4-001", "P4-002", "DEV-123", "PROD-001"];

        for sn in device_sns {
            let json = format!(
                r#"{{
                    "location": "Test",
                    "primaryScene": "Scene1",
                    "secondaryScene": "Scene2",
                    "tertiaryScene": "Task1",
                    "taskName": "Test Task",
                    "taskCode": "TEST",
                    "deviceSn": "{}"
                }}"#,
                sn
            );

            let data: AnnotationData = serde_json::from_str(&json).unwrap();
            assert_eq!(data.robot_type(), format!("kuavo_{}", sn));
        }
    }

    #[test]
    fn test_annotation_data_default_fields() {
        let json = r#"{
            "location": "Test",
            "primaryScene": "Scene1",
            "secondaryScene": "Scene2",
            "tertiaryScene": "Task1",
            "taskName": "Test Task",
            "taskCode": "TEST",
            "deviceSn": "P4-001"
        }"#;

        let data: AnnotationData = serde_json::from_str(json).unwrap();

        // Default fields should be empty strings
        assert_eq!(data.init_scene_text, "");
        assert_eq!(data.english_init_scene_text, "");
        assert_eq!(data.task_prompt, "");
        assert!(data.marks.is_empty());
    }

    #[test]
    fn test_annotation_data_with_optional_fields() {
        let json = r#"{
            "location": "Test",
            "primaryScene": "Scene1",
            "secondaryScene": "Scene2",
            "tertiaryScene": "Task1",
            "initSceneText": "Initial scene",
            "englishInitSceneText": "Initial scene description",
            "taskName": "Test Task",
            "taskCode": "TEST",
            "deviceSn": "P4-001",
            "taskPrompt": "Please complete the task"
        }"#;

        let data: AnnotationData = serde_json::from_str(json).unwrap();

        assert_eq!(data.init_scene_text, "Initial scene");
        assert_eq!(data.english_init_scene_text, "Initial scene description");
        assert_eq!(data.task_prompt, "Please complete the task");
    }

    #[test]
    fn test_skill_mark_all_fields() {
        let json = r#"{
            "taskId": "task-456",
            "markStart": "2025-01-01 10:30:00.000",
            "markEnd": "2025-01-01 10:30:15.500",
            "duration": 15.5,
            "startPosition": 0.25,
            "endPosition": 0.75,
            "skillAtomic": "insert",
            "skillDetail": "Insert peg into hole",
            "enSkillDetail": "Insert the peg into the hole",
            "markType": "primitive"
        }"#;

        let mark: SkillMark = serde_json::from_str(json).unwrap();

        assert_eq!(mark.task_id, "task-456");
        assert_eq!(mark.mark_start, "2025-01-01 10:30:00.000");
        assert_eq!(mark.mark_end, "2025-01-01 10:30:15.500");
        assert_eq!(mark.duration, 15.5);
        assert_eq!(mark.start_position, 0.25);
        assert_eq!(mark.end_position, 0.75);
        assert_eq!(mark.skill_atomic, "insert");
        assert_eq!(mark.skill_detail, "Insert peg into hole");
        assert_eq!(mark.en_skill_detail, "Insert the peg into the hole");
        assert_eq!(mark.mark_type, "primitive");
    }
}
