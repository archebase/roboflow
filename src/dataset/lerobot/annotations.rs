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

use crate::core::Result;

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
        let content = fs::read_to_string(path)
            .map_err(|e| crate::RoboflowError::parse("Annotation", &format!("Failed to read {}: {}", path.display(), e)))?;

        let data: AnnotationData = serde_json::from_str(&content)
            .map_err(|e| crate::RoboflowError::parse("Annotation", &format!("Failed to parse JSON: {}", e)))?;

        Ok(data)
    }

    /// Get episode segments based on skill marks.
    ///
    /// Returns a list of (start_pos, end_pos, task_description) tuples.
    pub fn episode_segments(&self) -> Vec<(f64, f64, String)> {
        self.marks.iter().map(|mark| {
            (mark.start_position, mark.end_position, mark.task_description())
        }).collect()
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
}
