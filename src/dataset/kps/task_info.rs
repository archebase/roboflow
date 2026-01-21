//! Task Info JSON generation for Kps datasets.
//!
//! Creates `task_info/<Scene>-<SubScene>-<Task>.json` files as per the v1.2 specification.

use serde::Serialize;
use std::fs;
use std::path::Path;

/// Task info metadata for a single episode.
#[derive(Debug, Clone, Serialize)]
pub struct TaskInfo {
    /// Unique identifier matching the UUID directory name
    pub episode_id: String,
    /// Scene name (e.g., "Housekeeper")
    pub scene_name: String,
    /// Sub-scene name (e.g., "Kitchen")
    pub sub_scene_name: String,
    /// Initial scene description in Chinese
    pub init_scene_text: String,
    /// Initial scene description in English
    pub english_init_scene_text: String,
    /// Task name in Chinese
    pub task_name: String,
    /// Task name in English
    pub english_task_name: String,
    /// Data type
    pub data_type: String,
    /// Episode status
    pub episode_status: String,
    /// Data generation mode: "real_machine" or "simulation"
    pub data_gen_mode: String,
    /// Machine serial number
    pub sn_code: String,
    /// Robot name in format: "厂家-机器人型号-末端执行器"
    pub sn_name: String,
    /// Label information with action segments
    pub label_info: LabelInfo,
}

/// Label information containing action segments.
#[derive(Debug, Clone, Serialize)]
pub struct LabelInfo {
    /// Array of labeled action segments
    pub action_config: Vec<ActionSegment>,
    /// Key frame annotations (optional, to be implemented)
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub key_frame: Vec<KeyFrame>,
}

/// A single action segment annotation.
#[derive(Debug, Clone, Serialize)]
pub struct ActionSegment {
    /// Start frame index (inclusive)
    pub start_frame: u64,
    /// End frame index (exclusive)
    pub end_frame: u64,
    /// UTC timestamp of segment start
    pub timestamp_utc: String,
    /// Action description in Chinese
    pub action_text: String,
    /// Skill type (e.g., "Pick", "Place", "Drop")
    pub skill: String,
    /// Whether this action was a mistake
    pub is_mistake: bool,
    /// Action description in English
    pub english_action_text: String,
}

/// Key frame annotation (future use).
#[derive(Debug, Clone, Serialize)]
pub struct KeyFrame {
    pub frame_number: u64,
    pub description: String,
    pub importance: String,
}

/// Builder for creating TaskInfo with defaults.
#[derive(Debug, Clone)]
pub struct TaskInfoBuilder {
    episode_id: Option<String>,
    scene_name: Option<String>,
    sub_scene_name: Option<String>,
    init_scene_text: Option<String>,
    english_init_scene_text: Option<String>,
    task_name: Option<String>,
    english_task_name: Option<String>,
    data_type: Option<String>,
    episode_status: Option<String>,
    data_gen_mode: Option<String>,
    sn_code: Option<String>,
    sn_name: Option<String>,
    action_segments: Vec<ActionSegment>,
}

impl Default for TaskInfoBuilder {
    fn default() -> Self {
        Self {
            episode_id: None,
            scene_name: None,
            sub_scene_name: None,
            init_scene_text: None,
            english_init_scene_text: None,
            task_name: None,
            english_task_name: None,
            data_type: Some("常规".to_string()),
            episode_status: Some("approved".to_string()),
            data_gen_mode: Some("real_machine".to_string()),
            sn_code: None,
            sn_name: None,
            action_segments: Vec::new(),
        }
    }
}

impl TaskInfoBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set episode ID (UUID).
    pub fn episode_id(mut self, id: impl Into<String>) -> Self {
        self.episode_id = Some(id.into());
        self
    }

    /// Set scene name.
    pub fn scene_name(mut self, name: impl Into<String>) -> Self {
        self.scene_name = Some(name.into());
        self
    }

    /// Set sub-scene name.
    pub fn sub_scene_name(mut self, name: impl Into<String>) -> Self {
        self.sub_scene_name = Some(name.into());
        self
    }

    /// Set initial scene description (Chinese).
    pub fn init_scene_text(mut self, text: impl Into<String>) -> Self {
        self.init_scene_text = Some(text.into());
        self
    }

    /// Set initial scene description (English).
    pub fn english_init_scene_text(mut self, text: impl Into<String>) -> Self {
        self.english_init_scene_text = Some(text.into());
        self
    }

    /// Set task name (Chinese).
    pub fn task_name(mut self, name: impl Into<String>) -> Self {
        self.task_name = Some(name.into());
        self
    }

    /// Set task name (English).
    pub fn english_task_name(mut self, name: impl Into<String>) -> Self {
        self.english_task_name = Some(name.into());
        self
    }

    /// Set data type.
    pub fn data_type(mut self, data_type: impl Into<String>) -> Self {
        self.data_type = Some(data_type.into());
        self
    }

    /// Set episode status.
    pub fn episode_status(mut self, status: impl Into<String>) -> Self {
        self.episode_status = Some(status.into());
        self
    }

    /// Set data generation mode.
    pub fn data_gen_mode(mut self, mode: impl Into<String>) -> Self {
        self.data_gen_mode = Some(mode.into());
        self
    }

    /// Set machine serial code.
    pub fn sn_code(mut self, code: impl Into<String>) -> Self {
        self.sn_code = Some(code.into());
        self
    }

    /// Set robot name in format "厂家-机器人型号-末端执行器".
    pub fn sn_name(mut self, name: impl Into<String>) -> Self {
        self.sn_name = Some(name.into());
        self
    }

    /// Add an action segment.
    pub fn add_action_segment(mut self, segment: ActionSegment) -> Self {
        self.action_segments.push(segment);
        self
    }

    /// Add multiple action segments.
    pub fn add_action_segments(mut self, segments: impl IntoIterator<Item = ActionSegment>) -> Self {
        self.action_segments.extend(segments);
        self
    }

    /// Build the TaskInfo.
    pub fn build(self) -> Result<TaskInfo, String> {
        Ok(TaskInfo {
            episode_id: self.episode_id.ok_or("episode_id is required")?,
            scene_name: self.scene_name.ok_or("scene_name is required")?,
            sub_scene_name: self.sub_scene_name.ok_or("sub_scene_name is required")?,
            init_scene_text: self.init_scene_text.ok_or("init_scene_text is required")?,
            english_init_scene_text: self.english_init_scene_text
                .ok_or("english_init_scene_text is required")?,
            task_name: self.task_name.ok_or("task_name is required")?,
            english_task_name: self.english_task_name.ok_or("english_task_name is required")?,
            data_type: self.data_type.unwrap_or_else(|| "常规".to_string()),
            episode_status: self.episode_status.unwrap_or_else(|| "approved".to_string()),
            data_gen_mode: self.data_gen_mode.unwrap_or_else(|| "real_machine".to_string()),
            sn_code: self.sn_code.ok_or("sn_code is required")?,
            sn_name: self.sn_name.ok_or("sn_name is required")?,
            label_info: LabelInfo {
                action_config: self.action_segments,
                key_frame: Vec::new(),
            },
        })
    }
}

/// Action segment builder for convenience.
#[derive(Debug, Clone)]
pub struct ActionSegmentBuilder {
    start_frame: u64,
    end_frame: u64,
    timestamp_utc: Option<String>,
    action_text: Option<String>,
    skill: String,
    is_mistake: bool,
    english_action_text: Option<String>,
}

impl ActionSegmentBuilder {
    /// Create a new action segment.
    pub fn new(start_frame: u64, end_frame: u64, skill: impl Into<String>) -> Self {
        Self {
            start_frame,
            end_frame,
            timestamp_utc: None,
            action_text: None,
            skill: skill.into(),
            is_mistake: false,
            english_action_text: None,
        }
    }

    /// Set the timestamp.
    pub fn timestamp(mut self, ts: impl Into<String>) -> Self {
        self.timestamp_utc = Some(ts.into());
        self
    }

    /// Set the Chinese action text.
    pub fn action_text(mut self, text: impl Into<String>) -> Self {
        self.action_text = Some(text.into());
        self
    }

    /// Set the English action text.
    pub fn english_action_text(mut self, text: impl Into<String>) -> Self {
        self.english_action_text = Some(text.into());
        self
    }

    /// Mark as a mistake.
    pub fn is_mistake(mut self, mistake: bool) -> Self {
        self.is_mistake = mistake;
        self
    }

    /// Build the ActionSegment.
    pub fn build(self) -> Result<ActionSegment, String> {
        Ok(ActionSegment {
            start_frame: self.start_frame,
            end_frame: self.end_frame,
            timestamp_utc: self.timestamp_utc.unwrap_or_else(|| {
                // Default to current time in RFC3339 format
                use std::time::{SystemTime, UNIX_EPOCH};
                let duration = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default();
                format!("{}", duration.as_secs())
            }),
            action_text: self.action_text.ok_or("action_text is required")?,
            skill: self.skill,
            is_mistake: self.is_mistake,
            english_action_text: self.english_action_text
                .ok_or("english_action_text is required")?,
        })
    }
}

/// Write task_info JSON file.
///
/// Creates the task_info directory and writes the JSON file with the format:
/// `<scene_name>-<sub_scene_name>-<english_task_name>.json`
///
/// # Arguments
/// * `output_dir` - Base output directory (task_info will be created inside)
/// * `task_info` - TaskInfo to write
pub fn write_task_info(
    output_dir: &Path,
    task_info: &TaskInfo,
) -> Result<(), Box<dyn std::error::Error>> {
    let task_info_dir = output_dir.join("task_info");
    fs::create_dir_all(&task_info_dir)?;

    // Create filename: Scene-SubScene-Task.json
    // Convert task name to PascalCase with underscores
    let task_name_safe = task_info.english_task_name.replace(' ', "_");
    let filename = format!(
        "{}-{}-{}.json",
        task_info.scene_name,
        task_info.sub_scene_name,
        task_name_safe
    );

    let filepath = task_info_dir.join(filename);

    // Write JSON with pretty formatting
    let json = serde_json::to_string_pretty(task_info)?;
    fs::write(&filepath, json)?;

    Ok(())
}

/// Write task_info from a list of TaskInfo (multi-episode support).
pub fn write_task_info_batch(
    output_dir: &Path,
    task_infos: &[TaskInfo],
) -> Result<(), Box<dyn std::error::Error>> {
    for task_info in task_infos {
        write_task_info(output_dir, task_info)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_action_segment_builder() {
        let segment = ActionSegmentBuilder::new(0, 100, "Pick")
            .action_text("拿起桌面上的外卖袋")
            .english_action_text("Pick up the takeout bag on the table")
            .timestamp("2025-06-16T02:22:48.391668+00:00")
            .build()
            .unwrap();

        assert_eq!(segment.start_frame, 0);
        assert_eq!(segment.end_frame, 100);
        assert_eq!(segment.skill, "Pick");
        assert_eq!(segment.action_text, "拿起桌面上的外卖袋");
    }

    #[test]
    fn test_task_info_builder() {
        let task_info = TaskInfoBuilder::new()
            .episode_id("test-uuid-123")
            .scene_name("Housekeeper")
            .sub_scene_name("Kitchen")
            .init_scene_text("外卖袋放置在桌面左侧")
            .english_init_scene_text("The takeout bag is on the left side of the desk")
            .task_name("收拾外卖盒")
            .english_task_name("Dispose of takeout containers")
            .sn_code("A2D0001AB00029")
            .sn_name("宇树-H1-Dexhand")
            .add_action_segment(
                ActionSegmentBuilder::new(0, 100, "Pick")
                    .action_text("左臂拿起桌面上的外卖袋")
                    .english_action_text("Pick up the takeout bag with left arm")
                    .timestamp("2025-06-16T02:22:48.391668+00:00")
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        assert_eq!(task_info.episode_id, "test-uuid-123");
        assert_eq!(task_info.scene_name, "Housekeeper");
        assert_eq!(task_info.label_info.action_config.len(), 1);
        assert_eq!(task_info.label_info.action_config[0].skill, "Pick");
    }

    #[test]
    fn test_serialize_task_info() {
        let task_info = TaskInfo {
            episode_id: "uuid123".to_string(),
            scene_name: "Housekeeper".to_string(),
            sub_scene_name: "Kitchen".to_string(),
            init_scene_text: "测试场景".to_string(),
            english_init_scene_text: "Test scene".to_string(),
            task_name: "测试任务".to_string(),
            english_task_name: "Test Task".to_string(),
            data_type: "常规".to_string(),
            episode_status: "approved".to_string(),
            data_gen_mode: "real_machine".to_string(),
            sn_code: "A2D0001AB00029".to_string(),
            sn_name: "宇树-H1-Dexhand".to_string(),
            label_info: LabelInfo {
                action_config: vec![
                    ActionSegment {
                        start_frame: 0,
                        end_frame: 100,
                        timestamp_utc: "2025-06-16T02:22:48.391668+00:00".to_string(),
                        action_text: "拿起".to_string(),
                        skill: "Pick".to_string(),
                        is_mistake: false,
                        english_action_text: "Pick up".to_string(),
                    }
                ],
                key_frame: vec![],
            },
        };

        let json = serde_json::to_string_pretty(&task_info).unwrap();
        assert!(json.contains("\"episode_id\": \"uuid123\""));
        assert!(json.contains("\"scene_name\": \"Housekeeper\""));
        assert!(json.contains("\"action_config\""));
    }
}
