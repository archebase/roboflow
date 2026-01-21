// Copyright (c) 2026 ArcheBase
// Roboflow is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//     http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

//! Example: Generate task_info.json for Kps dataset.
//!
//! This example shows how to create and write task_info JSON files
//! as specified in the Kps data format v1.2.

use roboflow::format::kps::{
    ActionSegmentBuilder, TaskInfo, TaskInfoBuilder, write_task_info,
};
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example 1: Creating task_info for the housekeeper scenario
    let task_info = TaskInfoBuilder::new()
        .episode_id("53p21GB-2000")
        .scene_name("Housekeeper")
        .sub_scene_name("Kitchen")
        .init_scene_text("外卖袋放置在桌面左或右侧，外卖盒凌乱摆放在桌面左或右侧，垃圾桶放置在桌子的左或右侧")
        .english_init_scene_text("The takeout bag is placed on the left or right side of the desk, takeout boxes are cluttered on the left or right side of the desk, and the trash can is positioned on the left or right side of the desk.")
        .task_name("收拾外卖盒")
        .english_task_name("Dispose of takeout containers")
        .sn_code("A2D0001AB00029")
        .sn_name("宇树-H1-Dexhand")
        .data_type("常规")
        .episode_status("approved")
        .data_gen_mode("real_machine")
        // Add action segments
        .add_action_segment(
            ActionSegmentBuilder::new(215, 511, "Pick")
                .action_text("左臂拿起桌面上的外卖袋")
                .english_action_text("Pick up the takeout bag on the table with left arm.")
                .timestamp("2025-06-16T02:22:48.391668+00:00")
                .build()?,
        )
        .add_action_segment(
            ActionSegmentBuilder::new(511, 724, "Pick")
                .action_text("右臂拿起桌面上的圆形外卖盒")
                .english_action_text("Take the round takeout container on the table with right arm.")
                .timestamp("2025-06-16T02:22:57.681320+00:00")
                .build()?,
        )
        .add_action_segment(
            ActionSegmentBuilder::new(724, 963, "Place")
                .action_text("用右臂把拿着的圆形外卖盒装进左臂拿着的外卖袋中")
                .english_action_text("Place the held round takeout container into the takeout bag held by left arm with right arm.")
                .timestamp("2025-06-16T02:23:08.268534+00:00")
                .build()?,
        )
        .add_action_segment(
            ActionSegmentBuilder::new(963, 1174, "Pick")
                .action_text("右臂拿起桌面上的方形外卖盒")
                .english_action_text("Pick up the square takeout container on the table with right arm.")
                .timestamp("2025-06-16T02:23:20.724682+00:00")
                .build()?,
        )
        .add_action_segment(
            ActionSegmentBuilder::new(1174, 1509, "Place")
                .action_text("用右臂把拿着的方形外卖盒装进左臂拿着的外卖袋中")
                .english_action_text("Pack the held square takeout container into the takeout bag held in left arm with right arm.")
                .timestamp("2025-06-16T02:23:32.954384+00:00")
                .build()?,
        )
        .add_action_segment(
            ActionSegmentBuilder::new(1509, 1692, "Pick")
                .action_text("右臂拿起桌面上的用过的餐具包装袋")
                .english_action_text("Pick up the used cutlery packaging bag on the table with right arm.")
                .timestamp("2025-06-16T02:23:37.246875+00:00")
                .build()?,
        )
        .add_action_segment(
            ActionSegmentBuilder::new(1692, 1897, "Place")
                .action_text("用右臂把拿着的餐具包装袋装进左臂拿着的外卖袋中")
                .english_action_text("Pack the utensil bag into the takeout bag held in left arm with right arm.")
                .timestamp("2025-06-16T02:23:48.463981+00:00")
                .build()?,
        )
        .add_action_segment(
            ActionSegmentBuilder::new(1897, 2268, "Drop")
                .action_text("左臂把拿着的外卖袋丢进垃圾桶里")
                .english_action_text("Discard the held takeout bag in the trash can with left arm.")
                .timestamp("2025-06-16T02:23:55.425176+00:00")
                .build()?,
        )
        .build()?;

    // Write to output directory
    let output_dir = PathBuf::from("./output");
    write_task_info(&output_dir, &task_info)?;

    println!("Created task_info JSON:");
    println!("  Directory: {}/task_info/", output_dir.display());
    println!("  File: Housekeeper-Kitchen-Dispose_of_takeout_containers.json");
    println!();

    // Example 2: Different skill types
    demonstrate_skill_types()?;

    println!("Task info examples generated successfully!");
    Ok(())
}

/// Demonstrate all supported skill types.
fn demonstrate_skill_types() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Supported Skill Types ===");

    let skills = vec![
        ("Pick", "拾起", "Pick up object"),
        ("Place", "放下", "Place object"),
        ("Drop", "丢弃", "Drop object"),
        ("Grasp", "抓取", "Grasp object"),
        ("Release", "释放", "Release object"),
        ("Move", "移动", "Move to location"),
        ("Push", "推", "Push object"),
        ("Pull", "拉", "Pull object"),
        ("Twist", "扭转", "Twist object"),
        ("Pour", "倒", "Pour contents"),
    ];

    for (skill, chinese, description) in skills {
        println!("  {} ({})", skill, description);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_task_info_example() {
        let task_info = TaskInfoBuilder::new()
            .episode_id("test-episode-001")
            .scene_name("TestScene")
            .sub_scene_name("TestSubScene")
            .init_scene_text("测试初始场景")
            .english_init_scene_text("Test initial scene")
            .task_name("测试任务")
            .english_task_name("Test Task")
            .sn_code("TEST001")
            .sn_name("TestCompany-RobotType-Gripper")
            .add_action_segment(
                ActionSegmentBuilder::new(0, 100, "Pick")
                    .action_text("拿起物体")
                    .english_action_text("Pick up object")
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        assert_eq!(task_info.episode_id, "test-episode-001");
        assert_eq!(task_info.label_info.action_config.len(), 1);
    }
}
