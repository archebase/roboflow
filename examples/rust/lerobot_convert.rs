// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Example: Convert ROS bag + JSON annotations to LeRobot v2.1 format
//!
//! This example demonstrates how to:
//! 1. Load ROS bag files with robot data
//! 2. Parse JSON annotation files for episode segmentation
//! 3. Convert to LeRobot v2.1 dataset format
//!
//! Usage:
//!   cargo run --example lerobot_convert -- \
//!     --bag /path/to/data.bag \
//!     --annotation /path/to/annotation.json \
//!     --output /path/to/lerobot_dataset \
//!     --config /path/to/config.toml

use std::path::PathBuf;

use roboflow::lerobot::{AnnotationData, LerobotConfig, LerobotWriter};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse arguments
    let args = parse_args()?;

    // Load configuration
    let config = if let Some(ref config_path) = args.config {
        LerobotConfig::from_file(config_path)?
    } else {
        // Default configuration for the rubbish sorting task
        default_config()?
    };

    // Load annotation file for episode segmentation
    let annotations = args
        .annotation
        .as_ref()
        .map(AnnotationData::from_file)
        .transpose()?;

    // Create output directory
    std::fs::create_dir_all(&args.output)?;

    // Create LeRobot writer
    let mut writer = LerobotWriter::new_local(&args.output, config)?;

    // Process the ROS bag
    println!("Processing ROS bag: {:?}", args.bag);

    // TODO: Integrate with robocodec to read the bag
    // For now, this is a skeleton showing the structure

    if let Some(ann) = annotations {
        println!(
            "Loaded {} episode segments from annotations",
            ann.marks.len()
        );

        // Register tasks from annotations
        for mark in &ann.marks {
            let task_desc = mark.task_description();
            let _task_index = writer.register_task(task_desc);
        }

        // Process each episode segment
        for (i, mark) in ann.marks.iter().enumerate() {
            println!(
                "Processing episode {}: {} ({})",
                i, mark.skill_atomic, mark.en_skill_detail
            );

            // TODO: Extract data from bag for this segment
            // The segment boundaries are given by mark.start_position and mark.end_position

            // For demonstration, create a placeholder episode
            let _ = writer.start_episode(Some(i));

            // In production, you would:
            // 1. Seek to mark.start_position in the bag
            // 2. Read messages until mark.end_position
            // 3. Extract images and state data
            // 4. Add frames using add_frame() and add_image()

            writer.finish_episode(Some(i))?;
        }
    } else {
        println!("No annotations provided, treating entire bag as one episode");

        // Process entire bag as one episode
        let _ = writer.start_episode(None);
        // TODO: Extract all data from bag
        writer.finish_episode(None)?;
    }

    // Finalize the dataset
    let episode_count = writer.metadata().episodes.len();
    let total_frames = writer.finalize()?;

    println!("Created LeRobot v2.1 dataset at {:?}", args.output);
    println!("Total episodes: {}", episode_count);
    println!("Total frames: {}", total_frames);

    Ok(())
}

/// Command-line arguments
struct Args {
    bag: PathBuf,
    annotation: Option<PathBuf>,
    output: PathBuf,
    config: Option<PathBuf>,
}

/// Parse command-line arguments
fn parse_args() -> Result<Args, Box<dyn std::error::Error>> {
    let mut bag = None;
    let mut annotation = None;
    let mut output = None;
    let mut config = None;

    let mut args_iter = std::env::args().skip(1);

    while let Some(arg) = args_iter.next() {
        match arg.as_str() {
            "--bag" | "-b" => {
                bag = Some(args_iter.next().ok_or("Missing --bag value")?.into());
            }
            "--annotation" | "-a" => {
                annotation = Some(args_iter.next().ok_or("Missing --annotation value")?.into());
            }
            "--output" | "-o" => {
                output = Some(args_iter.next().ok_or("Missing --output value")?.into());
            }
            "--config" | "-c" => {
                config = Some(args_iter.next().ok_or("Missing --config value")?.into());
            }
            _ => {
                return Err(format!("Unknown argument: {}", arg).into());
            }
        }
    }

    Ok(Args {
        bag: bag.ok_or("Missing --bag argument")?,
        annotation,
        output: output.ok_or("Missing --output argument")?,
        config,
    })
}

/// Create default configuration for rubbish sorting task
fn default_config() -> Result<LerobotConfig, Box<dyn std::error::Error>> {
    let toml = r#"
[dataset]
name = "rubbish_sorting"
fps = 30
robot_type = "kuavo_p4"

[[mappings]]
topic = "/cam_h/color/image_raw/compressed"
feature = "observation.images.cam_high"
mapping_type = "image"

[[mappings]]
topic = "/cam_l/color/image_raw/compressed"
feature = "observation.images.cam_left"
mapping_type = "image"

[[mappings]]
topic = "/cam_r/color/image_raw/compressed"
feature = "observation.images.cam_right"
mapping_type = "image"

[[mappings]]
topic = "/kuavo_arm_traj"
feature = "observation.state"
mapping_type = "state"

[[mappings]]
topic = "/joint_cmd"
feature = "action"
mapping_type = "action"

[video]
codec = "libx264"
crf = 18
preset = "fast"
"#;

    Ok(toml::from_str(toml)?)
}
