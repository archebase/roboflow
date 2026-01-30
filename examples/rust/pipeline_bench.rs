// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Benchmark: Full roboflow pipeline throughput with real bag data.
//!
//! This benchmark measures the actual throughput of converting
//! ROS bag data to LeRobot format, including:
//! - Bag file reading
//! - Message decoding
//! - Image decompression
//! - Video encoding
//! - Parquet writing
//!
//! Usage:
//!   cargo run --release --example pipeline_bench -- --input /path/to/file.bag --profile speed

use std::path::{Path, PathBuf};
use std::time::Instant;

use roboflow::dataset::common::{AlignedFrame, DatasetWriter, ImageData};
use roboflow::dataset::lerobot::{LerobotConfig, LerobotWriter};
use roboflow::io::ReaderFactory;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args()?;

    println!("==========================================");
    println!("Roboflow Pipeline Throughput Benchmark");
    println!("==========================================");
    println!("Input: {:?}", args.input);
    println!("Output: {:?}", args.output_dir);
    if let Some(profile) = &args.profile {
        println!("Profile: {}", profile);
    }
    println!();

    // Create LeRobot config
    let lerobot_config = create_lerobot_config(&args)?;

    // Create output directory
    std::fs::create_dir_all(&args.output_dir)?;

    // Create LeRobot writer
    let mut writer = LerobotWriter::create(&args.output_dir, lerobot_config.clone())?;
    writer.initialize(&lerobot_config)?;

    let total_start = Instant::now();
    let decode_start = Instant::now();

    // Open the input file
    let reader = ReaderFactory::open(&args.input)?;

    // Get file info
    let input_size = std::fs::metadata(&args.input)?.len();
    let input_size_mb = input_size as f64 / (1024.0 * 1024.0);

    println!("Input size: {:.2} MB", input_size_mb);
    println!();

    // Create schema and read messages
    let schema = reader.schema()?;

    let mut total_messages = 0usize;
    let mut processed_messages = 0usize;
    let mut total_frames = 0usize;
    let mut total_images = 0usize;

    // Channel mappings (for the LeJu robot dataset)
    let camera_channels = [
        (
            "/cam_h/color/image_raw/compressed",
            "observation.images.cam_high",
        ),
        (
            "/cam_r/color/image_raw/compressed",
            "observation.images.cam_right",
        ),
        (
            "/cam_l/color/image_raw/compressed",
            "observation.images.cam_left",
        ),
    ];
    let state_channel = "/kuavo_arm_traj";
    let action_channel = "/joint_cmd";

    writer.start_episode(Some(0));

    // Read and process messages
    println!("Processing messages...");

    let mut last_progress = Instant::now();
    let mut frame_count = 0;

    for result in reader.iter(schema.clone())? {
        total_messages += 1;

        let msg = result?;
        processed_messages += 1;

        let channel = msg.channel.clone();
        let channel_name = channel.name.as_str();

        // Check if this is an image channel
        if let Some((_, feature_name)) = camera_channels
            .iter()
            .find(|(name, _)| *name == channel_name)
        {
            // This is a camera image - decode and store for video encoding
            if let Some(img_data) = msg.message_as_compressed_image()? {
                let image_data = ImageData::encoded(img_data.width, img_data.height, img_data.data);
                writer.add_image(feature_name.to_string(), image_data);
                total_images += 1;
            }
        } else if channel_name == state_channel {
            // This is state/joint data
            if let Some(joint_state) = msg.message_as_joint_state()? {
                // Convert joint state to Vec<f32>
                let positions: Vec<f32> = joint_state.position.iter().map(|&v| v as f32).collect();
                // Store state for current frame
                // Note: We need to match images with state, so we'd need timestamp matching
            }
        } else if channel_name == action_channel {
            // This is action data
            if let Some(joint_cmd) = msg.message_as_joint_cmd()? {
                // Convert joint command to Vec<f32>
                let positions: Vec<f32> = joint_cmd.pos.iter().map(|&v| v as f32).collect();
            }
        }

        // Progress reporting every second
        if last_progress.elapsed().as_secs() >= 1 {
            let elapsed = total_start.elapsed().as_secs_f64();
            let throughput = (processed_messages as f64) / elapsed;
            let read_mb = (processed_messages as f64 * 500.0) / (1024.0 * 1024.0); // rough estimate
            print!(
                "\r  Messages: {}, throughput: {:.0} msg/s, read: {:.1} MB    ",
                processed_messages, throughput, read_mb
            );
            use std::io::Write;
            std::io::stdout().flush().ok();
            last_progress = Instant::now();
        }

        // For benchmark, limit to a reasonable number of messages
        if total_messages >= args.max_messages {
            break;
        }
    }

    println!("\n  Processed {} messages", processed_messages);

    // Finish episode (this triggers video encoding and parquet writing)
    let episode_start = Instant::now();
    writer.finish_episode(Some(0))?;
    let episode_time = episode_start.elapsed();

    // Finalize
    let finalize_start = Instant::now();
    let frames_written = writer.finalize()?;
    let finalize_time = finalize_start.elapsed();

    let total_duration = total_start.elapsed();

    // Calculate output size
    let output_size = calculate_dir_size(&args.output_dir);
    let output_size_mb = output_size as f64 / (1024.0 * 1024.0);

    println!();
    println!("==========================================");
    println!("Results Summary");
    println!("==========================================");
    println!("Input size: {:.2} MB", input_size_mb);
    println!("Output size: {:.2} MB", output_size_mb);
    println!(
        "Compression ratio: {:.1}%",
        (output_size_mb / input_size_mb) * 100.0
    );
    println!();
    println!("Messages processed: {}", processed_messages);
    println!("Images encoded: {}", total_images);
    println!("Frames written: {}", frames_written);
    println!();
    println!("Timing breakdown:");
    println!(
        "  Message decoding: {:.2}s",
        decode_start.elapsed().as_secs_f64()
    );
    println!("  Episode finish: {:.2}s", episode_time.as_secs_f64());
    println!("  Finalize: {:.2}s", finalize_time.as_secs_f64());
    println!("  Total: {:.2}s", total_duration.as_secs_f64());
    println!();

    // Calculate throughput metrics
    let msg_throughput = (processed_messages as f64) / total_duration.as_secs_f64();
    let read_throughput = input_size_mb / total_duration.as_secs_f64();
    let write_throughput = output_size_mb / total_duration.as_secs_f64();

    println!("Throughput:");
    println!("  Messages: {:.0} msg/s", msg_throughput);
    println!("  Read: {:.1} MB/s", read_throughput);
    println!("  Write: {:.1} MB/s", write_throughput);
    println!();

    // Extrapolate to full file
    if total_messages < args.max_messages {
        let total_in_file = 487882; // from bag info
        let estimated_time =
            total_duration.as_secs_f64() * (total_in_file as f64 / processed_messages as f64);
        println!("Full file extrapolation:");
        println!("  Total messages in file: {}", total_in_file);
        println!(
            "  Estimated total time: {:.1} s ({:.2} min)",
            estimated_time,
            estimated_time / 60.0
        );
    }

    Ok(())
}

struct BenchConfig {
    input: PathBuf,
    output_dir: PathBuf,
    profile: Option<String>,
    max_messages: usize,
}

fn parse_args() -> Result<BenchConfig, Box<dyn std::error::Error>> {
    let mut input = None;
    let mut output_dir = None;
    let mut profile = None;
    let mut max_messages = 10000; // Process 10k messages by default

    let mut args = std::env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--input" | "-i" => {
                input = Some(args.next().ok_or("Missing --input value")?.into());
            }
            "--output" | "-o" => {
                output_dir = Some(args.next().ok_or("Missing --output value")?.into());
            }
            "--profile" | "-p" => {
                profile = Some(args.next().ok_or("Missing --profile value")?);
            }
            "--max-messages" | "-m" => {
                max_messages = args.next().ok_or("Missing --max-messages value")?.parse()?;
            }
            _ => {
                return Err(format!("Unknown argument: {}", arg).into());
            }
        }
    }

    let input = input.unwrap_or_else(|| {
        PathBuf::from(
            "/Users/zhexuany/Downloads/leju_bag/Rubbish_sorting_P4-278_20250830101814.bag",
        )
    });

    let output_dir = output_dir.unwrap_or_else(|| PathBuf::from("./pipeline_bench_output"));

    Ok(BenchConfig {
        input,
        output_dir,
        profile,
        max_messages,
    })
}

fn create_lerobot_config(
    config: &BenchConfig,
) -> Result<LerobotConfig, Box<dyn std::error::Error>> {
    let profile_line = if let Some(profile) = &config.profile {
        format!("profile = \"{}\"", profile)
    } else {
        "".to_string()
    };

    let toml = format!(
        r#"
[dataset]
name = "leju_rubbish_sorting"
fps = 30

[[mappings]]
topic = "/cam_h/color/image_raw/compressed"
feature = "observation.images.cam_high"
mapping_type = "image"

[[mappings]]
topic = "/cam_r/color/image_raw/compressed"
feature = "observation.images.cam_right"
mapping_type = "image"

[[mappings]]
topic = "/cam_l/color/image_raw/compressed"
feature = "observation.images.cam_left"
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
{}
codec = "libx264"
crf = 18
preset = "fast"
"#,
        profile_line
    );

    Ok(toml::from_str(&toml)?)
}

fn calculate_dir_size(path: &Path) -> u64 {
    let mut size = 0u64;
    if path.is_dir() {
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                size += calculate_dir_size(&entry.path());
            }
        }
    } else if path.is_file() {
        size += std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    }
    size
}
