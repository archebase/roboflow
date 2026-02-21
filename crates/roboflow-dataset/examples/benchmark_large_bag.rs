// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use roboflow_dataset::formats::alignment::StreamingConfig;
use roboflow_dataset::formats::lerobot::{
    FlushingConfig, LerobotConfig, LerobotWriter, Mapping, MappingType,
    StreamingConfig as LerobotStreamingConfig, VideoConfig, config::DatasetBaseConfig,
    config::DatasetConfig,
};
use roboflow_dataset::formats::{ParallelPipelineExecutor, PipelineConfig};
use roboflow_dataset::sources::SourceConfig;

fn create_lerobot_config() -> LerobotConfig {
    LerobotConfig {
        dataset: DatasetConfig {
            base: DatasetBaseConfig {
                name: "benchmark".to_string(),
                fps: 30,
                robot_type: Some("kuavo_p4".to_string()),
            },
            env_type: None,
        },
        mappings: vec![
            Mapping {
                topic: "/cam_h/color/image_raw/compressed".to_string(),
                feature: "observation.images.cam_high".to_string(),
                mapping_type: MappingType::Image,
                camera_key: Some("cam_high".to_string()),
            },
            Mapping {
                topic: "/cam_l/color/image_raw/compressed".to_string(),
                feature: "observation.images.cam_left".to_string(),
                mapping_type: MappingType::Image,
                camera_key: Some("cam_left".to_string()),
            },
            Mapping {
                topic: "/cam_r/color/image_raw/compressed".to_string(),
                feature: "observation.images.cam_right".to_string(),
                mapping_type: MappingType::Image,
                camera_key: Some("cam_right".to_string()),
            },
            Mapping {
                topic: "/kuavo_arm_traj".to_string(),
                feature: "observation.state".to_string(),
                mapping_type: MappingType::State,
                camera_key: None,
            },
            Mapping {
                topic: "/joint_cmd".to_string(),
                feature: "action".to_string(),
                mapping_type: MappingType::Action,
                camera_key: None,
            },
        ],
        video: VideoConfig {
            codec: "libx264".to_string(),
            crf: 18,
            preset: "fast".to_string(),
            profile: None,
        },
        annotation_file: None,
        flushing: FlushingConfig::default(),
        streaming: LerobotStreamingConfig::default(),
    }
}

fn benchmark_bag_conversion(
    bag_path: &str,
    output_path: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("========================================");
    println!("Bag to LeRobot Conversion Benchmark");
    println!("========================================");
    println!("Input file: {}", bag_path);

    if let Ok(metadata) = std::fs::metadata(bag_path) {
        let size_mb = metadata.len() as f64 / (1024.0 * 1024.0);
        println!("File size: {:.1} MB", size_mb);
    }
    println!("Output directory: {}", output_path.display());
    println!();

    let config = create_lerobot_config();

    roboflow_dataset::sources::register_builtin_sources();

    let topic_mappings: HashMap<String, String> = config
        .mappings
        .iter()
        .map(|m| (m.topic.clone(), m.feature.clone()))
        .collect();

    let pipeline_streaming = StreamingConfig::with_fps(config.dataset.base.fps);
    let pipeline_config =
        PipelineConfig::new(pipeline_streaming).with_topic_mappings(topic_mappings);

    let writer = LerobotWriter::new_local(output_path, config.clone())?;
    let mut executor = ParallelPipelineExecutor::new(writer, pipeline_config)?;

    let source_config = SourceConfig::bag(bag_path);
    let mut source = roboflow_dataset::sources::create_source(&source_config)?;

    let rt = tokio::runtime::Runtime::new()?;
    let _metadata: roboflow_dataset::sources::SourceMetadata =
        rt.block_on(async { source.initialize(&source_config).await })?;

    let overall_start = Instant::now();

    let (all_messages, frame_count) = rt.block_on(async {
        let mut all_msgs = Vec::new();
        let mut count = 0usize;
        let mut last_report = Instant::now();

        loop {
            match source.read_batch(100).await {
                Ok(Some(messages)) if !messages.is_empty() => {
                    count += messages.len();
                    all_msgs.extend(messages);

                    if last_report.elapsed().as_secs() >= 5 {
                        println!("Collected {} messages...", count);
                        last_report = Instant::now();
                    }
                }
                Ok(Some(_)) => continue,
                Ok(None) => {
                    break;
                }
                Err(e) => {
                    eprintln!("Error reading batch: {}", e);
                    break;
                }
            }
        }
        (all_msgs, count)
    });

    println!(
        "Collected {} messages total, processing in parallel...",
        frame_count
    );

    let processing_start = Instant::now();
    executor.process_messages_parallel(all_messages)?;
    let processing_time = processing_start.elapsed();

    let stats = executor.finalize()?;
    let total_time = overall_start.elapsed();

    println!();
    println!("========================================");
    println!("Results");
    println!("========================================");
    println!("Frames processed: {}", frame_count);
    println!("Frames written: {}", stats.frames_written);
    println!("Messages processed: {}", stats.messages_processed);
    println!("Processing time: {:.2}s", processing_time.as_secs_f64());
    println!(
        "Total time (with finalization): {:.2}s",
        total_time.as_secs_f64()
    );
    println!("Throughput: {:.1} fps", stats.fps);
    println!("Parallel speedup: {:.1}x", stats.parallel_speedup);

    // Calculate total output size (recursively)
    fn calculate_dir_size(path: &std::path::Path) -> u64 {
        let mut total_size = 0u64;
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                if let Ok(meta) = entry.metadata() {
                    if meta.is_dir() {
                        total_size += calculate_dir_size(&entry.path());
                    } else {
                        total_size += meta.len();
                    }
                }
            }
        }
        total_size
    }

    let output_size = calculate_dir_size(output_path);
    println!(
        "Output size: {:.1} MB",
        output_size as f64 / (1024.0 * 1024.0)
    );

    // List output directory structure
    println!("\nOutput directory structure:");
    fn list_dir(path: &std::path::Path, prefix: &str) {
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                let full_path = entry.path();
                if full_path.is_dir() {
                    println!("{}{}/", prefix, name_str);
                    list_dir(&full_path, &format!("{}  ", prefix));
                } else if let Ok(meta) = entry.metadata() {
                    println!(
                        "{}{} ({:.1} KB)",
                        prefix,
                        name_str,
                        meta.len() as f64 / 1024.0
                    );
                }
            }
        }
    }
    list_dir(output_path, "  ");

    println!("\nVideo frame count verification:");
    fn check_videos(path: &std::path::Path) {
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                let full_path = entry.path();
                if full_path.is_dir() {
                    check_videos(&full_path);
                } else if full_path.extension().map(|e| e == "mp4").unwrap_or(false) {
                    let parent = full_path.parent().and_then(|p| p.file_name());
                    let camera_name = parent
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or_default();

                    let output = std::process::Command::new("ffprobe")
                        .args([
                            "-v",
                            "error",
                            "-select_streams",
                            "v:0",
                            "-show_entries",
                            "stream=nb_frames",
                            "-of",
                            "csv=s=x:p=0",
                            full_path.to_str().unwrap(),
                        ])
                        .output();

                    if let Ok(output) = output
                        && output.status.success()
                    {
                        let frame_count =
                            String::from_utf8_lossy(&output.stdout).trim().to_string();
                        println!("  {}: {} frames", camera_name, frame_count);
                    }
                }
            }
        }
    }
    check_videos(output_path);

    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let bag_file = args.get(1).map(|s| s.as_str()).unwrap_or(
        "tests/fixtures/A02-A01-37-45-77-factory_07-P4_210-leju_claw-20260104174020-v001.bag",
    );

    if !Path::new(bag_file).exists() {
        eprintln!("Error: Bag file not found: {}", bag_file);
        eprintln!(
            "Usage: cargo run --release --example benchmark_large_bag <path_to_bag> [output_dir]"
        );
        std::process::exit(1);
    }

    // Use provided output dir or create one that won't be deleted
    let output_dir = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "/tmp/benchmark_large_bag_output".to_string());

    let output_path = std::path::Path::new(&output_dir);

    // Clean up previous run if exists
    if output_path.exists() {
        let _ = std::fs::remove_dir_all(output_path);
    }
    std::fs::create_dir_all(output_path).expect("Failed to create output directory");

    match benchmark_bag_conversion(bag_file, output_path) {
        Ok(_) => {
            println!("\n========================================");
            println!("Benchmark completed successfully!");
            println!("Output preserved at: {}", output_path.display());
            println!("========================================");
        }
        Err(e) => {
            eprintln!("\nBenchmark failed: {}", e);
            std::process::exit(1);
        }
    }
}
