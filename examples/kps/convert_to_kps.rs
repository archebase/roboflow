//! Example: Convert MCAP to Kps dataset format using robocodec Rust API.
//!
//! This example demonstrates how to use robocodec's new streaming Kps pipeline
//! to convert robotics data from MCAP files to the Kps dataset format.
//!
//! # Usage
//!
//! ```bash
//! # Parquet + MP4 format (v3.0)
//! cargo run --example convert_to_kps --features kps-parquet -- \
//!     input.mcap output_dir kps_config.toml
//!
//! # HDF5 format (legacy v1.2)
//! cargo run --example convert_to_kps --features kps-hdf5 -- \
//!     input.mcap output_dir kps_config.toml
//! ```
//!
//! # Features
//!
//! - Time alignment with configurable strategies (linear, hold-last, nearest-neighbor)
//! - Camera parameter extraction from CameraInfo and TF messages
//! - MP4 video encoding via ffmpeg (with graceful fallback)
//! - Streaming pipeline for memory efficiency

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 4 {
        eprintln!("Usage: {} <input.mcap> <output_dir> <config.toml>", args[0]);
        eprintln!();
        eprintln!("Example:");
        eprintln!("  {} input.mcap ./output kps_config.toml", args[0]);
        eprintln!();
        eprintln!("Environment variables:");
        eprintln!("  ROBOCODEC_CAMERA_TOPICS   Comma-separated camera mappings (e.g., hand_high:/camera/high)");
        eprintln!("  ROBOCODEC_PARENT_FRAME     Parent frame for camera extrinsics (default: base_link)");
        std::process::exit(1);
    }

    let input_path = &args[1];
    let output_dir = Path::new(&args[2]);
    let config_path = &args[3];

    // Load configuration
    let config_content = fs::read_to_string(config_path)?;
    let config: robocodec::io::kps::KpsConfig =
        toml::from_str(&config_content)?;

    println!("Converting MCAP to Kps dataset");
    println!("  Input: {}", input_path);
    println!("  Output: {}", output_dir.display());
    println!("  Dataset: {}", config.dataset.name);
    println!("  FPS: {}", config.dataset.fps);

    // Build pipeline configuration with optional camera extraction
    let pipeline_config = build_pipeline_config(&config);

    // Create and run the pipeline
    let pipeline = robocodec::pipeline::kps::KpsPipeline::new(
        input_path,
        output_dir,
        pipeline_config,
    )?;

    let report = pipeline.run?;

    println!("\n=== Conversion Complete ===");
    println!("  Frames written: {}", report.frames_written);
    println!("  Images encoded: {}", report.images_encoded);
    println!("  State records: {}", report.state_records);
    println!("  Duration: {:.2}s", report.duration_sec);
    println!("  Output: {}", report.output_dir);

    Ok(())
}

/// Build pipeline configuration from Kps config and environment variables.
fn build_pipeline_config(
    config: &robocodec::io::kps::KpsConfig,
) -> robocodec::pipeline::kps::KpsPipelineConfig {
    use robocodec::pipeline::kps::{
        CameraExtractorConfig, KpsPipelineConfig, TimeAlignerConfig,
        TimeAlignmentStrategyType,
    };

    // Parse camera topics from environment
    let camera_topics = parse_camera_topics_from_env();
    let camera_enabled = !camera_topics.is_empty();

    let mut time_aligner = TimeAlignerConfig {
        target_fps: config.dataset.fps,
        ..Default::default()
    };

    // Set time alignment strategy from environment if specified
    if let Ok(strategy_str) = std::env::var("ROBOCODETime_ALIGNMENT_STRATEGY") {
        time_aligner.strategy = match strategy_str.as_str() {
            "linear" => TimeAlignmentStrategyType::LinearInterpolation,
            "hold" => TimeAlignmentStrategyType::HoldLastValue,
            "nearest" => TimeAlignmentStrategyType::NearestNeighbor,
            _ => {
                eprintln!("Unknown strategy '{}', using linear", strategy_str);
                TimeAlignmentStrategyType::LinearInterpolation
            }
        };
    }

    KpsPipelineConfig {
        kps_config: config.clone(),
        time_aligner,
        camera_extractor: CameraExtractorConfig {
            enabled: camera_enabled,
            camera_topics,
            parent_frame: std::env::var("ROBOCODET_PARENT_FRAME")
                .unwrap_or_else(|_| "base_link".to_string()),
            camera_info_suffix: std::env::var("ROBOCODET_CAMERA_INFO_SUFFIX")
                .unwrap_or_else(|_| "/camera_info".to_string()),
            tf_topic: std::env::var("ROBOCODET_TF_TOPIC")
                .unwrap_or_else(|_| "/tf".to_string()),
        },
        channel_capacity: 16,
    }
}

/// Parse camera topic mappings from environment variable.
///
/// Format: "camera_name:/camera/topic,another_name:/another/topic"
fn parse_camera_topics_from_env() -> HashMap<String, String> {
    let mut topics = HashMap::new();

    if let Ok(env_str) = std::env::var("ROBOCODET_CAMERA_TOPICS") {
        for mapping in env_str.split(',') {
            let parts: Vec<&str> = mapping.splitn(2, ':').collect();
            if parts.len() == 2 {
                topics.insert(parts[0].trim().to_string(), parts[1].trim().to_string());
                println!("  Camera mapping: {} -> {}", parts[0].trim(), parts[1].trim());
            }
        }
    }

    topics
}

/// Example: Create a minimal Kps config programmatically.
#[allow(dead_code)]
fn create_example_config() -> robocodec::io::kps::KpsConfig {
    use robocodec::io::kps::{
        DatasetConfig, ImageFormat, KpsConfig, Mapping, MappingType, OutputConfig,
        OutputFormat,
    };

    KpsConfig {
        dataset: DatasetConfig {
            name: "my_dataset".to_string(),
            fps: 30,
            robot_type: Some("my_robot".to_string()),
        },
        mappings: vec![
            // Camera images
            Mapping {
                topic: "/camera/high/image_raw".to_string(),
                feature: "observation.camera_high".to_string(),
                mapping_type: MappingType::Image,
            },
            Mapping {
                topic: "/camera/wrist/image_raw".to_string(),
                feature: "observation.camera_wrist".to_string(),
                mapping_type: MappingType::Image,
            },
            // Joint states
            Mapping {
                topic: "/joint_states".to_string(),
                feature: "observation.joint_state".to_string(),
                mapping_type: MappingType::State,
            },
            // Actions
            Mapping {
                topic: "/arm_controller/command".to_string(),
                feature: "action.arm_command".to_string(),
                mapping_type: MappingType::Action,
            },
        ],
        output: OutputConfig {
            formats: vec![OutputFormat::Parquet],
            image_format: ImageFormat::Mp4,
            max_frames: None,
        },
    }
}

/// Example: Write a config file to disk.
#[allow(dead_code)]
fn write_example_config(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let config = create_example_config();
    let toml_string = toml::to_string_pretty(&config)?;

    fs::write(path, toml_string)?;
    println!("Wrote example config to {}", path.display());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_example_config() {
        let config = create_example_config();
        assert_eq!(config.dataset.name, "my_dataset");
        assert_eq!(config.dataset.fps, 30);
        assert!(!config.mappings.is_empty());
    }

    #[test]
    fn test_parse_camera_topics_from_env() {
        // Test with valid input
        let input = "hand_high:/camera/high,hand_low:/camera/low";
        std::env::set_var("ROBOCODET_CAMERA_TOPICS", input);

        let topics = parse_camera_topics_from_env();
        assert_eq!(topics.len(), 2);
        assert_eq!(topics.get("hand_high"), Some(&"/camera/high".to_string()));
        assert_eq!(topics.get("hand_low"), Some(&"/camera/low".to_string()));

        // Clean up
        std::env::remove_var("ROBOCODET_CAMERA_TOPICS");
    }

    #[test]
    fn test_parse_camera_topics_empty() {
        std::env::remove_var("ROBOCODET_CAMERA_TOPICS");

        let topics = parse_camera_topics_from_env();
        assert!(topics.is_empty());
    }

    #[test]
    fn test_build_pipeline_config() {
        let config = create_example_config();
        let pipeline_config = build_pipeline_config(&config);

        assert_eq!(pipeline_config.time_aligner.target_fps, 30);
        assert_eq!(pipeline_config.channel_capacity, 16);
    }
}
