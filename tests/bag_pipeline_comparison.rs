use std::fs;
use std::path::Path;
use std::time::Instant;

use roboflow::sources::SourceConfig;
use roboflow::{DatasetBaseConfig, LerobotConfig, LerobotWriter, VideoConfig};
use roboflow_pipeline::common::DatasetWriter;
use roboflow_pipeline::lerobot::{FlushingConfig, Mapping, MappingType, StreamingConfig};

const TEST_BAG_PATH: &str =
    "tests/fixtures/A02-A01-37-45-77-factory_07-P4_210-leju_claw-20260104174020-v001.bag";

fn create_test_lerobot_config() -> LerobotConfig {
    LerobotConfig {
        dataset: roboflow::lerobot::DatasetConfig {
            base: DatasetBaseConfig {
                name: "benchmark_test".to_string(),
                fps: 30,
                robot_type: Some("test_robot".to_string()),
            },
            env_type: None,
        },
        mappings: vec![Mapping {
            topic: "/cam_h/color/image_raw/compressed".to_string(),
            feature: "observation.images.cam_high".to_string(),
            mapping_type: MappingType::Image,
            camera_key: Some("cam_high".to_string()),
        }],
        video: VideoConfig {
            codec: "libx264".to_string(),
            crf: 18,
            preset: "fast".to_string(),
            profile: None,
        },
        annotation_file: None,
        flushing: FlushingConfig::default(),
        streaming: StreamingConfig::default(),
    }
}

fn init_sources() {
    roboflow::sources::register_builtin_sources();
}

#[test]
#[ignore = "Requires real bag file - run manually"]
fn test_reading_vs_full_pipeline() {
    init_sources();

    if !Path::new(TEST_BAG_PATH).exists() {
        eprintln!(
            "Skipping benchmark: bag file not found at {}",
            TEST_BAG_PATH
        );
        return;
    }

    let rt = tokio::runtime::Runtime::new().unwrap();

    let input_metadata = fs::metadata(TEST_BAG_PATH).expect("Failed to read input metadata");
    let input_size_mb = input_metadata.len() as f64 / (1024.0 * 1024.0);

    println!("\n{}", "=".repeat(80));
    println!("READING vs FULL PIPELINE (with video encoding)");
    println!("{}", "=".repeat(80));
    println!("Input: {} ({:.2} MB)\n", TEST_BAG_PATH, input_size_mb);

    println!("Test 1: Reading only (no writer)...");
    let read_time = rt.block_on(run_read_only_benchmark());
    println!("  Completed in {:.2}s\n", read_time);

    println!("Test 2: Full pipeline with LerobotWriter (includes video encoding)...");
    let pipeline_time = rt.block_on(run_full_pipeline_benchmark());
    println!("  Completed in {:.2}s\n", pipeline_time);

    let overhead = pipeline_time - read_time;
    let overhead_pct = overhead / pipeline_time * 100.0;

    println!("{}", "=".repeat(80));
    println!("RESULTS:");
    println!("  Reading only:        {:.2}s", read_time);
    println!("  Full pipeline:       {:.2}s", pipeline_time);
    println!(
        "  Writer/encoding:     {:.2}s ({:.1}%)",
        overhead, overhead_pct
    );

    if overhead_pct > 50.0 {
        println!("\n  🔴 Video encoding IS the bottleneck!");
        println!("     Encoding takes {:.1}% of total time", overhead_pct);
    } else {
        println!("\n  🟡 Reading is the bottleneck");
        println!(
            "     Encoding only takes {:.1}% of total time",
            overhead_pct
        );
    }
    println!("{}", "=".repeat(80));
}

async fn run_read_only_benchmark() -> f64 {
    let start = Instant::now();

    let source_config = SourceConfig::bag(TEST_BAG_PATH);
    let mut source =
        roboflow::sources::create_source(&source_config).expect("Failed to create bag source");

    let _metadata = source
        .initialize(&source_config)
        .await
        .expect("Failed to initialize");

    loop {
        match source.read_batch(100).await {
            Ok(Some(_messages)) => {}
            Ok(None) => break,
            Err(_) => break,
        }
    }

    start.elapsed().as_secs_f64()
}

async fn run_full_pipeline_benchmark() -> f64 {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let output_path = temp_dir.path();

    let config = create_test_lerobot_config();

    let mut writer = LerobotWriter::new_local(output_path, config.clone())
        .expect("Failed to create LeRobot writer");

    writer
        .start_episode(Some(0))
        .expect("Failed to start episode");

    let source_config = SourceConfig::bag(TEST_BAG_PATH);
    let mut source =
        roboflow::sources::create_source(&source_config).expect("Failed to create bag source");

    let _metadata = source
        .initialize(&source_config)
        .await
        .expect("Failed to initialize");

    let start = Instant::now();

    loop {
        match source.read_batch(100).await {
            Ok(Some(_messages)) => {}
            Ok(None) => break,
            Err(_) => break,
        }
    }

    writer
        .finish_episode(Some(0))
        .expect("Failed to finish episode");

    let _stats = DatasetWriter::finalize(&mut writer).expect("Failed to finalize writer");

    let elapsed = start.elapsed().as_secs_f64();

    temp_dir.close().expect("Failed to clean up");

    elapsed
}
