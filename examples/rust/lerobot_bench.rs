// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Benchmark: DatasetWriter performance with LeRobot format.
//!
//! This benchmark measures the performance of writing to LeRobot v2.1 format
//! using the DatasetWriter trait with synthetic test data.
//!
//! Usage:
//!   cargo run --release --example lerobot_bench -- --frames 1000 --profile speed
//!
//! Profiles:
//!   - speed: Maximum encoding speed (lowest quality)
//!   - quality: Best quality (slowest encoding)
//!   - balanced: Balanced speed/quality (default)
//!   - storage: Compressed for storage
//!   - prototype: Fastest for prototyping

use std::path::PathBuf;
use std::time::{Duration, Instant};

use roboflow::DatasetWriter;
use roboflow::ImageData;
use roboflow::lerobot::{LerobotConfig, LerobotWriter};
use roboflow_dataset::common::AlignedFrame;

/// Timing breakdown for different operations
#[derive(Debug, Default)]
struct TimingBreakdown {
    frame_generation: Duration,
    frame_write: Duration,
    image_add: Duration,
    episode_finish: Duration,
    finalize: Duration,
    video_encoding: Duration,
    parquet_writing: Duration,
}

impl TimingBreakdown {
    fn print(&self) {
        let total = self.frame_generation
            + self.frame_write
            + self.image_add
            + self.episode_finish
            + self.finalize;

        println!("\n{} Timing Breakdown {}\n", "=".repeat(20), "=".repeat(20));
        println!("  {:<25} {:>10} {:>6}%", "Operation", "Time", "Pct");
        println!("  {}", "-".repeat(45));

        let pct = |d: Duration| (d.as_secs_f64() / total.as_secs_f64() * 100.0).max(0.0);

        println!(
            "  {:<25} {:>10.3}s {:>5.1}%",
            "Frame generation",
            self.frame_generation.as_secs_f64(),
            pct(self.frame_generation)
        );
        println!(
            "  {:<25} {:>10.3}s {:>5.1}%",
            "Frame write",
            self.frame_write.as_secs_f64(),
            pct(self.frame_write)
        );
        println!(
            "  {:<25} {:>10.3}s {:>5.1}%",
            "Image add",
            self.image_add.as_secs_f64(),
            pct(self.image_add)
        );
        println!(
            "  {:<25} {:>10.3}s {:>5.1}%",
            "Video encoding",
            self.video_encoding.as_secs_f64(),
            pct(self.video_encoding)
        );
        println!(
            "  {:<25} {:>10.3}s {:>5.1}%",
            "Parquet writing",
            self.parquet_writing.as_secs_f64(),
            pct(self.parquet_writing)
        );
        println!(
            "  {:<25} {:>10.3}s {:>5.1}%",
            "Episode finish",
            self.episode_finish.as_secs_f64(),
            pct(self.episode_finish)
        );
        println!(
            "  {:<25} {:>10.3}s {:>5.1}%",
            "Finalize",
            self.finalize.as_secs_f64(),
            pct(self.finalize)
        );
        println!("  {}", "-".repeat(45));
        println!("  {:<25} {:>10.3}s", "Total", total.as_secs_f64());
        println!();
    }
}

/// Benchmark configuration
struct BenchConfig {
    /// Output directory
    output_dir: PathBuf,

    /// Number of frames to generate
    num_frames: usize,

    /// Image dimensions (width, height)
    image_size: (u32, u32),

    /// State dimension
    state_dim: usize,

    /// Target FPS
    fps: u32,

    /// Video profile (speed, quality, balanced, storage, prototype)
    profile: Option<String>,
}

/// Benchmark results
#[derive(Debug)]
struct BenchResults {
    /// Frames written
    frames_written: usize,

    /// Images encoded
    images_encoded: usize,

    /// Output bytes
    output_bytes: u64,

    /// Total duration
    total_duration: Duration,

    /// Timing breakdown
    _timing: TimingBreakdown,

    /// Number of cameras
    camera_count: usize,
}

impl BenchResults {
    /// Print results as a table
    fn print(&self) {
        println!(
            "\n{} Benchmark Results {}\n",
            "=".repeat(20),
            "=".repeat(20)
        );
        println!("  {:<25} {:>15}", "Metric", "Value");
        println!("  {}", "-".repeat(42));

        let output_mb = self.output_bytes as f64 / (1024.0 * 1024.0);

        println!("  {:<25} {:>15}", "Frames written", self.frames_written);
        println!("  {:<25} {:>15}", "Images encoded", self.images_encoded);
        println!("  {:<25} {:>15}", "Camera count", self.camera_count);
        println!("  {:<25} {:.2} MB", "Output size", output_mb);

        println!();
        println!(
            "  {:<25} {:>15.3} s",
            "Total duration",
            self.total_duration.as_secs_f64()
        );

        println!();
        let fps = if self.total_duration.as_secs_f64() > 0.0 {
            self.frames_written as f64 / self.total_duration.as_secs_f64()
        } else {
            0.0
        };
        println!("  {:<25} {:>15.1} fps", "Throughput", fps);

        let mb_per_sec = if self.total_duration.as_secs_f64() > 0.0 {
            output_mb / self.total_duration.as_secs_f64()
        } else {
            0.0
        };
        println!("  {:<25} {:>15.2} MB/s", "Write speed", mb_per_sec);

        println!();
        println!("  {}", "=".repeat(42));
        println!();
    }
}

/// A wrapper around LerobotWriter that measures timing
struct TimedLerobotWriter {
    inner: LerobotWriter,
    timing: TimingBreakdown,
}

impl TimedLerobotWriter {
    fn create(inner: LerobotWriter) -> Self {
        Self {
            inner,
            timing: TimingBreakdown::default(),
        }
    }

    fn start_episode(&mut self, task_index: Option<usize>) {
        self.inner.start_episode(task_index);
        // start_episode is very fast, not worth measuring
    }

    fn write_frame(&mut self, frame: &AlignedFrame) -> Result<(), Box<dyn std::error::Error>> {
        let start = Instant::now();
        self.inner.write_frame(frame)?;
        self.timing.frame_write += start.elapsed();
        Ok(())
    }

    fn add_image(&mut self, camera: String, data: ImageData) {
        let start = Instant::now();
        self.inner.add_image(camera, data);
        self.timing.image_add += start.elapsed();
    }

    fn finish_episode(
        &mut self,
        task_index: Option<usize>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start = Instant::now();
        self.inner.finish_episode(task_index)?;
        self.timing.episode_finish += start.elapsed();
        Ok(())
    }

    fn into_inner(self) -> LerobotWriter {
        self.inner
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args()?;
    let config = args;

    println!("DatasetWriter LeRobot Benchmark (with timing breakdown)");
    println!("Output: {:?}", config.output_dir);
    println!("Frames: {}", config.num_frames);
    println!(
        "Image size: {}x{}",
        config.image_size.0, config.image_size.1
    );
    println!("State dim: {}", config.state_dim);
    if let Some(profile) = &config.profile {
        println!("Profile: {}", profile);
    }
    println!();

    // Create LeRobot config
    let lerobot_config = create_lerobot_config(&config)?;

    // Create output directory
    std::fs::create_dir_all(&config.output_dir)?;

    // Create LeRobot writer (already initialized via new_local)
    let mut writer = TimedLerobotWriter::create(LerobotWriter::new_local(
        &config.output_dir,
        lerobot_config.clone(),
    )?);

    let total_start = Instant::now();
    let mut timing = TimingBreakdown::default();

    println!("Generating and writing frames...");

    // Start episode
    writer.start_episode(Some(0));

    // Generate and write frames
    for frame_idx in 0..config.num_frames {
        let frame_start = Instant::now();

        let timestamp_ns = (frame_idx as u64) * 1_000_000_000 / config.fps as u64;

        // Create AlignedFrame
        let mut frame = AlignedFrame::new(frame_idx, timestamp_ns);

        // Add state data
        let state: Vec<f32> = (0..config.state_dim)
            .map(|i| ((i + frame_idx) % 100) as f32 / 100.0)
            .collect();
        frame.add_state("observation.state".to_string(), state.clone());

        // Add action data
        frame.add_action("action".to_string(), state);

        timing.frame_generation += frame_start.elapsed();

        // Write frame using DatasetWriter trait (without images - they're added separately)
        writer.write_frame(&frame)?;

        // Add images separately using add_image (for video encoding)
        let camera_names = vec!["cam_high", "cam_right", "cam_left"];
        for camera_name in &camera_names {
            let (width, height) = config.image_size;
            let data = generate_test_image(width, height, frame_idx);
            let img_data = ImageData::new(width, height, data);
            writer.add_image(camera_name.to_string(), img_data);
        }

        // Progress reporting
        if (frame_idx + 1) % 100 == 0 {
            print!(
                "\r  Frames written: {}/{} ",
                frame_idx + 1,
                config.num_frames
            );
            use std::io::Write;
            std::io::stdout().flush().ok();
        }
    }

    println!(
        "\r  Frames written: {}/{} ",
        config.num_frames, config.num_frames
    );

    // Finish episode
    writer.finish_episode(Some(0))?;

    // Extract timing before finalizing (which consumes the writer)
    let frame_gen_time = writer.timing.frame_generation;
    let frame_write_time = writer.timing.frame_write;
    let image_add_time = writer.timing.image_add;
    let episode_finish_time = writer.timing.episode_finish;

    // Finalize by consuming the writer
    let inner_writer = writer.into_inner();
    let finalize_start = Instant::now();
    let frames_finalized = inner_writer.finalize()?;
    let finalize_time = finalize_start.elapsed();

    let total_duration = total_start.elapsed();

    // Merge timings
    timing.frame_generation = frame_gen_time;
    timing.frame_write = frame_write_time;
    timing.image_add = image_add_time;
    timing.episode_finish = episode_finish_time;
    timing.finalize = finalize_time;

    // Estimate video encoding and parquet writing from episode_finish
    // episode_finish includes both video encoding and parquet writing
    timing.video_encoding = timing.episode_finish / 2;
    timing.parquet_writing = timing.episode_finish / 2;

    // Calculate output size
    let output_bytes = calculate_dir_size(&config.output_dir);

    // Print timing breakdown
    timing.print();

    // Print results
    let results = BenchResults {
        frames_written: frames_finalized,
        images_encoded: 0, // Not directly tracked
        output_bytes,
        total_duration,
        _timing: timing,
        camera_count: 3, // cam_high, cam_right, cam_left
    };

    results.print();

    // TB-scale extrapolation
    println!("TB-scale Extrapolation:");
    let camera_count = 3;
    let frames_per_tb = 1_000_000_000_000
        / (config.image_size.0 * config.image_size.1 * 3 * camera_count as u32) as u64;
    let projected_time_sec = (frames_per_tb as f64 / results.frames_written as f64)
        * results.total_duration.as_secs_f64();
    let projected_hours = projected_time_sec / 3600.0;
    println!(
        "  At 1 TB of image data: {:.2} hours ({:.2} days)",
        projected_hours,
        projected_hours / 24.0
    );
    println!();

    Ok(())
}

/// Parse command line arguments
fn parse_args() -> Result<BenchConfig, Box<dyn std::error::Error>> {
    let mut output_dir = None;
    let mut num_frames = 100;
    let mut image_size = (640, 480);
    let mut state_dim = 7;
    let mut fps = 30;
    let mut profile = None;

    let mut args = std::env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--output" | "-o" => {
                output_dir = Some(args.next().ok_or("Missing --output value")?.into());
            }
            "--frames" | "-f" => {
                num_frames = args.next().ok_or("Missing --frames value")?.parse()?;
            }
            "--image-size" => {
                let size = args.next().ok_or("Missing --image-size value")?;
                let parts: Vec<u32> = size
                    .split('x')
                    .map(|s| s.parse())
                    .collect::<Result<Vec<_>, _>>()?;
                if parts.len() == 2 {
                    image_size = (parts[0], parts[1]);
                }
            }
            "--state-dim" => {
                state_dim = args.next().ok_or("Missing --state-dim value")?.parse()?;
            }
            "--fps" => {
                fps = args.next().ok_or("Missing --fps value")?.parse()?;
            }
            "--profile" | "-p" => {
                profile = Some(args.next().ok_or("Missing --profile value")?);
            }
            _ => {
                return Err(format!("Unknown argument: {}", arg).into());
            }
        }
    }

    Ok(BenchConfig {
        output_dir: output_dir.unwrap_or_else(|| PathBuf::from("/tmp/lerobot_bench_output")),
        num_frames,
        image_size,
        state_dim,
        fps,
        profile,
    })
}

/// Create LeRobot configuration for the benchmark
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
name = "benchmark_dataset"
fps = {}

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
        config.fps, profile_line
    );

    Ok(toml::from_str(&toml)?)
}

/// Generate test image data (gradient pattern)
fn generate_test_image(width: u32, height: u32, frame_idx: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity((width * height * 3) as usize);
    for y in 0..height {
        for x in 0..width {
            let r = ((x * 255 / width) as u8).wrapping_add(frame_idx as u8);
            let g = ((y * 255 / height) as u8).wrapping_add(frame_idx as u8);
            let b = ((x * y / width / height * 255) as u8).wrapping_add(frame_idx as u8);
            data.push(r);
            data.push(g);
            data.push(b);
        }
    }
    data
}

/// Calculate total size of a directory recursively
fn calculate_dir_size(path: &std::path::Path) -> u64 {
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
