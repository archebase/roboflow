// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Stress test for profiling with flamegraph.
//!
//! Run with: cargo flamegraph --example stress_test --package roboflow-video --release
//! Or with samply: cargo build --release --example stress_test -p roboflow-video && samply record ./target/release/examples/stress_test

use std::time::{Duration, Instant};

use roboflow_video::{
    FragmentEncoderConfig, ImageData, select_best_encoder,
};
use roboflow_video::decode::{DecodePool, DecodePoolConfig, DecodedFrame};
use roboflow_video::encoder_pool::{EncodeCommand, EncoderPool, EncoderPoolConfig};
use roboflow_video::test_utils::{BENCHMARK_CAMERAS, create_synthetic_jpeg, hardware_video_config};

fn main() {
    // Initialize tracing for debug output
    tracing_subscriber::fmt::init();

    println!("=== Pipeline Stress Test ===");
    println!("CPU cores: {}", num_cpus::get());

    let best = select_best_encoder();
    println!(
        "Encoder: {} ({}x speedup)",
        best.name(),
        best.speedup_factor()
    );

    // Run multiple iterations for profiling (5 seconds worth)
    let iterations = 5;
    println!("Running {} iterations for profiling...\n", iterations);

    let width = 640u32;
    let height = 480u32;
    let jpeg_data = create_synthetic_jpeg(width, height);
    println!("JPEG size: {} bytes", jpeg_data.len());

    let cpu_count = num_cpus::get();
    let video_config = hardware_video_config();

    // OPTIMAL: Split CPU cores between decode and encode based on workload
    // Decode: frame-level parallelism, JPEG is CPU-intensive
    // Encode: fragment-level parallelism, VideoToolbox is hardware-accelerated
    let decode_workers = 5;
    let encode_workers = 4;
    println!(
        "\n=== Testing: {} decode + {} encode workers ({} total CPUs) ===",
        decode_workers, encode_workers, cpu_count
    );

    let decode_config = DecodePoolConfig {
        worker_count: decode_workers,
        pending_capacity: 1024,
        completed_capacity: 1024,
        ..Default::default()
    };

    let encode_config = EncoderPoolConfig {
        worker_count: encode_workers,
        pending_capacity: 512,
        completed_capacity: 512,
        fragment_config: FragmentEncoderConfig {
            video: video_config.clone(),
            camera_id: "cam0".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };

    println!("\nCreating pools...");
    let decode_pool = DecodePool::new(decode_config).expect("Decode pool");
    let encode_pool = EncoderPool::new(encode_config).expect("Encode pool");

    // 4 cameras, 250 frames each = 1000 total frames
    let frames_per_camera = 250u64;
    let total_frames = frames_per_camera * BENCHMARK_CAMERAS.len() as u64;
    let frames_per_fragment = 30usize;

    let overall_start = Instant::now();
    let mut all_iteration_times = Vec::new();

    for iter in 0..iterations {
        println!("\n--- Iteration {}/{} ---", iter + 1, iterations);
        println!(
            "Processing {} frames from {} cameras...",
            total_frames,
            BENCHMARK_CAMERAS.len()
        );
        let start = Instant::now();

        // Phase 1: Submit all frames for all cameras (non-blocking)
        for _ in 0..frames_per_camera {
            for cam in BENCHMARK_CAMERAS {
                let image = ImageData::encoded(width, height, jpeg_data.clone());
                decode_pool
                    .submit(cam.to_string(), image)
                    .expect("Submit failed");
            }
        }
        let submit_time = start.elapsed();
        println!("Submitted {} frames in {:?}", total_frames, submit_time);

        // Phase 2: Collect and encode per camera
        let mut camera_buffers: std::collections::HashMap<
            String,
            Vec<roboflow_video::DecodedFrame>,
        > = std::collections::HashMap::new();
        for cam in BENCHMARK_CAMERAS {
            camera_buffers.insert(
                cam.to_string(),
                Vec::with_capacity(frames_per_camera as usize),
            );
        }

        let mut decoded = 0u64;
        let mut fragments_submitted = 0u64;
        let mut seq: u64 = 0;

        let decode_start = Instant::now();
        while decoded < total_frames {
            if let Some(result) = decode_pool.try_recv() {
                decoded += 1;
                if let Ok(Some(frame)) = result.result {
                    let cam = &result.camera_id;
                    if let Some(buffer) = camera_buffers.get_mut(cam) {
                        buffer.push(frame);

                        // Submit fragment when buffer is full
                        if buffer.len() >= frames_per_fragment {
                            let frames: Vec<_> = buffer.drain(..frames_per_fragment).collect();
                            let cmd =
                                EncodeCommand::from_decoded(seq, cam.clone(), frames, seq as u32);
                            encode_pool.submit(cmd).expect("Encode submit failed");
                            fragments_submitted += 1;
                            seq += 1;
                        }
                    }
                }
            } else {
                std::thread::sleep(Duration::from_micros(1));
            }
        }
        let decode_time = decode_start.elapsed();
        println!("Decoded {} frames in {:?}", decoded, decode_time);

        // Submit remaining frames for each camera
        for (cam, buffer) in camera_buffers.iter_mut() {
            if !buffer.is_empty() {
                let cmd = EncodeCommand::from_decoded(
                    seq,
                    cam.clone(),
                    std::mem::take(buffer),
                    seq as u32,
                );
                encode_pool.submit(cmd).expect("Encode submit failed");
                fragments_submitted += 1;
                seq += 1;
            }
        }
        println!("Submitted {} fragments for encoding", fragments_submitted);

        // Phase 3: Wait for all encodes
        let encode_start = Instant::now();
        let mut encoded = 0u64;
        while encoded < fragments_submitted {
            if encode_pool.try_recv().is_some() {
                encoded += 1;
            } else {
                std::thread::sleep(Duration::from_micros(10));
            }
        }
        let encode_time = encode_start.elapsed();
        println!("Encoded {} fragments in {:?}", encoded, encode_time);

        let iter_time = start.elapsed();
        all_iteration_times.push(iter_time);
        println!(
            "Iteration throughput: {:.2} frames/sec",
            total_frames as f64 / iter_time.as_secs_f64()
        );
    }

    let overall_time = overall_start.elapsed();

    println!("\n=== Overall Results ===");
    println!("Total iterations: {}", iterations);
    println!(
        "Total frames processed: {}",
        total_frames * iterations as u64
    );
    println!("Total time: {:?}", overall_time);
    println!(
        "Overall throughput: {:.2} frames/sec",
        (total_frames * iterations as u64) as f64 / overall_time.as_secs_f64()
    );

    // Show per-iteration breakdown
    println!("\n=== Per-Iteration Breakdown ===");
    for (i, time) in all_iteration_times.iter().enumerate() {
        println!(
            "Iteration {}: {:?} ({:.2} fps)",
            i + 1,
            time,
            total_frames as f64 / time.as_secs_f64()
        );
    }

    decode_pool.shutdown();
    encode_pool.shutdown();
    println!("\nDone!");
}
