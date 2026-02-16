// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! End-to-end pipeline benchmark.
//!
//! Run with: cargo bench --package roboflow-video -- pipeline_benchmark

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::time::Duration;

use roboflow_video::{FragmentEncoderConfig, ImageData};
use roboflow_video::decode::{DecodePool, DecodePoolConfig, DecodedFrame};
use roboflow_video::encoder_pool::{EncodeCommand, EncoderPool, EncoderPoolConfig};
use roboflow_video::test_utils::{
    create_synthetic_jpeg, create_decoded_frame, optimal_worker_count, hardware_video_config,
    BENCHMARK_CAMERAS,
};

// ================================================================================================
// Benchmark Configuration
// ================================================================================================

/// Sample size for benchmarks (reduced from default 100 for faster iteration)
const BENCH_SAMPLE_SIZE: usize = 10;

/// Warmup time for benchmarks
const WARMUP_MS: u64 = 500;

/// Measurement time for benchmarks
const MEASURE_TIME_SECS: u64 = 3;

// ================================================================================================
// Decode Pool Benchmark
// ================================================================================================

fn bench_decode_pool(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode_pool");
    group.sample_size(BENCH_SAMPLE_SIZE);

    let sizes = [(640, 480, "640x480 (VGA)"), (1280, 720, "1280x720 (HD)")];

    for (width, height, name) in sizes {
        let jpeg_data = create_synthetic_jpeg(width, height);
        group.throughput(Throughput::Bytes(jpeg_data.len() as u64));

        let config = DecodePoolConfig {
            worker_count: optimal_worker_count(),
            pending_capacity: 256,
            completed_capacity: 256,
            ..Default::default()
        };

        let pool = DecodePool::new(config.clone()).expect("Failed to create decode pool");

        group.bench_with_input(
            BenchmarkId::new("decode_jpeg", name),
            &jpeg_data,
            |b, jpeg: &Vec<u8>| {
                b.iter(|| {
                    let image = ImageData::encoded(width, height, jpeg.to_vec());
                    pool.submit("cam0".to_string(), image)
                        .expect("Submit failed");
                    while pool.try_recv().is_none() {
                        std::thread::sleep(Duration::from_micros(10));
                    }
                });
            },
        );

        pool.shutdown();
    }

    group.finish();
}

// ================================================================================================
// Encoder Pool Benchmark
// ================================================================================================

fn bench_encoder_pool(c: &mut Criterion) {
    let mut group = c.benchmark_group("encoder_pool");
    group.sample_size(BENCH_SAMPLE_SIZE);

    let sizes = [(640, 480, "640x480 (VGA)"), (1280, 720, "1280x720 (HD)")];
    let worker_count = optimal_worker_count().min(4);
    let video_config = hardware_video_config();

    for (width, height, name) in sizes {
        let rgb_size = (width * height * 3) as usize;
        group.throughput(Throughput::Bytes(rgb_size as u64));

        let config = EncoderPoolConfig {
            worker_count,
            pending_capacity: 64,
            completed_capacity: 64,
            fragment_config: FragmentEncoderConfig {
                video: video_config.clone(),
                camera_id: "cam0".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        let pool = EncoderPool::new(config.clone()).expect("Failed to create encoder pool");
        let mut fragment_counter = 0u64;

        group.bench_with_input(
            BenchmarkId::new("encode_frames", name),
            &(width, height),
            |b, dims| {
                let (width, height) = *dims;
                b.iter(|| {
                    let frames: Vec<_> = (0..30)
                        .map(|_| create_decoded_frame(width, height))
                        .collect();

                    let seq = fragment_counter;
                    fragment_counter += 1;

                    let cmd = EncodeCommand::from_decoded(
                        seq,
                        "cam0".to_string(),
                        frames,
                        seq as u32,
                    );
                    pool.submit(cmd).expect("Submit failed");

                    while pool.try_recv().is_none() {
                        std::thread::sleep(Duration::from_micros(100));
                    }
                });
            },
        );

        pool.shutdown();
    }

    group.finish();
}

// ================================================================================================
// End-to-End Pipeline Benchmark
// ================================================================================================

fn bench_e2e_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_pipeline");
    group.sample_size(BENCH_SAMPLE_SIZE);

    let width = 640;
    let height = 480;
    let jpeg_data = create_synthetic_jpeg(width, height);
    let jpeg_size = jpeg_data.len() as u64;

    group.throughput(Throughput::Bytes(jpeg_size * 100));

    let cpu_count = optimal_worker_count();
    let video_config = hardware_video_config();

    let decode_config = DecodePoolConfig {
        worker_count: cpu_count,
        pending_capacity: 512,
        completed_capacity: 512,
        ..Default::default()
    };

    let encode_config = EncoderPoolConfig {
        worker_count: cpu_count,
        pending_capacity: 256,
        completed_capacity: 256,
        fragment_config: FragmentEncoderConfig {
            video: video_config.clone(),
            camera_id: "cam0".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };

    let decode_pool = DecodePool::new(decode_config.clone()).expect("Decode pool");
    let encode_pool = EncoderPool::new(encode_config.clone()).expect("Encode pool");

    group.bench_function("pipelined_100frames_batch30", |b| {
        b.iter(|| {
            let frame_count = 100u64;
            let frames_per_fragment = 30usize;

            // Phase 1: Submit all frames for decoding (non-blocking)
            for _ in 0..frame_count {
                let image = ImageData::encoded(width, height, jpeg_data.clone());
                decode_pool
                    .submit("cam0".to_string(), image)
                    .expect("Submit failed");
            }

            // Phase 2: Collect decoded frames and batch for encoding
            let mut frame_buffer: Vec<DecodedFrame> =
                Vec::with_capacity(frame_count as usize);
            let mut decoded = 0u64;
            let mut fragments_submitted = 0u64;
            let mut seq: u64 = 0;

            while decoded < frame_count {
                if let Some(result) = decode_pool.try_recv() {
                    decoded += 1;
                    if let Ok(Some(frame)) = result.result {
                        frame_buffer.push(frame);

                        if frame_buffer.len() >= frames_per_fragment {
                            let frames: Vec<_> =
                                frame_buffer.drain(..frames_per_fragment).collect();
                            let cmd = EncodeCommand::from_decoded(
                                seq,
                                "cam0".to_string(),
                                frames,
                                seq as u32,
                            );
                            encode_pool.submit(cmd).expect("Encode submit failed");
                            fragments_submitted += 1;
                            seq += 1;
                        }
                    }
                } else {
                    std::thread::sleep(Duration::from_micros(1));
                }
            }

            // Submit remaining frames as final fragment
            if !frame_buffer.is_empty() {
                let cmd = EncodeCommand::from_decoded(
                    seq,
                    "cam0".to_string(),
                    frame_buffer,
                    seq as u32,
                );
                encode_pool.submit(cmd).expect("Encode submit failed");
                fragments_submitted += 1;
            }

            // Phase 3: Wait for all encodes to complete
            let mut encoded = 0u64;
            while encoded < fragments_submitted {
                if encode_pool.try_recv().is_some() {
                    encoded += 1;
                } else {
                    std::thread::sleep(Duration::from_micros(10));
                }
            }
        });
    });

    decode_pool.shutdown();
    encode_pool.shutdown();
    group.finish();
}

// ================================================================================================
// Throughput Summary Benchmark
// ================================================================================================

fn bench_throughput_summary(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput_summary");
    group.sample_size(BENCH_SAMPLE_SIZE);

    let width = 640u32;
    let height = 480u32;
    let jpeg_data = create_synthetic_jpeg(width, height);

    let cpu_count = optimal_worker_count();
    let config = DecodePoolConfig {
        worker_count: cpu_count,
        pending_capacity: 512,
        completed_capacity: 512,
        ..Default::default()
    };

    let pool = DecodePool::new(config.clone()).expect("Decode pool");

    group.bench_function("decode_frame", |b| {
        b.iter(|| {
            let image = ImageData::encoded(width, height, jpeg_data.clone());
            pool.submit("cam0".to_string(), image).expect("Submit");
            while pool.try_recv().is_none() {
                std::thread::sleep(Duration::from_micros(10));
            }
        });
    });

    pool.shutdown();
    group.finish();
}

// ================================================================================================
// Multi-Camera Benchmark
// ================================================================================================

fn bench_multi_camera(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_camera");
    group.sample_size(BENCH_SAMPLE_SIZE);

    let width = 640u32;
    let height = 480u32;
    let jpeg_data = create_synthetic_jpeg(width, height);
    let cameras = BENCHMARK_CAMERAS;

    let cpu_count = optimal_worker_count();
    let config = DecodePoolConfig {
        worker_count: cpu_count,
        pending_capacity: 512,
        completed_capacity: 512,
        ..Default::default()
    };

    let pool = DecodePool::new(config.clone()).expect("Decode pool");
    let mut camera_idx = 0usize;

    group.bench_function("round_robin_4cameras", |b| {
        b.iter(|| {
            let cam = cameras[camera_idx % cameras.len()];
            camera_idx += 1;

            let image = ImageData::encoded(width, height, jpeg_data.clone());
            pool.submit(cam.to_string(), image).expect("Submit");

            while pool.try_recv().is_none() {
                std::thread::sleep(Duration::from_micros(10));
            }
        });
    });

    pool.shutdown();
    group.finish();
}

// ================================================================================================
// Stress Test - Maximum CPU/GPU Utilization
// ================================================================================================

fn bench_stress_test(c: &mut Criterion) {
    let mut group = c.benchmark_group("stress_test");
    group.sample_size(BENCH_SAMPLE_SIZE);

    let width = 640u32;
    let height = 480u32;
    let jpeg_data = create_synthetic_jpeg(width, height);
    let jpeg_size = jpeg_data.len() as u64;

    group.throughput(Throughput::Bytes(jpeg_size * 1000));

    let cpu_count = optimal_worker_count();
    let video_config = hardware_video_config();
    let cameras: [&str; 4] = BENCHMARK_CAMERAS.try_into().unwrap();

    let decode_config = DecodePoolConfig {
        worker_count: cpu_count,
        pending_capacity: 1024,
        completed_capacity: 1024,
        ..Default::default()
    };

    let encode_config = EncoderPoolConfig {
        worker_count: cpu_count,
        pending_capacity: 512,
        completed_capacity: 512,
        fragment_config: FragmentEncoderConfig {
            video: video_config.clone(),
            camera_id: "cam0".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };

    let decode_pool = DecodePool::new(decode_config.clone()).expect("Decode pool");
    let encode_pool = EncoderPool::new(encode_config.clone()).expect("Encode pool");

    let frames_per_camera = 250u64;
    let total_frames = frames_per_camera * cameras.len() as u64;
    let frames_per_fragment = 30usize;

    group.bench_function("4cameras_1000frames_full_pipeline", |b| {
        b.iter(|| {
            // Phase 1: Submit all frames for all cameras (non-blocking)
            for _ in 0..frames_per_camera {
                for &cam in &cameras {
                    let image = ImageData::encoded(width, height, jpeg_data.clone());
                    decode_pool
                        .submit(cam.to_string(), image)
                        .expect("Submit failed");
                }
            }

            // Phase 2: Collect and encode per camera
            let mut camera_buffers: std::collections::HashMap<
                String,
                Vec<DecodedFrame>,
            > = std::collections::HashMap::new();
            for cam in cameras.iter() {
                camera_buffers.insert(
                    cam.to_string(),
                    Vec::with_capacity(frames_per_camera as usize),
                );
            }

            let mut decoded = 0u64;
            let mut fragments_submitted = 0u64;
            let mut seq: u64 = 0;

            while decoded < total_frames {
                if let Some(result) = decode_pool.try_recv() {
                    decoded += 1;
                    if let Ok(Some(frame)) = result.result {
                        let cam = &result.camera_id;
                        if let Some(buffer) = camera_buffers.get_mut(cam) {
                            buffer.push(frame);

                            if buffer.len() >= frames_per_fragment {
                                let frames: Vec<_> = buffer.drain(..frames_per_fragment).collect();
                                let cmd = EncodeCommand::from_decoded(
                                    seq,
                                    cam.clone(),
                                    frames,
                                    seq as u32,
                                );
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

            // Phase 3: Wait for all encodes
            let mut encoded = 0u64;
            while encoded < fragments_submitted {
                if encode_pool.try_recv().is_some() {
                    encoded += 1;
                } else {
                    std::thread::sleep(Duration::from_micros(10));
                }
            }
        });
    });

    decode_pool.shutdown();
    encode_pool.shutdown();
    group.finish();
}

// ================================================================================================
// Criterion Configuration
// ================================================================================================

criterion_group! {
    name = benches;
    config = Criterion::default()
        .with_plots()
        .warm_up_time(Duration::from_millis(WARMUP_MS))
        .measurement_time(Duration::from_secs(MEASURE_TIME_SECS))
        .sample_size(BENCH_SAMPLE_SIZE);
    targets = bench_decode_pool, bench_encoder_pool, bench_e2e_pipeline,
              bench_throughput_summary, bench_multi_camera, bench_stress_test
}

criterion_main!(benches);
