// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! SIMD RGB to YUV colorspace conversion benchmarks.
//!
//! Run with: cargo bench --package roboflow-video -- simd_benchmark

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use roboflow_video::simd::{optimal_strategy, rgb_to_nv12, rgb_to_yuv420p};

/// Generate random RGB test data.
fn generate_rgb_data(width: usize, height: usize) -> Vec<u8> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..width * height * 3).map(|_| rng.r#gen::<u8>()).collect()
}

/// Benchmark RGB to YUV420P conversion.
fn bench_rgb_to_yuv420p(c: &mut Criterion) {
    let strategy = optimal_strategy();
    let mut group = c.benchmark_group("rgb_to_yuv420p");

    // Test various image sizes
    let sizes = [
        (640, 480, "640x480 (VGA)"),
        (1280, 720, "1280x720 (HD)"),
        (1920, 1080, "1920x1080 (FHD)"),
    ];

    for (width, height, name) in sizes {
        let rgb_data = generate_rgb_data(width, height);
        group.throughput(Throughput::Bytes(rgb_data.len() as u64));

        group.bench_with_input(
            BenchmarkId::new(name, strategy.name()),
            &rgb_data,
            |b, rgb| {
                b.iter(|| {
                    let result = rgb_to_yuv420p(black_box(rgb), width, height);
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark RGB to NV12 conversion.
fn bench_rgb_to_nv12(c: &mut Criterion) {
    let strategy = optimal_strategy();
    let mut group = c.benchmark_group("rgb_to_nv12");

    // Test various image sizes
    let sizes = [
        (640, 480, "640x480 (VGA)"),
        (1280, 720, "1280x720 (HD)"),
        (1920, 1080, "1920x1080 (FHD)"),
    ];

    for (width, height, name) in sizes {
        let rgb_data = generate_rgb_data(width, height);
        group.throughput(Throughput::Bytes(rgb_data.len() as u64));

        group.bench_with_input(
            BenchmarkId::new(name, strategy.name()),
            &rgb_data,
            |b, rgb| {
                b.iter(|| {
                    let result = rgb_to_nv12(black_box(rgb), width, height);
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark to show throughput in MB/s.
fn bench_throughput(c: &mut Criterion) {
    let strategy = optimal_strategy();
    let mut group = c.benchmark_group("throughput");

    // Use FHD resolution for throughput measurement
    let width = 1920;
    let height = 1080;
    let rgb_data = generate_rgb_data(width, height);

    group.throughput(Throughput::Bytes(rgb_data.len() as u64));

    group.bench_function(
        format!("yuv420p_{}_{}", strategy.name(), "1920x1080"),
        |b| {
            b.iter(|| {
                let result = rgb_to_yuv420p(black_box(&rgb_data), width, height);
                black_box(result)
            });
        },
    );

    group.bench_function(format!("nv12_{}_{}", strategy.name(), "1920x1080"), |b| {
        b.iter(|| {
            let result = rgb_to_nv12(black_box(&rgb_data), width, height);
            black_box(result)
        });
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_plots();
    targets = bench_rgb_to_yuv420p, bench_rgb_to_nv12, bench_throughput
}

criterion_main!(benches);
