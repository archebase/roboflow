// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Benchmarks for roboflow-dataset.
//!
//! Run with: cargo bench

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use roboflow_dataset::core::traits::FormatWriter;
use roboflow_dataset::testing::{FrameBuilder, InMemoryWriter, MessageBuilder, generate_test_frames};

fn bench_frame_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame_creation");

    group.bench_function("minimal_frame", |b| {
        b.iter(|| FrameBuilder::new(black_box(0)).build())
    });

    group.bench_function("frame_with_state", |b| {
        b.iter(|| {
            FrameBuilder::new(black_box(0))
                .add_state("observation.state", vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0])
                .build()
        })
    });

    group.bench_function("frame_with_image", |b| {
        b.iter(|| {
            FrameBuilder::new(black_box(0))
                .add_image("observation.camera", 640, 480)
                .build()
        })
    });

    group.bench_function("frame_with_encoded_image", |b| {
        b.iter(|| {
            FrameBuilder::new(black_box(0))
                .add_encoded_image("observation.camera", 640, 480)
                .build()
        })
    });

    group.bench_function("complete_frame", |b| {
        b.iter(|| {
            FrameBuilder::new(black_box(0))
                .add_state("observation.state", vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0])
                .add_action("action", vec![0.5, -0.5, 0.0])
                .add_encoded_image("observation.camera_0", 640, 480)
                .add_encoded_image("observation.camera_1", 640, 480)
                .build()
        })
    });

    group.finish();
}

fn bench_writer_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("writer_throughput");

    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::new("write_frames", size), size, |b, &size| {
            let frames: Vec<_> = (0..size)
                .map(|i| {
                    FrameBuilder::new(i)
                        .add_state("observation.state", vec![i as f32])
                        .build()
                })
                .collect();

            b.iter(|| {
                let mut writer = InMemoryWriter::new();
                for frame in &frames {
                    writer.write_frame(frame).unwrap();
                }
                writer.finalize().unwrap();
                black_box(writer)
            })
        });
    }

    group.finish();
}

fn bench_message_building(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_building");

    group.bench_function("minimal_message", |b| {
        b.iter(|| MessageBuilder::new("/test").build())
    });

    group.bench_function("image_message", |b| {
        b.iter(|| {
            MessageBuilder::new("/camera/image")
                .with_timestamp(black_box(1_000_000_000))
                .image(640, 480)
                .build()
        })
    });

    group.bench_function("state_message", |b| {
        b.iter(|| {
            MessageBuilder::new("/state")
                .with_timestamp(black_box(1_000_000_000))
                .float_array(vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0])
                .build()
        })
    });

    group.finish();
}

fn bench_generate_test_frames(c: &mut Criterion) {
    let mut group = c.benchmark_group("test_generation");

    group.bench_function("generate_100_frames", |b| {
        b.iter(|| {
            let frames = generate_test_frames(100, 640, 480);
            black_box(frames)
        })
    });

    group.bench_function("generate_1000_frames", |b| {
        b.iter(|| {
            let frames = generate_test_frames(1000, 640, 480);
            black_box(frames)
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_frame_creation,
    bench_writer_throughput,
    bench_message_building,
    bench_generate_test_frames,
);

criterion_main!(benches);
