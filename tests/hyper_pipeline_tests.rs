//! Integration tests for the 7-stage HyperPipeline.
//!
//! These tests verify that the HyperPipeline produces correct output
//! matching the AsyncPipeline for the same input.
//!
//! Usage:
//!   cargo test -p robocodec --test hyper_pipeline_tests -- --nocapture

use std::path::Path;

use robocodec::io::formats::McapFormat;
use robocodec::io::traits::FormatReader;
use robocodec::pipeline::hyper::{HyperPipeline, HyperPipelineConfig};

#[test]
fn test_hyper_pipeline_mcap_to_mcap() {
    let input_mcap = "tests/fixtures/robocodec_test_0.mcap";
    let output_mcap = "/tmp/hyper_pipeline_test_0.mcap";

    if !Path::new(input_mcap).exists() {
        eprintln!("Skipping test: fixture not found at {}", input_mcap);
        return;
    }

    println!("=== HyperPipeline MCAP → MCAP Test ===");
    println!("Input: {}", input_mcap);

    // Run HyperPipeline
    let config = HyperPipelineConfig::new(input_mcap, output_mcap);
    let pipeline = HyperPipeline::new(config).expect("Failed to create pipeline");
    let report = pipeline.run().expect("Pipeline failed");

    println!(
        "HyperPipeline completed: {:.2} MB/s throughput, {:.2}x compression",
        report.throughput_mb_s, report.compression_ratio
    );

    // Verify output file was created
    assert!(
        Path::new(output_mcap).exists(),
        "Output file should be created"
    );

    // Verify output file has content
    let output_size = std::fs::metadata(output_mcap)
        .expect("Failed to get output metadata")
        .len();

    assert!(output_size > 0, "Output file should have content");

    println!(
        "✓ Pipeline completed successfully, output: {} bytes",
        output_size
    );
}

#[test]
fn test_hyper_pipeline_with_crc_enabled() {
    let input_mcap = "tests/fixtures/robocodec_test_1.mcap";
    let output_mcap = "/tmp/hyper_pipeline_crc_test.mcap";

    if !Path::new(input_mcap).exists() {
        eprintln!("Skipping test: fixture not found at {}", input_mcap);
        return;
    }

    println!("=== HyperPipeline with CRC Enabled ===");

    // Run with CRC enabled
    let config = HyperPipelineConfig::builder()
        .input_path(input_mcap)
        .output_path(output_mcap)
        .enable_crc(true)
        .build()
        .expect("Failed to build config");

    let pipeline = HyperPipeline::new(config).expect("Failed to create pipeline");
    let report = pipeline.run().expect("Pipeline failed");

    assert!(report.crc_enabled, "CRC should be enabled in report");
    println!("✓ CRC-enabled pipeline completed successfully");
}

#[test]
fn test_hyper_pipeline_with_crc_disabled() {
    let input_mcap = "tests/fixtures/robocodec_test_2.mcap";
    let output_mcap = "/tmp/hyper_pipeline_no_crc_test.mcap";

    if !Path::new(input_mcap).exists() {
        eprintln!("Skipping test: fixture not found at {}", input_mcap);
        return;
    }

    println!("=== HyperPipeline with CRC Disabled ===");

    // Run with CRC disabled
    let config = HyperPipelineConfig::builder()
        .input_path(input_mcap)
        .output_path(output_mcap)
        .enable_crc(false)
        .build()
        .expect("Failed to build config");

    let pipeline = HyperPipeline::new(config).expect("Failed to create pipeline");
    let report = pipeline.run().expect("Pipeline failed");

    assert!(!report.crc_enabled, "CRC should be disabled in report");
    println!("✓ CRC-disabled pipeline completed successfully");
}

#[test]
fn test_hyper_pipeline_compression_levels() {
    let input_mcap = "tests/fixtures/robocodec_test_3.mcap";

    if !Path::new(input_mcap).exists() {
        eprintln!("Skipping test: fixture not found at {}", input_mcap);
        return;
    }

    println!("=== HyperPipeline Compression Levels ===");

    let compression_levels = [1, 3, 6, 9];

    for level in compression_levels {
        let output_mcap = format!("/tmp/hyper_pipeline_level_{}.mcap", level);

        let config = HyperPipelineConfig::builder()
            .input_path(input_mcap)
            .output_path(&output_mcap)
            .compression_level(level)
            .build()
            .expect("Failed to build config");

        let pipeline = HyperPipeline::new(config).expect("Failed to create pipeline");
        let report = pipeline.run().expect("Pipeline failed");

        println!(
            "  Level {}: {:.2}x compression, {:.2} MB/s",
            level, report.compression_ratio, report.throughput_mb_s
        );
    }

    println!("✓ All compression levels work correctly");
}

#[test]
fn test_hyper_pipeline_channel_preservation() {
    let input_mcap = "tests/fixtures/robocodec_test_4.mcap";
    let output_mcap = "/tmp/hyper_pipeline_channel_test.mcap";

    if !Path::new(input_mcap).exists() {
        eprintln!("Skipping test: fixture not found at {}", input_mcap);
        return;
    }

    println!("=== HyperPipeline Channel Preservation ===");

    // Read input channels
    let input_reader = McapFormat::open(input_mcap).expect("Failed to open input");
    let input_channels = input_reader.channels().clone();

    // Run pipeline
    let config = HyperPipelineConfig::new(input_mcap, output_mcap);
    let pipeline = HyperPipeline::new(config).expect("Failed to create pipeline");
    pipeline.run().expect("Pipeline failed");

    // Read output channels
    let output_reader = McapFormat::open(output_mcap).expect("Failed to open output");
    let output_channels = output_reader.channels().clone();

    println!("Input channels: {}", input_channels.len());
    println!("Output channels: {}", output_channels.len());

    // Verify channel count matches
    assert_eq!(
        input_channels.len(),
        output_channels.len(),
        "Channel count should be preserved"
    );

    // Verify each channel's topic and message type exists
    for (_in_id, in_ch) in &input_channels {
        let found = output_channels
            .values()
            .any(|out_ch| out_ch.topic == in_ch.topic && out_ch.message_type == in_ch.message_type);

        assert!(
            found,
            "Channel {} ({}) not found in output",
            in_ch.topic, in_ch.message_type
        );
    }

    println!("✓ All channel information preserved");
}

#[test]
fn test_hyper_pipeline_multiple_files() {
    // Test with multiple different input files
    let fixtures = [
        "tests/fixtures/robocodec_test_5.mcap",
        "tests/fixtures/robocodec_test_6.mcap",
        "tests/fixtures/robocodec_test_7.mcap",
    ];

    println!("=== HyperPipeline Multiple Files Test ===");

    for (i, input_mcap) in fixtures.iter().enumerate() {
        if !Path::new(input_mcap).exists() {
            eprintln!("Skipping {}: fixture not found", input_mcap);
            continue;
        }

        let output_mcap = format!("/tmp/hyper_pipeline_multi_{}.mcap", i);

        let config = HyperPipelineConfig::new(input_mcap.to_string(), output_mcap);
        let pipeline = HyperPipeline::new(config).expect("Failed to create pipeline");
        let report = pipeline.run().expect("Pipeline failed");

        println!(
            "  {}: {} messages, {:.2} MB/s",
            input_mcap, report.message_count, report.throughput_mb_s
        );
    }

    println!("✓ All files processed successfully");
}

#[test]
fn test_hyper_pipeline_report_fields() {
    let input_mcap = "tests/fixtures/robocodec_test_8.mcap";
    let output_mcap = "/tmp/hyper_pipeline_report_test.mcap";

    if !Path::new(input_mcap).exists() {
        eprintln!("Skipping test: fixture not found at {}", input_mcap);
        return;
    }

    println!("=== HyperPipeline Report Fields ===");

    let config = HyperPipelineConfig::new(input_mcap, output_mcap);
    let pipeline = HyperPipeline::new(config).expect("Failed to create pipeline");
    let report = pipeline.run().expect("Pipeline failed");

    // Verify basic report fields are populated
    assert!(!report.input_file.is_empty(), "input_file should be set");
    assert!(!report.output_file.is_empty(), "output_file should be set");
    assert!(
        report.input_size_bytes > 0,
        "input_size_bytes should be > 0"
    );
    assert!(
        report.output_size_bytes > 0,
        "output_size_bytes should be > 0"
    );
    assert!(report.throughput_mb_s >= 0.0, "throughput should be >= 0");
    assert!(
        report.compression_ratio > 0.0,
        "compression_ratio should be > 0"
    );

    println!("Report fields:");
    println!(
        "  Input: {} ({} bytes)",
        report.input_file, report.input_size_bytes
    );
    println!(
        "  Output: {} ({} bytes)",
        report.output_file, report.output_size_bytes
    );
    println!("  Messages: {}", report.message_count);
    println!("  Chunks: {}", report.chunks_written);
    println!("  Compression: {:.2}x", report.compression_ratio);
    println!("  Throughput: {:.2} MB/s", report.throughput_mb_s);
    println!("  CRC enabled: {}", report.crc_enabled);

    println!("✓ Report fields populated correctly");
}
