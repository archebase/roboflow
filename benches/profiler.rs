// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Benchmark and profiling tool for roboflow optimization.
//!
//! Examples:
//!   # Convert with metrics output
//!   cargo run --release --features profiling --bin profiler -- run -i file.bag -o output.mcap
//!
//!   # Benchmark with warmup and steady-state measurement
//!   cargo run --release --features profiling --bin profiler -- bench -i file.bag -o output.mcap
//!
//!   # Profile run with built-in flamegraph generation
//!   cargo run --release --features profiling --bin profiler -- profile -i file.bag -o output.mcap --profile-output profile
//!
//!   # Use auto-configuration with performance mode
//!   cargo bench --bench profiler --features profiling -- bench -i file.bag -o output.mcap --hyper --mode throughput

use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::{Parser, Subcommand, ValueEnum};
use roboflow::{CompressionPreset, PerformanceMode, Robocodec};
use roboflow_pipeline::{
    auto_config::PipelineAutoConfig,
    fluent::RunOutput,
    hyper::{HyperPipeline, HyperPipelineConfig},
};

#[derive(Parser, Debug)]
#[command(name = "profiler")]
#[command(about = "Benchmark/profiling tool for roboflow optimization")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Single run with metrics
    Run {
        /// Input file path (BAG or MCAP)
        #[arg(short = 'i', long = "input")]
        input: PathBuf,
        /// Output file path (MCAP)
        #[arg(short = 'o', long = "output")]
        output: PathBuf,
        /// Compression preset
        #[arg(short = 'p', long = "preset", default_value = "balanced")]
        preset: PresetArg,
        /// Use HyperPipeline (async staged pipeline)
        #[arg(long = "hyper")]
        hyper: bool,
        /// Performance mode for auto-configuration (requires --hyper)
        #[arg(long = "mode", value_name = "MODE")]
        mode: Option<ModeArg>,
        /// Batch/chunk size in bytes (for HyperPipeline)
        #[arg(long = "batch-size", value_name = "BYTES")]
        batch_size: Option<usize>,
        /// Number of compression threads (for HyperPipeline)
        #[arg(long = "compress-threads", value_name = "NUM")]
        compress_threads: Option<usize>,
    },
    /// Benchmark with warmup and steady-state measurement
    Bench {
        /// Input file path (BAG or MCAP)
        #[arg(short = 'i', long = "input")]
        input: PathBuf,
        /// Output file path (MCAP)
        #[arg(short = 'o', long = "output")]
        output: PathBuf,
        /// Warmup runs (to fill caches, discarded from stats)
        #[arg(short = 'w', long = "warmup", default_value = "2")]
        warmup: usize,
        /// Measured runs (for statistics)
        #[arg(short = 'r', long = "runs", default_value = "10")]
        runs: usize,
        /// Compression preset
        #[arg(short = 'p', long = "preset", default_value = "balanced")]
        preset: PresetArg,
        /// Show individual run times
        #[arg(long = "verbose")]
        verbose: bool,
        /// Use HyperPipeline (async staged pipeline)
        #[arg(long = "hyper")]
        hyper: bool,
        /// Performance mode for auto-configuration (requires --hyper)
        #[arg(long = "mode", value_name = "MODE")]
        mode: Option<ModeArg>,
        /// Batch/chunk size in bytes (for HyperPipeline)
        #[arg(long = "batch-size", value_name = "BYTES")]
        batch_size: Option<usize>,
        /// Number of compression threads (for HyperPipeline)
        #[arg(long = "compress-threads", value_name = "NUM")]
        compress_threads: Option<usize>,
    },
    /// Profile run with built-in flamegraph generation
    #[cfg(feature = "profiling")]
    Profile {
        /// Input file path (BAG or MCAP)
        #[arg(short = 'i', long = "input")]
        input: PathBuf,
        /// Output file path (MCAP)
        #[arg(short = 'o', long = "output")]
        output: PathBuf,
        /// Profile output path (without extension - creates .svg and optionally .pb)
        #[arg(long = "profile-output")]
        profile_output: PathBuf,
        /// Compression preset
        #[arg(short = 'p', long = "preset", default_value = "balanced")]
        preset: PresetArg,
        /// Sampling frequency in Hz (default: 99)
        #[arg(long = "freq", default_value = "99")]
        frequency: i32,
        /// Also save raw protobuf trace
        #[arg(long = "save-trace")]
        save_trace: bool,
        /// Use HyperPipeline (async staged pipeline)
        #[arg(long = "hyper")]
        hyper: bool,
        /// Performance mode for auto-configuration (requires --hyper)
        #[arg(long = "mode", value_name = "MODE")]
        mode: Option<ModeArg>,
        /// Batch/chunk size in bytes (for HyperPipeline)
        #[arg(long = "batch-size", value_name = "BYTES")]
        batch_size: Option<usize>,
        /// Number of compression threads (for HyperPipeline)
        #[arg(long = "compress-threads", value_name = "NUM")]
        compress_threads: Option<usize>,
    },
}

#[derive(ValueEnum, Debug, Clone, Copy)]
enum PresetArg {
    Fast,
    Balanced,
    Slow,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
enum ModeArg {
    /// Aggressive tuning for maximum throughput on beefy machines
    Throughput,
    /// Middle ground between throughput and resource usage
    Balanced,
    /// Conserve memory at the cost of some throughput
    MemoryEfficient,
}

impl ModeArg {
    fn to_mode(self) -> PerformanceMode {
        match self {
            ModeArg::Throughput => PerformanceMode::Throughput,
            ModeArg::Balanced => PerformanceMode::Balanced,
            ModeArg::MemoryEfficient => PerformanceMode::MemoryEfficient,
        }
    }
}

impl PresetArg {
    fn to_preset(self) -> CompressionPreset {
        match self {
            PresetArg::Fast => CompressionPreset::Fast,
            PresetArg::Balanced => CompressionPreset::Balanced,
            PresetArg::Slow => CompressionPreset::Slow,
        }
    }
}

#[derive(Default)]
struct ConversionConfig {
    mode: Option<PerformanceMode>,
    batch_size: Option<usize>,
    compress_threads: Option<usize>,
}

/// Run conversion once and return metrics.
fn run_conversion(
    input: &Path,
    output: &Path,
    preset: CompressionPreset,
    use_hyper: bool,
    conv_config: &ConversionConfig,
) -> Result<RunMetrics, Box<dyn std::error::Error>> {
    let input_size = std::fs::metadata(input)?.len();
    let start = Instant::now();

    if use_hyper {
        // Check if we should use auto-config
        let config = if let Some(mode) = conv_config.mode {
            // Use auto-config with performance mode
            let mut auto_config = PipelineAutoConfig::auto(mode);

            // Apply manual overrides if specified
            if let Some(batch_size) = conv_config.batch_size {
                auto_config = auto_config.with_batch_size(batch_size);
            }
            if let Some(threads) = conv_config.compress_threads {
                auto_config = auto_config.with_compression_threads(threads);
            }

            // Build config from auto-detected values
            auto_config.to_hyper_config(input, output).build()
        } else {
            // Use manual builder with legacy options
            let mut builder = HyperPipelineConfig::builder()
                .input_path(input)
                .output_path(output)
                .compression_level(preset.compression_level());

            // Apply batch size if specified
            if let Some(batch_size) = conv_config.batch_size {
                use roboflow_pipeline::hyper::config::{BatcherConfig, PrefetcherConfig};
                let batcher = BatcherConfig {
                    target_size: batch_size,
                    ..Default::default()
                };
                builder = builder.batcher(batcher);

                // Also scale prefetch block size proportionally
                let prefetcher = PrefetcherConfig {
                    block_size: (batch_size / 4).max(1024 * 1024), // At least 1MB
                    ..Default::default()
                };
                builder = builder.prefetcher(prefetcher);
            }

            // Apply compression threads if specified
            if let Some(threads) = conv_config.compress_threads {
                builder = builder.compression_threads(threads);
            }

            builder.build()?
        };

        let pipeline = HyperPipeline::new(config)?;
        let report = pipeline.run()?;

        let duration = start.elapsed();
        let output_size = std::fs::metadata(output)?.len();

        Ok(RunMetrics {
            duration_secs: duration.as_secs_f64(),
            throughput_mb_s: report.throughput_mb_s,
            compression_ratio: report.compression_ratio,
            message_count: report.message_count,
            chunks_written: report.chunks_written,
            input_size_mb: input_size as f64 / (1024.0 * 1024.0),
            output_size_mb: output_size as f64 / (1024.0 * 1024.0),
        })
    } else {
        // Use regular parallel pipeline
        let report = Robocodec::open(vec![input])?
            .write_to(output)
            .with_compression(preset)
            .run()?;

        let duration = start.elapsed();
        let output_size = std::fs::metadata(output)?.len();

        // Extract metrics from the report
        let report = match report {
            RunOutput::Hyper(r) => r,
            RunOutput::Batch(_) => {
                return Err("Expected single file report, got batch".into());
            }
        };

        Ok(RunMetrics {
            duration_secs: duration.as_secs_f64(),
            throughput_mb_s: report.throughput_mb_s,
            compression_ratio: report.compression_ratio,
            message_count: report.message_count,
            chunks_written: report.chunks_written,
            input_size_mb: input_size as f64 / (1024.0 * 1024.0),
            output_size_mb: output_size as f64 / (1024.0 * 1024.0),
        })
    }
}

struct RunMetrics {
    duration_secs: f64,
    throughput_mb_s: f64,
    compression_ratio: f64,
    message_count: u64,
    chunks_written: u64,
    input_size_mb: f64,
    output_size_mb: f64,
}

fn print_stats(label: &str, durations: &[f64], input_size: u64) {
    let n = durations.len();
    if n == 0 {
        eprintln!("Warning: {} called with empty durations slice", label);
        println!("{}: no data", label);
        return;
    }
    let avg = durations.iter().sum::<f64>() / n as f64;
    let min = durations.iter().fold(f64::INFINITY, |a, b| a.min(*b));
    let max = durations.iter().fold(f64::NEG_INFINITY, |a, b| a.max(*b));

    // Sorted for percentiles
    let mut sorted = durations.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p50 = sorted[n / 2];
    let p95 = sorted[(n * 95 / 100).min(n - 1)];
    let p99 = sorted[(n * 99 / 100).min(n - 1)];

    println!("{}:", label);
    println!("  avg: {:.2}s", avg);
    println!("  min: {:.2}s", min);
    println!("  max: {:.2}s", max);
    println!("  p50: {:.2}s", p50);
    println!("  p95: {:.2}s", p95);
    println!("  p99: {:.2}s", p99);
    println!(
        "  throughput: {:.1} MB/s",
        (input_size as f64 / 1024.0 / 1024.0) / avg
    );
}

/// Filters out cargo bench arguments that should not be passed to our CLI.
/// Properly handles both --flag=value and --flag value formats.
fn filter_cargo_bench_args(args: &[String]) -> Vec<String> {
    let mut filtered = Vec::new();
    let mut iter = args.iter().peekable();

    while let Some(arg) = iter.next() {
        // Skip --bench and its variants
        if arg.starts_with("--bench") {
            continue;
        }

        // Skip --nocapture
        if arg == "--nocapture" {
            continue;
        }

        // Handle --test-threads in both formats:
        // 1. --test-threads=N (single arg)
        // 2. --test-threads N (two args)
        if arg.starts_with("--test-threads") {
            // If it's the separate format (--test-threads N), skip the next arg too
            if arg == "--test-threads" {
                // Peek at next arg to see if it's the value (starts with digit)
                if let Some(next) = iter.peek() {
                    // If next looks like a number (the thread count), skip it
                    if next.chars().next().is_some_and(|c| c.is_ascii_digit()) {
                        iter.next();
                    }
                }
            }
            // Always skip --test-threads (whether it's --test-threads or --test-threads=N)
            continue;
        }

        filtered.push(arg.clone());
    }

    filtered
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Filter out cargo bench's extra arguments (--bench, --nocapture, --test-threads, etc.)
    // Properly handle both --test-threads=N and --test-threads N formats
    let raw_args: Vec<String> = std::env::args().collect();
    let args = filter_cargo_bench_args(&raw_args);
    let cli = Cli::parse_from(args);

    match cli.command {
        Commands::Run {
            input,
            output,
            preset,
            hyper,
            mode,
            batch_size,
            compress_threads,
        } => {
            if !input.exists() {
                eprintln!("Error: Input file not found: {}", input.display());
                std::process::exit(1);
            }

            // Check if output already exists
            if output.exists() {
                eprintln!("Error: Output file already exists: {}", output.display());
                std::process::exit(1);
            }

            println!("Converting: {} -> {}", input.display(), output.display());
            println!("Preset: {:?}", preset);
            println!(
                "Pipeline: {}",
                if hyper {
                    "HyperPipeline (async)"
                } else {
                    "Parallel"
                }
            );
            if hyper {
                if let Some(m) = mode {
                    println!("Performance mode: {:?}", m);
                }
                if let Some(bs) = batch_size {
                    println!(
                        "Batch size: {} bytes ({:.2} MB)",
                        bs,
                        bs as f64 / 1024.0 / 1024.0
                    );
                }
                if let Some(ct) = compress_threads {
                    println!("Compression threads: {}", ct);
                }
            }
            println!("WindowLog: auto-detected from CPU cache");
            println!();

            let conv_config = ConversionConfig {
                mode: mode.map(|m| m.to_mode()),
                batch_size,
                compress_threads,
            };
            let metrics = run_conversion(&input, &output, preset.to_preset(), hyper, &conv_config)?;

            println!();
            println!("=== Conversion Complete ===");
            println!("Output: {}", output.display());
            println!("Input size: {:.2} MB", metrics.input_size_mb);
            println!("Output size: {:.2} MB", metrics.output_size_mb);
            println!("Duration: {:.2}s", metrics.duration_secs);
            println!("Throughput: {:.2} MB/s", metrics.throughput_mb_s);
            println!("Compression ratio: {:.2}", metrics.compression_ratio);
            println!("Messages: {}", metrics.message_count);
            println!("Chunks: {}", metrics.chunks_written);
        }

        Commands::Bench {
            input,
            output,
            warmup,
            runs,
            preset,
            verbose,
            hyper,
            mode,
            batch_size,
            compress_threads,
        } => {
            if !input.exists() {
                eprintln!("Error: Input file not found: {}", input.display());
                std::process::exit(1);
            }

            // Remove output file if it exists (benchmark should overwrite)
            if output.exists() {
                let _ = std::fs::remove_file(&output);
            }

            let preset = preset.to_preset();
            let input_size = std::fs::metadata(&input)?.len();

            println!("profiler: {:?} preset", preset);
            println!(
                "pipeline: {}",
                if hyper {
                    "HyperPipeline (async)"
                } else {
                    "Parallel"
                }
            );
            if hyper {
                if let Some(m) = mode {
                    println!("mode: {:?}", m);
                }
                if let Some(bs) = batch_size {
                    println!(
                        "batch_size: {} bytes ({:.2} MB)",
                        bs,
                        bs as f64 / 1024.0 / 1024.0
                    );
                }
                if let Some(ct) = compress_threads {
                    println!("compress_threads: {}", ct);
                }
            }
            println!("input: {}", input.display());
            println!("input_mb: {:.2}", input_size as f64 / 1024.0 / 1024.0);
            println!("output: {}", output.display());
            println!("warmup: {}", warmup);
            println!("runs: {}", runs);
            if runs == 0 {
                eprintln!("Warning: runs=0: no measured runs will be executed");
            }
            println!("WindowLog: auto-detected from CPU cache");
            println!();

            let conv_config = ConversionConfig {
                mode: mode.map(|m| m.to_mode()),
                batch_size,
                compress_threads,
            };

            // Warmup phase (fill caches, stabilize)
            if warmup > 0 {
                for i in 0..warmup {
                    // Use a temp file for warmup
                    let warmup_output = output.with_extension(format!("warmup{}.mcap", i));
                    let _ = run_conversion(&input, &warmup_output, preset, hyper, &conv_config)?;
                    if let Err(e) = std::fs::remove_file(&warmup_output) {
                        eprintln!(
                            "Warning: Failed to remove warmup file {}: {}",
                            warmup_output.display(),
                            e
                        );
                    }
                    if verbose {
                        println!("  warmup {}/{}: ...", i + 1, warmup);
                    }
                }
            }

            // Measured runs - only keep the last one, delete previous outputs
            let mut durations = Vec::with_capacity(runs);
            for i in 0..runs {
                // For each run except the last, use a temp file and delete it
                let run_output = if i < runs - 1 {
                    output.with_extension(format!("run{}.mcap", i))
                } else {
                    output.clone()
                };

                let metrics = run_conversion(&input, &run_output, preset, hyper, &conv_config)?;
                durations.push(metrics.duration_secs);

                // Delete temp files from intermediate runs
                if i < runs - 1
                    && let Err(e) = std::fs::remove_file(&run_output)
                {
                    eprintln!(
                        "Warning: Failed to remove temp file {}: {}",
                        run_output.display(),
                        e
                    );
                }

                if verbose {
                    println!("  run {}/{}: {:.2}s", i + 1, runs, metrics.duration_secs);
                } else if runs <= 10 || (i + 1) % (runs / 2) == 0 {
                    println!("  {}/{}: {:.2}s", i + 1, runs, metrics.duration_secs);
                }
            }

            println!();
            print_stats("steady-state", &durations, input_size);
            println!();
            println!("Final output: {}", output.display());
        }

        #[cfg(feature = "profiling")]
        Commands::Profile {
            input,
            output,
            profile_output,
            preset,
            frequency,
            save_trace,
            hyper,
            mode,
            batch_size,
            compress_threads,
        } => {
            if !input.exists() {
                eprintln!("Error: Input file not found: {}", input.display());
                std::process::exit(1);
            }

            // Check if output already exists
            if output.exists() {
                eprintln!("Error: Output file already exists: {}", output.display());
                std::process::exit(1);
            }

            println!("Starting profile run...");
            println!("  input: {}", input.display());
            println!("  output: {}", output.display());
            println!("  profile output: {}", profile_output.display());
            println!("  frequency: {} Hz", frequency);
            println!(
                "  pipeline: {}",
                if hyper {
                    "HyperPipeline (async)"
                } else {
                    "Parallel"
                }
            );
            if hyper && let Some(m) = mode {
                println!("  mode: {:?}", m);
            }
            println!("  window_log: auto-detected from CPU cache");
            println!();

            let profile_dir = profile_output.parent().unwrap_or(Path::new("."));
            if !profile_dir.exists() {
                std::fs::create_dir_all(profile_dir)?;
            }

            // Run with profiling
            let guard = pprof::ProfilerGuard::new(frequency)
                .map_err(|e| format!("Failed to create profiler: {}", e))?;

            let conv_config = ConversionConfig {
                mode: mode.map(|m| m.to_mode()),
                batch_size,
                compress_threads,
            };
            let metrics = run_conversion(&input, &output, preset.to_preset(), hyper, &conv_config)?;

            // Generate reports
            let report = guard.report().build()?;

            // Save SVG flamegraph
            let svg_path = format!("{}.svg", profile_output.display());
            let file = std::fs::File::create(&svg_path)?;
            report.flamegraph(file)?;
            println!("Flamegraph saved to: {}", svg_path);

            // Save protobuf trace (for pprof tool, Google Chrome tracing, etc.)
            if save_trace {
                use pprof::protos::Message;
                use std::io::Write;
                let trace_path = format!("{}.pb", profile_output.display());
                let mut trace_file = std::fs::File::create(&trace_path)?;

                // Get the protobuf profile and encode it
                let proto = report.pprof()?;
                let encoded = proto.encode_to_vec();
                trace_file.write_all(&encoded)?;
                println!("Protobuf trace saved to: {}", trace_path);
            }

            println!();
            println!("=== Conversion Complete ===");
            println!("Output: {}", output.display());
            println!("Input size: {:.2} MB", metrics.input_size_mb);
            println!("Output size: {:.2} MB", metrics.output_size_mb);
            println!("Duration: {:.2}s", metrics.duration_secs);
            println!("Throughput: {:.2} MB/s", metrics.throughput_mb_s);
            println!("Compression ratio: {:.2}", metrics.compression_ratio);
            println!("Messages: {}", metrics.message_count);
            println!("Chunks: {}", metrics.chunks_written);
        }
    }

    Ok(())
}
